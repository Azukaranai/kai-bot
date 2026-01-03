const express = require("express");
const crypto = require("crypto");
const fetch = require("node-fetch");
const { google } = require("googleapis");

const CHANNEL_SECRET = process.env.LINE_CHANNEL_SECRET;
const CHANNEL_ACCESS_TOKEN = process.env.LINE_CHANNEL_ACCESS_TOKEN;

const SHEETS_SA_KEY_JSON = process.env.KAI_BOT_SHEETS_SA_KEY_JSON;
const SPREADSHEET_ID = process.env.KAI_BOT_SHEETS_SPREADSHEET_ID;

const app = express();

// ===== LINE: raw body for signature verify =====
app.use(
  express.json({
    verify: (req, res, buf) => {
      req.rawBody = buf;
    },
  })
);

function verifySignature(req) {
  const signature = req.get("x-line-signature");
  if (!signature || !req.rawBody || !CHANNEL_SECRET) return false;

  const hmac = crypto.createHmac("sha256", CHANNEL_SECRET);
  hmac.update(req.rawBody);
  const computed = hmac.digest("base64");

  if (computed.length !== signature.length) return false;
  return crypto.timingSafeEqual(Buffer.from(computed), Buffer.from(signature));
}

function parsePostbackData(data) {
  const out = {};
  if (!data) return out;
  for (const part of String(data).split("&")) {
    const [k, v] = part.split("=");
    if (!k) continue;
    out[decodeURIComponent(k)] = decodeURIComponent(v || "");
  }
  return out;
}

async function lineApi(path, body) {
  const res = await fetch(`https://api.line.me${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${CHANNEL_ACCESS_TOKEN}`,
    },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`LINE API failed: ${res.status} ${text}`);
  }
}

async function reply(replyToken, messages) {
  await lineApi("/v2/bot/message/reply", { replyToken, messages });
}

// ===== Trigger: KAI bot official name =====
// - "@KAI bot"/"＠KAI bot" は文中どこでも反応（@/＠必須）
// - "ボット/ぼっと/おーい" は文頭のみ反応
function isTriggeredText(textRaw) {
  const text = String(textRaw || "").replace(/\u3000/g, " ").trim();
  if (/[@＠]\s*KAI\s*bot/i.test(text)) return true;
  return /^(?:ボット|ぼっと|おーい)(?:\s|[、,。.!！?？:：]|$)/.test(text);
}

function stripTrigger(textRaw) {
  let text = String(textRaw || "").replace(/\u3000/g, " ").trim();
  text = text.replace(/[@＠]\s*KAI\s*bot\s*/gi, "").trim();
  text = text.replace(/^(?:ボット|ぼっと|おーい)(?:\s|[、,。.!！?？:：]|$)\s*/i, "").trim();
  return text;
}

// ===== Throttle (best-effort; per-instance) =====
const recentActions = new Map();
function isThrottled(key, ms = 2000) {
  const now = Date.now();
  const last = recentActions.get(key) || 0;
  if (now - last < ms) return true;
  recentActions.set(key, now);
  return false;
}

// ===== Flex UI (displayText付き) =====
function buildMenuFlex() {
  return {
    type: "flex",
    altText: "KAI bot メニュー",
    contents: {
      type: "bubble",
      header: {
        type: "box",
        layout: "vertical",
        contents: [{ type: "text", text: "KAI bot メニュー", weight: "bold", size: "lg" }],
      },
      body: {
        type: "box",
        layout: "vertical",
        spacing: "md",
        contents: [
          {
            type: "button",
            style: "primary",
            action: { type: "postback", label: "タスク追加", data: "a=task_new", displayText: "KAI bot：タスク追加" },
          },
          {
            type: "button",
            style: "secondary",
            action: { type: "postback", label: "タスク一覧", data: "a=task_list", displayText: "KAI bot：タスク一覧" },
          },
          {
            type: "button",
            style: "secondary",
            action: { type: "postback", label: "ヘルプ", data: "a=help", displayText: "KAI bot：ヘルプ" },
          },
        ],
      },
      footer: {
        type: "box",
        layout: "vertical",
        contents: [
          {
            type: "text",
            text: "呼び出し: @KAI bot（文中OK） / ボット・ぼっと・おーい（文頭のみ）",
            size: "sm",
            color: "#666666",
            wrap: true,
          },
        ],
      },
    },
  };
}

// ===== Sheets Adapter =====
function getSheetsClient() {
  if (!SHEETS_SA_KEY_JSON) throw new Error("Missing env: KAI_BOT_SHEETS_SA_KEY_JSON");
  if (!SPREADSHEET_ID) throw new Error("Missing env: KAI_BOT_SHEETS_SPREADSHEET_ID");

  const key = JSON.parse(SHEETS_SA_KEY_JSON);
  const auth = new google.auth.JWT({
    email: key.client_email,
    key: key.private_key,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });

  return google.sheets({ version: "v4", auth });
}

async function sheetsAppendRow(sheetName, rowValues) {
  const sheets = getSheetsClient();
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `${sheetName}!A:Z`,
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    requestBody: { values: [rowValues] },
  });
}

async function sheetsGetTasksByGroup(spaceId, limit = 20) {
  const sheets = getSheetsClient();
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: "Tasks!A:K",
  });

  const values = res.data.values || [];
  if (!values.length) return [];

  const firstRow = (values[0] || []).map((x) => String(x || "").trim());
  const looksHeader =
    firstRow.includes("task_id") ||
    firstRow.includes("group_id") ||
    firstRow.includes("title") ||
    firstRow.includes("status");

  const header = looksHeader ? firstRow : null;
  const rows = looksHeader ? values.slice(1) : values;

  // appendの列順（ヘッダが壊れていても読める）
  // A task_id / B group_id(spaceId) / D title / F status / H due_at
  const idx = header ? Object.fromEntries(header.map((h, i) => [h, i])) : {};
  const iGroup = idx["group_id"] ?? 1;
  const iTitle = idx["title"] ?? 3;
  const iStatus = idx["status"] ?? 5;
  const iDue = idx["due_at"] ?? 7;

  const gid = String(spaceId || "").trim();
  const sampleGroupValues = rows.slice(0, 8).map((r) => String((r || [])[iGroup] || "").trim());

  // 一覧が出ない原因はここで特定できる（Cloud Run logs で見る）
  console.log("tasks_sheet_debug", {
    looksHeader,
    headerPreview: header ? header.slice(0, 12) : null,
    spaceId: gid,
    iGroup,
    iTitle,
    iStatus,
    iDue,
    sampleGroupValues,
  });

  const out = [];
  for (const r of rows) {
    const rGid = String((r[iGroup] || "")).trim();
    if (rGid !== gid) continue;
    out.push({
      title: r[iTitle] || "",
      status: r[iStatus] || "",
      due_at: r[iDue] || "",
    });
    if (out.length >= limit) break;
  }
  return out;
}

function formatTaskList(tasks) {
  if (!tasks.length) return "このグループのタスクはまだありません。";
  const lines = tasks.map((t, i) => {
    const due = t.due_at ? `期限: ${t.due_at}` : "期限: なし";
    const st = t.status ? `status: ${t.status}` : "status: (未設定)";
    return `${i + 1}. ${t.title} / ${due} / ${st}`;
  });
  return lines.join("\n");
}

function getSpaceId(event) {
  const s = event.source || {};
  if (s.type === "group") return s.groupId;
  if (s.type === "room") return s.roomId; // 念のため
  return null;
}

function getUserId(event) {
  const s = event.source || {};
  return s.userId || "";
}

// 定型パース: "タスク: ... / 期限: ... / status: ..."
function parseTaskCommand(text) {
  const parts = String(text || "")
    .split(/[\/／]/)
    .map((p) => p.trim())
    .filter(Boolean);

  const kv = {};
  for (const p of parts) {
    const m = p.match(/^([^:：]+)\s*[:：]\s*(.+)$/);
    if (!m) continue;
    const k = m[1].trim().toLowerCase();
    const v = m[2].trim();
    kv[k] = v;
  }

  const title = kv["タスク"] || kv["task"] || kv["title"] || "";
  const due_at = kv["期限"] || kv["due"] || kv["due_at"] || "";
  const status = (kv["status"] || kv["ステータス"] || "open").toLowerCase();
  const description = kv["description"] || kv["説明"] || "";

  return { title, due_at, status, description };
}

function helpText() {
  return (
    "KAI bot ヘルプ（MVP）\n\n" +
    "1) メニュー表示\n" +
    "  おーい / ボット / ぼっと（文頭） または 文中に @KAI bot\n\n" +
    "2) タスク追加（定型）\n" +
    "  @KAI bot タスク: 議事録作成 / 期限: 2026-01-10 18:00 / status: open\n\n" +
    "3) タスク一覧\n" +
    "  メニュー → タスク一覧\n"
  );
}

app.get("/", (req, res) => res.status(200).send("ok"));

app.post("/line/webhook", async (req, res) => {
  if (!verifySignature(req)) return res.status(401).send("invalid signature");

  const events = req.body && Array.isArray(req.body.events) ? req.body.events : [];
  res.status(200).send("ok");

  for (const event of events) {
    try {
      // postback
      if (event.type === "postback" && event.replyToken) {
        const pb = parsePostbackData(event.postback && event.postback.data);
        const spaceId = getSpaceId(event);
        const userId = getUserId(event);

        const throttleKey = `${spaceId || "nospace"}:${userId || "nouser"}:${pb.a || "unknown"}`;
        if (isThrottled(throttleKey, 2000)) continue;

        if (pb.a === "task_new") {
          await reply(event.replyToken, [
            {
              type: "text",
              text:
                "タスク追加（定型）:\n" +
                "@KAI bot タスク: 議事録作成 / 期限: 2026-01-10 18:00 / status: open\n" +
                "（期限なしも可。status は open/done を想定）",
            },
          ]);
          continue;
        }

        if (pb.a === "task_list") {
          if (!spaceId) {
            await reply(event.replyToken, [{ type: "text", text: "タスク一覧はグループ（または複数人トーク）内で使用してください。" }]);
            continue;
          }
          const tasks = await sheetsGetTasksByGroup(spaceId, 20);
          await reply(event.replyToken, [{ type: "text", text: formatTaskList(tasks) }]);
          continue;
        }

        if (pb.a === "help") {
          await reply(event.replyToken, [{ type: "text", text: helpText() }]);
          continue;
        }

        await reply(event.replyToken, [buildMenuFlex()]);
        continue;
      }

      // trigger text -> menu or task add
      if (event.type === "message" && event.replyToken && event.message && event.message.type === "text") {
        const raw = event.message.text;
        if (!isTriggeredText(raw)) continue;

        const spaceId = getSpaceId(event);
        if (!spaceId) {
          await reply(event.replyToken, [{ type: "text", text: "KAI bot は現在グループ（または複数人トーク）での使用を前提にしています。" }]);
          continue;
        }

        const body = stripTrigger(raw);

        if (/^(help|ヘルプ)$/i.test(body)) {
          await reply(event.replyToken, [{ type: "text", text: helpText() }]);
          continue;
        }

        if (body.includes("タスク") || /task\s*[:：]/i.test(body)) {
          const { title, due_at, status, description } = parseTaskCommand(body);

          if (!title) {
            await reply(event.replyToken, [
              { type: "text", text: "タスク名が不足しています。例:\n@KAI bot タスク: 議事録作成 / 期限: 2026-01-10 18:00 / status: open" },
            ]);
            continue;
          }

          const now = new Date().toISOString();
          const task_id = `tsk_${crypto.randomUUID()}`;
          const creator_user_id = getUserId(event);

          const normalizedStatus = status === "done" ? "done" : "open";
          const done_at = normalizedStatus === "done" ? now : "";

          console.log("task_append_debug", { spaceId, task_id, title });

          await sheetsAppendRow("Tasks", [
            task_id,
            spaceId,            // group_id (space)
            "",                 // project_id
            title,
            description,
            normalizedStatus,
            now,                // created_at
            due_at || "",       // due_at
            done_at,            // done_at
            creator_user_id,
            "[]",               // assignees_json (MVP)
          ]);

          const dueMsg = due_at ? `期限: ${due_at}` : "期限: なし（設定されていません）";
          await reply(event.replyToken, [{ type: "text", text: `追加しました。\n${title}\n${dueMsg}\nstatus: ${normalizedStatus}` }]);
          continue;
        }

        await reply(event.replyToken, [buildMenuFlex()]);
      }
    } catch (e) {
      console.error("Event handling error:", e);
    }
  }
});

const port = process.env.PORT || 8080;
app.listen(port, () => console.log(`Listening on ${port}`));
