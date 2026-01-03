const express = require("express");
const crypto = require("crypto");
const fetch = require("node-fetch");
const { google } = require("googleapis");

const CHANNEL_SECRET = process.env.LINE_CHANNEL_SECRET;
const CHANNEL_ACCESS_TOKEN = process.env.LINE_CHANNEL_ACCESS_TOKEN;

const SHEETS_SA_KEY_JSON = process.env.KAI_BOT_SHEETS_SA_KEY_JSON; // JSON文字列
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

async function push(to, messages) {
  await lineApi("/v2/bot/message/push", { to, messages });
}

// ===== Trigger: KAI bot official name =====
// 仕様（あなたの要望どおり）
// - "@KAI bot"/"＠KAI bot" は文中どこでも反応（@/＠必須）
// - "ボット/ぼっと/おーい" は文頭のみ反応
function isTriggeredTextEvent(event) {
  if (event.type !== "message") return false;
  if (!event.message || event.message.type !== "text") return false;

  const raw = String(event.message.text || "");
  const text = raw.replace(/\u3000/g, " ").trim();

  const anywhereKai = /[@＠]\s*KAI\s*bot/i;
  if (anywhereKai.test(text)) return true;

  const headOnly = /^(?:ボット|ぼっと|おーい)(?:\s|[、,。.!！?？:：]|$)/;
  return headOnly.test(text);
}

// ===== Flex UI =====
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
          { type: "button", style: "primary", action: { type: "postback", label: "タスク追加", data: "a=task_new" } },
          { type: "button", style: "secondary", action: { type: "postback", label: "タスク一覧", data: "a=task_list" } },
          { type: "button", style: "secondary", action: { type: "postback", label: "設定", data: "a=settings" } },
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

// ===== Sheets Adapter (MVP) =====
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

async function sheetsGetTasksByGroup(groupId, limit = 20) {
  const sheets = getSheetsClient();
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: "Tasks!A:K",
  });

  const values = res.data.values || [];
  if (values.length <= 1) return [];

  const header = values[0];
  const rows = values.slice(1);

  const idx = Object.fromEntries(header.map((h, i) => [h, i]));
  const out = [];
  const gid = String(groupId || "").trim();

  const sampleGroupValues = rows.slice(0, 5).map((r) => String((r || [])[idx.group_id] || "").trim());
  console.log("tasks_sheet_debug", {
    groupId: gid,
    headerPreview: header.slice(0, 12),
    sampleGroupValues,
  });

  for (const r of rows) {
    const rGid = String(r[idx.group_id] || "").trim();
    if (rGid !== gid) continue;
    out.push({
      task_id: r[idx.task_id] || "",
      title: r[idx.title] || "",
      status: r[idx.status] || "",
      due_at: r[idx.due_at] || "",
      created_at: r[idx.created_at] || "",
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

function getGroupId(event) {
  const s = event.source || {};
  return s.type === "group" ? s.groupId : null;
}

function pickReplyTarget(event) {
  const s = event.source || {};
  return s.groupId || s.roomId || s.userId || null;
}

app.get("/", (req, res) => res.status(200).send("ok"));

app.post("/line/webhook", async (req, res) => {
  if (!verifySignature(req)) {
    console.warn("invalid signature (LINE_CHANNEL_SECRET mismatch or bad request)");
    return res.status(401).send("invalid signature");
  }

  const body = req.body || {};
  const events = Array.isArray(body.events) ? body.events : [];

  // 先に 200 を返す（LINE のタイムアウト回避）
  res.status(200).send("ok");

  console.log("webhook_received", {
    destination: body.destination || null,
    eventsCount: events.length,
  });

  for (const event of events) {
    try {
      const src = event.source || {};
      const summary = {
        type: event.type,
        replyToken: !!event.replyToken,
        sourceType: src.type || null,
        groupId: src.groupId ? "(present)" : null,
        roomId: src.roomId ? "(present)" : null,
        userId: src.userId ? "(present)" : null,
      };

      if (event.type === "message" && event.message) {
        summary.messageType = event.message.type;
        if (event.message.type === "text") {
          summary.text = String(event.message.text || "").slice(0, 200);
          summary.mention = !!event.message.mention;
        }
      }
      if (event.type === "postback") {
        summary.postbackData = String((event.postback && event.postback.data) || "").slice(0, 200);
      }

      console.log("event", summary);

      const target = pickReplyTarget(event);

      // ---- Postback は必ず即ACK（連打防止）＋ 誰が触ったか通知 ----
      if (event.type === "postback" && event.replyToken) {
        await reply(event.replyToken, [{ type: "text", text: "受け付けました。反映します…" }]);

        const who =
          src.userId ? `操作者: ${src.userId.slice(0, 6)}…` : "操作者: (unknown)"; // まずは識別子だけ
        if (target) {
          await push(target, [{ type: "text", text: `${who}\n処理中…（連打しないでOK）` }]);
        }

        const pb = parsePostbackData(event.postback && event.postback.data);
        const groupId = getGroupId(event);

        if (pb.a === "task_new") {
          if (target) {
            await push(target, [
              {
                type: "text",
                text:
                  "タスク追加（現状は定型入力）:\n" +
                  "例: @KAI bot タスク: 議事録作成 / 期限: 2026-01-10 18:00 / status: open",
              },
            ]);
          }
          continue;
        }

        if (pb.a === "task_list") {
          if (!groupId) {
            if (target) await push(target, [{ type: "text", text: "タスク一覧はグループ内で使用してください。" }]);
            continue;
          }
          const tasks = await sheetsGetTasksByGroup(groupId, 20);
          if (target) await push(target, [{ type: "text", text: formatTaskList(tasks) }]);
          continue;
        }

        if (pb.a === "settings") {
          if (target) await push(target, [{ type: "text", text: "設定UIは次で実装します（notify/quota/powerusers）。" }]);
          continue;
        }

        if (target) await push(target, [buildMenuFlex()]);
        continue;
      }

      // ---- テキストトリガー ----
      if (event.replyToken && event.type === "message" && event.message && event.message.type === "text") {
        const triggered = isTriggeredTextEvent(event);
        console.log("trigger_check", { text: String(event.message.text || "").slice(0, 200), triggered });
        if (triggered) {
          await reply(event.replyToken, [buildMenuFlex()]);
          continue;
        }
      }
    } catch (e) {
      console.error("Event handling error:", e);
    }
  }
});

const port = process.env.PORT || 8080;
app.listen(port, () => console.log(`Listening on ${port}`));
