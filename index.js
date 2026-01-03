
require("dotenv").config();

const express = require("express");
const crypto = require("crypto");
const fetch = require("node-fetch");
const { google } = require("googleapis");

// =====================
// Env
// =====================
const CHANNEL_SECRET = process.env.LINE_CHANNEL_SECRET;
const CHANNEL_ACCESS_TOKEN = process.env.LINE_CHANNEL_ACCESS_TOKEN;

const SHEETS_SA_KEY_JSON = process.env.KAI_BOT_SHEETS_SA_KEY_JSON; // JSON string
const SPREADSHEET_ID = process.env.KAI_BOT_SHEETS_SPREADSHEET_ID;

// Vertex AI (preferred: no API key; uses Cloud Run service account)
// Default to global endpoint so you don't have to match Cloud Run region.
const VERTEX_PROJECT =
  process.env.KAI_BOT_GCP_PROJECT ||
  process.env.GOOGLE_CLOUD_PROJECT ||
  process.env.GCLOUD_PROJECT ||
  process.env.GCP_PROJECT;
const VERTEX_LOCATION = process.env.KAI_BOT_VERTEX_LOCATION || process.env.KAI_BOT_GCP_LOCATION || "global";
const VERTEX_MODEL_ID = process.env.KAI_BOT_VERTEX_MODEL_ID || process.env.KAI_BOT_GEMINI_MODEL || "gemini-2.5-flash";

// =====================
// Express / LINE signature verify
// =====================
const app = express();
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

// =====================
// LINE Messaging API
// =====================
async function lineApi(path, method, body) {
  const res = await fetch(`https://api.line.me${path}`, {
    method,
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${CHANNEL_ACCESS_TOKEN}`,
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`LINE API failed: ${res.status} ${text}`);
  }

  // Some endpoints return empty body
  const ct = res.headers.get("content-type") || "";
  if (ct.includes("application/json")) return res.json();
  return res.text();
}

async function reply(replyToken, messages) {
  await lineApi("/v2/bot/message/reply", "POST", { replyToken, messages });
}

async function push(to, messages) {
  await lineApi("/v2/bot/message/push", "POST", { to, messages });
}

async function getLineDisplayName(source) {
  // Best-effort. If it fails, fall back to masked userId.
  try {
    if (!source || !source.userId) return null;

    if (source.type === "user") {
      const prof = await lineApi(`/v2/bot/profile/${source.userId}`, "GET");
      return prof && prof.displayName ? prof.displayName : null;
    }

    if (source.type === "group" && source.groupId) {
      const prof = await lineApi(
        `/v2/bot/group/${source.groupId}/member/${source.userId}`,
        "GET"
      );
      return prof && prof.displayName ? prof.displayName : null;
    }

    if (source.type === "room" && source.roomId) {
      const prof = await lineApi(
        `/v2/bot/room/${source.roomId}/member/${source.userId}`,
        "GET"
      );
      return prof && prof.displayName ? prof.displayName : null;
    }

    return null;
  } catch (e) {
    console.warn("getLineDisplayName failed", e && e.message ? e.message : e);
    return null;
  }
}

// =====================
// Trigger: KAI bot official name
// =====================
// Spec (per your requirement)
// - "@KAI bot" / "＠KAI bot" triggers anywhere in text (requires @/＠)
// - "ボット/ぼっと/おーい" triggers only at head
function isTriggeredText(text) {
  const t = String(text || "").replace(/\u3000/g, " ").trim();
  const anywhereKai = /[@＠]\s*KAI\s*bot/i;
  if (anywhereKai.test(t)) return true;

  const headOnly = /^(?:ボット|ぼっと|おーい)(?:\s|[、,。.!！?？:：]|$)/;
  return headOnly.test(t);
}

function stripTriggerPrefix(text) {
  let t = String(text || "").replace(/\u3000/g, " ").trim();
  // remove @KAI bot anywhere
  t = t.replace(/[@＠]\s*KAI\s*bot\s*/gi, " ");
  // remove head triggers
  t = t.replace(/^(?:ボット|ぼっと|おーい)(?:\s|[、,。.!！?？:：])*/i, " ");
  return t.replace(/\s+/g, " ").trim();
}

// =====================
// Flex UI
// =====================
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
          { type: "button", style: "secondary", action: { type: "postback", label: "プロジェクト追加", data: "a=project_new" } },
          { type: "button", style: "secondary", action: { type: "postback", label: "プロジェクト一覧", data: "a=project_list" } },
          { type: "button", style: "secondary", action: { type: "postback", label: "設定", data: "a=settings" } },
        ],
      },
      footer: {
        type: "box",
        layout: "vertical",
        contents: [
          {
            type: "text",
            text: "呼び出し: @KAI bot（文中OK） / ボット・ぼっと・おーい（文頭のみ）\n自然文例: ‘議事録作成を明日18時までに追加’",
            size: "sm",
            color: "#666666",
            wrap: true,
          },
        ],
      },
    },
  };
}

// =====================
// Sheets (Google Sheets API)
// =====================
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

async function sheetsGetValues(rangeA1) {
  const sheets = getSheetsClient();
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: rangeA1,
  });
  return res.data.values || [];
}

async function sheetsAppendRow(sheetName, rowValues) {
  const sheets = getSheetsClient();
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `${sheetName}!A:Z`,
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    resource: { values: [rowValues] },
  });
}

function headerIndex(headerRow) {
  return Object.fromEntries((headerRow || []).map((h, i) => [String(h || "").trim(), i]));
}

function requireColumns(idx, cols, sheetName) {
  const missing = cols.filter((c) => idx[c] === undefined);
  if (missing.length) {
    throw new Error(
      `${sheetName} sheet is missing required columns: ${missing.join(", ")}. ` +
        `Please ensure header row contains at least: ${cols.join(", ")}`
    );
  }
}

function makeId(prefix) {
  const ts = Date.now().toString(36);
  const rnd = crypto.randomBytes(3).toString("hex");
  return `${prefix}_${ts}_${rnd}`;
}

function getSpaceId(event) {
  const s = event.source || {};
  // For tasks/projects, treat group/room/user uniformly.
  return s.groupId || s.roomId || s.userId || null;
}

async function sheetsGetTasksBySpace(spaceId, limit = 20) {
  const values = await sheetsGetValues("Tasks!A:Z");
  if (values.length <= 1) return [];

  const header = values[0];
  const rows = values.slice(1);
  const idx = headerIndex(header);

  requireColumns(idx, ["task_id", "group_id", "title"], "Tasks");

  const sid = String(spaceId || "").trim();
  const out = [];

  for (const r of rows) {
    const rSid = String(r[idx.group_id] || "").trim();
    if (rSid !== sid) continue;
    out.push({
      task_id: r[idx.task_id] || "",
      project_id: idx.project_id !== undefined ? r[idx.project_id] || "" : "",
      title: r[idx.title] || "",
      description: idx.description !== undefined ? r[idx.description] || "" : "",
      status: idx.status !== undefined ? r[idx.status] || "" : "",
      due_at: idx.due_at !== undefined ? r[idx.due_at] || "" : "",
      created_at: idx.created_at !== undefined ? r[idx.created_at] || "" : "",
      done_at: idx.done_at !== undefined ? r[idx.done_at] || "" : "",
      created_by: idx.created_by !== undefined ? r[idx.created_by] || "" : "",
      updated_at: idx.updated_at !== undefined ? r[idx.updated_at] || "" : "",
    });
    if (out.length >= limit) break;
  }
  return out;
}

function formatTaskList(tasks) {
  if (!tasks.length) return "このスペースのタスクはまだありません。";
  const lines = tasks.map((t, i) => {
    const due = t.due_at ? `期限: ${t.due_at}` : "期限: なし";
    const st = t.status ? `status: ${t.status}` : "status: (未設定)";
    return `${i + 1}. ${t.title} / ${due} / ${st}`;
  });
  return lines.join("\n");
}

async function findTasksByQuery(spaceId, query, limit = 200) {
  const q = normalizeText(query);
  if (!q) return [];
  const tasks = await sheetsGetTasksBySpace(spaceId, limit);
  const idMatch = tasks.find((t) => String(t.task_id || "") === q);
  if (idMatch) return [idMatch];

  const low = q.toLowerCase();
  return tasks.filter((t) => String(t.title || "").toLowerCase().includes(low));
}

async function sheetsGetProjectsBySpace(spaceId, limit = 50) {
  const values = await sheetsGetValues("Projects!A:Z");
  if (values.length <= 1) return [];

  const header = values[0];
  const rows = values.slice(1);
  const idx = headerIndex(header);

  requireColumns(idx, ["project_id", "group_id", "title"], "Projects");

  const sid = String(spaceId || "").trim();
  const out = [];
  for (const r of rows) {
    const rSid = String(r[idx.group_id] || "").trim();
    if (rSid !== sid) continue;
    out.push({
      project_id: r[idx.project_id] || "",
      title: r[idx.title] || "",
      description: idx.description !== undefined ? r[idx.description] || "" : "",
      status: idx.status !== undefined ? r[idx.status] || "" : "",
      due_at: idx.due_at !== undefined ? r[idx.due_at] || "" : "",
      created_at: idx.created_at !== undefined ? r[idx.created_at] || "" : "",
    });
    if (out.length >= limit) break;
  }
  return out;
}

function formatProjectList(projects) {
  if (!projects.length) return "このスペースのプロジェクトはまだありません。";
  return projects
    .map((p, i) => {
      const st = p.status ? `status: ${p.status}` : "status: (未設定)";
      const due = p.due_at ? `期限: ${p.due_at}` : "期限: なし";
      return `${i + 1}. ${p.title} / ${due} / ${st}`;
    })
    .join("\n");
}

async function findProjectsByQuery(spaceId, query, limit = 200) {
  const q = normalizeText(query);
  if (!q) return [];
  const projects = await sheetsGetProjectsBySpace(spaceId, limit);
  const idMatch = projects.find((p) => String(p.project_id || "") === q);
  if (idMatch) return [idMatch];

  const low = q.toLowerCase();
  return projects.filter((p) => String(p.title || "").toLowerCase().includes(low));
}

async function sheetsAppendProject({ spaceId, title, description, status, due_at, created_by }) {
  const values = await sheetsGetValues("Projects!A:Z");
  if (values.length <= 0) throw new Error("Projects sheet is empty (need header row)");

  const header = values[0];
  const idx = headerIndex(header);
  requireColumns(idx, ["project_id", "group_id", "title"], "Projects");

  const row = new Array(header.length).fill("");
  const now = new Date().toISOString();

  row[idx.project_id] = makeId("prj");
  row[idx.group_id] = String(spaceId || "");
  row[idx.title] = title || "";
  if (idx.description !== undefined) row[idx.description] = description || "";
  if (idx.status !== undefined) row[idx.status] = status || "open";
  if (idx.due_at !== undefined) row[idx.due_at] = due_at || "";
  if (idx.created_at !== undefined) row[idx.created_at] = now;
  if (idx.created_by !== undefined) row[idx.created_by] = created_by || "";

  await sheetsAppendRow("Projects", row);
  return row[idx.project_id];
}

async function sheetsAppendTask({ spaceId, project_id, title, description, status, due_at, created_by }) {
  const values = await sheetsGetValues("Tasks!A:Z");
  if (values.length <= 0) throw new Error("Tasks sheet is empty (need header row)");

  const header = values[0];
  const idx = headerIndex(header);
  requireColumns(idx, ["task_id", "group_id", "title"], "Tasks");

  const row = new Array(header.length).fill("");
  const now = new Date().toISOString();

  row[idx.task_id] = makeId("tsk");
  row[idx.group_id] = String(spaceId || "");
  if (idx.project_id !== undefined) row[idx.project_id] = project_id || "";
  row[idx.title] = title || "";
  if (idx.description !== undefined) row[idx.description] = description || "";
  if (idx.status !== undefined) row[idx.status] = status || "open";
  if (idx.due_at !== undefined) row[idx.due_at] = due_at || "";
  if (idx.created_at !== undefined) row[idx.created_at] = now;
  if (idx.created_by !== undefined) row[idx.created_by] = created_by || "";
  if (idx.updated_at !== undefined) row[idx.updated_at] = now;

  await sheetsAppendRow("Tasks", row);
  return row[idx.task_id];
}

async function sheetsFindRowById(sheetName, id, idColumnIndex = 0) {
  const values = await sheetsGetValues(`${sheetName}!A:Z`);
  if (!values.length) return null;

  const firstRow = (values[0] || []).map((x) => String(x || "").trim());
  const looksHeader = firstRow.includes("task_id") || firstRow.includes("project_id") || firstRow.includes("group_id");
  const rows = looksHeader ? values.slice(1) : values;
  const baseRowNumber = looksHeader ? 2 : 1; // 1-indexed

  const targetId = String(id || "").trim();
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i] || [];
    const cell = String(r[idColumnIndex] || "").trim();
    if (cell === targetId) return baseRowNumber + i;
  }
  return null;
}

async function sheetsUpdateTask(taskId, patch) {
  const sheets = getSheetsClient();

  const values = await sheetsGetValues("Tasks!A:Z");
  if (!values.length) throw new Error("Tasks sheet is empty");

  const firstRow = (values[0] || []).map((x) => String(x || "").trim());
  const looksHeader = firstRow.includes("task_id") || firstRow.includes("group_id") || firstRow.includes("title");
  const header = looksHeader ? firstRow : null;
  const idx = header ? headerIndex(header) : {};

  const rowNumber = await sheetsFindRowById("Tasks", taskId, 0);
  if (!rowNumber) throw new Error(`Task not found: ${taskId}`);

  const rowRes = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `Tasks!A${rowNumber}:Z${rowNumber}`,
  });
  const row = rowRes.data.values && rowRes.data.values[0] ? rowRes.data.values[0] : [];

  const iTitle = idx["title"];
  const iDescription = idx["description"];
  const iStatus = idx["status"] ?? 5;
  const iDue = idx["due_at"] ?? 6;
  const iDone = idx["done_at"] ?? 8;
  const iProject = idx["project_id"];
  const iDeleted = idx["deleted_at"];
  const iUpdated = idx["updated_at"] ?? 11;

  function setCell(i, v) {
    while (row.length <= i) row.push("");
    row[i] = v;
  }

  const now = new Date().toISOString();
  if (patch.title !== undefined && iTitle !== undefined) setCell(iTitle, patch.title);
  if (patch.description !== undefined && iDescription !== undefined) setCell(iDescription, patch.description);
  if (patch.status !== undefined) setCell(iStatus, patch.status);
  if (patch.due_at !== undefined) setCell(iDue, patch.due_at);
  if (patch.done_at !== undefined) setCell(iDone, patch.done_at);
  if (patch.project_id !== undefined && iProject !== undefined) setCell(iProject, patch.project_id);
  if (patch.deleted_at !== undefined && iDeleted !== undefined) setCell(iDeleted, patch.deleted_at);
  if (idx["updated_at"] !== undefined) setCell(iUpdated, patch.updated_at !== undefined ? patch.updated_at : now);

  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: `Tasks!A${rowNumber}:Z${rowNumber}`,
    valueInputOption: "RAW",
    requestBody: { values: [row] },
  });
}

async function sheetsUpdateProject(projectId, patch) {
  const sheets = getSheetsClient();

  const values = await sheetsGetValues("Projects!A:Z");
  if (!values.length) throw new Error("Projects sheet is empty");

  const firstRow = (values[0] || []).map((x) => String(x || "").trim());
  const looksHeader = firstRow.includes("project_id") || firstRow.includes("group_id") || firstRow.includes("title");
  const header = looksHeader ? firstRow : null;
  const idx = header ? headerIndex(header) : {};

  const rowNumber = await sheetsFindRowById("Projects", projectId, 0);
  if (!rowNumber) throw new Error(`Project not found: ${projectId}`);

  const rowRes = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `Projects!A${rowNumber}:Z${rowNumber}`,
  });
  const row = rowRes.data.values && rowRes.data.values[0] ? rowRes.data.values[0] : [];

  const iTitle = idx["title"];
  const iDescription = idx["description"];
  const iStatus = idx["status"] ?? 4;
  const iDue = idx["due_at"] ?? 5;
  const iDeleted = idx["deleted_at"];
  const iUpdated = idx["updated_at"] ?? 8;

  function setCell(i, v) {
    while (row.length <= i) row.push("");
    row[i] = v;
  }

  const now = new Date().toISOString();
  if (patch.title !== undefined && iTitle !== undefined) setCell(iTitle, patch.title);
  if (patch.description !== undefined && iDescription !== undefined) setCell(iDescription, patch.description);
  if (patch.status !== undefined) setCell(iStatus, patch.status);
  if (patch.due_at !== undefined) setCell(iDue, patch.due_at);
  if (patch.deleted_at !== undefined && iDeleted !== undefined) setCell(iDeleted, patch.deleted_at);
  if (idx["updated_at"] !== undefined) setCell(iUpdated, patch.updated_at !== undefined ? patch.updated_at : now);

  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: `Projects!A${rowNumber}:Z${rowNumber}`,
    valueInputOption: "RAW",
    requestBody: { values: [row] },
  });
}

// =====================
// Vertex AI: Natural language -> structured command
// =====================
function extractJsonObject(text) {
  const s = String(text || "");
  const first = s.indexOf("{");
  const last = s.lastIndexOf("}");
  if (first === -1 || last === -1 || last <= first) return null;
  return s.slice(first, last + 1);
}

async function vertexGenerateJson({ userText, locale = "ja" }) {
  if (!VERTEX_PROJECT) {
    throw new Error(
      "Missing Vertex project id. Set env one of: KAI_BOT_GCP_PROJECT / GOOGLE_CLOUD_PROJECT / GCLOUD_PROJECT / GCP_PROJECT"
    );
  }

  const auth = new google.auth.GoogleAuth({
    scopes: ["https://www.googleapis.com/auth/cloud-platform"],
  });
  const client = await auth.getClient();
  const tokenResp = await client.getAccessToken();
  const token = typeof tokenResp === "string" ? tokenResp : tokenResp && tokenResp.token;
  if (!token) {
    throw new Error("Vertex access token is empty. Check service account auth.");
  }

  const endpoint = `https://${VERTEX_LOCATION}-aiplatform.googleapis.com/v1/projects/${VERTEX_PROJECT}/locations/${VERTEX_LOCATION}/publishers/google/models/${VERTEX_MODEL_ID}:generateContent`;

  const prompt =
    `あなたはタスク管理ボットのコマンド解析器です。必ずJSONのみで返してください。\n` +
    `次のいずれかの action を返してください: create_task, update_task, delete_task, complete_task, reopen_task, list_tasks, create_project, update_project, delete_project, list_projects, help, unknown\n` +
    `出力JSONスキーマ（省略可のキーは空文字でも可）:\n` +
    `{\n  "action":"...",\n  "task_id":"",\n  "project_id":"",\n  "title":"",\n  "new_title":"",\n  "description":"",\n  "due_at":"",\n  "status":"",\n  "project_title":"",\n  "query":""\n}\n` +
    `注意: ID が文中に無い場合は空文字にする。対象がID不明の場合は query にタイトル断片を入れる。期限は文にある場合だけ入れる（例: 2026-01-10 18:00）。\n` +
    `ユーザー入力: ${userText}`;

  const body = {
    contents: [{ role: "user", parts: [{ text: prompt }] }],
    generationConfig: {
      temperature: 0,
      maxOutputTokens: 512,
      // If supported, encourages JSON-only output.
      responseMimeType: "application/json",
    },
  };

  const res = await fetch(endpoint, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const t = await res.text();
    throw new Error(`Vertex generateContent failed: ${res.status} ${t}`);
  }

  const data = await res.json();
  const cand = data && data.candidates && data.candidates[0];
  const parts = (cand && cand.content && cand.content.parts) || [];
  const text = parts.map((p) => p.text || "").join("\n");

  const jsonText = extractJsonObject(text) || text;
  try {
    return JSON.parse(jsonText);
  } catch {
    // fallback: unknown
    return { action: "unknown" };
  }
}

function normalizeText(text) {
  return String(text || "").replace(/\u3000/g, " ").trim();
}

function extractQuotedText(text) {
  const t = String(text || "");
  const m = t.match(/[「『"“](.+?)[」』"”]/);
  return m ? String(m[1] || "").trim() : "";
}

function parseStatusFromText(text) {
  const t = normalizeText(text);
  if (!t) return "";
  if (/(完了|done|終了|終わり)/i.test(t)) return "done";
  if (/(進行|作業中|着手|doing|in\s*progress)/i.test(t)) return "doing";
  if (/(未着手|未完了|再開|open|todo)/i.test(t)) return "open";
  return "";
}

function toJstDate(d = new Date()) {
  return new Date(d.getTime() + 9 * 60 * 60 * 1000);
}

function formatJst(dateJst) {
  const y = dateJst.getUTCFullYear();
  const m = String(dateJst.getUTCMonth() + 1).padStart(2, "0");
  const d = String(dateJst.getUTCDate()).padStart(2, "0");
  const hh = String(dateJst.getUTCHours()).padStart(2, "0");
  const mm = String(dateJst.getUTCMinutes()).padStart(2, "0");
  return `${y}-${m}-${d} ${hh}:${mm}`;
}

function parseTimeFromText(text) {
  const t = String(text || "");

  let m = t.match(/(\d{1,2})[:：](\d{2})/);
  if (m) return { hour: Number(m[1]), minute: Number(m[2]) };

  m = t.match(/(\d{1,2})\s*時(?:\s*(\d{1,2})\s*分?)?/);
  if (m) return { hour: Number(m[1]), minute: m[2] ? Number(m[2]) : 0 };

  if (/正午/.test(t)) return { hour: 12, minute: 0 };
  if (/今夜/.test(t)) return { hour: 21, minute: 0 };

  return null;
}

function parseDueAtFromText(text, now = new Date()) {
  const t = normalizeText(text);
  if (!t) return "";

  const nowJst = toJstDate(now);
  let y = nowJst.getUTCFullYear();
  let m = null;
  let d = null;

  // YYYY-MM-DD or YYYY/MM/DD
  let m1 = t.match(/(20\d{2})[\/\-](\d{1,2})[\/\-](\d{1,2})/);
  if (m1) {
    y = Number(m1[1]);
    m = Number(m1[2]);
    d = Number(m1[3]);
  }

  // M/D or M月D日 (no year)
  if (m === null) {
    m1 = t.match(/(\d{1,2})[\/\-](\d{1,2})/);
    if (m1) {
      m = Number(m1[1]);
      d = Number(m1[2]);
    } else {
      m1 = t.match(/(\d{1,2})\s*月\s*(\d{1,2})\s*日/);
      if (m1) {
        m = Number(m1[1]);
        d = Number(m1[2]);
      }
    }
  }

  // relative day words
  if (m === null && d === null) {
    if (/明日/.test(t)) {
      const next = new Date(nowJst.getTime() + 24 * 60 * 60 * 1000);
      y = next.getUTCFullYear();
      m = next.getUTCMonth() + 1;
      d = next.getUTCDate();
    } else if (/明後日/.test(t)) {
      const next = new Date(nowJst.getTime() + 2 * 24 * 60 * 60 * 1000);
      y = next.getUTCFullYear();
      m = next.getUTCMonth() + 1;
      d = next.getUTCDate();
    } else if (/今日|本日/.test(t)) {
      y = nowJst.getUTCFullYear();
      m = nowJst.getUTCMonth() + 1;
      d = nowJst.getUTCDate();
    }
  }

  if (m === null || d === null) return "";

  const tm = parseTimeFromText(t) || { hour: 18, minute: 0 };
  const dueJst = new Date(Date.UTC(y, m - 1, d, tm.hour, tm.minute));

  // If year not specified and date is in the past, roll to next year.
  if (!/(20\d{2})/.test(t)) {
    const nowJstDateOnly = new Date(Date.UTC(nowJst.getUTCFullYear(), nowJst.getUTCMonth(), nowJst.getUTCDate()));
    const dueDateOnly = new Date(Date.UTC(y, m - 1, d));
    if (dueDateOnly.getTime() < nowJstDateOnly.getTime()) {
      dueJst.setUTCFullYear(y + 1);
    }
  }

  return formatJst(dueJst);
}

function regexQuickParse(text) {
  const t = normalizeText(text);
  const quoted = extractQuotedText(t);

  // list
  if (/タスク一覧|list\s*tasks/i.test(t)) return { action: "list_tasks" };
  if (/プロジェクト一覧|list\s*projects/i.test(t)) return { action: "list_projects" };

  // project complete/reopen/delete (status-based)
  if (/プロジェクト|project/i.test(t) && /(完了|終わった|終わりました|済んだ|done)/i.test(t)) {
    const title = quoted || t
      .replace(/(おーい|ボット|@?KAI\s*bot)/gi, " ")
      .replace(/(プロジェクト|project)/gi, " ")
      .replace(/(完了|終わった|終わりました|済んだ|done)/gi, " ")
      .replace(/(を|は|が|の|です|だ|よ|ね)$/g, " ")
      .trim();
    if (title) return { action: "update_project", query: title, status: "done" };
  }
  if (/プロジェクト|project/i.test(t) && /(再開|未完了|戻す|reopen)/i.test(t)) {
    const title = quoted || t
      .replace(/(おーい|ボット|@?KAI\s*bot)/gi, " ")
      .replace(/(プロジェクト|project)/gi, " ")
      .replace(/(再開|未完了|戻す|reopen)/gi, " ")
      .replace(/(を|は|が|の|です|だ|よ|ね)$/g, " ")
      .trim();
    if (title) return { action: "update_project", query: title, status: "open" };
  }
  if (/プロジェクト|project/i.test(t) && /(削除|消して|消す|取り消し|delete)/i.test(t)) {
    const title = quoted || t
      .replace(/(おーい|ボット|@?KAI\s*bot)/gi, " ")
      .replace(/(プロジェクト|project)/gi, " ")
      .replace(/(削除|消して|消す|取り消し|delete)/gi, " ")
      .replace(/(を|は|が|の|です|だ|よ|ね)$/g, " ")
      .trim();
    if (title) return { action: "delete_project", query: title };
  }

  // complete/reopen
  const mId = t.match(/(tsk_[0-9a-z]+_[0-9a-z]+)/i);
  if (/(タスク完了|完了|終わった|終わりました|済んだ|done)/i.test(t)) {
    if (mId) return { action: "complete_task", task_id: mId[1] };
    const title = quoted || t
      .replace(/(おーい|ボット|@?KAI\s*bot)/gi, " ")
      .replace(/(タスク|task)/gi, " ")
      .replace(/(完了|終わった|終わりました|済んだ|done)/gi, " ")
      .replace(/(を|は|が|の|です|だ|よ|ね)$/g, " ")
      .trim();
    if (title) return { action: "complete_task", query: title };
  }

  if (/(タスク再開|再開|未完了|戻す|reopen)/i.test(t)) {
    if (mId) return { action: "reopen_task", task_id: mId[1] };
    const title = quoted || t
      .replace(/(おーい|ボット|@?KAI\s*bot)/gi, " ")
      .replace(/(タスク|task)/gi, " ")
      .replace(/(再開|未完了|戻す|reopen)/gi, " ")
      .replace(/(を|は|が|の|です|だ|よ|ね)$/g, " ")
      .trim();
    if (title) return { action: "reopen_task", query: title };
  }

  // delete
  if (/(削除|消して|消す|取り消し|delete)/i.test(t)) {
    if (mId) return { action: "delete_task", task_id: mId[1] };
    const title = quoted || t
      .replace(/(おーい|ボット|@?KAI\s*bot)/gi, " ")
      .replace(/(タスク|task)/gi, " ")
      .replace(/(削除|消して|消す|取り消し|delete)/gi, " ")
      .replace(/(を|は|が|の|です|だ|よ|ね)$/g, " ")
      .trim();
    if (title) return { action: "delete_task", query: title };
  }

  // create task (label)
  const mTitle = t.match(/(?:タスク|task)[:：\s]+([^/\n]+?)(?:\s*(?:\/|$|\n))/i);
  if (mTitle) {
    const mDue = t.match(/(?:期限|due)[:：\s]+([^/\n]+?)(?:\s*(?:\/|$|\n))/i);
    const mStatus = t.match(/(?:status)[:：\s]+([^/\n]+?)(?:\s*(?:\/|$|\n))/i);
    return {
      action: "create_task",
      title: String(mTitle[1] || "").trim(),
      due_at: mDue ? String(mDue[1] || "").trim() : "",
      status: mStatus ? String(mStatus[1] || "").trim() : "",
    };
  }

  // create project (label)
  const pTitle = t.match(/(?:プロジェクト|project)[:：\s]+([^/\n]+?)(?:\s*(?:\/|$|\n))/i);
  if (pTitle) {
    const mDue = t.match(/(?:期限|due)[:：\s]+([^/\n]+?)(?:\s*(?:\/|$|\n))/i);
    const mStatus = t.match(/(?:status)[:：\s]+([^/\n]+?)(?:\s*(?:\/|$|\n))/i);
    return {
      action: "create_project",
      title: String(pTitle[1] || "").trim(),
      due_at: mDue ? String(mDue[1] || "").trim() : "",
      status: mStatus ? String(mStatus[1] || "").trim() : "",
    };
  }

  // update task/project (status or due)
  if (/(編集|更新|変更|修正)/.test(t)) {
    const isProject = /プロジェクト|project/i.test(t);
    const target = quoted || (() => {
      const m = t.match(/(.+?)(?:の)?(?:タスク|プロジェクト)?(?:の)?(?:期限|ステータス|状態)/);
      return m ? String(m[1] || "").trim() : "";
    })();
    const due = parseDueAtFromText(t);
    const status = parseStatusFromText(t);
    if (target || due || status) {
      return {
        action: isProject ? "update_project" : "update_task",
        query: target || "",
        due_at: due,
        status,
      };
    }
  }

  return null;
}

async function parseCommandFromText(text) {
  const stripped = stripTriggerPrefix(text);
  if (!stripped) return { action: "help" };

  // quick regex first (fast + no Vertex cost)
  const quick = regexQuickParse(stripped);
  if (quick) {
    const due = parseDueAtFromText(stripped);
    if (due) quick.due_at = due;
    return quick;
  }

  // Vertex AI (Gemini)
  try {
    const obj = await vertexGenerateJson({ userText: stripped });
    // Normalize keys
    const cmd = {
      action: String(obj.action || "unknown"),
      task_id: String(obj.task_id || ""),
      project_id: String(obj.project_id || ""),
      title: String(obj.title || ""),
      new_title: String(obj.new_title || ""),
      description: String(obj.description || ""),
      due_at: String(obj.due_at || ""),
      status: String(obj.status || ""),
      project_title: String(obj.project_title || ""),
      query: String(obj.query || ""),
    };
    const due = parseDueAtFromText(stripped);
    if (due) cmd.due_at = due;
    return cmd;
  } catch (e) {
    console.error("vertex parse failed", e);
    // last resort: unknown
    return { action: "unknown" };
  }
}

function formatTaskMatches(tasks) {
  return tasks
    .slice(0, 5)
    .map((t, i) => `${i + 1}. ${t.title || "(no title)"} / id: ${t.task_id || ""}`)
    .join("\n");
}

function formatProjectMatches(projects) {
  return projects
    .slice(0, 5)
    .map((p, i) => `${i + 1}. ${p.title || "(no title)"} / id: ${p.project_id || ""}`)
    .join("\n");
}

function buildCreatedSummary(kind, item) {
  const lines = [`${kind}を追加しました: ${item.title}`];
  if (item.project_title) lines.push(`プロジェクト: ${item.project_title}`);
  if (item.due_at) lines.push(`期限: ${item.due_at}`);
  if (item.status) lines.push(`status: ${item.status}`);
  if (item.description) lines.push(`詳細: ${item.description}`);
  return lines.join("\n");
}

// =====================
// Postback data parsing
// =====================
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

// =====================
// Web
// =====================
app.get("/", (req, res) => res.status(200).send("ok"));

app.post("/line/webhook", async (req, res) => {
  if (!verifySignature(req)) {
    console.warn("invalid signature (LINE_CHANNEL_SECRET mismatch or bad request)");
    return res.status(401).send("invalid signature");
  }

  const body = req.body || {};
  const events = Array.isArray(body.events) ? body.events : [];

  // Respond immediately to avoid LINE timeout
  res.status(200).send("ok");

  console.log("webhook_received", {
    destination: body.destination || null,
    eventsCount: events.length,
  });

  for (const event of events) {
    try {
      const src = event.source || {};
      const spaceId = getSpaceId(event);
      const displayName = (await getLineDisplayName(src)) || (src.userId ? `${src.userId.slice(0, 6)}…` : "(unknown)");

      // ---- Postback: ACK + broadcast who clicked + execute ----
      if (event.type === "postback" && event.replyToken) {
        const pb = parsePostbackData(event.postback && event.postback.data);

        // 1) Immediate ACK to clicker (prevents repeated tapping)
        await reply(event.replyToken, [{ type: "text", text: "受け付けました。反映します…" }]);

        // 2) Broadcast (so everyone knows who touched UI)
        if (spaceId) {
          const label = pb.a || "(unknown)";
          await push(spaceId, [{ type: "text", text: `操作者: ${displayName}\n操作: ${label}\n処理中…（連打しないでOK）` }]);
        }

        if (pb.a === "task_new") {
          if (spaceId) {
            await push(spaceId, [
              {
                type: "text",
                text:
                  "タスクを自然文で追加できます。例:\n" +
                  "・議事録作成を明日18時までに追加\n" +
                  "・来週火曜までにレポート提出（status open）\n" +
                  "または: @KAI bot タスク: 議事録作成 / 期限: 2026-01-10 18:00 / status: open",
              },
            ]);
          }
          continue;
        }

        if (pb.a === "task_list") {
          if (!spaceId) continue;
          const tasks = await sheetsGetTasksBySpace(spaceId, 20);
          await push(spaceId, [{ type: "text", text: formatTaskList(tasks) }]);
          continue;
        }

        if (pb.a === "project_new") {
          if (spaceId) {
            await push(spaceId, [
              {
                type: "text",
                text:
                  "プロジェクトを自然文で追加できます。例:\n" +
                  "・プロジェクト『卒論』を追加\n" +
                  "・Hikari開発をプロジェクト追加（期限: 2026-03-01）\n" +
                  "または: @KAI bot プロジェクト: 卒論 / 期限: 2026-03-01 / status: open",
              },
            ]);
          }
          continue;
        }

        if (pb.a === "project_list") {
          if (!spaceId) continue;
          const projects = await sheetsGetProjectsBySpace(spaceId, 50);
          await push(spaceId, [{ type: "text", text: formatProjectList(projects) }]);
          continue;
        }

        if (pb.a === "settings") {
          if (spaceId) await push(spaceId, [{ type: "text", text: "設定UIは次で実装します（notify/quota/powerusers）。" }]);
          continue;
        }

        // default
        if (spaceId) await push(spaceId, [buildMenuFlex()]);
        continue;
      }

      // ---- Message text: trigger -> command parse ----
      if (event.type === "message" && event.message && event.message.type === "text" && event.replyToken) {
        const rawText = String(event.message.text || "");
        const triggered = isTriggeredText(rawText);
        console.log("trigger_check", { triggered, textPreview: rawText.slice(0, 200) });

        if (!triggered) continue;

        // If user only called bot, show menu.
        const stripped = stripTriggerPrefix(rawText);
        if (!stripped) {
          await reply(event.replyToken, [buildMenuFlex()]);
          continue;
        }

        if (!spaceId) {
          await reply(event.replyToken, [{ type: "text", text: "スペースIDが取得できませんでした（source）。" }]);
          continue;
        }

        // Immediate ack to reduce uncertainty
        await reply(event.replyToken, [{ type: "text", text: "解釈中…（少し待ってね）" }]);

        // Parse command (regex fast path + Vertex AI fallback)
        const cmd = await parseCommandFromText(rawText);
        console.log("parsed_command", cmd);

        // Execute (push results)
        const createdBy = src.userId || "";

        if (cmd.action === "help") {
          await push(spaceId, [buildMenuFlex()]);
          continue;
        }

        if (cmd.action === "list_tasks") {
          const tasks = await sheetsGetTasksBySpace(spaceId, 20);
          await push(spaceId, [{ type: "text", text: formatTaskList(tasks) }]);
          continue;
        }

        if (cmd.action === "list_projects") {
          const projects = await sheetsGetProjectsBySpace(spaceId, 50);
          await push(spaceId, [{ type: "text", text: formatProjectList(projects) }]);
          continue;
        }

        if (cmd.action === "complete_task") {
          const q = cmd.task_id || cmd.query || cmd.title;
          if (!q) {
            await push(spaceId, [{ type: "text", text: "完了にするタスクが見つかりません。例: @KAI bot 議事録のタスク終わったよ / タスク完了 tsk_xxx" }]);
            continue;
          }
          const matches = await findTasksByQuery(spaceId, q, 200);
          if (!matches.length) {
            await push(spaceId, [{ type: "text", text: "一致するタスクが見つかりませんでした。" }]);
            continue;
          }
          if (matches.length > 1) {
            await push(spaceId, [
              { type: "text", text: `複数見つかりました。idで指定してください:\n${formatTaskMatches(matches)}` },
            ]);
            continue;
          }
          await sheetsUpdateTask(matches[0].task_id, { status: "done", done_at: new Date().toISOString() });
          await push(spaceId, [{ type: "text", text: `タスクを完了にしました: ${matches[0].title}` }]);
          continue;
        }

        if (cmd.action === "reopen_task") {
          const q = cmd.task_id || cmd.query || cmd.title;
          if (!q) {
            await push(spaceId, [{ type: "text", text: "再開するタスクが見つかりません。例: @KAI bot 議事録のタスク再開 / タスク再開 tsk_xxx" }]);
            continue;
          }
          const matches = await findTasksByQuery(spaceId, q, 200);
          if (!matches.length) {
            await push(spaceId, [{ type: "text", text: "一致するタスクが見つかりませんでした。" }]);
            continue;
          }
          if (matches.length > 1) {
            await push(spaceId, [
              { type: "text", text: `複数見つかりました。idで指定してください:\n${formatTaskMatches(matches)}` },
            ]);
            continue;
          }
          await sheetsUpdateTask(matches[0].task_id, { status: "open", done_at: "" });
          await push(spaceId, [{ type: "text", text: `タスクを再開にしました: ${matches[0].title}` }]);
          continue;
        }

        if (cmd.action === "delete_task") {
          const q = cmd.task_id || cmd.query || cmd.title;
          if (!q) {
            await push(spaceId, [{ type: "text", text: "削除するタスクが見つかりません。例: @KAI bot 議事録のタスク削除 / タスク削除 tsk_xxx" }]);
            continue;
          }
          const matches = await findTasksByQuery(spaceId, q, 200);
          if (!matches.length) {
            await push(spaceId, [{ type: "text", text: "一致するタスクが見つかりませんでした。" }]);
            continue;
          }
          if (matches.length > 1) {
            await push(spaceId, [
              { type: "text", text: `複数見つかりました。idで指定してください:\n${formatTaskMatches(matches)}` },
            ]);
            continue;
          }
          await push(spaceId, [{ type: "text", text: "削除中…" }]);
          await sheetsUpdateTask(matches[0].task_id, { status: "deleted", deleted_at: new Date().toISOString() });
          await push(spaceId, [{ type: "text", text: `タスクを削除扱いにしました: ${matches[0].title}` }]);
          continue;
        }

        if (cmd.action === "update_task") {
          const patch = {};
          if (cmd.new_title) patch.title = cmd.new_title;
          if (cmd.description) patch.description = cmd.description;
          if (cmd.status) patch.status = cmd.status;
          if (cmd.due_at) patch.due_at = cmd.due_at;
          if (cmd.project_id) patch.project_id = cmd.project_id;

          const q = cmd.task_id || cmd.query || cmd.title;
          if (!q) {
            await push(spaceId, [{ type: "text", text: "編集するタスクが見つかりません。例: @KAI bot 議事録の期限を明日18時に変更" }]);
            continue;
          }
          if (Object.keys(patch).length === 0) {
            await push(spaceId, [{ type: "text", text: "更新内容が見つかりませんでした（期限/ステータス/内容/タイトル）。" }]);
            continue;
          }
          const matches = await findTasksByQuery(spaceId, q, 200);
          if (!matches.length) {
            await push(spaceId, [{ type: "text", text: "一致するタスクが見つかりませんでした。" }]);
            continue;
          }
          if (matches.length > 1) {
            await push(spaceId, [
              { type: "text", text: `複数見つかりました。idで指定してください:\n${formatTaskMatches(matches)}` },
            ]);
            continue;
          }
          await push(spaceId, [{ type: "text", text: "更新中…" }]);
          await sheetsUpdateTask(matches[0].task_id, patch);
          await push(spaceId, [{ type: "text", text: `タスクを更新しました: ${matches[0].title}` }]);
          continue;
        }

        if (cmd.action === "create_project") {
          const title = (cmd.title || cmd.project_title || "").trim();
          if (!title) {
            await push(spaceId, [{ type: "text", text: "プロジェクト名が分かりません。例: @KAI bot プロジェクト『卒論』を追加" }]);
            continue;
          }
          await push(spaceId, [{ type: "text", text: "追加中…" }]);
          const pid = await sheetsAppendProject({
            spaceId,
            title,
            description: cmd.description || "",
            status: cmd.status || "open",
            due_at: cmd.due_at || "",
            created_by: createdBy,
          });
          await push(spaceId, [
            { type: "text", text: buildCreatedSummary("プロジェクト", { title, description: cmd.description || "", status: cmd.status || "open", due_at: cmd.due_at || "" }) },
          ]);
          continue;
        }

        if (cmd.action === "update_project") {
          const patch = {};
          if (cmd.new_title) patch.title = cmd.new_title;
          if (cmd.description) patch.description = cmd.description;
          if (cmd.status) patch.status = cmd.status;
          if (cmd.due_at) patch.due_at = cmd.due_at;

          const q = cmd.project_id || cmd.query || cmd.project_title || cmd.title;
          if (!q) {
            await push(spaceId, [{ type: "text", text: "編集するプロジェクトが見つかりません。例: @KAI bot 卒論プロジェクトの期限を3/1に変更" }]);
            continue;
          }
          if (Object.keys(patch).length === 0) {
            await push(spaceId, [{ type: "text", text: "更新内容が見つかりませんでした（期限/ステータス/内容/タイトル）。" }]);
            continue;
          }
          const matches = await findProjectsByQuery(spaceId, q, 200);
          if (!matches.length) {
            await push(spaceId, [{ type: "text", text: "一致するプロジェクトが見つかりませんでした。" }]);
            continue;
          }
          if (matches.length > 1) {
            await push(spaceId, [
              { type: "text", text: `複数見つかりました。idで指定してください:\n${formatProjectMatches(matches)}` },
            ]);
            continue;
          }
          await push(spaceId, [{ type: "text", text: "更新中…" }]);
          await sheetsUpdateProject(matches[0].project_id, patch);
          await push(spaceId, [{ type: "text", text: `プロジェクトを更新しました: ${matches[0].title}` }]);
          continue;
        }

        if (cmd.action === "delete_project") {
          const q = cmd.project_id || cmd.query || cmd.project_title || cmd.title;
          if (!q) {
            await push(spaceId, [{ type: "text", text: "削除するプロジェクトが見つかりません。例: @KAI bot 卒論プロジェクト削除" }]);
            continue;
          }
          const matches = await findProjectsByQuery(spaceId, q, 200);
          if (!matches.length) {
            await push(spaceId, [{ type: "text", text: "一致するプロジェクトが見つかりませんでした。" }]);
            continue;
          }
          if (matches.length > 1) {
            await push(spaceId, [
              { type: "text", text: `複数見つかりました。idで指定してください:\n${formatProjectMatches(matches)}` },
            ]);
            continue;
          }
          await push(spaceId, [{ type: "text", text: "削除中…" }]);
          await sheetsUpdateProject(matches[0].project_id, { status: "deleted", deleted_at: new Date().toISOString() });
          await push(spaceId, [{ type: "text", text: `プロジェクトを削除扱いにしました: ${matches[0].title}` }]);
          continue;
        }

        if (cmd.action === "create_task") {
          const title = (cmd.title || "").trim();
          if (!title) {
            await push(spaceId, [{ type: "text", text: "タスク名が分かりません。例: @KAI bot 議事録作成を明日18時までに追加" }]);
            continue;
          }
          await push(spaceId, [{ type: "text", text: "追加中…" }]);
          const tid = await sheetsAppendTask({
            spaceId,
            project_id: cmd.project_id || "",
            title,
            description: cmd.description || "",
            status: cmd.status || "open",
            due_at: cmd.due_at || "",
            created_by: createdBy,
          });
          await push(spaceId, [
            { type: "text", text: buildCreatedSummary("タスク", { title, description: cmd.description || "", status: cmd.status || "open", due_at: cmd.due_at || "", project_title: cmd.project_title || "" }) },
          ]);
          continue;
        }

        // Unknown -> show menu + hint
        await push(spaceId, [
          { type: "text", text: "解釈できませんでした。例: ‘議事録作成を明日18時までに追加’ / ‘タスク一覧’ / ‘タスク完了 tsk_xxx’" },
          buildMenuFlex(),
        ]);
        continue;
      }

    } catch (e) {
      console.error("Event handling error:", e);
      // Best-effort: do not throw
      try {
        if (event && event.replyToken) {
          await reply(event.replyToken, [{ type: "text", text: `エラー: ${String(e && e.message ? e.message : e)}` }]);
        }
      } catch (_) {
        // ignore
      }
    }
  }
});

const port = process.env.PORT || 8080;
app.listen(port, () => console.log(`Listening on ${port}`));
