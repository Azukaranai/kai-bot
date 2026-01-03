
require("dotenv").config();

const express = require("express");
const crypto = require("crypto");
const fetch = require("node-fetch");
const { google } = require("googleapis");
const nacl = require("tweetnacl");

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

// Discord
const DISCORD_PUBLIC_KEY = process.env.DISCORD_PUBLIC_KEY;
const DISCORD_APP_ID = process.env.DISCORD_APP_ID;

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
// Discord helpers
// =====================
function verifyDiscordSignature(req) {
  const signature = req.get("x-signature-ed25519");
  const timestamp = req.get("x-signature-timestamp");
  if (!signature || !timestamp || !DISCORD_PUBLIC_KEY || !req.rawBody) return false;

  const message = Buffer.concat([Buffer.from(timestamp), Buffer.from(req.rawBody)]);
  const sig = Buffer.from(signature, "hex");
  const pub = Buffer.from(DISCORD_PUBLIC_KEY, "hex");
  return nacl.sign.detached.verify(message, sig, pub);
}

async function discordFollowup(appId, token, content) {
  if (!appId || !token) return;
  await fetch(`https://discord.com/api/v10/webhooks/${appId}/${token}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ content }),
  });
}

function getDiscordUserId(interaction) {
  const u = (interaction.member && interaction.member.user) || interaction.user || {};
  return u.id || "";
}

function getDiscordSpaceId(interaction) {
  return interaction.guild_id || interaction.channel_id || getDiscordUserId(interaction) || null;
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
// Pending actions (follow-up prompts)
// =====================
const _pendingBySpace = new Map();
function getPending(spaceId, userId) {
  if (!spaceId) return null;
  const p = _pendingBySpace.get(spaceId);
  if (!p) return null;
  if (Date.now() > p.expiresAt) {
    _pendingBySpace.delete(spaceId);
    return null;
  }
  if (p.userId && userId && p.userId !== userId) return null;
  return p;
}

function setPending(spaceId, userId, pending, ttlMs = 5 * 60 * 1000) {
  if (!spaceId) return;
  _pendingBySpace.set(spaceId, { ...pending, userId: userId || "", expiresAt: Date.now() + ttlMs });
}

function clearPending(spaceId, userId) {
  if (!spaceId) return;
  const p = _pendingBySpace.get(spaceId);
  if (!p) return;
  if (p.userId && userId && p.userId !== userId) return;
  _pendingBySpace.delete(spaceId);
}

async function handlePendingText({ spaceId, userId, text, send }) {
  if (!spaceId || !userId) return false;
  const pending = getPending(spaceId, userId);
  if (!pending) return false;

  const followText = normalizeText(text);
  if (/^(キャンセル|やめる|中止)$/i.test(followText)) {
    clearPending(spaceId, userId);
    await send("キャンセルしました。");
    return true;
  }

  if (pending.action === "create_task") {
    const title = followText;
    if (!title) {
      await send("タスク名が分かりません。もう一度教えてください。");
      return true;
    }
    await send("追加中…");
    await sheetsAppendTask({
      spaceId,
      project_id: "",
      title,
      description: "",
      status: "open",
      due_at: "",
      created_by: userId,
    });
    await send(buildCreatedSummary("タスク", { title, status: "open" }));
    await send("未設定: 期限 / 詳細 / プロジェクト");
    clearPending(spaceId, userId);
    return true;
  }

  if (pending.action === "create_project") {
    const title = followText;
    if (!title) {
      await send("プロジェクト名が分かりません。もう一度教えてください。");
      return true;
    }
    await send("追加中…");
    await sheetsAppendProject({
      spaceId,
      title,
      description: "",
      status: "open",
      due_at: "",
      created_by: userId,
    });
    await send(buildCreatedSummary("プロジェクト", { title, status: "open" }));
    await send("未設定: 期限 / 詳細");
    clearPending(spaceId, userId);
    return true;
  }

  if (pending.action === "delete_task") {
    const matches = await findTasksByQuery(spaceId, sanitizeQuery(followText), 200);
    if (!matches.length) {
      await send("一致するタスクが見つかりませんでした。もう一度教えてください。");
      return true;
    }
    if (matches.length > 1) {
      await send(`複数見つかりました。より具体的に教えてください:\n${formatTaskMatches(matches)}`);
      return true;
    }
    await send("削除中…");
    await sheetsUpdateTask(matches[0].task_id, { status: "deleted", deleted_at: new Date().toISOString() });
    await send(`タスクを削除しました: ${matches[0].title}`);
    clearPending(spaceId, userId);
    return true;
  }

  if (pending.action === "delete_project") {
    const matches = await findProjectsByQuery(spaceId, sanitizeQuery(followText), 200);
    if (!matches.length) {
      await send("一致するプロジェクトが見つかりませんでした。もう一度教えてください。");
      return true;
    }
    if (matches.length > 1) {
      await send(`複数見つかりました。より具体的に教えてください:\n${formatProjectMatches(matches)}`);
      return true;
    }
    await send("削除中…");
    await sheetsUpdateProject(matches[0].project_id, { status: "deleted", deleted_at: new Date().toISOString() });
    await send(`プロジェクトを削除しました: ${matches[0].title}`);
    clearPending(spaceId, userId);
    return true;
  }

  return false;
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

async function sheetsGetTasksBySpace(spaceId, limit = 20, { includeDeleted = false } = {}) {
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
    const row = {
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
    };
    if (!includeDeleted && String(row.status || "").toLowerCase() === "deleted") continue;
    out.push(row);
    if (out.length >= limit) break;
  }
  return out;
}

function formatStatusJa(status) {
  const s = String(status || "").toLowerCase();
  if (s === "open") return "未着手";
  if (s === "doing") return "進行中";
  if (s === "done") return "完了";
  if (s === "deleted") return "削除";
  return status || "未設定";
}

function formatTaskList(tasks) {
  if (!tasks.length) return "このスペースのタスクはまだありません。";
  const lines = tasks.map((t, i) => {
    const due = t.due_at ? t.due_at : "未設定";
    const st = formatStatusJa(t.status);
    const parts = [`${i + 1}. ${t.title}`, `期限: ${due}`, `状態: ${st}`];
    return parts.join("\n");
  });
  return lines.join("\n\n");
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

async function sheetsGetProjectsBySpace(spaceId, limit = 50, { includeDeleted = false } = {}) {
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
    const row = {
      project_id: r[idx.project_id] || "",
      title: r[idx.title] || "",
      description: idx.description !== undefined ? r[idx.description] || "" : "",
      status: idx.status !== undefined ? r[idx.status] || "" : "",
      due_at: idx.due_at !== undefined ? r[idx.due_at] || "" : "",
      created_at: idx.created_at !== undefined ? r[idx.created_at] || "" : "",
    };
    if (!includeDeleted && String(row.status || "").toLowerCase() === "deleted") continue;
    out.push(row);
    if (out.length >= limit) break;
  }
  return out;
}

function formatProjectList(projects) {
  if (!projects.length) return "このスペースのプロジェクトはまだありません。";
  return projects
    .map((p, i) => {
      const st = formatStatusJa(p.status);
      const due = p.due_at ? p.due_at : "未設定";
      const parts = [`${i + 1}. ${p.title}`, `期限: ${due}`, `状態: ${st}`];
      return parts.join("\n");
    })
    .join("\n\n");
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
    `次のいずれかの action を返してください: create_task, update_task, delete_task, complete_task, reopen_task, list_tasks, create_project, update_project, delete_project, list_projects, help, ask_user, unknown\n` +
    `出力JSONスキーマ（省略可のキーは空文字でも可）:\n` +
    `{\n  "action":"...",\n  "next_action":"",\n  "target_type":"task|project|none",\n  "question":"",\n  "task_id":"",\n  "project_id":"",\n  "title":"",\n  "new_title":"",\n  "description":"",\n  "due_at":"",\n  "status":"",\n  "project_title":"",\n  "query":""\n}\n` +
    `注意: 対象が曖昧な場合は action=ask_user にして question と next_action を返す。\n` +
    `ID が文中に無い場合は空文字にする。対象がID不明の場合は query にタイトル断片を入れる。期限は文にある場合だけ入れる（例: 2026-01-10 18:00）。\n` +
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

function extractQueryFromText(text, removePatterns = []) {
  let t = normalizeText(text);
  if (!t) return "";
  for (const re of removePatterns) t = t.replace(re, " ");
  t = t.replace(/\s+/g, " ").trim();
  if (!t || t.length < 2) return "";
  return t;
}

function splitQueries(text) {
  return String(text || "")
    .split(/[、,，\n]/)
    .map((s) => normalizeText(s))
    .filter(Boolean);
}

function sanitizeQuery(text) {
  let t = normalizeText(text);
  if (!t) return "";
  t = t.replace(/^[「『"“](.+)[」』"”]$/, "$1");
  t = t.replace(/(を|は|が|の|です|だ|よ|ね)\s*$/g, "");
  return t.trim();
}

function inferIntentFromText(text) {
  const t = normalizeText(text);
  const hasTask = /タスク|task/i.test(t);
  const hasProject = /プロジェクト|project/i.test(t);

  let action = "";
  if (/(削除|消して|消す|取り消し|delete)/i.test(t)) action = "delete";
  else if (/(完了|終わった|終わりました|済んだ|done)/i.test(t)) action = "complete";
  else if (/(再開|未完了|戻す|reopen)/i.test(t)) action = "reopen";
  else if (/(変更|更新|修正|編集)/.test(t)) action = "update";
  else if (/(追加|作成|登録|つくる|作る)/.test(t)) action = "create";
  else if (/(一覧|見せて|リスト|list)/i.test(t)) action = "list";

  const targetType = hasProject ? "project" : hasTask ? "task" : "";
  const query = extractQueryFromText(t, [
    /(おーい|ボット|@?KAI\s*bot)/gi,
    /(タスク|task|プロジェクト|project)/gi,
    /(削除|消して|消す|取り消し|delete|完了|終わった|終わりました|済んだ|done|再開|未完了|戻す|reopen|変更|更新|修正|編集|追加|作成|登録|つくる|作る|一覧|見せて|リスト|list)/gi,
  ]);

  const missingTarget = ["delete", "complete", "reopen", "update"].includes(action) && !query;
  return { action, targetType, query, missingTarget };
}

function buildUnknownResponse(text, intent) {
  const lines = ["申し訳ありません。内容を十分に理解できませんでした。"];
  const understood = [];
  if (intent.action) understood.push(`意図: ${intent.action}`);
  if (intent.targetType) understood.push(`対象: ${intent.targetType}`);
  if (intent.query) understood.push(`名前候補: ${intent.query}`);
  if (understood.length) lines.push(`分かったこと: ${understood.join(" / ")}`);

  const missing = [];
  if (!intent.action) missing.push("やりたいこと（追加/完了/削除など）");
  if (intent.action && !intent.targetType) missing.push("対象（タスク or プロジェクト）");
  if (intent.missingTarget) missing.push("対象の名前");
  if (missing.length) lines.push(`足りないこと: ${missing.join(" / ")}`);

  lines.push("例: タスク追加 企画書作成 / タスク完了 議事録 / プロジェクト追加 卒論");
  return lines.join("\n");
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

function parseProjectTitleFromText(text) {
  const t = normalizeText(text);
  if (!t) return "";
  const m =
    t.match(/プロジェクト[「『"“]?(.+?)[」』"”]?(?:の|に|で|を|$)/) ||
    t.match(/([^\s]+?)プロジェクト/);
  if (!m) return "";
  return String(m[1] || "").trim();
}

// =====================
// Templates (learned phrases)
// =====================
let _templateCache = { data: null, expMs: 0 };

async function loadTemplates() {
  const now = Date.now();
  if (_templateCache.data && now < _templateCache.expMs) return _templateCache.data;

  try {
    const values = await sheetsGetValues("Templates!A:Z");
    if (!values.length) return [];
    const header = values[0].map((v) => String(v || "").trim());
    const idx = headerIndex(header);
    if (idx.text === undefined || idx.action === undefined) return [];

    const rows = values.slice(1);
    const data = rows
      .map((r) => ({
        text: String(r[idx.text] || "").trim(),
        action: String(r[idx.action] || "").trim(),
        target_type: idx.target_type !== undefined ? String(r[idx.target_type] || "").trim() : "",
        query: idx.query !== undefined ? String(r[idx.query] || "").trim() : "",
        project_title: idx.project_title !== undefined ? String(r[idx.project_title] || "").trim() : "",
        status: idx.status !== undefined ? String(r[idx.status] || "").trim() : "",
        due_at: idx.due_at !== undefined ? String(r[idx.due_at] || "").trim() : "",
      }))
      .filter((r) => r.text && r.action);

    _templateCache = { data, expMs: now + 60 * 1000 };
    return data;
  } catch {
    return [];
  }
}

async function matchTemplate(text) {
  const key = normalizeText(text).toLowerCase();
  if (!key) return null;
  const templates = await loadTemplates();
  const hit = templates.find((t) => t.text.toLowerCase() === key);
  if (!hit) return null;
  return {
    action: hit.action,
    target_type: hit.target_type || "",
    query: hit.query || "",
    project_title: hit.project_title || "",
    status: hit.status || "",
    due_at: hit.due_at || "",
  };
}

async function recordTemplate(text, cmd) {
  const key = normalizeText(text);
  if (!key || !cmd || !cmd.action) return;
  const templates = await loadTemplates();
  if (templates.some((t) => t.text.toLowerCase() === key.toLowerCase())) return;

  try {
    await sheetsAppendRow("Templates", [
      key,
      cmd.action || "",
      cmd.target_type || "",
      cmd.query || cmd.title || "",
      cmd.project_title || "",
      cmd.status || "",
      cmd.due_at || "",
      new Date().toISOString(),
    ]);
    _templateCache.expMs = 0;
  } catch {
    // If Templates sheet doesn't exist, ignore.
  }
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
    const title = quoted || extractQueryFromText(t, [
      /(おーい|ボット|@?KAI\s*bot)/gi,
      /(タスク|task)/gi,
      /(完了|終わった|終わりました|済んだ|done)/gi,
      /(を|は|が|の|です|だ|よ|ね)$/g,
    ]);
    if (title) return { action: "complete_task", query: title };
    return { action: "complete_task", query: "" };
  }

  if (/(タスク再開|再開|未完了|戻す|reopen)/i.test(t)) {
    if (mId) return { action: "reopen_task", task_id: mId[1] };
    const title = quoted || extractQueryFromText(t, [
      /(おーい|ボット|@?KAI\s*bot)/gi,
      /(タスク|task)/gi,
      /(再開|未完了|戻す|reopen)/gi,
      /(を|は|が|の|です|だ|よ|ね)$/g,
    ]);
    if (title) return { action: "reopen_task", query: title };
    return { action: "reopen_task", query: "" };
  }

  // delete
  if (/(削除|消して|消す|取り消し|delete)/i.test(t)) {
    if (mId) return { action: "delete_task", task_id: mId[1] };
    const title = quoted || extractQueryFromText(t, [
      /(おーい|ボット|@?KAI\s*bot)/gi,
      /(タスク|task)/gi,
      /(削除|消して|消す|取り消し|delete)/gi,
      /(を|は|が|の|です|だ|よ|ね)$/g,
    ]);
    if (title) return { action: "delete_task", query: title };
    return { action: "delete_task", query: "" };
  }

  // create task (label)
  const mTitle = t.match(/(?:タスク|task)[:：\s]+([^/\n]+?)(?:\s*(?:\/|$|\n))/i);
  if (mTitle) {
    const mDue = t.match(/(?:期限|due)[:：\s]+([^/\n]+?)(?:\s*(?:\/|$|\n))/i);
    const mStatus = t.match(/(?:status)[:：\s]+([^/\n]+?)(?:\s*(?:\/|$|\n))/i);
    let projectTitle = "";
    if (/プロジェクト|project/i.test(t)) {
      projectTitle =
        quoted ||
        (t.match(/プロジェクト\s*([^\sの/]+?)(?:の|\s|$)/) || [])[1] ||
        (t.match(/([^\s]+?)プロジェクト/) || [])[1] ||
        "";
      projectTitle = String(projectTitle || "").trim();
    }
    return {
      action: "create_task",
      title: String(mTitle[1] || "").trim(),
      due_at: mDue ? String(mDue[1] || "").trim() : "",
      status: mStatus ? String(mStatus[1] || "").trim() : "",
      project_title: projectTitle,
    };
  }

  // create task (natural + project relation)
  const mProjTask = t.match(/プロジェクト[「『"“]?(.+?)[」』"”]?\s*(?:の|に)\s*(.+?)\s*(?:を)?\s*(追加|作成|登録)/);
  if (mProjTask) {
    return {
      action: "create_task",
      title: String(mProjTask[2] || "").trim(),
      project_title: String(mProjTask[1] || "").trim(),
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

  // create project (natural)
  if (/(プロジェクト|project)/i.test(t) && /(追加|作成|登録|つくる|作る)/.test(t)) {
    const title =
      quoted ||
      (t.match(/([^\s]+?)プロジェクト/) || [])[1] ||
      t
        .replace(/(おーい|ボット|@?KAI\s*bot)/gi, " ")
        .replace(/(プロジェクト|project)/gi, " ")
        .replace(/(追加|作成|登録|つくる|作る)/g, " ")
        .trim();
    if (title) return { action: "create_project", title: String(title).trim() };
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

  // move task to project
  if (/(移動|紐付け|割り当て|関連)/.test(t) && /プロジェクト/.test(t)) {
    const projectTitle = parseProjectTitleFromText(t);
    const target = extractQueryFromText(t, [
      /(おーい|ボット|@?KAI\s*bot)/gi,
      /(タスク|task)/gi,
      /(を)?\s*プロジェクト[「『"“]?.+?[」』"”]?に/gi,
      /(移動|紐付け|割り当て|関連)/g,
    ]);
    if (target || projectTitle) {
      return { action: "update_task", query: target || "", project_title: projectTitle };
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
      next_action: String(obj.next_action || ""),
      target_type: String(obj.target_type || ""),
      question: String(obj.question || ""),
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
    if (!cmd.project_title && /プロジェクト|project/i.test(stripped)) {
      const guessed = extractQuotedText(stripped) || parseProjectTitleFromText(stripped);
      if (guessed) cmd.project_title = guessed;
    }
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

function buildMissingNotes(text, cmd) {
  const t = normalizeText(text);
  const notes = [];

  const mentionedDue = /(期限|締切|までに|まで|due)/i.test(t);
  if (mentionedDue && !cmd.due_at) notes.push("期限が不明だったため未設定です。");

  const mentionedStatus = /(status|ステータス|状態|open|doing|done|完了|再開|未完了)/i.test(t);
  if (mentionedStatus && !cmd.status) notes.push("ステータスが不明だったため未設定です。");

  const mentionedProject = /プロジェクト|project/i.test(t);
  if (mentionedProject && !cmd.project_title && !cmd.project_id) {
    notes.push("プロジェクト指定が不明だったため未紐付けです。");
  }

  return notes;
}

function buildCreatedSummary(kind, item) {
  const lines = [`${kind}を追加しました。`, `名前: ${item.title}`];
  if (item.project_title) lines.push(`プロジェクト: ${item.project_title}`);
  if (item.due_at) lines.push(`期限: ${item.due_at}`);
  if (item.status) lines.push(`状態: ${formatStatusJa(item.status)}`);
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

        const userId = src.userId || "";

        if (!triggered) {
          if (spaceId && userId) {
            const pending = getPending(spaceId, userId);
            if (pending) {
              const followText = normalizeText(rawText);
              if (/^(キャンセル|やめる|中止)$/i.test(followText)) {
                clearPending(spaceId, userId);
                await reply(event.replyToken, [{ type: "text", text: "キャンセルしました。" }]);
                continue;
              }

              const query = followText;
              if (pending.action === "delete_task") {
                const matches = await findTasksByQuery(spaceId, sanitizeQuery(query), 200);
                if (!matches.length) {
                  await reply(event.replyToken, [{ type: "text", text: "一致するタスクが見つかりませんでした。もう一度教えてください。" }]);
                  continue;
                }
                if (matches.length > 1) {
                  await reply(event.replyToken, [
                    { type: "text", text: `複数見つかりました。より具体的に教えてください:\n${formatTaskMatches(matches)}` },
                  ]);
                  continue;
                }
                await push(spaceId, [{ type: "text", text: "削除中…" }]);
                await sheetsUpdateTask(matches[0].task_id, { status: "deleted", deleted_at: new Date().toISOString() });
                await push(spaceId, [{ type: "text", text: `タスクを削除しました: ${matches[0].title}` }]);
                clearPending(spaceId, userId);
                continue;
              }

              if (pending.action === "delete_project") {
                const matches = await findProjectsByQuery(spaceId, sanitizeQuery(query), 200);
                if (!matches.length) {
                  await reply(event.replyToken, [{ type: "text", text: "一致するプロジェクトが見つかりませんでした。もう一度教えてください。" }]);
                  continue;
                }
                if (matches.length > 1) {
                  await reply(event.replyToken, [
                    { type: "text", text: `複数見つかりました。より具体的に教えてください:\n${formatProjectMatches(matches)}` },
                  ]);
                  continue;
                }
                await push(spaceId, [{ type: "text", text: "削除中…" }]);
                await sheetsUpdateProject(matches[0].project_id, { status: "deleted", deleted_at: new Date().toISOString() });
                await push(spaceId, [{ type: "text", text: `プロジェクトを削除扱いにしました: ${matches[0].title}` }]);
                clearPending(spaceId, userId);
                continue;
              }
            }
          }
          continue;
        }

        // If user only called bot, show menu.
        const stripped = stripTriggerPrefix(rawText);
        if (!stripped) {
          await reply(event.replyToken, [buildMenuFlex()]);
          continue;
        }

        const pending = spaceId && userId ? getPending(spaceId, userId) : null;
        if (pending) {
          // If the user replied with just a name, treat as follow-up.
          const hasActionKeyword = /(削除|消して|消す|取り消し|完了|終わった|再開|更新|変更|修正|追加|作成|一覧)/.test(stripped);
          if (!hasActionKeyword) {
            const followText = normalizeText(stripped);
            if (/^(キャンセル|やめる|中止)$/i.test(followText)) {
              clearPending(spaceId, userId);
              await reply(event.replyToken, [{ type: "text", text: "キャンセルしました。" }]);
              continue;
            }
            if (pending.action === "create_task") {
              const title = followText;
              if (!title) {
                await reply(event.replyToken, [{ type: "text", text: "タスク名が分かりません。もう一度教えてください。" }]);
                continue;
              }
              await push(spaceId, [{ type: "text", text: "追加中…" }]);
              const tid = await sheetsAppendTask({
                spaceId,
                project_id: "",
                title,
                description: "",
                status: "open",
                due_at: "",
                created_by: src.userId || "",
              });
              await push(spaceId, [{ type: "text", text: buildCreatedSummary("タスク", { title, status: "open" }) }]);
              await push(spaceId, [{ type: "text", text: "未設定: 期限 / 詳細 / プロジェクト" }]);
              clearPending(spaceId, userId);
              continue;
            }
            if (pending.action === "create_project") {
              const title = followText;
              if (!title) {
                await reply(event.replyToken, [{ type: "text", text: "プロジェクト名が分かりません。もう一度教えてください。" }]);
                continue;
              }
              await push(spaceId, [{ type: "text", text: "追加中…" }]);
              const pid = await sheetsAppendProject({
                spaceId,
                title,
                description: "",
                status: "open",
                due_at: "",
                created_by: src.userId || "",
              });
              await push(spaceId, [{ type: "text", text: buildCreatedSummary("プロジェクト", { title, status: "open" }) }]);
              await push(spaceId, [{ type: "text", text: "未設定: 期限 / 詳細" }]);
              clearPending(spaceId, userId);
              continue;
            }
            if (pending.action === "delete_task") {
            const matches = await findTasksByQuery(spaceId, sanitizeQuery(followText), 200);
              if (!matches.length) {
                await reply(event.replyToken, [{ type: "text", text: "一致するタスクが見つかりませんでした。もう一度教えてください。" }]);
                continue;
              }
              if (matches.length > 1) {
                await reply(event.replyToken, [
                  { type: "text", text: `複数見つかりました。より具体的に教えてください:\n${formatTaskMatches(matches)}` },
                ]);
                continue;
              }
              await push(spaceId, [{ type: "text", text: "削除中…" }]);
              await sheetsUpdateTask(matches[0].task_id, { status: "deleted", deleted_at: new Date().toISOString() });
              await push(spaceId, [{ type: "text", text: `タスクを削除しました: ${matches[0].title}` }]);
              clearPending(spaceId, userId);
              continue;
            }
            if (pending.action === "delete_project") {
              const matches = await findProjectsByQuery(spaceId, sanitizeQuery(followText), 200);
              if (!matches.length) {
                await reply(event.replyToken, [{ type: "text", text: "一致するプロジェクトが見つかりませんでした。もう一度教えてください。" }]);
                continue;
              }
              if (matches.length > 1) {
                await reply(event.replyToken, [
                  { type: "text", text: `複数見つかりました。より具体的に教えてください:\n${formatProjectMatches(matches)}` },
                ]);
                continue;
              }
              await push(spaceId, [{ type: "text", text: "削除中…" }]);
              await sheetsUpdateProject(matches[0].project_id, { status: "deleted", deleted_at: new Date().toISOString() });
              await push(spaceId, [{ type: "text", text: `プロジェクトを削除扱いにしました: ${matches[0].title}` }]);
              clearPending(spaceId, userId);
              continue;
            }
          }
        }

        if (spaceId && userId) clearPending(spaceId, userId);

        if (!spaceId) {
          await reply(event.replyToken, [{ type: "text", text: "スペースIDが取得できませんでした（source）。" }]);
          continue;
        }

        // Show processing indicator
        await push(spaceId, [{ type: "text", text: "解釈中…" }]);

        // Fast path: templates/regex without LLM
        const templ = await matchTemplate(stripped);
        const fast = templ || regexQuickParse(stripped);
        let cmd;
        if (fast) {
          const due = parseDueAtFromText(stripped);
          if (due) fast.due_at = due;
          cmd = fast;
        } else {
          cmd = await parseCommandFromText(rawText);
        }
        console.log("parsed_command", cmd);
        await recordTemplate(stripped, cmd);

        // Execute (push results)
        const createdBy = src.userId || "";

        if (cmd.action === "ask_user") {
          const question = cmd.question || "対象を教えてください。";
          await push(spaceId, [{ type: "text", text: question }]);
          const pendingAction = cmd.next_action || (cmd.target_type === "project" ? "update_project" : "update_task");
          setPending(spaceId, userId, { action: pendingAction });
          continue;
        }

        if (cmd.action === "unknown") {
          const intent = inferIntentFromText(stripped);
          if (intent.action === "delete" && intent.targetType && intent.missingTarget) {
            const q = intent.targetType === "project" ? "どのプロジェクトですか？" : "どのタスクですか？";
            await push(spaceId, [{ type: "text", text: q }]);
            setPending(spaceId, userId, { action: intent.targetType === "project" ? "delete_project" : "delete_task" });
            continue;
          }
          if (intent.action === "create" && intent.targetType) {
            const q = intent.targetType === "project" ? "追加するプロジェクト名を教えてください。" : "追加するタスク名を教えてください。";
            await push(spaceId, [{ type: "text", text: q }]);
            setPending(spaceId, userId, { action: intent.targetType === "project" ? "create_project" : "create_task" });
            continue;
          }
          await push(spaceId, [{ type: "text", text: buildUnknownResponse(stripped, intent) }]);
          continue;
        }

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
            await push(spaceId, [{ type: "text", text: "どのタスクを削除しますか？" }]);
            setPending(spaceId, userId, { action: "delete_task" });
            continue;
          }
          const items = splitQueries(q).map(sanitizeQuery).filter(Boolean);
          const deleted = [];
          for (const item of items) {
            const matches = await findTasksByQuery(spaceId, item, 200);
            if (!matches.length) {
              await push(spaceId, [{ type: "text", text: `一致するタスクが見つかりませんでした: ${item}` }]);
              continue;
            }
            if (matches.length > 1) {
              await push(spaceId, [
                { type: "text", text: `複数見つかりました。より具体的に教えてください:\n${formatTaskMatches(matches)}` },
              ]);
              continue;
            }
            await sheetsUpdateTask(matches[0].task_id, { status: "deleted", deleted_at: new Date().toISOString() });
            deleted.push(matches[0].title);
          }
          if (deleted.length) {
            await push(spaceId, [{ type: "text", text: `削除しました: ${deleted.join("、")}` }]);
          }
          continue;
        }

        if (cmd.action === "update_task") {
          const patch = {};
          if (cmd.new_title) patch.title = cmd.new_title;
          if (cmd.description) patch.description = cmd.description;
          if (cmd.status) patch.status = cmd.status;
          if (cmd.due_at) patch.due_at = cmd.due_at;
          if (cmd.project_id) patch.project_id = cmd.project_id;
          if (cmd.project_title && !cmd.project_id) {
            const projMatches = await findProjectsByQuery(spaceId, cmd.project_title, 200);
            if (!projMatches.length) {
              await push(spaceId, [{ type: "text", text: `プロジェクトが見つかりませんでした: ${cmd.project_title}` }]);
              continue;
            }
            if (projMatches.length > 1) {
              await push(spaceId, [
                { type: "text", text: `複数のプロジェクトが見つかりました。より具体的に教えてください:\n${formatProjectMatches(projMatches)}` },
              ]);
              continue;
            }
            patch.project_id = projMatches[0].project_id;
          }

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
            {
              type: "text",
              text: buildCreatedSummary("プロジェクト", { title, description: cmd.description || "", status: cmd.status || "open", due_at: cmd.due_at || "" }),
            },
          ]);
          const notes = buildMissingNotes(stripped, cmd);
          if (notes.length) {
            await push(spaceId, [{ type: "text", text: notes.join("\n") }]);
          }
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
            await push(spaceId, [{ type: "text", text: "どのプロジェクトを削除しますか？" }]);
            setPending(spaceId, userId, { action: "delete_project" });
            continue;
          }
          const items = splitQueries(q).map(sanitizeQuery).filter(Boolean);
          const deleted = [];
          for (const item of items) {
            const matches = await findProjectsByQuery(spaceId, item, 200);
            if (!matches.length) {
              await push(spaceId, [{ type: "text", text: `一致するプロジェクトが見つかりませんでした: ${item}` }]);
              continue;
            }
            if (matches.length > 1) {
              await push(spaceId, [
                { type: "text", text: `複数見つかりました。より具体的に教えてください:\n${formatProjectMatches(matches)}` },
              ]);
              continue;
            }
            await sheetsUpdateProject(matches[0].project_id, { status: "deleted", deleted_at: new Date().toISOString() });
            deleted.push(matches[0].title);
          }
          if (deleted.length) {
            await push(spaceId, [{ type: "text", text: `削除扱いにしました: ${deleted.join("、")}` }]);
          }
          continue;
        }

        if (cmd.action === "create_task") {
          const title = (cmd.title || "").trim();
          if (!title) {
            await push(spaceId, [{ type: "text", text: "タスク名が分かりません。例: @KAI bot 議事録作成を明日18時までに追加" }]);
            continue;
          }
          await push(spaceId, [{ type: "text", text: "追加中…" }]);
          let projectId = cmd.project_id || "";
          let projectTitle = cmd.project_title || "";
          if (projectTitle && !projectId) {
            const matches = await findProjectsByQuery(spaceId, projectTitle, 200);
            if (matches.length > 1) {
              await push(spaceId, [
                { type: "text", text: `複数のプロジェクトが見つかりました。名前をもう少し具体的にしてください:\n${formatProjectMatches(matches)}` },
              ]);
              continue;
            }
            if (matches.length === 1) {
              projectId = matches[0].project_id;
              projectTitle = matches[0].title || projectTitle;
            } else {
              projectId = "";
            }
          }
          const tid = await sheetsAppendTask({
            spaceId,
            project_id: projectId,
            title,
            description: cmd.description || "",
            status: cmd.status || "open",
            due_at: cmd.due_at || "",
            created_by: createdBy,
          });
          await push(spaceId, [
            {
              type: "text",
              text: buildCreatedSummary("タスク", {
                title,
                description: cmd.description || "",
                status: cmd.status || "open",
                due_at: cmd.due_at || "",
                project_title: projectTitle,
              }),
            },
          ]);
          const notes = buildMissingNotes(stripped, { ...cmd, project_title: projectTitle, project_id: projectId });
          if (notes.length) {
            await push(spaceId, [{ type: "text", text: notes.join("\n") }]);
          }
          continue;
        }

        // Unknown -> show menu + hint
        await push(spaceId, [{ type: "text", text: "解釈できませんでした。例: ‘議事録作成を明日18時までに追加’ / ‘タスク一覧’ / ‘タスク完了 tsk_xxx’" }]);
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

// =====================
// Discord Interactions
// =====================
app.post("/discord/interactions", async (req, res) => {
  const sig = req.get("x-signature-ed25519");
  const ts = req.get("x-signature-timestamp");
  console.log("discord_incoming", {
    hasSignature: !!sig,
    hasTimestamp: !!ts,
    hasPublicKey: !!DISCORD_PUBLIC_KEY,
    hasBody: !!req.rawBody,
  });
  if (!verifyDiscordSignature(req)) {
    console.warn("discord_invalid_signature");
    return res.status(401).send("invalid signature");
  }

  const interaction = req.body || {};
  if (interaction.type === 1) {
    return res.json({ type: 1 }); // PING
  }

  const userId = getDiscordUserId(interaction);
  const spaceId = getDiscordSpaceId(interaction);
  if (!userId || !spaceId) {
    return res.json({ type: 4, data: { content: "ユーザー情報が取得できませんでした。" } });
  }

  // Acknowledge immediately to avoid timeouts
  res.json({ type: 5 }); // DEFERRED_CHANNEL_MESSAGE_WITH_SOURCE

  const options = (interaction.data && interaction.data.options) || [];
  const textOpt = options.find((o) => o.name === "text") || options[0];
  let text = textOpt && textOpt.value ? String(textOpt.value) : "";
  if (!text) {
    text = (interaction.data && interaction.data.name) || "";
  }

  const send = async (content) => {
    await discordFollowup(DISCORD_APP_ID, interaction.token, content);
  };

  // Follow-up handling without mention (same user only)
  const pendingHandled = await handlePendingText({ spaceId, userId, text, send });
  if (pendingHandled) return;

  const stripped = normalizeText(text);
  if (!stripped) {
    await send("内容を教えてください。");
    return;
  }

  const templ = await matchTemplate(stripped);
  const fast = templ || regexQuickParse(stripped);
  let cmd;
  if (fast) {
    const due = parseDueAtFromText(stripped);
    if (due) fast.due_at = due;
    cmd = fast;
  } else {
    await send("解釈中…");
    cmd = await parseCommandFromText(text);
  }
  await recordTemplate(stripped, cmd);

  if (cmd.action === "ask_user") {
    const question = cmd.question || "対象を教えてください。";
    await send(question);
    const pendingAction = cmd.next_action || (cmd.target_type === "project" ? "update_project" : "update_task");
    setPending(spaceId, userId, { action: pendingAction });
    return;
  }

  if (cmd.action === "unknown") {
    const intent = inferIntentFromText(stripped);
    if (intent.action === "delete" && intent.targetType && intent.missingTarget) {
      await send(intent.targetType === "project" ? "どのプロジェクトですか？" : "どのタスクですか？");
      setPending(spaceId, userId, { action: intent.targetType === "project" ? "delete_project" : "delete_task" });
      return;
    }
    if (intent.action === "create" && intent.targetType) {
      await send(intent.targetType === "project" ? "追加するプロジェクト名を教えてください。" : "追加するタスク名を教えてください。");
      setPending(spaceId, userId, { action: intent.targetType === "project" ? "create_project" : "create_task" });
      return;
    }
    await send(buildUnknownResponse(stripped, intent));
    return;
  }

  if (cmd.action === "list_tasks") {
    const tasks = await sheetsGetTasksBySpace(spaceId, 20);
    await send(formatTaskList(tasks));
    return;
  }

  if (cmd.action === "list_projects") {
    const projects = await sheetsGetProjectsBySpace(spaceId, 50);
    await send(formatProjectList(projects));
    return;
  }

  if (cmd.action === "complete_task") {
    const q = cmd.task_id || cmd.query || cmd.title;
    if (!q) {
      await send("完了にするタスクが見つかりません。例: 議事録のタスク終わったよ");
      return;
    }
    const matches = await findTasksByQuery(spaceId, sanitizeQuery(q), 200);
    if (!matches.length) {
      await send("一致するタスクが見つかりませんでした。");
      return;
    }
    if (matches.length > 1) {
      await send(`複数見つかりました。より具体的に教えてください:\n${formatTaskMatches(matches)}`);
      return;
    }
    await sheetsUpdateTask(matches[0].task_id, { status: "done", done_at: new Date().toISOString() });
    await send(`タスクを完了にしました: ${matches[0].title}`);
    return;
  }

  if (cmd.action === "reopen_task") {
    const q = cmd.task_id || cmd.query || cmd.title;
    if (!q) {
      await send("再開するタスクが見つかりません。例: 議事録のタスク再開");
      return;
    }
    const matches = await findTasksByQuery(spaceId, sanitizeQuery(q), 200);
    if (!matches.length) {
      await send("一致するタスクが見つかりませんでした。");
      return;
    }
    if (matches.length > 1) {
      await send(`複数見つかりました。より具体的に教えてください:\n${formatTaskMatches(matches)}`);
      return;
    }
    await sheetsUpdateTask(matches[0].task_id, { status: "open", done_at: "" });
    await send(`タスクを再開にしました: ${matches[0].title}`);
    return;
  }

  if (cmd.action === "delete_task") {
    const q = cmd.task_id || cmd.query || cmd.title;
    if (!q) {
      await send("どのタスクを削除しますか？");
      setPending(spaceId, userId, { action: "delete_task" });
      return;
    }
    const items = splitQueries(q).map(sanitizeQuery).filter(Boolean);
    const deleted = [];
    for (const item of items) {
      const matches = await findTasksByQuery(spaceId, item, 200);
      if (!matches.length) {
        await send(`一致するタスクが見つかりませんでした: ${item}`);
        continue;
      }
      if (matches.length > 1) {
        await send(`複数見つかりました。より具体的に教えてください:\n${formatTaskMatches(matches)}`);
        continue;
      }
      await sheetsUpdateTask(matches[0].task_id, { status: "deleted", deleted_at: new Date().toISOString() });
      deleted.push(matches[0].title);
    }
    if (deleted.length) await send(`削除しました: ${deleted.join("、")}`);
    return;
  }

  if (cmd.action === "create_task") {
    const title = (cmd.title || "").trim();
    if (!title) {
      await send("タスク名が分かりません。例: 議事録作成を明日18時までに追加");
      return;
    }
    let projectId = cmd.project_id || "";
    let projectTitle = cmd.project_title || "";
    if (projectTitle && !projectId) {
      const matches = await findProjectsByQuery(spaceId, projectTitle, 200);
      if (matches.length === 1) {
        projectId = matches[0].project_id;
        projectTitle = matches[0].title || projectTitle;
      } else if (matches.length > 1) {
        await send(`複数のプロジェクトが見つかりました。より具体的に教えてください:\n${formatProjectMatches(matches)}`);
        return;
      }
    }
    await sheetsAppendTask({
      spaceId,
      project_id: projectId,
      title,
      description: cmd.description || "",
      status: cmd.status || "open",
      due_at: cmd.due_at || "",
      created_by: userId,
    });
    await send(buildCreatedSummary("タスク", { title, description: cmd.description || "", status: cmd.status || "open", due_at: cmd.due_at || "", project_title: projectTitle }));
    return;
  }

  if (cmd.action === "create_project") {
    const title = (cmd.title || cmd.project_title || "").trim();
    if (!title) {
      await send("プロジェクト名が分かりません。例: プロジェクト『卒論』を追加");
      return;
    }
    await sheetsAppendProject({
      spaceId,
      title,
      description: cmd.description || "",
      status: cmd.status || "open",
      due_at: cmd.due_at || "",
      created_by: userId,
    });
    await send(buildCreatedSummary("プロジェクト", { title, description: cmd.description || "", status: cmd.status || "open", due_at: cmd.due_at || "" }));
    return;
  }

  await send("解釈できませんでした。『タスク一覧』『タスク追加』などをお試しください。");
});

const port = process.env.PORT || 8080;
app.listen(port, () => console.log(`Listening on ${port}`));
