require("dotenv").config();

const { google } = require("googleapis");
const { Pool } = require("pg");
const { Connector } = require("@google-cloud/cloud-sql-connector");

const SHEETS_SA_KEY_JSON = process.env.KAI_BOT_SHEETS_SA_KEY_JSON;
const SPREADSHEET_ID = process.env.KAI_BOT_SHEETS_SPREADSHEET_ID;

const DB_INSTANCE = process.env.KAI_BOT_DB_INSTANCE;
const DB_NAME = process.env.KAI_BOT_DB_NAME || "kai_bot";
const DB_USER = process.env.KAI_BOT_DB_USER || "kai_bot";
const DB_PASSWORD = process.env.KAI_BOT_DB_PASSWORD;

function headerIndex(headerRow) {
  return Object.fromEntries((headerRow || []).map((h, i) => [String(h || "").trim(), i]));
}

function requireColumns(idx, cols, sheetName) {
  const missing = cols.filter((c) => idx[c] === undefined);
  if (missing.length) {
    throw new Error(`${sheetName} sheet is missing required columns: ${missing.join(", ")}`);
  }
}

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

async function getDbPool() {
  if (!DB_INSTANCE) throw new Error("Missing env: KAI_BOT_DB_INSTANCE");
  if (!DB_PASSWORD) throw new Error("Missing env: KAI_BOT_DB_PASSWORD");

  const connector = new Connector();
  const clientOpts = await connector.getOptions({ instanceConnectionName: DB_INSTANCE });
  return new Pool({
    ...clientOpts,
    user: DB_USER,
    password: DB_PASSWORD,
    database: DB_NAME,
    max: 2,
  });
}

async function ensureSchema(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS projects (
      project_id TEXT PRIMARY KEY,
      space_id TEXT NOT NULL,
      title TEXT NOT NULL,
      description TEXT DEFAULT '',
      status TEXT DEFAULT 'open',
      due_at TEXT DEFAULT '',
      created_at TEXT DEFAULT '',
      created_by TEXT DEFAULT '',
      updated_at TEXT DEFAULT '',
      deleted_at TEXT DEFAULT ''
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS tasks (
      task_id TEXT PRIMARY KEY,
      space_id TEXT NOT NULL,
      project_id TEXT DEFAULT '',
      title TEXT NOT NULL,
      description TEXT DEFAULT '',
      status TEXT DEFAULT 'open',
      due_at TEXT DEFAULT '',
      created_at TEXT DEFAULT '',
      done_at TEXT DEFAULT '',
      created_by TEXT DEFAULT '',
      updated_at TEXT DEFAULT '',
      deleted_at TEXT DEFAULT ''
    );
  `);
}

async function migrateProjects(pool) {
  const values = await sheetsGetValues("Projects!A:Z");
  if (values.length <= 1) return 0;
  const header = values[0];
  const rows = values.slice(1);
  const idx = headerIndex(header);
  requireColumns(idx, ["project_id", "group_id", "title"], "Projects");

  await pool.query("DELETE FROM projects");

  let count = 0;
  for (const r of rows) {
    const project_id = String(r[idx.project_id] || "").trim();
    if (!project_id) continue;
    const space_id = String(r[idx.group_id] || "").trim();
    const title = String(r[idx.title] || "").trim();
    const description = idx.description !== undefined ? String(r[idx.description] || "") : "";
    const status = idx.status !== undefined ? String(r[idx.status] || "") : "";
    const due_at = idx.due_at !== undefined ? String(r[idx.due_at] || "") : "";
    const created_at = idx.created_at !== undefined ? String(r[idx.created_at] || "") : "";
    const created_by = idx.created_by !== undefined ? String(r[idx.created_by] || "") : "";
    const updated_at = idx.updated_at !== undefined ? String(r[idx.updated_at] || "") : "";
    const deleted_at = idx.deleted_at !== undefined ? String(r[idx.deleted_at] || "") : "";

    await pool.query(
      `INSERT INTO projects (project_id, space_id, title, description, status, due_at, created_at, created_by, updated_at, deleted_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
      [project_id, space_id, title, description, status, due_at, created_at, created_by, updated_at, deleted_at]
    );
    count += 1;
  }
  return count;
}

async function migrateTasks(pool) {
  const values = await sheetsGetValues("Tasks!A:Z");
  if (values.length <= 1) return 0;
  const header = values[0];
  const rows = values.slice(1);
  const idx = headerIndex(header);
  requireColumns(idx, ["task_id", "group_id", "title"], "Tasks");

  await pool.query("DELETE FROM tasks");

  let count = 0;
  for (const r of rows) {
    const task_id = String(r[idx.task_id] || "").trim();
    if (!task_id) continue;
    const space_id = String(r[idx.group_id] || "").trim();
    const project_id = idx.project_id !== undefined ? String(r[idx.project_id] || "") : "";
    const title = String(r[idx.title] || "").trim();
    const description = idx.description !== undefined ? String(r[idx.description] || "") : "";
    const status = idx.status !== undefined ? String(r[idx.status] || "") : "";
    const due_at = idx.due_at !== undefined ? String(r[idx.due_at] || "") : "";
    const created_at = idx.created_at !== undefined ? String(r[idx.created_at] || "") : "";
    const done_at = idx.done_at !== undefined ? String(r[idx.done_at] || "") : "";
    const created_by = idx.created_by !== undefined ? String(r[idx.created_by] || "") : "";
    const updated_at = idx.updated_at !== undefined ? String(r[idx.updated_at] || "") : "";
    const deleted_at = idx.deleted_at !== undefined ? String(r[idx.deleted_at] || "") : "";

    await pool.query(
      `INSERT INTO tasks (task_id, space_id, project_id, title, description, status, due_at, created_at, done_at, created_by, updated_at, deleted_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
      [task_id, space_id, project_id, title, description, status, due_at, created_at, done_at, created_by, updated_at, deleted_at]
    );
    count += 1;
  }
  return count;
}

async function main() {
  const pool = await getDbPool();
  await ensureSchema(pool);

  const projects = await migrateProjects(pool);
  const tasks = await migrateTasks(pool);

  await pool.end();
  console.log(`Migrated projects=${projects}, tasks=${tasks}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
