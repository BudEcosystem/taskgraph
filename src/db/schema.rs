use crate::db::Database;
use anyhow::Result;
use rusqlite::Connection;

const PRAGMA_WAL: &str = "PRAGMA journal_mode = WAL;";
const PRAGMA_FOREIGN_KEYS: &str = "PRAGMA foreign_keys = ON;";

const CREATE_PROJECTS: &str = r#"
CREATE TABLE IF NOT EXISTS projects (
  id           TEXT PRIMARY KEY,
  user_id      TEXT,
  name         TEXT NOT NULL,
  description  TEXT,
  status       TEXT NOT NULL DEFAULT 'active',
  metadata     JSON,
  created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"#;

const CREATE_TASKS: &str = r#"
CREATE TABLE IF NOT EXISTS tasks (
  id                 TEXT PRIMARY KEY,
  project_id         TEXT NOT NULL REFERENCES projects(id),
  parent_task_id     TEXT REFERENCES tasks(id),
  is_composite       BOOLEAN NOT NULL DEFAULT FALSE,
  title              TEXT NOT NULL,
  description        TEXT,
  status             TEXT NOT NULL DEFAULT 'pending',
  kind               TEXT NOT NULL DEFAULT 'generic',
  priority           INTEGER NOT NULL DEFAULT 0,
  agent_id           TEXT,
  claimed_at         DATETIME,
  started_at         DATETIME,
  completed_at       DATETIME,
  result             JSON,
  error              TEXT,
  progress           INTEGER,
  progress_note      TEXT,
  max_retries        INTEGER NOT NULL DEFAULT 0,
  retry_count        INTEGER NOT NULL DEFAULT 0,
  retry_backoff      TEXT NOT NULL DEFAULT 'exponential',
  retry_delay_ms     INTEGER NOT NULL DEFAULT 1000,
  timeout_seconds    INTEGER,
  heartbeat_interval INTEGER NOT NULL DEFAULT 30,
  last_heartbeat     DATETIME,
  sleep_id           TEXT,
  sleep_until        DATETIME,
  sleep_state_ref    JSON,
  sleep_reason       TEXT,
  wake_emitted_at    DATETIME,
  requires_approval  BOOLEAN NOT NULL DEFAULT FALSE,
  approval_status    TEXT,
  approved_by        TEXT,
  approval_comment   TEXT,
  metadata           JSON,
  created_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"#;

const CREATE_DEPENDENCIES: &str = r#"
CREATE TABLE IF NOT EXISTS dependencies (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  from_task    TEXT NOT NULL REFERENCES tasks(id),
  to_task      TEXT NOT NULL REFERENCES tasks(id),
  kind         TEXT NOT NULL DEFAULT 'blocks',
  condition    TEXT NOT NULL DEFAULT 'all',
  metadata     JSON,
  UNIQUE(from_task, to_task)
);
"#;

const CREATE_ARTIFACTS: &str = r#"
CREATE TABLE IF NOT EXISTS artifacts (
  id           TEXT PRIMARY KEY,
  task_id      TEXT NOT NULL REFERENCES tasks(id),
  name         TEXT NOT NULL,
  kind         TEXT,
  content      TEXT,
  path         TEXT,
  size_bytes   INTEGER,
  mime_type    TEXT,
  metadata     JSON,
  created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"#;

const CREATE_EVENTS: &str = r#"
CREATE TABLE IF NOT EXISTS events (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id      TEXT REFERENCES tasks(id),
  project_id   TEXT REFERENCES projects(id),
  agent_id     TEXT,
  event_type   TEXT NOT NULL,
  payload      JSON,
  timestamp    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"#;

const CREATE_TASK_TAGS: &str = r#"
CREATE TABLE IF NOT EXISTS task_tags (
  task_id  TEXT NOT NULL REFERENCES tasks(id),
  tag      TEXT NOT NULL,
  PRIMARY KEY (task_id, tag)
);
"#;

const CREATE_TASKGRAPH_META: &str = r#"
CREATE TABLE IF NOT EXISTS taskgraph_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
"#;

const CREATE_TASK_NOTES: &str = r#"
CREATE TABLE IF NOT EXISTS task_notes (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(id),
  agent_id TEXT,
  content TEXT NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"#;

const CREATE_TASK_FILES: &str = r#"
CREATE TABLE IF NOT EXISTS task_files (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id TEXT NOT NULL REFERENCES tasks(id),
  path TEXT NOT NULL,
  UNIQUE(task_id, path)
);
"#;

const CREATE_PROCESS_RUNS: &str = r#"
CREATE TABLE IF NOT EXISTS process_runs (
  id                 TEXT PRIMARY KEY,
  task_id            TEXT NOT NULL REFERENCES tasks(id),
  project_id         TEXT NOT NULL REFERENCES projects(id),
  agent_id           TEXT NOT NULL,
  wait_id            TEXT NOT NULL,
  status             TEXT NOT NULL DEFAULT 'created',
  command            JSON NOT NULL,
  cwd                TEXT,
  pid                INTEGER,
  runner_pid         INTEGER,
  started_at         DATETIME,
  ended_at           DATETIME,
  last_heartbeat_at  DATETIME,
  last_output_at     DATETIME,
  exit_code          INTEGER,
  exit_signal        INTEGER,
  terminal_reason    TEXT,
  hook_name          TEXT,
  hook_signal        TEXT,
  stdout_path        TEXT,
  stderr_path        TEXT,
  callback_url       TEXT,
  state_ref          JSON,
  idle_timeout_ms    INTEGER,
  timeout_ms         INTEGER,
  created_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"#;

const CREATE_PROCESS_HOOKS: &str = r#"
CREATE TABLE IF NOT EXISTS process_hooks (
  id          TEXT PRIMARY KEY,
  run_id      TEXT NOT NULL REFERENCES process_runs(id),
  name        TEXT NOT NULL,
  stream      TEXT NOT NULL DEFAULT 'any',
  pattern     TEXT NOT NULL,
  created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"#;

const CREATE_TASK_WAITS: &str = r#"
CREATE TABLE IF NOT EXISTS task_waits (
  id          TEXT PRIMARY KEY,
  task_id     TEXT NOT NULL REFERENCES tasks(id),
  project_id  TEXT NOT NULL REFERENCES projects(id),
  agent_id    TEXT NOT NULL,
  kind        TEXT NOT NULL,
  source_id   TEXT NOT NULL,
  status      TEXT NOT NULL DEFAULT 'waiting',
  state_ref   JSON,
  due_reason  TEXT,
  due_at      DATETIME,
  resumed_at  DATETIME,
  created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"#;

const CREATE_NOTIFICATION_OUTBOX: &str = r#"
CREATE TABLE IF NOT EXISTS notification_outbox (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id        INTEGER NOT NULL REFERENCES events(id),
  run_id          TEXT,
  task_id         TEXT,
  project_id      TEXT,
  agent_id        TEXT,
  target_url      TEXT NOT NULL,
  payload         JSON NOT NULL,
  status          TEXT NOT NULL DEFAULT 'pending',
  lease_id        TEXT,
  attempts        INTEGER NOT NULL DEFAULT 0,
  next_attempt_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_error      TEXT,
  created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"#;

const INDEX_TASKS_PROJECT_STATUS: &str =
    "CREATE INDEX IF NOT EXISTS idx_tasks_project_status ON tasks(project_id, status);";
const INDEX_TASKS_PARENT: &str =
    "CREATE INDEX IF NOT EXISTS idx_tasks_parent ON tasks(parent_task_id);";
const INDEX_DEPS_FROM: &str =
    "CREATE INDEX IF NOT EXISTS idx_dependencies_from ON dependencies(from_task);";
const INDEX_DEPS_TO: &str =
    "CREATE INDEX IF NOT EXISTS idx_dependencies_to ON dependencies(to_task);";
const INDEX_EVENTS_PROJECT_TS: &str =
    "CREATE INDEX IF NOT EXISTS idx_events_project_timestamp ON events(project_id, timestamp);";
const INDEX_TASK_TAGS_TAG: &str = "CREATE INDEX IF NOT EXISTS idx_task_tags_tag ON task_tags(tag);";
const INDEX_TASK_NOTES_TASK: &str =
    "CREATE INDEX IF NOT EXISTS idx_task_notes_task ON task_notes(task_id);";
const INDEX_TASK_FILES_TASK: &str =
    "CREATE INDEX IF NOT EXISTS idx_task_files_task ON task_files(task_id);";
const INDEX_TASK_FILES_PATH: &str =
    "CREATE INDEX IF NOT EXISTS idx_task_files_path ON task_files(path);";
const INDEX_PROJECTS_USER: &str =
    "CREATE INDEX IF NOT EXISTS idx_projects_user ON projects(user_id);";
const INDEX_TASKS_SLEEP_UNTIL: &str =
    "CREATE INDEX IF NOT EXISTS idx_tasks_sleep_until ON tasks(status, sleep_until, wake_emitted_at);";
const INDEX_PROCESS_RUNS_TASK: &str =
    "CREATE INDEX IF NOT EXISTS idx_process_runs_task ON process_runs(task_id, status);";
const INDEX_PROCESS_RUNS_WAIT: &str =
    "CREATE INDEX IF NOT EXISTS idx_process_runs_wait ON process_runs(wait_id);";
const INDEX_PROCESS_HOOKS_RUN: &str =
    "CREATE INDEX IF NOT EXISTS idx_process_hooks_run ON process_hooks(run_id);";
const INDEX_TASK_WAITS_TASK: &str =
    "CREATE INDEX IF NOT EXISTS idx_task_waits_task ON task_waits(task_id, status);";
const INDEX_TASK_WAITS_SOURCE: &str =
    "CREATE INDEX IF NOT EXISTS idx_task_waits_source ON task_waits(kind, source_id, status);";
const INDEX_NOTIFICATION_OUTBOX_DUE: &str =
    "CREATE INDEX IF NOT EXISTS idx_notification_outbox_due ON notification_outbox(status, next_attempt_at);";

const CREATE_TASK_READINESS_VIEW: &str = r#"
CREATE VIEW IF NOT EXISTS task_readiness AS
SELECT
  t.id,
  t.status,
  COUNT(CASE
    WHEN d.kind IN ('blocks', 'feeds_into')
     AND upstream.status NOT IN ('done', 'done_partial')
    THEN 1
  END) AS unmet_deps,
  CASE
    WHEN t.status = 'pending'
     AND COUNT(CASE
         WHEN d.kind IN ('blocks','feeds_into')
          AND upstream.status NOT IN ('done','done_partial')
         THEN 1 END) = 0
    THEN 1 ELSE 0
  END AS promotable
FROM tasks t
LEFT JOIN dependencies d ON d.to_task = t.id
LEFT JOIN tasks upstream ON upstream.id = d.from_task
GROUP BY t.id;
"#;

pub fn init_db(path: &str) -> Result<Database> {
    let conn = Connection::open(path)?;
    conn.execute_batch(PRAGMA_FOREIGN_KEYS)?;
    conn.execute_batch(PRAGMA_WAL)?;
    conn.execute_batch(CREATE_PROJECTS)?;
    conn.execute_batch(CREATE_TASKS)?;
    conn.execute_batch(CREATE_DEPENDENCIES)?;
    conn.execute_batch(CREATE_ARTIFACTS)?;
    conn.execute_batch(CREATE_EVENTS)?;
    conn.execute_batch(CREATE_TASK_TAGS)?;
    conn.execute_batch(CREATE_TASKGRAPH_META)?;
    conn.execute_batch(CREATE_TASK_NOTES)?;
    conn.execute_batch(CREATE_TASK_FILES)?;
    conn.execute_batch(CREATE_PROCESS_RUNS)?;
    conn.execute_batch(CREATE_PROCESS_HOOKS)?;
    conn.execute_batch(CREATE_TASK_WAITS)?;
    conn.execute_batch(CREATE_NOTIFICATION_OUTBOX)?;
    conn.execute_batch(INDEX_TASKS_PROJECT_STATUS)?;
    conn.execute_batch(INDEX_TASKS_PARENT)?;
    conn.execute_batch(INDEX_DEPS_FROM)?;
    conn.execute_batch(INDEX_DEPS_TO)?;
    conn.execute_batch(INDEX_EVENTS_PROJECT_TS)?;
    conn.execute_batch(INDEX_TASK_TAGS_TAG)?;
    conn.execute_batch(INDEX_TASK_NOTES_TASK)?;
    conn.execute_batch(INDEX_TASK_FILES_TASK)?;
    conn.execute_batch(INDEX_TASK_FILES_PATH)?;
    let _ = conn.execute_batch("ALTER TABLE tasks ADD COLUMN sleep_id TEXT;");
    let _ = conn.execute_batch("ALTER TABLE tasks ADD COLUMN sleep_until DATETIME;");
    let _ = conn.execute_batch("ALTER TABLE tasks ADD COLUMN sleep_state_ref JSON;");
    let _ = conn.execute_batch("ALTER TABLE tasks ADD COLUMN sleep_reason TEXT;");
    let _ = conn.execute_batch("ALTER TABLE tasks ADD COLUMN wake_emitted_at DATETIME;");
    let _ = conn.execute_batch("ALTER TABLE notification_outbox ADD COLUMN lease_id TEXT;");
    conn.execute_batch(INDEX_TASKS_SLEEP_UNTIL)?;
    conn.execute_batch(INDEX_PROCESS_RUNS_TASK)?;
    conn.execute_batch(INDEX_PROCESS_RUNS_WAIT)?;
    conn.execute_batch(INDEX_PROCESS_HOOKS_RUN)?;
    conn.execute_batch(INDEX_TASK_WAITS_TASK)?;
    conn.execute_batch(INDEX_TASK_WAITS_SOURCE)?;
    conn.execute_batch(INDEX_NOTIFICATION_OUTBOX_DUE)?;
    // Migration: add user_id column for existing databases (must run before index)
    let _ = conn.execute_batch("ALTER TABLE projects ADD COLUMN user_id TEXT;");
    conn.execute_batch(INDEX_PROJECTS_USER)?;
    conn.execute_batch(CREATE_TASK_READINESS_VIEW)?;
    Ok(Database::from_connection_with_path(conn, path.to_string()))
}
