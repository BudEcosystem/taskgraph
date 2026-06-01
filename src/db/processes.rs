use crate::db::{
    get_task, json_to_sql, now_utc_naive, parse_dt, parse_json, sleep_dt_to_sql, Database,
    TaskgraphError,
};
use crate::models::{generate_id, Event, EventType, TaskStatus};
use anyhow::{anyhow, Result};
use chrono::Duration;
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

const PROCESS_WAIT_SENTINEL: &str = "9999-12-31 23:59:59.999";

const INSERT_PROCESS_RUN: &str = r#"
INSERT INTO process_runs (
  id, task_id, project_id, agent_id, wait_id, status, command, cwd,
  callback_url, state_ref, idle_timeout_ms, timeout_ms, created_at, updated_at
) VALUES (
  ?1, ?2, ?3, ?4, ?5, 'created', ?6, ?7,
  ?8, ?9, ?10, ?11, ?12, ?12
);
"#;

const INSERT_PROCESS_HOOK: &str = r#"
INSERT INTO process_hooks(id, run_id, name, stream, pattern, created_at)
VALUES (?1, ?2, ?3, ?4, ?5, ?6);
"#;

const INSERT_TASK_WAIT: &str = r#"
INSERT INTO task_waits (
  id, task_id, project_id, agent_id, kind, source_id, status,
  state_ref, created_at, updated_at
) VALUES (
  ?1, ?2, ?3, ?4, 'process', ?5, 'waiting',
  ?6, ?7, ?7
);
"#;

const PUT_TASK_IN_PROCESS_WAIT: &str = r#"
UPDATE tasks
SET status = 'sleeping',
    sleep_id = ?2,
    sleep_until = ?3,
    sleep_state_ref = ?4,
    sleep_reason = ?5,
    wake_emitted_at = NULL,
    last_heartbeat = NULL,
    updated_at = ?6
WHERE id = ?1
  AND status IN ('claimed', 'running')
  AND agent_id = ?7;
"#;

const SELECT_PROCESS_RUN: &str = r#"
SELECT
  id, task_id, project_id, agent_id, wait_id, status, command, cwd,
  pid, runner_pid, started_at, ended_at, last_heartbeat_at, last_output_at,
  exit_code, exit_signal, terminal_reason, hook_name, hook_signal,
  stdout_path, stderr_path, callback_url, state_ref, idle_timeout_ms,
  timeout_ms, created_at, updated_at
FROM process_runs
WHERE id = ?1;
"#;

const SELECT_PROCESS_RUNS: &str = r#"
SELECT
  id, task_id, project_id, agent_id, wait_id, status, command, cwd,
  pid, runner_pid, started_at, ended_at, last_heartbeat_at, last_output_at,
  exit_code, exit_signal, terminal_reason, hook_name, hook_signal,
  stdout_path, stderr_path, callback_url, state_ref, idle_timeout_ms,
  timeout_ms, created_at, updated_at
FROM process_runs
WHERE (?1 IS NULL OR project_id = ?1)
  AND (?2 IS NULL OR task_id = ?2)
  AND (?3 IS NULL OR status = ?3)
ORDER BY created_at DESC, id DESC;
"#;

const SELECT_TASK_WAIT: &str = r#"
SELECT
  id, task_id, project_id, agent_id, kind, source_id, status,
  state_ref, due_reason, due_at, resumed_at, created_at, updated_at
FROM task_waits
WHERE id = ?1;
"#;

const SELECT_PROCESS_HOOKS: &str = r#"
SELECT id, run_id, name, stream, pattern, created_at
FROM process_hooks
WHERE run_id = ?1
ORDER BY created_at ASC, id ASC;
"#;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessHookSpec {
    pub name: String,
    #[serde(default = "default_hook_stream")]
    pub stream: String,
    pub pattern: String,
}

fn default_hook_stream() -> String {
    "any".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessLaunchRequest {
    pub task_id: String,
    pub agent_id: String,
    pub command: Vec<String>,
    pub cwd: Option<String>,
    pub hooks: Vec<ProcessHookSpec>,
    pub callback_url: Option<String>,
    pub state_ref: Option<Value>,
    pub idle_timeout_ms: Option<i64>,
    pub timeout_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProcessLaunchResult {
    pub run: ProcessRun,
    pub wait: TaskWait,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessRun {
    pub id: String,
    pub task_id: String,
    pub project_id: String,
    pub agent_id: String,
    pub wait_id: String,
    pub status: String,
    pub command: Vec<String>,
    pub cwd: Option<String>,
    pub pid: Option<i64>,
    pub runner_pid: Option<i64>,
    pub started_at: Option<chrono::NaiveDateTime>,
    pub ended_at: Option<chrono::NaiveDateTime>,
    pub last_heartbeat_at: Option<chrono::NaiveDateTime>,
    pub last_output_at: Option<chrono::NaiveDateTime>,
    pub exit_code: Option<i32>,
    pub exit_signal: Option<i32>,
    pub terminal_reason: Option<String>,
    pub hook_name: Option<String>,
    pub hook_signal: Option<String>,
    pub stdout_path: Option<String>,
    pub stderr_path: Option<String>,
    pub callback_url: Option<String>,
    pub state_ref: Option<Value>,
    pub idle_timeout_ms: Option<i64>,
    pub timeout_ms: Option<i64>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessHook {
    pub id: String,
    pub run_id: String,
    pub name: String,
    pub stream: String,
    pub pattern: String,
    pub created_at: chrono::NaiveDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskWait {
    pub id: String,
    pub task_id: String,
    pub project_id: String,
    pub agent_id: String,
    pub kind: String,
    pub source_id: String,
    pub status: String,
    pub state_ref: Option<Value>,
    pub due_reason: Option<String>,
    pub due_at: Option<chrono::NaiveDateTime>,
    pub resumed_at: Option<chrono::NaiveDateTime>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

#[derive(Debug, Clone, Default)]
pub struct ProcessRunFilters {
    pub project_id: Option<String>,
    pub task_id: Option<String>,
    pub status: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProcessLogs {
    pub run_id: String,
    pub stdout: Option<String>,
    pub stderr: Option<String>,
}

#[derive(Debug)]
struct OutboxRow {
    id: i64,
    event_id: i64,
    target_url: String,
    payload: String,
    attempts: i32,
}

fn row_to_process_run(row: &rusqlite::Row<'_>) -> rusqlite::Result<ProcessRun> {
    let conv = |idx: usize, e: anyhow::Error| {
        rusqlite::Error::FromSqlConversionFailure(
            idx,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                e.to_string(),
            )),
        )
    };
    let command_raw: String = row.get(6)?;
    let started_at: Option<String> = row.get(10)?;
    let ended_at: Option<String> = row.get(11)?;
    let last_heartbeat_at: Option<String> = row.get(12)?;
    let last_output_at: Option<String> = row.get(13)?;
    let state_ref: Option<String> = row.get(22)?;
    Ok(ProcessRun {
        id: row.get(0)?,
        task_id: row.get(1)?,
        project_id: row.get(2)?,
        agent_id: row.get(3)?,
        wait_id: row.get(4)?,
        status: row.get(5)?,
        command: serde_json::from_str(&command_raw).map_err(|e| conv(6, e.into()))?,
        cwd: row.get(7)?,
        pid: row.get(8)?,
        runner_pid: row.get(9)?,
        started_at: started_at
            .map(parse_dt)
            .transpose()
            .map_err(|e| conv(10, e))?,
        ended_at: ended_at
            .map(parse_dt)
            .transpose()
            .map_err(|e| conv(11, e))?,
        last_heartbeat_at: last_heartbeat_at
            .map(parse_dt)
            .transpose()
            .map_err(|e| conv(12, e))?,
        last_output_at: last_output_at
            .map(parse_dt)
            .transpose()
            .map_err(|e| conv(13, e))?,
        exit_code: row.get(14)?,
        exit_signal: row.get(15)?,
        terminal_reason: row.get(16)?,
        hook_name: row.get(17)?,
        hook_signal: row.get(18)?,
        stdout_path: row.get(19)?,
        stderr_path: row.get(20)?,
        callback_url: row.get(21)?,
        state_ref: parse_json(state_ref).map_err(|e| conv(22, e))?,
        idle_timeout_ms: row.get(23)?,
        timeout_ms: row.get(24)?,
        created_at: parse_dt(row.get::<_, String>(25)?).map_err(|e| conv(25, e))?,
        updated_at: parse_dt(row.get::<_, String>(26)?).map_err(|e| conv(26, e))?,
    })
}

fn row_to_process_hook(row: &rusqlite::Row<'_>) -> rusqlite::Result<ProcessHook> {
    let created_at: String = row.get(5)?;
    Ok(ProcessHook {
        id: row.get(0)?,
        run_id: row.get(1)?,
        name: row.get(2)?,
        stream: row.get(3)?,
        pattern: row.get(4)?,
        created_at: parse_dt(created_at).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                5,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    e.to_string(),
                )),
            )
        })?,
    })
}

fn row_to_task_wait(row: &rusqlite::Row<'_>) -> rusqlite::Result<TaskWait> {
    let conv = |idx: usize, e: anyhow::Error| {
        rusqlite::Error::FromSqlConversionFailure(
            idx,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                e.to_string(),
            )),
        )
    };
    let state_ref: Option<String> = row.get(7)?;
    let due_at: Option<String> = row.get(9)?;
    let resumed_at: Option<String> = row.get(10)?;
    Ok(TaskWait {
        id: row.get(0)?,
        task_id: row.get(1)?,
        project_id: row.get(2)?,
        agent_id: row.get(3)?,
        kind: row.get(4)?,
        source_id: row.get(5)?,
        status: row.get(6)?,
        state_ref: parse_json(state_ref).map_err(|e| conv(7, e))?,
        due_reason: row.get(8)?,
        due_at: due_at.map(parse_dt).transpose().map_err(|e| conv(9, e))?,
        resumed_at: resumed_at
            .map(parse_dt)
            .transpose()
            .map_err(|e| conv(10, e))?,
        created_at: parse_dt(row.get::<_, String>(11)?).map_err(|e| conv(11, e))?,
        updated_at: parse_dt(row.get::<_, String>(12)?).map_err(|e| conv(12, e))?,
    })
}

fn normalize_hook_stream(raw: &str) -> Result<String> {
    match raw {
        "any" | "stdout" | "stderr" => Ok(raw.to_string()),
        other => Err(anyhow!(
            "invalid hook stream '{other}'. Use any, stdout, or stderr"
        )),
    }
}

pub fn launch_process_run(
    db: &Database,
    request: ProcessLaunchRequest,
) -> Result<ProcessLaunchResult> {
    if request.command.is_empty() {
        return Err(anyhow!("process command cannot be empty"));
    }
    if let Some(ms) = request.idle_timeout_ms {
        if ms <= 0 {
            return Err(anyhow!("idle_timeout_ms must be positive"));
        }
    }
    if let Some(ms) = request.timeout_ms {
        if ms <= 0 {
            return Err(anyhow!("timeout_ms must be positive"));
        }
    }
    for hook in &request.hooks {
        if hook.name.trim().is_empty() {
            return Err(anyhow!("hook name cannot be empty"));
        }
        if hook.pattern.is_empty() {
            return Err(anyhow!("hook pattern cannot be empty"));
        }
        normalize_hook_stream(&hook.stream)?;
    }

    let task = get_task(db, &request.task_id)?;
    if !matches!(task.status, TaskStatus::Claimed | TaskStatus::Running) {
        return Err(TaskgraphError::InvalidTransition(format!(
            "task {} must be claimed or running to launch a process wait",
            request.task_id
        ))
        .into());
    }
    if task.agent_id.as_deref() != Some(&request.agent_id) {
        return Err(TaskgraphError::Conflict(format!(
            "task {} is owned by agent {}",
            request.task_id,
            task.agent_id.unwrap_or_else(|| "<none>".to_string())
        ))
        .into());
    }

    let run_id = generate_id("run");
    let wait_id = generate_id("wait");
    let now = sleep_dt_to_sql(now_utc_naive());
    let command_json = serde_json::to_string(&request.command)?;
    let state_ref_json = json_to_sql(&request.state_ref)?;
    let sleep_state_ref = json!({
        "kind": "process",
        "run_id": run_id,
        "wait_id": wait_id,
        "command": request.command,
        "state_ref": request.state_ref,
    });
    let sleep_state_ref_json = Some(serde_json::to_string(&sleep_state_ref)?);
    let sleep_reason = Some(format!("process:{run_id}"));

    {
        let mut conn = db.lock()?;
        let tx = conn.transaction()?;
        tx.execute(
            INSERT_PROCESS_RUN,
            params![
                &run_id,
                &task.id,
                &task.project_id,
                &request.agent_id,
                &wait_id,
                &command_json,
                &request.cwd,
                &request.callback_url,
                &state_ref_json,
                request.idle_timeout_ms,
                request.timeout_ms,
                &now,
            ],
        )?;
        for hook in &request.hooks {
            tx.execute(
                INSERT_PROCESS_HOOK,
                params![
                    generate_id("hook"),
                    &run_id,
                    hook.name.trim(),
                    normalize_hook_stream(&hook.stream)?,
                    &hook.pattern,
                    &now,
                ],
            )?;
        }
        tx.execute(
            INSERT_TASK_WAIT,
            params![
                &wait_id,
                &task.id,
                &task.project_id,
                &request.agent_id,
                &run_id,
                &state_ref_json,
                &now,
            ],
        )?;
        let changed = tx.execute(
            PUT_TASK_IN_PROCESS_WAIT,
            params![
                &task.id,
                &wait_id,
                PROCESS_WAIT_SENTINEL,
                &sleep_state_ref_json,
                &sleep_reason,
                &now,
                &request.agent_id,
            ],
        )?;
        if changed == 0 {
            return Err(TaskgraphError::InvalidTransition(format!(
                "task {} could not enter process wait",
                task.id
            ))
            .into());
        }
        tx.commit()?;
    }

    Ok(ProcessLaunchResult {
        run: get_process_run(db, &run_id)?,
        wait: get_task_wait(db, &wait_id)?,
    })
}

pub fn spawn_process_runner(db: &Database, run_id: &str) -> Result<()> {
    let db_path = db
        .path()
        .ok_or_else(|| anyhow!("database path is required to spawn process runner"))?;
    let exe = std::env::current_exe()?;
    Command::new(exe)
        .arg("--db")
        .arg(db_path)
        .arg("process-runner")
        .arg("--run-id")
        .arg(run_id)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    Ok(())
}

pub fn get_process_run(db: &Database, run_id: &str) -> Result<ProcessRun> {
    let conn = db.lock()?;
    let mut stmt = conn.prepare(SELECT_PROCESS_RUN)?;
    let run = stmt
        .query_row(params![run_id], row_to_process_run)
        .optional()?;
    run.ok_or_else(|| TaskgraphError::NotFound(format!("process run {run_id}")).into())
}

fn get_task_wait(db: &Database, wait_id: &str) -> Result<TaskWait> {
    let conn = db.lock()?;
    let mut stmt = conn.prepare(SELECT_TASK_WAIT)?;
    let wait = stmt
        .query_row(params![wait_id], row_to_task_wait)
        .optional()?;
    wait.ok_or_else(|| TaskgraphError::NotFound(format!("task wait {wait_id}")).into())
}

pub fn list_process_runs(db: &Database, filters: ProcessRunFilters) -> Result<Vec<ProcessRun>> {
    let conn = db.lock()?;
    let mut stmt = conn.prepare(SELECT_PROCESS_RUNS)?;
    let mut rows = stmt.query(params![filters.project_id, filters.task_id, filters.status])?;
    let mut runs = Vec::new();
    while let Some(row) = rows.next()? {
        runs.push(row_to_process_run(row)?);
    }
    Ok(runs)
}

pub fn list_process_hooks(db: &Database, run_id: &str) -> Result<Vec<ProcessHook>> {
    let conn = db.lock()?;
    let mut stmt = conn.prepare(SELECT_PROCESS_HOOKS)?;
    let mut rows = stmt.query(params![run_id])?;
    let mut hooks = Vec::new();
    while let Some(row) = rows.next()? {
        hooks.push(row_to_process_hook(row)?);
    }
    Ok(hooks)
}

pub fn mark_process_started(
    db: &Database,
    run_id: &str,
    pid: i64,
    runner_pid: i64,
    stdout_path: &str,
    stderr_path: &str,
) -> Result<ProcessRun> {
    let now = sleep_dt_to_sql(now_utc_naive());
    {
        let conn = db.lock()?;
        let changed = conn.execute(
            r#"
            UPDATE process_runs
            SET status = 'running',
                pid = ?2,
                runner_pid = ?3,
                started_at = ?4,
                last_heartbeat_at = ?4,
                stdout_path = ?5,
                stderr_path = ?6,
                updated_at = ?4
            WHERE id = ?1
              AND status = 'created';
            "#,
            params![run_id, pid, runner_pid, &now, stdout_path, stderr_path],
        )?;
        if changed == 0 {
            return Err(TaskgraphError::InvalidTransition(format!(
                "process run {run_id} could not be started"
            ))
            .into());
        }
    }
    let run = get_process_run(db, run_id)?;
    insert_process_event(
        db,
        &run,
        EventType::ProcessStarted,
        json!({
            "run_id": run.id,
            "wait_id": run.wait_id,
            "pid": run.pid,
            "runner_pid": run.runner_pid,
            "command": run.command,
        }),
        false,
    )?;
    Ok(run)
}

pub fn mark_process_output(db: &Database, run_id: &str) -> Result<()> {
    let now = sleep_dt_to_sql(now_utc_naive());
    let conn = db.lock()?;
    conn.execute(
        "UPDATE process_runs SET last_output_at = ?2, last_heartbeat_at = ?2, updated_at = ?2 WHERE id = ?1",
        params![run_id, now],
    )?;
    Ok(())
}

pub fn mark_process_heartbeat(db: &Database, run_id: &str) -> Result<()> {
    let now = sleep_dt_to_sql(now_utc_naive());
    let conn = db.lock()?;
    conn.execute(
        "UPDATE process_runs SET last_heartbeat_at = ?2, updated_at = ?2 WHERE id = ?1",
        params![run_id, now],
    )?;
    Ok(())
}

pub fn mark_process_hook_matched(
    db: &Database,
    run_id: &str,
    hook_name: &str,
    hook_signal: &str,
    stream: &str,
    line: &str,
) -> Result<ProcessRun> {
    let existing = get_process_run(db, run_id)?;
    if existing.hook_name.is_some() {
        return Ok(existing);
    }
    let now = sleep_dt_to_sql(now_utc_naive());
    {
        let conn = db.lock()?;
        conn.execute(
            r#"
            UPDATE process_runs
            SET hook_name = ?2,
                hook_signal = ?3,
                last_output_at = ?4,
                last_heartbeat_at = ?4,
                updated_at = ?4
            WHERE id = ?1
              AND hook_name IS NULL;
            "#,
            params![run_id, hook_name, hook_signal, &now],
        )?;
    }
    let run = get_process_run(db, run_id)?;
    let payload = json!({
        "run_id": run.id,
        "wait_id": run.wait_id,
        "hook": {
            "name": hook_name,
            "signal": hook_signal,
            "stream": stream,
            "line": line,
        },
        "status": run.status,
    });
    let event = insert_process_event(db, &run, EventType::ProcessHookMatched, payload, true)?;
    mark_process_wait_due(db, &run, "process_hook", Some(event.id))?;
    Ok(run)
}

pub fn mark_process_terminal(
    db: &Database,
    run_id: &str,
    status: &str,
    exit_code: Option<i32>,
    exit_signal: Option<i32>,
    terminal_reason: &str,
) -> Result<ProcessRun> {
    let existing = get_process_run(db, run_id)?;
    if is_terminal_status(&existing.status) {
        return Ok(existing);
    }
    let now = sleep_dt_to_sql(now_utc_naive());
    {
        let conn = db.lock()?;
        conn.execute(
            r#"
            UPDATE process_runs
            SET status = ?2,
                ended_at = ?3,
                last_heartbeat_at = ?3,
                exit_code = ?4,
                exit_signal = ?5,
                terminal_reason = ?6,
                updated_at = ?3
            WHERE id = ?1
              AND status NOT IN ('succeeded', 'failed', 'killed', 'stuck');
            "#,
            params![
                run_id,
                status,
                &now,
                exit_code,
                exit_signal,
                terminal_reason
            ],
        )?;
    }
    let run = get_process_run(db, run_id)?;
    let event_type = match status {
        "killed" => EventType::ProcessKilled,
        "stuck" => EventType::ProcessStuck,
        _ => EventType::ProcessExited,
    };
    let payload = json!({
        "run_id": run.id,
        "wait_id": run.wait_id,
        "status": run.status,
        "exit_code": run.exit_code,
        "exit_signal": run.exit_signal,
        "terminal_reason": run.terminal_reason,
    });
    let event = insert_process_event(db, &run, event_type, payload, true)?;
    mark_process_wait_due(db, &run, terminal_reason, Some(event.id))?;
    Ok(run)
}

pub fn request_process_kill(db: &Database, run_id: &str) -> Result<ProcessRun> {
    let run = get_process_run(db, run_id)?;
    if is_terminal_status(&run.status) {
        return Ok(run);
    }
    let pid = run
        .pid
        .ok_or_else(|| anyhow!("process run {run_id} has no child pid yet"))?;
    kill_pid(pid)?;
    let now = sleep_dt_to_sql(now_utc_naive());
    {
        let conn = db.lock()?;
        conn.execute(
            "UPDATE process_runs SET status = 'kill_requested', terminal_reason = 'kill_requested', updated_at = ?2 WHERE id = ?1 AND status NOT IN ('succeeded', 'failed', 'killed', 'stuck')",
            params![run_id, now],
        )?;
    }
    get_process_run(db, run_id)
}

fn kill_pid(pid: i64) -> Result<()> {
    #[cfg(unix)]
    {
        let status = Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .status()?;
        if status.success() {
            Ok(())
        } else {
            Err(anyhow!("failed to send TERM to pid {pid}"))
        }
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        Err(anyhow!("process kill is not supported on this platform"))
    }
}

fn is_terminal_status(status: &str) -> bool {
    matches!(status, "succeeded" | "failed" | "killed" | "stuck")
}

fn mark_process_wait_due(
    db: &Database,
    run: &ProcessRun,
    reason: &str,
    source_event_id: Option<i64>,
) -> Result<bool> {
    let now_dt = now_utc_naive();
    let now = sleep_dt_to_sql(now_dt);
    let mut changed = false;
    {
        let conn = db.lock()?;
        let wait_changed = conn.execute(
            r#"
            UPDATE task_waits
            SET status = 'due',
                due_reason = ?2,
                due_at = ?3,
                updated_at = ?3
            WHERE id = ?1
              AND status = 'waiting';
            "#,
            params![&run.wait_id, reason, &now],
        )?;
        if wait_changed > 0 {
            changed = true;
            conn.execute(
                r#"
                UPDATE tasks
                SET sleep_until = ?3,
                    wake_emitted_at = ?3,
                    updated_at = ?3
                WHERE id = ?1
                  AND status = 'sleeping'
                  AND sleep_id = ?2;
                "#,
                params![&run.task_id, &run.wait_id, &now],
            )?;
        }
    }

    if changed {
        let wait = get_task_wait(db, &run.wait_id)?;
        crate::db::insert_event(
            db,
            Some(&run.task_id),
            Some(&run.project_id),
            Some(&run.agent_id),
            EventType::TaskWakeDue,
            Some(json!({
                "task_id": run.task_id,
                "project_id": run.project_id,
                "agent_id": run.agent_id,
                "sleep_id": run.wait_id,
                "wait_id": run.wait_id,
                "run_id": run.id,
                "wake_at": now_dt,
                "reason": reason,
                "state_ref": wait.state_ref,
                "source_event_id": source_event_id,
            })),
            now_dt,
        )?;
    }

    Ok(changed)
}

fn insert_process_event(
    db: &Database,
    run: &ProcessRun,
    event_type: EventType,
    payload: Value,
    notify: bool,
) -> Result<Event> {
    let event = crate::db::insert_event(
        db,
        Some(&run.task_id),
        Some(&run.project_id),
        Some(&run.agent_id),
        event_type,
        Some(payload.clone()),
        now_utc_naive(),
    )?;
    if notify {
        enqueue_notification(db, run, &event, payload)?;
    }
    Ok(event)
}

fn enqueue_notification(
    db: &Database,
    run: &ProcessRun,
    event: &Event,
    payload: Value,
) -> Result<Option<i64>> {
    let Some(target_url) = run.callback_url.as_deref() else {
        return Ok(None);
    };
    let now = sleep_dt_to_sql(now_utc_naive());
    let callback_payload = json!({
        "event_id": event.id,
        "event": event.event_type,
        "task_id": run.task_id,
        "project_id": run.project_id,
        "agent_id": run.agent_id,
        "run_id": run.id,
        "wait_id": run.wait_id,
        "status": run.status,
        "payload": payload,
    });
    let payload_json = serde_json::to_string(&callback_payload)?;
    let conn = db.lock()?;
    conn.execute(
        r#"
        INSERT INTO notification_outbox(
          event_id, run_id, task_id, project_id, agent_id, target_url,
          payload, status, attempts, next_attempt_at, created_at, updated_at
        ) VALUES (
          ?1, ?2, ?3, ?4, ?5, ?6,
          ?7, 'pending', 0, ?8, ?8, ?8
        );
        "#,
        params![
            event.id,
            &run.id,
            &run.task_id,
            &run.project_id,
            &run.agent_id,
            target_url,
            &payload_json,
            &now,
        ],
    )?;
    Ok(Some(conn.last_insert_rowid()))
}

pub fn dispatch_due_notifications(db: &Database, limit: usize) -> Result<usize> {
    let now = sleep_dt_to_sql(now_utc_naive());
    let rows = {
        let conn = db.lock()?;
        let mut stmt = conn.prepare(
            r#"
            SELECT id, event_id, target_url, payload, attempts
            FROM notification_outbox
            WHERE status = 'pending'
              AND next_attempt_at <= ?1
            ORDER BY id ASC
            LIMIT ?2;
            "#,
        )?;
        let mut rows = stmt.query(params![&now, limit as i64])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(OutboxRow {
                id: row.get(0)?,
                event_id: row.get(1)?,
                target_url: row.get(2)?,
                payload: row.get(3)?,
                attempts: row.get(4)?,
            });
        }
        out
    };

    let mut delivered = 0usize;
    for row in rows {
        match send_callback(&row) {
            Ok(()) => {
                let now = sleep_dt_to_sql(now_utc_naive());
                let conn = db.lock()?;
                conn.execute(
                    "UPDATE notification_outbox SET status = 'delivered', attempts = attempts + 1, updated_at = ?2 WHERE id = ?1",
                    params![row.id, now],
                )?;
                delivered += 1;
            }
            Err(err) => {
                let next_attempts = row.attempts + 1;
                let terminal = next_attempts >= 8;
                let backoff_ms = 1_000_i64
                    .saturating_mul(2_i64.saturating_pow((next_attempts - 1).clamp(0, 6) as u32));
                let now_dt = now_utc_naive();
                let next = if terminal {
                    sleep_dt_to_sql(now_dt)
                } else {
                    sleep_dt_to_sql(now_dt + Duration::milliseconds(backoff_ms))
                };
                let now = sleep_dt_to_sql(now_dt);
                let conn = db.lock()?;
                conn.execute(
                    r#"
                    UPDATE notification_outbox
                    SET status = ?2,
                        attempts = ?3,
                        next_attempt_at = ?4,
                        last_error = ?5,
                        updated_at = ?6
                    WHERE id = ?1;
                    "#,
                    params![
                        row.id,
                        if terminal { "failed" } else { "pending" },
                        next_attempts,
                        next,
                        err.to_string(),
                        now,
                    ],
                )?;
            }
        }
    }
    Ok(delivered)
}

fn send_callback(row: &OutboxRow) -> Result<()> {
    let response = ureq::post(&row.target_url)
        .timeout(std::time::Duration::from_secs(5))
        .set("Content-Type", "application/json")
        .set(
            "Idempotency-Key",
            &format!("taskgraph:event:{}", row.event_id),
        )
        .send_string(&row.payload);
    match response {
        Ok(resp) if (200..300).contains(&resp.status()) => Ok(()),
        Ok(resp) => Err(anyhow!("callback returned HTTP {}", resp.status())),
        Err(err) => Err(anyhow!("callback failed: {err}")),
    }
}

pub fn get_process_logs(db: &Database, run_id: &str) -> Result<ProcessLogs> {
    let run = get_process_run(db, run_id)?;
    let stdout = read_optional_path(run.stdout_path.as_deref())?;
    let stderr = read_optional_path(run.stderr_path.as_deref())?;
    Ok(ProcessLogs {
        run_id: run.id,
        stdout,
        stderr,
    })
}

fn read_optional_path(path: Option<&str>) -> Result<Option<String>> {
    match path {
        Some(path) if Path::new(path).exists() => Ok(Some(fs::read_to_string(path)?)),
        _ => Ok(None),
    }
}

pub fn process_log_dir(db_path: &str, run_id: &str) -> PathBuf {
    let db_path = Path::new(db_path);
    let base = db_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    base.join(".taskgraph").join("process-runs").join(run_id)
}
