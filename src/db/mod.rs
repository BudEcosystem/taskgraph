pub mod artifacts;
pub mod dependencies;
pub mod effects;
pub mod events;
pub mod files;
pub mod meta;
pub mod notes;
pub mod processes;
pub mod projects;
pub mod schema;
pub mod sweeper;
pub mod tasks;

use anyhow::{anyhow, Result};
use chrono::{Duration, NaiveDateTime, Utc};
use rusqlite::Connection;
use serde_json::Value;
use std::sync::{Arc, Mutex, MutexGuard};
use thiserror::Error;

pub use artifacts::{create_artifact, get_artifact, get_upstream_artifacts, list_artifacts};
pub use dependencies::{
    add_dependency, get_downstream_tasks, get_upstream_tasks, list_dependencies, remove_dependency,
};
pub use effects::{compute_effects, snapshot_task_statuses, MutationEffect};
pub use events::{insert_event, list_events, EventFilters};
pub use files::{add_task_files, check_file_conflicts, list_task_files, FileConflict};
pub use meta::{delete_meta, get_meta, set_meta};
pub use notes::{add_note, list_notes};
pub use processes::{
    dispatch_due_notifications, get_process_logs, get_process_run, launch_process_run,
    list_process_hooks, list_process_runs, mark_process_heartbeat, mark_process_hook_matched,
    mark_process_output, mark_process_started, mark_process_terminal, process_launch_enabled,
    process_log_capture_limit_bytes, process_log_dir, process_runner_stale_after,
    reap_stale_process_runs, request_process_kill, request_process_kill_for_task,
    spawn_process_runner, spawn_process_runner_or_mark_failed, ProcessHook, ProcessHookSpec,
    ProcessLaunchRequest, ProcessLaunchResult, ProcessLogs, ProcessRun, ProcessRunFilters,
    TaskWait, PROCESS_LOG_TRUNCATION_NOTICE,
};
pub use projects::{
    create_project, fuzzy_find_project, get_project, list_projects, update_project_status,
};
pub use schema::init_db;
pub use sweeper::{run_sweep, SweepResult};
pub use tasks::{
    amend_task_description, approve_task, batch_create_tasks, cancel_task, claim_next_task,
    claim_task, complete_task, create_task, emit_due_wakes, fail_task, fuzzy_find_task,
    get_handoff_context, get_lookahead, get_task, insert_task_between, list_due_wakes, list_tasks,
    next_sleep_due_at, parse_sleep_duration_ms, pause_task, pivot_subtree, project_state,
    promote_ready_tasks, resume_task, sleep_task, split_task, start_task, update_heartbeat,
    update_progress, update_task, DueWake, HandoffEntry, LookaheadResult, NewSubtask, PivotResult,
    ProjectState, ResumeResult, SleepResult, SplitPart, SplitResult, TaskListFilters,
};

#[derive(Debug, Error)]
pub enum TaskgraphError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("invalid transition: {0}")]
    InvalidTransition(String),
    #[error("conflict: {0}")]
    Conflict(String),
}

#[derive(Clone)]
pub struct Database {
    conn: Arc<Mutex<Connection>>,
    path: Option<Arc<String>>,
}

impl Database {
    pub fn from_connection(conn: Connection) -> Self {
        Self {
            conn: Arc::new(Mutex::new(conn)),
            path: None,
        }
    }

    pub fn from_connection_with_path(conn: Connection, path: String) -> Self {
        Self {
            conn: Arc::new(Mutex::new(conn)),
            path: Some(Arc::new(path)),
        }
    }

    pub fn lock(&self) -> Result<MutexGuard<'_, Connection>> {
        self.conn
            .lock()
            .map_err(|_| anyhow!("database connection mutex poisoned"))
    }

    pub fn path(&self) -> Option<&str> {
        self.path.as_deref().map(String::as_str)
    }
}

const DATETIME_FMT: &str = "%Y-%m-%d %H:%M:%S";
const SLEEP_DATETIME_FMT: &str = "%Y-%m-%d %H:%M:%S%.3f";

pub(crate) fn now_utc_naive() -> NaiveDateTime {
    Utc::now().naive_utc()
}

pub(crate) fn dt_to_sql(dt: NaiveDateTime) -> String {
    dt.format(DATETIME_FMT).to_string()
}

pub(crate) fn truncate_to_millis(dt: NaiveDateTime) -> NaiveDateTime {
    let extra_nanos = i64::from(dt.and_utc().timestamp_subsec_nanos() % 1_000_000);
    dt - Duration::nanoseconds(extra_nanos)
}

pub(crate) fn sleep_dt_to_sql(dt: NaiveDateTime) -> String {
    truncate_to_millis(dt)
        .format(SLEEP_DATETIME_FMT)
        .to_string()
}

pub(crate) fn parse_dt(value: String) -> Result<NaiveDateTime> {
    NaiveDateTime::parse_from_str(&value, SLEEP_DATETIME_FMT)
        .or_else(|_| NaiveDateTime::parse_from_str(&value, DATETIME_FMT))
        .map_err(Into::into)
}

pub(crate) fn json_to_sql(value: &Option<Value>) -> Result<Option<String>> {
    match value {
        Some(v) => Ok(Some(serde_json::to_string(v)?)),
        None => Ok(None),
    }
}

pub(crate) fn parse_json(value: Option<String>) -> Result<Option<Value>> {
    match value {
        Some(v) => Ok(Some(serde_json::from_str(&v)?)),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests;
