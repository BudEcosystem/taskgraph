use crate::db::*;
use crate::models::*;
use chrono::{Duration, Utc};
use serde_json::json;
use std::sync::{Arc, Barrier};
use std::thread;

fn test_db_path() -> String {
    let mut path = std::env::temp_dir();
    path.push(format!("taskgraph-test-{}.db", generate_id("tmp")));
    path.to_string_lossy().to_string()
}

fn now() -> chrono::NaiveDateTime {
    Utc::now().naive_utc()
}

fn make_task(project_id: &str, title: &str, status: TaskStatus) -> Task {
    let t = now();
    Task {
        id: generate_id("task"),
        project_id: project_id.to_owned(),
        parent_task_id: None,
        is_composite: false,
        title: title.to_owned(),
        description: None,
        status,
        kind: TaskKind::Generic,
        priority: 0,
        agent_id: None,
        claimed_at: None,
        started_at: None,
        completed_at: None,
        result: None,
        error: None,
        progress: None,
        progress_note: None,
        max_retries: 0,
        retry_count: 0,
        retry_backoff: RetryBackoff::Exponential,
        retry_delay_ms: 1000,
        timeout_seconds: None,
        heartbeat_interval: 30,
        last_heartbeat: None,
        sleep_id: None,
        sleep_until: None,
        sleep_state_ref: None,
        sleep_reason: None,
        wake_emitted_at: None,
        requires_approval: false,
        approval_status: None,
        approved_by: None,
        approval_comment: None,
        metadata: None,
        created_at: t,
        updated_at: t,
    }
}

#[test]
fn create_project_and_task() {
    let db_path = test_db_path();
    let db = init_db(&db_path).unwrap();

    let project = create_project(&db, "Demo", Some("desc".to_string()), None, None).unwrap();
    assert_eq!(project.status, ProjectStatus::Active);

    let task = make_task(&project.id, "first", TaskStatus::Pending);
    let created_task = create_task(&db, &task, &[]).unwrap();
    assert_eq!(created_task.status, TaskStatus::Pending);

    let fetched = get_task(&db, &created_task.id).unwrap();
    assert_eq!(fetched.title, "first");
}

#[test]
fn claim_task_concurrency_single_winner() {
    let db_path = test_db_path();
    let db = init_db(&db_path).unwrap();
    let project = create_project(&db, "Claim", None, None, None).unwrap();

    let task = make_task(&project.id, "claim me", TaskStatus::Ready);
    let created_task = create_task(&db, &task, &[]).unwrap();

    let db1 = db.clone();
    let db2 = db.clone();
    let barrier = Arc::new(Barrier::new(2));
    let b1 = barrier.clone();
    let b2 = barrier.clone();
    let task_id_1 = created_task.id.clone();
    let task_id_2 = created_task.id.clone();

    let t1 = thread::spawn(move || {
        b1.wait();
        claim_task(&db1, &task_id_1, "agent-a").unwrap().is_some()
    });
    let t2 = thread::spawn(move || {
        b2.wait();
        claim_task(&db2, &task_id_2, "agent-b").unwrap().is_some()
    });

    let won_1 = t1.join().unwrap();
    let won_2 = t2.join().unwrap();
    assert!(won_1 ^ won_2);
}

#[test]
fn promote_sweep_moves_pending_to_ready() {
    let db_path = test_db_path();
    let db = init_db(&db_path).unwrap();
    let project = create_project(&db, "Promote", None, None, None).unwrap();

    let upstream = create_task(&db, &make_task(&project.id, "up", TaskStatus::Done), &[]).unwrap();
    let downstream = create_task(
        &db,
        &make_task(&project.id, "down", TaskStatus::Pending),
        &[],
    )
    .unwrap();

    add_dependency(
        &db,
        &upstream.id,
        &downstream.id,
        DependencyKind::Blocks,
        DependencyCondition::All,
        None,
    )
    .unwrap();

    let sweep = run_sweep(&db).unwrap();
    assert_eq!(sweep.promoted, 1);
    let updated = get_task(&db, &downstream.id).unwrap();
    assert_eq!(updated.status, TaskStatus::Ready);
}

#[test]
fn sweeper_reclaims_retries_and_rolls_up_composites() {
    let db_path = test_db_path();
    let db = init_db(&db_path).unwrap();
    let project = create_project(&db, "Sweep", None, None, None).unwrap();
    let now = now();

    let mut heartbeat_stale = make_task(&project.id, "stale heartbeat", TaskStatus::Running);
    heartbeat_stale.agent_id = Some("agent-x".to_string());
    heartbeat_stale.started_at = Some(now - Duration::seconds(120));
    heartbeat_stale.last_heartbeat = Some(now - Duration::seconds(120));
    heartbeat_stale.heartbeat_interval = 10;
    heartbeat_stale.timeout_seconds = Some(600);
    create_task(&db, &heartbeat_stale, &[]).unwrap();

    let mut timed_out = make_task(&project.id, "timeout", TaskStatus::Running);
    timed_out.started_at = Some(now - Duration::seconds(90));
    timed_out.last_heartbeat = Some(now - Duration::seconds(1));
    timed_out.heartbeat_interval = 100_000;
    timed_out.timeout_seconds = Some(10);
    create_task(&db, &timed_out, &[]).unwrap();

    let mut retryable = make_task(&project.id, "retry", TaskStatus::Failed);
    retryable.max_retries = 3;
    retryable.retry_count = 0;
    retryable.retry_backoff = RetryBackoff::Fixed;
    retryable.retry_delay_ms = 1;
    retryable.completed_at = Some(now - Duration::seconds(10));
    create_task(&db, &retryable, &[]).unwrap();

    let mut parent = make_task(&project.id, "parent", TaskStatus::Pending);
    parent.is_composite = true;
    let parent = create_task(&db, &parent, &[]).unwrap();

    let mut child1 = make_task(&project.id, "child1", TaskStatus::Done);
    child1.parent_task_id = Some(parent.id.clone());
    create_task(&db, &child1, &[]).unwrap();

    let mut child2 = make_task(&project.id, "child2", TaskStatus::Done);
    child2.parent_task_id = Some(parent.id.clone());
    create_task(&db, &child2, &[]).unwrap();

    let sweep = run_sweep(&db).unwrap();
    assert!(sweep.reclaimed >= 1);
    assert!(sweep.timed_out >= 1);
    assert!(sweep.retried >= 1);
    assert!(sweep.composites_completed >= 1);
}

#[test]
fn parse_sleep_duration_accepts_ms_and_suffixes() {
    assert_eq!(parse_sleep_duration_ms("3000").unwrap(), 3_000);
    assert_eq!(parse_sleep_duration_ms("250ms").unwrap(), 250);
    assert_eq!(parse_sleep_duration_ms("3s").unwrap(), 3_000);
    assert_eq!(parse_sleep_duration_ms("2m").unwrap(), 120_000);
    assert_eq!(parse_sleep_duration_ms("1h").unwrap(), 3_600_000);
    assert!(parse_sleep_duration_ms("0s").is_err());
    assert!(parse_sleep_duration_ms("abc").is_err());
    assert!(parse_sleep_duration_ms("9223372036854775807h").is_err());
}

#[test]
fn sleep_resume_round_trip_returns_state_ref() {
    let db_path = test_db_path();
    let db = init_db(&db_path).unwrap();
    let project = create_project(&db, "SleepResume", None, None, None).unwrap();

    let task = create_task(
        &db,
        &make_task(&project.id, "download model", TaskStatus::Ready),
        &[],
    )
    .unwrap();
    claim_task(&db, &task.id, "agent-a").unwrap().unwrap();
    start_task(&db, &task.id).unwrap();

    let sleep = sleep_task(
        &db,
        &task.id,
        3_000,
        Some(json!({"checkpoint": "hf-download-1"})),
        Some("waiting for download".to_string()),
    )
    .unwrap();
    assert_eq!(sleep.task.status, TaskStatus::Sleeping);
    assert_eq!(sleep.task.agent_id.as_deref(), Some("agent-a"));
    assert_eq!(
        sleep.state_ref,
        Some(json!({"checkpoint": "hf-download-1"}))
    );

    let stale = resume_task(&db, &task.id, "agent-a", Some("s-stale")).unwrap_err();
    assert!(stale.to_string().contains("sleep_id mismatch"));

    let early = resume_task(&db, &task.id, "agent-a", Some(&sleep.sleep_id)).unwrap_err();
    assert!(early.to_string().contains("not due until"));

    {
        let conn = db.lock().unwrap();
        conn.execute(
            "UPDATE tasks SET sleep_until = ?2 WHERE id = ?1",
            rusqlite::params![
                &task.id,
                crate::db::sleep_dt_to_sql(now() - Duration::seconds(1))
            ],
        )
        .unwrap();
    }

    let resumed = resume_task(&db, &task.id, "agent-a", Some(&sleep.sleep_id)).unwrap();
    assert_eq!(resumed.task.status, TaskStatus::Running);
    assert_eq!(resumed.task.agent_id.as_deref(), Some("agent-a"));
    assert_eq!(resumed.sleep_id.as_deref(), Some(sleep.sleep_id.as_str()));
    assert_eq!(
        resumed.state_ref,
        Some(json!({"checkpoint": "hf-download-1"}))
    );

    let fetched = get_task(&db, &task.id).unwrap();
    assert!(fetched.sleep_id.is_none());
    assert!(fetched.sleep_until.is_none());
    assert!(fetched.sleep_state_ref.is_none());
}

#[test]
fn process_launch_enters_sleeping_wait_and_blocks_early_resume() {
    let db_path = test_db_path();
    let db = init_db(&db_path).unwrap();
    let project = create_project(&db, "ProcessWait", None, None, None).unwrap();

    let task = create_task(
        &db,
        &make_task(&project.id, "download model", TaskStatus::Ready),
        &[],
    )
    .unwrap();
    claim_task(&db, &task.id, "agent-a").unwrap().unwrap();
    start_task(&db, &task.id).unwrap();

    let launched = launch_process_run(
        &db,
        ProcessLaunchRequest {
            task_id: task.id.clone(),
            agent_id: "agent-a".to_string(),
            command: vec!["sh".to_string(), "-c".to_string(), "echo ready".to_string()],
            cwd: None,
            hooks: vec![],
            callback_url: None,
            state_ref: Some(json!({"checkpoint": "download-1"})),
            idle_timeout_ms: None,
            timeout_ms: None,
        },
    )
    .unwrap();

    assert_eq!(launched.run.status, "created");
    assert_eq!(launched.run.task_id, task.id);
    assert_eq!(launched.wait.kind, "process");
    assert_eq!(launched.wait.status, "waiting");

    let sleeping = get_task(&db, &task.id).unwrap();
    assert_eq!(sleeping.status, TaskStatus::Sleeping);
    assert_eq!(sleeping.agent_id.as_deref(), Some("agent-a"));
    assert_eq!(
        sleeping.sleep_id.as_deref(),
        Some(launched.wait.id.as_str())
    );
    assert_eq!(
        sleeping.sleep_state_ref.as_ref().unwrap()["run_id"],
        launched.run.id
    );

    let due = list_due_wakes(&db, Some(&project.id)).unwrap();
    assert!(due.is_empty());

    let early = resume_task(&db, &task.id, "agent-a", Some(&launched.wait.id)).unwrap_err();
    assert!(early.to_string().contains("not due until"));
}

#[test]
fn process_terminal_marks_wait_due_and_resume_returns_state() {
    let db_path = test_db_path();
    let db = init_db(&db_path).unwrap();
    let project = create_project(&db, "ProcessDone", None, None, None).unwrap();

    let task = create_task(
        &db,
        &make_task(&project.id, "download model", TaskStatus::Ready),
        &[],
    )
    .unwrap();
    claim_task(&db, &task.id, "agent-a").unwrap().unwrap();
    start_task(&db, &task.id).unwrap();

    let launched = launch_process_run(
        &db,
        ProcessLaunchRequest {
            task_id: task.id.clone(),
            agent_id: "agent-a".to_string(),
            command: vec!["sh".to_string(), "-c".to_string(), "echo done".to_string()],
            cwd: None,
            hooks: vec![],
            callback_url: None,
            state_ref: Some(json!({"checkpoint": "download-2"})),
            idle_timeout_ms: None,
            timeout_ms: None,
        },
    )
    .unwrap();

    mark_process_started(
        &db,
        &launched.run.id,
        123,
        456,
        "/tmp/stdout",
        "/tmp/stderr",
    )
    .unwrap();
    let terminal =
        mark_process_terminal(&db, &launched.run.id, "succeeded", Some(0), None, "exit").unwrap();
    assert_eq!(terminal.status, "succeeded");

    let due = list_due_wakes(&db, Some(&project.id)).unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].task_id, task.id);
    assert_eq!(due[0].sleep_id, launched.wait.id);
    assert_eq!(
        due[0].state_ref.as_ref().unwrap()["state_ref"]["checkpoint"],
        "download-2"
    );

    let resumed = resume_task(&db, &task.id, "agent-a", Some(&launched.wait.id)).unwrap();
    assert_eq!(resumed.task.status, TaskStatus::Running);
    assert_eq!(resumed.sleep_id.as_deref(), Some(launched.wait.id.as_str()));
    assert_eq!(
        resumed.state_ref.as_ref().unwrap()["state_ref"]["checkpoint"],
        "download-2"
    );
}

#[test]
fn process_hook_marks_wait_due_once() {
    let db_path = test_db_path();
    let db = init_db(&db_path).unwrap();
    let project = create_project(&db, "ProcessHook", None, None, None).unwrap();

    let task = create_task(
        &db,
        &make_task(&project.id, "download model", TaskStatus::Ready),
        &[],
    )
    .unwrap();
    claim_task(&db, &task.id, "agent-a").unwrap().unwrap();
    start_task(&db, &task.id).unwrap();

    let launched = launch_process_run(
        &db,
        ProcessLaunchRequest {
            task_id: task.id.clone(),
            agent_id: "agent-a".to_string(),
            command: vec!["sh".to_string(), "-c".to_string(), "echo READY".to_string()],
            cwd: None,
            hooks: vec![ProcessHookSpec {
                name: "model_ready".to_string(),
                stream: "stdout".to_string(),
                pattern: "READY".to_string(),
            }],
            callback_url: None,
            state_ref: None,
            idle_timeout_ms: None,
            timeout_ms: None,
        },
    )
    .unwrap();

    mark_process_started(
        &db,
        &launched.run.id,
        123,
        456,
        "/tmp/stdout",
        "/tmp/stderr",
    )
    .unwrap();
    mark_process_hook_matched(
        &db,
        &launched.run.id,
        "model_ready",
        "READY",
        "stdout",
        "READY",
    )
    .unwrap();
    mark_process_hook_matched(
        &db,
        &launched.run.id,
        "model_ready",
        "READY",
        "stdout",
        "READY",
    )
    .unwrap();

    let run = get_process_run(&db, &launched.run.id).unwrap();
    assert_eq!(run.hook_name.as_deref(), Some("model_ready"));

    let due = list_due_wakes(&db, Some(&project.id)).unwrap();
    assert_eq!(due.len(), 1);

    let wake_events = list_events(
        &db,
        EventFilters {
            project_id: Some(project.id),
            event_type: Some(EventType::TaskWakeDue),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(wake_events.len(), 1);
}

#[test]
fn process_due_wake_reports_actual_process_reason() {
    let db_path = test_db_path();
    let db = init_db(&db_path).unwrap();
    let project = create_project(&db, "ProcessReason", None, None, None).unwrap();

    let task = create_task(
        &db,
        &make_task(&project.id, "download model", TaskStatus::Ready),
        &[],
    )
    .unwrap();
    claim_task(&db, &task.id, "agent-a").unwrap().unwrap();
    start_task(&db, &task.id).unwrap();

    let launched = launch_process_run(
        &db,
        ProcessLaunchRequest {
            task_id: task.id.clone(),
            agent_id: "agent-a".to_string(),
            command: vec!["sh".to_string(), "-c".to_string(), "echo READY".to_string()],
            cwd: None,
            hooks: vec![ProcessHookSpec {
                name: "model_ready".to_string(),
                stream: "stdout".to_string(),
                pattern: "READY".to_string(),
            }],
            callback_url: None,
            state_ref: Some(json!({"checkpoint": "reason"})),
            idle_timeout_ms: None,
            timeout_ms: None,
        },
    )
    .unwrap();
    mark_process_started(
        &db,
        &launched.run.id,
        123,
        456,
        "/tmp/stdout",
        "/tmp/stderr",
    )
    .unwrap();
    mark_process_hook_matched(
        &db,
        &launched.run.id,
        "model_ready",
        "READY",
        "stdout",
        "READY",
    )
    .unwrap();

    let due = list_due_wakes(&db, Some(&project.id)).unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].reason.as_deref(), Some("process_hook"));

    let resumed = resume_task(&db, &task.id, "agent-a", Some(&launched.wait.id)).unwrap();
    assert_eq!(resumed.reason.as_deref(), Some("process_hook"));
}

#[test]
fn process_kill_can_be_queued_before_child_pid_exists() {
    let db_path = test_db_path();
    let db = init_db(&db_path).unwrap();
    let project = create_project(&db, "ProcessQueuedKill", None, None, None).unwrap();

    let task = create_task(
        &db,
        &make_task(&project.id, "queued kill", TaskStatus::Ready),
        &[],
    )
    .unwrap();
    claim_task(&db, &task.id, "agent-a").unwrap().unwrap();
    start_task(&db, &task.id).unwrap();

    let launched = launch_process_run(
        &db,
        ProcessLaunchRequest {
            task_id: task.id.clone(),
            agent_id: "agent-a".to_string(),
            command: vec!["sh".to_string(), "-c".to_string(), "sleep 10".to_string()],
            cwd: None,
            hooks: vec![],
            callback_url: None,
            state_ref: None,
            idle_timeout_ms: None,
            timeout_ms: None,
        },
    )
    .unwrap();

    let queued = request_process_kill(&db, &launched.run.id).unwrap();
    assert_eq!(queued.status, "kill_requested");
    assert!(queued.pid.is_none());

    let started = mark_process_started(
        &db,
        &launched.run.id,
        999_999_991,
        999_999_992,
        "/tmp/stdout",
        "/tmp/stderr",
    )
    .unwrap();
    assert_eq!(started.status, "kill_requested");
    assert_eq!(started.pid, Some(999_999_991));
}

#[test]
fn stale_created_process_run_is_reaped_and_wakes_task() {
    let db_path = test_db_path();
    let db = init_db(&db_path).unwrap();
    let project = create_project(&db, "ProcessCreatedReap", None, None, None).unwrap();

    let task = create_task(
        &db,
        &make_task(&project.id, "created stale", TaskStatus::Ready),
        &[],
    )
    .unwrap();
    claim_task(&db, &task.id, "agent-a").unwrap().unwrap();
    start_task(&db, &task.id).unwrap();

    let launched = launch_process_run(
        &db,
        ProcessLaunchRequest {
            task_id: task.id.clone(),
            agent_id: "agent-a".to_string(),
            command: vec!["sh".to_string(), "-c".to_string(), "sleep 10".to_string()],
            cwd: None,
            hooks: vec![],
            callback_url: None,
            state_ref: Some(json!({"case": "created"})),
            idle_timeout_ms: None,
            timeout_ms: None,
        },
    )
    .unwrap();

    let old = crate::db::sleep_dt_to_sql(now() - Duration::seconds(60));
    db.lock()
        .unwrap()
        .execute(
            "UPDATE process_runs SET created_at = ?2, updated_at = ?2 WHERE id = ?1",
            rusqlite::params![&launched.run.id, old],
        )
        .unwrap();

    let reaped = reap_stale_process_runs(&db, Duration::milliseconds(1)).unwrap();
    assert_eq!(reaped, 1);
    let run = get_process_run(&db, &launched.run.id).unwrap();
    assert_eq!(run.status, "stuck");
    assert_eq!(run.terminal_reason.as_deref(), Some("runner_missing"));

    let due = list_due_wakes(&db, Some(&project.id)).unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].sleep_id, launched.wait.id);
    assert_eq!(due[0].reason.as_deref(), Some("runner_missing"));
}

#[test]
fn stale_running_process_run_is_reaped_and_wakes_task() {
    let db_path = test_db_path();
    let db = init_db(&db_path).unwrap();
    let project = create_project(&db, "ProcessRunningReap", None, None, None).unwrap();

    let task = create_task(
        &db,
        &make_task(&project.id, "running stale", TaskStatus::Ready),
        &[],
    )
    .unwrap();
    claim_task(&db, &task.id, "agent-a").unwrap().unwrap();
    start_task(&db, &task.id).unwrap();

    let launched = launch_process_run(
        &db,
        ProcessLaunchRequest {
            task_id: task.id.clone(),
            agent_id: "agent-a".to_string(),
            command: vec!["sh".to_string(), "-c".to_string(), "sleep 10".to_string()],
            cwd: None,
            hooks: vec![],
            callback_url: None,
            state_ref: Some(json!({"case": "running"})),
            idle_timeout_ms: None,
            timeout_ms: None,
        },
    )
    .unwrap();
    mark_process_started(
        &db,
        &launched.run.id,
        999_999_981,
        999_999_982,
        "/tmp/stdout",
        "/tmp/stderr",
    )
    .unwrap();

    let old = crate::db::sleep_dt_to_sql(now() - Duration::seconds(60));
    db.lock()
        .unwrap()
        .execute(
            "UPDATE process_runs SET last_heartbeat_at = ?2, updated_at = ?2 WHERE id = ?1",
            rusqlite::params![&launched.run.id, old],
        )
        .unwrap();

    let reaped = reap_stale_process_runs(&db, Duration::milliseconds(1)).unwrap();
    assert_eq!(reaped, 1);
    let run = get_process_run(&db, &launched.run.id).unwrap();
    assert_eq!(run.status, "stuck");
    assert_eq!(run.terminal_reason.as_deref(), Some("observer_lost"));

    let due = list_due_wakes(&db, Some(&project.id)).unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].sleep_id, launched.wait.id);
    assert_eq!(due[0].reason.as_deref(), Some("observer_lost"));
}

#[test]
fn cancelling_task_requests_owned_process_kill() {
    let db_path = test_db_path();
    let db = init_db(&db_path).unwrap();
    let project = create_project(&db, "ProcessCancelKill", None, None, None).unwrap();

    let task = create_task(
        &db,
        &make_task(&project.id, "cancel kills", TaskStatus::Ready),
        &[],
    )
    .unwrap();
    claim_task(&db, &task.id, "agent-a").unwrap().unwrap();
    start_task(&db, &task.id).unwrap();

    let launched = launch_process_run(
        &db,
        ProcessLaunchRequest {
            task_id: task.id.clone(),
            agent_id: "agent-a".to_string(),
            command: vec!["sh".to_string(), "-c".to_string(), "sleep 10".to_string()],
            cwd: None,
            hooks: vec![],
            callback_url: None,
            state_ref: None,
            idle_timeout_ms: None,
            timeout_ms: None,
        },
    )
    .unwrap();
    mark_process_started(
        &db,
        &launched.run.id,
        999_999_971,
        999_999_972,
        "/tmp/stdout",
        "/tmp/stderr",
    )
    .unwrap();

    let cancelled = cancel_task(&db, &task.id, false).unwrap();
    assert_eq!(cancelled, 1);
    let run = get_process_run(&db, &launched.run.id).unwrap();
    assert_eq!(run.status, "kill_requested");
}

#[test]
fn process_logs_are_returned_as_bounded_tail() {
    let db_path = test_db_path();
    let db = init_db(&db_path).unwrap();
    let project = create_project(&db, "ProcessLogBound", None, None, None).unwrap();

    let task = create_task(
        &db,
        &make_task(&project.id, "log bound", TaskStatus::Ready),
        &[],
    )
    .unwrap();
    claim_task(&db, &task.id, "agent-a").unwrap().unwrap();
    start_task(&db, &task.id).unwrap();

    let launched = launch_process_run(
        &db,
        ProcessLaunchRequest {
            task_id: task.id.clone(),
            agent_id: "agent-a".to_string(),
            command: vec!["sh".to_string(), "-c".to_string(), "echo logs".to_string()],
            cwd: None,
            hooks: vec![],
            callback_url: None,
            state_ref: None,
            idle_timeout_ms: None,
            timeout_ms: None,
        },
    )
    .unwrap();
    let stdout_path = std::env::temp_dir().join(format!("taskgraph-log-{}.out", launched.run.id));
    let stderr_path = std::env::temp_dir().join(format!("taskgraph-log-{}.err", launched.run.id));
    let mut big = vec![b'a'; 300_000];
    big.extend_from_slice(b"tail-marker");
    std::fs::write(&stdout_path, big).unwrap();
    std::fs::write(&stderr_path, b"small stderr").unwrap();
    mark_process_started(
        &db,
        &launched.run.id,
        999_999_961,
        999_999_962,
        &stdout_path.to_string_lossy(),
        &stderr_path.to_string_lossy(),
    )
    .unwrap();

    let logs = get_process_logs(&db, &launched.run.id).unwrap();
    assert!(logs.stdout_truncated);
    assert!(!logs.stderr_truncated);
    assert!(logs.stdout.unwrap().contains("tail-marker"));
    assert_eq!(logs.max_bytes, 256 * 1024);
}

#[test]
fn concurrent_notification_dispatchers_lease_outbox_rows_once() {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration as StdDuration, Instant};

    let db_path = test_db_path();
    let db = init_db(&db_path).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let callback_url = format!("http://{}", listener.local_addr().unwrap());
    let request_count = Arc::new(AtomicUsize::new(0));
    let server_count = request_count.clone();
    let server = thread::spawn(move || {
        let deadline = Instant::now() + StdDuration::from_secs(2);
        while Instant::now() < deadline {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    server_count.fetch_add(1, Ordering::SeqCst);
                    let _ = stream.set_read_timeout(Some(StdDuration::from_millis(100)));
                    let mut buf = [0_u8; 4096];
                    let _ = stream.read(&mut buf);
                    thread::sleep(StdDuration::from_millis(250));
                    let _ =
                        stream.write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n");
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(StdDuration::from_millis(10));
                }
                Err(_) => break,
            }
        }
    });

    let project = create_project(&db, "OutboxLease", None, None, None).unwrap();
    let task = create_task(
        &db,
        &make_task(&project.id, "callback", TaskStatus::Ready),
        &[],
    )
    .unwrap();
    claim_task(&db, &task.id, "agent-a").unwrap().unwrap();
    start_task(&db, &task.id).unwrap();
    let launched = launch_process_run(
        &db,
        ProcessLaunchRequest {
            task_id: task.id.clone(),
            agent_id: "agent-a".to_string(),
            command: vec!["sh".to_string(), "-c".to_string(), "echo READY".to_string()],
            cwd: None,
            hooks: vec![ProcessHookSpec {
                name: "ready".to_string(),
                stream: "stdout".to_string(),
                pattern: "READY".to_string(),
            }],
            callback_url: Some(callback_url),
            state_ref: None,
            idle_timeout_ms: None,
            timeout_ms: None,
        },
    )
    .unwrap();
    mark_process_started(
        &db,
        &launched.run.id,
        999_999_951,
        999_999_952,
        "/tmp/stdout",
        "/tmp/stderr",
    )
    .unwrap();
    mark_process_hook_matched(&db, &launched.run.id, "ready", "READY", "stdout", "READY").unwrap();

    let barrier = Arc::new(Barrier::new(3));
    let db_path_a = db_path.clone();
    let db_path_b = db_path.clone();
    let barrier_a = barrier.clone();
    let barrier_b = barrier.clone();
    let dispatch_a = thread::spawn(move || {
        let db = init_db(&db_path_a).unwrap();
        barrier_a.wait();
        dispatch_due_notifications(&db, 16).unwrap()
    });
    let dispatch_b = thread::spawn(move || {
        let db = init_db(&db_path_b).unwrap();
        barrier_b.wait();
        dispatch_due_notifications(&db, 16).unwrap()
    });
    barrier.wait();
    let delivered = dispatch_a.join().unwrap() + dispatch_b.join().unwrap();
    server.join().unwrap();

    assert_eq!(delivered, 1);
    assert_eq!(request_count.load(Ordering::SeqCst), 1);
}

#[test]
fn sleep_uses_millisecond_precision_for_due_checks() {
    let db_path = test_db_path();
    let db = init_db(&db_path).unwrap();
    let project = create_project(&db, "SleepPrecision", None, None, None).unwrap();
    let task = create_task(
        &db,
        &make_task(&project.id, "precise", TaskStatus::Ready),
        &[],
    )
    .unwrap();
    claim_task(&db, &task.id, "agent-a").unwrap().unwrap();
    start_task(&db, &task.id).unwrap();

    let sleep = sleep_task(&db, &task.id, 3_000, None, None).unwrap();
    let fetched = get_task(&db, &task.id).unwrap();
    assert_eq!(fetched.sleep_until, Some(sleep.wake_at));

    let stored: String = db
        .lock()
        .unwrap()
        .query_row(
            "SELECT sleep_until FROM tasks WHERE id = ?1",
            rusqlite::params![&task.id],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        stored.contains('.'),
        "sleep_until should include milliseconds: {stored}"
    );
    assert_eq!(stored.len(), "YYYY-MM-DD HH:MM:SS.mmm".len());

    {
        let conn = db.lock().unwrap();
        conn.execute(
            "UPDATE tasks SET sleep_until = ?2 WHERE id = ?1",
            rusqlite::params![
                &task.id,
                crate::db::sleep_dt_to_sql(now() + Duration::milliseconds(250)),
            ],
        )
        .unwrap();
    }
    assert!(list_due_wakes(&db, Some(&project.id)).unwrap().is_empty());
}

#[test]
fn sleep_rejects_duration_that_cannot_fit_in_datetime() {
    let db_path = test_db_path();
    let db = init_db(&db_path).unwrap();
    let project = create_project(&db, "SleepOverflow", None, None, None).unwrap();
    let task = create_task(
        &db,
        &make_task(&project.id, "overflow", TaskStatus::Ready),
        &[],
    )
    .unwrap();
    claim_task(&db, &task.id, "agent-a").unwrap().unwrap();
    start_task(&db, &task.id).unwrap();

    let err = sleep_task(&db, &task.id, i64::MAX, None, None).unwrap_err();
    assert!(err.to_string().contains("duration is too large"));
}

#[test]
fn due_wake_parses_legacy_second_precision_sleep_rows() {
    let db_path = test_db_path();
    let db = init_db(&db_path).unwrap();
    let project = create_project(&db, "LegacySleep", None, None, None).unwrap();
    let task = create_task(
        &db,
        &make_task(&project.id, "legacy", TaskStatus::Ready),
        &[],
    )
    .unwrap();
    claim_task(&db, &task.id, "agent-a").unwrap().unwrap();
    start_task(&db, &task.id).unwrap();
    sleep_task(&db, &task.id, 60_000, Some(json!({"legacy": true})), None).unwrap();

    {
        let conn = db.lock().unwrap();
        conn.execute(
            "UPDATE tasks SET sleep_until = ?2 WHERE id = ?1",
            rusqlite::params![&task.id, crate::db::dt_to_sql(now() - Duration::seconds(1))],
        )
        .unwrap();
    }

    let due = list_due_wakes(&db, Some(&project.id)).unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].task_id, task.id);
    assert_eq!(due[0].state_ref, Some(json!({"legacy": true})));
}

#[test]
fn due_wake_emission_is_idempotent_and_sleeping_is_not_reclaimed() {
    let db_path = test_db_path();
    let db = init_db(&db_path).unwrap();
    let project = create_project(&db, "DueWake", None, None, None).unwrap();

    let task = create_task(&db, &make_task(&project.id, "wait", TaskStatus::Ready), &[]).unwrap();
    claim_task(&db, &task.id, "agent-a").unwrap().unwrap();
    start_task(&db, &task.id).unwrap();
    let sleep = sleep_task(&db, &task.id, 60_000, Some(json!({"state": "x"})), None).unwrap();

    {
        let conn = db.lock().unwrap();
        conn.execute(
            "UPDATE tasks SET sleep_until = ?2, last_heartbeat = ?3 WHERE id = ?1",
            rusqlite::params![
                &task.id,
                crate::db::sleep_dt_to_sql(now() - Duration::seconds(1)),
                crate::db::dt_to_sql(now() - Duration::seconds(600)),
            ],
        )
        .unwrap();
    }

    let sweep = run_sweep(&db).unwrap();
    assert_eq!(sweep.wakes_emitted, 1);
    assert_eq!(sweep.reclaimed, 0);
    let still_sleeping = get_task(&db, &task.id).unwrap();
    assert_eq!(still_sleeping.status, TaskStatus::Sleeping);
    assert_eq!(
        still_sleeping.sleep_id.as_deref(),
        Some(sleep.sleep_id.as_str())
    );
    assert!(still_sleeping.wake_emitted_at.is_some());

    let due = list_due_wakes(&db, Some(&project.id)).unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].task_id, task.id);
    assert_eq!(due[0].state_ref, Some(json!({"state": "x"})));

    let second_sweep = run_sweep(&db).unwrap();
    assert_eq!(second_sweep.wakes_emitted, 0);
    let events = list_events(
        &db,
        EventFilters {
            project_id: Some(project.id),
            task_id: Some(task.id),
            event_type: Some(EventType::TaskWakeDue),
            since: None,
        },
    )
    .unwrap();
    assert_eq!(events.len(), 1);
}

#[test]
fn lenient_done_from_ready() {
    let db_path = test_db_path();
    let db = init_db(&db_path).unwrap();
    let project = create_project(&db, "LenientReady", None, None, None).unwrap();

    let task = create_task(
        &db,
        &make_task(&project.id, "ready->done", TaskStatus::Ready),
        &[],
    )
    .unwrap();
    let completed = complete_task(&db, &task.id, None).unwrap();

    assert_eq!(completed.status, TaskStatus::Done);
    assert!(completed.claimed_at.is_some());
    assert!(completed.started_at.is_some());
    assert!(completed.completed_at.is_some());
}

#[test]
fn lenient_done_from_claimed() {
    let db_path = test_db_path();
    let db = init_db(&db_path).unwrap();
    let project = create_project(&db, "LenientClaimed", None, None, None).unwrap();

    let task = create_task(
        &db,
        &make_task(&project.id, "claimed->done", TaskStatus::Ready),
        &[],
    )
    .unwrap();
    let claimed = claim_task(&db, &task.id, "agent-a").unwrap().unwrap();
    assert_eq!(claimed.status, TaskStatus::Claimed);
    assert!(claimed.claimed_at.is_some());
    assert!(claimed.started_at.is_none());

    let completed = complete_task(&db, &task.id, None).unwrap();
    assert_eq!(completed.status, TaskStatus::Done);
    assert!(completed.claimed_at.is_some());
    assert!(completed.started_at.is_some());
    assert!(completed.completed_at.is_some());
}

#[test]
fn lenient_done_from_running() {
    let db_path = test_db_path();
    let db = init_db(&db_path).unwrap();
    let project = create_project(&db, "LenientRunning", None, None, None).unwrap();

    let task = create_task(
        &db,
        &make_task(&project.id, "running->done", TaskStatus::Ready),
        &[],
    )
    .unwrap();
    claim_task(&db, &task.id, "agent-a").unwrap().unwrap();
    let running = start_task(&db, &task.id).unwrap();
    assert_eq!(running.status, TaskStatus::Running);

    let completed = complete_task(&db, &task.id, None).unwrap();
    assert_eq!(completed.status, TaskStatus::Done);
    assert!(completed.claimed_at.is_some());
    assert!(completed.started_at.is_some());
    assert!(completed.completed_at.is_some());
}

#[test]
fn done_rejects_pending() {
    let db_path = test_db_path();
    let db = init_db(&db_path).unwrap();
    let project = create_project(&db, "LenientPending", None, None, None).unwrap();

    let task = create_task(
        &db,
        &make_task(&project.id, "pending cannot complete", TaskStatus::Pending),
        &[],
    )
    .unwrap();

    let err = complete_task(&db, &task.id, None).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("is 'pending'"));
    assert!(msg.contains("ready/claimed/running"));
}
