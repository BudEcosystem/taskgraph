use crate::cli::{print_json, resolve_project_id};
use crate::db::{
    get_process_logs, get_process_run, launch_process_run, list_process_runs,
    parse_sleep_duration_ms, request_process_kill, spawn_process_runner, Database, ProcessHookSpec,
    ProcessLaunchRequest, ProcessRunFilters,
};
use anyhow::Result;
use clap::{Args, Subcommand};

#[derive(Args, Debug)]
#[command(about = "Launch and observe external processes for owned tasks")]
pub struct ProcessCommand {
    #[command(subcommand)]
    command: ProcessSubcommand,
}

#[derive(Subcommand, Debug)]
enum ProcessSubcommand {
    #[command(
        about = "Launch a process, observe it, and wake the owning agent on exit/hook/stuck"
    )]
    Launch(ProcessLaunchArgs),
    #[command(about = "Get process run status")]
    Get(ProcessGetArgs),
    #[command(about = "List process runs")]
    List(ProcessListArgs),
    #[command(about = "Read captured stdout/stderr logs")]
    Logs(ProcessGetArgs),
    #[command(about = "Request termination of a running process")]
    Kill(ProcessGetArgs),
}

#[derive(Args, Debug)]
struct ProcessLaunchArgs {
    #[arg(help = "Owned task ID to put into process wait")]
    task_id: String,
    #[arg(long, help = "Agent that owns the task")]
    agent: String,
    #[arg(long, help = "Working directory for the launched process")]
    cwd: Option<String>,
    #[arg(long, help = "HTTP callback URL to call on hook/exit/killed/stuck")]
    callback: Option<String>,
    #[arg(
        long = "state-ref",
        alias = "state",
        help = "Opaque JSON state reference returned when the task resumes"
    )]
    state_ref: Option<String>,
    #[arg(
        long = "hook",
        value_parser = parse_hook,
        help = "Hook as name=pattern, name=stdout:pattern, name=stderr:pattern, or name=any:pattern"
    )]
    hooks: Vec<ProcessHookSpec>,
    #[arg(
        long = "idle-timeout",
        help = "Kill and wake if no output is seen for this duration: 3000, 3s, 5m, 1h"
    )]
    idle_timeout: Option<String>,
    #[arg(
        long = "timeout",
        help = "Kill and wake if total runtime exceeds this duration: 3000, 3s, 5m, 1h"
    )]
    timeout: Option<String>,
    #[arg(last = true, required = true, help = "Command to launch after --")]
    command: Vec<String>,
}

#[derive(Args, Debug)]
struct ProcessGetArgs {
    #[arg(help = "Process run ID")]
    run_id: String,
}

#[derive(Args, Debug)]
struct ProcessListArgs {
    #[arg(long, help = "Project ID (uses default if set)")]
    project: Option<String>,
    #[arg(long, help = "Task ID filter")]
    task: Option<String>,
    #[arg(long, help = "Run status filter")]
    status: Option<String>,
}

pub fn run(db: &Database, command: ProcessCommand, json: bool, compact: bool) -> Result<()> {
    match command.command {
        ProcessSubcommand::Launch(args) => launch_cmd(db, args, json, compact),
        ProcessSubcommand::Get(args) => get_cmd(db, &args.run_id, json, compact),
        ProcessSubcommand::List(args) => list_cmd(db, args, json),
        ProcessSubcommand::Logs(args) => logs_cmd(db, &args.run_id, json),
        ProcessSubcommand::Kill(args) => kill_cmd(db, &args.run_id, json),
    }
}

fn launch_cmd(db: &Database, args: ProcessLaunchArgs, json: bool, compact: bool) -> Result<()> {
    let state_ref = parse_state_ref(args.state_ref)?;
    let idle_timeout_ms = args
        .idle_timeout
        .as_deref()
        .map(parse_sleep_duration_ms)
        .transpose()?;
    let timeout_ms = args
        .timeout
        .as_deref()
        .map(parse_sleep_duration_ms)
        .transpose()?;
    let result = launch_process_run(
        db,
        ProcessLaunchRequest {
            task_id: args.task_id,
            agent_id: args.agent,
            command: args.command,
            cwd: args.cwd,
            hooks: args.hooks,
            callback_url: args.callback,
            state_ref,
            idle_timeout_ms,
            timeout_ms,
        },
    )?;
    spawn_process_runner(db, &result.run.id)?;

    if json {
        if compact {
            print_json(&serde_json::json!({
                "run_id": result.run.id,
                "task_id": result.run.task_id,
                "wait_id": result.wait.id,
                "status": result.run.status,
                "sleep_id": result.wait.id,
            }))?;
        } else {
            print_json(&result)?;
        }
    } else {
        println!(
            "watching process {} for task {} (wait_id={}, sleep_id={})",
            result.run.id, result.run.task_id, result.wait.id, result.wait.id
        );
        if !compact {
            eprintln!(
                "resume when due: taskgraph resume {} --agent {} --sleep-id {}",
                result.run.task_id, result.run.agent_id, result.wait.id
            );
        }
    }
    Ok(())
}

fn get_cmd(db: &Database, run_id: &str, json: bool, compact: bool) -> Result<()> {
    let run = get_process_run(db, run_id)?;
    if json {
        if compact {
            print_json(&serde_json::json!({
                "id": run.id,
                "task_id": run.task_id,
                "wait_id": run.wait_id,
                "status": run.status,
                "exit_code": run.exit_code,
                "exit_signal": run.exit_signal,
                "terminal_reason": run.terminal_reason,
                "hook_name": run.hook_name,
            }))?;
        } else {
            print_json(&run)?;
        }
    } else {
        println!(
            "{} {} task={} agent={} wait={}",
            run.id, run.status, run.task_id, run.agent_id, run.wait_id
        );
        if let Some(code) = run.exit_code {
            println!("exit_code: {code}");
        }
        if let Some(signal) = run.exit_signal {
            println!("exit_signal: {signal}");
        }
        if let Some(reason) = run.terminal_reason {
            println!("reason: {reason}");
        }
        if let Some(hook) = run.hook_name {
            println!("hook: {hook}");
        }
    }
    Ok(())
}

fn list_cmd(db: &Database, args: ProcessListArgs, json: bool) -> Result<()> {
    let project_id = match args.project {
        Some(project) => Some(project),
        None => resolve_project_id(db, None).ok(),
    };
    let runs = list_process_runs(
        db,
        ProcessRunFilters {
            project_id,
            task_id: args.task,
            status: args.status,
        },
    )?;
    if json {
        print_json(&runs)?;
    } else if runs.is_empty() {
        println!("no process runs");
    } else {
        for run in runs {
            println!(
                "{} {} task={} wait={} agent={}",
                run.id, run.status, run.task_id, run.wait_id, run.agent_id
            );
        }
    }
    Ok(())
}

fn logs_cmd(db: &Database, run_id: &str, json: bool) -> Result<()> {
    let logs = get_process_logs(db, run_id)?;
    if json {
        print_json(&logs)?;
    } else {
        if let Some(stdout) = logs.stdout {
            print!("{stdout}");
        }
        if let Some(stderr) = logs.stderr {
            eprint!("{stderr}");
        }
    }
    Ok(())
}

fn kill_cmd(db: &Database, run_id: &str, json: bool) -> Result<()> {
    let run = request_process_kill(db, run_id)?;
    if json {
        print_json(&run)?;
    } else {
        println!("kill requested for {}", run.id);
    }
    Ok(())
}

fn parse_state_ref(raw: Option<String>) -> Result<Option<serde_json::Value>> {
    match raw {
        Some(text) => match serde_json::from_str(&text) {
            Ok(value) => Ok(Some(value)),
            Err(_) => Ok(Some(serde_json::Value::String(text))),
        },
        None => Ok(None),
    }
}

fn parse_hook(raw: &str) -> std::result::Result<ProcessHookSpec, String> {
    let (name, signal) = raw
        .split_once('=')
        .ok_or_else(|| "hook must be name=pattern or name=stream:pattern".to_string())?;
    if name.trim().is_empty() {
        return Err("hook name cannot be empty".to_string());
    }
    let (stream, pattern) = if let Some(rest) = signal.strip_prefix("stdout:") {
        ("stdout", rest)
    } else if let Some(rest) = signal.strip_prefix("stderr:") {
        ("stderr", rest)
    } else if let Some(rest) = signal.strip_prefix("any:") {
        ("any", rest)
    } else {
        ("any", signal)
    };
    if pattern.is_empty() {
        return Err("hook pattern cannot be empty".to_string());
    }
    Ok(ProcessHookSpec {
        name: name.trim().to_string(),
        stream: stream.to_string(),
        pattern: pattern.to_string(),
    })
}
