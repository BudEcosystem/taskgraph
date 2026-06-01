use crate::db::{
    dispatch_due_notifications, get_process_run, init_db, list_process_hooks,
    mark_process_heartbeat, mark_process_hook_matched, mark_process_output, mark_process_started,
    mark_process_terminal, process_log_dir,
};
use anyhow::{anyhow, Result};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Write};
#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug)]
struct OutputMessage {
    stream: &'static str,
    text: String,
}

pub fn run_process_runner(db_path: &str, run_id: &str) -> Result<()> {
    let db = init_db(db_path)?;
    let run = get_process_run(&db, run_id)?;
    if run.command.is_empty() {
        return Err(anyhow!("process run {run_id} has an empty command"));
    }

    let log_dir = process_log_dir(db_path, run_id);
    fs::create_dir_all(&log_dir)?;
    let stdout_path = log_dir.join("stdout.log");
    let stderr_path = log_dir.join("stderr.log");

    let mut command = Command::new(&run.command[0]);
    command.args(&run.command[1..]);
    if let Some(cwd) = &run.cwd {
        command.current_dir(cwd);
    }
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(err) => {
            let _ = mark_process_terminal(
                &db,
                run_id,
                "failed",
                None,
                None,
                &format!("launch_error:{err}"),
            );
            let _ = dispatch_due_notifications(&db, 16);
            return Err(err.into());
        }
    };

    mark_process_started(
        &db,
        run_id,
        i64::from(child.id()),
        i64::from(std::process::id()),
        &stdout_path.to_string_lossy(),
        &stderr_path.to_string_lossy(),
    )?;

    let hooks = list_process_hooks(&db, run_id)?;
    let (tx, rx) = mpsc::channel::<OutputMessage>();
    if let Some(stdout) = child.stdout.take() {
        spawn_reader("stdout", stdout, stdout_path, tx.clone());
    }
    if let Some(stderr) = child.stderr.take() {
        spawn_reader("stderr", stderr, stderr_path, tx.clone());
    }
    drop(tx);

    let started = Instant::now();
    let mut last_output = Instant::now();
    let mut last_heartbeat = Instant::now();
    let poll_interval = Duration::from_millis(100);

    loop {
        while let Ok(message) = rx.try_recv() {
            last_output = Instant::now();
            mark_process_output(&db, run_id)?;
            for hook in &hooks {
                if (hook.stream == "any" || hook.stream == message.stream)
                    && message.text.contains(&hook.pattern)
                {
                    mark_process_hook_matched(
                        &db,
                        run_id,
                        &hook.name,
                        &hook.pattern,
                        message.stream,
                        message.text.trim_end(),
                    )?;
                    let _ = dispatch_due_notifications(&db, 16);
                    break;
                }
            }
        }

        if last_heartbeat.elapsed() >= Duration::from_secs(1) {
            mark_process_heartbeat(&db, run_id)?;
            last_heartbeat = Instant::now();
        }

        let current = get_process_run(&db, run_id)?;
        if current.status == "kill_requested" {
            let _ = child.kill();
        }

        if let Some(timeout_ms) = current.timeout_ms {
            if started.elapsed() >= Duration::from_millis(timeout_ms as u64) {
                let _ = child.kill();
                let _ = child.wait();
                mark_process_terminal(&db, run_id, "stuck", None, None, "timeout")?;
                let _ = dispatch_due_notifications(&db, 16);
                return Ok(());
            }
        }

        if let Some(idle_timeout_ms) = current.idle_timeout_ms {
            if last_output.elapsed() >= Duration::from_millis(idle_timeout_ms as u64) {
                let _ = child.kill();
                let _ = child.wait();
                mark_process_terminal(&db, run_id, "stuck", None, None, "idle_timeout")?;
                let _ = dispatch_due_notifications(&db, 16);
                return Ok(());
            }
        }

        if let Some(status) = child.try_wait()? {
            while let Ok(message) = rx.try_recv() {
                mark_process_output(&db, run_id)?;
                for hook in &hooks {
                    if (hook.stream == "any" || hook.stream == message.stream)
                        && message.text.contains(&hook.pattern)
                    {
                        mark_process_hook_matched(
                            &db,
                            run_id,
                            &hook.name,
                            &hook.pattern,
                            message.stream,
                            message.text.trim_end(),
                        )?;
                        break;
                    }
                }
            }

            let exit_code = status.code();
            let exit_signal = exit_signal(&status);
            let final_status = if exit_signal.is_some() || current.status == "kill_requested" {
                "killed"
            } else if exit_code == Some(0) {
                "succeeded"
            } else {
                "failed"
            };
            let reason = if exit_signal.is_some() {
                "signal"
            } else {
                "exit"
            };
            mark_process_terminal(&db, run_id, final_status, exit_code, exit_signal, reason)?;
            let _ = dispatch_due_notifications(&db, 16);
            return Ok(());
        }

        thread::sleep(poll_interval);
    }
}

fn spawn_reader<R>(
    stream: &'static str,
    reader: R,
    path: impl Into<std::path::PathBuf>,
    tx: mpsc::Sender<OutputMessage>,
) where
    R: Read + Send + 'static,
{
    let path = path.into();
    thread::spawn(move || {
        let mut output = match File::create(path) {
            Ok(file) => file,
            Err(_) => return,
        };
        let mut reader = BufReader::new(reader);
        let mut buf = Vec::new();
        loop {
            buf.clear();
            match reader.read_until(b'\n', &mut buf) {
                Ok(0) => break,
                Ok(_) => {
                    let _ = output.write_all(&buf);
                    let text = String::from_utf8_lossy(&buf).to_string();
                    let _ = tx.send(OutputMessage { stream, text });
                }
                Err(_) => break,
            }
        }
    });
}

fn exit_signal(status: &std::process::ExitStatus) -> Option<i32> {
    #[cfg(unix)]
    {
        status.signal()
    }
    #[cfg(not(unix))]
    {
        let _ = status;
        None
    }
}
