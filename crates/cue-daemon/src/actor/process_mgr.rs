//! ProcessManager actor — OS child process lifecycle.
//!
//! Spawns real child processes via `tokio::process::Command`, reads their
//! stdout/stderr into a [`RingBuffer`], writes a persistent log file, and
//! publishes output chunks + state-change events.

use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

use cue_core::JobId;
use cue_core::ipc::{EventPayload, Stream as OutputStream};
use cue_core::job::JobStatus;
use cue_core::pipeline::{JobPlan, command_prefers_foreground};
use cue_core::scope::EnvSnapshot;

use super::{
    ActorSystem, EventBusMsg, GatewayMsg, OutputSnapshot, ProcessMgrMsg, SchedulerMsg,
    ScopeStoreMsg, StderrSnapshot,
};
use crate::ring_buffer::RingBuffer;
use crate::runtime_env::effective_snapshot;
use crate::word_expansion::expand_command_line;

// ── Per-child bookkeeping ──

struct ProcessEntry {
    job_id: JobId,
    status: JobStatus,
    /// Handle for the background reader/waiter task.
    _reader_handle: tokio::task::JoinHandle<()>,
    /// Send on this channel to request a kill.
    kill_tx: mpsc::Sender<()>,
    /// Shared ring buffer holding the latest output bytes for live-tail queries.
    ring_buffer: Arc<Mutex<RingBuffer>>,
    /// Separate stderr ring buffer.  `None` in PTY mode (streams are merged).
    stderr_ring: Option<Arc<Mutex<RingBuffer>>>,
    /// Job stdin, either the PTY master or a pipe to the first process.
    input: Option<JobInput>,
    /// PTY master fd used for resize ioctls.
    resize: Option<Arc<std::fs::File>>,
    /// Which client, if any, owns the foreground stream.
    fg_owner: Arc<Mutex<Option<u64>>>,
}

#[derive(Clone)]
enum JobInput {
    Pty(Arc<AsyncFd<std::fs::File>>),
    Pipe(Arc<tokio::sync::Mutex<tokio::process::ChildStdin>>),
}

const DEFAULT_PTY_COLS: u16 = 80;
const DEFAULT_PTY_ROWS: u16 = 24;

// ── Actor entry point ──

struct NativePipelineSpawn {
    children: Vec<tokio::process::Child>,
    input: Option<JobInput>,
    stdout_sources: Vec<tokio::process::ChildStdout>,
    stderr_sources: Vec<tokio::process::ChildStderr>,
}

#[derive(Clone, Copy)]
enum PipelineStreamKind {
    Stdout,
    Stderr,
}

enum PipelineReaderMsg {
    Chunk {
        kind: PipelineStreamKind,
        data: Vec<u8>,
    },
    Closed,
}

enum JobLocalBuiltin {
    Cd { path: String },
    EnvSet { assignments: Vec<String> },
}

/// Spawn the ProcessManager actor task.
pub fn spawn(mut rx: mpsc::Receiver<ProcessMgrMsg>, sys: ActorSystem) {
    tokio::spawn(async move {
        debug!("process_mgr: started");

        let mut children: HashMap<u32, ProcessEntry> = HashMap::new();

        // Internal channel for reader tasks to request cleanup.
        let (cleanup_tx, mut cleanup_rx) = mpsc::channel::<JobId>(super::ACTOR_CHANNEL_CAP);

        loop {
            tokio::select! {
                msg = rx.recv() => {
                    let Some(msg) = msg else { break; };
                    match msg {
                ProcessMgrMsg::SpawnJob {
                    job_id,
                    plan,
                    scope_hash,
                    cwd_override,
                    wrapper_enabled,
                    pty_enabled,
                } => {
                    info!(%job_id, plan = %job_plan_to_text(&plan), %scope_hash, "process_mgr: spawn");

                    // 1. Query ScopeStore for the environment snapshot.
                    let snapshot = {
                        let (tx, rx) = oneshot::channel();
                        if sys
                            .scope_store
                            .send(ScopeStoreMsg::GetScope {
                                hash: scope_hash,
                                reply: tx,
                            })
                            .await
                            .is_err()
                        {
                            error!(%job_id, "process_mgr: scope_store channel closed");
                            // Fail the job instead of continuing with the daemon environment.
                            emit_state_change(&sys, job_id, JobStatus::Pending, JobStatus::Failed).await;
                            let _ = sys.scheduler.send(SchedulerMsg::JobFinished { job_id, exit_code: -1 }).await;
                            continue;
                        }
                        match rx.await {
                            Ok(Some(scope)) => scope.snapshot,
                            Ok(None) => {
                                // Scope resolution failed, so the job cannot safely inherit env.
                                error!(%job_id, %scope_hash, "process_mgr: scope not found");
                                emit_state_change(&sys, job_id, JobStatus::Pending, JobStatus::Failed).await;
                                let _ = sys.scheduler.send(SchedulerMsg::JobFinished { job_id, exit_code: -1 }).await;
                                continue;
                            }
                            Err(_) => {
                                // Scope resolution failed, so the job cannot safely inherit env.
                                error!(%job_id, "process_mgr: scope_store reply dropped");
                                emit_state_change(&sys, job_id, JobStatus::Pending, JobStatus::Failed).await;
                                let _ = sys.scheduler.send(SchedulerMsg::JobFinished { job_id, exit_code: -1 }).await;
                                continue;
                            }
                        }
                    };

                    let effective_snapshot = snapshot.as_ref().map(effective_snapshot);
                    // Resolve effective cwd: explicit override wins, else scope cwd.
                    let effective_cwd = cwd_override.as_ref().or_else(|| {
                        effective_snapshot.as_ref().map(|s| &s.cwd)
                    });
                    if let Some(cwd) = effective_cwd
                        && !cwd.is_dir()
                    {
                        error!(
                            %job_id,
                            cwd = %cwd.display(),
                            "process_mgr: invalid cwd for job spawn"
                        );
                        emit_state_change(&sys, job_id, JobStatus::Pending, JobStatus::Failed)
                            .await;
                        let _ = sys
                            .scheduler
                            .send(SchedulerMsg::JobFinished {
                                job_id,
                                exit_code: -1,
                            })
                            .await;
                        continue;
                    }

                    clear_job_logs(job_id).await;

                    let entry = spawn_job_plan(
                        job_id,
                        &plan,
                        effective_snapshot.as_ref(),
                        cwd_override.as_ref(),
                        wrapper_enabled,
                        pty_enabled,
                        sys.clone(),
                        cleanup_tx.clone(),
                    )
                    .await;

                    match entry {
                        Ok(entry) => {
                            emit_state_change(&sys, job_id, JobStatus::Pending, JobStatus::Running)
                                .await;
                            children.insert(job_id.0, entry);
                        }
                        Err(()) => {
                            emit_state_change(&sys, job_id, JobStatus::Pending, JobStatus::Failed)
                                .await;
                            let _ = sys
                                .scheduler
                                .send(SchedulerMsg::JobFinished {
                                    job_id,
                                    exit_code: -1,
                                })
                                .await;
                        }
                    }
                }

                ProcessMgrMsg::KillJob { job_id } => {
                    info!(%job_id, "process_mgr: kill requested");
                    if let Some(entry) = children.get_mut(&job_id.0) {
                        if !entry.status.is_terminal() {
                            entry.status = JobStatus::Killed;
                            // Signal the reader task to perform the kill sequence.
                            let _ = entry.kill_tx.send(()).await;
                        }
                    } else {
                        warn!(%job_id, "process_mgr: kill — job not found");
                    }
                }

                // Expose ring-buffer contents for live-tail queries.
                ProcessMgrMsg::GetOutput { job_id, tail_bytes, reply } => {
                    let result = children.get(&job_id.0).map(|entry| {
                        let (data, truncated) = entry
                            .ring_buffer
                            .lock()
                            .unwrap()
                            .tail_with_truncation(tail_bytes);
                        OutputSnapshot { data, truncated }
                    });
                    let _ = reply.send(result);
                }

                ProcessMgrMsg::GetStderr { job_id, tail_bytes, reply } => {
                    let result = children.get(&job_id.0).map(|entry| match &entry.stderr_ring {
                        Some(ring) => {
                            let (data, truncated) =
                                ring.lock().unwrap().tail_with_truncation(tail_bytes);
                            StderrSnapshot {
                                pty_merged: false,
                                data,
                                truncated,
                            }
                        }
                        None => StderrSnapshot {
                            pty_merged: true,
                            data: Vec::new(),
                            truncated: false,
                        },
                    });
                    let _ = reply.send(result);
                }

                ProcessMgrMsg::SendJobInput { job_id, data, reply } => {
                    let input = children.get(&job_id.0).and_then(|entry| entry.input.clone());
                    let handled = if let Some(input) = input {
                        match write_job_input(&input, &data).await {
                            Ok(()) => Ok(()),
                            Err(error) => Err(format!("failed to write job input: {error}")),
                        }
                    } else {
                        Err(format!("job {job_id} does not accept stdin"))
                    };
                    let _ = reply.send(handled);
                }

                ProcessMgrMsg::AttachFg { client_id, job_id, reply } => {
                    let (result, snapshot) = if let Some(entry) = children.get_mut(&job_id.0) {
                        if entry.status != JobStatus::Running {
                            (Err(format!("job {job_id} is not running")), None)
                        } else if let Some(owner) = *entry.fg_owner.lock().unwrap()
                            && owner != client_id
                        {
                            (Err(format!("job {job_id} is already foreground-attached")), None)
                        } else if entry.resize.is_none() {
                            (Err(format!("job {job_id} does not support foreground attach")), None)
                        } else {
                            *entry.fg_owner.lock().unwrap() = Some(client_id);
                            (
                                Ok(()),
                                Some(
                                    entry
                                        .ring_buffer
                                        .lock()
                                        .unwrap()
                                        .tail(crate::ring_buffer::DEFAULT_CAPACITY),
                                ),
                            )
                        }
                    } else {
                        (Err(format!("job {job_id} not found")), None)
                    };
                    let attached = result.is_ok();
                    let _ = reply.send(result);
                    if attached
                        && let Some(snapshot) = snapshot
                        && !snapshot.is_empty()
                    {
                        let _ = sys
                            .gateway
                            .send(GatewayMsg::SendEvent {
                                client_id,
                                payload: EventPayload::FgOutput { data: snapshot },
                            })
                            .await;
                    }
                }

                ProcessMgrMsg::DetachFg { client_id, reason } => {
                    let mut detached_jobs = Vec::new();
                    for entry in children.values_mut() {
                        if *entry.fg_owner.lock().unwrap() == Some(client_id) {
                            *entry.fg_owner.lock().unwrap() = None;
                            detached_jobs.push(entry.job_id.to_string());
                        }
                    }
                    for job_id in detached_jobs {
                        let _ = sys
                            .gateway
                            .send(GatewayMsg::SendEvent {
                                client_id,
                                payload: EventPayload::FgExited {
                                    id: job_id,
                                    reason: reason.clone(),
                                },
                            })
                            .await;
                    }
                }

                ProcessMgrMsg::FgInput { client_id, data, reply } => {
                    let input = children
                        .values()
                        .find(|entry| *entry.fg_owner.lock().unwrap() == Some(client_id))
                        .and_then(|entry| entry.input.clone());
                    let handled = if let Some(input) = input {
                        match write_job_input(&input, &data).await {
                            Ok(()) => Ok(()),
                            Err(error) => Err(format!("failed to write fg input: {error}")),
                        }
                    } else {
                        Err("no foreground session attached".to_string())
                    };
                    let _ = reply.send(handled);
                }

                ProcessMgrMsg::FgResize { client_id, cols, rows, reply } => {
                    let resize = children
                        .values()
                        .find(|entry| *entry.fg_owner.lock().unwrap() == Some(client_id))
                        .map(|entry| entry.resize.clone());
                    let _ = reply.send(if let Some(Some(resize)) = resize {
                        set_winsize(resize.as_raw_fd(), cols, rows)
                            .map_err(|error| format!("failed to resize fg session: {error}"))
                    } else {
                        Err("no foreground session attached".into())
                    });
                }

                ProcessMgrMsg::Shutdown => {
                    debug!("process_mgr: shutting down — killing all children");
                    for entry in children.values() {
                        if !entry.status.is_terminal() {
                            // Non-blocking send keeps shutdown from stalling on a full channel.
                            let _ = entry.kill_tx.try_send(());
                        }
                    }
                    // Give children a moment to exit.
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    break;
                }
                    }
                }

                // Reader task finished; remove the stale entry.
                Some(job_id) = cleanup_rx.recv() => {
                    debug!(%job_id, "process_mgr: cleaning up finished child");
                    children.remove(&job_id.0);
                }
            }
        }

        debug!("process_mgr: stopped");
    });
}

// ── Helpers ──

/// Apply the scope's environment snapshot to a Command.
fn apply_env(cmd: &mut tokio::process::Command, snap: &EnvSnapshot) {
    // Clear inherited env and set from snapshot.
    cmd.env_clear();
    cmd.envs(snap.env.iter());
    cmd.current_dir(&snap.cwd);
}

fn set_nonblocking(fd: std::os::fd::RawFd) -> std::io::Result<()> {
    // SAFETY: fcntl operates on a valid fd owned by this process.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags == -1 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: fcntl operates on a valid fd owned by this process.
    let rc = unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
    if rc == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

fn set_winsize(fd: std::os::fd::RawFd, cols: u16, rows: u16) -> std::io::Result<()> {
    let winsize = libc::winsize {
        ws_row: rows,
        ws_col: cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    // SAFETY: ioctl operates on a valid tty/pty fd and a properly initialized winsize.
    let rc = unsafe { libc::ioctl(fd, libc::TIOCSWINSZ, &winsize) };
    if rc == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

async fn read_pty(fd: &AsyncFd<std::fs::File>, buf: &mut [u8]) -> std::io::Result<usize> {
    loop {
        let mut guard = fd.readable().await?;
        match guard.try_io(|inner| inner.get_ref().read(buf)) {
            Ok(result) => return result,
            Err(_would_block) => continue,
        }
    }
}

async fn write_pty(fd: &AsyncFd<std::fs::File>, data: &[u8]) -> std::io::Result<()> {
    let mut written = 0;
    while written < data.len() {
        let mut guard = fd.writable().await?;
        match guard.try_io(|inner| inner.get_ref().write(&data[written..])) {
            Ok(Ok(0)) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "pty write returned 0 bytes",
                ));
            }
            Ok(Ok(n)) => written += n,
            Ok(Err(error)) => return Err(error),
            Err(_would_block) => continue,
        }
    }
    Ok(())
}

async fn write_job_input(input: &JobInput, data: &[u8]) -> std::io::Result<()> {
    match input {
        JobInput::Pty(fd) => write_pty(fd, data).await,
        JobInput::Pipe(stdin) => {
            let mut stdin = stdin.lock().await;
            stdin.write_all(data).await?;
            stdin.flush().await
        }
    }
}

#[derive(Clone)]
struct ExpandedSegment {
    command_line: Vec<String>,
    program: String,
    args: Vec<String>,
    pipe_to_next: Option<cue_core::pipeline::PipeOp>,
}

fn expand_pipeline_segments(
    job_id: JobId,
    pipeline: &cue_core::pipeline::Pipeline,
    snapshot: Option<&EnvSnapshot>,
) -> Result<Vec<ExpandedSegment>, ()> {
    let mut expanded = Vec::with_capacity(pipeline.segments.len());
    for segment in &pipeline.segments {
        let command_line = expand_command_line(&segment.command, snapshot);
        let Some(program) = command_line
            .first()
            .cloned()
            .filter(|word| !word.is_empty())
        else {
            error!(
                %job_id,
                pipeline = ?segment.command,
                "process_mgr: command is empty"
            );
            return Err(());
        };
        let args = command_line.get(1..).unwrap_or(&[]).to_vec();
        expanded.push(ExpandedSegment {
            command_line,
            program,
            args,
            pipe_to_next: segment.pipe_to_next,
        });
    }
    if expanded.is_empty() {
        error!(%job_id, "process_mgr: pipeline is empty");
        return Err(());
    }
    Ok(expanded)
}

fn configure_command(
    cmd: &mut tokio::process::Command,
    snap: Option<&EnvSnapshot>,
    cwd_override: Option<&std::path::PathBuf>,
) {
    if let Some(snap) = snap {
        apply_env(cmd, snap);
    }
    if let Some(cwd) = cwd_override {
        cmd.current_dir(cwd);
    }
    cmd.kill_on_drop(true);
}

fn log_spawn_failure(
    job_id: JobId,
    program: &str,
    args: &[String],
    snapshot: Option<&EnvSnapshot>,
    error: &std::io::Error,
) {
    error!(
        %job_id,
        program,
        args = ?args,
        cwd = %snapshot
            .map(|snap| snap.cwd.display().to_string())
            .unwrap_or_else(|| "<daemon cwd>".into()),
        path = ?snapshot.and_then(|snap| snap.env.get("PATH").cloned()),
        err = %error,
        "process_mgr: spawn failed"
    );
}

fn pipeline_has_job_local_builtin(pipeline: &cue_core::pipeline::Pipeline) -> bool {
    pipeline.segments.len() == 1
        && detect_job_local_builtin(&pipeline.segments[0].command).is_some()
}

fn detect_job_local_builtin(words: &[String]) -> Option<JobLocalBuiltin> {
    let command = words.first()?.as_str();
    match command {
        "cd" => Some(JobLocalBuiltin::Cd {
            path: words.get(1).cloned().unwrap_or_else(|| "~".into()),
        }),
        "env" if words.get(1).map(String::as_str) == Some("set") => Some(JobLocalBuiltin::EnvSet {
            assignments: words.get(2..).unwrap_or(&[]).to_vec(),
        }),
        _ => None,
    }
}

fn job_plan_to_text(plan: &JobPlan) -> String {
    match plan {
        JobPlan::Pipeline(pipeline) => pipeline_to_text(pipeline),
        JobPlan::And { left, right } => {
            format!("{} && {}", job_plan_to_text(left), job_plan_to_text(right))
        }
        JobPlan::Or { left, right } => {
            format!("{} || {}", job_plan_to_text(left), job_plan_to_text(right))
        }
    }
}

fn pipeline_to_text(pipeline: &cue_core::pipeline::Pipeline) -> String {
    pipeline
        .segments
        .iter()
        .map(|segment| {
            let cmd = segment.command.join(" ");
            match segment.pipe_to_next {
                Some(cue_core::pipeline::PipeOp::Stdout) => format!("{cmd} |>"),
                Some(cue_core::pipeline::PipeOp::StdoutStderr) => format!("{cmd} |&>"),
                Some(cue_core::pipeline::PipeOp::StderrOnly) => format!("{cmd} |!>"),
                None => cmd,
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[allow(clippy::too_many_arguments)]
async fn spawn_job_plan(
    job_id: JobId,
    plan: &JobPlan,
    snapshot: Option<&EnvSnapshot>,
    cwd_override: Option<&std::path::PathBuf>,
    wrapper_enabled: bool,
    pty_enabled: bool,
    sys: ActorSystem,
    cleanup_tx: mpsc::Sender<JobId>,
) -> Result<ProcessEntry, ()> {
    match plan {
        JobPlan::Pipeline(pipeline) if pipeline_has_job_local_builtin(pipeline) => {
            spawn_logical_job(
                job_id,
                plan.clone(),
                snapshot.cloned(),
                cwd_override.cloned(),
                wrapper_enabled,
                sys,
                cleanup_tx,
            )
            .await
        }
        JobPlan::Pipeline(pipeline) if pipeline.segments.len() == 1 && pty_enabled => {
            spawn_single_pty_job(
                job_id,
                pipeline,
                snapshot,
                cwd_override,
                wrapper_enabled,
                sys,
                cleanup_tx,
            )
            .await
        }
        // Single-segment without PTY → spawn with pipes.
        JobPlan::Pipeline(pipeline) if pipeline.segments.len() == 1 => {
            spawn_single_pipe_job(
                job_id,
                pipeline,
                snapshot,
                cwd_override,
                wrapper_enabled,
                sys,
                cleanup_tx,
            )
            .await
        }
        JobPlan::Pipeline(pipeline) => {
            spawn_native_pipeline_job(
                job_id,
                pipeline,
                snapshot,
                cwd_override,
                wrapper_enabled,
                sys,
                cleanup_tx,
            )
            .await
        }
        JobPlan::And { .. } | JobPlan::Or { .. } => {
            spawn_logical_job(
                job_id,
                plan.clone(),
                snapshot.cloned(),
                cwd_override.cloned(),
                wrapper_enabled,
                sys,
                cleanup_tx,
            )
            .await
        }
    }
}

/// Spawn a single-segment job with pipes (stdout/stderr piped, no PTY).
/// Used when `pty=false` is specified — the child cannot detect a terminal.
async fn spawn_single_pipe_job(
    job_id: JobId,
    pipeline: &cue_core::pipeline::Pipeline,
    snapshot: Option<&EnvSnapshot>,
    cwd_override: Option<&std::path::PathBuf>,
    wrapper_enabled: bool,
    sys: ActorSystem,
    cleanup_tx: mpsc::Sender<JobId>,
) -> Result<ProcessEntry, ()> {
    use tokio::io::AsyncReadExt;

    let segments = expand_pipeline_segments(job_id, pipeline, snapshot)?;
    let segment = &segments[0];
    let (program, args) = wrap_segment_if_enabled(&sys, wrapper_enabled, segment);

    let mut cmd = tokio::process::Command::new(&program);
    if !args.is_empty() {
        cmd.args(&args);
    }
    if let Some(snap) = snapshot {
        apply_env(&mut cmd, snap);
    }
    if let Some(ref cwd) = cwd_override {
        cmd.current_dir(cwd);
    }
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    cmd.kill_on_drop(true);

    let mut child = cmd.spawn().map_err(|e| {
        error!(%job_id, program = %program, err = %e, "process_mgr: pipe spawn failed");
    })?;
    let pid = child.id().unwrap_or(0);
    info!(%job_id, pid, "process_mgr: pipe child spawned");

    let mut stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();

    let ring_buffer = Arc::new(Mutex::new(RingBuffer::default()));
    let stderr_ring = Arc::new(Mutex::new(RingBuffer::default()));
    let fg_owner = Arc::new(Mutex::new(None));
    let sys_clone = sys.clone();
    let ring_clone = ring_buffer.clone();
    let stderr_clone = stderr_ring.clone();
    let fg_clone = fg_owner.clone();
    let cleanup_tx_clone = cleanup_tx.clone();

    // Read stdout and stderr concurrently, wait for exit.
    let log_file = open_output_log(job_id).await;
    let reader_handle = tokio::spawn(async move {
        let log = Arc::new(Mutex::new(log_file));
        let log_clone = log.clone();
        let sys_emit = sys_clone.clone();

        let stdout_task = tokio::spawn(async move {
            let mut buf = vec![0u8; 8192];
            loop {
                match stdout.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        let chunk = buf[..n].to_vec();
                        ring_clone.lock().unwrap().push(&chunk);
                        if let Ok(mut guard) = log_clone.lock()
                            && let Some(f) = guard.as_mut()
                        {
                            use std::io::Write;
                            let _ = f.write_all(&chunk);
                        }
                        emit_output(&sys_emit, job_id, OutputStream::Stdout, &chunk).await;
                    }
                    Err(_) => break,
                }
            }
        });

        let stderr_log = open_stderr_log(job_id).await;
        let stderr_log = Arc::new(Mutex::new(stderr_log));
        let stderr_task = tokio::spawn(async move {
            let mut buf = vec![0u8; 8192];
            loop {
                match stderr.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        let chunk = buf[..n].to_vec();
                        stderr_clone.lock().unwrap().push(&chunk);
                        if let Ok(mut guard) = stderr_log.lock()
                            && let Some(f) = guard.as_mut()
                        {
                            use std::io::Write;
                            let _ = f.write_all(&chunk);
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        let _ = stdout_task.await;
        let _ = stderr_task.await;

        let status = child.wait().await.ok();
        let exit_code = status.and_then(|s| s.code()).unwrap_or(-1);
        info!(%job_id, exit_code, "process_mgr: pipe child exited");

        let _ = sys_clone
            .event_bus
            .send(EventBusMsg::Publish {
                payload: EventPayload::OutputEof {
                    id: job_id.to_string(),
                },
                channel: format!("output:{job_id}"),
            })
            .await;

        let new_state = if exit_code == 0 {
            JobStatus::Done
        } else {
            JobStatus::Failed
        };
        emit_state_change(&sys_clone, job_id, JobStatus::Running, new_state).await;
        emit_fg_exit(&sys_clone, &fg_clone, job_id, &format!("exit {exit_code}")).await;
        let _ = sys_clone
            .scheduler
            .send(SchedulerMsg::JobFinished { job_id, exit_code })
            .await;
        let _ = cleanup_tx_clone.send(job_id).await;
    });

    Ok(ProcessEntry {
        job_id,
        status: JobStatus::Running,
        _reader_handle: reader_handle,
        kill_tx: mpsc::channel::<()>(1).0,
        ring_buffer,
        stderr_ring: Some(stderr_ring),
        input: None,
        resize: None,
        fg_owner,
    })
}

async fn spawn_single_pty_job(
    job_id: JobId,
    pipeline: &cue_core::pipeline::Pipeline,
    snapshot: Option<&EnvSnapshot>,
    cwd_override: Option<&std::path::PathBuf>,
    wrapper_enabled: bool,
    sys: ActorSystem,
    cleanup_tx: mpsc::Sender<JobId>,
) -> Result<ProcessEntry, ()> {
    let segments = expand_pipeline_segments(job_id, pipeline, snapshot)?;
    let segment = &segments[0];
    let (program, args) = wrap_segment_if_enabled(&sys, wrapper_enabled, segment);

    let mut cmd = tokio::process::Command::new(&program);
    if !args.is_empty() {
        cmd.args(&args);
    }
    configure_command(&mut cmd, snapshot, cwd_override);

    let pty_pair = crate::pty::open_pty().map_err(|error| {
        error!(%job_id, err = %error, "process_mgr: open pty failed");
    })?;
    let master_file = std::fs::File::from(pty_pair.master);
    let slave = pty_pair.slave;
    if let Err(error) = set_nonblocking(master_file.as_raw_fd()) {
        error!(%job_id, err = %error, "process_mgr: set pty nonblocking failed");
        return Err(());
    }
    if let Err(error) = set_winsize(slave.as_raw_fd(), DEFAULT_PTY_COLS, DEFAULT_PTY_ROWS) {
        warn!(%job_id, err = %error, "process_mgr: set initial pty size failed");
    }
    let reader_file = master_file.try_clone().map_err(|error| {
        error!(%job_id, err = %error, "process_mgr: clone pty reader failed");
    })?;
    let input_file = master_file.try_clone().map_err(|error| {
        error!(%job_id, err = %error, "process_mgr: clone pty input failed");
    })?;
    let resize_file = Arc::new(master_file.try_clone().map_err(|error| {
        error!(%job_id, err = %error, "process_mgr: clone pty resize failed");
    })?);

    let slave_fd = slave.as_raw_fd();
    let master_fd = master_file.as_raw_fd();
    // SAFETY: the child process is single-threaded after fork here; the closure
    // only performs async-signal-safe libc calls on valid inherited fds.
    unsafe {
        cmd.pre_exec(move || {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            #[cfg(target_os = "macos")]
            let tiocsctty = libc::TIOCSCTTY.into();
            #[cfg(not(target_os = "macos"))]
            let tiocsctty = libc::TIOCSCTTY;
            if libc::ioctl(slave_fd, tiocsctty, 0) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            for target in [libc::STDIN_FILENO, libc::STDOUT_FILENO, libc::STDERR_FILENO] {
                if libc::dup2(slave_fd, target) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
            }
            if slave_fd > libc::STDERR_FILENO {
                libc::close(slave_fd);
            }
            if master_fd > libc::STDERR_FILENO {
                libc::close(master_fd);
            }
            Ok(())
        });
    }
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let mut child = cmd
        .spawn()
        .map_err(|error| log_spawn_failure(job_id, &program, &args, snapshot, &error))?;
    drop(slave);
    drop(master_file);

    info!(%job_id, pid = child.id().unwrap_or(0), "process_mgr: child spawned");

    let log_file = open_output_log(job_id).await;
    let input = match AsyncFd::new(input_file) {
        Ok(file) => Arc::new(file),
        Err(error) => {
            error!(%job_id, err = %error, "process_mgr: async pty input failed");
            let _ = child.start_kill();
            let _ = child.wait().await;
            return Err(());
        }
    };
    let reader = match AsyncFd::new(reader_file) {
        Ok(file) => file,
        Err(error) => {
            error!(%job_id, err = %error, "process_mgr: async pty reader failed");
            let _ = child.start_kill();
            let _ = child.wait().await;
            return Err(());
        }
    };

    let (kill_tx, kill_rx) = mpsc::channel::<()>(1);
    let ring_buffer = Arc::new(Mutex::new(RingBuffer::default()));
    let ring_clone = ring_buffer.clone();
    let fg_owner = Arc::new(Mutex::new(None));
    let fg_owner_clone = fg_owner.clone();
    let sys_clone = sys.clone();
    let cleanup_tx_clone = cleanup_tx.clone();
    let reader_handle = tokio::spawn(reader_task(
        job_id,
        child,
        reader,
        log_file,
        kill_rx,
        sys_clone,
        ring_clone,
        fg_owner_clone,
        cleanup_tx_clone,
    ));

    Ok(ProcessEntry {
        job_id,
        status: JobStatus::Running,
        _reader_handle: reader_handle,
        kill_tx,
        ring_buffer,
        stderr_ring: None,
        input: Some(JobInput::Pty(input)),
        resize: Some(resize_file),
        fg_owner,
    })
}

fn wrap_segment_if_enabled(
    sys: &ActorSystem,
    wrapper_enabled: bool,
    segment: &ExpandedSegment,
) -> (String, Vec<String>) {
    let program = segment.program.clone();
    let args = segment.args.clone();
    if !wrapper_enabled {
        return (program, args);
    }

    let wrapper = &sys.config.wrapper;
    let is_foreground = command_prefers_foreground(&segment.command_line);
    if wrapper.binary.is_empty() || !wrapper.should_wrap(&program, is_foreground, Some(true)) {
        return (program, args);
    }

    let mut wrapped_args = Vec::with_capacity(1 + args.len());
    wrapped_args.push(program);
    wrapped_args.extend(args);
    (wrapper.binary.clone(), wrapped_args)
}

async fn spawn_native_pipeline_job(
    job_id: JobId,
    pipeline: &cue_core::pipeline::Pipeline,
    snapshot: Option<&EnvSnapshot>,
    cwd_override: Option<&std::path::PathBuf>,
    wrapper_enabled: bool,
    sys: ActorSystem,
    cleanup_tx: mpsc::Sender<JobId>,
) -> Result<ProcessEntry, ()> {
    let segments = expand_pipeline_segments(job_id, pipeline, snapshot)?;
    let NativePipelineSpawn {
        children,
        input,
        stdout_sources,
        stderr_sources,
    } = spawn_native_pipeline(
        job_id,
        &segments,
        snapshot,
        cwd_override,
        wrapper_enabled,
        &sys,
    )?;

    let pids: Vec<u32> = children
        .iter()
        .filter_map(tokio::process::Child::id)
        .collect();
    info!(%job_id, ?pids, "process_mgr: native pipeline spawned");

    let log_file = open_output_log(job_id).await;
    let stderr_log = open_stderr_log(job_id).await;
    let (kill_tx, kill_rx) = mpsc::channel::<()>(1);
    let ring_buffer = Arc::new(Mutex::new(RingBuffer::default()));
    let stderr_ring = Arc::new(Mutex::new(RingBuffer::default()));
    let fg_owner = Arc::new(Mutex::new(None));
    let reader_handle = tokio::spawn(pipeline_reader_task(
        job_id,
        children,
        stdout_sources,
        stderr_sources,
        log_file,
        stderr_log,
        kill_rx,
        sys.clone(),
        ring_buffer.clone(),
        stderr_ring.clone(),
        fg_owner.clone(),
        cleanup_tx.clone(),
    ));

    Ok(ProcessEntry {
        job_id,
        status: JobStatus::Running,
        _reader_handle: reader_handle,
        kill_tx,
        ring_buffer,
        stderr_ring: Some(stderr_ring),
        input,
        resize: None,
        fg_owner,
    })
}

async fn spawn_logical_job(
    job_id: JobId,
    plan: JobPlan,
    snapshot: Option<EnvSnapshot>,
    cwd_override: Option<std::path::PathBuf>,
    wrapper_enabled: bool,
    sys: ActorSystem,
    cleanup_tx: mpsc::Sender<JobId>,
) -> Result<ProcessEntry, ()> {
    let log_file = open_output_log(job_id).await;
    let stderr_log = open_stderr_log(job_id).await;
    let (kill_tx, kill_rx) = mpsc::channel::<()>(1);
    let ring_buffer = Arc::new(Mutex::new(RingBuffer::default()));
    let stderr_ring = Arc::new(Mutex::new(RingBuffer::default()));
    let fg_owner = Arc::new(Mutex::new(None));
    let reader_handle = tokio::spawn(logical_job_task(
        job_id,
        plan,
        snapshot,
        cwd_override,
        log_file,
        stderr_log,
        kill_rx,
        wrapper_enabled,
        sys.clone(),
        ring_buffer.clone(),
        stderr_ring.clone(),
        fg_owner.clone(),
        cleanup_tx.clone(),
    ));

    Ok(ProcessEntry {
        job_id,
        status: JobStatus::Running,
        _reader_handle: reader_handle,
        kill_tx,
        ring_buffer,
        stderr_ring: Some(stderr_ring),
        input: None,
        resize: None,
        fg_owner,
    })
}

fn spawn_native_pipeline(
    job_id: JobId,
    segments: &[ExpandedSegment],
    snapshot: Option<&EnvSnapshot>,
    cwd_override: Option<&std::path::PathBuf>,
    wrapper_enabled: bool,
    sys: &ActorSystem,
) -> Result<NativePipelineSpawn, ()> {
    let mut children = Vec::with_capacity(segments.len());
    let mut stdout_sources = Vec::new();
    let mut stderr_sources = Vec::new();
    let mut input = None;
    let mut next_stdin = None;

    for (idx, segment) in segments.iter().enumerate() {
        let (program, args) = wrap_segment_if_enabled(sys, wrapper_enabled, segment);
        let mut cmd = tokio::process::Command::new(&program);
        if !args.is_empty() {
            cmd.args(&args);
        }
        configure_command(&mut cmd, snapshot, cwd_override);

        if idx == 0 {
            cmd.stdin(Stdio::piped());
        } else if let Some(stdin) = next_stdin.take() {
            cmd.stdin(Stdio::from(stdin));
        } else {
            error!(%job_id, segment = idx, "process_mgr: missing pipeline stdin");
            return Err(());
        }

        match segment.pipe_to_next {
            Some(cue_core::pipeline::PipeOp::Stdout) => {
                let (read_end, write_end) = create_pipe().map_err(|error| {
                    error!(%job_id, segment = idx, err = %error, "process_mgr: create stdout pipe failed");
                })?;
                cmd.stdout(Stdio::from(write_end));
                cmd.stderr(Stdio::piped());
                next_stdin = Some(read_end);
            }
            Some(cue_core::pipeline::PipeOp::StdoutStderr) => {
                let (read_end, write_end) = create_pipe().map_err(|error| {
                    error!(%job_id, segment = idx, err = %error, "process_mgr: create stdout+stderr pipe failed");
                })?;
                let stderr_write = write_end.try_clone().map_err(|error| {
                    error!(%job_id, segment = idx, err = %error, "process_mgr: clone combined pipe failed");
                })?;
                cmd.stdout(Stdio::from(write_end));
                cmd.stderr(Stdio::from(stderr_write));
                next_stdin = Some(read_end);
            }
            Some(cue_core::pipeline::PipeOp::StderrOnly) => {
                let (read_end, write_end) = create_pipe().map_err(|error| {
                    error!(%job_id, segment = idx, err = %error, "process_mgr: create stderr-only pipe failed");
                })?;
                cmd.stdout(Stdio::piped());
                cmd.stderr(Stdio::from(write_end));
                next_stdin = Some(read_end);
            }
            None => {
                cmd.stdout(Stdio::piped());
                cmd.stderr(Stdio::piped());
            }
        }

        let mut child = cmd.spawn().map_err(|error| {
            log_spawn_failure(job_id, &program, &args, snapshot, &error);
        })?;
        if idx == 0 {
            input = child
                .stdin
                .take()
                .map(|stdin| JobInput::Pipe(Arc::new(tokio::sync::Mutex::new(stdin))));
        }
        if let Some(stdout) = child.stdout.take() {
            stdout_sources.push(stdout);
        }
        if let Some(stderr) = child.stderr.take() {
            stderr_sources.push(stderr);
        }
        children.push(child);
    }

    Ok(NativePipelineSpawn {
        children,
        input,
        stdout_sources,
        stderr_sources,
    })
}

fn create_pipe() -> std::io::Result<(std::fs::File, std::fs::File)> {
    let mut fds = [0; 2];
    // SAFETY: `pipe` initializes two owned fds on success.
    let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
    if rc == -1 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: the returned fds are fresh and uniquely owned here.
    Ok(unsafe {
        (
            std::fs::File::from_raw_fd(fds[0]),
            std::fs::File::from_raw_fd(fds[1]),
        )
    })
}

/// Open (or create) the append-only log file for a job's output.
///
/// Runs on the blocking thread pool so filesystem syscalls do not stall the
/// Tokio runtime thread.
async fn open_output_log(job_id: JobId) -> Option<std::fs::File> {
    tokio::task::spawn_blocking(move || {
        let dir = crate::dirs::output_dir();
        if let Err(e) = std::fs::create_dir_all(&dir) {
            error!(%job_id, err = %e, "process_mgr: cannot create output dir");
            return None;
        }
        let path = dir.join(format!("{job_id}.log"));
        match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
        {
            Ok(f) => Some(f),
            Err(e) => {
                error!(%job_id, path = %path.display(), err = %e, "process_mgr: open log file");
                None
            }
        }
    })
    .await
    .unwrap_or(None)
}

async fn open_stderr_log(job_id: JobId) -> Option<std::fs::File> {
    tokio::task::spawn_blocking(move || {
        let dir = crate::dirs::output_dir();
        if let Err(e) = std::fs::create_dir_all(&dir) {
            error!(%job_id, err = %e, "process_mgr: cannot create output dir");
            return None;
        }
        let path = dir.join(format!("{job_id}.stderr"));
        match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
        {
            Ok(f) => Some(f),
            Err(e) => {
                error!(%job_id, path = %path.display(), err = %e, "process_mgr: open stderr log");
                None
            }
        }
    })
    .await
    .unwrap_or(None)
}

async fn clear_job_logs(job_id: JobId) {
    let _ = tokio::task::spawn_blocking(move || {
        for suffix in [".log", ".stderr"] {
            let path = crate::dirs::output_dir().join(format!("{job_id}{suffix}"));
            if let Err(error) = std::fs::remove_file(&path)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                warn!(
                    %job_id,
                    path = %path.display(),
                    err = %error,
                    "process_mgr: failed to remove stale output log"
                );
            }
        }
    })
    .await;
}

/// Background task that reads PTY output, populates the ring buffer,
/// writes to the log file, emits events, and waits for the child to exit.
#[allow(clippy::too_many_arguments)]
async fn reader_task(
    job_id: JobId,
    mut child: tokio::process::Child,
    reader: AsyncFd<std::fs::File>,
    log_file: Option<std::fs::File>,
    mut kill_rx: mpsc::Receiver<()>,
    sys: ActorSystem,
    ring: Arc<Mutex<RingBuffer>>,
    fg_owner: Arc<Mutex<Option<u64>>>,
    cleanup_tx: mpsc::Sender<JobId>,
) {
    // Wrap the log file so it can be shared with `spawn_blocking`.
    let log_file = Arc::new(Mutex::new(log_file));
    let mut pty_buf = vec![0u8; 8192];
    let mut pty_done = false;

    loop {
        tokio::select! {
            // Kill signal from the main actor loop.
            _ = kill_rx.recv() => {
                info!(%job_id, "process_mgr: sending SIGTERM");
                let _ = child.start_kill();

                // Wait up to 10 s for graceful exit, then SIGKILL (kill_on_drop).
                let timeout = tokio::time::sleep(std::time::Duration::from_secs(10));
                tokio::select! {
                    status = child.wait() => {
                        let code = status.map(|s| s.code().unwrap_or(-1)).unwrap_or(-1);
                        debug!(%job_id, code, "process_mgr: child exited after SIGTERM");
                    }
                    () = timeout => {
                        warn!(%job_id, "process_mgr: child did not exit in 10 s — dropping (SIGKILL)");
                        // child is dropped here → kill_on_drop sends SIGKILL
                        drop(child);
                    }
                }

                emit_state_change(&sys, job_id, JobStatus::Running, JobStatus::Killed).await;
                emit_fg_exit(&sys, &fg_owner, job_id, "killed").await;
                let _ = sys.scheduler.send(SchedulerMsg::JobFinished { job_id, exit_code: -1 }).await;
                // Tell the main loop to remove our entry.
                let _ = cleanup_tx.send(job_id).await;
                return;
            }

            result = read_pty(&reader, &mut pty_buf), if !pty_done => {
                match result {
                    Ok(0) => { pty_done = true; }
                    Ok(n) => {
                        let chunk = &pty_buf[..n];
                        ring.lock().unwrap().push(chunk);
                        write_log(&log_file, chunk).await;
                        emit_output(&sys, job_id, OutputStream::Stdout, chunk).await;
                        emit_fg_output(&sys, &fg_owner, chunk).await;
                    }
                    Err(e) => {
                        if e.raw_os_error() == Some(libc::EIO) {
                            pty_done = true;
                        } else {
                            debug!(%job_id, err = %e, "process_mgr: pty read error");
                            pty_done = true;
                        }
                    }
                }
            }
        }

        if pty_done {
            break;
        }
    }

    // Wait for exit status while still honoring late kill requests.
    let (exit_code, was_killed) = tokio::select! {
        status = child.wait() => {
            let code = match status {
                Ok(s) => s.code().unwrap_or(-1),
                Err(e) => {
                    error!(%job_id, err = %e, "process_mgr: wait failed");
                    -1
                }
            };
            (code, false)
        }
        _ = kill_rx.recv() => {
            child.start_kill().ok();
            let code = match child.wait().await {
                Ok(s) => s.code().unwrap_or(-1),
                Err(_) => -1,
            };
            (code, true)
        }
    };

    let ring_len = ring.lock().unwrap().len();
    info!(%job_id, exit_code, bytes = ring_len, "process_mgr: child exited");

    // Emit OutputEof.
    let _ = sys
        .event_bus
        .send(EventBusMsg::Publish {
            payload: EventPayload::OutputEof {
                id: job_id.to_string(),
            },
            channel: format!("output:{job_id}"),
        })
        .await;

    if was_killed {
        emit_state_change(&sys, job_id, JobStatus::Running, JobStatus::Killed).await;
        emit_fg_exit(&sys, &fg_owner, job_id, "killed").await;
        let _ = sys
            .scheduler
            .send(SchedulerMsg::JobFinished {
                job_id,
                exit_code: -1,
            })
            .await;
    } else {
        // Determine final state.
        let new_state = if exit_code == 0 {
            JobStatus::Done
        } else {
            JobStatus::Failed
        };

        emit_state_change(&sys, job_id, JobStatus::Running, new_state).await;
        let reason = if exit_code == 0 {
            "done".to_string()
        } else {
            format!("exit {exit_code}")
        };
        emit_fg_exit(&sys, &fg_owner, job_id, &reason).await;

        let _ = sys
            .scheduler
            .send(SchedulerMsg::JobFinished { job_id, exit_code })
            .await;
    }

    // Tell the main loop to remove our entry.
    let _ = cleanup_tx.send(job_id).await;
}

#[allow(clippy::too_many_arguments)]
async fn pipeline_reader_task(
    job_id: JobId,
    mut children: Vec<tokio::process::Child>,
    stdout_sources: Vec<tokio::process::ChildStdout>,
    stderr_sources: Vec<tokio::process::ChildStderr>,
    log_file: Option<std::fs::File>,
    stderr_log: Option<std::fs::File>,
    mut kill_rx: mpsc::Receiver<()>,
    sys: ActorSystem,
    ring: Arc<Mutex<RingBuffer>>,
    stderr_ring: Arc<Mutex<RingBuffer>>,
    fg_owner: Arc<Mutex<Option<u64>>>,
    cleanup_tx: mpsc::Sender<JobId>,
) {
    let log_file = Arc::new(Mutex::new(log_file));
    let stderr_log = Arc::new(Mutex::new(stderr_log));
    let (chunk_tx, mut chunk_rx) = mpsc::unbounded_channel();
    let mut active_readers = 0usize;

    for stdout in stdout_sources {
        active_readers += 1;
        spawn_pipeline_stream_reader(stdout, PipelineStreamKind::Stdout, chunk_tx.clone());
    }
    for stderr in stderr_sources {
        active_readers += 1;
        spawn_pipeline_stream_reader(stderr, PipelineStreamKind::Stderr, chunk_tx.clone());
    }
    drop(chunk_tx);

    let mut was_killed = false;
    while active_readers > 0 {
        tokio::select! {
            _ = kill_rx.recv(), if !was_killed => {
                was_killed = true;
                info!(%job_id, "process_mgr: killing native pipeline");
                terminate_children(&mut children).await;
            }
            Some(msg) = chunk_rx.recv() => {
                match msg {
                    PipelineReaderMsg::Chunk { kind: PipelineStreamKind::Stdout, data } => {
                        ring.lock().unwrap().push(&data);
                        write_log(&log_file, &data).await;
                        emit_output(&sys, job_id, OutputStream::Stdout, &data).await;
                    }
                    PipelineReaderMsg::Chunk { kind: PipelineStreamKind::Stderr, data } => {
                        stderr_ring.lock().unwrap().push(&data);
                        write_log(&stderr_log, &data).await;
                        emit_output(&sys, job_id, OutputStream::Stderr, &data).await;
                    }
                    PipelineReaderMsg::Closed => {
                        active_readers = active_readers.saturating_sub(1);
                    }
                }
            }
            else => break,
        }
    }

    let exit_code = if was_killed {
        wait_for_children(&mut children).await
    } else {
        tokio::select! {
            _ = kill_rx.recv() => {
                was_killed = true;
                terminate_children(&mut children).await;
                wait_for_children(&mut children).await
            }
            code = wait_for_children(&mut children) => code,
        }
    };

    let stdout_len = ring.lock().unwrap().len();
    let stderr_len = stderr_ring.lock().unwrap().len();
    info!(%job_id, exit_code, stdout_bytes = stdout_len, stderr_bytes = stderr_len, "process_mgr: native pipeline exited");

    let _ = sys
        .event_bus
        .send(EventBusMsg::Publish {
            payload: EventPayload::OutputEof {
                id: job_id.to_string(),
            },
            channel: format!("output:{job_id}"),
        })
        .await;

    if was_killed {
        emit_state_change(&sys, job_id, JobStatus::Running, JobStatus::Killed).await;
        emit_fg_exit(&sys, &fg_owner, job_id, "killed").await;
        let _ = sys
            .scheduler
            .send(SchedulerMsg::JobFinished {
                job_id,
                exit_code: -1,
            })
            .await;
    } else {
        let new_state = if exit_code == 0 {
            JobStatus::Done
        } else {
            JobStatus::Failed
        };
        emit_state_change(&sys, job_id, JobStatus::Running, new_state).await;
        let reason = if exit_code == 0 {
            "done".to_string()
        } else {
            format!("exit {exit_code}")
        };
        emit_fg_exit(&sys, &fg_owner, job_id, &reason).await;
        let _ = sys
            .scheduler
            .send(SchedulerMsg::JobFinished { job_id, exit_code })
            .await;
    }

    let _ = cleanup_tx.send(job_id).await;
}

#[allow(clippy::too_many_arguments)]
async fn logical_job_task(
    job_id: JobId,
    plan: JobPlan,
    snapshot: Option<EnvSnapshot>,
    cwd_override: Option<std::path::PathBuf>,
    log_file: Option<std::fs::File>,
    stderr_log: Option<std::fs::File>,
    mut kill_rx: mpsc::Receiver<()>,
    wrapper_enabled: bool,
    sys: ActorSystem,
    ring: Arc<Mutex<RingBuffer>>,
    stderr_ring: Arc<Mutex<RingBuffer>>,
    fg_owner: Arc<Mutex<Option<u64>>>,
    cleanup_tx: mpsc::Sender<JobId>,
) {
    let log_file = Arc::new(Mutex::new(log_file));
    let stderr_log = Arc::new(Mutex::new(stderr_log));
    let mut was_killed = false;
    let mut local_snapshot = snapshot.unwrap_or_else(|| EnvSnapshot {
        env: std::env::vars().collect::<BTreeMap<_, _>>(),
        cwd: std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from(".")),
    });
    if let Some(cwd) = cwd_override.as_ref() {
        local_snapshot.cwd = cwd.clone();
    }
    let exit_code = run_job_plan_streaming(
        job_id,
        &plan,
        &mut local_snapshot,
        &mut kill_rx,
        &mut was_killed,
        wrapper_enabled,
        &sys,
        &ring,
        &stderr_ring,
        &log_file,
        &stderr_log,
    )
    .await;

    let _ = sys
        .event_bus
        .send(EventBusMsg::Publish {
            payload: EventPayload::OutputEof {
                id: job_id.to_string(),
            },
            channel: format!("output:{job_id}"),
        })
        .await;

    if was_killed {
        emit_state_change(&sys, job_id, JobStatus::Running, JobStatus::Killed).await;
        emit_fg_exit(&sys, &fg_owner, job_id, "killed").await;
        let _ = sys
            .scheduler
            .send(SchedulerMsg::JobFinished {
                job_id,
                exit_code: -1,
            })
            .await;
    } else {
        let new_state = if exit_code == 0 {
            JobStatus::Done
        } else {
            JobStatus::Failed
        };
        emit_state_change(&sys, job_id, JobStatus::Running, new_state).await;
        let reason = if exit_code == 0 {
            "done".to_string()
        } else {
            format!("exit {exit_code}")
        };
        emit_fg_exit(&sys, &fg_owner, job_id, &reason).await;
        let _ = sys
            .scheduler
            .send(SchedulerMsg::JobFinished { job_id, exit_code })
            .await;
    }

    let _ = cleanup_tx.send(job_id).await;
}

#[allow(clippy::too_many_arguments)]
async fn run_job_plan_streaming(
    job_id: JobId,
    plan: &JobPlan,
    snapshot: &mut EnvSnapshot,
    kill_rx: &mut mpsc::Receiver<()>,
    was_killed: &mut bool,
    wrapper_enabled: bool,
    sys: &ActorSystem,
    ring: &Arc<Mutex<RingBuffer>>,
    stderr_ring: &Arc<Mutex<RingBuffer>>,
    log_file: &Arc<Mutex<Option<std::fs::File>>>,
    stderr_log: &Arc<Mutex<Option<std::fs::File>>>,
) -> i32 {
    if *was_killed {
        return -1;
    }
    match plan {
        JobPlan::Pipeline(pipeline) => {
            run_pipeline_streaming(
                job_id,
                pipeline,
                snapshot,
                kill_rx,
                was_killed,
                wrapper_enabled,
                sys,
                ring,
                stderr_ring,
                log_file,
                stderr_log,
            )
            .await
        }
        JobPlan::And { left, right } => {
            let code = Box::pin(run_job_plan_streaming(
                job_id,
                left,
                snapshot,
                kill_rx,
                was_killed,
                wrapper_enabled,
                sys,
                ring,
                stderr_ring,
                log_file,
                stderr_log,
            ))
            .await;
            if code == 0 && !*was_killed {
                Box::pin(run_job_plan_streaming(
                    job_id,
                    right,
                    snapshot,
                    kill_rx,
                    was_killed,
                    wrapper_enabled,
                    sys,
                    ring,
                    stderr_ring,
                    log_file,
                    stderr_log,
                ))
                .await
            } else {
                code
            }
        }
        JobPlan::Or { left, right } => {
            let code = Box::pin(run_job_plan_streaming(
                job_id,
                left,
                snapshot,
                kill_rx,
                was_killed,
                wrapper_enabled,
                sys,
                ring,
                stderr_ring,
                log_file,
                stderr_log,
            ))
            .await;
            if code != 0 && !*was_killed {
                Box::pin(run_job_plan_streaming(
                    job_id,
                    right,
                    snapshot,
                    kill_rx,
                    was_killed,
                    wrapper_enabled,
                    sys,
                    ring,
                    stderr_ring,
                    log_file,
                    stderr_log,
                ))
                .await
            } else {
                code
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_pipeline_streaming(
    job_id: JobId,
    pipeline: &cue_core::pipeline::Pipeline,
    snapshot: &mut EnvSnapshot,
    kill_rx: &mut mpsc::Receiver<()>,
    was_killed: &mut bool,
    wrapper_enabled: bool,
    sys: &ActorSystem,
    ring: &Arc<Mutex<RingBuffer>>,
    stderr_ring: &Arc<Mutex<RingBuffer>>,
    log_file: &Arc<Mutex<Option<std::fs::File>>>,
    stderr_log: &Arc<Mutex<Option<std::fs::File>>>,
) -> i32 {
    if let Some(code) =
        run_job_local_builtin(job_id, pipeline, snapshot, stderr_ring, stderr_log).await
    {
        return code;
    }

    let segments = match expand_pipeline_segments(job_id, pipeline, Some(snapshot)) {
        Ok(segments) => segments,
        Err(()) => return -1,
    };
    let mut spawn = match spawn_native_pipeline(
        job_id,
        &segments,
        Some(snapshot),
        None,
        wrapper_enabled,
        sys,
    ) {
        Ok(spawn) => spawn,
        Err(()) => return -1,
    };

    let (chunk_tx, mut chunk_rx) = mpsc::unbounded_channel();
    let mut active_readers = 0usize;

    for stdout in spawn.stdout_sources.drain(..) {
        active_readers += 1;
        spawn_pipeline_stream_reader(stdout, PipelineStreamKind::Stdout, chunk_tx.clone());
    }
    for stderr in spawn.stderr_sources.drain(..) {
        active_readers += 1;
        spawn_pipeline_stream_reader(stderr, PipelineStreamKind::Stderr, chunk_tx.clone());
    }
    drop(chunk_tx);

    while active_readers > 0 {
        tokio::select! {
            _ = kill_rx.recv(), if !*was_killed => {
                *was_killed = true;
                terminate_children(&mut spawn.children).await;
            }
            Some(msg) = chunk_rx.recv() => {
                match msg {
                    PipelineReaderMsg::Chunk { kind: PipelineStreamKind::Stdout, data } => {
                        ring.lock().unwrap().push(&data);
                        write_log(log_file, &data).await;
                        emit_output(sys, job_id, OutputStream::Stdout, &data).await;
                    }
                    PipelineReaderMsg::Chunk { kind: PipelineStreamKind::Stderr, data } => {
                        stderr_ring.lock().unwrap().push(&data);
                        write_log(stderr_log, &data).await;
                        emit_output(sys, job_id, OutputStream::Stderr, &data).await;
                    }
                    PipelineReaderMsg::Closed => {
                        active_readers = active_readers.saturating_sub(1);
                    }
                }
            }
            else => break,
        }
    }

    if *was_killed {
        wait_for_children(&mut spawn.children).await;
        -1
    } else {
        tokio::select! {
            _ = kill_rx.recv() => {
                *was_killed = true;
                terminate_children(&mut spawn.children).await;
                wait_for_children(&mut spawn.children).await;
                -1
            }
            code = wait_for_children(&mut spawn.children) => code,
        }
    }
}

async fn run_job_local_builtin(
    job_id: JobId,
    pipeline: &cue_core::pipeline::Pipeline,
    snapshot: &mut EnvSnapshot,
    stderr_ring: &Arc<Mutex<RingBuffer>>,
    stderr_log: &Arc<Mutex<Option<std::fs::File>>>,
) -> Option<i32> {
    if pipeline.segments.len() != 1 {
        return None;
    }
    let segment = &pipeline.segments[0];
    if segment.pipe_to_next.is_some() {
        return None;
    }

    let expanded = expand_command_line(&segment.command, Some(snapshot));
    match detect_job_local_builtin(&expanded)? {
        JobLocalBuiltin::Cd { path } => {
            if expanded.len() > 2 {
                write_job_local_stderr(
                    job_id,
                    stderr_ring,
                    stderr_log,
                    b"cd: too many arguments\n",
                )
                .await;
                return Some(1);
            }
            match resolve_job_local_cd_target(snapshot, &path) {
                Ok(cwd) => {
                    snapshot.cwd = cwd;
                    Some(0)
                }
                Err(message) => {
                    let line = format!("{message}\n");
                    write_job_local_stderr(job_id, stderr_ring, stderr_log, line.as_bytes()).await;
                    Some(1)
                }
            }
        }
        JobLocalBuiltin::EnvSet { assignments } => {
            if assignments.is_empty() {
                write_job_local_stderr(
                    job_id,
                    stderr_ring,
                    stderr_log,
                    b"env set: expected KEY=VALUE\n",
                )
                .await;
                return Some(1);
            }
            for assignment in assignments {
                let Some((key, value)) = assignment.split_once('=') else {
                    let line = format!("env set: expected KEY=VALUE, got `{assignment}`\n");
                    write_job_local_stderr(job_id, stderr_ring, stderr_log, line.as_bytes()).await;
                    return Some(1);
                };
                if key.is_empty() {
                    write_job_local_stderr(
                        job_id,
                        stderr_ring,
                        stderr_log,
                        b"env set: empty variable name\n",
                    )
                    .await;
                    return Some(1);
                }
                snapshot.env.insert(key.to_string(), value.to_string());
            }
            Some(0)
        }
    }
}

fn resolve_job_local_cd_target(
    snapshot: &EnvSnapshot,
    path: &str,
) -> Result<std::path::PathBuf, String> {
    let requested = std::path::PathBuf::from(path);
    let target = if requested.is_absolute() {
        requested
    } else {
        snapshot.cwd.join(requested)
    };
    let resolved = std::fs::canonicalize(&target)
        .map_err(|error| format!("cd: {}: {error}", target.display()))?;
    if !resolved.is_dir() {
        return Err(format!("cd: {}: not a directory", resolved.display()));
    }
    Ok(resolved)
}

async fn write_job_local_stderr(
    job_id: JobId,
    stderr_ring: &Arc<Mutex<RingBuffer>>,
    stderr_log: &Arc<Mutex<Option<std::fs::File>>>,
    data: &[u8],
) {
    stderr_ring.lock().unwrap().push(data);
    write_log(stderr_log, data).await;
    debug!(%job_id, bytes = data.len(), "process_mgr: job-local builtin stderr");
}

fn spawn_pipeline_stream_reader<R>(
    mut reader: R,
    kind: PipelineStreamKind,
    tx: mpsc::UnboundedSender<PipelineReaderMsg>,
) where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut buf = vec![0u8; 8192];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    if tx
                        .send(PipelineReaderMsg::Chunk {
                            kind,
                            data: buf[..n].to_vec(),
                        })
                        .is_err()
                    {
                        return;
                    }
                }
                Err(error) => {
                    debug!(err = %error, "process_mgr: pipeline stream read error");
                    break;
                }
            }
        }
        let _ = tx.send(PipelineReaderMsg::Closed);
    });
}

async fn terminate_children(children: &mut [tokio::process::Child]) {
    for child in children.iter_mut() {
        let _ = child.start_kill();
    }
}

async fn wait_for_children(children: &mut [tokio::process::Child]) -> i32 {
    let mut exit_code = -1;
    let last_idx = children.len().saturating_sub(1);
    for (idx, child) in children.iter_mut().enumerate() {
        match child.wait().await {
            Ok(status) => {
                if idx == last_idx {
                    exit_code = status.code().unwrap_or(-1);
                }
            }
            Err(error) => {
                error!(err = %error, "process_mgr: child wait failed");
                if idx == last_idx {
                    exit_code = -1;
                }
            }
        }
    }
    exit_code
}

/// Emit a `JobStateChanged` event.
async fn emit_state_change(
    sys: &ActorSystem,
    job_id: JobId,
    old_state: JobStatus,
    new_state: JobStatus,
) {
    let _ = sys
        .event_bus
        .send(EventBusMsg::Publish {
            payload: EventPayload::JobStateChanged {
                job_id: job_id.to_string(),
                old_state,
                new_state,
                end_scope: None,
                chain_id: None,
                chain_index: None,
            },
            channel: "jobs".into(),
        })
        .await;
}

/// Emit an `OutputChunk` event.
async fn emit_output(sys: &ActorSystem, job_id: JobId, stream: OutputStream, data: &[u8]) {
    let text = String::from_utf8_lossy(data).into_owned();
    let _ = sys
        .event_bus
        .send(EventBusMsg::Publish {
            payload: EventPayload::OutputChunk {
                id: job_id.to_string(),
                stream,
                data: text,
            },
            channel: format!("output:{job_id}"),
        })
        .await;
}

async fn emit_fg_output(sys: &ActorSystem, fg_owner: &Arc<Mutex<Option<u64>>>, data: &[u8]) {
    let client_id = *fg_owner.lock().unwrap();
    if let Some(client_id) = client_id {
        let _ = sys
            .gateway
            .send(GatewayMsg::SendEvent {
                client_id,
                payload: EventPayload::FgOutput {
                    data: data.to_vec(),
                },
            })
            .await;
    }
}

async fn emit_fg_exit(
    sys: &ActorSystem,
    fg_owner: &Arc<Mutex<Option<u64>>>,
    job_id: JobId,
    reason: &str,
) {
    let client_id = fg_owner.lock().unwrap().take();
    if let Some(client_id) = client_id {
        let _ = sys
            .gateway
            .send(GatewayMsg::SendEvent {
                client_id,
                payload: EventPayload::FgExited {
                    id: job_id.to_string(),
                    reason: reason.to_string(),
                },
            })
            .await;
    }
}

/// Write a chunk to the log file (best-effort).
///
/// Offloaded to the blocking thread pool so the async reader task never stalls
/// the Tokio runtime with synchronous I/O.
async fn write_log(file: &Arc<Mutex<Option<std::fs::File>>>, data: &[u8]) {
    let file = file.clone();
    let data = data.to_vec();
    let _ = tokio::task::spawn_blocking(move || {
        if let Ok(mut guard) = file.lock()
            && let Some(f) = guard.as_mut()
        {
            let _ = f.write_all(&data);
        }
    })
    .await;
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use super::*;

    fn snapshot() -> EnvSnapshot {
        EnvSnapshot {
            env: BTreeMap::from([
                ("HOME".into(), "/tmp/cue-home".into()),
                ("USER".into(), "tester".into()),
            ]),
            cwd: PathBuf::from("/tmp/work"),
        }
    }

    #[test]
    fn expands_scope_words_for_jobs() {
        let expanded = expand_command_line(
            &[
                "~/bin/tool".into(),
                "~".into(),
                "$HOME".into(),
                "${USER}".into(),
                "prefix-$USER-suffix".into(),
            ],
            Some(&snapshot()),
        );

        assert_eq!(
            expanded,
            vec![
                "/tmp/cue-home/bin/tool",
                "/tmp/cue-home",
                "/tmp/cue-home",
                "tester",
                "prefix-tester-suffix",
            ]
        );
    }

    #[test]
    fn preserves_unsupported_parameter_forms() {
        let expanded = expand_command_line(
            &[
                "echo".into(),
                "${USER:-guest}".into(),
                "${BROKEN".into(),
                "$1".into(),
                "\\$USER".into(),
            ],
            Some(&snapshot()),
        );

        assert_eq!(
            expanded,
            vec!["echo", "${USER:-guest}", "${BROKEN", "$1", "$USER"]
        );
    }

    #[test]
    fn multi_segment_pipeline_expands_each_segment_independently() {
        let pipeline = cue_core::pipeline::Pipeline {
            segments: vec![
                cue_core::pipeline::PipeSegment {
                    command: vec!["printf".into(), "%s".into(), "hello world".into()],
                    pipe_to_next: Some(cue_core::pipeline::PipeOp::Stdout),
                },
                cue_core::pipeline::PipeSegment {
                    command: vec!["grep".into(), "hello world".into()],
                    pipe_to_next: None,
                },
            ],
        };

        let segments = expand_pipeline_segments(JobId(7), &pipeline, Some(&snapshot()))
            .expect("expanded segments");

        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].program, "printf");
        assert_eq!(segments[0].args, vec!["%s", "hello world"]);
        assert_eq!(segments[1].program, "grep");
        assert_eq!(segments[1].args, vec!["hello world"]);
    }

    #[test]
    fn stderr_only_pipeline_keeps_metacharacters_as_data() {
        let pipeline = cue_core::pipeline::Pipeline {
            segments: vec![
                cue_core::pipeline::PipeSegment {
                    command: vec!["producer".into(), "semi;colon".into()],
                    pipe_to_next: Some(cue_core::pipeline::PipeOp::StderrOnly),
                },
                cue_core::pipeline::PipeSegment {
                    command: vec!["consumer".into()],
                    pipe_to_next: None,
                },
            ],
        };

        let segments = expand_pipeline_segments(JobId(9), &pipeline, Some(&snapshot()))
            .expect("expanded segments");

        assert_eq!(segments[0].args, vec!["semi;colon"]);
        assert!(matches!(
            segments[0].pipe_to_next,
            Some(cue_core::pipeline::PipeOp::StderrOnly)
        ));
    }
}
