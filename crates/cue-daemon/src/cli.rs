//! cued — background daemon entry point.
//!
//! Subcommands:
//!   `cued start [--fg] [-F] [--socket PATH]` — start the daemon
//!   `cued restart [--socket PATH]`           — restart the daemon
//!   `cued stop`                              — send Shutdown to a running daemon
//!   `cued status`                            — check if daemon is running
//!   `cued gateway --stdio`                   — relay IPC over stdin/stdout
//!   `cued install`                           — install systemd/launchd service
//!   `cued uninstall`                         — remove service registration
//!   `cued upgrade`                           — self-update from GitHub Releases

use std::ffi::OsString;
use std::fs::File;
use std::io::{Read as _, Write as _};
#[cfg(target_os = "linux")]
use std::os::fd::OwnedFd;
use std::os::fd::{AsRawFd as _, FromRawFd as _};
#[cfg(target_os = "linux")]
use std::os::unix::fs::MetadataExt as _;
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result};
use bpaf::Parser as _;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use tokio::signal;
use tracing::{error, info};

const PLANNED_RESTART_EXIT_CODE: i32 = 75;
const STOP_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DaemonExit {
    Stopped,
    PlannedRestart,
}

impl DaemonExit {
    fn code(self) -> i32 {
        match self {
            Self::Stopped => 0,
            Self::PlannedRestart => PLANNED_RESTART_EXIT_CODE,
        }
    }
}

fn restart_ownership(
    socket_path: &Path,
    default_socket_path: &Path,
    ownership: crate::service::CurrentProcessOwnership,
) -> crate::lifecycle::RestartOwnership {
    if socket_path != default_socket_path {
        return crate::lifecycle::RestartOwnership::Standalone;
    }
    match ownership {
        crate::service::CurrentProcessOwnership::Managed => {
            crate::lifecycle::RestartOwnership::Supervisor
        }
        crate::service::CurrentProcessOwnership::NotManaged => {
            crate::lifecycle::RestartOwnership::Standalone
        }
        crate::service::CurrentProcessOwnership::Unknown => {
            crate::lifecycle::RestartOwnership::Unknown
        }
    }
}

fn completed_restart_exit(supervisor_restart: bool) -> DaemonExit {
    if supervisor_restart {
        DaemonExit::PlannedRestart
    } else {
        DaemonExit::Stopped
    }
}

// ── CLI definition (combinator API, no derive feature needed) ──

#[derive(Debug, Clone, PartialEq, Eq)]
enum Cli {
    Start {
        fg: bool,
        force: bool,
        preserve_restart_fence: bool,
        socket: Option<PathBuf>,
    },
    Stop {
        socket: Option<PathBuf>,
    },
    Restart {
        socket: Option<PathBuf>,
        wait: bool,
    },
    RestartSuccessor {
        socket: PathBuf,
        restart_id: String,
        predecessor_instance_id: String,
        target_generation: String,
        protocol_version: u32,
        handoff_fd: i32,
    },
    Status {
        socket: Option<PathBuf>,
    },
    Gateway {
        stdio: bool,
        socket: Option<PathBuf>,
    },
    Install,
    Uninstall,
    Upgrade,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PidFileState {
    Missing,
    Running(u32),
    Dead(u32),
    Malformed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DaemonRuntimePaths {
    socket: PathBuf,
    pid: PathBuf,
    lock: PathBuf,
}

fn socket_arg() -> impl bpaf::Parser<Option<PathBuf>> {
    bpaf::long("socket")
        .help("Override socket path")
        .argument::<PathBuf>("PATH")
        .optional()
}

fn start_cmd() -> impl bpaf::Parser<Cli> {
    let fg = bpaf::short('f')
        .long("fg")
        .help("Run in foreground")
        .switch();
    let force = bpaf::short('F')
        .long("force")
        .help("Kill any running daemon and start fresh")
        .switch();
    let preserve_restart_fence = bpaf::long("preserve-restart-fence")
        .help("Internal: keep a durable restart cancellation fence")
        .switch();
    let socket = socket_arg();
    bpaf::construct!(Cli::Start {
        fg,
        force,
        preserve_restart_fence,
        socket,
    })
    .to_options()
    .command("start")
    .help("Start the daemon")
}

fn stop_cmd() -> impl bpaf::Parser<Cli> {
    let socket = socket_arg();
    bpaf::construct!(Cli::Stop { socket })
        .to_options()
        .command("stop")
        .help("Stop a running daemon")
}

fn restart_cmd() -> impl bpaf::Parser<Cli> {
    let socket = socket_arg();
    let wait = bpaf::long("wait")
        .help("Wait until the successor daemon passes readiness checks")
        .switch();
    bpaf::construct!(Cli::Restart { socket, wait })
        .to_options()
        .command("restart")
        .help("Drain active work and restart the daemon")
}

fn restart_successor_cmd() -> impl bpaf::Parser<Cli> {
    let socket = bpaf::long("socket").argument::<PathBuf>("PATH");
    let restart_id = bpaf::long("restart-id").argument::<String>("ID");
    let predecessor_instance_id = bpaf::long("predecessor-instance-id").argument::<String>("ID");
    let target_generation = bpaf::long("target-generation").argument::<String>("ID");
    let protocol_version = bpaf::long("protocol-version").argument::<u32>("VERSION");
    let handoff_fd = bpaf::long("handoff-fd").argument::<i32>("FD");
    bpaf::construct!(Cli::RestartSuccessor {
        socket,
        restart_id,
        predecessor_instance_id,
        target_generation,
        protocol_version,
        handoff_fd,
    })
    .to_options()
    .command("__restart-successor")
    .help("Internal restart successor watchdog")
}

fn status_cmd() -> impl bpaf::Parser<Cli> {
    let socket = socket_arg();
    bpaf::construct!(Cli::Status { socket })
        .to_options()
        .command("status")
        .help("Check daemon status")
}

fn gateway_cmd() -> impl bpaf::Parser<Cli> {
    let stdio = bpaf::long("stdio")
        .help("Relay the local IPC socket over stdin/stdout")
        .req_flag(true);
    let socket = socket_arg();
    bpaf::construct!(Cli::Gateway { stdio, socket })
        .to_options()
        .command("gateway")
        .help("Run a stateless IPC bridge")
}

fn install_cmd() -> impl bpaf::Parser<Cli> {
    bpaf::pure(Cli::Install)
        .to_options()
        .command("install")
        .help("Install cued as a system service (launchd on macOS, systemd on Linux)")
}

fn uninstall_cmd() -> impl bpaf::Parser<Cli> {
    bpaf::pure(Cli::Uninstall)
        .to_options()
        .command("uninstall")
        .help("Remove the installed cued service")
}

fn upgrade_cmd() -> impl bpaf::Parser<Cli> {
    bpaf::pure(Cli::Upgrade)
        .to_options()
        .command("upgrade")
        .help("Self-update cued from the latest GitHub Release")
}

fn cli() -> bpaf::OptionParser<Cli> {
    let parser = bpaf::construct!([
        start_cmd(),
        stop_cmd(),
        restart_cmd(),
        restart_successor_cmd(),
        status_cmd(),
        gateway_cmd(),
        install_cmd(),
        uninstall_cmd(),
        upgrade_cmd(),
    ]);
    parser
        .to_options()
        .version(env!("CARGO_PKG_VERSION"))
        .descr("cued — background daemon for cue-shell")
}

pub(crate) fn run() -> i32 {
    let parser = cli();
    let args = normalized_cli_args();
    let args = bpaf::Args::from(args.as_slice()).set_name("cued");
    let cmd = match parser.run_inner(args) {
        Ok(cmd) => cmd,
        Err(err) => {
            err.print_message(100);
            return err.exit_code();
        }
    };
    let result: Result<i32> = match cmd {
        Cli::Start {
            fg,
            force,
            preserve_restart_fence,
            socket,
        } => run_start(fg, force, preserve_restart_fence, socket),
        Cli::Stop { socket } => run_stop(socket).map(|()| 0),
        Cli::Restart { socket, wait } => run_restart(socket, wait).map(|()| 0),
        Cli::RestartSuccessor {
            socket,
            restart_id,
            predecessor_instance_id,
            target_generation,
            protocol_version,
            handoff_fd,
        } => run_restart_successor(
            socket,
            restart_id,
            predecessor_instance_id,
            target_generation,
            protocol_version,
            handoff_fd,
        )
        .map(|()| 0),
        Cli::Status { socket } => run_status(socket).map(|()| 0),
        Cli::Gateway { stdio, socket } => run_gateway(stdio, socket).map(|()| 0),
        Cli::Install => run_install().map(|()| 0),
        Cli::Uninstall => run_uninstall().map(|()| 0),
        Cli::Upgrade => run_upgrade().map(|()| 0),
    };
    match result {
        Ok(code) => code,
        Err(error) => {
            eprintln!("cued: {error:#}");
            1
        }
    }
}

// ── Start ──

fn run_start(
    fg: bool,
    force: bool,
    preserve_restart_fence: bool,
    socket_override: Option<PathBuf>,
) -> Result<i32> {
    let paths = daemon_runtime_paths(socket_override.as_deref())?;

    if force {
        if crate::lifecycle::restart_record_exists(&paths.socket)? {
            run_stop(socket_override.clone())?;
        }
        // When the service manager owns cued, delegate restart to it rather than
        // sending a raw SIGTERM (which would fight launchd/systemd's KeepAlive).
        if crate::service::is_installed()? && socket_override.is_none() {
            wait_and_clear_cancelled_restart(&paths, Duration::from_secs(10))?;
            println!("cued: daemon is managed by the service manager — restarting via service");
            crate::service::restart()?;
            println!("cued: daemon restarted");
            return Ok(0);
        }
        force_stop_if_running_with_pid_path(&paths.pid, &paths.socket, &paths.lock)?;
    } else {
        // This is only a fast preflight. The foreground process repeats the
        // liveness check and any stale cleanup while holding `paths.lock`.
        ensure_socket_not_live(&paths.socket, "startup preflight")?;
    }

    if fg {
        return run_start_foreground(paths, !preserve_restart_fence);
    }

    clear_cancelled_restart_for_explicit_start(&paths)?;
    run_start_background(socket_override, false)?;
    Ok(0)
}

fn clear_cancelled_restart_for_explicit_start(paths: &DaemonRuntimePaths) -> Result<()> {
    let instance_lock = acquire_instance_lock(&paths.lock).with_context(|| {
        format!(
            "cannot clear a cancelled restart while {} is still owned",
            paths.lock.display()
        )
    })?;
    // This is an explicit supersession of stop, not background GC. Once the
    // user asks to start again, any already queued launcher for this same
    // binary/socket is authorized; the instance lock still elects one owner.
    crate::lifecycle::clear_cancelled_restart_record(&paths.socket)?;
    drop(instance_lock);
    Ok(())
}

fn wait_and_clear_cancelled_restart(paths: &DaemonRuntimePaths, timeout: Duration) -> Result<()> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Ok(instance_lock) = acquire_instance_lock(&paths.lock) {
            crate::lifecycle::clear_cancelled_restart_record(&paths.socket)?;
            drop(instance_lock);
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            anyhow::bail!(
                "daemon did not release {} within {} s",
                paths.lock.display(),
                timeout.as_secs()
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn run_restart(socket_override: Option<PathBuf>, wait: bool) -> Result<()> {
    let paths = daemon_runtime_paths(socket_override.as_deref())?;
    let socket_path = paths.socket.clone();
    if !daemon_responding(&socket_path) {
        if let PidFileState::Running(pid) = inspect_pid_file(&paths.pid)? {
            anyhow::bail!(
                "refusing restart: cued pid {pid} is alive but {} is not ready; no force-stop was attempted",
                socket_path.display()
            );
        }
        let lock = acquire_instance_lock(&paths.lock).with_context(|| {
            format!(
                "refusing restart while daemon ownership of {} is ambiguous",
                paths.lock.display()
            )
        })?;
        crate::lifecycle::clear_cancelled_restart_record(&socket_path)?;
        drop(lock);
        if crate::service::is_installed()?
            && socket_override.is_none()
            && socket_path == crate::dirs::socket_path()
        {
            crate::service::restart()?;
        } else {
            run_start_background(socket_override, false)?;
        }
        if wait {
            wait_for_any_ready_daemon(&socket_path, Duration::from_secs(60))?;
        }
        return Ok(());
    }

    let rt = tokio::runtime::Runtime::new().context("create restart request runtime")?;
    let accepted = rt.block_on(request_graceful_restart(&socket_path))?;
    println!(
        "cued: restart {} accepted by daemon {}",
        accepted.restart_id, accepted.predecessor.instance_id
    );
    if wait {
        rt.block_on(wait_for_successor(
            &socket_path,
            &accepted,
            Duration::from_secs(60),
        ))?;
        println!("cued: successor daemon is ready");
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DaemonIdentity {
    instance_id: String,
    generation_id: String,
    protocol_version: u32,
    ready: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AcceptedRestart {
    restart_id: String,
    predecessor: DaemonIdentity,
    target_generation: String,
}

fn is_exact_successor(identity: &DaemonIdentity, record: &crate::lifecycle::RestartRecord) -> bool {
    identity.instance_id != record.daemon_instance_id
        && identity.generation_id == record.target_generation
        && identity.protocol_version == record.protocol_version
}

async fn ping_daemon(socket_path: &Path) -> Result<DaemonIdentity> {
    let mut stream = tokio::time::timeout(
        Duration::from_secs(2),
        tokio::net::UnixStream::connect(socket_path),
    )
    .await
    .context("time out connecting to daemon")?
    .with_context(|| format!("connect to {}", socket_path.display()))?;
    let ping = cue_core::ipc::Message::Request {
        id: 0,
        operation_id: None,
        payload: cue_core::ipc::RequestPayload::Ping {},
    };
    crate::actor::gateway::write_message(&mut stream, &ping).await?;
    let response = tokio::time::timeout(
        Duration::from_secs(2),
        crate::actor::gateway::read_message(&mut stream),
    )
    .await
    .context("time out waiting for daemon Pong")??;
    match response {
        cue_core::ipc::Message::Response {
            id: 0,
            payload:
                cue_core::ipc::ResponsePayload::Ok(cue_core::ipc::OkPayload::Pong {
                    instance_id,
                    generation_id,
                    protocol_version,
                    ready,
                    ..
                }),
        } => Ok(DaemonIdentity {
            instance_id,
            generation_id,
            protocol_version,
            ready,
        }),
        other => anyhow::bail!("daemon returned unexpected Ping response: {other:?}"),
    }
}

async fn request_graceful_restart(socket_path: &Path) -> Result<AcceptedRestart> {
    let predecessor = ping_daemon(socket_path).await?;
    let mut stream = tokio::time::timeout(
        Duration::from_secs(2),
        tokio::net::UnixStream::connect(socket_path),
    )
    .await
    .context("time out connecting for restart")?
    .with_context(|| format!("connect to {}", socket_path.display()))?;
    let request = cue_core::ipc::Message::Request {
        id: 0,
        operation_id: None,
        payload: cue_core::ipc::RequestPayload::Restart {},
    };
    crate::actor::gateway::write_message(&mut stream, &request).await?;
    let response = tokio::time::timeout(
        Duration::from_secs(5),
        crate::actor::gateway::read_message(&mut stream),
    )
    .await
    .context(
        "restart acknowledgement was not received; the daemon may already be draining (no force-stop was attempted)",
    )??;
    match response {
        cue_core::ipc::Message::Response {
            id: 0,
            payload:
                cue_core::ipc::ResponsePayload::Ok(cue_core::ipc::OkPayload::RestartAccepted {
                    restart_id,
                    daemon_instance_id,
                    target_generation,
                }),
        } => {
            anyhow::ensure!(
                daemon_instance_id == predecessor.instance_id,
                "restart was accepted by daemon {daemon_instance_id}, but Ping identified {}",
                predecessor.instance_id
            );
            Ok(AcceptedRestart {
                restart_id,
                predecessor,
                target_generation,
            })
        }
        cue_core::ipc::Message::Response {
            payload: cue_core::ipc::ResponsePayload::Err { code, message },
            ..
        } => anyhow::bail!("daemon rejected restart ({code}): {message}"),
        other => anyhow::bail!("daemon returned unexpected restart response: {other:?}"),
    }
}

async fn wait_for_successor(
    socket_path: &Path,
    accepted: &AcceptedRestart,
    timeout: Duration,
) -> Result<DaemonIdentity> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Ok(identity) = ping_daemon(socket_path).await
            && identity.instance_id != accepted.predecessor.instance_id
            && identity.generation_id == accepted.target_generation
            && identity.protocol_version == accepted.predecessor.protocol_version
            && identity.ready
        {
            return Ok(identity);
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "successor for restart {} was not ready within {} s; drain/restart continues and no task was killed",
                accepted.restart_id,
                timeout.as_secs()
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn wait_for_any_ready_daemon(socket_path: &Path, timeout: Duration) -> Result<()> {
    let rt = tokio::runtime::Runtime::new().context("create readiness runtime")?;
    rt.block_on(async {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if ping_daemon(socket_path)
                .await
                .is_ok_and(|identity| identity.ready)
            {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!("daemon was not ready within {} s", timeout.as_secs());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
}

fn run_start_background(
    socket_override: Option<PathBuf>,
    preserve_restart_fence: bool,
) -> Result<()> {
    let socket_path = daemon_socket_path(socket_override.as_deref())?;
    let current_exe = std::env::current_exe().context("resolve current cued executable")?;

    let mut cmd = Command::new(current_exe);
    cmd.arg("start").arg("--fg");
    if preserve_restart_fence {
        cmd.arg("--preserve-restart-fence");
    }
    if let Some(path) = &socket_override {
        cmd.arg("--socket").arg(path);
    }
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    #[cfg(unix)]
    unsafe {
        cmd.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }

    let mut child = cmd.spawn().context("spawn background cued")?;
    let child_pid = child.id();

    for _ in 0..20 {
        if let Some(status) = child.try_wait().context("poll background cued")? {
            anyhow::bail!("background cued exited early with status {status}");
        }
        if daemon_ready(&socket_path) {
            println!(
                "cued started in background (pid {}, socket {})",
                child_pid,
                socket_path.display()
            );
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    println!(
        "cued is starting in background (pid {}, socket {})",
        child_pid,
        socket_path.display()
    );
    Ok(())
}

const RESTART_HANDOFF_WAITING: &str = "WAITING";
const RESTART_HANDOFF_COMMIT: &str = "COMMIT";
const RESTART_HANDOFF_ACTIVE: &str = "ACTIVE";
const RESTART_HANDOFF_ABORT: &str = "ABORT";
const RESTART_HANDOFF_MAX_FRAME: usize = 32;

pub(crate) struct RestartSuccessorHandoff {
    child: Option<Child>,
    control: StdUnixStream,
    detached: bool,
}

impl RestartSuccessorHandoff {
    fn expect_signal(&mut self, expected: &str) -> Result<()> {
        let signal = read_restart_handoff_signal(&mut self.control)?
            .context("restart successor closed its handoff channel")?;
        anyhow::ensure!(
            signal == expected,
            "restart successor sent {signal:?}, expected {expected:?}"
        );
        Ok(())
    }

    fn terminate_child_and_wait(&mut self) -> Result<()> {
        let _ = write_restart_handoff_signal(&mut self.control, RESTART_HANDOFF_ABORT);
        let _ = self.control.shutdown(std::net::Shutdown::Both);
        let Some(child) = self.child.as_mut() else {
            return Ok(());
        };
        if child
            .try_wait()
            .context("poll restart successor helper before termination")?
            .is_none()
        {
            match child.kill() {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::InvalidInput => {}
                Err(error) => {
                    return Err(error).context("terminate restart successor helper");
                }
            }
            child
                .wait()
                .context("wait for terminated restart successor helper")?;
        }
        self.child = None;
        Ok(())
    }
}

impl crate::lifecycle::RestartWatchdogHandoff for RestartSuccessorHandoff {
    fn activate(&mut self) -> Result<()> {
        write_restart_handoff_signal(&mut self.control, RESTART_HANDOFF_COMMIT)
            .context("commit durable restart fence to successor helper")?;
        self.expect_signal(RESTART_HANDOFF_ACTIVE)
            .context("wait for successor helper to verify the exact Armed fence")
    }

    fn terminate_and_reap(&mut self) -> Result<()> {
        self.terminate_child_and_wait()
    }

    fn detach(&mut self) {
        self.detached = true;
        // Dropping Child does not terminate it. Once ACTIVE has been observed,
        // the durable exact ticket is the helper's source of authority.
        self.child.take();
    }
}

impl Drop for RestartSuccessorHandoff {
    fn drop(&mut self) {
        if !self.detached && self.child.is_some() {
            let _ = self.terminate_child_and_wait();
        }
    }
}

pub(crate) fn spawn_restart_successor(
    socket_path: &Path,
    intent: &crate::lifecycle::RestartRecord,
) -> Result<RestartSuccessorHandoff> {
    let (parent_control, child_control) =
        StdUnixStream::pair().context("create restart successor handoff socket pair")?;
    let handoff_fd = child_control.as_raw_fd();
    let timeout = Some(Duration::from_secs(10));
    parent_control
        .set_read_timeout(timeout)
        .context("set restart handoff read timeout")?;
    parent_control
        .set_write_timeout(timeout)
        .context("set restart handoff write timeout")?;

    let current_exe = std::env::current_exe().context("resolve current cued executable")?;
    let mut cmd = Command::new(current_exe);
    cmd.arg("__restart-successor")
        .arg("--socket")
        .arg(socket_path)
        .arg("--restart-id")
        .arg(&intent.restart_id)
        .arg("--predecessor-instance-id")
        .arg(&intent.daemon_instance_id)
        .arg("--target-generation")
        .arg(&intent.target_generation)
        .arg("--protocol-version")
        .arg(intent.protocol_version.to_string())
        .arg("--handoff-fd")
        .arg(handoff_fd.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(unix)]
    unsafe {
        cmd.pre_exec(move || {
            let flags = libc::fcntl(handoff_fd, libc::F_GETFD);
            if flags == -1
                || libc::fcntl(handoff_fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) == -1
            {
                return Err(std::io::Error::last_os_error());
            }
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = cmd
        .spawn()
        .context("spawn daemon restart successor watchdog")?;
    drop(child_control);

    let mut handoff = RestartSuccessorHandoff {
        child: Some(child),
        control: parent_control,
        detached: false,
    };
    if let Err(error) = handoff.expect_signal(RESTART_HANDOFF_WAITING) {
        let reap = handoff.terminate_child_and_wait();
        return match reap {
            Ok(()) => Err(error.context("successor helper did not reach WAITING")),
            Err(reap_error) => Err(anyhow::anyhow!(
                "successor helper did not reach WAITING: {error:#}; helper could not be reaped: {reap_error:#}"
            )),
        };
    }
    Ok(handoff)
}

fn run_restart_successor(
    socket_path: PathBuf,
    restart_id: String,
    predecessor_instance_id: String,
    target_generation: String,
    protocol_version: u32,
    handoff_fd: i32,
) -> Result<()> {
    validate_daemon_socket_path("--socket", &socket_path)?;
    anyhow::ensure!(handoff_fd >= 3, "invalid restart handoff fd {handoff_fd}");
    // SAFETY: this internal subcommand receives sole ownership of the inherited
    // socket endpoint. The parent clears CLOEXEC only in the forked child.
    let mut control = unsafe { StdUnixStream::from_raw_fd(handoff_fd) };
    let rt = tokio::runtime::Runtime::new().context("create successor watchdog runtime")?;
    rt.block_on(async {
        let Some(_watchdog_lock) = claim_restart_watchdog(&socket_path, &mut control).await?
        else {
            return Ok(());
        };

        let authorized = complete_restart_successor_handoff(&mut control, || {
            crate::lifecycle::restart_intent_matches_ticket(
                &socket_path,
                &restart_id,
                &predecessor_instance_id,
                &target_generation,
                protocol_version,
            )
        })?;
        if !authorized {
            return Ok(());
        }

        let lock_path = crate::dirs::lock_path_for_socket(&socket_path);
        let mut last_start_attempt = None;
        loop {
            let Some(record) = crate::lifecycle::restart_record_for_ticket(
                &socket_path,
                &restart_id,
                &predecessor_instance_id,
                &target_generation,
                protocol_version,
            )?
            else {
                return Ok(());
            };

            if let Ok(identity) = ping_daemon(&socket_path).await
                && is_exact_successor(&identity, &record)
                && identity.ready
            {
                return Ok(());
            }

            let owner_released = acquire_instance_lock(&lock_path)
                .map(|lock| {
                    drop(lock);
                    true
                })
                .unwrap_or(false);
            let may_retry = last_start_attempt.is_none_or(|attempt: tokio::time::Instant| {
                attempt.elapsed() >= Duration::from_secs(3)
            });
            if owner_released && may_retry {
                last_start_attempt = Some(tokio::time::Instant::now());
                let start_result = start_successor_for_record(
                    &record,
                    crate::service::start_if_needed,
                    || run_start_background(Some(socket_path.clone()), true),
                );
                if let Err(error) = start_result {
                    tracing::warn!(%error, %restart_id, "restart watchdog: successor start failed; retrying");
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
}

fn start_successor_for_record<S, D>(
    record: &crate::lifecycle::RestartRecord,
    service_start: S,
    direct_start: D,
) -> Result<()>
where
    S: FnOnce() -> Result<()>,
    D: FnOnce() -> Result<()>,
{
    if record.supervisor_restart {
        service_start()
    } else {
        direct_start()
    }
}

async fn claim_restart_watchdog(
    socket_path: &Path,
    control: &mut StdUnixStream,
) -> Result<Option<File>> {
    control
        .set_nonblocking(true)
        .context("set restart handoff nonblocking while claiming watchdog lock")?;
    loop {
        let mut parent_state = [0_u8; RESTART_HANDOFF_MAX_FRAME];
        match control.read(&mut parent_state) {
            Ok(0) => return Ok(None),
            Ok(_) => return Ok(None),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error).context("check restart handoff parent liveness"),
        }
        if let Some(lock) = acquire_restart_watchdog_lock(socket_path)? {
            control
                .set_nonblocking(false)
                .context("restore blocking restart handoff channel")?;
            return Ok(Some(lock));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn write_restart_handoff_signal(stream: &mut StdUnixStream, signal: &str) -> Result<()> {
    stream
        .write_all(format!("{signal}\n").as_bytes())
        .with_context(|| format!("write restart handoff signal {signal}"))
}

fn read_restart_handoff_signal(stream: &mut StdUnixStream) -> Result<Option<String>> {
    let mut frame = Vec::with_capacity(RESTART_HANDOFF_MAX_FRAME);
    loop {
        let mut byte = [0_u8; 1];
        match stream.read(&mut byte) {
            Ok(0) if frame.is_empty() => return Ok(None),
            Ok(0) => anyhow::bail!("restart handoff closed with a partial frame"),
            Ok(_) if byte[0] == b'\n' => {
                return String::from_utf8(frame)
                    .context("restart handoff signal is not UTF-8")
                    .map(Some);
            }
            Ok(_) => {
                anyhow::ensure!(
                    frame.len() < RESTART_HANDOFF_MAX_FRAME,
                    "restart handoff signal exceeds {RESTART_HANDOFF_MAX_FRAME} bytes"
                );
                frame.push(byte[0]);
            }
            Err(error) => return Err(error).context("read restart handoff signal"),
        }
    }
}

fn complete_restart_successor_handoff<F>(
    control: &mut StdUnixStream,
    exact_armed: F,
) -> Result<bool>
where
    F: Fn() -> Result<bool>,
{
    write_restart_handoff_signal(control, RESTART_HANDOFF_WAITING)
        .context("announce WAITING restart successor")?;
    let parent_signal = read_restart_handoff_signal(control)?;
    let authorized = match parent_signal.as_deref() {
        Some(RESTART_HANDOFF_COMMIT) | None => exact_armed()?,
        Some(RESTART_HANDOFF_ABORT) => false,
        Some(other) => anyhow::bail!("invalid restart handoff command {other:?}"),
    };
    if !authorized {
        return Ok(false);
    }
    // If the parent died after its durable CAS, ACTIVE cannot be delivered;
    // the exact Armed record still authorizes this helper to take over.
    if let Err(error) = write_restart_handoff_signal(control, RESTART_HANDOFF_ACTIVE)
        && !exact_armed()?
    {
        return Err(error).context("publish ACTIVE restart successor");
    }
    Ok(true)
}

fn acquire_restart_watchdog_lock(socket_path: &Path) -> Result<Option<File>> {
    let mut lock_path = socket_path.as_os_str().to_os_string();
    lock_path.push(".cued.restart.watchdog.lock");
    let lock_path = PathBuf::from(lock_path);
    let file = crate::dirs::open_private_read_write(&lock_path)
        .with_context(|| format!("open restart watchdog lock {}", lock_path.display()))?;
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        return Ok(Some(file));
    }
    let error = std::io::Error::last_os_error();
    if matches!(
        error.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::AlreadyExists
    ) {
        return Ok(None);
    }
    Err(error).with_context(|| format!("lock restart watchdog {}", lock_path.display()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ForegroundStartup {
    SuppressedByCancellation,
    Start(Option<crate::lifecycle::RestartRecord>),
}

fn prepare_foreground_startup(
    socket_path: &Path,
    clear_cancelled: bool,
) -> Result<ForegroundStartup> {
    if clear_cancelled {
        crate::lifecycle::clear_cancelled_restart_record(socket_path)?;
    }
    let restart = crate::lifecycle::restart_record_for_startup(socket_path)?;
    if restart
        .as_ref()
        .is_some_and(|record| record.phase == crate::lifecycle::RestartPhase::Cancelled)
    {
        return Ok(ForegroundStartup::SuppressedByCancellation);
    }
    Ok(ForegroundStartup::Start(restart))
}

fn run_start_foreground(paths: DaemonRuntimePaths, clear_cancelled: bool) -> Result<i32> {
    init_stderr_tracing("info")?;

    // Ensure directories exist.
    crate::dirs::ensure_dirs().context("create directories")?;

    // Hold the socket-specific lock for the entire daemon lifetime. All stale
    // marker cleanup happens only after this succeeds, closing the check/bind
    // race between concurrent foreground starts.
    let _instance_lock = acquire_instance_lock(&paths.lock)?;
    ensure_not_running_with_pid_path(&paths.pid, &paths.socket)?;

    let startup_restart = match prepare_foreground_startup(&paths.socket, clear_cancelled)? {
        ForegroundStartup::SuppressedByCancellation => {
            info!(
                socket = %paths.socket.display(),
                "cued startup suppressed by durable restart cancellation"
            );
            return Ok(DaemonExit::Stopped.code());
        }
        ForegroundStartup::Start(restart) => restart,
    };

    // Write PID file.
    let owner_pid = std::process::id();
    crate::dirs::write_private_file(&paths.pid, owner_pid.to_string().as_bytes())
        .with_context(|| format!("write PID file {}", paths.pid.display()))?;

    info!(
        version = crate::version(),
        pid = owner_pid,
        socket = %paths.socket.display(),
        "cued starting"
    );

    // Build Tokio runtime and run the async entry point.
    let rt = tokio::runtime::Runtime::new().context("create tokio runtime")?;
    let result = rt.block_on(async_main(paths.socket.clone(), startup_restart));
    rt.shutdown_timeout(Duration::from_secs(2));

    // Cleanup while the instance lock is still held. A failed bind must never
    // unlink a socket that belongs to another process.
    cleanup_owned_pid_file(&paths.pid, owner_pid);
    if result.is_ok() {
        cleanup_runtime_file(&paths.socket, "socket");
    }
    if result.is_ok() {
        info!("cued stopped");
    }
    result.map(DaemonExit::code)
}

async fn async_main(
    socket_path: PathBuf,
    startup_restart: Option<crate::lifecycle::RestartRecord>,
) -> Result<DaemonExit> {
    crate::initialize_daemon_generation(startup_restart.as_ref())?;
    let restart_ownership = restart_ownership(
        &socket_path,
        &crate::dirs::socket_path(),
        crate::service::current_process_ownership(),
    );
    let lifecycle = std::sync::Arc::new(crate::lifecycle::DaemonLifecycle::new_with_startup(
        socket_path.clone(),
        restart_ownership,
        startup_restart.clone(),
    ));
    // Load config.
    let config = crate::config::Config::load().context("load daemon config")?;

    // Open database.
    let db_path = crate::dirs::db_path()?;
    let scope_db = crate::storage::open_db(&db_path)
        .with_context(|| format!("open database {}", db_path.display()))?;
    let scheduler_db = crate::storage::open_db(&db_path)
        .with_context(|| format!("open database {}", db_path.display()))?;

    // Spawn all actors.
    let sys = crate::actor::spawn_all(
        socket_path.clone(),
        scope_db,
        scheduler_db,
        config,
        lifecycle.clone(),
    )
    .await?;

    if let Some(record) = startup_restart.as_ref()
        && let Some(exit) = finalize_startup_restart(&socket_path, record, &lifecycle, &sys).await?
    {
        drop(sys);
        tokio::time::sleep(Duration::from_millis(200)).await;
        return Ok(exit);
    }

    info!("cued ready — waiting for signals");

    // Wait for SIGTERM or SIGINT.
    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;
    let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())?;

    enum ExitRequest {
        Signal(&'static str),
        Restart,
    }

    let exit_request = tokio::select! {
        _ = sigterm.recv() => {
            info!("received SIGTERM");
            ExitRequest::Signal("SIGTERM")
        }
        _ = sigint.recv()  => {
            info!("received SIGINT");
            ExitRequest::Signal("SIGINT")
        }
        _ = lifecycle.wait_for_stop() => {
            info!("restart preparation failed; stopping closed admission state");
            ExitRequest::Signal("restart fail-stop")
        }
        _ = lifecycle.wait_for_restart() => ExitRequest::Restart,
    };

    let (shutdown_reason, daemon_exit) = match exit_request {
        ExitRequest::Signal(reason) => {
            lifecycle.cancel_restart_for_shutdown()?;
            (reason, DaemonExit::Stopped)
        }
        ExitRequest::Restart => {
            info!("cued draining accepted work before restart");
            tokio::select! {
                _ = lifecycle.wait_for_handoff_ready() => {
                    let exit = completed_restart_exit(lifecycle.supervisor_restart_requested());
                    ("restart", exit)
                },
                _ = sigterm.recv() => {
                    info!("received SIGTERM while draining; explicit stop wins");
                    lifecycle.cancel_restart_for_shutdown()?;
                    ("SIGTERM", DaemonExit::Stopped)
                }
                _ = sigint.recv() => {
                    info!("received SIGINT while draining; explicit stop wins");
                    lifecycle.cancel_restart_for_shutdown()?;
                    ("SIGINT", DaemonExit::Stopped)
                }
                _ = lifecycle.wait_for_stop() => {
                    info!("restart preparation failed while draining; stopping");
                    lifecycle.cancel_restart_for_shutdown()?;
                    ("restart fail-stop", DaemonExit::Stopped)
                }
            }
        }
    };

    // Graceful shutdown.
    info!("cued shutting down");
    sys.shutdown_with_reason(shutdown_reason).await;
    drop(sys);

    // Give actors a moment to drain.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    Ok(daemon_exit)
}

/// Commit an exact restart successor before opening any execution admission.
///
/// The gateway is already available for the self-Ping and control/status
/// traffic, while both gateway execution admission and scheduler cron/job
/// execution remain paused. Only a matching durable completion CAS may release
/// the scheduler, and lifecycle admission opens after that activation ACK.
async fn finalize_startup_restart(
    socket_path: &Path,
    record: &crate::lifecycle::RestartRecord,
    lifecycle: &crate::lifecycle::DaemonLifecycle,
    sys: &crate::actor::ActorSystem,
) -> Result<Option<DaemonExit>> {
    let exact_readiness = ping_daemon(socket_path)
        .await
        .is_ok_and(|identity| is_exact_successor(&identity, record));
    if !exact_readiness {
        info!(
            restart_id = %record.restart_id,
            "cued successor failed its exact self-readiness probe"
        );
        sys.shutdown_with_reason("restart readiness failed").await;
        return Ok(Some(completed_restart_exit(record.supervisor_restart)));
    }
    if let Err(error) = sys.activate_restart_successor().await {
        info!(
            restart_id = %record.restart_id,
            %error,
            "cued successor scheduler could not arm execution activation"
        );
        sys.shutdown_with_reason("restart activation arm failed")
            .await;
        return Ok(Some(completed_restart_exit(record.supervisor_restart)));
    }
    let completion = crate::lifecycle::complete_matching_armed_restart(socket_path, record)?;
    if completion == crate::lifecycle::RestartCompletion::CancelledOrReplaced {
        info!(
            restart_id = %record.restart_id,
            "cued successor startup cancelled before readiness commit"
        );
        sys.shutdown_with_reason("restart cancelled").await;
        return Ok(Some(DaemonExit::Stopped));
    }
    lifecycle.mark_startup_restart_completed();
    if tokio::time::timeout(Duration::from_secs(2), lifecycle.wait_for_execution_ready())
        .await
        .is_err()
    {
        info!(
            restart_id = %record.restart_id,
            "cued successor scheduler did not publish execution readiness"
        );
        sys.shutdown_with_reason("restart activation failed").await;
        return Ok(Some(completed_restart_exit(record.supervisor_restart)));
    }
    Ok(None)
}

// ── Stop ──

fn run_stop(socket_override: Option<PathBuf>) -> Result<()> {
    let paths = daemon_runtime_paths(socket_override.as_deref())?;
    run_stop_with_paths(&paths, STOP_WAIT_TIMEOUT)
}

fn run_stop_with_paths(paths: &DaemonRuntimePaths, timeout: Duration) -> Result<()> {
    // The tombstone linearizes stop against every current or delayed restart
    // helper. It does not, by itself, prove that the current daemon has exited.
    crate::lifecycle::cancel_restart_intent_for_stop(&paths.socket)?;
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(wait_for_stop_release(paths, timeout))
}

async fn wait_for_stop_release(paths: &DaemonRuntimePaths, timeout: Duration) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut shutdown_requested = false;
    let mut last_socket_error = None;

    loop {
        let last_lock_error = match acquire_instance_lock(&paths.lock) {
            Ok(instance_lock) => {
                drop(instance_lock);
                if shutdown_requested {
                    println!("cued: daemon stopped");
                } else {
                    println!("cued: no active daemon; restart handoff durably cancelled");
                }
                return Ok(());
            }
            Err(error) => format!("{error:#}"),
        };

        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "cued stop timed out after {} ms: daemon ownership lock {} was not released; last lock result: {}; last socket result: {}",
                timeout.as_millis(),
                paths.lock.display(),
                last_lock_error,
                last_socket_error.as_deref().unwrap_or("not attempted"),
            );
        }

        if !shutdown_requested {
            match tokio::net::UnixStream::connect(&paths.socket).await {
                Ok(mut stream) => {
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    match tokio::time::timeout(remaining, request_shutdown(&mut stream)).await {
                        Ok(Ok(())) => shutdown_requested = true,
                        Ok(Err(error)) => last_socket_error = Some(format!("{error:#}")),
                        Err(_) => {
                            last_socket_error = Some("shutdown request timed out".to_string())
                        }
                    }
                }
                Err(error) => {
                    last_socket_error =
                        Some(format!("connect to {}: {error}", paths.socket.display()));
                }
            }
        }

        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        tokio::time::sleep(STOP_POLL_INTERVAL.min(remaining)).await;
    }
}

async fn request_shutdown(stream: &mut tokio::net::UnixStream) -> Result<()> {
    let msg = cue_core::ipc::Message::Request {
        id: 0,
        operation_id: None,
        payload: cue_core::ipc::RequestPayload::Shutdown {},
    };
    crate::actor::gateway::write_message(stream, &msg).await?;

    match crate::actor::gateway::read_message(stream).await {
        Ok(cue_core::ipc::Message::Response { payload, .. }) => match payload {
            cue_core::ipc::ResponsePayload::Ok(_) => {
                println!("cued: shutdown acknowledged");
            }
            cue_core::ipc::ResponsePayload::Err { message, .. } => {
                error!(%message, "cued: shutdown error");
            }
        },
        Ok(_) => println!("cued: unexpected response"),
        Err(e) => {
            println!("cued: connection closed ({e}) — daemon likely stopped");
        }
    }
    Ok(())
}

// ── Status ──

fn run_status(socket_override: Option<PathBuf>) -> Result<()> {
    let paths = daemon_runtime_paths(socket_override.as_deref())?;

    println!(
        "{}",
        daemon_status_message(&paths.pid, &paths.socket, is_process_alive, daemon_ready)?
    );
    Ok(())
}

// ── Gateway ──

fn run_gateway(stdio: bool, socket_override: Option<PathBuf>) -> Result<()> {
    anyhow::ensure!(stdio, "gateway currently supports only --stdio");

    let socket_path = daemon_socket_path(socket_override.as_deref())?;
    let rt = tokio::runtime::Runtime::new().context("create tokio runtime")?;
    rt.block_on(crate::gateway_stdio::run(socket_path))
}

// ── Install / Uninstall / Upgrade ──

fn run_install() -> Result<()> {
    let exe = std::env::current_exe().context("resolve current executable path")?;
    crate::service::install(&exe)
}

fn run_uninstall() -> Result<()> {
    crate::service::uninstall()
}

fn run_upgrade() -> Result<()> {
    crate::upgrade::run_upgrade()
}

// ── Helpers ──

/// Check if a process is alive using `kill(pid, 0)`.
fn is_process_alive(pid: u32) -> bool {
    // SAFETY: signal 0 doesn't send a signal, just checks existence.
    unsafe { libc_kill(pid as i32, 0) == 0 }
}

unsafe fn libc_kill(pid: i32, sig: i32) -> i32 {
    unsafe { libc_kill_ffi(pid, sig) }
}

unsafe extern "C" {
    #[link_name = "kill"]
    fn libc_kill_ffi(pid: i32, sig: i32) -> i32;
}

fn normalized_cli_args() -> Vec<OsString> {
    let args: Vec<OsString> = std::env::args_os().skip(1).collect();
    normalize_cli_args_vec(args)
}

fn normalize_cli_args_vec(mut args: Vec<OsString>) -> Vec<OsString> {
    if should_insert_start(&args) {
        args.insert(0, OsString::from("start"));
    }
    args
}

fn should_insert_start(args: &[OsString]) -> bool {
    if args.is_empty() {
        return false;
    }

    match args[0].to_str() {
        Some(
            "start"
            | "stop"
            | "status"
            | "gateway"
            | "install"
            | "uninstall"
            | "upgrade"
            | "-h"
            | "restart"
            | "__restart-successor"
            | "--help"
            | "-V"
            | "--version",
        ) => false,
        _ => implicit_start_args_only(args),
    }
}

fn implicit_start_args_only(args: &[OsString]) -> bool {
    let mut expecting_socket_path = false;
    for arg in args {
        if expecting_socket_path {
            expecting_socket_path = false;
            continue;
        }

        let Some(arg) = arg.to_str() else {
            return false;
        };

        match arg {
            "-f" | "--fg" | "-F" | "--force" => {}
            "--socket" => expecting_socket_path = true,
            _ if arg.starts_with("--socket=") => {}
            _ => return false,
        }
    }

    !expecting_socket_path
}

fn daemon_socket_path(socket_override: Option<&Path>) -> Result<PathBuf> {
    match socket_override {
        Some(path) => {
            validate_daemon_socket_path("--socket", path)?;
            Ok(path.to_path_buf())
        }
        None => Ok(crate::dirs::socket_path()),
    }
}

fn daemon_runtime_paths(socket_override: Option<&Path>) -> Result<DaemonRuntimePaths> {
    let socket = daemon_socket_path(socket_override)?;
    Ok(DaemonRuntimePaths {
        pid: crate::dirs::pid_path_for_socket(&socket),
        lock: crate::dirs::lock_path_for_socket(&socket),
        socket,
    })
}

fn validate_daemon_socket_path(field: &str, path: &Path) -> Result<()> {
    let Some(path) = path.to_str() else {
        anyhow::bail!("{field} must be valid UTF-8");
    };
    if path.trim().is_empty() {
        anyhow::bail!("{field} must not be empty");
    }
    if path.trim() != path {
        anyhow::bail!("{field} must not have leading or trailing whitespace");
    }
    Ok(())
}

const RUST_LOG_ENV: &str = "RUST_LOG";

fn init_stderr_tracing(default_directive: &str) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(env_filter_from_env(
            default_directive,
            std::env::var_os(RUST_LOG_ENV),
        )?)
        .with_writer(std::io::stderr)
        .try_init()
        .map_err(|error| anyhow::anyhow!("initialize tracing subscriber: {error}"))
}

fn env_filter_from_env(
    default_directive: &str,
    rust_log: Option<OsString>,
) -> Result<tracing_subscriber::EnvFilter> {
    let Some(rust_log) = rust_log else {
        return default_env_filter(default_directive);
    };
    if rust_log.is_empty() {
        anyhow::bail!("{RUST_LOG_ENV} must not be empty");
    }
    let Some(rust_log) = rust_log.to_str() else {
        anyhow::bail!("{RUST_LOG_ENV} must be valid UTF-8");
    };
    tracing_subscriber::EnvFilter::try_new(rust_log)
        .with_context(|| format!("parse {RUST_LOG_ENV} `{rust_log}`"))
}

fn default_env_filter(default_directive: &str) -> Result<tracing_subscriber::EnvFilter> {
    tracing_subscriber::EnvFilter::try_new(default_directive)
        .with_context(|| format!("parse default tracing directive `{default_directive}`"))
}

fn acquire_instance_lock(lock_path: &Path) -> Result<File> {
    let file = crate::dirs::open_private_read_write(lock_path)
        .with_context(|| format!("open daemon lock {}", lock_path.display()))?;
    // SAFETY: `file` owns a valid descriptor for the full duration of the
    // call, and the returned `File` keeps the successful advisory lock alive.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        return Ok(file);
    }

    let error = std::io::Error::last_os_error();
    if matches!(
        error.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::AlreadyExists
    ) {
        anyhow::bail!(
            "another cued instance is starting or running (lock {})",
            lock_path.display()
        );
    }
    Err(error).with_context(|| format!("lock daemon instance {}", lock_path.display()))
}

/// Stop any running daemon and wait for it to release the socket-specific lock.
fn force_stop_if_running_with_pid_path(
    pid_path: &Path,
    socket_path: &Path,
    lock_path: &Path,
) -> Result<()> {
    let pid_state = inspect_pid_file(pid_path)?;
    if daemon_ready(socket_path) {
        request_confirmed_shutdown(socket_path)?;
    } else if let PidFileState::Running(pid) = pid_state {
        terminate_verified_unreachable_daemon(pid, pid_path, socket_path, lock_path)?;
    } else {
        return Ok(());
    }

    wait_for_daemon_release(socket_path, lock_path)
}

fn wait_for_daemon_release(socket_path: &Path, lock_path: &Path) -> Result<()> {
    for _ in 0..50 {
        if !daemon_responding(socket_path)
            && let Ok(lock) = acquire_instance_lock(lock_path)
        {
            drop(lock);
            println!("cued: previous daemon stopped");
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    anyhow::bail!(
        "confirmed cued daemon on {} did not release its socket and lock within 5 s",
        socket_path.display()
    );
}

fn terminate_verified_unreachable_daemon(
    pid: u32,
    pid_path: &Path,
    socket_path: &Path,
    lock_path: &Path,
) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        let refusal = || {
            format!(
                "refusing --force: PID marker {} names live pid {pid}, but socket {} cannot be confirmed as cued",
                pid_path.display(),
                socket_path.display()
            )
        };
        let pidfd = open_pidfd(pid).with_context(refusal)?;
        anyhow::ensure!(
            process_executable_matches_current(pid).with_context(refusal)?,
            "refusing --force: PID marker {} names live pid {pid}, but that process is not this cued executable",
            pid_path.display()
        );
        anyhow::ensure!(
            process_holds_daemon_lock(pid, lock_path).with_context(refusal)?,
            "refusing --force: PID marker {} names live pid {pid}, but that process does not hold daemon lock {}",
            pid_path.display(),
            lock_path.display()
        );

        println!(
            "cued: socket {} is unreachable; stopping verified cued pid {pid}",
            socket_path.display()
        );
        send_pidfd_sigterm(&pidfd, pid)?;
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = lock_path;
        anyhow::bail!(
            "refusing --force: PID marker {} names live pid {pid}, but socket {} cannot be confirmed as cued",
            pid_path.display(),
            socket_path.display()
        );
    }
}

#[cfg(target_os = "linux")]
fn open_pidfd(pid: u32) -> Result<OwnedFd> {
    // SAFETY: pidfd_open does not dereference userspace pointers. The returned
    // descriptor pins process identity so PID reuse cannot redirect SIGTERM.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("open pidfd for pid {pid}"));
    }
    // SAFETY: a successful pidfd_open returns a new owned descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(fd as i32) })
}

#[cfg(target_os = "linux")]
fn send_pidfd_sigterm(pidfd: &OwnedFd, pid: u32) -> Result<()> {
    // SAFETY: the pidfd is owned and valid, the signal is a standard POSIX
    // value, and a null siginfo pointer is supported by pidfd_send_signal.
    let result = unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            pidfd.as_raw_fd(),
            libc::SIGTERM,
            std::ptr::null::<libc::siginfo_t>(),
            0,
        )
    };
    if result < 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("send SIGTERM to verified cued pid {pid}"));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn process_executable_matches_current(pid: u32) -> Result<bool> {
    let running_exe = std::fs::read_link(format!("/proc/{pid}/exe"))
        .with_context(|| format!("inspect executable for pid {pid}"))?;
    let current_exe = std::env::current_exe().context("resolve current cued executable")?;
    Ok(normalize_proc_exe_path(&running_exe) == current_exe)
}

#[cfg(target_os = "linux")]
fn normalize_proc_exe_path(path: &Path) -> PathBuf {
    const DELETED_SUFFIX: &str = " (deleted)";
    path.to_str()
        .and_then(|value| value.strip_suffix(DELETED_SUFFIX))
        .map(PathBuf::from)
        .unwrap_or_else(|| path.to_path_buf())
}

#[cfg(target_os = "linux")]
fn process_holds_daemon_lock(pid: u32, lock_path: &Path) -> Result<bool> {
    let lock_metadata = std::fs::metadata(lock_path)
        .with_context(|| format!("inspect daemon lock {}", lock_path.display()))?;
    let fd_dir = PathBuf::from(format!("/proc/{pid}/fd"));
    let entries = std::fs::read_dir(&fd_dir)
        .with_context(|| format!("inspect file descriptors for pid {pid}"))?;

    for entry in entries {
        let entry = entry.with_context(|| format!("inspect file descriptor for pid {pid}"))?;
        let Ok(metadata) = std::fs::metadata(entry.path()) else {
            continue;
        };
        if metadata.dev() != lock_metadata.dev() || metadata.ino() != lock_metadata.ino() {
            continue;
        }

        let fd_name = entry.file_name();
        let fdinfo_path = PathBuf::from(format!("/proc/{pid}/fdinfo")).join(fd_name);
        let fdinfo = std::fs::read_to_string(&fdinfo_path)
            .with_context(|| format!("inspect daemon lock ownership for pid {pid}"))?;
        let pid_text = pid.to_string();
        if fdinfo.lines().any(|line| {
            let fields: Vec<_> = line.split_whitespace().collect();
            line.starts_with("lock:")
                && fields
                    .windows(2)
                    .any(|pair| pair == ["WRITE", pid_text.as_str()])
        }) {
            return Ok(true);
        }
    }

    Ok(false)
}

fn request_confirmed_shutdown(socket_path: &Path) -> Result<()> {
    let rt = tokio::runtime::Runtime::new().context("create shutdown confirmation runtime")?;
    rt.block_on(async {
        let mut stream = tokio::time::timeout(
            Duration::from_secs(2),
            tokio::net::UnixStream::connect(socket_path),
        )
        .await
        .context("time out connecting to candidate daemon")?
        .with_context(|| format!("connect to {}", socket_path.display()))?;

        let ping = cue_core::ipc::Message::Request {
            id: 0,
            operation_id: None,
            payload: cue_core::ipc::RequestPayload::Ping {},
        };
        crate::actor::gateway::write_message(&mut stream, &ping).await?;
        let response = tokio::time::timeout(
            Duration::from_secs(2),
            crate::actor::gateway::read_message(&mut stream),
        )
        .await
        .context("time out confirming candidate daemon")??;
        anyhow::ensure!(
            matches!(
                response,
                cue_core::ipc::Message::Response {
                    id: 0,
                    payload: cue_core::ipc::ResponsePayload::Ok(
                        cue_core::ipc::OkPayload::Pong { .. }
                    )
                }
            ),
            "refusing --force: socket {} did not return a cued Pong",
            socket_path.display()
        );

        let shutdown = cue_core::ipc::Message::Request {
            id: 1,
            operation_id: None,
            payload: cue_core::ipc::RequestPayload::Shutdown {},
        };
        crate::actor::gateway::write_message(&mut stream, &shutdown).await?;
        let _ = tokio::time::timeout(
            Duration::from_secs(2),
            crate::actor::gateway::read_message(&mut stream),
        )
        .await;
        Ok(())
    })
}

fn ensure_not_running_with_pid_path(pid_path: &Path, socket_path: &Path) -> Result<()> {
    ensure_not_running_with_pid_path_and_ready(pid_path, socket_path, daemon_ready)
}

fn ensure_not_running_with_pid_path_and_ready(
    pid_path: &Path,
    socket_path: &Path,
    daemon_is_ready: impl Fn(&Path) -> bool + Copy,
) -> Result<()> {
    match inspect_pid_file(pid_path)? {
        PidFileState::Running(pid) => {
            anyhow::bail!(
                "cued already running (pid {pid}). If stale, remove {} and retry.",
                pid_path.display()
            );
        }
        PidFileState::Dead(pid) => {
            remove_stale_daemon_markers_with_ready(
                pid_path,
                socket_path,
                &format!("PID file points to dead pid {pid}"),
                daemon_is_ready,
            )?;
        }
        PidFileState::Malformed => {
            remove_stale_daemon_markers_with_ready(
                pid_path,
                socket_path,
                "PID file is malformed",
                daemon_is_ready,
            )?;
        }
        PidFileState::Missing => {}
    }

    if daemon_is_ready(socket_path) {
        anyhow::bail!("cued already running on socket {}", socket_path.display());
    }

    remove_runtime_file(socket_path, "stale socket")?;
    Ok(())
}

fn inspect_pid_file(pid_path: &Path) -> Result<PidFileState> {
    inspect_pid_file_with(pid_path, is_process_alive)
}

fn inspect_pid_file_with(
    pid_path: &Path,
    is_alive: impl FnOnce(u32) -> bool,
) -> Result<PidFileState> {
    let content = match std::fs::read_to_string(pid_path) {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(PidFileState::Missing);
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("read cued PID file {}", pid_path.display()));
        }
    };

    let Ok(pid) = content.trim().parse::<u32>() else {
        return Ok(PidFileState::Malformed);
    };

    if is_alive(pid) {
        Ok(PidFileState::Running(pid))
    } else {
        Ok(PidFileState::Dead(pid))
    }
}

fn daemon_status_message(
    pid_path: &Path,
    socket_path: &Path,
    is_alive: impl FnOnce(u32) -> bool,
    daemon_is_ready: impl Fn(&Path) -> bool,
) -> Result<String> {
    let pid_state = inspect_pid_file_with(pid_path, is_alive)?;
    let socket_ready = daemon_is_ready(socket_path);

    let message = match (pid_state, socket_ready) {
        (PidFileState::Running(pid), true) => {
            format!(
                "cued is running (pid {pid}, socket {} reachable)",
                socket_path.display()
            )
        }
        (PidFileState::Running(pid), false) => {
            format!(
                "cued process is running (pid {pid}) but socket {} is not reachable",
                socket_path.display()
            )
        }
        (PidFileState::Dead(pid), true) => {
            format!(
                "cued is running (socket {} reachable, PID file points to dead pid {pid})",
                socket_path.display()
            )
        }
        (PidFileState::Dead(pid), false) => {
            format!("cued: stale PID file (pid {pid} not running)")
        }
        (PidFileState::Malformed, true) => {
            format!(
                "cued is running (socket {} reachable, PID file is malformed)",
                socket_path.display()
            )
        }
        (PidFileState::Malformed, false) => "cued: stale PID file (malformed)".to_string(),
        (PidFileState::Missing, true) => {
            format!(
                "cued is running (socket {} reachable, PID file is missing)",
                socket_path.display()
            )
        }
        (PidFileState::Missing, false) => "cued is not running".to_string(),
    };

    Ok(message)
}

fn remove_stale_daemon_markers_with_ready(
    pid_path: &Path,
    socket_path: &Path,
    reason: &str,
    daemon_is_ready: impl Fn(&Path) -> bool,
) -> Result<()> {
    ensure_socket_not_live_with(socket_path, reason, daemon_is_ready)?;
    remove_runtime_file(pid_path, "stale PID file")?;
    remove_runtime_file(socket_path, "stale socket")
}

fn ensure_socket_not_live(socket_path: &Path, reason: &str) -> Result<()> {
    ensure_socket_not_live_with(socket_path, reason, daemon_ready)
}

fn ensure_socket_not_live_with(
    socket_path: &Path,
    reason: &str,
    daemon_is_ready: impl Fn(&Path) -> bool,
) -> Result<()> {
    if daemon_is_ready(socket_path) {
        anyhow::bail!(
            "cued socket {} is reachable but {reason}; run `cued stop --socket {}` first",
            socket_path.display(),
            socket_path.display()
        );
    }
    Ok(())
}

fn cleanup_runtime_file(path: &Path, label: &str) {
    if let Err(error) = remove_runtime_file(path, label) {
        error!(
            %error,
            path = %path.display(),
            label,
            "failed to remove cued runtime file"
        );
    }
}

fn cleanup_owned_pid_file(pid_path: &Path, owner_pid: u32) {
    match std::fs::read_to_string(pid_path) {
        Ok(contents) if contents.trim().parse::<u32>() == Ok(owner_pid) => {
            cleanup_runtime_file(pid_path, "PID file");
        }
        Ok(contents) => {
            warn_owned_pid_mismatch(pid_path, owner_pid, contents.trim());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            error!(
                %error,
                path = %pid_path.display(),
                owner_pid,
                "failed to inspect cued PID file during cleanup"
            );
        }
    }
}

fn warn_owned_pid_mismatch(pid_path: &Path, owner_pid: u32, found: &str) {
    tracing::warn!(
        path = %pid_path.display(),
        owner_pid,
        found,
        "leaving PID file not owned by this daemon"
    );
}

fn remove_runtime_file(path: &Path, label: &str) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("remove cued {label} {}", path.display())),
    }
}

fn daemon_ready(socket_path: &Path) -> bool {
    daemon_pong_ready(socket_path) == Some(true)
}

fn daemon_responding(socket_path: &Path) -> bool {
    daemon_pong_ready(socket_path).is_some()
}

fn daemon_pong_ready(socket_path: &Path) -> Option<bool> {
    let Ok(mut stream) = StdUnixStream::connect(socket_path) else {
        return None;
    };
    let timeout = Some(Duration::from_secs(2));
    if stream.set_read_timeout(timeout).is_err() || stream.set_write_timeout(timeout).is_err() {
        return None;
    }
    let request = cue_core::ipc::Message::Request {
        id: 0,
        operation_id: None,
        payload: cue_core::ipc::RequestPayload::Ping {},
    };
    let Ok(encoded) = cue_core::ipc::encode_message(&request) else {
        return None;
    };
    if stream.write_all(&encoded).is_err() {
        return None;
    }
    let mut length = [0_u8; 4];
    if stream.read_exact(&mut length).is_err() {
        return None;
    }
    let length = u32::from_be_bytes(length) as usize;
    if length > cue_core::ipc::MAX_MESSAGE_SIZE {
        return None;
    }
    let mut body = vec![0_u8; length];
    if stream.read_exact(&mut body).is_err() {
        return None;
    }
    match serde_json::from_slice::<cue_core::ipc::Message>(&body) {
        Ok(cue_core::ipc::Message::Response {
            id: 0,
            payload:
                cue_core::ipc::ResponsePayload::Ok(cue_core::ipc::OkPayload::Pong { ready, .. }),
        }) => Some(ready),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn make_temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "cued-test-{}-{}",
            std::process::id(),
            TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn normalize(args: &[&str]) -> Vec<String> {
        normalize_cli_args_vec(args.iter().map(OsString::from).collect())
            .into_iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect()
    }

    fn parse(args: &[&str]) -> Cli {
        let args: Vec<OsString> = args.iter().map(OsString::from).collect();
        let args = bpaf::Args::from(args.as_slice()).set_name("cued");
        cli().run_inner(args).expect("parse CLI")
    }

    #[test]
    fn inserts_start_for_top_level_fg_flag() {
        assert_eq!(normalize(&["-f"]), vec!["start", "-f"]);
        assert_eq!(normalize(&["--fg"]), vec!["start", "--fg"]);
    }

    #[test]
    fn inserts_start_for_socket_override() {
        assert_eq!(
            normalize(&["--socket", "/tmp/cued.sock", "-f"]),
            vec!["start", "--socket", "/tmp/cued.sock", "-f"]
        );
        assert_eq!(
            normalize(&["--socket=/tmp/cued.sock"]),
            vec!["start", "--socket=/tmp/cued.sock"]
        );
    }

    #[test]
    fn preserves_real_subcommands_and_help() {
        assert_eq!(normalize(&["start", "--fg"]), vec!["start", "--fg"]);
        assert_eq!(normalize(&["restart"]), vec!["restart"]);
        assert_eq!(normalize(&["status"]), vec!["status"]);
        assert_eq!(
            normalize(&["gateway", "--stdio"]),
            vec!["gateway", "--stdio"]
        );
        assert_eq!(normalize(&["--help"]), vec!["--help"]);
    }

    #[test]
    fn does_not_rewrite_unknown_top_level_args() {
        assert_eq!(normalize(&["oops"]), vec!["oops"]);
    }

    #[test]
    fn parses_gateway_stdio_subcommand() {
        assert_eq!(
            parse(&["gateway", "--stdio", "--socket", "bridge.sock"]),
            Cli::Gateway {
                stdio: true,
                socket: Some(PathBuf::from("bridge.sock")),
            }
        );
    }

    #[test]
    fn parses_start_subcommand_and_internal_fence_flag() {
        assert_eq!(
            parse(&["start", "--fg"]),
            Cli::Start {
                fg: true,
                force: false,
                preserve_restart_fence: false,
                socket: None,
            }
        );
        assert_eq!(
            parse(&[
                "start",
                "--fg",
                "--preserve-restart-fence",
                "--socket",
                "daemon.sock",
            ]),
            Cli::Start {
                fg: true,
                force: false,
                preserve_restart_fence: true,
                socket: Some(PathBuf::from("daemon.sock")),
            }
        );
    }

    #[test]
    fn parses_restart_subcommand() {
        assert_eq!(
            parse(&["restart", "--socket", "daemon.sock"]),
            Cli::Restart {
                socket: Some(PathBuf::from("daemon.sock")),
                wait: false,
            }
        );
    }

    #[test]
    fn legacy_foreground_service_activation_honours_cancelled_tombstone() {
        let dir = make_temp_dir();
        let socket = dir.join("cued.sock");
        let cancelled = crate::lifecycle::RestartRecord {
            restart_id: "cancelled-restart".into(),
            daemon_instance_id: "old-instance".into(),
            protocol_version: cue_core::ipc::IPC_PROTOCOL_VERSION,
            target_generation: "cancelled-generation".into(),
            phase: crate::lifecycle::RestartPhase::Cancelled,
            supervisor_restart: true,
        };
        crate::lifecycle::write_restart_record(&socket, &cancelled).unwrap();

        assert_eq!(
            prepare_foreground_startup(&socket, false).unwrap(),
            ForegroundStartup::SuppressedByCancellation
        );
        assert_eq!(
            crate::lifecycle::restart_record_for_startup(&socket)
                .unwrap()
                .expect("legacy start --fg must retain tombstone")
                .phase,
            crate::lifecycle::RestartPhase::Cancelled
        );

        assert_eq!(
            prepare_foreground_startup(&socket, true).unwrap(),
            ForegroundStartup::Start(None)
        );
        std::fs::remove_dir_all(dir).expect("remove temp dir");
    }

    #[test]
    fn default_installed_restart_uses_supervisor_failure_exit() {
        let default_socket = PathBuf::from("/tmp/cued-default.sock");
        assert_eq!(
            restart_ownership(
                &default_socket,
                &default_socket,
                crate::service::CurrentProcessOwnership::Managed,
            ),
            crate::lifecycle::RestartOwnership::Supervisor
        );
        assert_eq!(
            restart_ownership(
                &default_socket,
                &default_socket,
                crate::service::CurrentProcessOwnership::Unknown,
            ),
            crate::lifecycle::RestartOwnership::Unknown
        );
        assert_eq!(completed_restart_exit(true).code(), 75);
        assert_eq!(completed_restart_exit(false).code(), 0);
        assert_eq!(
            restart_ownership(
                &default_socket,
                &default_socket,
                crate::service::CurrentProcessOwnership::NotManaged,
            ),
            crate::lifecycle::RestartOwnership::Standalone
        );
        assert_eq!(
            restart_ownership(
                Path::new("/tmp/cued-custom.sock"),
                &default_socket,
                crate::service::CurrentProcessOwnership::Managed,
            ),
            crate::lifecycle::RestartOwnership::Standalone
        );
    }

    #[test]
    fn watchdog_start_owner_is_fixed_by_the_durable_record() {
        let manager_calls = std::cell::Cell::new(0);
        let direct_calls = std::cell::Cell::new(0);
        let mut record = crate::lifecycle::RestartRecord {
            restart_id: "restart".into(),
            daemon_instance_id: "old".into(),
            protocol_version: cue_core::ipc::IPC_PROTOCOL_VERSION,
            target_generation: "target".into(),
            phase: crate::lifecycle::RestartPhase::Armed,
            supervisor_restart: true,
        };

        start_successor_for_record(
            &record,
            || {
                manager_calls.set(manager_calls.get() + 1);
                Ok(())
            },
            || {
                direct_calls.set(direct_calls.get() + 1);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!((manager_calls.get(), direct_calls.get()), (1, 0));

        record.supervisor_restart = false;
        start_successor_for_record(
            &record,
            || {
                manager_calls.set(manager_calls.get() + 1);
                Ok(())
            },
            || {
                direct_calls.set(direct_calls.get() + 1);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!((manager_calls.get(), direct_calls.get()), (1, 1));
    }

    #[test]
    fn readiness_fence_requires_exact_successor_identity() {
        let record = crate::lifecycle::RestartRecord {
            restart_id: "restart".into(),
            daemon_instance_id: "old".into(),
            protocol_version: cue_core::ipc::IPC_PROTOCOL_VERSION,
            target_generation: "target".into(),
            phase: crate::lifecycle::RestartPhase::Armed,
            supervisor_restart: false,
        };
        let exact = DaemonIdentity {
            instance_id: "new".into(),
            generation_id: "target".into(),
            protocol_version: cue_core::ipc::IPC_PROTOCOL_VERSION,
            ready: true,
        };
        assert!(is_exact_successor(&exact, &record));

        let mut wrong = exact.clone();
        wrong.instance_id = "old".into();
        assert!(!is_exact_successor(&wrong, &record));
        wrong = exact.clone();
        wrong.generation_id = "other".into();
        assert!(!is_exact_successor(&wrong, &record));
        wrong = exact;
        wrong.protocol_version += 1;
        assert!(!is_exact_successor(&wrong, &record));
    }

    fn startup_restart_record(socket: &Path, restart_id: &str) -> crate::lifecycle::RestartRecord {
        let record = crate::lifecycle::RestartRecord {
            restart_id: restart_id.into(),
            daemon_instance_id: format!("predecessor-{restart_id}"),
            protocol_version: cue_core::ipc::IPC_PROTOCOL_VERSION,
            target_generation: crate::daemon_generation_id().to_string(),
            phase: crate::lifecycle::RestartPhase::Armed,
            supervisor_restart: false,
        };
        crate::lifecycle::write_restart_record(socket, &record).expect("write restart record");
        record
    }

    fn startup_actor_databases(
        dir: &Path,
        cron_marker: &Path,
    ) -> (rusqlite::Connection, rusqlite::Connection) {
        let db_path = dir.join("startup-gate.db");
        let setup = crate::storage::open_db(&db_path).expect("open startup gate database");
        let scope = cue_core::scope::Scope::root(cue_core::scope::EnvSnapshot {
            env: BTreeMap::from([("PATH".into(), "/usr/bin:/bin".into())]),
            cwd: dir.to_path_buf(),
        });
        assert_eq!(
            crate::storage::insert_scope(&setup, &scope).expect("insert cron scope"),
            crate::storage::ScopePersistence::Persisted
        );
        crate::storage::upsert_cron(
            &setup,
            &crate::storage::StoredCron {
                id: "C1".into(),
                session_id: None,
                schedule: "every 100ms".into(),
                command: format!("/usr/bin/touch {}", cron_marker.display()),
                status: cue_core::cron::CronStatus::Scheduled,
                scope_hash: Some(scope.hash),
                cwd_override: None,
                scope_enabled: false,
                wrapper_enabled: false,
            },
        )
        .expect("insert startup cron");
        drop(setup);
        (
            crate::storage::open_db(&db_path).expect("open scope database"),
            crate::storage::open_db(&db_path).expect("open scheduler database"),
        )
    }

    async fn startup_roundtrip(
        stream: &mut tokio::net::UnixStream,
        id: u32,
        payload: cue_core::ipc::RequestPayload,
    ) -> cue_core::ipc::ResponsePayload {
        crate::actor::gateway::write_message(
            stream,
            &cue_core::ipc::Message::Request {
                id,
                operation_id: None,
                payload,
            },
        )
        .await
        .expect("write startup gate request");
        loop {
            match crate::actor::gateway::read_message(stream)
                .await
                .expect("read startup gate response")
            {
                cue_core::ipc::Message::Response {
                    id: response_id,
                    payload,
                } if response_id == id => return payload,
                _ => {}
            }
        }
    }

    async fn connect_starting_successor(socket: &Path, cwd: &Path) -> tokio::net::UnixStream {
        let mut stream = tokio::net::UnixStream::connect(socket)
            .await
            .expect("connect starting successor");
        let response = startup_roundtrip(
            &mut stream,
            1,
            cue_core::ipc::RequestPayload::Handshake {
                session_id: format!("startup-gate:{}", socket.display()),
                cwd: cwd.to_string_lossy().into_owned(),
                env: BTreeMap::from([("PATH".into(), "/usr/bin:/bin".into())]),
                refresh: false,
            },
        )
        .await;
        assert!(matches!(
            response,
            cue_core::ipc::ResponsePayload::Ok(cue_core::ipc::OkPayload::Ack {})
        ));
        match startup_roundtrip(&mut stream, 19, cue_core::ipc::RequestPayload::Ping {}).await {
            cue_core::ipc::ResponsePayload::Ok(cue_core::ipc::OkPayload::Pong {
                ready, ..
            }) => assert!(
                !ready,
                "Starting Pong must not advertise external readiness"
            ),
            other => panic!("expected Starting Pong, got {other:?}"),
        }
        stream
    }

    async fn assert_startup_execution_rejected(stream: &mut tokio::net::UnixStream, marker: &Path) {
        let response = startup_roundtrip(
            stream,
            2,
            cue_core::ipc::RequestPayload::Eval {
                input: format!("/usr/bin/touch {}", marker.display()),
                mode: cue_core::mode::Mode::Job,
            },
        )
        .await;
        match response {
            cue_core::ipc::ResponsePayload::Err { code, .. } => {
                assert_eq!(code, cue_core::ipc::error_code::DAEMON_DRAINING);
            }
            other => panic!("startup execution must be rejected, got {other:?}"),
        }
        assert!(!marker.exists(), "rejected Eval must not create its marker");

        let restart =
            startup_roundtrip(stream, 20, cue_core::ipc::RequestPayload::Restart {}).await;
        match restart {
            cue_core::ipc::ResponsePayload::Err { code, .. } => {
                assert_eq!(code, cue_core::ipc::error_code::DAEMON_DRAINING);
            }
            other => panic!("Starting successor must reject nested restart, got {other:?}"),
        }
    }

    async fn wait_for_marker(path: &Path) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !path.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("activated execution did not create marker");
    }

    async fn sync_readiness(socket: &Path) -> (bool, bool) {
        let socket = socket.to_path_buf();
        tokio::task::spawn_blocking(move || (daemon_responding(&socket), daemon_ready(&socket)))
            .await
            .expect("join synchronous readiness probe")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_startup_restart_never_runs_overdue_cron_or_eval() {
        let dir = make_temp_dir();
        let socket = dir.join("cancelled-startup.sock");
        let cron_marker = dir.join("cancelled-cron-ran");
        let eval_marker = dir.join("cancelled-eval-ran");
        let record = startup_restart_record(&socket, "cancelled-startup");
        let lifecycle = std::sync::Arc::new(crate::lifecycle::DaemonLifecycle::new_with_startup(
            socket.clone(),
            crate::lifecycle::RestartOwnership::Standalone,
            Some(record.clone()),
        ));
        let (scope_db, scheduler_db) = startup_actor_databases(&dir, &cron_marker);
        let sys = crate::actor::spawn_all(
            socket.clone(),
            scope_db,
            scheduler_db,
            crate::config::Config::default(),
            lifecycle.clone(),
        )
        .await
        .expect("spawn starting actor system");
        let mut stream = connect_starting_successor(&socket, &dir).await;

        assert_startup_execution_rejected(&mut stream, &eval_marker).await;
        assert_eq!(sync_readiness(&socket).await, (true, false));
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(
            !cron_marker.exists(),
            "cron that became overdue before the readiness CAS must remain paused"
        );

        crate::lifecycle::cancel_restart_intent_for_stop(&socket).expect("cancel startup restart");
        assert_eq!(
            finalize_startup_restart(&socket, &record, &lifecycle, &sys)
                .await
                .expect("finalize cancelled startup"),
            Some(DaemonExit::Stopped)
        );
        drop(stream);
        drop(sys);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!cron_marker.exists());
        assert!(!eval_marker.exists());
        crate::lifecycle::remove_matching_restart_record(&socket, &record.restart_id)
            .expect("remove cancelled record");
        std::fs::remove_dir_all(dir).expect("remove startup cancellation test dir");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn completed_startup_cas_activates_cron_and_eval_only_after_ack() {
        let dir = make_temp_dir();
        let socket = dir.join("activated-startup.sock");
        let cron_marker = dir.join("activated-cron-ran");
        let eval_marker = dir.join("activated-eval-ran");
        let record = startup_restart_record(&socket, "activated-startup");
        let lifecycle = std::sync::Arc::new(crate::lifecycle::DaemonLifecycle::new_with_startup(
            socket.clone(),
            crate::lifecycle::RestartOwnership::Standalone,
            Some(record.clone()),
        ));
        let (scope_db, scheduler_db) = startup_actor_databases(&dir, &cron_marker);
        let sys = crate::actor::spawn_all(
            socket.clone(),
            scope_db,
            scheduler_db,
            crate::config::Config::default(),
            lifecycle.clone(),
        )
        .await
        .expect("spawn starting actor system");
        let mut stream = connect_starting_successor(&socket, &dir).await;

        assert_startup_execution_rejected(&mut stream, &eval_marker).await;
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(!cron_marker.exists());

        assert_eq!(
            finalize_startup_restart(&socket, &record, &lifecycle, &sys)
                .await
                .expect("finalize successful startup"),
            None
        );
        assert_eq!(lifecycle.state(), crate::lifecycle::LifecycleState::Running);
        match startup_roundtrip(&mut stream, 21, cue_core::ipc::RequestPayload::Ping {}).await {
            cue_core::ipc::ResponsePayload::Ok(cue_core::ipc::OkPayload::Pong {
                ready, ..
            }) => assert!(ready, "activated successor must advertise readiness"),
            other => panic!("expected activated Pong, got {other:?}"),
        }
        assert_eq!(sync_readiness(&socket).await, (true, true));
        wait_for_marker(&cron_marker).await;

        let response = startup_roundtrip(
            &mut stream,
            3,
            cue_core::ipc::RequestPayload::Eval {
                input: format!("/usr/bin/touch {}", eval_marker.display()),
                mode: cue_core::mode::Mode::Job,
            },
        )
        .await;
        assert!(matches!(
            response,
            cue_core::ipc::ResponsePayload::Ok(cue_core::ipc::OkPayload::JobCreated { .. })
        ));
        wait_for_marker(&eval_marker).await;

        sys.shutdown_with_reason("startup activation test complete")
            .await;
        drop(stream);
        drop(sys);
        tokio::time::sleep(Duration::from_millis(100)).await;
        crate::lifecycle::remove_matching_restart_record(&socket, &record.restart_id)
            .expect("remove completed record");
        std::fs::remove_dir_all(dir).expect("remove startup activation test dir");
    }

    #[test]
    fn daemon_socket_override_rejects_empty_or_padded_values() {
        for (path, expected) in [
            (PathBuf::new(), "--socket must not be empty"),
            (PathBuf::from("   "), "--socket must not be empty"),
            (
                PathBuf::from(" /tmp/cued.sock"),
                "--socket must not have leading or trailing whitespace",
            ),
            (
                PathBuf::from("/tmp/cued.sock "),
                "--socket must not have leading or trailing whitespace",
            ),
        ] {
            let error = daemon_socket_path(Some(path.as_path()))
                .expect_err("invalid --socket override should fail");

            assert!(
                format!("{error:#}").contains(expected),
                "wrong error for socket {path:?}: {error:#}"
            );
        }
    }

    #[test]
    fn daemon_run_paths_reject_invalid_socket_before_side_effects() {
        let start_error = run_start(false, false, false, Some(PathBuf::new()))
            .expect_err("start should reject invalid socket before process checks");
        assert!(format!("{start_error:#}").contains("--socket must not be empty"));

        let status_error = run_status(Some(PathBuf::from(" /tmp/cued.sock")))
            .expect_err("status should reject invalid socket before probing");
        assert!(
            format!("{status_error:#}")
                .contains("--socket must not have leading or trailing whitespace")
        );
    }

    #[test]
    fn env_filter_uses_default_when_rust_log_is_absent() {
        env_filter_from_env("info", None).expect("default tracing directive should parse");
    }

    #[test]
    fn env_filter_rejects_empty_rust_log() {
        let error = env_filter_from_env("info", Some(OsString::new()))
            .expect_err("explicit empty RUST_LOG should fail");

        assert_eq!(format!("{error:#}"), "RUST_LOG must not be empty");
    }

    #[test]
    fn env_filter_rejects_invalid_rust_log_instead_of_falling_back() {
        let error = env_filter_from_env("info", Some(OsString::from("cue_daemon=debug,[")))
            .expect_err("invalid RUST_LOG should fail");

        assert!(
            format!("{error:#}").contains("parse RUST_LOG `cue_daemon=debug,[`"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn runtime_file_removal_deletes_existing_file() {
        let dir = make_temp_dir();
        let path = dir.join("cued.pid");
        std::fs::write(&path, "123").expect("write runtime file");

        remove_runtime_file(&path, "PID file").expect("remove runtime file");

        assert!(!path.exists());
        std::fs::remove_dir_all(dir).expect("remove temp dir");
    }

    #[test]
    fn runtime_file_removal_accepts_already_missing_file() {
        let dir = make_temp_dir();
        let path = dir.join("cued.sock");

        remove_runtime_file(&path, "socket").expect("missing file is clean");

        std::fs::remove_dir_all(dir).expect("remove temp dir");
    }

    #[test]
    fn custom_socket_derives_isolated_runtime_markers() {
        let socket = PathBuf::from("/tmp/cue-custom/worker.sock");
        let paths = daemon_runtime_paths(Some(&socket)).expect("derive daemon paths");

        assert_eq!(paths.socket, socket);
        assert_eq!(
            paths.pid,
            PathBuf::from("/tmp/cue-custom/worker.sock.cued.pid")
        );
        assert_eq!(
            paths.lock,
            PathBuf::from("/tmp/cue-custom/worker.sock.cued.lock")
        );
        assert_ne!(
            paths.pid,
            crate::dirs::pid_path_for_socket(&crate::dirs::socket_path())
        );
    }

    #[test]
    fn instance_lock_allows_only_one_holder() {
        let dir = make_temp_dir();
        let lock_path = dir.join("cued.lock");
        let first = acquire_instance_lock(&lock_path).expect("acquire first lock");

        let error = acquire_instance_lock(&lock_path).expect_err("second lock must fail");
        assert!(error.to_string().contains("another cued instance"));

        drop(first);
        acquire_instance_lock(&lock_path).expect("lock is released when owner drops");
        std::fs::remove_dir_all(dir).expect("remove temp dir");
    }

    #[test]
    fn stop_fails_closed_when_socket_is_unreachable_but_instance_lock_is_owned() {
        let dir = make_temp_dir();
        let paths = DaemonRuntimePaths {
            socket: dir.join("cued.sock"),
            pid: dir.join("cued.pid"),
            lock: dir.join("cued.lock"),
        };
        let owner = acquire_instance_lock(&paths.lock).expect("hold daemon instance lock");

        let error = run_stop_with_paths(&paths, Duration::from_millis(125))
            .expect_err("stop must not report success while a live owner retains the lock");
        let message = format!("{error:#}");
        assert!(message.contains("stop timed out"), "{message}");
        assert!(message.contains("was not released"), "{message}");
        assert_eq!(
            crate::lifecycle::restart_record_for_startup(&paths.socket)
                .expect("read stop tombstone")
                .expect("stop must write a durable tombstone")
                .phase,
            crate::lifecycle::RestartPhase::Cancelled
        );

        drop(owner);
        std::fs::remove_dir_all(dir).expect("remove stop timeout test dir");
    }

    #[test]
    fn stop_retries_after_connect_failure_until_instance_lock_is_released() {
        let dir = make_temp_dir();
        let paths = DaemonRuntimePaths {
            socket: dir.join("cued.sock"),
            pid: dir.join("cued.pid"),
            lock: dir.join("cued.lock"),
        };
        let owner = acquire_instance_lock(&paths.lock).expect("hold daemon instance lock");
        let release = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(75));
            drop(owner);
        });

        run_stop_with_paths(&paths, Duration::from_secs(1))
            .expect("stop should succeed only after the owner releases the lock");
        release.join().expect("join lock owner");

        std::fs::remove_dir_all(dir).expect("remove stop retry test dir");
    }

    #[tokio::test]
    async fn next_restart_watchdog_reaches_waiting_only_after_previous_holder_releases() {
        let dir = make_temp_dir();
        let socket = dir.join("cued.sock");
        let previous_holder = acquire_restart_watchdog_lock(&socket)
            .unwrap()
            .expect("hold previous watchdog lock");
        let (parent_control, mut child_control) = StdUnixStream::pair().unwrap();
        let socket_for_claim = socket.clone();
        let claim = tokio::spawn(async move {
            claim_restart_watchdog(&socket_for_claim, &mut child_control).await
        });

        tokio::time::sleep(Duration::from_millis(75)).await;
        assert!(
            !claim.is_finished(),
            "new watchdog must wait for old holder"
        );

        drop(previous_holder);
        let claimed = tokio::time::timeout(Duration::from_secs(1), claim)
            .await
            .expect("new watchdog did not acquire released lock")
            .expect("watchdog task panicked")
            .expect("watchdog claim failed")
            .expect("parent handoff channel disappeared");
        drop(claimed);
        drop(parent_control);
        std::fs::remove_dir_all(dir).expect("remove temp dir");
    }

    #[test]
    fn restart_handoff_requires_commit_and_exact_armed_ticket() {
        let (mut parent, mut child) = StdUnixStream::pair().unwrap();
        let worker =
            std::thread::spawn(move || complete_restart_successor_handoff(&mut child, || Ok(true)));

        assert_eq!(
            read_restart_handoff_signal(&mut parent).unwrap().as_deref(),
            Some(RESTART_HANDOFF_WAITING)
        );
        write_restart_handoff_signal(&mut parent, RESTART_HANDOFF_COMMIT).unwrap();
        assert_eq!(
            read_restart_handoff_signal(&mut parent).unwrap().as_deref(),
            Some(RESTART_HANDOFF_ACTIVE)
        );
        assert!(worker.join().unwrap().unwrap());

        let (mut parent, mut child) = StdUnixStream::pair().unwrap();
        let worker = std::thread::spawn(move || {
            complete_restart_successor_handoff(&mut child, || Ok(false))
        });
        assert_eq!(
            read_restart_handoff_signal(&mut parent).unwrap().as_deref(),
            Some(RESTART_HANDOFF_WAITING)
        );
        write_restart_handoff_signal(&mut parent, RESTART_HANDOFF_COMMIT).unwrap();
        assert_eq!(read_restart_handoff_signal(&mut parent).unwrap(), None);
        assert!(!worker.join().unwrap().unwrap());
    }

    #[test]
    fn restart_handoff_parent_eof_takes_over_only_for_exact_armed_ticket() {
        for (exact_armed, expected_takeover) in [(false, false), (true, true)] {
            let (mut parent, mut child) = StdUnixStream::pair().unwrap();
            let worker = std::thread::spawn(move || {
                complete_restart_successor_handoff(&mut child, || Ok(exact_armed))
            });
            assert_eq!(
                read_restart_handoff_signal(&mut parent).unwrap().as_deref(),
                Some(RESTART_HANDOFF_WAITING)
            );
            drop(parent);
            assert_eq!(worker.join().unwrap().unwrap(), expected_takeover);
        }
    }

    #[test]
    fn pid_cleanup_removes_only_the_current_daemon_marker() {
        let dir = make_temp_dir();
        let pid_path = dir.join("cued.pid");
        let owner = std::process::id();

        std::fs::write(&pid_path, (owner + 1).to_string()).expect("write foreign marker");
        cleanup_owned_pid_file(&pid_path, owner);
        assert!(pid_path.exists(), "foreign PID marker must be preserved");

        std::fs::write(&pid_path, owner.to_string()).expect("write owned marker");
        cleanup_owned_pid_file(&pid_path, owner);
        assert!(!pid_path.exists(), "owned PID marker should be removed");

        std::fs::remove_dir_all(dir).expect("remove temp dir");
    }

    #[test]
    fn force_fails_closed_for_live_pid_without_confirmed_socket() {
        let dir = make_temp_dir();
        let socket = dir.join("cued.sock");
        let pid_path = crate::dirs::pid_path_for_socket(&socket);
        let lock_path = crate::dirs::lock_path_for_socket(&socket);
        std::fs::write(&pid_path, std::process::id().to_string()).expect("write live PID marker");
        std::fs::write(&lock_path, "").expect("write unlocked daemon marker");

        let error = force_stop_if_running_with_pid_path(&pid_path, &socket, &lock_path)
            .expect_err("unconfirmed live PID must never be signalled");
        assert!(format!("{error:#}").contains("refusing --force"));
        #[cfg(target_os = "linux")]
        assert!(format!("{error:#}").contains("does not hold daemon lock"));
        assert!(pid_path.exists(), "fail-closed force must preserve marker");

        std::fs::remove_dir_all(dir).expect("remove temp dir");
    }

    #[test]
    fn pid_file_inspection_distinguishes_running_dead_malformed_and_missing() {
        let dir = make_temp_dir();
        let pid_path = dir.join("cued.pid");

        assert_eq!(
            inspect_pid_file_with(&pid_path, |_| unreachable!(
                "missing pid should not check liveness"
            ))
            .expect("missing pid file should inspect cleanly"),
            PidFileState::Missing
        );

        std::fs::write(&pid_path, "123").expect("write pid file");
        assert_eq!(
            inspect_pid_file_with(&pid_path, |pid| pid == 123)
                .expect("running pid should inspect cleanly"),
            PidFileState::Running(123)
        );
        assert_eq!(
            inspect_pid_file_with(&pid_path, |_| false).expect("dead pid should inspect cleanly"),
            PidFileState::Dead(123)
        );

        std::fs::write(&pid_path, "not-a-pid").expect("write malformed pid file");
        assert_eq!(
            inspect_pid_file_with(&pid_path, |_| unreachable!(
                "malformed pid should not check liveness"
            ))
            .expect("malformed pid file should inspect cleanly"),
            PidFileState::Malformed
        );

        std::fs::remove_dir_all(dir).expect("remove temp dir");
    }

    #[test]
    fn status_reports_reachable_socket_when_pid_file_is_missing() {
        let dir = make_temp_dir();
        let pid_path = dir.join("cued.pid");
        let socket = dir.join("cued.sock");

        let message = daemon_status_message(
            &pid_path,
            &socket,
            |_| unreachable!("missing pid should not check liveness"),
            |_| true,
        )
        .expect("status message should render");

        assert_eq!(
            message,
            format!(
                "cued is running (socket {} reachable, PID file is missing)",
                socket.display()
            )
        );
        std::fs::remove_dir_all(dir).expect("remove temp dir");
    }

    #[test]
    fn status_reports_reachable_socket_when_pid_file_is_malformed() {
        let dir = make_temp_dir();
        let pid_path = dir.join("cued.pid");
        let socket = dir.join("cued.sock");
        std::fs::write(&pid_path, "not-a-pid").expect("write malformed pid file");

        let message = daemon_status_message(
            &pid_path,
            &socket,
            |_| unreachable!("malformed pid should not check liveness"),
            |_| true,
        )
        .expect("status message should render");

        assert_eq!(
            message,
            format!(
                "cued is running (socket {} reachable, PID file is malformed)",
                socket.display()
            )
        );
        std::fs::remove_dir_all(dir).expect("remove temp dir");
    }

    #[test]
    fn status_reports_unreachable_socket_for_live_pid() {
        let dir = make_temp_dir();
        let pid_path = dir.join("cued.pid");
        let socket = dir.join("cued.sock");
        std::fs::write(&pid_path, "123").expect("write pid file");

        let message = daemon_status_message(&pid_path, &socket, |pid| pid == 123, |_| false)
            .expect("status message should render");

        assert_eq!(
            message,
            format!(
                "cued process is running (pid 123) but socket {} is not reachable",
                socket.display()
            )
        );
        std::fs::remove_dir_all(dir).expect("remove temp dir");
    }

    #[test]
    fn status_reports_not_running_when_pid_and_socket_are_missing() {
        let dir = make_temp_dir();
        let pid_path = dir.join("cued.pid");
        let socket = dir.join("cued.sock");

        let message = daemon_status_message(
            &pid_path,
            &socket,
            |_| unreachable!("missing pid should not check liveness"),
            |_| false,
        )
        .expect("status message should render");

        assert_eq!(message, "cued is not running");
        std::fs::remove_dir_all(dir).expect("remove temp dir");
    }

    #[test]
    fn unreadable_pid_file_is_not_removed_as_stale() {
        let dir = make_temp_dir();
        let pid_path = dir.join("cued.pid");
        let socket = dir.join("cued.sock");
        std::fs::create_dir(&pid_path).expect("create unreadable pid path");

        let error = ensure_not_running_with_pid_path(&pid_path, &socket)
            .expect_err("pid read failure should stop startup cleanup");

        assert!(format!("{error:#}").contains("read cued PID file"));
        assert!(pid_path.exists());
        std::fs::remove_dir_all(dir).expect("remove temp dir");
    }

    #[test]
    fn malformed_pid_file_is_not_removed_while_socket_is_live() {
        let dir = make_temp_dir();
        let pid_path = dir.join("cued.pid");
        let socket = dir.join("cued.sock");
        std::fs::write(&pid_path, "not-a-pid").expect("write malformed pid file");

        let error = ensure_not_running_with_pid_path_and_ready(&pid_path, &socket, |_| true)
            .expect_err("live socket should prevent stale marker cleanup");

        assert!(format!("{error:#}").contains("reachable"));
        assert!(pid_path.exists());
        std::fs::remove_dir_all(dir).expect("remove temp dir");
    }

    #[test]
    fn socket_liveness_guard_rejects_reachable_socket() {
        let dir = make_temp_dir();
        let socket = dir.join("cued.sock");

        let error = ensure_socket_not_live_with(&socket, "PID file is missing", |_| true)
            .expect_err("socket is live");

        assert!(error.to_string().contains("socket"));
        assert!(error.to_string().contains("reachable"));
        std::fs::remove_dir_all(dir).expect("remove temp dir");
    }
}
