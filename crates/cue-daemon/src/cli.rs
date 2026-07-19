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
use std::os::fd::AsRawFd as _;
#[cfg(target_os = "linux")]
use std::os::fd::{FromRawFd as _, OwnedFd};
#[cfg(target_os = "linux")]
use std::os::unix::fs::MetadataExt as _;
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result};
use bpaf::Parser as _;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use tokio::signal;
use tracing::{error, info};

// ── CLI definition (combinator API, no derive feature needed) ──

#[derive(Debug, Clone, PartialEq, Eq)]
enum Cli {
    Start {
        fg: bool,
        force: bool,
        socket: Option<PathBuf>,
    },
    Stop {
        socket: Option<PathBuf>,
    },
    Restart {
        socket: Option<PathBuf>,
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
    let socket = socket_arg();
    bpaf::construct!(Cli::Start { fg, force, socket })
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
    bpaf::construct!(Cli::Restart { socket })
        .to_options()
        .command("restart")
        .help("Restart the daemon")
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
    let result = match cmd {
        Cli::Start { fg, force, socket } => run_start(fg, force, socket),
        Cli::Stop { socket } => run_stop(socket),
        Cli::Restart { socket } => run_restart(socket),
        Cli::Status { socket } => run_status(socket),
        Cli::Gateway { stdio, socket } => run_gateway(stdio, socket),
        Cli::Install => run_install(),
        Cli::Uninstall => run_uninstall(),
        Cli::Upgrade => run_upgrade(),
    };
    if let Err(e) = result {
        eprintln!("cued: {e:#}");
        return 1;
    }
    0
}

// ── Start ──

fn run_start(fg: bool, force: bool, socket_override: Option<PathBuf>) -> Result<()> {
    let paths = daemon_runtime_paths(socket_override.as_deref())?;

    if force {
        // When the service manager owns cued, delegate restart to it rather than
        // sending a raw SIGTERM (which would fight launchd/systemd's KeepAlive).
        if crate::service::is_installed()? && socket_override.is_none() {
            println!("cued: daemon is managed by the service manager — restarting via service");
            crate::service::restart()?;
            println!("cued: daemon restarted");
            return Ok(());
        }
        force_stop_if_running_with_pid_path(&paths.pid, &paths.socket, &paths.lock)?;
    } else {
        // This is only a fast preflight. The foreground process repeats the
        // liveness check and any stale cleanup while holding `paths.lock`.
        ensure_socket_not_live(&paths.socket, "startup preflight")?;
    }

    if fg {
        return run_start_foreground(paths);
    }

    run_start_background(socket_override)
}

fn run_restart(socket_override: Option<PathBuf>) -> Result<()> {
    run_start(false, true, socket_override)
}

fn run_start_background(socket_override: Option<PathBuf>) -> Result<()> {
    let socket_path = daemon_socket_path(socket_override.as_deref())?;
    let current_exe = std::env::current_exe().context("resolve current cued executable")?;

    let mut cmd = Command::new(current_exe);
    cmd.arg("start").arg("--fg");
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

fn run_start_foreground(paths: DaemonRuntimePaths) -> Result<()> {
    init_stderr_tracing("info")?;

    // Ensure directories exist.
    crate::dirs::ensure_dirs().context("create directories")?;

    // Hold the socket-specific lock for the entire daemon lifetime. All stale
    // marker cleanup happens only after this succeeds, closing the check/bind
    // race between concurrent foreground starts.
    let _instance_lock = acquire_instance_lock(&paths.lock)?;
    ensure_not_running_with_pid_path(&paths.pid, &paths.socket)?;

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
    let result = rt.block_on(async_main(paths.socket.clone()));
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
    result
}

async fn async_main(socket_path: PathBuf) -> Result<()> {
    // Load config.
    let config = crate::config::Config::load().context("load daemon config")?;

    // Open database.
    let db_path = crate::dirs::db_path()?;
    let scope_db = crate::storage::open_db(&db_path)
        .with_context(|| format!("open database {}", db_path.display()))?;
    let scheduler_db = crate::storage::open_db(&db_path)
        .with_context(|| format!("open database {}", db_path.display()))?;

    // Spawn all actors.
    let sys = crate::actor::spawn_all(socket_path, scope_db, scheduler_db, config).await?;

    info!("cued ready — waiting for signals");

    // Wait for SIGTERM or SIGINT.
    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;
    let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())?;

    let shutdown_reason = tokio::select! {
        _ = sigterm.recv() => {
            info!("received SIGTERM");
            "SIGTERM"
        }
        _ = sigint.recv()  => {
            info!("received SIGINT");
            "SIGINT"
        }
    };

    // Graceful shutdown.
    info!("cued shutting down");
    sys.shutdown_with_reason(shutdown_reason).await;
    drop(sys);

    // Give actors a moment to drain.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    Ok(())
}

// ── Stop ──

fn run_stop(socket_override: Option<PathBuf>) -> Result<()> {
    let socket_path = daemon_socket_path(socket_override.as_deref())?;
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let mut stream = tokio::net::UnixStream::connect(&socket_path)
            .await
            .with_context(|| format!("connect to {}", socket_path.display()))?;

        let msg = cue_core::ipc::Message::Request {
            id: 0,
            operation_id: None,
            payload: cue_core::ipc::RequestPayload::Shutdown {},
        };
        crate::actor::gateway::write_message(&mut stream, &msg).await?;

        // Read ack.
        match crate::actor::gateway::read_message(&mut stream).await {
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
                // Connection might close before we read — that's OK.
                println!("cued: connection closed ({e}) — daemon likely stopped");
            }
        }
        Ok(())
    })
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
            "start" | "stop" | "status" | "gateway" | "install" | "uninstall" | "upgrade" | "-h"
            | "restart" | "--help" | "-V" | "--version",
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
        if !daemon_ready(socket_path)
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
    StdUnixStream::connect(socket_path).is_ok()
}

#[cfg(test)]
mod tests {
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
    fn parses_restart_subcommand() {
        assert_eq!(
            parse(&["restart", "--socket", "daemon.sock"]),
            Cli::Restart {
                socket: Some(PathBuf::from("daemon.sock")),
            }
        );
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
        let start_error = run_start(false, false, Some(PathBuf::new()))
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
