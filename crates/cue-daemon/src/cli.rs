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
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::PathBuf;
use std::process;
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

pub fn run() {
    let parser = cli();
    let args = normalized_cli_args();
    let args = bpaf::Args::from(args.as_slice()).set_name("cued");
    let cmd = match parser.run_inner(args) {
        Ok(cmd) => cmd,
        Err(err) => {
            err.print_message(100);
            process::exit(err.exit_code());
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
        process::exit(1);
    }
}

// ── Start ──

fn run_start(fg: bool, force: bool, socket_override: Option<PathBuf>) -> Result<()> {
    if force {
        // When the service manager owns cued, delegate restart to it rather than
        // sending a raw SIGTERM (which would fight launchd/systemd's KeepAlive).
        if crate::service::is_installed() && socket_override.is_none() {
            println!("cued: daemon is managed by the service manager — restarting via service");
            crate::service::restart()?;
            println!("cued: daemon restarted");
            return Ok(());
        }
        force_stop_if_running()?;
    } else {
        ensure_not_running()?;
    }

    if fg {
        return run_start_foreground(socket_override);
    }

    run_start_background(socket_override)
}

fn run_restart(socket_override: Option<PathBuf>) -> Result<()> {
    run_start(false, true, socket_override)
}

fn run_start_background(socket_override: Option<PathBuf>) -> Result<()> {
    let socket_path = socket_override
        .clone()
        .unwrap_or_else(crate::dirs::socket_path);
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

fn run_start_foreground(socket_override: Option<PathBuf>) -> Result<()> {
    // Initialize tracing (stderr, env-filter).
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let pid_path = crate::dirs::pid_path();
    let socket_path = socket_override.unwrap_or_else(crate::dirs::socket_path);

    // Ensure directories exist.
    crate::dirs::ensure_dirs().context("create directories")?;

    // Write PID file.
    std::fs::write(&pid_path, format!("{}", process::id()))
        .with_context(|| format!("write PID file {}", pid_path.display()))?;

    info!(
        version = cue_core::version(),
        pid = process::id(),
        socket = %socket_path.display(),
        "cued starting"
    );

    // Build Tokio runtime and run the async entry point.
    let rt = tokio::runtime::Runtime::new().context("create tokio runtime")?;
    let result = rt.block_on(async_main(socket_path.clone()));
    rt.shutdown_timeout(Duration::from_secs(2));

    // Cleanup.
    cleanup(&pid_path, &socket_path);
    if result.is_ok() {
        info!("cued stopped");
    }
    result
}

async fn async_main(socket_path: PathBuf) -> Result<()> {
    // Load config.
    let config = crate::config::Config::load().context("load daemon config")?;

    // Open database.
    let db_path = crate::dirs::db_path();
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

    tokio::select! {
        _ = sigterm.recv() => info!("received SIGTERM"),
        _ = sigint.recv()  => info!("received SIGINT"),
    }

    // Graceful shutdown.
    info!("cued shutting down");
    sys.shutdown().await;
    drop(sys);

    // Give actors a moment to drain.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    Ok(())
}

fn cleanup(pid_path: &PathBuf, socket_path: &PathBuf) {
    std::fs::remove_file(pid_path).ok();
    std::fs::remove_file(socket_path).ok();
}

// ── Stop ──

fn run_stop(socket_override: Option<PathBuf>) -> Result<()> {
    let socket_path = socket_override.unwrap_or_else(crate::dirs::socket_path);
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let mut stream = tokio::net::UnixStream::connect(&socket_path)
            .await
            .with_context(|| format!("connect to {}", socket_path.display()))?;

        let msg = cue_core::ipc::Message::Request {
            id: 0,
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
    let pid_path = crate::dirs::pid_path();
    let socket_path = socket_override.unwrap_or_else(crate::dirs::socket_path);

    // Check PID file.
    if pid_path.exists()
        && let Ok(content) = std::fs::read_to_string(&pid_path)
        && let Ok(pid) = content.trim().parse::<u32>()
    {
        if is_process_alive(pid) {
            println!(
                "cued is running (pid {pid}, socket {})",
                socket_path.display()
            );
            return Ok(());
        }
        println!("cued: stale PID file (pid {pid} not running)");
        return Ok(());
    }

    println!("cued is not running");
    Ok(())
}

// ── Gateway ──

fn run_gateway(stdio: bool, socket_override: Option<PathBuf>) -> Result<()> {
    anyhow::ensure!(stdio, "gateway currently supports only --stdio");

    let socket_path = socket_override.unwrap_or_else(crate::dirs::socket_path);
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

/// Kill any running daemon and wait for it to exit (used by `--force`).
fn force_stop_if_running() -> Result<()> {
    let pid_path = crate::dirs::pid_path();
    let socket_path = crate::dirs::socket_path();

    if !pid_path.exists() {
        return Ok(());
    }

    let Ok(content) = std::fs::read_to_string(&pid_path) else {
        std::fs::remove_file(&pid_path).ok();
        return Ok(());
    };

    let Ok(pid) = content.trim().parse::<u32>() else {
        std::fs::remove_file(&pid_path).ok();
        return Ok(());
    };

    if !is_process_alive(pid) {
        std::fs::remove_file(&pid_path).ok();
        std::fs::remove_file(&socket_path).ok();
        return Ok(());
    }

    println!("cued: sending SIGTERM to pid {pid}");
    // SAFETY: standard POSIX signal.
    let rc = unsafe { libc_kill(pid as i32, 15) }; // 15 = SIGTERM
    if rc != 0 {
        anyhow::bail!("failed to send SIGTERM to pid {pid}");
    }

    // Poll for up to 5 s.
    for _ in 0..50 {
        std::thread::sleep(Duration::from_millis(100));
        if !is_process_alive(pid) {
            println!("cued: previous daemon stopped");
            std::fs::remove_file(&pid_path).ok();
            std::fs::remove_file(&socket_path).ok();
            return Ok(());
        }
    }

    anyhow::bail!(
        "cued: pid {pid} did not exit within 5 s after SIGTERM; try `kill -9 {pid}` manually"
    );
}

fn ensure_not_running() -> Result<()> {
    let pid_path = crate::dirs::pid_path();
    if !pid_path.exists() {
        return Ok(());
    }

    if let Ok(content) = std::fs::read_to_string(&pid_path)
        && let Ok(pid) = content.trim().parse::<u32>()
        && is_process_alive(pid)
    {
        anyhow::bail!(
            "cued already running (pid {pid}). If stale, remove {} and retry.",
            pid_path.display()
        );
    }

    std::fs::remove_file(&pid_path).ok();
    Ok(())
}

fn daemon_ready(socket_path: &PathBuf) -> bool {
    StdUnixStream::connect(socket_path).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
