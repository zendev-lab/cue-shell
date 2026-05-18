//! `cue` — TUI entry point for cue-shell.
//!
//! 1. Load client-side transport config from `client.toml` (or legacy `config.toml`).
//! 2. For local Unix transport, try to connect to `cued`, auto-starting it if needed.
//! 3. For remote SSH transport, speak the same IPC over `ssh ... "cued gateway --stdio"`.

use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::{Command as StdCommand, Stdio};
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use cue_client::{ClientConnector, CuedClient, RestartHandle, default_socket_path};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command as TokioCommand};
use tokio::task::JoinHandle;
use tracing::info;

use crate::config::{Config, ResolvedTransport};

pub fn run() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build tokio runtime")?;

    rt.block_on(async_main())
}

async fn async_main() -> Result<()> {
    let client_config = Config::load()?;
    let transport =
        client_config.resolve_transport(std::env::var_os("CUE_SOCKET").map(PathBuf::from))?;
    validate_transport(&transport)?;
    let restart_handle = Some(restart_handle_for_transport(&transport));

    let connector = transport_connector(&transport);
    let session_profile_name = Some(match &transport {
        ResolvedTransport::Unix { profile_name, .. }
        | ResolvedTransport::Ssh { profile_name, .. } => profile_name.clone(),
    });

    match transport {
        ResolvedTransport::Unix { socket_path, .. } => {
            // Connect (auto-starting daemon if needed). The client is reused by TUI.
            let client = ensure_daemon_running(&socket_path).await;
            cue_tui::run(connector, client, session_profile_name, restart_handle).await
        }
        ssh_transport @ ResolvedTransport::Ssh { .. } => {
            let client = connect_ssh_transport(&ssh_transport).await?;
            cue_tui::run(
                connector,
                Some(client),
                session_profile_name,
                restart_handle,
            )
            .await
        }
    }
}

fn transport_connector(transport: &ResolvedTransport) -> ClientConnector {
    match transport {
        ResolvedTransport::Unix { socket_path, .. } => ClientConnector::unix(socket_path.clone()),
        ssh_transport @ ResolvedTransport::Ssh { .. } => {
            let ssh_transport = ssh_transport.clone();
            ClientConnector::new(move || {
                let ssh_transport = ssh_transport.clone();
                async move { connect_ssh_transport(&ssh_transport).await }
            })
        }
    }
}

fn push_unique(paths: &mut Vec<String>, path: String) {
    if !paths.contains(&path) {
        paths.push(path);
    }
}

#[derive(Clone, Default)]
struct SharedStderr(Arc<Mutex<Vec<u8>>>);

impl SharedStderr {
    fn push(&self, chunk: &[u8]) {
        if let Ok(mut buffer) = self.0.lock() {
            buffer.extend_from_slice(chunk);
        }
    }

    fn snapshot(&self) -> String {
        self.0
            .lock()
            .ok()
            .map(|buffer| String::from_utf8_lossy(&buffer).trim().to_string())
            .unwrap_or_default()
    }
}

struct SpawnedSshTransport {
    stream: SshChildStream,
    stderr: SharedStderr,
}

struct SshChildStream {
    _child: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
    _stderr_task: JoinHandle<()>,
}

impl AsyncRead for SshChildStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stdout).poll_read(cx, buf)
    }
}

impl AsyncWrite for SshChildStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stdin).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stdin).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stdin).poll_shutdown(cx)
    }
}

async fn connect_ssh_transport(transport: &ResolvedTransport) -> Result<CuedClient> {
    let ResolvedTransport::Ssh {
        profile_name,
        destination,
        gateway_command,
        start_command,
    } = transport
    else {
        bail!("expected an ssh transport profile");
    };

    let SpawnedSshTransport { stream, stderr } =
        spawn_ssh_transport(destination, gateway_command).await?;
    let mut client = CuedClient::from_stream(stream);
    match client.ping_roundtrip().await {
        Ok(()) => Ok(client),
        Err(error) => Err(ssh_connect_error(
            profile_name,
            destination,
            gateway_command,
            start_command,
            &stderr,
            error,
        )
        .await),
    }
}

async fn spawn_ssh_transport(
    destination: &str,
    gateway_command: &str,
) -> Result<SpawnedSshTransport> {
    let stderr = SharedStderr::default();
    let mut command = TokioCommand::new("ssh");
    command
        .arg(destination)
        .arg(gateway_command)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let mut child = command
        .spawn()
        .with_context(|| format!("spawn `{}`", ssh_invocation(destination, gateway_command)))?;
    let stdin = child.stdin.take().context("capture ssh stdin")?;
    let stdout = child.stdout.take().context("capture ssh stdout")?;
    let stderr_reader = child.stderr.take().context("capture ssh stderr")?;
    let stderr_capture = stderr.clone();
    let stderr_task = tokio::spawn(async move {
        capture_ssh_stderr(stderr_reader, stderr_capture).await;
    });

    Ok(SpawnedSshTransport {
        stream: SshChildStream {
            _child: child,
            stdin,
            stdout,
            _stderr_task: stderr_task,
        },
        stderr,
    })
}

async fn capture_ssh_stderr(mut stderr: ChildStderr, buffer: SharedStderr) {
    let mut chunk = [0u8; 1024];
    loop {
        match stderr.read(&mut chunk).await {
            Ok(0) => break,
            Ok(read) => buffer.push(&chunk[..read]),
            Err(_) => break,
        }
    }
}

async fn ssh_connect_error(
    profile_name: &str,
    destination: &str,
    gateway_command: &str,
    start_command: &str,
    stderr: &SharedStderr,
    error: anyhow::Error,
) -> anyhow::Error {
    let detail = ssh_error_detail(stderr, format!("{error:#}")).await;

    if remote_daemon_missing(&detail) {
        ssh_handshake_error(
            profile_name,
            destination,
            gateway_command,
            start_command,
            &detail,
            &detail,
        )
    } else {
        anyhow::anyhow!(
            "client profile `{profile_name}` failed to connect via `{}`: {detail}",
            ssh_invocation(destination, gateway_command),
        )
    }
}

async fn ssh_error_detail(stderr: &SharedStderr, fallback: String) -> String {
    let detail = stderr.snapshot();
    if !detail.is_empty() {
        return detail;
    }

    tokio::time::sleep(Duration::from_millis(50)).await;
    let detail = stderr.snapshot();
    if detail.is_empty() { fallback } else { detail }
}

fn ssh_handshake_error(
    profile_name: &str,
    destination: &str,
    gateway_command: &str,
    start_command: &str,
    stderr: impl AsRef<str>,
    detail: impl AsRef<str>,
) -> anyhow::Error {
    let stderr = stderr.as_ref();
    let detail = detail.as_ref();
    let remote_start = ssh_invocation(destination, start_command);
    let remote_detail = if stderr.is_empty() {
        detail.to_string()
    } else {
        stderr.to_string()
    };
    anyhow::anyhow!(
        "client profile `{profile_name}` reached `{}` but the remote cue daemon is unavailable. Start it explicitly with `{remote_start}` (or log in and run `{start_command}`) and retry. Remote error: {remote_detail}",
        ssh_invocation(destination, gateway_command),
    )
}

fn remote_daemon_missing(detail: &str) -> bool {
    let detail = detail.to_ascii_lowercase();
    detail.contains("daemon socket not found")
        || detail.contains("daemon socket refused the connection")
        || detail.contains("failed to connect to daemon socket")
}

fn ssh_invocation(destination: &str, remote_command: &str) -> String {
    format!(
        "ssh {} {}",
        shell_quote(destination),
        shell_quote(remote_command)
    )
}

fn shell_quote(input: &str) -> String {
    if !input.chars().any(|ch| {
        matches!(
            ch,
            ' ' | '\t' | '\n' | '\'' | '"' | '\\' | '$' | '`' | '!' | ';' | '&' | '|' | '<' | '>'
        )
    }) {
        return input.to_string();
    }

    format!("'{}'", input.replace('\'', r"'\''"))
}

fn daemon_candidates_for_path(path: &Path) -> Vec<String> {
    let mut candidates = Vec::new();
    let sibling = path.with_file_name("cued");
    if sibling.is_file() {
        push_unique(&mut candidates, sibling.display().to_string());
    }

    if let Some(parent) = path.parent()
        && parent.file_name().is_some_and(|name| name == "deps")
        && let Some(bin_dir) = parent.parent()
    {
        let cargo_bin = bin_dir.join("cued");
        if cargo_bin.is_file() {
            push_unique(&mut candidates, cargo_bin.display().to_string());
        }
    }

    candidates
}

fn argv0_path() -> Option<PathBuf> {
    let argv0 = std::env::args_os().next()?;
    let path = PathBuf::from(argv0);
    if path.components().count() == 1 {
        return None;
    }

    let absolute = if path.is_absolute() {
        path
    } else {
        std::env::current_dir().ok()?.join(path)
    };

    absolute.is_file().then_some(absolute)
}

fn daemon_bin_candidates_from_sources(
    env_override: Option<String>,
    current_exe: Option<PathBuf>,
    argv0: Option<PathBuf>,
) -> Vec<String> {
    if let Some(path) = env_override {
        return vec![path];
    }

    let mut candidates = Vec::new();

    if let Some(path) = current_exe {
        for candidate in daemon_candidates_for_path(&path) {
            push_unique(&mut candidates, candidate);
        }
    }

    if let Some(path) = argv0 {
        for candidate in daemon_candidates_for_path(&path) {
            push_unique(&mut candidates, candidate);
        }
    }

    push_unique(&mut candidates, "cued".into());
    candidates
}

fn daemon_bin_candidates() -> Vec<String> {
    daemon_bin_candidates_from_sources(
        std::env::var("CUE_DAEMON_BIN").ok(),
        std::env::current_exe().ok(),
        argv0_path(),
    )
}

fn restart_handle_for_transport(transport: &ResolvedTransport) -> RestartHandle {
    let transport = transport.clone();
    RestartHandle::new(move || restart_transport(&transport))
}

fn restart_transport(transport: &ResolvedTransport) -> Result<()> {
    match transport {
        ResolvedTransport::Unix { socket_path, .. } => restart_local_daemon(socket_path),
        ResolvedTransport::Ssh {
            destination,
            start_command,
            ..
        } => restart_remote_daemon(destination, start_command),
    }
}

fn restart_local_daemon(socket_path: &Path) -> Result<()> {
    let socket_override = socket_path != default_socket_path().as_path();
    let mut last_failure = None;

    for cued_bin in daemon_bin_candidates() {
        let mut command = StdCommand::new(&cued_bin);
        command.arg("restart");
        if socket_override {
            command.arg("--socket").arg(socket_path);
        }

        match command.status() {
            Ok(status) if status.success() => return Ok(()),
            Ok(status) => {
                last_failure = Some(format!(
                    "`{cued_bin}` exited with status {status} while restarting {}",
                    socket_path.display()
                ));
            }
            Err(error) => {
                last_failure = Some(format!(
                    "failed to run `{cued_bin}` while restarting {}: {error}",
                    socket_path.display()
                ));
            }
        }
    }

    bail!(
        "{}",
        last_failure.unwrap_or_else(|| {
            format!(
                "no cued executable was able to restart {}",
                socket_path.display()
            )
        })
    )
}

fn remote_restart_command(start_command: &str) -> String {
    let mut parts = start_command.split_whitespace();
    if matches!(parts.next(), Some("cued")) && matches!(parts.next(), Some("start")) {
        let rest: Vec<&str> = parts
            .filter(|part| *part != "-F" && *part != "--force")
            .collect();
        if rest.is_empty() {
            "cued restart".into()
        } else {
            format!("cued restart {}", rest.join(" "))
        }
    } else if start_command
        .split_whitespace()
        .any(|part| part == "-F" || part == "--force")
    {
        start_command.to_string()
    } else {
        format!("{start_command} -F")
    }
}

fn restart_remote_daemon(destination: &str, start_command: &str) -> Result<()> {
    let remote_command = remote_restart_command(start_command);
    let status = StdCommand::new("ssh")
        .arg(destination)
        .arg(&remote_command)
        .status()
        .with_context(|| format!("run `{}`", ssh_invocation(destination, &remote_command)))?;
    if status.success() {
        Ok(())
    } else {
        bail!(
            "`{}` exited with status {status} while restarting the remote daemon",
            ssh_invocation(destination, &remote_command)
        )
    }
}

/// Try to connect to the daemon, auto-starting it if needed.
///
/// Returns the connected client for the TUI to reuse (no double-connect).
/// Returns `None` for offline mode with auto-reconnect.
async fn ensure_daemon_running(socket_path: &Path) -> Option<CuedClient> {
    // Try direct connect first.
    if let Ok(client) = CuedClient::connect(socket_path).await {
        info!("cued already running");
        return Some(client);
    }

    // Connection failed. Clean up stale socket if present.
    if socket_path.exists() {
        info!("stale socket detected, removing {}", socket_path.display());
        std::fs::remove_file(socket_path).ok();
    }

    // Auto-start the daemon.
    info!("cued not running, attempting to start");
    let mut spawned = false;
    let mut last_error = None;
    for cued_bin in daemon_bin_candidates() {
        match StdCommand::new(&cued_bin)
            .arg("start")
            .arg("--socket")
            .arg(socket_path)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
        {
            Ok(_) => {
                spawned = true;
                break;
            }
            Err(error) => {
                last_error = Some((cued_bin, error));
            }
        }
    }

    if !spawned && let Some((daemon_bin, error)) = last_error {
        tracing::warn!(daemon_bin = %daemon_bin, %error, "failed to spawn cued start");
    }

    // Retry connect with backoff: 100ms, 200ms, 400ms, 800ms, 1600ms.
    let mut delay = Duration::from_millis(100);
    for _ in 0..5 {
        tokio::time::sleep(delay).await;
        if let Ok(client) = CuedClient::connect(socket_path).await {
            info!("connected after auto-start");
            return Some(client);
        }
        delay *= 2;
    }

    tracing::warn!("cued did not start in time, entering offline mode");
    None
}

fn validate_transport(transport: &ResolvedTransport) -> Result<()> {
    validate_transport_with_lookup(transport, command_in_path)
}

fn validate_transport_with_lookup<F>(
    transport: &ResolvedTransport,
    command_in_path: F,
) -> Result<()>
where
    F: Fn(&str) -> bool,
{
    if let ResolvedTransport::Ssh {
        profile_name,
        destination,
        gateway_command,
        start_command,
    } = transport
    {
        if !command_in_path("ssh") {
            anyhow::bail!(ssh_install_hint(profile_name));
        }
        if destination.trim().is_empty() {
            anyhow::bail!("client profile `{profile_name}` has an empty SSH destination");
        }
        if gateway_command.trim().is_empty() {
            anyhow::bail!("client profile `{profile_name}` has an empty `gateway_command`");
        }
        if start_command.trim().is_empty() {
            anyhow::bail!("client profile `{profile_name}` has an empty `start_command`");
        }
    }
    Ok(())
}

fn ssh_install_hint(profile_name: &str) -> String {
    format!(
        "client profile `{profile_name}` uses `transport = \"ssh\"`, but OpenSSH `ssh` was not found in PATH. cue-shell phase 1 uses the system OpenSSH client. Install it (macOS: `brew install openssh`; Debian/Ubuntu: `sudo apt install openssh-client`; Fedora: `sudo dnf install openssh-clients`) or switch back to a unix transport profile."
    )
}

fn command_in_path(program: &str) -> bool {
    let Some(path) = std::env::var_os("PATH") else {
        return false;
    };

    std::env::split_paths(&path).any(|dir| is_executable_file(&dir.join(program)))
}

#[cfg(unix)]
fn is_executable_file(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;

    std::fs::metadata(path)
        .map(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn is_executable_file(path: &Path) -> bool {
    path.is_file()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicU64, Ordering};

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn make_temp_bin_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "cue-cli-bin-test-{}-{}",
            std::process::id(),
            TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).expect("create temp bin dir");
        dir
    }

    fn touch(path: &Path) {
        std::fs::write(path, []).expect("create temp file");
    }

    #[test]
    fn daemon_bin_prefers_override() {
        assert_eq!(
            daemon_bin_candidates_from_sources(
                Some("/custom/cued".into()),
                Some(PathBuf::from("/bin/cue")),
                Some(PathBuf::from("./target/debug/cue"))
            ),
            vec!["/custom/cued".to_string()]
        );
    }

    #[test]
    fn daemon_bin_uses_current_exe_sibling() {
        let dir = make_temp_bin_dir();
        let cue = dir.join("cue");
        let cued = dir.join("cued");
        touch(&cue);
        touch(&cued);

        assert_eq!(
            daemon_bin_candidates_from_sources(None, Some(cue), None),
            vec![cued.display().to_string(), "cued".to_string()]
        );

        std::fs::remove_dir_all(dir).expect("remove temp bin dir");
    }

    #[test]
    fn daemon_bin_falls_back_to_argv0_sibling() {
        let dir = make_temp_bin_dir();
        let cue = dir.join("cue");
        let cued = dir.join("cued");
        touch(&cue);
        touch(&cued);

        assert_eq!(
            daemon_bin_candidates_from_sources(None, None, Some(cue)),
            vec![cued.display().to_string(), "cued".to_string()]
        );

        std::fs::remove_dir_all(dir).expect("remove temp bin dir");
    }

    #[test]
    fn daemon_bin_uses_cargo_bin_for_deps_path() {
        let dir = make_temp_bin_dir();
        let deps = dir.join("deps");
        std::fs::create_dir_all(&deps).expect("create deps dir");
        let cue = deps.join("cue-123");
        let cued = dir.join("cued");
        touch(&cue);
        touch(&cued);

        assert_eq!(
            daemon_bin_candidates_from_sources(None, Some(cue), None),
            vec![cued.display().to_string(), "cued".to_string()]
        );

        std::fs::remove_dir_all(dir).expect("remove temp bin dir");
    }

    #[test]
    fn daemon_bin_falls_back_to_path_lookup() {
        assert_eq!(
            daemon_bin_candidates_from_sources(None, None, None),
            vec!["cued".to_string()]
        );
    }

    #[test]
    fn remote_restart_command_prefers_cued_restart() {
        assert_eq!(
            remote_restart_command("cued start --socket /tmp/cued.sock"),
            "cued restart --socket /tmp/cued.sock"
        );
        assert_eq!(
            remote_restart_command("cued start -F --socket /tmp/cued.sock"),
            "cued restart --socket /tmp/cued.sock"
        );
        assert_eq!(
            remote_restart_command("custom-wrapper"),
            "custom-wrapper -F"
        );
    }

    #[test]
    fn ssh_transport_without_ssh_shows_install_hint() {
        let error = validate_transport_with_lookup(
            &ResolvedTransport::Ssh {
                profile_name: "remote".into(),
                destination: "devbox".into(),
                gateway_command: "cued gateway --stdio".into(),
                start_command: "cued start".into(),
            },
            |_| false,
        )
        .expect_err("missing ssh should fail");

        let message = format!("{error:#}");
        assert!(message.contains("OpenSSH `ssh` was not found in PATH"));
        assert!(message.contains("brew install openssh"));
        assert!(message.contains("sudo apt install openssh-client"));
    }

    #[test]
    fn ssh_invocation_quotes_remote_command() {
        assert_eq!(
            ssh_invocation(
                "user@example.com",
                "cued start --socket ~/.cache/cue shell.sock"
            ),
            "ssh user@example.com 'cued start --socket ~/.cache/cue shell.sock'"
        );
    }

    #[tokio::test]
    async fn remote_daemon_error_includes_explicit_start_command() {
        let error = ssh_connect_error(
            "remote",
            "devbox",
            "cued gateway --stdio",
            "cued start --socket ~/.cache/cue-shell/remote.sock",
            &SharedStderr::default(),
            anyhow::anyhow!(
                "connect to /run/user/1000/cue-shell/cued.sock: daemon socket not found; is cued running?"
            ),
        )
        .await;

        let message = format!("{error:#}");
        assert!(message.contains("Start it explicitly with"));
        assert!(
            message.contains("ssh devbox 'cued start --socket ~/.cache/cue-shell/remote.sock'")
        );
        assert!(message.contains("daemon socket not found"));
    }

    #[test]
    fn ssh_transport_rejects_empty_gateway_command() {
        let error = validate_transport_with_lookup(
            &ResolvedTransport::Ssh {
                profile_name: "remote".into(),
                destination: "devbox".into(),
                gateway_command: String::new(),
                start_command: "cued start".into(),
            },
            |_| true,
        )
        .expect_err("empty gateway command should fail");

        assert!(format!("{error:#}").contains("empty `gateway_command`"));
    }
}
