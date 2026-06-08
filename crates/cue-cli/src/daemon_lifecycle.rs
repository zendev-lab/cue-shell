use std::ffi::{OsStr, OsString};
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command as StdCommand, Output, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use cue_client::{CuedClient, default_socket_path};
#[cfg(feature = "tui")]
use cue_client::{ResolvedTransport, RestartHandle};
use tracing::info;

#[cfg(feature = "tui")]
pub(crate) fn restart_handle_for_transport(transport: &ResolvedTransport) -> RestartHandle {
    let transport = transport.clone();
    RestartHandle::new(move || restart_transport(&transport))
}

#[cfg(feature = "tui")]
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

#[cfg(feature = "tui")]
fn restart_local_daemon(socket_path: &Path) -> Result<()> {
    let candidates = daemon_bin_candidates()?;
    restart_local_daemon_with_candidates(&candidates, socket_path)
}

fn restart_local_daemon_with_candidates(candidates: &[OsString], socket_path: &Path) -> Result<()> {
    let socket_override = socket_path != default_socket_path().as_path();
    let mut failures = Vec::new();

    for cued_bin in candidates {
        let args = local_restart_args(socket_override, socket_path);
        let invocation = command_display(cued_bin.as_os_str(), &args);

        match StdCommand::new(cued_bin)
            .args(&args)
            .stdin(Stdio::null())
            .output()
        {
            Ok(output) if output.status.success() => return Ok(()),
            Ok(output) => failures.push(command_failure_summary(&invocation, &output)),
            Err(error) => failures.push(format!("failed to run `{invocation}`: {error}")),
        }
    }

    bail!(
        "{}",
        local_command_failures("restart", socket_path, failures)
    )
}

#[cfg(feature = "tui")]
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

#[cfg(feature = "tui")]
fn restart_remote_daemon(destination: &str, start_command: &str) -> Result<()> {
    let remote_command = remote_restart_command(start_command);
    let invocation = remote_restart_invocation(destination, &remote_command);
    let output = StdCommand::new("ssh")
        .arg(destination)
        .arg(&remote_command)
        .stdin(Stdio::null())
        .output()
        .with_context(|| format!("run `{invocation}`"))?;
    if output.status.success() {
        Ok(())
    } else {
        bail!(
            "{}\nwhile restarting the remote daemon",
            command_failure_summary(&invocation, &output)
        )
    }
}

#[cfg(feature = "tui")]
fn remote_restart_invocation(destination: &str, remote_command: &str) -> String {
    command_display(
        OsStr::new("ssh"),
        &[OsString::from(destination), OsString::from(remote_command)],
    )
}

fn local_restart_args(socket_override: bool, socket_path: &Path) -> Vec<OsString> {
    let mut args = vec![OsString::from("restart")];
    if socket_override {
        args.push(OsString::from("--socket"));
        args.push(socket_path.as_os_str().to_os_string());
    }
    args
}

fn start_local_daemon_with_candidates(candidates: &[OsString], socket_path: &Path) -> Result<()> {
    let args = local_start_args(socket_path);
    let mut failures = Vec::new();

    for cued_bin in candidates {
        let invocation = command_display(cued_bin.as_os_str(), &args);
        match StdCommand::new(cued_bin)
            .args(&args)
            .stdin(Stdio::null())
            .output()
        {
            Ok(output) if output.status.success() => return Ok(()),
            Ok(output) => failures.push(command_failure_summary(&invocation, &output)),
            Err(error) => failures.push(format!("failed to run `{invocation}`: {error}")),
        }
    }

    bail!("{}", local_command_failures("start", socket_path, failures))
}

fn local_start_args(socket_path: &Path) -> Vec<OsString> {
    vec![
        OsString::from("start"),
        OsString::from("--socket"),
        socket_path.as_os_str().to_os_string(),
    ]
}

fn local_command_failures(action: &str, socket_path: &Path, failures: Vec<String>) -> String {
    let summary = format!(
        "no cued executable was able to {action} {}",
        socket_path.display()
    );
    if failures.is_empty() {
        format!(
            "{summary}\nno first-party `cued` companion binary was found next to `cue`; set CUE_DAEMON_BIN to an explicit daemon path"
        )
    } else {
        format!("{summary}\n{}", failures.join("\n"))
    }
}

fn command_display(program: &OsStr, args: &[OsString]) -> String {
    std::iter::once(program)
        .chain(args.iter().map(OsString::as_os_str))
        .map(shell_quote)
        .collect::<Vec<_>>()
        .join(" ")
}

fn shell_quote(value: &OsStr) -> String {
    let value = value.to_string_lossy();
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_./:=@+".contains(&byte))
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

fn command_failure_summary(invocation: &str, output: &Output) -> String {
    let mut summary = format!("`{invocation}` exited with status {}", output.status);
    append_command_output(&mut summary, "stderr", &output.stderr);
    append_command_output(&mut summary, "stdout", &output.stdout);
    summary
}

fn append_command_output(summary: &mut String, label: &str, data: &[u8]) {
    let text = String::from_utf8_lossy(data);
    let text = text.trim();
    if !text.is_empty() {
        summary.push_str(&format!("\n{label}: {text}"));
    }
}

/// Try to connect to the daemon, auto-starting it if needed.
///
/// Returns the connected client for the TUI to reuse (no double-connect).
/// Returns `None` for offline mode with auto-reconnect.
#[cfg(feature = "tui")]
pub(crate) async fn ensure_daemon_running(socket_path: &Path) -> Option<CuedClient> {
    match require_daemon_running(socket_path).await {
        Ok(client) => Some(client),
        Err(error) => {
            tracing::warn!(
                %error,
                socket_path = %socket_path.display(),
                "local cued unavailable, entering offline mode"
            );
            eprintln!(
                "cue: local cued is unavailable at {}:\n{error:#}",
                socket_path.display()
            );
            None
        }
    }
}

/// Connect to the local daemon, auto-starting it if needed.
///
/// Unlike [`ensure_daemon_running`], this returns the startup error to callers
/// that cannot enter offline mode, such as `cue run`.
pub(crate) async fn require_daemon_running(socket_path: &Path) -> Result<CuedClient> {
    let candidates = daemon_bin_candidates()?;
    require_daemon_running_with_candidates(socket_path, &candidates).await
}

async fn require_daemon_running_with_candidates(
    socket_path: &Path,
    candidates: &[OsString],
) -> Result<CuedClient> {
    let initial_error = match CuedClient::connect(socket_path).await {
        Ok(client) => {
            info!("cued already running");
            return Ok(client);
        }
        Err(error) => error,
    };

    remove_stale_socket(socket_path)?;

    info!("cued not running, attempting to start");
    start_local_daemon_with_candidates(candidates, socket_path)
        .with_context(|| format!("auto-start cued for {}", socket_path.display()))?;

    let mut delay = Duration::from_millis(100);
    for _ in 0..5 {
        tokio::time::sleep(delay).await;
        if let Ok(client) = CuedClient::connect(socket_path).await {
            info!("connected after auto-start");
            return Ok(client);
        }
        delay *= 2;
    }

    bail!(
        "cued did not accept connections at {} after auto-start; initial connection failed: {initial_error:#}",
        socket_path.display()
    )
}

fn remove_stale_socket(socket_path: &Path) -> Result<()> {
    if !socket_path.exists() {
        return Ok(());
    }

    info!("stale socket detected, removing {}", socket_path.display());
    match std::fs::remove_file(socket_path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error)
            .with_context(|| format!("remove stale cued socket {}", socket_path.display())),
    }
}

/// Query the freshly connected local `cued` for its version and warn if it
/// disagrees with the running `cue` build. Optionally auto-restarts the
/// daemon when `CUE_AUTO_UPDATE_CUED=1` is set, returning a fresh client.
#[cfg(feature = "tui")]
pub(crate) async fn check_local_daemon_version(
    client: Option<CuedClient>,
    socket_path: &Path,
) -> Option<CuedClient> {
    let client = client?;
    match check_required_local_daemon_version(client, socket_path).await {
        Ok(client) => Some(client),
        Err(error) => {
            tracing::warn!(%error, "local cued IPC handshake failed");
            eprintln!("cue: {error:#}");
            None
        }
    }
}

pub(crate) async fn check_required_local_daemon_version(
    mut client: CuedClient,
    socket_path: &Path,
) -> Result<CuedClient> {
    use crate::version_check::{
        VersionMatch, auto_update_enabled, check_disabled, local_version, query_daemon_version,
        warn_on_mismatch,
    };

    if check_disabled() {
        return Ok(client);
    }

    let daemon = query_daemon_version(&mut client)
        .await
        .context("local cued did not complete the IPC handshake")?;
    let verdict = VersionMatch::classify(&daemon, local_version());
    if !verdict.is_actionable() {
        return Ok(client);
    }

    // Restart only when a candidate on disk reports the same version as this
    // `cue` binary; otherwise restarting would just relaunch stale `cued`.
    if auto_update_enabled() {
        let candidates = daemon_bin_candidates()?;
        if !local_cued_disk_version_matches_cue(&candidates) {
            eprintln!(
                "cue: CUE_AUTO_UPDATE_CUED=1 was set, but no local cued candidate reports version {}. Restart skipped.",
                local_version()
            );
            warn_on_mismatch(&verdict, false);
            return Ok(client);
        }

        eprintln!(
            "cue: cued is on {}, restarting it to load the matching {} build...",
            describe_daemon_version(&daemon),
            local_version()
        );
        match restart_local_daemon_with_candidates(&candidates, socket_path) {
            Ok(()) => {
                drop(client);
                return require_daemon_running(socket_path).await;
            }
            Err(error) => {
                eprintln!("cue: auto-restart failed: {error:#}");
                warn_on_mismatch(&verdict, false);
                return Ok(client);
            }
        }
    }

    warn_on_mismatch(&verdict, true);
    Ok(client)
}

pub(crate) fn version_from_ping(version: String) -> crate::version_check::DaemonVersion {
    crate::version_check::DaemonVersion(version)
}

fn describe_daemon_version(version: &crate::version_check::DaemonVersion) -> String {
    format!("v{}", version.0)
}

/// Print a one-shot version-mismatch warning for an SSH-attached daemon.
///
/// Auto-restart is intentionally not offered for remote daemons: the
/// deployment story (which user account owns `cued`, who restarts it) is
/// site-specific.
pub(crate) fn warn_on_remote_version_mismatch(daemon: crate::version_check::DaemonVersion) {
    use crate::version_check::{VersionMatch, check_disabled, local_version, warn_on_mismatch};
    if check_disabled() {
        return;
    }
    let verdict = VersionMatch::classify(&daemon, local_version());
    warn_on_mismatch(&verdict, false);
}

fn local_cued_disk_version_matches_cue(candidates: &[OsString]) -> bool {
    let target = crate::version_check::local_version();
    for cued_bin in candidates {
        let Ok(output) = StdCommand::new(cued_bin).arg("--version").output() else {
            continue;
        };
        if !output.status.success() {
            continue;
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        if stdout.split_whitespace().any(|tok| tok == target) {
            return true;
        }
    }
    false
}

fn daemon_bin_candidates() -> Result<Vec<OsString>> {
    daemon_bin_candidates_from_runtime_sources(
        std::env::var_os("CUE_DAEMON_BIN"),
        std::env::current_exe(),
        crate::companion_binary::argv0_path,
    )
}

fn daemon_bin_candidates_from_runtime_sources(
    env_override: Option<OsString>,
    current_exe: io::Result<PathBuf>,
    argv0_path: impl FnOnce() -> Result<Option<PathBuf>>,
) -> Result<Vec<OsString>> {
    if env_override.is_some() {
        return Ok(daemon_bin_candidates_from_sources(env_override, None, None));
    }

    let current_exe =
        current_exe.context("resolve current executable path for cued companion lookup")?;
    let mut candidates = daemon_bin_candidates_from_sources(None, Some(current_exe.clone()), None);
    if !candidates.is_empty() {
        return Ok(candidates);
    }

    candidates = daemon_bin_candidates_from_sources(None, Some(current_exe), argv0_path()?);
    Ok(candidates)
}

fn daemon_bin_candidates_from_sources(
    env_override: Option<OsString>,
    current_exe: Option<PathBuf>,
    argv0: Option<PathBuf>,
) -> Vec<OsString> {
    if let Some(path) = env_override {
        return vec![path];
    }

    let mut candidates = Vec::new();

    if let Some(candidate) = current_exe
        .as_deref()
        .and_then(|path| crate::companion_binary::companion_binary_for_path(path, "cued"))
    {
        push_unique(&mut candidates, candidate.into_os_string());
    }

    if let Some(candidate) = argv0
        .as_deref()
        .and_then(|path| crate::companion_binary::companion_binary_for_path(path, "cued"))
    {
        push_unique(&mut candidates, candidate.into_os_string());
    }

    candidates
}

fn push_unique(paths: &mut Vec<OsString>, path: OsString) {
    if !paths.contains(&path) {
        paths.push(path);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

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

    fn path_os(path: &Path) -> OsString {
        path.as_os_str().to_os_string()
    }

    #[cfg(unix)]
    fn write_executable(path: &Path, body: &str) {
        use std::os::unix::fs::PermissionsExt;

        std::fs::write(path, body).expect("write executable");
        let mut permissions = std::fs::metadata(path)
            .expect("stat executable")
            .permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(path, permissions).expect("chmod executable");
    }

    #[cfg(not(unix))]
    fn write_executable(path: &Path, body: &str) {
        std::fs::write(path, body).expect("write executable");
    }

    #[test]
    fn daemon_bin_prefers_override() {
        assert_eq!(
            daemon_bin_candidates_from_sources(
                Some(OsString::from("/custom/cued")),
                Some(PathBuf::from("/bin/cue")),
                Some(PathBuf::from("./target/debug/cue"))
            ),
            vec![OsString::from("/custom/cued")]
        );
    }

    #[test]
    fn daemon_bin_override_does_not_require_current_exe() {
        let candidates = daemon_bin_candidates_from_runtime_sources(
            Some(OsString::from("/custom/cued")),
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "current executable disappeared",
            )),
            || panic!("argv0 should not be consulted when CUE_DAEMON_BIN is set"),
        )
        .expect("explicit daemon override should be enough");

        assert_eq!(candidates, vec![OsString::from("/custom/cued")]);
    }

    #[test]
    fn daemon_bin_reports_current_exe_failure_without_override() {
        let error = daemon_bin_candidates_from_runtime_sources(
            None,
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "current executable disappeared",
            )),
            || panic!("argv0 should not hide current_exe failures"),
        )
        .expect_err("current_exe errors should not fall through to argv0 or implicit lookup");

        let message = format!("{error:#}");
        assert!(message.contains("resolve current executable path"));
        assert!(message.contains("current executable disappeared"));
    }

    #[test]
    fn daemon_bin_runtime_lookup_uses_argv0_after_current_exe_has_no_companion() {
        let current_dir = make_temp_bin_dir();
        let argv0_dir = make_temp_bin_dir();
        let current_cue = current_dir.join("cue");
        let argv0_cue = argv0_dir.join("cue");
        let argv0_cued = argv0_dir.join("cued");
        touch(&current_cue);
        touch(&argv0_cue);
        write_executable(&argv0_cued, "#!/bin/sh\n");

        let candidates = daemon_bin_candidates_from_runtime_sources(None, Ok(current_cue), || {
            Ok(Some(argv0_cue))
        })
        .expect("argv0 daemon lookup should succeed");

        assert_eq!(candidates, vec![path_os(&argv0_cued)]);
        std::fs::remove_dir_all(current_dir).expect("remove current temp bin dir");
        std::fs::remove_dir_all(argv0_dir).expect("remove argv0 temp bin dir");
    }

    #[test]
    fn daemon_bin_uses_current_exe_sibling() {
        let dir = make_temp_bin_dir();
        let cue = dir.join("cue");
        let cued = dir.join("cued");
        touch(&cue);
        write_executable(&cued, "#!/bin/sh\n");

        assert_eq!(
            daemon_bin_candidates_from_sources(None, Some(cue), None),
            vec![path_os(&cued)]
        );

        std::fs::remove_dir_all(dir).expect("remove temp bin dir");
    }

    #[test]
    fn daemon_bin_falls_back_to_argv0_sibling() {
        let dir = make_temp_bin_dir();
        let cue = dir.join("cue");
        let cued = dir.join("cued");
        touch(&cue);
        write_executable(&cued, "#!/bin/sh\n");

        assert_eq!(
            daemon_bin_candidates_from_sources(None, None, Some(cue)),
            vec![path_os(&cued)]
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
        write_executable(&cued, "#!/bin/sh\n");

        assert_eq!(
            daemon_bin_candidates_from_sources(None, Some(cue), None),
            vec![path_os(&cued)]
        );

        std::fs::remove_dir_all(dir).expect("remove temp bin dir");
    }

    #[test]
    fn daemon_bin_without_companion_has_no_implicit_path_lookup() {
        assert_eq!(
            daemon_bin_candidates_from_sources(None, None, None),
            Vec::<OsString>::new()
        );
    }

    #[test]
    fn local_start_without_candidates_reports_required_companion_or_override() {
        let dir = make_temp_bin_dir();
        let socket = dir.join("custom.sock");

        let error = start_local_daemon_with_candidates(&[], &socket)
            .expect_err("missing daemon candidates should fail loudly");
        let message = format!("{error:#}");

        assert!(
            message.contains("no cued executable was able to start"),
            "{message}"
        );
        assert!(
            message.contains("first-party `cued` companion"),
            "{message}"
        );
        assert!(message.contains("CUE_DAEMON_BIN"), "{message}");
        assert!(message.contains(&socket.display().to_string()), "{message}");

        std::fs::remove_dir_all(dir).expect("remove temp bin dir");
    }

    #[test]
    fn stale_socket_cleanup_removes_existing_socket_file() {
        let dir = make_temp_bin_dir();
        let socket = dir.join("cued.sock");
        touch(&socket);

        remove_stale_socket(&socket).expect("remove stale socket");

        assert!(!socket.exists());
        std::fs::remove_dir_all(dir).expect("remove temp bin dir");
    }

    #[test]
    fn stale_socket_cleanup_allows_missing_socket() {
        let dir = make_temp_bin_dir();
        let socket = dir.join("cued.sock");

        remove_stale_socket(&socket).expect("missing socket is already clean");

        std::fs::remove_dir_all(dir).expect("remove temp bin dir");
    }

    #[cfg(unix)]
    #[test]
    fn local_restart_failure_includes_process_output() {
        let dir = make_temp_bin_dir();
        let cued = dir.join("cued");
        write_executable(
            &cued,
            "#!/bin/sh\necho restart failed >&2\necho restart note\nexit 7\n",
        );
        let socket = dir.join("custom.sock");

        let error = restart_local_daemon_with_candidates(&[path_os(&cued)], &socket)
            .expect_err("restart should fail");
        let message = format!("{error:#}");

        assert!(
            message.contains("no cued executable was able to restart"),
            "{message}"
        );
        assert!(message.contains("exit status: 7"), "{message}");
        assert!(message.contains("stderr: restart failed"), "{message}");
        assert!(message.contains("stdout: restart note"), "{message}");
        assert!(message.contains(&socket.display().to_string()), "{message}");

        std::fs::remove_dir_all(dir).expect("remove temp bin dir");
    }

    #[cfg(unix)]
    #[test]
    fn local_start_failure_includes_process_output() {
        let dir = make_temp_bin_dir();
        let cued = dir.join("cued");
        write_executable(
            &cued,
            "#!/bin/sh\necho start failed >&2\necho start note\nexit 8\n",
        );
        let socket = dir.join("custom.sock");

        let error = start_local_daemon_with_candidates(&[path_os(&cued)], &socket)
            .expect_err("start should fail");
        let message = format!("{error:#}");

        assert!(
            message.contains("no cued executable was able to start"),
            "{message}"
        );
        assert!(message.contains("exit status: 8"), "{message}");
        assert!(message.contains("stderr: start failed"), "{message}");
        assert!(message.contains("stdout: start note"), "{message}");
        assert!(message.contains(&socket.display().to_string()), "{message}");

        std::fs::remove_dir_all(dir).expect("remove temp bin dir");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn required_daemon_start_failure_returns_process_output() {
        let dir = make_temp_bin_dir();
        let cued = dir.join("cued");
        write_executable(
            &cued,
            "#!/bin/sh\necho required start failed >&2\necho required start note\nexit 9\n",
        );
        let socket = dir.join("required.sock");

        let error = match require_daemon_running_with_candidates(&socket, &[path_os(&cued)]).await {
            Ok(_) => panic!("required daemon startup should fail loudly"),
            Err(error) => error,
        };
        let message = format!("{error:#}");

        assert!(message.contains("auto-start cued for"), "{message}");
        assert!(
            message.contains("no cued executable was able to start"),
            "{message}"
        );
        assert!(message.contains("exit status: 9"), "{message}");
        assert!(
            message.contains("stderr: required start failed"),
            "{message}"
        );
        assert!(message.contains("stdout: required start note"), "{message}");
        assert!(message.contains(&socket.display().to_string()), "{message}");

        std::fs::remove_dir_all(dir).expect("remove temp bin dir");
    }

    #[cfg(unix)]
    #[test]
    fn disk_version_check_matches_candidate_version_output() {
        let dir = make_temp_bin_dir();
        let cued = dir.join("cued");
        write_executable(
            &cued,
            &format!(
                "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 'cued {}'; exit 0; fi\nexit 1\n",
                crate::version_check::local_version()
            ),
        );

        assert!(local_cued_disk_version_matches_cue(&[path_os(&cued)]));

        std::fs::remove_dir_all(dir).expect("remove temp bin dir");
    }

    #[cfg(unix)]
    #[test]
    fn disk_version_check_rejects_missing_or_stale_candidates() {
        let dir = make_temp_bin_dir();
        let stale = dir.join("cued-stale");
        write_executable(
            &stale,
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 'cued 0.0.0'; exit 0; fi\nexit 1\n",
        );

        assert!(!local_cued_disk_version_matches_cue(&[
            path_os(&dir.join("missing")),
            path_os(&stale),
        ]));

        std::fs::remove_dir_all(dir).expect("remove temp bin dir");
    }

    #[cfg(feature = "tui")]
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

    #[cfg(feature = "tui")]
    #[test]
    fn remote_restart_invocation_renders_actual_ssh_command() {
        assert_eq!(
            remote_restart_invocation("dev box", "cued restart --socket /tmp/cued.sock"),
            "ssh 'dev box' 'cued restart --socket /tmp/cued.sock'"
        );
    }
}
