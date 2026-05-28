use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::{Command as StdCommand, Output, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use cue_client::{
    CuedClient, ResolvedTransport, RestartHandle, default_socket_path, ssh_invocation,
};
use tracing::info;

pub(crate) fn restart_handle_for_transport(transport: &ResolvedTransport) -> RestartHandle {
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
    let candidates = daemon_bin_candidates();
    restart_local_daemon_with_candidates(&candidates, socket_path)
}

fn restart_local_daemon_with_candidates(candidates: &[String], socket_path: &Path) -> Result<()> {
    let socket_override = socket_path != default_socket_path().as_path();
    let mut failures = Vec::new();

    for cued_bin in candidates {
        let args = local_restart_args(socket_override, socket_path);
        let invocation = command_display(OsStr::new(cued_bin), &args);

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
    let invocation = ssh_invocation(destination, &remote_command);
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

fn local_restart_args(socket_override: bool, socket_path: &Path) -> Vec<OsString> {
    let mut args = vec![OsString::from("restart")];
    if socket_override {
        args.push(OsString::from("--socket"));
        args.push(socket_path.as_os_str().to_os_string());
    }
    args
}

fn start_local_daemon(socket_path: &Path) -> Result<()> {
    let candidates = daemon_bin_candidates();
    start_local_daemon_with_candidates(&candidates, socket_path)
}

fn start_local_daemon_with_candidates(candidates: &[String], socket_path: &Path) -> Result<()> {
    let args = local_start_args(socket_path);
    let mut failures = Vec::new();

    for cued_bin in candidates {
        let invocation = command_display(OsStr::new(cued_bin), &args);
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
        summary
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
pub(crate) async fn ensure_daemon_running(socket_path: &Path) -> Option<CuedClient> {
    if let Ok(client) = CuedClient::connect(socket_path).await {
        info!("cued already running");
        return Some(client);
    }

    if let Err(error) = remove_stale_socket(socket_path) {
        tracing::warn!(
            %error,
            socket_path = %socket_path.display(),
            "failed to remove stale cued socket"
        );
        return None;
    }

    info!("cued not running, attempting to start");
    if let Err(error) = start_local_daemon(socket_path) {
        tracing::warn!(%error, "failed to run cued start");
        eprintln!(
            "cue: failed to auto-start cued for {}:\n{error:#}",
            socket_path.display()
        );
        return None;
    }

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
pub(crate) async fn check_local_daemon_version(
    client: Option<CuedClient>,
    socket_path: &Path,
) -> Option<CuedClient> {
    use crate::version_check::{
        VersionMatch, auto_update_enabled, check_disabled, local_version, query_daemon_version,
        warn_on_mismatch,
    };

    let mut client = client?;
    if check_disabled() {
        return Some(client);
    }

    let daemon = match query_daemon_version(&mut client).await {
        Ok(version) => version,
        Err(error) => {
            tracing::warn!(%error, "local cued IPC handshake failed");
            eprintln!("cue: local cued did not complete the IPC handshake: {error:#}");
            return None;
        }
    };
    let verdict = VersionMatch::classify(&daemon, local_version());
    if !verdict.is_actionable() {
        return Some(client);
    }

    // Restart only when a candidate on disk reports the same version as this
    // `cue` binary; otherwise restarting would just relaunch stale `cued`.
    if auto_update_enabled() {
        let candidates = daemon_bin_candidates();
        if !local_cued_disk_version_matches_cue(&candidates) {
            eprintln!(
                "cue: CUE_AUTO_UPDATE_CUED=1 was set, but no local cued candidate reports version {}. Restart skipped.",
                local_version()
            );
            warn_on_mismatch(&verdict, false);
            return Some(client);
        }

        eprintln!(
            "cue: cued is on {}, restarting it to load the matching {} build...",
            describe_daemon_version(&daemon),
            local_version()
        );
        match restart_local_daemon_with_candidates(&candidates, socket_path) {
            Ok(()) => {
                drop(client);
                let new_client = ensure_daemon_running(socket_path).await?;
                return Some(new_client);
            }
            Err(error) => {
                eprintln!("cue: auto-restart failed: {error:#}");
                warn_on_mismatch(&verdict, false);
                return Some(client);
            }
        }
    }

    warn_on_mismatch(&verdict, true);
    Some(client)
}

pub(crate) fn version_from_ping(version: Option<String>) -> crate::version_check::DaemonVersion {
    match version {
        Some(version) => crate::version_check::DaemonVersion::Reported(version),
        None => crate::version_check::DaemonVersion::Unknown,
    }
}

fn describe_daemon_version(version: &crate::version_check::DaemonVersion) -> String {
    use crate::version_check::DaemonVersion;
    match version {
        DaemonVersion::Reported(v) => format!("v{v}"),
        DaemonVersion::Unknown => "an unknown older version".to_string(),
    }
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

fn local_cued_disk_version_matches_cue(candidates: &[String]) -> bool {
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

fn daemon_bin_candidates() -> Vec<String> {
    daemon_bin_candidates_from_sources(
        std::env::var("CUE_DAEMON_BIN").ok(),
        std::env::current_exe().ok(),
        argv0_path(),
    )
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

fn push_unique(paths: &mut Vec<String>, path: String) {
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

        let error = restart_local_daemon_with_candidates(&[cued.display().to_string()], &socket)
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

        let error = start_local_daemon_with_candidates(&[cued.display().to_string()], &socket)
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

        assert!(local_cued_disk_version_matches_cue(&[cued
            .display()
            .to_string()]));

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
            dir.join("missing").display().to_string(),
            stale.display().to_string(),
        ]));

        std::fs::remove_dir_all(dir).expect("remove temp bin dir");
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
}
