//! Platform service management.
//!
//! - macOS: `launchd` via `~/Library/LaunchAgents/com.cue-shell.cued.plist`
//! - Linux: `systemd --user` via `~/.config/systemd/user/cued.service`
//!
//! The design uses `KeepAlive: { SuccessfulExit: false }` on macOS so that a
//! normal daemon shutdown (exit code 0) does **not** trigger an automatic
//! restart, while crashes do.  On Linux `Restart=on-failure` achieves the same
//! semantics.

use std::path::Path;

use anyhow::{Context, Result, bail};

use crate::command_util::CommandSpec;

#[cfg(target_os = "macos")]
const SERVICE_LABEL: &str = "com.cue-shell.cued";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ServiceProcessState {
    Active(u32),
    Inactive,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CurrentProcessOwnership {
    Managed,
    NotManaged,
    Unknown,
}

// ── Public API ──────────────────────────────────────────────────────────────

/// Returns `true` if the service unit/plist file is present on disk.
pub fn is_installed() -> Result<bool> {
    Ok(service_file_path()?.exists())
}

pub(crate) fn process_state() -> ServiceProcessState {
    service_process_state().unwrap_or(ServiceProcessState::Unknown)
}

pub(crate) fn current_process_ownership() -> CurrentProcessOwnership {
    current_process_ownership_from_state(process_state(), std::process::id())
}

fn current_process_ownership_from_state(
    state: ServiceProcessState,
    current_pid: u32,
) -> CurrentProcessOwnership {
    match state {
        ServiceProcessState::Active(pid) if pid == current_pid => CurrentProcessOwnership::Managed,
        ServiceProcessState::Active(_) | ServiceProcessState::Inactive => {
            CurrentProcessOwnership::NotManaged
        }
        ServiceProcessState::Unknown => CurrentProcessOwnership::Unknown,
    }
}

#[cfg(any(target_os = "linux", test))]
fn systemd_process_state_from_output(
    success: bool,
    stdout: &[u8],
    stderr: &[u8],
) -> ServiceProcessState {
    if success {
        return match String::from_utf8_lossy(stdout).trim().parse::<u32>() {
            Ok(0) => ServiceProcessState::Inactive,
            Ok(pid) => ServiceProcessState::Active(pid),
            Err(_) => ServiceProcessState::Unknown,
        };
    }

    let failure = format!(
        "{}\n{}",
        String::from_utf8_lossy(stdout),
        String::from_utf8_lossy(stderr)
    )
    .to_ascii_lowercase();
    let names_cued_unit = failure.contains("cued.service") || failure.contains("unit cued");
    if names_cued_unit
        && (failure.contains("not found")
            || failure.contains("not loaded")
            || failure.contains("no such unit")
            || failure.contains("could not be found"))
    {
        ServiceProcessState::Inactive
    } else {
        ServiceProcessState::Unknown
    }
}

/// Ask the service manager to start the installed job without replacing a
/// process that may already be the exact restart successor.
pub(crate) fn start_if_needed() -> Result<()> {
    start_service_if_needed()
}

/// Write the service file and activate it so cued starts at login.
pub fn install(exe_path: &Path) -> Result<()> {
    let file = service_file_path()?;
    if let Some(parent) = file.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create service dir {}", parent.display()))?;
    }

    let log = crate::dirs::log_path()?;
    if let Some(parent) = log.parent() {
        crate::dirs::ensure_private_dir(parent)
            .with_context(|| format!("create log directory {}", parent.display()))?;
    }
    crate::dirs::ensure_private_file(&log)
        .with_context(|| format!("secure log file {}", log.display()))?;
    let content = service_file_content(exe_path, &log)?;
    std::fs::write(&file, &content)
        .with_context(|| format!("write service file {}", file.display()))?;

    activate(&file)?;

    println!("cued: service installed ({})", file.display());
    println!("cued: daemon started — will run automatically at login");
    Ok(())
}

/// Deactivate and remove the service file.
pub fn uninstall() -> Result<()> {
    let file = service_file_path()?;
    if !file.exists() {
        println!("cued: service is not installed");
        return Ok(());
    }
    deactivate(&file)?;
    std::fs::remove_file(&file)
        .with_context(|| format!("remove service file {}", file.display()))?;
    println!("cued: service uninstalled");
    Ok(())
}

/// Restart the managed service (e.g., after a binary upgrade).
pub fn restart() -> Result<()> {
    restart_service()
}

fn warn_deactivate_failures(failures: Vec<String>) {
    if failures.is_empty() {
        return;
    }
    eprintln!(
        "cued: warning: service manager did not confirm deactivation; removing the service file anyway\n{}",
        failures.join("\n")
    );
}

fn canonical_service_exe_path(exe_path: &Path) -> Result<std::path::PathBuf> {
    exe_path
        .canonicalize()
        .with_context(|| format!("resolve service executable path {}", exe_path.display()))
}

// ── macOS (launchd) ─────────────────────────────────────────────────────────

#[cfg(target_os = "macos")]
fn service_file_path() -> Result<std::path::PathBuf> {
    Ok(crate::dirs::home_dir()?
        .join("Library/LaunchAgents")
        .join(format!("{SERVICE_LABEL}.plist")))
}

#[cfg(target_os = "macos")]
fn service_file_content(exe_path: &Path, log_path: &Path) -> Result<String> {
    let exe = canonical_service_exe_path(exe_path)?;
    Ok(format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
    "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{label}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{exe}</string>
        <string>start</string>
        <string>--fg</string>
        <string>--preserve-restart-fence</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
    </dict>
    <key>StandardErrorPath</key>
    <string>{log}</string>
</dict>
</plist>
"#,
        label = SERVICE_LABEL,
        exe = exe.display(),
        log = log_path.display(),
    ))
}

#[cfg(target_os = "macos")]
fn activate(plist: &Path) -> Result<()> {
    let uid = unsafe { libc::getuid() };
    let target = format!("gui/{uid}");
    let plist_str = plist.to_string_lossy();

    let bootstrap_cmd =
        CommandSpec::new("launchctl").args(["bootstrap", target.as_str(), plist_str.as_ref()]);
    let bootstrap = bootstrap_cmd.output()?;
    if !bootstrap.status.success() {
        bail!(
            "launchctl bootstrap failed — check the plist at {}\n{}",
            plist.display(),
            bootstrap_cmd.failure_summary(&bootstrap)
        );
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn deactivate(plist: &Path) -> Result<()> {
    let uid = unsafe { libc::getuid() };
    let target = format!("gui/{uid}");
    let plist_str = plist.to_string_lossy();
    let bootout_cmd =
        CommandSpec::new("launchctl").args(["bootout", target.as_str(), plist_str.as_ref()]);
    match bootout_cmd.output() {
        Ok(output) if output.status.success() => return Ok(()),
        Ok(output) => warn_deactivate_failures(vec![bootout_cmd.failure_summary(&output)]),
        Err(error) => warn_deactivate_failures(vec![format!(
            "`{}` failed to run: {error:#}",
            bootout_cmd.display()
        )]),
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn restart_service() -> Result<()> {
    let uid = unsafe { libc::getuid() };
    let service = format!("gui/{uid}/{SERVICE_LABEL}");
    let command = CommandSpec::new("launchctl").args(["kickstart", "-k", service.as_str()]);
    let output = command.output()?;
    if !output.status.success() {
        bail!("{}", command.failure_summary(&output));
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn start_service_command() -> CommandSpec {
    let uid = unsafe { libc::getuid() };
    let service = format!("gui/{uid}/{SERVICE_LABEL}");
    CommandSpec::new("launchctl").args(["kickstart", service.as_str()])
}

#[cfg(target_os = "macos")]
fn start_service_if_needed() -> Result<()> {
    let command = start_service_command();
    let output = command.output()?;
    if !output.status.success() {
        bail!("{}", command.failure_summary(&output));
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn service_process_state() -> Result<ServiceProcessState> {
    let uid = unsafe { libc::getuid() };
    let service = format!("gui/{uid}/{SERVICE_LABEL}");
    let command = CommandSpec::new("launchctl").args(["print", service.as_str()]);
    let output = command.output()?;
    if !output.status.success() {
        let summary = command.failure_summary(&output).to_ascii_lowercase();
        if summary.contains("could not find service") || summary.contains("not found") {
            return Ok(ServiceProcessState::Inactive);
        }
        return Ok(ServiceProcessState::Unknown);
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout
        .lines()
        .find_map(|line| line.trim().strip_prefix("pid = "))
        .and_then(|pid| pid.trim().parse::<u32>().ok())
        .filter(|pid| *pid != 0)
        .map_or(ServiceProcessState::Inactive, ServiceProcessState::Active))
}

// ── Linux (systemd --user) ───────────────────────────────────────────────────

#[cfg(target_os = "linux")]
fn service_file_path() -> Result<std::path::PathBuf> {
    // Systemd user units live at ~/.config/systemd/user/.
    Ok(crate::dirs::home_dir()?.join(".config/systemd/user/cued.service"))
}

#[cfg(target_os = "linux")]
fn service_file_content(exe_path: &Path, _log_path: &Path) -> Result<String> {
    let exe = canonical_service_exe_path(exe_path)?;
    Ok(format!(
        "[Unit]\n\
         Description=cued — background daemon for cue-shell\n\
         After=default.target\n\
         StartLimitIntervalSec=30\n\
         StartLimitBurst=5\n\
         \n\
         [Service]\n\
         ExecStart={exe} start --fg --preserve-restart-fence\n\
         Restart=on-failure\n\
         RestartSec=250ms\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n",
        exe = exe.display(),
    ))
}

#[cfg(target_os = "linux")]
fn activate(_unit: &Path) -> Result<()> {
    let reload_cmd = CommandSpec::new("systemctl").args(["--user", "daemon-reload"]);
    let reload = reload_cmd.output()?;
    if !reload.status.success() {
        bail!("{}", reload_cmd.failure_summary(&reload));
    }
    let enable_cmd = CommandSpec::new("systemctl").args(["--user", "enable", "--now", "cued"]);
    let enable = enable_cmd.output()?;
    if !enable.status.success() {
        bail!("{}", enable_cmd.failure_summary(&enable));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn deactivate(_unit: &Path) -> Result<()> {
    let mut failures = Vec::new();

    let disable_cmd = CommandSpec::new("systemctl").args(["--user", "disable", "--now", "cued"]);
    match disable_cmd.output() {
        Ok(output) if output.status.success() => {}
        Ok(output) => failures.push(disable_cmd.failure_summary(&output)),
        Err(error) => failures.push(format!(
            "`{}` failed to run: {error:#}",
            disable_cmd.display()
        )),
    }

    let reload_cmd = CommandSpec::new("systemctl").args(["--user", "daemon-reload"]);
    match reload_cmd.output() {
        Ok(output) if output.status.success() => {}
        Ok(output) => failures.push(reload_cmd.failure_summary(&output)),
        Err(error) => failures.push(format!(
            "`{}` failed to run: {error:#}",
            reload_cmd.display()
        )),
    }

    warn_deactivate_failures(failures);
    Ok(())
}

#[cfg(target_os = "linux")]
fn restart_service() -> Result<()> {
    let command = CommandSpec::new("systemctl").args(["--user", "restart", "cued"]);
    let output = command.output()?;
    if !output.status.success() {
        bail!("{}", command.failure_summary(&output));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn start_service_command() -> CommandSpec {
    CommandSpec::new("systemctl").args(["--user", "start", "cued"])
}

#[cfg(target_os = "linux")]
fn start_service_if_needed() -> Result<()> {
    let command = start_service_command();
    let output = command.output()?;
    if !output.status.success() {
        bail!("{}", command.failure_summary(&output));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn service_process_state() -> Result<ServiceProcessState> {
    let command = CommandSpec::new("systemctl").args([
        "--user",
        "show",
        "--property=MainPID",
        "--value",
        "cued",
    ]);
    let output = command.output()?;
    Ok(systemd_process_state_from_output(
        output.status.success(),
        &output.stdout,
        &output.stderr,
    ))
}

// ── Unsupported platforms ────────────────────────────────────────────────────

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn service_file_path() -> Result<std::path::PathBuf> {
    Ok(std::path::PathBuf::from("/unsupported"))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn service_file_content(_exe: &Path, _log: &Path) -> Result<String> {
    bail!("service management is not supported on this platform")
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn activate(_: &Path) -> Result<()> {
    bail!("service management is not supported on this platform")
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn deactivate(_: &Path) -> Result<()> {
    bail!("service management is not supported on this platform")
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn restart_service() -> Result<()> {
    bail!("service management is not supported on this platform")
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn start_service_if_needed() -> Result<()> {
    bail!("service management is not supported on this platform")
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn service_process_state() -> Result<ServiceProcessState> {
    Ok(ServiceProcessState::Unknown)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicU64, Ordering};

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn make_temp_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "cue-service-test-{}-{}",
            std::process::id(),
            TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).expect("create temp service test dir");
        dir
    }

    #[test]
    fn service_file_content_rejects_missing_executable_path() {
        let dir = make_temp_dir();
        let missing = dir.join("missing-cued");

        let error = service_file_content(&missing, &dir.join("cued.log"))
            .expect_err("service content should not hide missing executable paths");

        let message = format!("{error:#}");
        assert!(message.contains("resolve service executable path"));
        assert!(message.contains("missing-cued"));
        std::fs::remove_dir_all(dir).expect("remove temp service test dir");
    }

    #[cfg(unix)]
    #[test]
    fn service_file_content_uses_canonical_executable_path() {
        let dir = make_temp_dir();
        let bin_dir = dir.join("bin");
        std::fs::create_dir_all(&bin_dir).expect("create bin dir");
        let exe = bin_dir.join("cued");
        std::fs::write(&exe, "#!/bin/sh\n").expect("write executable");
        let symlink = dir.join("cued-link");
        std::os::unix::fs::symlink(&exe, &symlink).expect("create executable symlink");

        let content = service_file_content(&symlink, &dir.join("cued.log"))
            .expect("service content should resolve existing executable symlink");

        assert!(content.contains(&format!("{}", exe.display())));
        assert!(!content.contains(&format!("{}", symlink.display())));
        std::fs::remove_dir_all(dir).expect("remove temp service test dir");
    }

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn start_if_needed_command_is_non_destructive() {
        let command = start_service_command().display();
        assert!(!command.contains(" restart "), "{command}");
        assert!(!command.contains(" -k "), "{command}");
        #[cfg(target_os = "macos")]
        assert!(command.contains("launchctl kickstart"), "{command}");
        #[cfg(target_os = "linux")]
        assert!(command.contains("systemctl --user start cued"), "{command}");
    }

    #[test]
    fn current_process_ownership_requires_exact_manager_main_pid() {
        assert_eq!(
            current_process_ownership_from_state(ServiceProcessState::Active(7), 7),
            CurrentProcessOwnership::Managed
        );
        assert_eq!(
            current_process_ownership_from_state(ServiceProcessState::Active(8), 7),
            CurrentProcessOwnership::NotManaged
        );
        assert_eq!(
            current_process_ownership_from_state(ServiceProcessState::Inactive, 7),
            CurrentProcessOwnership::NotManaged
        );
        assert_eq!(
            current_process_ownership_from_state(ServiceProcessState::Unknown, 7),
            CurrentProcessOwnership::Unknown
        );
    }

    #[test]
    fn systemd_state_parser_distinguishes_inactive_from_unknown_failures() {
        assert_eq!(
            systemd_process_state_from_output(true, b"42\n", b""),
            ServiceProcessState::Active(42)
        );
        assert_eq!(
            systemd_process_state_from_output(true, b"0\n", b""),
            ServiceProcessState::Inactive
        );
        for message in [
            "Unit cued.service not found.",
            "Unit cued.service is not loaded.",
            "No such unit: cued.service",
        ] {
            assert_eq!(
                systemd_process_state_from_output(false, b"", message.as_bytes()),
                ServiceProcessState::Inactive,
                "{message}"
            );
        }
        assert_eq!(
            systemd_process_state_from_output(false, b"", b"Failed to connect to bus: denied"),
            ServiceProcessState::Unknown
        );
        assert_eq!(
            systemd_process_state_from_output(true, b"not-a-pid", b""),
            ServiceProcessState::Unknown
        );
    }
}
