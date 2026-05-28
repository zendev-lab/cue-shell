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

// ── Public API ──────────────────────────────────────────────────────────────

/// Returns `true` if the service unit/plist file is present on disk.
pub fn is_installed() -> bool {
    service_file_path().exists()
}

/// Write the service file and activate it so cued starts at login.
pub fn install(exe_path: &Path) -> Result<()> {
    let file = service_file_path();
    if let Some(parent) = file.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create service dir {}", parent.display()))?;
    }

    let log = crate::dirs::log_path();
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
    let file = service_file_path();
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

// ── macOS (launchd) ─────────────────────────────────────────────────────────

#[cfg(target_os = "macos")]
fn service_file_path() -> std::path::PathBuf {
    crate::dirs::home_dir()
        .join("Library/LaunchAgents")
        .join(format!("{SERVICE_LABEL}.plist"))
}

#[cfg(target_os = "macos")]
fn service_file_content(exe_path: &Path, log_path: &Path) -> Result<String> {
    let exe = exe_path
        .canonicalize()
        .unwrap_or_else(|_| exe_path.to_path_buf());
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

    // Try modern `bootstrap` (macOS 10.15+), fall back to legacy `load`.
    let bootstrap_cmd =
        CommandSpec::new("launchctl").args(["bootstrap", target.as_str(), plist_str.as_ref()]);
    let bootstrap = bootstrap_cmd.output()?;
    if bootstrap.status.success() {
        return Ok(());
    }

    let load_cmd = CommandSpec::new("launchctl").args(["load", "-w", plist_str.as_ref()]);
    let load = load_cmd.output()?;
    if !load.status.success() {
        bail!(
            "launchctl bootstrap and launchctl load both failed — \
             check the plist at {}\n{}\n{}",
            plist.display(),
            bootstrap_cmd.failure_summary(&bootstrap),
            load_cmd.failure_summary(&load)
        );
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn deactivate(plist: &Path) -> Result<()> {
    let uid = unsafe { libc::getuid() };
    let target = format!("gui/{uid}");
    let plist_str = plist.to_string_lossy();
    let mut failures = Vec::new();

    // Try both modern and legacy forms. A missing/unloaded service should not
    // prevent removing the stale plist, but the service manager diagnostics
    // should still be visible to the user.
    let bootout_cmd =
        CommandSpec::new("launchctl").args(["bootout", target.as_str(), plist_str.as_ref()]);
    match bootout_cmd.output() {
        Ok(output) if output.status.success() => return Ok(()),
        Ok(output) => failures.push(bootout_cmd.failure_summary(&output)),
        Err(error) => failures.push(format!(
            "`{}` failed to run: {error:#}",
            bootout_cmd.display()
        )),
    }

    let unload_cmd = CommandSpec::new("launchctl").args(["unload", "-w", plist_str.as_ref()]);
    match unload_cmd.output() {
        Ok(output) if output.status.success() => return Ok(()),
        Ok(output) => failures.push(unload_cmd.failure_summary(&output)),
        Err(error) => failures.push(format!(
            "`{}` failed to run: {error:#}",
            unload_cmd.display()
        )),
    }

    warn_deactivate_failures(failures);
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

// ── Linux (systemd --user) ───────────────────────────────────────────────────

#[cfg(target_os = "linux")]
fn service_file_path() -> std::path::PathBuf {
    // Systemd user units live at ~/.config/systemd/user/.
    crate::dirs::home_dir().join(".config/systemd/user/cued.service")
}

#[cfg(target_os = "linux")]
fn service_file_content(exe_path: &Path, _log_path: &Path) -> Result<String> {
    let exe = exe_path
        .canonicalize()
        .unwrap_or_else(|_| exe_path.to_path_buf());
    Ok(format!(
        "[Unit]\n\
         Description=cued — background daemon for cue-shell\n\
         After=default.target\n\
         \n\
         [Service]\n\
         ExecStart={exe} start --fg\n\
         Restart=on-failure\n\
         RestartSec=3\n\
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

// ── Unsupported platforms ────────────────────────────────────────────────────

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn service_file_path() -> std::path::PathBuf {
    std::path::PathBuf::from("/unsupported")
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
