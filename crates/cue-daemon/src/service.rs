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
    let ok = std::process::Command::new("launchctl")
        .args(["bootstrap", &target, plist_str.as_ref()])
        .status()
        .context("run launchctl bootstrap")?
        .success();
    if ok {
        return Ok(());
    }

    let ok2 = std::process::Command::new("launchctl")
        .args(["load", "-w", plist_str.as_ref()])
        .status()
        .context("run launchctl load")?
        .success();
    if !ok2 {
        bail!(
            "launchctl bootstrap and launchctl load both failed — \
             check the plist at {}",
            plist.display()
        );
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn deactivate(plist: &Path) -> Result<()> {
    let uid = unsafe { libc::getuid() };
    let target = format!("gui/{uid}");
    let plist_str = plist.to_string_lossy();
    // Best-effort: try both modern and legacy forms.
    let _ = std::process::Command::new("launchctl")
        .args(["bootout", &target, plist_str.as_ref()])
        .status();
    let _ = std::process::Command::new("launchctl")
        .args(["unload", "-w", plist_str.as_ref()])
        .status();
    Ok(())
}

#[cfg(target_os = "macos")]
fn restart_service() -> Result<()> {
    let uid = unsafe { libc::getuid() };
    let service = format!("gui/{uid}/{SERVICE_LABEL}");
    let ok = std::process::Command::new("launchctl")
        .args(["kickstart", "-k", &service])
        .status()
        .context("run launchctl kickstart")?
        .success();
    if !ok {
        bail!("launchctl kickstart -k {service} failed");
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
    let ok = std::process::Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .status()
        .context("systemctl daemon-reload")?
        .success();
    if !ok {
        bail!("systemctl --user daemon-reload failed");
    }
    let ok2 = std::process::Command::new("systemctl")
        .args(["--user", "enable", "--now", "cued"])
        .status()
        .context("systemctl enable --now cued")?
        .success();
    if !ok2 {
        bail!("systemctl --user enable --now cued failed");
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn deactivate(_unit: &Path) -> Result<()> {
    // Best-effort.
    let _ = std::process::Command::new("systemctl")
        .args(["--user", "disable", "--now", "cued"])
        .status();
    let _ = std::process::Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .status();
    Ok(())
}

#[cfg(target_os = "linux")]
fn restart_service() -> Result<()> {
    let ok = std::process::Command::new("systemctl")
        .args(["--user", "restart", "cued"])
        .status()
        .context("systemctl --user restart cued")?
        .success();
    if !ok {
        bail!("systemctl --user restart cued failed");
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
