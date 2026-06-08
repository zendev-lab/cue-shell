//! XDG-compliant directory resolution for cued.
//!
//! ```text
//! Runtime:  $XDG_RUNTIME_DIR/cue-shell/  (socket, pid)
//! Data:     $XDG_DATA_HOME/cue-shell/    (db, output)
//! State:    $XDG_STATE_HOME/cue-shell/   (logs)
//! Config:   $XDG_CONFIG_HOME/cue-shell/  (config)
//! ```

use std::ffi::OsString;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};

const APP_DIR: &str = "cue-shell";

// ── Runtime dir (socket + PID) ──

fn runtime_dir() -> PathBuf {
    runtime_dir_from_env(std::env::var_os("XDG_RUNTIME_DIR"), std::env::temp_dir())
}

fn runtime_dir_from_env(xdg_runtime_dir: Option<OsString>, temp_dir: PathBuf) -> PathBuf {
    if let Some(dir) = non_empty_env(xdg_runtime_dir) {
        PathBuf::from(dir).join(APP_DIR)
    } else {
        temp_dir.join(APP_DIR)
    }
}

/// Path to the Unix domain socket: `$XDG_RUNTIME_DIR/cue-shell/cued.sock`.
pub fn socket_path() -> PathBuf {
    runtime_dir().join("cued.sock")
}

/// Path to the PID file: `$XDG_RUNTIME_DIR/cue-shell/cued.pid`.
pub fn pid_path() -> PathBuf {
    runtime_dir().join("cued.pid")
}

// ── Data dir (SQLite + output logs) ──

/// `$XDG_DATA_HOME/cue-shell/` (fallback `~/.local/share/cue-shell/`).
pub fn data_dir() -> Result<PathBuf> {
    data_dir_from_env(std::env::var_os("XDG_DATA_HOME"), std::env::var_os("HOME"))
}

fn data_dir_from_env(xdg_data_home: Option<OsString>, home: Option<OsString>) -> Result<PathBuf> {
    if let Some(dir) = non_empty_env(xdg_data_home) {
        Ok(PathBuf::from(dir).join(APP_DIR))
    } else {
        Ok(home_dir_from_env(home, "XDG_DATA_HOME")?
            .join(".local/share")
            .join(APP_DIR))
    }
}

/// SQLite database path: `<data_dir>/cued.db`.
pub fn db_path() -> Result<PathBuf> {
    Ok(data_dir()?.join("cued.db"))
}

/// Output spool directory: `<data_dir>/output/`.
pub fn output_dir() -> Result<PathBuf> {
    Ok(data_dir()?.join("output"))
}

// ── State dir (logs) ──

/// `$XDG_STATE_HOME/cue-shell/` (fallback `~/.local/state/cue-shell/`).
pub fn state_dir() -> Result<PathBuf> {
    state_dir_from_env(std::env::var_os("XDG_STATE_HOME"), std::env::var_os("HOME"))
}

fn state_dir_from_env(xdg_state_home: Option<OsString>, home: Option<OsString>) -> Result<PathBuf> {
    if let Some(dir) = non_empty_env(xdg_state_home) {
        Ok(PathBuf::from(dir).join(APP_DIR))
    } else {
        Ok(home_dir_from_env(home, "XDG_STATE_HOME")?
            .join(".local/state")
            .join(APP_DIR))
    }
}

/// Log file path: `<state_dir>/cued.log`.
pub fn log_path() -> Result<PathBuf> {
    Ok(state_dir()?.join("cued.log"))
}

// ── Config dir ──

/// `$XDG_CONFIG_HOME/cue-shell/` (fallback `~/.config/cue-shell/`).
pub fn config_dir() -> Result<PathBuf> {
    config_dir_from_env(
        std::env::var_os("XDG_CONFIG_HOME"),
        std::env::var_os("HOME"),
    )
}

fn config_dir_from_env(
    xdg_config_home: Option<OsString>,
    home: Option<OsString>,
) -> Result<PathBuf> {
    if let Some(dir) = non_empty_env(xdg_config_home) {
        Ok(PathBuf::from(dir).join(APP_DIR))
    } else {
        Ok(home_dir_from_env(home, "XDG_CONFIG_HOME")?
            .join(".config")
            .join(APP_DIR))
    }
}

// ── Helpers ──

pub(crate) fn home_dir() -> Result<PathBuf> {
    home_dir_from_env(std::env::var_os("HOME"), "HOME")
}

fn home_dir_from_env(home: Option<OsString>, xdg_override: &str) -> Result<PathBuf> {
    let Some(home) = non_empty_env(home) else {
        bail!("HOME is not set; set HOME or {xdg_override} to resolve cued paths");
    };
    Ok(PathBuf::from(home))
}

fn non_empty_env(value: Option<OsString>) -> Option<OsString> {
    value.filter(|value| !value.is_empty())
}

/// Create all required directories.  Idempotent — safe to call on every startup.
pub fn ensure_dirs() -> Result<()> {
    let dirs = [
        runtime_dir(),
        data_dir()?,
        output_dir()?,
        state_dir()?,
        config_dir()?,
    ];
    for d in &dirs {
        std::fs::create_dir_all(d)
            .with_context(|| format!("failed to create directory {}", d.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn socket_path_ends_with_sock() {
        let p = socket_path();
        assert!(p.ends_with("cued.sock"), "got: {}", p.display());
    }

    #[test]
    fn pid_path_sibling_of_socket() {
        let sock = socket_path();
        let pid = pid_path();
        assert_eq!(sock.parent(), pid.parent());
    }

    #[test]
    fn db_inside_data_dir() {
        let data = data_dir_from_env(None, Some(OsString::from("/home/test"))).unwrap();
        let db = data.join("cued.db");
        assert!(
            db.starts_with(&data),
            "db={}, data={}",
            db.display(),
            data.display()
        );
    }

    #[test]
    fn xdg_overrides() {
        assert_eq!(
            runtime_dir_from_env(Some(OsString::from("/runtime")), PathBuf::from("/tmp")),
            PathBuf::from("/runtime").join(APP_DIR)
        );
        assert_eq!(
            data_dir_from_env(Some(OsString::from("/data")), None).unwrap(),
            PathBuf::from("/data").join(APP_DIR)
        );
        assert_eq!(
            state_dir_from_env(Some(OsString::from("/state")), None).unwrap(),
            PathBuf::from("/state").join(APP_DIR)
        );
        assert_eq!(
            config_dir_from_env(Some(OsString::from("/config")), None).unwrap(),
            PathBuf::from("/config").join(APP_DIR)
        );
    }

    #[test]
    fn runtime_dir_uses_temp_dir_when_xdg_runtime_is_missing_or_empty() {
        assert_eq!(
            runtime_dir_from_env(None, PathBuf::from("/tmp")),
            PathBuf::from("/tmp").join(APP_DIR)
        );
        assert_eq!(
            runtime_dir_from_env(Some(OsString::new()), PathBuf::from("/tmp")),
            PathBuf::from("/tmp").join(APP_DIR)
        );
    }

    #[test]
    fn persistent_dirs_require_home_when_xdg_override_is_missing() {
        let data_error = data_dir_from_env(None, None).expect_err("missing data base should fail");
        let state_error =
            state_dir_from_env(None, None).expect_err("missing state base should fail");
        let config_error =
            config_dir_from_env(None, None).expect_err("missing config base should fail");

        assert!(format!("{data_error:#}").contains("XDG_DATA_HOME"));
        assert!(format!("{state_error:#}").contains("XDG_STATE_HOME"));
        assert!(format!("{config_error:#}").contains("XDG_CONFIG_HOME"));
    }

    #[test]
    fn persistent_dirs_reject_empty_home() {
        let error = home_dir_from_env(Some(OsString::new()), "XDG_DATA_HOME")
            .expect_err("empty HOME should fail");

        assert!(format!("{error:#}").contains("HOME is not set"));
    }
}
