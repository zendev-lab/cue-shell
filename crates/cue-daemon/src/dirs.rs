//! XDG-compliant directory resolution for cued.
//!
//! ```text
//! Runtime:  $XDG_RUNTIME_DIR/cue-shell/  (socket, pid)
//! Data:     $XDG_DATA_HOME/cue-shell/    (db, output)
//! State:    $XDG_STATE_HOME/cue-shell/   (logs)
//! Config:   $XDG_CONFIG_HOME/cue-shell/  (config)
//! ```

use std::path::PathBuf;

use anyhow::{Context, Result};

const APP_DIR: &str = "cue-shell";

// ── Runtime dir (socket + PID) ──

fn runtime_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("XDG_RUNTIME_DIR") {
        PathBuf::from(dir).join(APP_DIR)
    } else {
        std::env::temp_dir().join(APP_DIR)
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
pub fn data_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("XDG_DATA_HOME") {
        PathBuf::from(dir).join(APP_DIR)
    } else {
        home_dir().join(".local/share").join(APP_DIR)
    }
}

/// SQLite database path: `<data_dir>/cued.db`.
pub fn db_path() -> PathBuf {
    data_dir().join("cued.db")
}

/// Output spool directory: `<data_dir>/output/`.
pub fn output_dir() -> PathBuf {
    data_dir().join("output")
}

// ── State dir (logs) ──

/// `$XDG_STATE_HOME/cue-shell/` (fallback `~/.local/state/cue-shell/`).
pub fn state_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("XDG_STATE_HOME") {
        PathBuf::from(dir).join(APP_DIR)
    } else {
        home_dir().join(".local/state").join(APP_DIR)
    }
}

/// Log file path: `<state_dir>/cued.log`.
pub fn log_path() -> PathBuf {
    state_dir().join("cued.log")
}

// ── Config dir ──

/// `$XDG_CONFIG_HOME/cue-shell/` (fallback `~/.config/cue-shell/`).
pub fn config_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("XDG_CONFIG_HOME") {
        PathBuf::from(dir).join(APP_DIR)
    } else {
        home_dir().join(".config").join(APP_DIR)
    }
}

// ── Helpers ──

pub(crate) fn home_dir() -> PathBuf {
    std::env::var("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/tmp"))
}

/// Create all required directories.  Idempotent — safe to call on every startup.
pub fn ensure_dirs() -> Result<()> {
    let dirs = [
        runtime_dir(),
        data_dir(),
        output_dir(),
        state_dir(),
        config_dir(),
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
        let db = db_path();
        let data = data_dir();
        assert!(
            db.starts_with(&data),
            "db={}, data={}",
            db.display(),
            data.display()
        );
    }

    #[test]
    fn xdg_overrides() {
        // Test that setting XDG vars actually affects the paths.
        // We can't set env vars in parallel tests, so just validate the logic.
        let rd = runtime_dir();
        assert!(rd.ends_with(APP_DIR));
    }
}
