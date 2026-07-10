use std::ffi::OsString;
use std::fs::{OpenOptions, Permissions};
use std::io::Write as _;
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

const APP_DIR: &str = "cue-shell";
const HISTORY_FILE: &str = "input-history.json";
const PRIVATE_DIR_MODE: u32 = 0o700;
const PRIVATE_FILE_MODE: u32 = 0o600;

fn home_dir_from_env(home: Option<OsString>) -> Result<PathBuf> {
    let Some(home) = non_empty_env(home) else {
        bail!("HOME is not set; set HOME or XDG_DATA_HOME to resolve cue-tui history path");
    };
    Ok(PathBuf::from(home))
}

fn data_dir() -> Result<PathBuf> {
    data_dir_from_env(std::env::var_os("XDG_DATA_HOME"), std::env::var_os("HOME"))
}

fn data_dir_from_env(xdg_data_home: Option<OsString>, home: Option<OsString>) -> Result<PathBuf> {
    if let Some(dir) = non_empty_env(xdg_data_home) {
        Ok(PathBuf::from(dir).join(APP_DIR))
    } else {
        Ok(home_dir_from_env(home)?.join(".local/share").join(APP_DIR))
    }
}

fn non_empty_env(value: Option<OsString>) -> Option<OsString> {
    value.filter(|value| !value.is_empty())
}

pub(crate) fn history_path() -> Result<PathBuf> {
    Ok(data_dir()?.join(HISTORY_FILE))
}

fn load_history_from(path: &Path) -> Result<Vec<String>> {
    prepare_history_path(path)?;
    match std::fs::read_to_string(path) {
        Ok(content) => serde_json::from_str(&content)
            .with_context(|| format!("parse history file {}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(error).with_context(|| format!("read history file {}", path.display())),
    }
}

fn save_history_to(path: &Path, history: &[String]) -> Result<()> {
    prepare_history_path(path)?;
    let content = serde_json::to_string(history).context("serialize history")?;
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(PRIVATE_FILE_MODE)
        .open(path)
        .with_context(|| format!("open history file {}", path.display()))?;
    file.set_permissions(Permissions::from_mode(PRIVATE_FILE_MODE))
        .with_context(|| format!("secure history file {}", path.display()))?;
    file.write_all(content.as_bytes())
        .with_context(|| format!("write history file {}", path.display()))
}

fn prepare_history_path(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create history directory {}", parent.display()))?;
        std::fs::set_permissions(parent, Permissions::from_mode(PRIVATE_DIR_MODE))
            .with_context(|| format!("secure history directory {}", parent.display()))?;
    }
    match std::fs::set_permissions(path, Permissions::from_mode(PRIVATE_FILE_MODE)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("secure history file {}", path.display())),
    }
}

pub(crate) fn load_history() -> Result<Vec<String>> {
    load_history_from(&history_path()?)
}

pub(crate) fn save_history(history: &[String]) -> Result<()> {
    save_history_to(&history_path()?, history)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "cue-tui-history-test-{}-{}",
            std::process::id(),
            name
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        root.join(HISTORY_FILE)
    }

    #[test]
    fn missing_history_file_loads_as_empty() {
        let path = temp_path("missing");
        assert!(load_history_from(&path).unwrap().is_empty());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn history_roundtrip_preserves_multiline_entries() {
        let path = temp_path("roundtrip");
        let history = vec!["ls".into(), "echo hi\npwd".into()];
        save_history_to(&path, &history).unwrap();
        assert_eq!(load_history_from(&path).unwrap(), history);
        assert_eq!(
            std::fs::metadata(path.parent().expect("history parent"))
                .expect("stat history parent")
                .permissions()
                .mode()
                & 0o777,
            PRIVATE_DIR_MODE
        );
        assert_eq!(
            std::fs::metadata(&path)
                .expect("stat history")
                .permissions()
                .mode()
                & 0o777,
            PRIVATE_FILE_MODE
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn loading_history_migrates_wide_permissions() {
        let path = temp_path("permissions");
        std::fs::write(&path, "[\"secret command\"]").expect("write history");
        std::fs::set_permissions(
            path.parent().expect("history parent"),
            Permissions::from_mode(0o755),
        )
        .expect("set wide directory permissions");
        std::fs::set_permissions(&path, Permissions::from_mode(0o644))
            .expect("set wide history permissions");

        assert_eq!(
            load_history_from(&path).expect("load history"),
            ["secret command"]
        );
        assert_eq!(
            std::fs::metadata(path.parent().expect("history parent"))
                .expect("stat history parent")
                .permissions()
                .mode()
                & 0o777,
            PRIVATE_DIR_MODE
        );
        assert_eq!(
            std::fs::metadata(&path)
                .expect("stat history")
                .permissions()
                .mode()
                & 0o777,
            PRIVATE_FILE_MODE
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn history_path_requires_home_when_xdg_data_home_is_missing() {
        let error = data_dir_from_env(None, None).expect_err("missing HOME and XDG should fail");

        assert!(format!("{error:#}").contains("HOME is not set"));
    }

    #[test]
    fn history_path_uses_xdg_data_home_without_home() {
        assert_eq!(
            data_dir_from_env(Some(OsString::from("/xdg-data")), None).unwrap(),
            PathBuf::from("/xdg-data").join(APP_DIR)
        );
    }

    #[test]
    fn history_path_rejects_empty_home() {
        let error = home_dir_from_env(Some(OsString::new())).expect_err("empty HOME should fail");

        assert!(format!("{error:#}").contains("HOME is not set"));
    }
}
