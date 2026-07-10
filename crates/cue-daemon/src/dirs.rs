//! XDG-compliant directory resolution for cued.
//!
//! ```text
//! Runtime:  $XDG_RUNTIME_DIR/cue-shell/  (socket, pid)
//! Data:     $XDG_DATA_HOME/cue-shell/    (db, output)
//! State:    $XDG_STATE_HOME/cue-shell/   (logs)
//! Config:   $XDG_CONFIG_HOME/cue-shell/  (config)
//! ```

use std::ffi::OsString;
use std::fs::{File, OpenOptions, Permissions};
use std::io::{self, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

const APP_DIR: &str = "cue-shell";
const PRIVATE_DIR_MODE: u32 = 0o700;
const PRIVATE_FILE_MODE: u32 = 0o600;

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

/// PID marker paired with a daemon socket.
///
/// Custom sockets must never share the default daemon's ownership markers.
pub(crate) fn pid_path_for_socket(socket_path: &Path) -> PathBuf {
    socket_sidecar_path(socket_path, ".cued.pid")
}

/// Advisory single-instance lock paired with a daemon socket.
pub(crate) fn lock_path_for_socket(socket_path: &Path) -> PathBuf {
    socket_sidecar_path(socket_path, ".cued.lock")
}

fn socket_sidecar_path(socket_path: &Path, suffix: &str) -> PathBuf {
    let mut path = socket_path.as_os_str().to_os_string();
    path.push(suffix);
    PathBuf::from(path)
}

/// Runtime-owned sandbox work directory.
pub(crate) fn runtime_sandbox_dir() -> PathBuf {
    runtime_dir().join("sandbox")
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
    let layout = DirectoryLayout {
        runtime: runtime_dir(),
        sandbox: runtime_sandbox_dir(),
        data: data_dir()?,
        output: output_dir()?,
        state: state_dir()?,
        config: config_dir()?,
    };
    ensure_layout(&layout)
}

struct DirectoryLayout {
    runtime: PathBuf,
    sandbox: PathBuf,
    data: PathBuf,
    output: PathBuf,
    state: PathBuf,
    config: PathBuf,
}

fn ensure_layout(layout: &DirectoryLayout) -> Result<()> {
    for dir in [
        &layout.runtime,
        &layout.sandbox,
        &layout.data,
        &layout.output,
        &layout.state,
        &layout.config,
    ] {
        ensure_private_dir(dir).with_context(|| format!("secure directory {}", dir.display()))?;
    }

    for file in [
        pid_path_for_socket(&layout.runtime.join("cued.sock")),
        lock_path_for_socket(&layout.runtime.join("cued.sock")),
        layout.data.join("cued.db"),
        database_sidecar_path(&layout.data.join("cued.db"), "-wal"),
        database_sidecar_path(&layout.data.join("cued.db"), "-shm"),
        layout.data.join("input-history.json"),
        layout.state.join("cued.log"),
        layout.config.join("daemon.toml"),
    ] {
        secure_private_file(&file).with_context(|| format!("secure file {}", file.display()))?;
    }

    for entry in std::fs::read_dir(&layout.output)
        .with_context(|| format!("read output directory {}", layout.output.display()))?
    {
        let entry = entry.with_context(|| {
            format!("read output directory entry in {}", layout.output.display())
        })?;
        if entry
            .file_type()
            .with_context(|| format!("inspect output path {}", entry.path().display()))?
            .is_file()
        {
            secure_private_file(&entry.path())
                .with_context(|| format!("secure output file {}", entry.path().display()))?;
        }
    }
    Ok(())
}

pub(crate) fn ensure_private_dir(path: &Path) -> io::Result<()> {
    std::fs::create_dir_all(path)?;
    reject_symlink(path)?;
    std::fs::set_permissions(path, Permissions::from_mode(PRIVATE_DIR_MODE))
}

pub(crate) fn secure_private_file(path: &Path) -> io::Result<()> {
    match reject_symlink(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    }
    match std::fs::set_permissions(path, Permissions::from_mode(PRIVATE_FILE_MODE)) {
        Ok(()) => Ok(()),
        Err(error) => Err(error),
    }
}

fn reject_symlink(path: &Path) -> io::Result<()> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("refusing to use symlinked private path {}", path.display()),
        ));
    }
    Ok(())
}

fn secure_private_file_handle(file: &File) -> io::Result<()> {
    file.set_permissions(Permissions::from_mode(PRIVATE_FILE_MODE))
}

pub(crate) fn ensure_private_file(path: &Path) -> io::Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .mode(PRIVATE_FILE_MODE)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    secure_private_file_handle(&file)
}

pub(crate) fn create_private_file(path: &Path) -> io::Result<Option<File>> {
    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(PRIVATE_FILE_MODE)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
    {
        Ok(file) => {
            secure_private_file_handle(&file)?;
            Ok(Some(file))
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            secure_private_file(path)?;
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

pub(crate) fn write_private_file(path: &Path, contents: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(PRIVATE_FILE_MODE)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    secure_private_file_handle(&file)?;
    file.write_all(contents)
}

pub(crate) fn open_private_read_write(path: &Path) -> io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(PRIVATE_FILE_MODE)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    secure_private_file_handle(&file)?;
    Ok(file)
}

pub(crate) fn open_private_append(path: &Path) -> io::Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(PRIVATE_FILE_MODE)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    secure_private_file_handle(&file)?;
    Ok(file)
}

pub(crate) fn secure_database_files(path: &Path) -> io::Result<()> {
    if path == Path::new(":memory:") {
        return Ok(());
    }
    for path in [
        path.to_path_buf(),
        database_sidecar_path(path, "-wal"),
        database_sidecar_path(path, "-shm"),
    ] {
        secure_private_file(&path)?;
    }
    Ok(())
}

pub(crate) fn database_sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut sidecar = path.as_os_str().to_os_string();
    sidecar.push(suffix);
    PathBuf::from(sidecar)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "cue-daemon-dirs-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("create temp root");
        root
    }

    fn mode(path: &Path) -> u32 {
        std::fs::metadata(path)
            .expect("stat path")
            .permissions()
            .mode()
            & 0o777
    }

    #[test]
    fn socket_path_ends_with_sock() {
        let p = socket_path();
        assert!(p.ends_with("cued.sock"), "got: {}", p.display());
    }

    #[test]
    fn pid_path_sibling_of_socket() {
        let sock = socket_path();
        let pid = pid_path_for_socket(&sock);
        assert_eq!(sock.parent(), pid.parent());
        assert_eq!(pid.file_name().unwrap(), "cued.sock.cued.pid");
    }

    #[test]
    fn custom_socket_gets_isolated_pid_and_lock_paths() {
        let socket = Path::new("/tmp/cue-tests/custom-worker.sock");

        assert_eq!(
            pid_path_for_socket(socket),
            PathBuf::from("/tmp/cue-tests/custom-worker.sock.cued.pid")
        );
        assert_eq!(
            lock_path_for_socket(socket),
            PathBuf::from("/tmp/cue-tests/custom-worker.sock.cued.lock")
        );
        assert_ne!(
            pid_path_for_socket(socket),
            pid_path_for_socket(&socket_path())
        );
    }

    #[test]
    fn marker_paths_preserve_the_full_socket_filename() {
        let first = Path::new("/tmp/cue-tests/foo.sock");
        let second = Path::new("/tmp/cue-tests/foo.other");

        assert_ne!(pid_path_for_socket(first), pid_path_for_socket(second));
        assert_ne!(lock_path_for_socket(first), lock_path_for_socket(second));
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

    #[test]
    fn ensure_layout_creates_private_directories_and_migrates_existing_files() {
        let root = temp_dir("layout");
        let layout = DirectoryLayout {
            runtime: root.join("runtime"),
            sandbox: root.join("runtime/sandbox"),
            data: root.join("data"),
            output: root.join("data/output"),
            state: root.join("state"),
            config: root.join("config"),
        };
        for dir in [
            &layout.runtime,
            &layout.sandbox,
            &layout.data,
            &layout.output,
            &layout.state,
            &layout.config,
        ] {
            std::fs::create_dir_all(dir).expect("create wide directory");
            std::fs::set_permissions(dir, Permissions::from_mode(0o755))
                .expect("set wide directory mode");
        }
        let db = layout.data.join("cued.db");
        let files = [
            layout.runtime.join("cued.pid"),
            db.clone(),
            database_sidecar_path(&db, "-wal"),
            database_sidecar_path(&db, "-shm"),
            layout.data.join("input-history.json"),
            layout.state.join("cued.log"),
            layout.config.join("daemon.toml"),
            layout.output.join("J1.log"),
            layout.output.join("J1.stderr"),
        ];
        for file in &files {
            std::fs::write(file, b"private data").expect("create wide file");
            std::fs::set_permissions(file, Permissions::from_mode(0o644))
                .expect("set wide file mode");
        }

        ensure_layout(&layout).expect("secure layout");

        for dir in [
            &layout.runtime,
            &layout.sandbox,
            &layout.data,
            &layout.output,
            &layout.state,
            &layout.config,
        ] {
            assert_eq!(mode(dir), PRIVATE_DIR_MODE, "{}", dir.display());
        }
        for file in &files {
            assert_eq!(mode(file), PRIVATE_FILE_MODE, "{}", file.display());
        }
        std::fs::remove_dir_all(root).expect("remove temp root");
    }

    #[test]
    fn private_file_helpers_secure_new_and_existing_files() {
        let root = temp_dir("files");
        let created = root.join("created");
        let existing = root.join("existing");
        std::fs::write(&existing, b"old").expect("create existing file");
        std::fs::set_permissions(&existing, Permissions::from_mode(0o644))
            .expect("set wide file mode");

        write_private_file(&created, b"new").expect("write private file");
        let mut appended = open_private_append(&existing).expect("open private append");
        appended.write_all(b" data").expect("append data");

        assert_eq!(mode(&created), PRIVATE_FILE_MODE);
        assert_eq!(mode(&existing), PRIVATE_FILE_MODE);
        std::fs::remove_dir_all(root).expect("remove temp root");
    }

    #[test]
    fn private_path_helpers_reject_symlinks() {
        use std::os::unix::fs::symlink;

        let root = temp_dir("symlinks");
        let target_dir = root.join("target-dir");
        let linked_dir = root.join("linked-dir");
        std::fs::create_dir(&target_dir).expect("create target directory");
        std::fs::set_permissions(&target_dir, Permissions::from_mode(0o755))
            .expect("set target directory mode");
        symlink(&target_dir, &linked_dir).expect("link directory");

        let error = ensure_private_dir(&linked_dir).expect_err("symlinked directory must fail");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            mode(&target_dir),
            0o755,
            "target mode must remain unchanged"
        );

        let target_file = root.join("target-file");
        let linked_file = root.join("linked-file");
        std::fs::write(&target_file, b"unchanged").expect("create target file");
        std::fs::set_permissions(&target_file, Permissions::from_mode(0o644))
            .expect("set target file mode");
        symlink(&target_file, &linked_file).expect("link file");

        let error = write_private_file(&linked_file, b"replaced")
            .expect_err("symlinked private file must fail");
        assert_eq!(error.raw_os_error(), Some(libc::ELOOP));
        assert_eq!(std::fs::read(&target_file).unwrap(), b"unchanged");
        assert_eq!(
            mode(&target_file),
            0o644,
            "target mode must remain unchanged"
        );

        std::fs::remove_dir_all(root).expect("remove temp root");
    }
}
