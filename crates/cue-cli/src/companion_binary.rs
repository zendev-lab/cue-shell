use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};

use anyhow::Context;

#[cfg(test)]
pub(crate) fn companion_binary_from_sources(
    program: &str,
    current_exe: Option<PathBuf>,
    argv0: Option<PathBuf>,
) -> Option<PathBuf> {
    current_exe
        .as_deref()
        .and_then(|path| companion_binary_for_path(path, program))
        .or_else(|| {
            argv0
                .as_deref()
                .and_then(|path| companion_binary_for_path(path, program))
        })
}

pub(crate) fn companion_binary_for_path(path: &Path, program: &str) -> Option<PathBuf> {
    let sibling = path.with_file_name(program);
    if is_executable_file(&sibling) {
        return Some(sibling);
    }

    if let Some(parent) = path.parent()
        && parent.file_name().is_some_and(|name| name == "deps")
        && let Some(bin_dir) = parent.parent()
    {
        let cargo_bin = bin_dir.join(program);
        if is_executable_file(&cargo_bin) {
            return Some(cargo_bin);
        }
    }

    None
}

pub(crate) fn argv0_path() -> anyhow::Result<Option<PathBuf>> {
    argv0_path_from_sources(std::env::args_os().next(), std::env::current_dir)
}

fn argv0_path_from_sources(
    argv0: Option<OsString>,
    current_dir: impl FnOnce() -> io::Result<PathBuf>,
) -> anyhow::Result<Option<PathBuf>> {
    let Some(argv0) = argv0 else {
        return Ok(None);
    };
    let path = PathBuf::from(argv0);
    if path.components().count() == 1 {
        return Ok(None);
    }

    let absolute = if path.is_absolute() {
        path
    } else {
        current_dir()
            .context("resolve relative argv[0] against current directory")?
            .join(path)
    };

    Ok(absolute.is_file().then_some(absolute))
}

#[cfg(unix)]
pub(crate) fn is_executable_file(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;

    std::fs::metadata(path)
        .map(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(not(unix))]
pub(crate) fn is_executable_file(path: &Path) -> bool {
    path.is_file()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicU64, Ordering};

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn make_temp_bin_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "cue-companion-bin-test-{name}-{}-{}",
            std::process::id(),
            TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).expect("create temp bin dir");
        dir
    }

    #[cfg(unix)]
    fn write_executable(path: &Path) {
        use std::os::unix::fs::PermissionsExt;

        std::fs::write(path, "#!/bin/sh\n").expect("write executable");
        let mut permissions = std::fs::metadata(path)
            .expect("stat executable")
            .permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(path, permissions).expect("chmod executable");
    }

    #[cfg(not(unix))]
    fn write_executable(path: &Path) {
        std::fs::write(path, "").expect("write executable");
    }

    #[test]
    fn companion_binary_uses_current_exe_sibling() {
        let dir = make_temp_bin_dir("sibling");
        let cue = dir.join("cue");
        let tui = dir.join("cue-tui");
        write_executable(&tui);

        assert_eq!(
            companion_binary_from_sources("cue-tui", Some(cue), None),
            Some(tui)
        );

        std::fs::remove_dir_all(dir).expect("remove temp bin dir");
    }

    #[test]
    fn companion_binary_falls_back_to_argv0_sibling() {
        let dir = make_temp_bin_dir("argv0");
        let cue = dir.join("cue");
        let tui = dir.join("cue-tui");
        write_executable(&tui);

        assert_eq!(
            companion_binary_from_sources("cue-tui", None, Some(cue)),
            Some(tui)
        );

        std::fs::remove_dir_all(dir).expect("remove temp bin dir");
    }

    #[test]
    fn companion_binary_uses_cargo_bin_for_deps_path() {
        let dir = make_temp_bin_dir("cargo-deps");
        let deps = dir.join("deps");
        std::fs::create_dir_all(&deps).expect("create deps dir");
        let cue = deps.join("cue-123");
        let tui = dir.join("cue-tui");
        write_executable(&tui);

        assert_eq!(
            companion_binary_from_sources("cue-tui", Some(cue), None),
            Some(tui)
        );

        std::fs::remove_dir_all(dir).expect("remove temp bin dir");
    }

    #[cfg(unix)]
    #[test]
    fn companion_binary_rejects_non_executable_sibling() {
        let dir = make_temp_bin_dir("non-executable");
        let cue = dir.join("cue");
        let tui = dir.join("cue-tui");
        std::fs::write(&tui, "#!/bin/sh\n").expect("write non-executable sibling");

        assert_eq!(
            companion_binary_from_sources("cue-tui", Some(cue), None),
            None
        );

        std::fs::remove_dir_all(dir).expect("remove temp bin dir");
    }

    #[test]
    fn argv0_path_reports_current_dir_failure_for_relative_path() {
        let error = argv0_path_from_sources(Some(OsString::from("./cue")), || {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "current directory was removed",
            ))
        })
        .expect_err("relative argv0 should not hide current_dir errors");

        let message = format!("{error:#}");
        assert!(message.contains("resolve relative argv[0]"));
        assert!(message.contains("current directory was removed"));
    }
}
