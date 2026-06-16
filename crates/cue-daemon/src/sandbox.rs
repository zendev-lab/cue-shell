#[cfg(target_os = "linux")]
use std::ffi::CString;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(target_os = "linux")]
use anyhow::Context;
use anyhow::{Result, bail};
use cue_core::command::{ModeParams, ParamValue};
#[cfg(target_os = "linux")]
use tracing::debug;
use tracing::warn;

#[cfg(target_os = "linux")]
use crate::dirs;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SandboxMode {
    Overlay,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SandboxUpper {
    Directory(PathBuf),
    Tmpfs,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SandboxConfig {
    pub mode: SandboxMode,
    pub upper: Option<SandboxUpper>,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedSandbox {
    lower_dir: PathBuf,
    mount_dir: PathBuf,
    _cleanup: Option<Arc<SandboxCleanup>>,
}

impl PreparedSandbox {
    pub fn cwd_for(&self, original_cwd: &Path) -> PathBuf {
        let canonical_cwd =
            std::fs::canonicalize(original_cwd).unwrap_or_else(|_| original_cwd.to_path_buf());
        match canonical_cwd.strip_prefix(&self.lower_dir) {
            Ok(relative) if relative.as_os_str().is_empty() => self.mount_dir.clone(),
            Ok(relative) => self.mount_dir.join(relative),
            Err(_) => original_cwd.to_path_buf(),
        }
    }
}

#[derive(Debug)]
struct SandboxCleanup {
    mount_dir: PathBuf,
    _upper_dir: PathBuf,
    work_dir: PathBuf,
    tmpfs_upper_mount: Option<PathBuf>,
    root_dir: PathBuf,
}

impl Drop for SandboxCleanup {
    fn drop(&mut self) {
        if let Err(error) = unmount(&self.mount_dir) {
            warn!(path = %self.mount_dir.display(), err = %error, "sandbox: failed to unmount overlay");
        }
        if let Some(path) = self.tmpfs_upper_mount.as_ref()
            && let Err(error) = unmount(path)
        {
            warn!(path = %path.display(), err = %error, "sandbox: failed to unmount tmpfs upperdir");
        }
        if let Err(error) = std::fs::remove_dir_all(&self.work_dir)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            warn!(path = %self.work_dir.display(), err = %error, "sandbox: failed to remove sandbox workdir");
        }
        if let Err(error) = std::fs::remove_dir_all(&self.root_dir)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            warn!(path = %self.root_dir.display(), err = %error, "sandbox: failed to remove sandbox root");
        }
    }
}

impl From<SandboxConfig> for cue_core::scope::SandboxSettings {
    fn from(value: SandboxConfig) -> Self {
        Self {
            mode: match value.mode {
                SandboxMode::Overlay => cue_core::scope::SandboxMode::Overlay,
            },
            upper: value.upper.map(|upper| match upper {
                SandboxUpper::Directory(path) => cue_core::scope::SandboxUpper::Directory(path),
                SandboxUpper::Tmpfs => cue_core::scope::SandboxUpper::Tmpfs,
            }),
        }
    }
}

impl From<&cue_core::scope::SandboxSettings> for SandboxConfig {
    fn from(value: &cue_core::scope::SandboxSettings) -> Self {
        Self {
            mode: match value.mode {
                cue_core::scope::SandboxMode::Overlay => SandboxMode::Overlay,
            },
            upper: value.upper.as_ref().map(|upper| match upper {
                cue_core::scope::SandboxUpper::Directory(path) => {
                    SandboxUpper::Directory(path.clone())
                }
                cue_core::scope::SandboxUpper::Tmpfs => SandboxUpper::Tmpfs,
            }),
        }
    }
}

impl SandboxConfig {
    pub fn from_params(params: &ModeParams) -> Result<Option<Self>, String> {
        let mode = match params.get("sandbox") {
            None => {
                if params.get("sandbox.upper").is_some() {
                    return Err("sandbox.upper requires sandbox=overlay".into());
                }
                return Ok(None);
            }
            Some(ParamValue::Str(value)) if value == "overlay" => SandboxMode::Overlay,
            Some(ParamValue::Str(value)) => {
                return Err(format!(
                    "unsupported sandbox `{value}`; supported value: overlay"
                ));
            }
            Some(ParamValue::Bool(_)) => return Err("sandbox expects a string value".into()),
        };

        let upper = match params.get("sandbox.upper") {
            None => None,
            Some(ParamValue::Str(value)) if value == "tmpfs" => Some(SandboxUpper::Tmpfs),
            Some(ParamValue::Str(value)) => Some(SandboxUpper::Directory(PathBuf::from(value))),
            Some(ParamValue::Bool(_)) => return Err("sandbox.upper expects a string value".into()),
        };

        Ok(Some(Self { mode, upper }))
    }
}

pub(crate) fn prepare(
    job_id: cue_core::JobId,
    config: &SandboxConfig,
    lower_dir: &Path,
) -> Result<PreparedSandbox> {
    match config.mode {
        SandboxMode::Overlay => prepare_overlay(job_id, config, lower_dir),
    }
}

#[cfg(target_os = "linux")]
fn prepare_overlay(
    job_id: cue_core::JobId,
    config: &SandboxConfig,
    lower_dir: &Path,
) -> Result<PreparedSandbox> {
    let lower_dir = std::fs::canonicalize(lower_dir)
        .with_context(|| format!("canonicalize sandbox lowerdir {}", lower_dir.display()))?;
    if !lower_dir.is_dir() {
        bail!(
            "sandbox lowerdir {} is not a directory",
            lower_dir.display()
        );
    }

    let root_dir = sandbox_root(job_id)?;
    let mount_dir = root_dir.join("merged");
    let default_upper_dir = root_dir.join("upper");
    let default_work_dir = root_dir.join("work");
    let tmpfs_dir = root_dir.join("tmpfs");
    std::fs::create_dir_all(&mount_dir)
        .with_context(|| format!("create sandbox mount dir {}", mount_dir.display()))?;

    let (upper_dir, work_dir, tmpfs_upper_mount) = match config.upper.as_ref() {
        Some(SandboxUpper::Directory(path)) => {
            std::fs::create_dir_all(path)
                .with_context(|| format!("create sandbox upperdir {}", path.display()))?;
            let upper_dir = std::fs::canonicalize(path)
                .with_context(|| format!("canonicalize sandbox upperdir {}", path.display()))?;
            let work_dir = upper_dir
                .parent()
                .unwrap_or_else(|| Path::new("/tmp"))
                .join(format!(".cue-shell-work-{job_id}"));
            if let Err(error) = std::fs::remove_dir_all(&work_dir)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                return Err(error).with_context(|| {
                    format!("remove stale sandbox workdir {}", work_dir.display())
                });
            }
            (upper_dir, work_dir, None)
        }
        Some(SandboxUpper::Tmpfs) => {
            std::fs::create_dir_all(&tmpfs_dir)
                .with_context(|| format!("create sandbox tmpfs dir {}", tmpfs_dir.display()))?;
            mount_tmpfs(&tmpfs_dir)
                .with_context(|| format!("mount tmpfs sandbox dir {}", tmpfs_dir.display()))?;
            let upper_dir = tmpfs_dir.join("upper");
            let work_dir = tmpfs_dir.join("work");
            std::fs::create_dir_all(&upper_dir).with_context(|| {
                format!("create sandbox tmpfs upperdir {}", upper_dir.display())
            })?;
            std::fs::create_dir_all(&work_dir)
                .with_context(|| format!("create sandbox tmpfs workdir {}", work_dir.display()))?;
            (upper_dir, work_dir, Some(tmpfs_dir))
        }
        None => {
            std::fs::create_dir_all(&default_upper_dir).with_context(|| {
                format!("create sandbox upperdir {}", default_upper_dir.display())
            })?;
            (default_upper_dir, default_work_dir, None)
        }
    };
    std::fs::create_dir_all(&work_dir)
        .with_context(|| format!("create sandbox work dir {}", work_dir.display()))?;

    if let Err(error) =
        mount_overlay(&lower_dir, &upper_dir, &work_dir, &mount_dir).with_context(|| {
            format!(
                "mount overlay sandbox lowerdir={} upperdir={} workdir={} merged={}",
                lower_dir.display(),
                upper_dir.display(),
                work_dir.display(),
                mount_dir.display()
            )
        })
    {
        cleanup_failed_mount(&root_dir, &work_dir, tmpfs_upper_mount.as_deref());
        return Err(error);
    }

    debug!(
        %job_id,
        lower = %lower_dir.display(),
        upper = %upper_dir.display(),
        work = %work_dir.display(),
        merged = %mount_dir.display(),
        tmpfs_upper = tmpfs_upper_mount.is_some(),
        "sandbox: overlay prepared"
    );

    Ok(PreparedSandbox {
        lower_dir,
        mount_dir: mount_dir.clone(),
        _cleanup: Some(Arc::new(SandboxCleanup {
            mount_dir,
            _upper_dir: upper_dir,
            work_dir,
            tmpfs_upper_mount,
            root_dir,
        })),
    })
}

#[cfg(not(target_os = "linux"))]
fn prepare_overlay(
    _job_id: cue_core::JobId,
    _config: &SandboxConfig,
    _lower_dir: &Path,
) -> Result<PreparedSandbox> {
    bail!("overlay sandbox is only supported on Linux")
}

#[cfg(target_os = "linux")]
fn sandbox_root(job_id: cue_core::JobId) -> Result<PathBuf> {
    let dir = dirs::runtime_sandbox_dir().join(job_id.to_string());
    cleanup_stale_sandbox_root(&dir)?;
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("create sandbox dir {}", dir.display()))?;
    Ok(dir)
}

#[cfg(target_os = "linux")]
fn cleanup_stale_sandbox_root(dir: &Path) -> Result<()> {
    cleanup_stale_sandbox_root_with(dir, unmount)
        .with_context(|| format!("remove stale sandbox dir {}", dir.display()))
}

#[cfg(test)]
fn cleanup_stale_sandbox_root_for_test(dir: &Path) -> Result<()> {
    cleanup_stale_sandbox_root_with(dir, |_| Ok(()))
}

#[cfg(any(target_os = "linux", test))]
fn cleanup_stale_sandbox_root_with(
    dir: &Path,
    mut unmount_fn: impl FnMut(&Path) -> Result<()>,
) -> Result<()> {
    let merged = dir.join("merged");
    let tmpfs = dir.join("tmpfs");
    if merged.exists() {
        let _ = unmount_fn(&merged);
    }
    if tmpfs.exists() {
        let _ = unmount_fn(&tmpfs);
    }
    if let Err(error) = std::fs::remove_dir_all(dir)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        return Err(error.into());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn mount_data_path(path: &Path, label: &str) -> Result<String> {
    let value = path.to_string_lossy();
    if value.contains(',') || value.contains(':') || value.contains('\n') {
        bail!(
            "sandbox {label} path contains an unsupported character for overlay mount data: {}",
            path.display()
        );
    }
    Ok(value.into_owned())
}

#[cfg(target_os = "linux")]
fn mount_overlay(
    lower_dir: &Path,
    upper_dir: &Path,
    work_dir: &Path,
    mount_dir: &Path,
) -> Result<()> {
    let lower_dir = mount_data_path(lower_dir, "lowerdir")?;
    let upper_dir = mount_data_path(upper_dir, "upperdir")?;
    let work_dir = mount_data_path(work_dir, "workdir")?;
    let data = CString::new(format!(
        "lowerdir={lower_dir},upperdir={upper_dir},workdir={work_dir}"
    ))?;
    mount(
        Some("overlay"),
        mount_dir,
        Some("overlay"),
        0,
        Some(data.as_c_str()),
    )
}

#[cfg(target_os = "linux")]
fn mount_tmpfs(target: &Path) -> Result<()> {
    mount(Some("tmpfs"), target, Some("tmpfs"), 0, Some(c"mode=700"))
}

#[cfg(target_os = "linux")]
fn mount(
    source: Option<&str>,
    target: &Path,
    fstype: Option<&str>,
    flags: libc::c_ulong,
    data: Option<&std::ffi::CStr>,
) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;

    let source = source.map(CString::new).transpose()?;
    let target = CString::new(target.as_os_str().as_bytes())?;
    let fstype = fstype.map(CString::new).transpose()?;
    let rc = unsafe {
        libc::mount(
            source
                .as_ref()
                .map_or(std::ptr::null(), |value| value.as_ptr()),
            target.as_ptr(),
            fstype
                .as_ref()
                .map_or(std::ptr::null(), |value| value.as_ptr()),
            flags,
            data.map_or(std::ptr::null(), |value| value.as_ptr().cast()),
        )
    };
    if rc == -1 {
        return Err(std::io::Error::last_os_error()).context("mount syscall failed");
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn cleanup_failed_mount(root_dir: &Path, work_dir: &Path, tmpfs_upper_mount: Option<&Path>) {
    if let Some(path) = tmpfs_upper_mount
        && let Err(error) = unmount(path)
    {
        warn!(path = %path.display(), err = %error, "sandbox: failed to clean up tmpfs upperdir after mount error");
    }
    if let Err(error) = std::fs::remove_dir_all(work_dir)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        warn!(path = %work_dir.display(), err = %error, "sandbox: failed to clean up workdir after mount error");
    }
    if let Err(error) = std::fs::remove_dir_all(root_dir)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        warn!(path = %root_dir.display(), err = %error, "sandbox: failed to clean up root after mount error");
    }
}

fn unmount(path: &Path) -> Result<()> {
    unmount_impl(path)
}

#[cfg(target_os = "linux")]
fn unmount_impl(path: &Path) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;

    let target = CString::new(path.as_os_str().as_bytes())?;
    let rc = unsafe { libc::umount2(target.as_ptr(), libc::MNT_DETACH) };
    if rc == -1 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("umount2 {}", path.display()));
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn unmount_impl(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "cue-sandbox-{name}-{}-{}",
            std::process::id(),
            TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn parses_overlay_sandbox_params() {
        let mut params = ModeParams {
            params: BTreeMap::new(),
        };
        params.insert("sandbox", ParamValue::Str("overlay".into()));
        params.insert("sandbox.upper", ParamValue::Str("tmpfs".into()));

        let config = SandboxConfig::from_params(&params)
            .expect("parse sandbox params")
            .expect("sandbox enabled");
        assert_eq!(config.mode, SandboxMode::Overlay);
        assert_eq!(config.upper, Some(SandboxUpper::Tmpfs));
    }

    #[test]
    fn rejects_unknown_sandbox_mode() {
        let mut params = ModeParams {
            params: BTreeMap::new(),
        };
        params.insert("sandbox", ParamValue::Str("docker".into()));

        let error = SandboxConfig::from_params(&params).expect_err("unknown mode should fail");
        assert!(error.contains("unsupported sandbox"));
    }

    #[test]
    fn rejects_sandbox_upper_without_overlay_mode() {
        let mut params = ModeParams {
            params: BTreeMap::new(),
        };
        params.insert("sandbox.upper", ParamValue::Str("tmpfs".into()));

        let error = SandboxConfig::from_params(&params).expect_err("orphan upper should fail");
        assert!(error.contains("requires sandbox=overlay"));
    }

    #[test]
    fn rewrites_cwd_relative_to_overlay_lowerdir() {
        let prepared = PreparedSandbox {
            lower_dir: PathBuf::from("/repo"),
            mount_dir: PathBuf::from("/merged"),
            _cleanup: None,
        };

        assert_eq!(
            prepared.cwd_for(Path::new("/repo")),
            PathBuf::from("/merged")
        );
        assert_eq!(
            prepared.cwd_for(Path::new("/repo/subdir")),
            PathBuf::from("/merged/subdir")
        );
        assert_eq!(
            prepared.cwd_for(Path::new("/other")),
            PathBuf::from("/other")
        );
    }

    #[test]
    fn rewrites_symlink_cwd_via_canonical_lowerdir() {
        let temp = temp_path("symlink");
        let _ = std::fs::remove_dir_all(&temp);
        let lower = temp.join("real");
        let child = lower.join("child");
        std::fs::create_dir_all(&child).expect("create lower child");
        let prepared = PreparedSandbox {
            lower_dir: std::fs::canonicalize(&lower).expect("canonical lower"),
            mount_dir: temp.join("merged"),
            _cleanup: None,
        };

        assert_eq!(prepared.cwd_for(&child), temp.join("merged/child"));

        std::fs::remove_dir_all(&temp).expect("remove temp");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn rejects_overlay_mount_data_paths_with_reserved_separators() {
        let error = mount_data_path(Path::new("/tmp/cue,lower"), "lowerdir")
            .expect_err("comma should fail");
        assert!(error.to_string().contains("unsupported character"));

        let error = mount_data_path(Path::new("/tmp/cue:lower"), "lowerdir")
            .expect_err("colon should fail");
        assert!(error.to_string().contains("unsupported character"));
    }

    #[test]
    fn cleanup_stale_sandbox_root_removes_plain_stale_dirs() {
        let root = temp_path("stale-root");
        std::fs::create_dir_all(root.join("merged")).expect("create stale merged");
        std::fs::write(root.join("merged/stale.txt"), "stale").expect("write stale file");

        cleanup_stale_sandbox_root_for_test(&root).expect("cleanup stale root");

        assert!(!root.exists());
    }

    #[test]
    fn cleanup_stale_sandbox_root_unmounts_known_mountpoints_before_removal() {
        let root = temp_path("stale-mounts");
        std::fs::create_dir_all(root.join("merged")).expect("create stale merged");
        std::fs::create_dir_all(root.join("tmpfs")).expect("create stale tmpfs");
        let mut unmounted = Vec::new();

        cleanup_stale_sandbox_root_with(&root, |path| {
            unmounted.push(path.file_name().expect("mountpoint name").to_owned());
            Ok(())
        })
        .expect("cleanup stale root");

        assert_eq!(unmounted, vec!["merged", "tmpfs"]);
        assert!(!root.exists());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn overlay_smoke_is_supported_or_reports_mount_permission() {
        let lower = temp_path("lower");
        std::fs::create_dir_all(&lower).expect("create lower");
        std::fs::write(lower.join("kept.txt"), "lower").expect("write lower file");
        let config = SandboxConfig {
            mode: SandboxMode::Overlay,
            upper: None,
        };

        match prepare(cue_core::JobId(424242), &config, &lower) {
            Ok(prepared) => {
                let merged = prepared.cwd_for(&lower);
                assert!(merged.join("kept.txt").exists());
                std::fs::write(merged.join("created.txt"), "overlay").expect("write overlay file");
                assert!(!lower.join("created.txt").exists());
            }
            Err(error) => {
                let message = error.to_string();
                assert!(
                    message.contains("mount overlay sandbox")
                        || message.contains("Operation not permitted")
                        || message.contains("permission denied"),
                    "unexpected overlay smoke error: {message}"
                );
            }
        }

        let _ = std::fs::remove_dir_all(&lower);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn overlay_tmpfs_upper_smoke_cleans_up_after_drop_or_reports_mount_permission() {
        let lower = temp_path("tmpfs-lower");
        std::fs::create_dir_all(&lower).expect("create lower");
        let config = SandboxConfig {
            mode: SandboxMode::Overlay,
            upper: Some(SandboxUpper::Tmpfs),
        };

        match prepare(cue_core::JobId(424243), &config, &lower) {
            Ok(prepared) => {
                let merged = prepared.cwd_for(&lower);
                std::fs::write(merged.join("tmpfs-created.txt"), "overlay")
                    .expect("write tmpfs overlay file");
                let root_dir =
                    dirs::runtime_sandbox_dir().join(cue_core::JobId(424243).to_string());
                let tmpfs_dir = root_dir.join("tmpfs");
                assert!(tmpfs_dir.exists());
                drop(prepared);
                assert!(
                    !tmpfs_dir.exists(),
                    "tmpfs upper mount directory should be removed after sandbox drop"
                );
            }
            Err(error) => {
                let message = error.to_string();
                assert!(
                    message.contains("mount tmpfs sandbox dir")
                        || message.contains("mount overlay sandbox")
                        || message.contains("Operation not permitted")
                        || message.contains("permission denied"),
                    "unexpected tmpfs overlay smoke error: {message}"
                );
            }
        }

        let _ = std::fs::remove_dir_all(&lower);
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn overlay_prepare_reports_not_supported_on_non_linux() {
        let config = SandboxConfig {
            mode: SandboxMode::Overlay,
            upper: None,
        };
        let error = prepare(cue_core::JobId(424242), &config, Path::new("/tmp"))
            .expect_err("non-linux overlay should be unsupported");
        assert!(error.to_string().contains("only supported on Linux"));
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn overlay_tmpfs_upper_reports_not_supported_on_non_linux() {
        let config = SandboxConfig {
            mode: SandboxMode::Overlay,
            upper: Some(SandboxUpper::Tmpfs),
        };
        let error = prepare(cue_core::JobId(424243), &config, Path::new("/tmp"))
            .expect_err("non-linux tmpfs overlay should be unsupported");
        assert!(error.to_string().contains("only supported on Linux"));
    }
}
