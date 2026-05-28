//! Self-update via GitHub Releases.
//!
//! Fetches the latest release from `github.com/zrr1999/cue-shell`, finds a
//! binary asset matching the current OS and architecture, downloads it with
//! `curl`, extracts it (if a tarball), and atomically replaces the running
//! executable.  If the service manager is installed, it is restarted
//! automatically.
//!
//! Asset naming convention expected in GitHub Releases:
//! - `cued-{version}-{os}-{arch}.tar.gz`  (preferred)
//! - `cued-{os}-{arch}.tar.gz`            (fallback without version prefix)
//! - `cued-{version}-{os}-{arch}`         (raw binary)
//! - `cued-{os}-{arch}`                   (raw binary, no version)
//!
//! where `{os}` is `macos` or `linux` and `{arch}` is `aarch64` or `x86_64`.
//! The tarball must contain a top-level (or one-level-deep) file named `cued`.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::command_util::CommandSpec;

const REPO: &str = "zrr1999/cue-shell";
const API_URL: &str = "https://api.github.com/repos/zrr1999/cue-shell/releases/latest";

#[derive(Deserialize)]
struct GithubRelease {
    tag_name: String,
    assets: Vec<GithubAsset>,
}

#[derive(Deserialize)]
struct GithubAsset {
    name: String,
    browser_download_url: String,
}

// ── Public entry point ───────────────────────────────────────────────────────

pub fn run_upgrade() -> Result<()> {
    let current = env!("CARGO_PKG_VERSION");
    let os = platform_os();
    let arch = platform_arch();

    if os == "unknown" || arch == "unknown" {
        bail!("upgrade is not supported on this platform (os={os}, arch={arch})");
    }

    println!("cued upgrade: current version v{current}");
    println!("cued upgrade: fetching latest release from github.com/{REPO}…");

    let json = curl_get(API_URL).context("fetch GitHub releases API")?;
    let release: GithubRelease =
        serde_json::from_str(&json).context("parse GitHub release JSON")?;

    let tag = &release.tag_name;
    let latest = tag.trim_start_matches('v');
    println!("cued upgrade: latest release {tag}");

    if latest == current {
        println!("cued upgrade: already up to date (v{current})");
        return Ok(());
    }

    // Try several naming conventions in order of preference.
    let candidates = [
        format!("cued-{latest}-{os}-{arch}.tar.gz"),
        format!("cued-{os}-{arch}.tar.gz"),
        format!("cued-{latest}-{os}-{arch}"),
        format!("cued-{os}-{arch}"),
    ];

    let asset = candidates
        .iter()
        .find_map(|pat| release.assets.iter().find(|a| a.name == *pat))
        .ok_or_else(|| {
            let names: Vec<_> = release.assets.iter().map(|a| a.name.as_str()).collect();
            anyhow::anyhow!(
                "no asset found for {os}-{arch} in release {tag}\n\
                 available assets: {names:?}"
            )
        })?;

    println!("cued upgrade: downloading {}…", asset.name);

    let current_exe = std::env::current_exe().context("resolve current executable path")?;
    let tmp_dir = std::env::temp_dir().join(format!("cued-upgrade-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).context("create temp dir")?;

    let result = download_and_replace(
        &asset.browser_download_url,
        &asset.name,
        &tmp_dir,
        &current_exe,
    );
    if let Err(error) = remove_temp_dir(&tmp_dir) {
        eprintln!(
            "cued upgrade: warning: failed to remove temp dir {}: {error:#}",
            tmp_dir.display()
        );
    }
    result?;

    println!("cued upgrade: updated to {tag} ✓");

    if crate::service::is_installed() {
        println!("cued upgrade: restarting managed service…");
        crate::service::restart().context("restart service after upgrade")?;
        println!("cued upgrade: service restarted");
    } else {
        println!("cued upgrade: run `cued restart` to apply the new binary");
    }

    Ok(())
}

// ── Download + replace ───────────────────────────────────────────────────────

fn download_and_replace(url: &str, asset_name: &str, tmp_dir: &Path, target: &Path) -> Result<()> {
    let archive_path = tmp_dir.join(asset_name);

    let download_cmd = CommandSpec::new("curl")
        .args([
            "--fail",
            "--location",
            "--silent",
            "--show-error",
            "--output",
        ])
        .arg(&archive_path)
        .arg(url);
    let download = download_cmd.output().context("download release asset")?;
    if !download.status.success() {
        bail!(
            "download release asset failed\n{}",
            download_cmd.failure_summary(&download)
        );
    }

    let binary_path = if asset_name.ends_with(".tar.gz") || asset_name.ends_with(".tgz") {
        let extract_cmd = CommandSpec::new("tar")
            .args(["xzf"])
            .arg(&archive_path)
            .arg("-C")
            .arg(tmp_dir);
        let extract = extract_cmd.output().context("extract release archive")?;
        if !extract.status.success() {
            bail!(
                "extract release archive failed\n{}",
                extract_cmd.failure_summary(&extract)
            );
        }
        find_binary_in_dir(tmp_dir)?
    } else {
        archive_path
    };

    make_executable(&binary_path)?;

    // Atomic replace: copy to a sibling temp file, then rename.
    let staging = target.with_extension("new");
    std::fs::copy(&binary_path, &staging)
        .with_context(|| format!("copy binary to staging {}", staging.display()))?;
    make_executable(&staging)?;
    std::fs::rename(&staging, target)
        .with_context(|| format!("atomic rename to {}", target.display()))?;

    Ok(())
}

fn find_binary_in_dir(dir: &std::path::Path) -> Result<PathBuf> {
    // Search up to depth 2 for a file named `cued`.
    for entry in std::fs::read_dir(dir).context("read tmp dir")? {
        let entry = entry?;
        let path = entry.path();
        if path.file_name().and_then(|n| n.to_str()) == Some("cued") && path.is_file() {
            return Ok(path);
        }
        if path.is_dir() {
            for inner in std::fs::read_dir(&path).context("read sub-dir")? {
                let inner = inner?;
                let ipath = inner.path();
                if ipath.file_name().and_then(|n| n.to_str()) == Some("cued") && ipath.is_file() {
                    return Ok(ipath);
                }
            }
        }
    }
    bail!(
        "could not find `cued` binary after extraction in {}",
        dir.display()
    )
}

fn remove_temp_dir(path: &Path) -> Result<()> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("remove temp dir {}", path.display())),
    }
}

#[cfg(unix)]
fn make_executable(path: &std::path::Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(path)
        .with_context(|| format!("stat {}", path.display()))?
        .permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(path, perms).with_context(|| format!("chmod +x {}", path.display()))
}

#[cfg(not(unix))]
fn make_executable(_path: &std::path::Path) -> Result<()> {
    Ok(())
}

// ── HTTP helper ──────────────────────────────────────────────────────────────

fn curl_get(url: &str) -> Result<String> {
    let command = CommandSpec::new("curl").args([
        "--fail",
        "--silent",
        "--show-error",
        "--location",
        "--header",
        "Accept: application/vnd.github.v3+json",
        "--header",
        "User-Agent: cued-upgrade/1.0",
        url,
    ]);
    let output = command.output().context("fetch GitHub release metadata")?;
    if !output.status.success() {
        bail!("{}", command.failure_summary(&output));
    }
    String::from_utf8(output.stdout).context("parse curl output as UTF-8")
}

// ── Platform constants ───────────────────────────────────────────────────────

fn platform_os() -> &'static str {
    #[cfg(target_os = "macos")]
    return "macos";
    #[cfg(target_os = "linux")]
    return "linux";
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    return "unknown";
}

fn platform_arch() -> &'static str {
    #[cfg(target_arch = "aarch64")]
    return "aarch64";
    #[cfg(target_arch = "x86_64")]
    return "x86_64";
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    return "unknown";
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn temp_dir_cleanup_removes_existing_directory() {
        let dir = unique_temp_path("existing");
        std::fs::create_dir_all(dir.join("nested")).expect("create temp dir");
        std::fs::write(dir.join("nested/file"), b"tmp").expect("write temp file");

        remove_temp_dir(&dir).expect("remove temp dir");

        assert!(!dir.exists(), "temp dir should be removed");
    }

    #[test]
    fn temp_dir_cleanup_allows_missing_directory() {
        let dir = unique_temp_path("missing");
        match std::fs::remove_dir_all(&dir) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => panic!("remove stale test dir {}: {error}", dir.display()),
        }

        remove_temp_dir(&dir).expect("missing temp dir is already clean");
    }

    fn unique_temp_path(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "cued-upgrade-{name}-{}-{suffix}",
            std::process::id()
        ))
    }
}
