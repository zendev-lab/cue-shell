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
//! - `cued-{version}-{os}-{arch}`         (raw binary)
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

#[derive(Debug, Deserialize)]
struct GithubRelease {
    tag_name: String,
    assets: Vec<GithubAsset>,
}

#[derive(Debug, Deserialize)]
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
    let latest = release_asset_version_from_tag(tag)?;
    println!("cued upgrade: latest release {tag}");

    if latest == current {
        println!("cued upgrade: already up to date (v{current})");
        return Ok(());
    }

    let asset = select_release_asset(&release, latest, os, arch)?;
    let download_url = release_asset_download_url(asset)?;

    println!("cued upgrade: downloading {}…", asset.name);

    let current_exe = std::env::current_exe().context("resolve current executable path")?;
    let tmp_dir = std::env::temp_dir().join(format!("cued-upgrade-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).context("create temp dir")?;

    let result = download_and_replace(download_url, &asset.name, &tmp_dir, &current_exe);
    if let Err(error) = remove_temp_dir(&tmp_dir) {
        eprintln!(
            "cued upgrade: warning: failed to remove temp dir {}: {error:#}",
            tmp_dir.display()
        );
    }
    result?;

    println!("cued upgrade: updated to {tag} ✓");

    if crate::service::is_installed()? {
        println!("cued upgrade: restarting managed service…");
        crate::service::restart().context("restart service after upgrade")?;
        println!("cued upgrade: service restarted");
    } else {
        println!("cued upgrade: run `cued restart` to apply the new binary");
    }

    Ok(())
}

fn release_asset_version_from_tag(tag: &str) -> Result<&str> {
    if tag.is_empty() {
        bail!("GitHub release tag must not be empty");
    }
    if tag.trim() != tag {
        bail!("GitHub release tag `{tag}` must not have leading or trailing whitespace");
    }

    let version = tag.strip_prefix('v').unwrap_or(tag);
    if version.is_empty() {
        bail!("GitHub release tag `{tag}` does not contain a version");
    }
    if version.contains('/') || version.contains('\\') {
        bail!("GitHub release tag `{tag}` must not contain path separators");
    }
    Ok(version)
}

fn select_release_asset<'a>(
    release: &'a GithubRelease,
    version: &str,
    os: &str,
    arch: &str,
) -> Result<&'a GithubAsset> {
    let candidates = [
        format!("cued-{version}-{os}-{arch}.tar.gz"),
        format!("cued-{version}-{os}-{arch}"),
    ];

    for candidate in &candidates {
        if let Some(asset) = release.assets.iter().find(|asset| asset.name == *candidate) {
            return Ok(asset);
        }
    }

    let names: Vec<_> = release
        .assets
        .iter()
        .map(|asset| asset.name.as_str())
        .collect();
    bail!(
        "no versioned asset found for {os}-{arch} in release {}\n\
         expected one of: {candidates:?}\n\
         available assets: {names:?}",
        release.tag_name
    )
}

fn release_asset_download_url(asset: &GithubAsset) -> Result<&str> {
    let url = asset.browser_download_url.as_str();
    if url.is_empty() {
        bail!(
            "download URL for release asset `{}` must not be empty",
            asset.name
        );
    }
    if url.chars().any(char::is_whitespace) {
        bail!(
            "download URL for release asset `{}` must not contain whitespace",
            asset.name
        );
    }
    let Some(rest) = url.strip_prefix("https://") else {
        bail!(
            "download URL for release asset `{}` must use https://",
            asset.name
        );
    };
    if rest.is_empty() {
        bail!(
            "download URL for release asset `{}` must include a host",
            asset.name
        );
    }
    Ok(url)
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
    let mut candidates = Vec::new();
    for entry in std::fs::read_dir(dir).context("read tmp dir")? {
        let entry = entry?;
        let path = entry.path();
        if path.file_name().and_then(|n| n.to_str()) == Some("cued") && path.is_file() {
            candidates.push(path);
            continue;
        }
        if path.is_dir() {
            for inner in std::fs::read_dir(&path).context("read sub-dir")? {
                let inner = inner?;
                let ipath = inner.path();
                if ipath.file_name().and_then(|n| n.to_str()) == Some("cued") && ipath.is_file() {
                    candidates.push(ipath);
                }
            }
        }
    }

    match candidates.as_slice() {
        [binary] => Ok(binary.clone()),
        [] => bail!(
            "could not find `cued` binary after extraction in {}",
            dir.display()
        ),
        _ => {
            candidates.sort();
            let paths: Vec<_> = candidates
                .iter()
                .map(|path| path.display().to_string())
                .collect();
            bail!(
                "multiple `cued` binaries found after extraction in {}: {paths:?}",
                dir.display()
            )
        }
    }
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

    #[test]
    fn release_asset_version_accepts_optional_v_prefix() {
        assert_eq!(
            release_asset_version_from_tag("v1.2.3").expect("v-prefixed tag"),
            "1.2.3"
        );
        assert_eq!(
            release_asset_version_from_tag("1.2.3").expect("bare version tag"),
            "1.2.3"
        );
    }

    #[test]
    fn release_asset_version_rejects_malformed_tags() {
        for (tag, expected) in [
            ("", "must not be empty"),
            (" v1.2.3", "leading or trailing whitespace"),
            ("v1.2.3 ", "leading or trailing whitespace"),
            ("v", "does not contain a version"),
            ("v1.2.3/macos", "must not contain path separators"),
            ("v1.2.3\\macos", "must not contain path separators"),
        ] {
            let error = release_asset_version_from_tag(tag).expect_err("malformed tag should fail");
            assert!(
                format!("{error:#}").contains(expected),
                "wrong error for {tag:?}: {error:#}"
            );
        }
    }

    #[test]
    fn release_asset_selection_prefers_versioned_tarball() {
        let release = GithubRelease {
            tag_name: "v1.2.3".into(),
            assets: vec![
                GithubAsset {
                    name: "cued-1.2.3-macos-aarch64".into(),
                    browser_download_url: "https://example.test/raw".into(),
                },
                GithubAsset {
                    name: "cued-1.2.3-macos-aarch64.tar.gz".into(),
                    browser_download_url: "https://example.test/tar".into(),
                },
            ],
        };

        let asset = select_release_asset(&release, "1.2.3", "macos", "aarch64")
            .expect("versioned tarball should match");

        assert_eq!(asset.browser_download_url, "https://example.test/tar");
    }

    #[test]
    fn release_asset_selection_rejects_unversioned_fallback_assets() {
        let release = GithubRelease {
            tag_name: "v1.2.3".into(),
            assets: vec![GithubAsset {
                name: "cued-macos-aarch64.tar.gz".into(),
                browser_download_url: "https://example.test/tar".into(),
            }],
        };

        let error = select_release_asset(&release, "1.2.3", "macos", "aarch64")
            .expect_err("unversioned assets should not satisfy upgrade release selection");

        let message = format!("{error:#}");
        assert!(message.contains("no versioned asset found for macos-aarch64"));
        assert!(message.contains("cued-1.2.3-macos-aarch64.tar.gz"));
        assert!(message.contains("cued-macos-aarch64.tar.gz"));
    }

    #[test]
    fn release_asset_download_url_accepts_https_url() {
        let asset = GithubAsset {
            name: "cued-1.2.3-macos-aarch64.tar.gz".into(),
            browser_download_url: "https://example.test/releases/cued.tar.gz".into(),
        };

        assert_eq!(
            release_asset_download_url(&asset).expect("https URL should be accepted"),
            "https://example.test/releases/cued.tar.gz"
        );
    }

    #[test]
    fn release_asset_download_url_rejects_unusable_urls() {
        for (url, expected) in [
            ("", "must not be empty"),
            (" https://example.test/cued", "must not contain whitespace"),
            ("https://example.test/cued\n", "must not contain whitespace"),
            ("http://example.test/cued", "must use https://"),
            ("https://", "must include a host"),
        ] {
            let asset = GithubAsset {
                name: "cued-1.2.3-macos-aarch64.tar.gz".into(),
                browser_download_url: url.into(),
            };
            let error =
                release_asset_download_url(&asset).expect_err("bad download URL should fail");

            assert!(
                format!("{error:#}").contains(expected),
                "wrong error for {url:?}: {error:#}"
            );
        }
    }

    #[test]
    fn find_binary_in_dir_finds_single_cued_binary_one_level_deep() {
        let dir = unique_temp_path("single-cued");
        let bin_dir = dir.join("release");
        std::fs::create_dir_all(&bin_dir).expect("create release dir");
        let binary = bin_dir.join("cued");
        std::fs::write(&binary, b"#!/bin/sh\n").expect("write cued binary");

        let found = find_binary_in_dir(&dir).expect("single cued binary should be selected");

        assert_eq!(found, binary);
        remove_temp_dir(&dir).expect("remove test dir");
    }

    #[test]
    fn find_binary_in_dir_rejects_ambiguous_cued_binaries() {
        let dir = unique_temp_path("multiple-cued");
        let nested = dir.join("release");
        std::fs::create_dir_all(&nested).expect("create release dir");
        std::fs::write(dir.join("cued"), b"top").expect("write top-level cued");
        std::fs::write(nested.join("cued"), b"nested").expect("write nested cued");

        let error =
            find_binary_in_dir(&dir).expect_err("multiple cued binaries should be ambiguous");

        let message = format!("{error:#}");
        assert!(message.contains("multiple `cued` binaries found after extraction"));
        assert!(message.contains("release/cued"));
        remove_temp_dir(&dir).expect("remove test dir");
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
