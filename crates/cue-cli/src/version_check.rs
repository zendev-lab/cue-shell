//! Detect mismatched `cued` versions and warn on `cue` startup.
//!
//! `cue` and `cued` are versioned together as a single workspace. When the
//! daemon binary on a host falls behind — for example when a developer
//! upgrades their `cue` binary but leaves an older `cued` running under
//! launchd/systemd — IPC stays mostly compatible but new features can quietly
//! degrade. This module centralises the detection logic so the TUI entry can:
//!
//! 1. Ask the running `cued` for its version via `Ping`/`Pong` (the `Pong`
//!    payload now carries a `version` field; older daemons send `None`, which
//!    we treat as "outdated, version unknown").
//! 2. Compare against the `cue` build's `CARGO_PKG_VERSION` and emit a
//!    one-shot stderr warning when they disagree.
//! 3. Optionally let the TUI startup layer auto-restart the local `cued` when
//!    `CUE_AUTO_UPDATE_CUED=1` is set.
//!
//! A version mismatch is warning-only. A failed `Ping` is different: it means
//! the connected process did not complete the IPC handshake.
//!
//! Disable the check entirely with `CUE_NO_VERSION_CHECK=1`.
//!
//! See `docs/design/ipc-protocol.md` for the wire-level details of the Pong
//! version field.
use cue_client::CuedClient;

/// `cue` build version this binary was compiled with.
pub fn local_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Outcome of querying the running daemon's version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DaemonVersion {
    /// Daemon reported a concrete version string.
    Reported(String),
    /// Daemon predates Pong-version reporting (`Pong {}` with no `version`).
    Unknown,
}

/// Compare a daemon version against the local `cue` version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionMatch {
    /// Daemon and `cue` report the same version string.
    Match,
    /// Daemon is on a different reported version.
    Mismatch { daemon: String, local: String },
    /// Daemon predates version reporting; cannot prove a match.
    DaemonUnknown { local: String },
}

impl VersionMatch {
    pub fn classify(daemon: &DaemonVersion, local: &str) -> Self {
        match daemon {
            DaemonVersion::Reported(v) if v == local => Self::Match,
            DaemonVersion::Reported(v) => Self::Mismatch {
                daemon: v.clone(),
                local: local.to_string(),
            },
            DaemonVersion::Unknown => Self::DaemonUnknown {
                local: local.to_string(),
            },
        }
    }

    pub fn is_actionable(&self) -> bool {
        !matches!(self, Self::Match)
    }
}

/// Render the user-visible warning shown on stderr.
pub fn render_warning(verdict: &VersionMatch, suggest_auto_update: bool) -> Option<String> {
    let body = match verdict {
        VersionMatch::Match => return None,
        VersionMatch::Mismatch { daemon, local } => format!(
            "warning: cued is running an older/different version (cued={daemon}, cue={local}).",
        ),
        VersionMatch::DaemonUnknown { local } => {
            format!("warning: cued does not report its version (likely older than cue={local}).",)
        }
    };
    let mut lines = vec![body];
    lines.push("  Restart it to pick up the new binary:  `cued restart`".into());
    lines.push("  Or self-update + restart:              `cued upgrade`".into());
    if suggest_auto_update {
        lines.push("  Set CUE_AUTO_UPDATE_CUED=1 to auto-restart on the next launch.".into());
    }
    lines.push("  Suppress this check with CUE_NO_VERSION_CHECK=1.".into());
    Some(lines.join("\n"))
}

/// Whether the user opted out of the check entirely.
pub fn check_disabled() -> bool {
    matches!(
        std::env::var_os("CUE_NO_VERSION_CHECK")
            .as_deref()
            .and_then(|v| v.to_str()),
        Some("1") | Some("true") | Some("yes"),
    )
}

/// Whether the user opted in to auto-restarting a stale local `cued`.
pub fn auto_update_enabled() -> bool {
    matches!(
        std::env::var_os("CUE_AUTO_UPDATE_CUED")
            .as_deref()
            .and_then(|v| v.to_str()),
        Some("1") | Some("true") | Some("yes"),
    )
}

/// Send a `Ping` and return the daemon's reported version.
///
/// Returns `Err` only when the IPC roundtrip itself failed.
pub async fn query_daemon_version(client: &mut CuedClient) -> anyhow::Result<DaemonVersion> {
    let version = client.ping_for_version().await?;
    Ok(match version {
        Some(v) => DaemonVersion::Reported(v),
        None => DaemonVersion::Unknown,
    })
}

/// Print the warning to stderr if the daemon is not on the local version.
pub fn warn_on_mismatch(verdict: &VersionMatch, suggest_auto_update: bool) {
    if let Some(message) = render_warning(verdict, suggest_auto_update) {
        eprintln!("cue: {message}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_match_when_versions_equal() {
        assert_eq!(
            VersionMatch::classify(&DaemonVersion::Reported("0.1.0".into()), "0.1.0"),
            VersionMatch::Match
        );
    }

    #[test]
    fn classify_mismatch_when_versions_differ() {
        assert_eq!(
            VersionMatch::classify(&DaemonVersion::Reported("0.0.9".into()), "0.1.0"),
            VersionMatch::Mismatch {
                daemon: "0.0.9".into(),
                local: "0.1.0".into(),
            }
        );
    }

    #[test]
    fn classify_unknown_when_daemon_predates_reporting() {
        assert_eq!(
            VersionMatch::classify(&DaemonVersion::Unknown, "0.1.0"),
            VersionMatch::DaemonUnknown {
                local: "0.1.0".into()
            }
        );
    }

    #[test]
    fn render_warning_is_none_for_match() {
        assert!(render_warning(&VersionMatch::Match, false).is_none());
    }

    #[test]
    fn render_warning_for_mismatch_includes_both_versions() {
        let msg = render_warning(
            &VersionMatch::Mismatch {
                daemon: "0.0.9".into(),
                local: "0.1.0".into(),
            },
            false,
        )
        .unwrap();
        assert!(msg.contains("cued=0.0.9"), "{msg}");
        assert!(msg.contains("cue=0.1.0"), "{msg}");
        assert!(msg.contains("cued restart"), "{msg}");
        assert!(!msg.contains("CUE_AUTO_UPDATE_CUED"), "{msg}");
    }

    #[test]
    fn render_warning_for_unknown_omits_daemon_version() {
        let msg = render_warning(
            &VersionMatch::DaemonUnknown {
                local: "0.1.0".into(),
            },
            true,
        )
        .unwrap();
        assert!(msg.contains("does not report its version"), "{msg}");
        assert!(msg.contains("CUE_AUTO_UPDATE_CUED=1"), "{msg}");
        assert!(msg.contains("CUE_NO_VERSION_CHECK=1"), "{msg}");
    }

    #[test]
    fn is_actionable_only_for_non_match() {
        assert!(!VersionMatch::Match.is_actionable());
        assert!(
            VersionMatch::Mismatch {
                daemon: "a".into(),
                local: "b".into()
            }
            .is_actionable()
        );
        assert!(VersionMatch::DaemonUnknown { local: "b".into() }.is_actionable());
    }
}
