//! Detect mismatched `cued` versions and warn on frontend startup.

use cue_client::CuedClient;

/// Frontend build version this binary was compiled with.
pub fn local_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Outcome of querying the running daemon's version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonVersion(pub String);

/// Compare a daemon version against the local frontend version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionMatch {
    Match,
    Mismatch { daemon: String, local: String },
}

impl VersionMatch {
    pub fn classify(daemon: &DaemonVersion, local: &str) -> Self {
        if daemon.0 == local {
            Self::Match
        } else {
            Self::Mismatch {
                daemon: daemon.0.clone(),
                local: local.to_string(),
            }
        }
    }

    pub fn is_actionable(&self) -> bool {
        !matches!(self, Self::Match)
    }
}

pub fn render_warning(verdict: &VersionMatch, suggest_auto_update: bool) -> Option<String> {
    let body = match verdict {
        VersionMatch::Match => return None,
        VersionMatch::Mismatch { daemon, local } => {
            format!(
                "warning: cued is running a different version (cued={daemon}, cue-tui={local})."
            )
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

pub fn check_disabled() -> bool {
    matches!(
        std::env::var_os("CUE_NO_VERSION_CHECK")
            .as_deref()
            .and_then(|v| v.to_str()),
        Some("1") | Some("true") | Some("yes"),
    )
}

pub fn auto_update_enabled() -> bool {
    matches!(
        std::env::var_os("CUE_AUTO_UPDATE_CUED")
            .as_deref()
            .and_then(|v| v.to_str()),
        Some("1") | Some("true") | Some("yes"),
    )
}

pub async fn query_daemon_version(client: &mut CuedClient) -> anyhow::Result<DaemonVersion> {
    Ok(DaemonVersion(client.ping_for_version().await?))
}

pub fn warn_on_mismatch(verdict: &VersionMatch, suggest_auto_update: bool) {
    if let Some(message) = render_warning(verdict, suggest_auto_update) {
        eprintln!("cue-tui: {message}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_match_when_versions_equal() {
        assert_eq!(
            VersionMatch::classify(&DaemonVersion("0.1.0".into()), "0.1.0"),
            VersionMatch::Match
        );
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
        assert!(msg.contains("cue-tui=0.1.0"), "{msg}");
    }
}
