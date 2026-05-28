use std::path::Path;

use anyhow::Result;
use cue_client::{
    ResolvedTransport, home_dir, load_transport_config, load_transport_settings_snapshot,
    save_default_transport_profile,
};

pub use cue_client::{
    TransportProfileSource as TargetProfileSource, TransportProfileSummary as TargetProfileSummary,
    TransportSettingsSnapshot as TargetSettingsSnapshot,
};

pub fn load_target_settings() -> Result<TargetSettingsSnapshot> {
    load_transport_settings_snapshot()
}

/// Build a [`cue_client::ClientConnector`] for the named target profile by
/// reading the on-disk configuration.
///
/// Only the `"unix"` transport is supported for live reconnects; attempting
/// to build a connector for an `"ssh"` profile returns an error.
pub fn connector_for_profile(profile_name: &str) -> anyhow::Result<cue_client::ClientConnector> {
    match load_transport_config()?.resolve_profile(profile_name)? {
        ResolvedTransport::Unix { socket_path, .. } => {
            Ok(cue_client::ClientConnector::unix(socket_path))
        }
        ResolvedTransport::Ssh { .. } => anyhow::bail!(
            "SSH transport does not support live reconnect; \
             restart cue-tui to apply the profile change"
        ),
    }
}

pub fn save_default_profile(
    profile_name: &str,
    known_snapshot: &TargetSettingsSnapshot,
) -> Result<TargetSettingsSnapshot> {
    save_default_transport_profile(profile_name, known_snapshot)
}

pub fn display_path(path: &Path) -> String {
    let home = home_dir();
    if let Ok(rest) = path.strip_prefix(&home) {
        if rest.as_os_str().is_empty() {
            "~".into()
        } else {
            format!("~/{}", rest.display())
        }
    } else {
        path.display().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_path_abbreviates_home_prefix() {
        let home = cue_client::home_dir();
        let rendered = display_path(&home.join("cue-shell/client.toml"));
        assert_eq!(
            rendered,
            format!("~/{}", Path::new("cue-shell/client.toml").display())
        )
    }
}
