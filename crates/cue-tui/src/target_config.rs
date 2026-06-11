use std::ffi::OsString;
use std::path::{Path, PathBuf};

use anyhow::Result;
use cue_client::{
    load_transport_config, load_transport_settings_snapshot, save_default_transport_profile,
    transport_connector,
};

use crate::client::ClientConnector;

pub(crate) use cue_client::{
    TransportProfileKind as TargetProfileKind, TransportProfileSource as TargetProfileSource,
    TransportProfileSummary as TargetProfileSummary,
    TransportSettingsSnapshot as TargetSettingsSnapshot,
};

pub(crate) fn load_target_settings() -> Result<TargetSettingsSnapshot> {
    load_transport_settings_snapshot()
}

/// Build a [`ClientConnector`] for the named target profile by reading the
/// on-disk configuration.
pub(crate) fn connector_for_profile(profile_name: &str) -> anyhow::Result<ClientConnector> {
    let transport = load_transport_config()?.resolve_profile(profile_name)?;
    Ok(transport_connector(&transport))
}

pub(crate) fn save_default_profile(
    profile_name: &str,
    known_snapshot: &TargetSettingsSnapshot,
) -> Result<TargetSettingsSnapshot> {
    save_default_transport_profile(profile_name, known_snapshot)
}

pub(crate) fn display_path(path: &Path) -> String {
    let home = home_path_from_env(std::env::var_os("HOME"));
    display_path_with_home(path, home.as_deref())
}

fn home_path_from_env(home: Option<OsString>) -> Option<PathBuf> {
    home.filter(|value| !value.is_empty()).map(PathBuf::from)
}

fn display_path_with_home(path: &Path, home: Option<&Path>) -> String {
    if let Some(home) = home
        && let Ok(rest) = path.strip_prefix(home)
    {
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
        let home = Path::new("/home/tester");
        let rendered = display_path_with_home(&home.join("cue-shell/client.toml"), Some(home));
        assert_eq!(
            rendered,
            format!("~/{}", Path::new("cue-shell/client.toml").display())
        )
    }

    #[test]
    fn display_path_keeps_absolute_path_without_home() {
        let path = Path::new("/tmp/cue-shell/client.toml");

        assert_eq!(
            display_path_with_home(path, None),
            path.display().to_string()
        );
    }

    #[test]
    fn display_home_rejects_empty_home() {
        assert_eq!(home_path_from_env(Some(OsString::new())), None);
    }
}
