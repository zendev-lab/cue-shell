use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::{
    client_config_path, default_socket_path, detected_ssh_hosts, legacy_config_path,
    read_config_source,
};

#[derive(Debug, Clone, Default, Deserialize)]
struct TransportConfigFile {
    #[serde(default)]
    transport: TransportConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TransportConfig {
    #[serde(default = "default_profile_name")]
    pub default_profile: String,
    #[serde(default = "default_auto_detect_ssh")]
    pub auto_detect_ssh: bool,
    #[serde(default = "default_profiles")]
    pub profiles: BTreeMap<String, TransportProfile>,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            default_profile: default_profile_name(),
            auto_detect_ssh: default_auto_detect_ssh(),
            profiles: default_profiles(),
        }
    }
}

impl TransportConfig {
    pub fn normalize_defaults(&mut self) {
        if self.default_profile.is_empty() {
            self.default_profile = default_profile_name();
        }
    }

    pub fn resolve_transport(&self, socket_override: Option<PathBuf>) -> Result<ResolvedTransport> {
        self.resolve_transport_with_detected(socket_override, detected_ssh_hosts())
    }

    pub fn resolve_transport_with_detected(
        &self,
        socket_override: Option<PathBuf>,
        detected_hosts: BTreeSet<String>,
    ) -> Result<ResolvedTransport> {
        if let Some(socket_path) = socket_override {
            return Ok(ResolvedTransport::Unix {
                profile_name: "env:CUE_SOCKET".into(),
                socket_path,
            });
        }

        self.resolve_profile_with_detected(&self.default_profile, &detected_hosts)
    }

    pub fn resolve_profile(&self, profile_name: &str) -> Result<ResolvedTransport> {
        self.resolve_profile_with_detected(profile_name, &detected_ssh_hosts())
    }

    pub fn resolve_profile_with_detected(
        &self,
        profile_name: &str,
        detected_hosts: &BTreeSet<String>,
    ) -> Result<ResolvedTransport> {
        let profiles = self.merged_profiles(detected_hosts);
        let profile = profiles
            .get(profile_name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unknown client transport profile `{profile_name}`"))?;

        Ok(match profile {
            TransportProfile::Unix(profile) => ResolvedTransport::Unix {
                profile_name: profile_name.to_string(),
                socket_path: profile.socket.unwrap_or_else(default_socket_path),
            },
            TransportProfile::Ssh(profile) => ResolvedTransport::Ssh {
                profile_name: profile_name.to_string(),
                destination: profile.destination,
                gateway_command: profile.gateway_command,
                start_command: profile.start_command,
            },
        })
    }

    pub fn merged_profiles(
        &self,
        detected_hosts: &BTreeSet<String>,
    ) -> BTreeMap<String, TransportProfile> {
        let mut profiles = self.profiles.clone();

        match profiles.get("local") {
            Some(TransportProfile::Unix(_)) => {}
            _ => {
                profiles.insert(
                    "local".into(),
                    TransportProfile::Unix(UnixProfile::default()),
                );
            }
        }

        if self.auto_detect_ssh {
            for host in detected_hosts {
                if host == "local" {
                    continue;
                }
                profiles.entry(host.clone()).or_insert_with(|| {
                    TransportProfile::Ssh(SshProfile {
                        destination: host.clone(),
                        gateway_command: default_gateway_command(),
                        start_command: default_start_command(),
                    })
                });
            }
        }

        profiles
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "transport", rename_all = "snake_case")]
pub enum TransportProfile {
    Unix(UnixProfile),
    Ssh(SshProfile),
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct UnixProfile {
    #[serde(default)]
    pub socket: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SshProfile {
    pub destination: String,
    #[serde(default = "default_gateway_command")]
    pub gateway_command: String,
    #[serde(default = "default_start_command")]
    pub start_command: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedTransport {
    Unix {
        profile_name: String,
        socket_path: PathBuf,
    },
    Ssh {
        profile_name: String,
        destination: String,
        gateway_command: String,
        start_command: String,
    },
}

pub fn load_transport_config() -> Result<TransportConfig> {
    let client_path = client_config_path();
    let legacy_path = legacy_config_path();
    load_transport_config_from_sources(
        read_config_source(&client_path)?
            .as_deref()
            .map(|text| (client_path.as_path(), text)),
        read_config_source(&legacy_path)?
            .as_deref()
            .map(|text| (legacy_path.as_path(), text)),
    )
}

pub fn load_transport_config_from_sources(
    client: Option<(&Path, &str)>,
    legacy: Option<(&Path, &str)>,
) -> Result<TransportConfig> {
    if let Some((path, text)) = client {
        return parse_transport_config(text, path);
    }
    if let Some((path, text)) = legacy {
        return parse_transport_config(text, path);
    }
    Ok(TransportConfig::default())
}

pub fn parse_transport_config(text: &str, path: &Path) -> Result<TransportConfig> {
    let mut file: TransportConfigFile =
        toml::from_str(text).with_context(|| format!("parse config {}", path.display()))?;
    file.transport.normalize_defaults();
    Ok(file.transport)
}

pub(crate) fn default_profile_name() -> String {
    "local".into()
}

fn default_auto_detect_ssh() -> bool {
    true
}

pub(crate) fn default_gateway_command() -> String {
    "cued gateway --stdio".into()
}

fn default_start_command() -> String {
    "cued start".into()
}

fn default_profiles() -> BTreeMap<String, TransportProfile> {
    BTreeMap::from([(
        "local".into(),
        TransportProfile::Unix(UnixProfile::default()),
    )])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_transport_uses_local_unix_socket() {
        let config = TransportConfig::default();
        let transport = config.resolve_transport(None).expect("resolve transport");
        assert_eq!(
            transport,
            ResolvedTransport::Unix {
                profile_name: "local".into(),
                socket_path: default_socket_path(),
            }
        );
    }

    #[test]
    fn client_toml_takes_precedence_over_legacy_config_toml() {
        let config = load_transport_config_from_sources(
            Some((
                Path::new("client.toml"),
                r#"
[transport]
default_profile = "remote"

[transport.profiles.remote]
transport = "ssh"
destination = "devbox"
gateway_command = "cued gateway --stdio --socket ~/.cache/cue-shell/remote.sock"
start_command = "cued start --socket ~/.cache/cue-shell/remote.sock"
"#,
            )),
            Some((
                Path::new("config.toml"),
                r#"
[transport]
default_profile = "legacy"

[transport.profiles.legacy]
transport = "unix"
socket = "/legacy.sock"
"#,
            )),
        )
        .expect("load config");

        let transport = config.resolve_transport(None).expect("resolve transport");
        assert_eq!(
            transport,
            ResolvedTransport::Ssh {
                profile_name: "remote".into(),
                destination: "devbox".into(),
                gateway_command: "cued gateway --stdio --socket ~/.cache/cue-shell/remote.sock"
                    .into(),
                start_command: "cued start --socket ~/.cache/cue-shell/remote.sock".into(),
            }
        );
    }

    #[test]
    fn ssh_profile_defaults_remote_commands() {
        let config = load_transport_config_from_sources(
            Some((
                Path::new("client.toml"),
                r#"
[transport]
default_profile = "remote"

[transport.profiles.remote]
transport = "ssh"
destination = "devbox"
"#,
            )),
            None,
        )
        .expect("load config");

        let transport = config.resolve_transport(None).expect("resolve transport");
        assert_eq!(
            transport,
            ResolvedTransport::Ssh {
                profile_name: "remote".into(),
                destination: "devbox".into(),
                gateway_command: "cued gateway --stdio".into(),
                start_command: "cued start".into(),
            }
        );
    }

    #[test]
    fn legacy_config_toml_still_loads_transport_profiles() {
        let config = load_transport_config_from_sources(
            None,
            Some((
                Path::new("config.toml"),
                r#"
[transport]
default_profile = "legacy"

[transport.profiles.legacy]
transport = "unix"
socket = "/legacy.sock"
"#,
            )),
        )
        .expect("load config");

        let transport = config.resolve_transport(None).expect("resolve transport");
        assert_eq!(
            transport,
            ResolvedTransport::Unix {
                profile_name: "legacy".into(),
                socket_path: PathBuf::from("/legacy.sock"),
            }
        );
    }

    #[test]
    fn detected_ssh_hosts_extend_profiles_without_removing_local() {
        let config = load_transport_config_from_sources(
            Some((
                Path::new("client.toml"),
                r#"
[transport]
default_profile = "remote"

[transport.profiles.remote]
transport = "ssh"
destination = "configured-devbox"
"#,
            )),
            None,
        )
        .expect("load config");

        let profiles = config.merged_profiles(&BTreeSet::from(["devbox".into(), "remote".into()]));

        assert!(matches!(
            profiles.get("local"),
            Some(TransportProfile::Unix(_))
        ));
        assert!(matches!(
            profiles.get("devbox"),
            Some(TransportProfile::Ssh(SshProfile { destination, .. })) if destination == "devbox"
        ));
        assert!(matches!(
            profiles.get("remote"),
            Some(TransportProfile::Ssh(SshProfile { destination, .. })) if destination == "configured-devbox"
        ));
    }

    #[test]
    fn default_profile_can_resolve_detected_ssh_host() {
        let config = load_transport_config_from_sources(
            Some((
                Path::new("client.toml"),
                r#"
[transport]
default_profile = "devbox"
"#,
            )),
            None,
        )
        .expect("load config");

        let transport = config
            .resolve_transport_with_detected(None, BTreeSet::from(["devbox".into()]))
            .expect("resolve transport");

        assert_eq!(
            transport,
            ResolvedTransport::Ssh {
                profile_name: "devbox".into(),
                destination: "devbox".into(),
                gateway_command: "cued gateway --stdio".into(),
                start_command: "cued start".into(),
            }
        );
    }

    #[test]
    fn auto_detect_ssh_can_be_disabled() {
        let config = load_transport_config_from_sources(
            Some((
                Path::new("client.toml"),
                r#"
[transport]
auto_detect_ssh = false
"#,
            )),
            None,
        )
        .expect("load config");

        let profiles = config.merged_profiles(&BTreeSet::from(["devbox".into()]));

        assert!(profiles.contains_key("local"));
        assert!(!profiles.contains_key("devbox"));
    }
}
