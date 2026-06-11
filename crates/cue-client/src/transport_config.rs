use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use toml::Value;

use crate::client::default_socket_path;
use crate::config_paths::{client_config_paths, read_client_config_sources};
use crate::host_discovery::{HostDiscoveryConfig, detected_configured_hosts};
use crate::ssh_config::detected_ssh_hosts;

const CLIENT_ROOT_SECTIONS: &[&str] = &["extensions", "transport"];

#[derive(Debug, Clone, Default, Deserialize)]
struct TransportConfigFile {
    #[serde(default)]
    transport: TransportConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransportConfig {
    #[serde(default = "default_profile_name")]
    default_profile: String,
    #[serde(default = "default_auto_detect_ssh")]
    auto_detect_ssh: bool,
    #[serde(default)]
    discovery: HostDiscoveryConfig,
    #[serde(default = "default_profiles")]
    profiles: BTreeMap<String, TransportProfile>,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            default_profile: default_profile_name(),
            auto_detect_ssh: default_auto_detect_ssh(),
            discovery: HostDiscoveryConfig::default(),
            profiles: default_profiles(),
        }
    }
}

impl TransportConfig {
    pub fn resolve_transport(&self, socket_override: Option<PathBuf>) -> Result<ResolvedTransport> {
        self.resolve_transport_with_detection(socket_override, || self.detected_hosts())
    }

    fn resolve_transport_with_detection<F>(
        &self,
        socket_override: Option<PathBuf>,
        detect_hosts: F,
    ) -> Result<ResolvedTransport>
    where
        F: FnOnce() -> Result<BTreeSet<String>>,
    {
        self.validate()?;

        if let Some(socket_path) = socket_override {
            validate_socket_path("CUE_SOCKET", &socket_path)?;
            return Ok(ResolvedTransport::Unix {
                profile_name: "env:CUE_SOCKET".into(),
                socket_path,
            });
        }

        self.resolve_profile_with_detection_after_validate(&self.default_profile, detect_hosts)
    }

    #[cfg(test)]
    fn resolve_transport_with_detected(
        &self,
        socket_override: Option<PathBuf>,
        detected_hosts: BTreeSet<String>,
    ) -> Result<ResolvedTransport> {
        self.validate()?;

        if let Some(socket_path) = socket_override {
            validate_socket_path("CUE_SOCKET", &socket_path)?;
            return Ok(ResolvedTransport::Unix {
                profile_name: "env:CUE_SOCKET".into(),
                socket_path,
            });
        }

        self.resolve_profile_with_detected(&self.default_profile, &detected_hosts)
    }

    pub fn resolve_profile(&self, profile_name: &str) -> Result<ResolvedTransport> {
        self.validate()?;
        self.resolve_profile_with_detection_after_validate(profile_name, || self.detected_hosts())
    }

    fn resolve_profile_with_detection_after_validate<F>(
        &self,
        profile_name: &str,
        detect_hosts: F,
    ) -> Result<ResolvedTransport>
    where
        F: FnOnce() -> Result<BTreeSet<String>>,
    {
        if let Some(transport) = self.resolve_profile_from_detected(profile_name, &BTreeSet::new())
        {
            return Ok(transport);
        }

        if self.auto_detect_ssh {
            let detected_hosts = detect_hosts().with_context(|| {
                format!(
                    "auto-detect SSH profiles while resolving unknown client transport profile `{profile_name}`"
                )
            })?;
            if let Some(transport) =
                self.resolve_profile_from_detected(profile_name, &detected_hosts)
            {
                return Ok(transport);
            }
        }

        bail!("unknown client transport profile `{profile_name}`")
    }

    #[cfg(test)]
    fn resolve_profile_with_detected(
        &self,
        profile_name: &str,
        detected_hosts: &BTreeSet<String>,
    ) -> Result<ResolvedTransport> {
        self.validate()?;
        self.resolve_profile_from_detected(profile_name, detected_hosts)
            .ok_or_else(|| anyhow::anyhow!("unknown client transport profile `{profile_name}`"))
    }

    fn detected_hosts(&self) -> Result<BTreeSet<String>> {
        detected_transport_hosts(&self.discovery)
    }

    fn resolve_profile_from_detected(
        &self,
        profile_name: &str,
        detected_hosts: &BTreeSet<String>,
    ) -> Option<ResolvedTransport> {
        debug_assert!(
            self.validate().is_ok(),
            "resolve_profile_from_detected requires a validated transport config"
        );

        let profiles = self.merged_profiles(detected_hosts);
        let profile = profiles.get(profile_name).cloned()?;

        Some(match profile {
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

    fn merged_profiles(
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

    pub fn validate(&self) -> Result<()> {
        validate_default_profile_name(&self.default_profile)?;
        if matches!(self.profiles.get("local"), Some(TransportProfile::Ssh(_))) {
            bail!("transport.profiles.local is reserved for unix transport");
        }
        for (name, profile) in &self.profiles {
            validate_profile_name(name)?;
            match profile {
                TransportProfile::Unix(profile) => validate_unix_profile(name, profile)?,
                TransportProfile::Ssh(profile) => validate_ssh_profile(name, profile)?,
            }
        }
        Ok(())
    }
}

pub(crate) fn validate_default_profile_name(name: &str) -> Result<()> {
    validate_name_boundary(
        name,
        "transport.default_profile must not be empty",
        "transport.default_profile must not have leading or trailing whitespace",
    )
}

pub(crate) fn validate_profile_name(name: &str) -> Result<()> {
    validate_name_boundary(
        name,
        "transport profile names must not be empty",
        "transport profile names must not have leading or trailing whitespace",
    )
}

fn validate_name_boundary(name: &str, empty_message: &str, padded_message: &str) -> Result<()> {
    if name.trim().is_empty() {
        bail!("{empty_message}");
    }
    if name.trim() != name {
        bail!("{padded_message}");
    }
    Ok(())
}

fn validate_unix_profile(name: &str, profile: &UnixProfile) -> Result<()> {
    if let Some(socket) = &profile.socket {
        validate_socket_path(&format!("transport.profiles.{name}.socket"), socket)?;
    }
    Ok(())
}

fn validate_socket_path(field: &str, socket: &Path) -> Result<()> {
    let Some(socket) = socket.to_str() else {
        bail!("{field} must be valid UTF-8");
    };
    validate_name_boundary(
        socket,
        &format!("{field} must not be empty"),
        &format!("{field} must not have leading or trailing whitespace"),
    )
}

fn validate_ssh_profile(name: &str, profile: &SshProfile) -> Result<()> {
    validate_name_boundary(
        &profile.destination,
        &format!("transport.profiles.{name}.destination must not be empty"),
        &format!(
            "transport.profiles.{name}.destination must not have leading or trailing whitespace"
        ),
    )?;
    validate_name_boundary(
        &profile.gateway_command,
        &format!("transport.profiles.{name}.gateway_command must not be empty"),
        &format!(
            "transport.profiles.{name}.gateway_command must not have leading or trailing whitespace"
        ),
    )?;
    validate_name_boundary(
        &profile.start_command,
        &format!("transport.profiles.{name}.start_command must not be empty"),
        &format!(
            "transport.profiles.{name}.start_command must not have leading or trailing whitespace"
        ),
    )
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "transport", rename_all = "snake_case", deny_unknown_fields)]
pub enum TransportProfile {
    Unix(UnixProfile),
    Ssh(SshProfile),
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnixProfile {
    #[serde(default)]
    pub socket: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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
    let paths = client_config_paths()?;
    let sources = read_client_config_sources(&paths)?;
    load_transport_config_from_sources(
        sources
            .primary()
            .map(|source| (source.path(), source.text())),
    )
}

pub fn load_transport_config_from_sources(
    source: Option<(&Path, &str)>,
) -> Result<TransportConfig> {
    if let Some((path, text)) = source {
        return parse_transport_config(text, path);
    }
    Ok(TransportConfig::default())
}

pub fn parse_transport_config(text: &str, path: &Path) -> Result<TransportConfig> {
    validate_client_config_root_sections(text, path)?;
    let file: TransportConfigFile =
        toml::from_str(text).with_context(|| format!("parse config {}", path.display()))?;
    file.transport.validate()?;
    Ok(file.transport)
}

pub(crate) fn detected_transport_hosts(
    discovery: &HostDiscoveryConfig,
) -> Result<BTreeSet<String>> {
    let mut hosts = detected_ssh_hosts()?;
    hosts.extend(detected_configured_hosts(discovery)?);
    Ok(hosts)
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

pub(crate) fn default_start_command() -> String {
    "cued start".into()
}

fn default_profiles() -> BTreeMap<String, TransportProfile> {
    BTreeMap::from([(
        "local".into(),
        TransportProfile::Unix(UnixProfile::default()),
    )])
}

pub fn validate_client_config_root_sections(text: &str, path: &Path) -> Result<()> {
    let value: Value = toml::from_str(text)
        .with_context(|| format!("parse config root sections {}", path.display()))?;
    let Some(table) = value.as_table() else {
        bail!("config root must be a TOML table");
    };
    for key in table.keys() {
        if !CLIENT_ROOT_SECTIONS.contains(&key.as_str()) {
            bail!(
                "unknown top-level client config section `{key}` in {}; supported top-level sections: {}",
                path.display(),
                CLIENT_ROOT_SECTIONS.join(", ")
            );
        }
    }

    Ok(())
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
    fn client_toml_takes_precedence() {
        let config = load_transport_config_from_sources(Some((
            Path::new("client.toml"),
            r#"
[transport]
default_profile = "remote"

[transport.profiles.remote]
transport = "ssh"
destination = "devbox"
"#,
        )))
        .expect("load config");

        assert_eq!(
            config.resolve_profile("remote").expect("resolve remote"),
            ResolvedTransport::Ssh {
                profile_name: "remote".into(),
                destination: "devbox".into(),
                gateway_command: default_gateway_command(),
                start_command: default_start_command(),
            }
        );
    }

    #[test]
    fn default_profile_can_resolve_detected_ssh_host() {
        let config = load_transport_config_from_sources(Some((
            Path::new("client.toml"),
            r#"
[transport]
default_profile = "devbox"
"#,
        )))
        .expect("load config");

        let detected = BTreeSet::from(["devbox".to_string()]);
        assert_eq!(
            config
                .resolve_transport_with_detected(None, detected)
                .expect("resolve detected"),
            ResolvedTransport::Ssh {
                profile_name: "devbox".into(),
                destination: "devbox".into(),
                gateway_command: default_gateway_command(),
                start_command: default_start_command(),
            }
        );
    }

    #[test]
    fn auto_detect_ssh_can_be_disabled() {
        let config = load_transport_config_from_sources(Some((
            Path::new("client.toml"),
            r#"
[transport]
default_profile = "devbox"
auto_detect_ssh = false
"#,
        )))
        .expect("load config");

        let detected = BTreeSet::from(["devbox".to_string()]);
        let error = config
            .resolve_transport_with_detected(None, detected)
            .expect_err("disabled detection should not resolve detected host");

        assert!(format!("{error:#}").contains("unknown client transport profile `devbox`"));
    }

    #[test]
    fn detected_ssh_hosts_extend_profiles_without_removing_local() {
        let config = TransportConfig::default();
        let profiles = config.merged_profiles(&BTreeSet::from(["devbox".to_string()]));

        assert!(matches!(
            profiles.get("local"),
            Some(TransportProfile::Unix(_))
        ));
        assert!(matches!(
            profiles.get("devbox"),
            Some(TransportProfile::Ssh(_))
        ));
    }

    #[test]
    fn ssh_profile_defaults_remote_commands() {
        let config = load_transport_config_from_sources(Some((
            Path::new("client.toml"),
            r#"
[transport.profiles.remote]
transport = "ssh"
destination = "devbox"
"#,
        )))
        .expect("load config");

        assert_eq!(
            config.resolve_profile("remote").expect("resolve remote"),
            ResolvedTransport::Ssh {
                profile_name: "remote".into(),
                destination: "devbox".into(),
                gateway_command: "cued gateway --stdio".into(),
                start_command: "cued start".into(),
            }
        );
    }

    #[test]
    fn parses_generic_host_discovery_config() {
        let config = load_transport_config_from_sources(Some((
            Path::new("client.toml"),
            r#"
[transport.discovery]
env_hosts = ["CLUSTER_HOSTS"]
env_endpoints = ["CLUSTER_ENDPOINTS"]
env_hostfiles = ["CLUSTER_HOSTFILE"]
env_bracket_ranges = ["CLUSTER_NODELIST"]
"#,
        )))
        .expect("load config");

        assert_eq!(config.discovery.env_hosts, vec!["CLUSTER_HOSTS"]);
        assert_eq!(config.discovery.env_endpoints, vec!["CLUSTER_ENDPOINTS"]);
        assert_eq!(config.discovery.env_hostfiles, vec!["CLUSTER_HOSTFILE"]);
        assert_eq!(
            config.discovery.env_bracket_ranges,
            vec!["CLUSTER_NODELIST"]
        );
    }

    #[test]
    fn local_profile_rejects_non_unix_transport() {
        let error = load_transport_config_from_sources(Some((
            Path::new("client.toml"),
            r#"
[transport.profiles.local]
transport = "ssh"
destination = "localhost"
"#,
        )))
        .expect_err("local ssh profile should fail");

        assert!(format!("{error:#}").contains("local"));
    }

    #[test]
    fn rejects_unknown_root_section() {
        let error = load_transport_config_from_sources(Some((
            Path::new("client.toml"),
            r#"
[daemon]
foo = true
"#,
        )))
        .expect_err("unknown client root section should fail");

        assert!(format!("{error:#}").contains("unknown top-level client config section `daemon`"));
    }
}
