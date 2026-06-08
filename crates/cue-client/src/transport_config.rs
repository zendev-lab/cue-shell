use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use toml::Value;

use crate::client::default_socket_path;
use crate::config_paths::{client_config_paths, read_client_config_sources};
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
    #[serde(default = "default_profiles")]
    profiles: BTreeMap<String, TransportProfile>,
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
    pub fn resolve_transport(&self, socket_override: Option<PathBuf>) -> Result<ResolvedTransport> {
        self.resolve_transport_with_detection(socket_override, detected_ssh_hosts)
    }

    fn resolve_transport_with_detection<F>(
        &self,
        socket_override: Option<PathBuf>,
        detect_ssh_hosts: F,
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

        self.resolve_profile_with_detection_after_validate(&self.default_profile, detect_ssh_hosts)
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
        self.resolve_profile_with_detection_after_validate(profile_name, detected_ssh_hosts)
    }

    fn resolve_profile_with_detection_after_validate<F>(
        &self,
        profile_name: &str,
        detect_ssh_hosts: F,
    ) -> Result<ResolvedTransport>
    where
        F: FnOnce() -> Result<BTreeSet<String>>,
    {
        if let Some(transport) = self.resolve_profile_from_detected(profile_name, &BTreeSet::new())
        {
            return Ok(transport);
        }

        if self.auto_detect_ssh {
            let detected_hosts = detect_ssh_hosts().with_context(|| {
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
    if socket.trim().is_empty() {
        bail!("{field} must not be empty");
    }
    if socket.trim() != socket {
        bail!("{field} must not have leading or trailing whitespace");
    }
    Ok(())
}

fn validate_ssh_profile(name: &str, profile: &SshProfile) -> Result<()> {
    validate_ssh_profile_field(name, "destination", &profile.destination)?;
    validate_ssh_profile_field(name, "gateway_command", &profile.gateway_command)?;
    validate_ssh_profile_field(name, "start_command", &profile.start_command)
}

fn validate_ssh_profile_field(name: &str, field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("transport.profiles.{name}.{field} must not be empty");
    }
    if value.trim() != value {
        bail!("transport.profiles.{name}.{field} must not have leading or trailing whitespace");
    }
    Ok(())
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "transport", rename_all = "snake_case")]
enum TransportProfile {
    Unix(UnixProfile),
    Ssh(SshProfile),
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct UnixProfile {
    #[serde(default)]
    socket: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SshProfile {
    destination: String,
    #[serde(default = "default_gateway_command")]
    gateway_command: String,
    #[serde(default = "default_start_command")]
    start_command: String,
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

fn load_transport_config_from_sources(source: Option<(&Path, &str)>) -> Result<TransportConfig> {
    if let Some((path, text)) = source {
        return parse_transport_config(text, path);
    }
    Ok(TransportConfig::default())
}

fn parse_transport_config(text: &str, path: &Path) -> Result<TransportConfig> {
    validate_client_config_root_sections(text, path)?;
    let file: TransportConfigFile =
        toml::from_str(text).with_context(|| format!("parse config {}", path.display()))?;
    file.transport.validate()?;
    Ok(file.transport)
}

pub fn validate_client_config_root_sections(text: &str, path: &Path) -> Result<()> {
    let value: Value =
        toml::from_str(text).with_context(|| format!("parse config {}", path.display()))?;
    let Some(root) = value.as_table() else {
        bail!("config {} must be a TOML table", path.display());
    };

    for section in root.keys() {
        if CLIENT_ROOT_SECTIONS.contains(&section.as_str()) {
            continue;
        }

        bail!(
            "unknown top-level client config section `{section}` in {}; expected client sections [{}]",
            path.display(),
            CLIENT_ROOT_SECTIONS.join(", ")
        );
    }

    Ok(())
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
    fn socket_override_rejects_empty_or_padded_values() {
        let config = TransportConfig::default();
        for (socket_path, expected) in [
            (PathBuf::new(), "CUE_SOCKET must not be empty"),
            (PathBuf::from("   "), "CUE_SOCKET must not be empty"),
            (
                PathBuf::from(" /tmp/cue.sock"),
                "CUE_SOCKET must not have leading or trailing whitespace",
            ),
            (
                PathBuf::from("/tmp/cue.sock "),
                "CUE_SOCKET must not have leading or trailing whitespace",
            ),
        ] {
            let error = config
                .resolve_transport(Some(socket_path.clone()))
                .expect_err("invalid CUE_SOCKET override should fail before connect");

            assert!(
                format!("{error:#}").contains(expected),
                "wrong error for override {socket_path:?}: {error:#}"
            );
        }
    }

    #[test]
    fn socket_override_uses_explicit_unix_transport() {
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

        let transport = config
            .resolve_transport(Some(PathBuf::from("/tmp/cue-override.sock")))
            .expect("resolve CUE_SOCKET override");

        assert_eq!(
            transport,
            ResolvedTransport::Unix {
                profile_name: "env:CUE_SOCKET".into(),
                socket_path: PathBuf::from("/tmp/cue-override.sock"),
            }
        );
    }

    #[test]
    fn client_config_source_loads_transport_profiles() {
        let config = load_transport_config_from_sources(Some((
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
        )))
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
    fn transport_config_rejects_unknown_transport_fields() {
        let error = load_transport_config_from_sources(Some((
            Path::new("client.toml"),
            r#"
[transport]
default_profile = "local"
profile_default = "remote"
"#,
        )))
        .expect_err("unknown transport keys should fail during config loading");

        let message = format!("{error:#}");
        assert!(message.contains("parse config client.toml"));
        assert!(message.contains("unknown field `profile_default`"));
    }

    #[test]
    fn transport_config_rejects_unknown_profile_fields() {
        let error = load_transport_config_from_sources(Some((
            Path::new("client.toml"),
            r#"
[transport.profiles.remote]
transport = "ssh"
destination = "devbox"
gateway_cmd = "cued gateway --stdio"
"#,
        )))
        .expect_err("unknown profile keys should fail during config loading");

        let message = format!("{error:#}");
        assert!(message.contains("parse config client.toml"));
        assert!(message.contains("unknown field `gateway_cmd`"));
    }

    #[test]
    fn transport_config_rejects_unknown_top_level_sections() {
        let error = load_transport_config_from_sources(Some((
            Path::new("client.toml"),
            r#"
[transprt]
default_profile = "remote"
"#,
        )))
        .expect_err("top-level client config typos should fail before defaults apply");

        let message = format!("{error:#}");
        assert!(message.contains("unknown top-level client config section `transprt`"));
        assert!(message.contains("client.toml"));
        assert!(message.contains("transport"));
        assert!(message.contains("extensions"));
    }

    #[test]
    fn client_toml_rejects_daemon_sections() {
        let error = load_transport_config_from_sources(Some((
            Path::new("client.toml"),
            r#"
[weft]
socket_path = "/tmp/cue.sock"
"#,
        )))
        .expect_err("daemon sections belong in daemon.toml, not client.toml");

        assert!(format!("{error:#}").contains("unknown top-level client config section `weft`"));
    }

    #[test]
    fn ssh_profile_defaults_remote_commands() {
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
    fn detected_ssh_hosts_extend_profiles_without_removing_local() {
        let config = load_transport_config_from_sources(Some((
            Path::new("client.toml"),
            r#"
[transport]
default_profile = "remote"

[transport.profiles.remote]
transport = "ssh"
destination = "configured-devbox"
"#,
        )))
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
        let config = load_transport_config_from_sources(Some((
            Path::new("client.toml"),
            r#"
[transport]
default_profile = "devbox"
"#,
        )))
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
    fn local_default_profile_does_not_read_ssh_config_detection() {
        let config = load_transport_config_from_sources(Some((
            Path::new("client.toml"),
            r#"
[transport]
default_profile = "local"
"#,
        )))
        .expect("load config");

        let transport = config
            .resolve_transport_with_detection(None, || {
                panic!("local profile should resolve without SSH host auto-detection")
            })
            .expect("resolve local transport");

        assert_eq!(
            transport,
            ResolvedTransport::Unix {
                profile_name: "local".into(),
                socket_path: default_socket_path(),
            }
        );
    }

    #[test]
    fn missing_auto_detected_profile_surfaces_ssh_config_error() {
        let config = load_transport_config_from_sources(Some((
            Path::new("client.toml"),
            r#"
[transport]
default_profile = "devbox"
"#,
        )))
        .expect("load config");

        let error = config
            .resolve_transport_with_detection(None, || {
                Err(anyhow::anyhow!("cannot read /home/me/.ssh/config"))
            })
            .expect_err("missing auto-detected profile should include detection failures");
        let message = format!("{error:#}");

        assert!(
            message.contains(
                "auto-detect SSH profiles while resolving unknown client transport profile `devbox`"
            ),
            "{message}"
        );
        assert!(
            message.contains("cannot read /home/me/.ssh/config"),
            "{message}"
        );
    }

    #[test]
    fn disabled_auto_detect_does_not_read_ssh_config_for_missing_profile() {
        let config = load_transport_config_from_sources(Some((
            Path::new("client.toml"),
            r#"
[transport]
default_profile = "devbox"
auto_detect_ssh = false
"#,
        )))
        .expect("load config");

        let error = config
            .resolve_transport_with_detection(None, || {
                panic!("disabled SSH auto-detection should not read SSH config")
            })
            .expect_err("missing profile should stay a profile error");

        assert_eq!(
            format!("{error:#}"),
            "unknown client transport profile `devbox`"
        );
    }

    #[test]
    fn default_profile_rejects_empty_blank_or_padded_values() {
        for (default_profile, expected) in [
            (r#""""#, "transport.default_profile must not be empty"),
            (r#""   ""#, "transport.default_profile must not be empty"),
            (
                r#"" remote""#,
                "transport.default_profile must not have leading or trailing whitespace",
            ),
            (
                r#""remote ""#,
                "transport.default_profile must not have leading or trailing whitespace",
            ),
        ] {
            let error = load_transport_config_from_sources(Some((
                Path::new("client.toml"),
                &format!(
                    r#"
[transport]
default_profile = {default_profile}
"#
                ),
            )))
            .expect_err("explicitly empty default_profile should fail config loading");

            assert!(format!("{error:#}").contains(expected));
        }
    }

    #[test]
    fn profile_names_reject_empty_blank_or_padded_values() {
        for (profile_name, expected) in [
            (r#""""#, "transport profile names must not be empty"),
            (r#""   ""#, "transport profile names must not be empty"),
            (
                r#"" remote""#,
                "transport profile names must not have leading or trailing whitespace",
            ),
            (
                r#""remote ""#,
                "transport profile names must not have leading or trailing whitespace",
            ),
        ] {
            let error = load_transport_config_from_sources(Some((
                Path::new("client.toml"),
                &format!(
                    r#"
[transport.profiles.{profile_name}]
transport = "unix"
"#
                ),
            )))
            .expect_err("explicitly empty profile name should fail config loading");

            assert!(format!("{error:#}").contains(expected));
        }
    }

    #[test]
    fn auto_detect_ssh_can_be_disabled() {
        let config = load_transport_config_from_sources(Some((
            Path::new("client.toml"),
            r#"
[transport]
auto_detect_ssh = false
"#,
        )))
        .expect("load config");

        let profiles = config.merged_profiles(&BTreeSet::from(["devbox".into()]));

        assert!(profiles.contains_key("local"));
        assert!(!profiles.contains_key("devbox"));
    }

    #[test]
    fn local_profile_rejects_non_unix_transport() {
        let error = load_transport_config_from_sources(Some((
            Path::new("client.toml"),
            r#"
[transport.profiles.local]
transport = "ssh"
destination = "devbox"
"#,
        )))
        .expect_err("local profile is reserved for the built-in unix transport");

        assert!(format!("{error:#}").contains("transport.profiles.local is reserved"));
    }

    #[test]
    fn unix_profile_rejects_empty_or_padded_socket() {
        for (socket, expected) in [
            (
                r#""""#,
                "transport.profiles.remote.socket must not be empty",
            ),
            (
                r#""   ""#,
                "transport.profiles.remote.socket must not be empty",
            ),
            (
                r#"" /tmp/cue.sock""#,
                "transport.profiles.remote.socket must not have leading or trailing whitespace",
            ),
            (
                r#""/tmp/cue.sock ""#,
                "transport.profiles.remote.socket must not have leading or trailing whitespace",
            ),
        ] {
            let error = load_transport_config_from_sources(Some((
                Path::new("client.toml"),
                &format!(
                    r#"
[transport.profiles.remote]
transport = "unix"
socket = {socket}
"#
                ),
            )))
            .expect_err("invalid unix socket should fail config loading");

            assert!(
                format!("{error:#}").contains(expected),
                "wrong error for socket {socket}: {error:#}"
            );
        }
    }

    #[test]
    fn ssh_profile_rejects_empty_connection_fields() {
        for (field, config) in [
            (
                "destination",
                r#"
[transport.profiles.remote]
transport = "ssh"
destination = " "
"#,
            ),
            (
                "gateway_command",
                r#"
[transport.profiles.remote]
transport = "ssh"
destination = "devbox"
gateway_command = " "
"#,
            ),
            (
                "start_command",
                r#"
[transport.profiles.remote]
transport = "ssh"
destination = "devbox"
start_command = " "
"#,
            ),
        ] {
            let error =
                load_transport_config_from_sources(Some((Path::new("client.toml"), config)))
                    .expect_err("empty SSH profile field should fail config loading");

            assert!(
                format!("{error:#}").contains(&format!(
                    "transport.profiles.remote.{field} must not be empty"
                )),
                "wrong error for {field}: {error:#}"
            );
        }
    }

    #[test]
    fn ssh_profile_rejects_padded_connection_fields() {
        for (field, config) in [
            (
                "destination",
                r#"
[transport.profiles.remote]
transport = "ssh"
destination = " devbox"
"#,
            ),
            (
                "gateway_command",
                r#"
[transport.profiles.remote]
transport = "ssh"
destination = "devbox"
gateway_command = "cued gateway --stdio "
"#,
            ),
            (
                "start_command",
                r#"
[transport.profiles.remote]
transport = "ssh"
destination = "devbox"
start_command = " cued start"
"#,
            ),
        ] {
            let error =
                load_transport_config_from_sources(Some((Path::new("client.toml"), config)))
                    .expect_err("padded SSH profile field should fail config loading");

            assert!(
                format!("{error:#}").contains(&format!(
                    "transport.profiles.remote.{field} must not have leading or trailing whitespace"
                )),
                "wrong error for {field}: {error:#}"
            );
        }
    }
}
