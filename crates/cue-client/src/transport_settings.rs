use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use toml::Value;
use toml::map::Map;

use crate::client::default_socket_path;
use crate::config_paths::{client_config_paths, read_client_config_sources, read_config_source};
use crate::ssh_config::detected_ssh_hosts;
use crate::transport_config::{
    default_gateway_command, default_profile_name, default_start_command,
    validate_default_profile_name, validate_profile_name,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportProfileSummary {
    pub name: String,
    pub transport: String,
    pub detail: String,
    pub source: TransportProfileSource,
}

impl TransportProfileSummary {
    pub fn is_usable_target(&self) -> bool {
        self.source != TransportProfileSource::Missing
            && matches!(self.transport.as_str(), "unix" | "ssh")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportProfileSource {
    Local,
    Configured,
    AutoDetectedSsh,
    Missing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportSettingsSnapshot {
    pub source_path: PathBuf,
    pub auto_detect_ssh: bool,
    pub default_profile: String,
    pub profiles: Vec<TransportProfileSummary>,
}

impl TransportSettingsSnapshot {
    pub fn contains_profile(&self, profile_name: &str) -> bool {
        self.profiles
            .iter()
            .any(|profile| profile.name == profile_name)
    }
}

pub fn load_transport_settings_snapshot() -> Result<TransportSettingsSnapshot> {
    let paths = client_config_paths()?;
    let sources = read_client_config_sources(&paths)?;
    let detected_hosts = detected_ssh_hosts_for_snapshot();

    if let Some(source) = sources.client() {
        return parse_transport_snapshot(source.path(), source.text(), &detected_hosts);
    }

    Ok(TransportSettingsSnapshot {
        source_path: paths.client().to_path_buf(),
        auto_detect_ssh: true,
        default_profile: default_profile_name(),
        profiles: merged_profile_summaries(None, &detected_hosts),
    })
}

fn parse_transport_snapshot(
    path: &Path,
    text: &str,
    detected_hosts: &BTreeSet<String>,
) -> Result<TransportSettingsSnapshot> {
    let document: Value =
        toml::from_str(text).with_context(|| format!("parse config {}", path.display()))?;
    snapshot_from_value(path.to_path_buf(), &document, detected_hosts)
        .with_context(|| format!("parse transport settings {}", path.display()))
}

pub fn save_default_transport_profile(
    profile_name: &str,
    known_snapshot: &TransportSettingsSnapshot,
) -> Result<TransportSettingsSnapshot> {
    validate_default_profile_name(profile_name)?;

    let Some(profile) = known_snapshot
        .profiles
        .iter()
        .find(|profile| profile.name == profile_name)
    else {
        bail!("unknown target profile `{profile_name}`");
    };
    if !profile.is_usable_target() {
        bail!(
            "target profile `{profile_name}` is not usable: {}",
            profile.detail
        );
    }

    let write_path = transport_settings_write_path(known_snapshot)?;
    let mut document = match read_config_source(&write_path)? {
        Some(text) => toml::from_str::<Value>(&text)
            .with_context(|| format!("parse config {}", write_path.display()))?,
        None => Value::Table(Map::new()),
    };
    update_default_profile(&mut document, profile_name)?;

    let serialized =
        toml::to_string_pretty(&document).context("serialize target settings document")?;
    if let Some(parent) = write_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create config dir {}", parent.display()))?;
    }
    std::fs::write(&write_path, serialized)
        .with_context(|| format!("write config {}", write_path.display()))?;

    let text = std::fs::read_to_string(&write_path)
        .with_context(|| format!("read config {}", write_path.display()))?;
    parse_transport_snapshot(&write_path, &text, &detected_ssh_hosts_for_snapshot())
}

fn detected_ssh_hosts_for_snapshot() -> BTreeSet<String> {
    match detected_ssh_hosts() {
        Ok(hosts) => hosts,
        Err(error) => {
            tracing::warn!(%error, "failed to auto-detect SSH transport profiles");
            BTreeSet::new()
        }
    }
}

fn snapshot_from_value(
    source_path: PathBuf,
    document: &Value,
    detected_hosts: &BTreeSet<String>,
) -> Result<TransportSettingsSnapshot> {
    let auto_detect_ssh = transport_auto_detect_ssh(document)?;
    let default_profile = transport_default_profile(document)?;

    let empty_detected = BTreeSet::new();
    let mut profiles = merged_profile_summaries(
        transport_profiles_table(document)?,
        if auto_detect_ssh {
            detected_hosts
        } else {
            &empty_detected
        },
    );

    if !profiles
        .iter()
        .any(|profile| profile.name == default_profile)
    {
        profiles.push(TransportProfileSummary {
            name: default_profile.clone(),
            transport: "missing".into(),
            detail: "profile is referenced by default_profile but not defined".into(),
            source: TransportProfileSource::Missing,
        });
        profiles.sort_by(|left, right| {
            left.name
                .cmp(&right.name)
                .then_with(|| left.transport.cmp(&right.transport))
        });
    }

    Ok(TransportSettingsSnapshot {
        source_path,
        auto_detect_ssh,
        default_profile,
        profiles,
    })
}

fn merged_profile_summaries(
    profiles: Option<&Map<String, Value>>,
    detected_hosts: &BTreeSet<String>,
) -> Vec<TransportProfileSummary> {
    let mut summaries = BTreeMap::new();
    summaries.insert(
        "local".to_string(),
        summarize_local_profile(profiles.and_then(|profiles| profiles.get("local"))),
    );

    if let Some(profiles) = profiles {
        for (name, profile) in profiles {
            if name == "local" {
                continue;
            }
            summaries.insert(name.clone(), summarize_profile(name, profile));
        }
    }

    for host in detected_hosts {
        if host == "local" {
            continue;
        }
        summaries
            .entry(host.clone())
            .or_insert_with(|| TransportProfileSummary {
                name: host.clone(),
                transport: "ssh".into(),
                detail: format!("{host} | {}", default_gateway_command()),
                source: TransportProfileSource::AutoDetectedSsh,
            });
    }

    let mut profiles = Vec::with_capacity(summaries.len());
    if let Some(local) = summaries.remove("local") {
        profiles.push(local);
    }
    profiles.extend(summaries.into_values());
    profiles
}

fn summarize_profile(name: &str, profile: &Value) -> TransportProfileSummary {
    let Some(table) = profile.as_table() else {
        return TransportProfileSummary {
            name: name.to_string(),
            transport: "invalid".into(),
            detail: "profile value must be a TOML table".into(),
            source: TransportProfileSource::Configured,
        };
    };

    let Some(transport) = table.get("transport").and_then(Value::as_str) else {
        return TransportProfileSummary {
            name: name.to_string(),
            transport: "invalid".into(),
            detail: "profile is missing transport".into(),
            source: TransportProfileSource::Configured,
        };
    };

    match transport {
        "unix" => {
            let socket = match optional_socket_string(table) {
                Ok(socket) => socket,
                Err(detail) => return invalid_configured_profile(name, detail),
            };
            TransportProfileSummary {
                name: name.to_string(),
                transport: "unix".into(),
                detail: format!("socket: {socket}"),
                source: TransportProfileSource::Configured,
            }
        }
        "ssh" => {
            let destination = match required_non_empty_string(table, "destination") {
                Ok(destination) => destination,
                Err(detail) => return invalid_configured_profile(name, detail),
            };
            let gateway_command = match optional_non_empty_string(
                table,
                "gateway_command",
                default_gateway_command,
            ) {
                Ok(command) => command,
                Err(detail) => return invalid_configured_profile(name, detail),
            };
            if let Err(detail) =
                optional_non_empty_string(table, "start_command", default_start_command)
            {
                return invalid_configured_profile(name, detail);
            }
            TransportProfileSummary {
                name: name.to_string(),
                transport: "ssh".into(),
                detail: format!("{destination} | {gateway_command}"),
                source: TransportProfileSource::Configured,
            }
        }
        other => TransportProfileSummary {
            name: name.to_string(),
            transport: other.to_string(),
            detail: "unrecognized transport kind".into(),
            source: TransportProfileSource::Configured,
        },
    }
}

fn optional_socket_string(table: &Map<String, Value>) -> Result<String, String> {
    match table.get("socket") {
        Some(value) => socket_string(value).map(str::to_string),
        None => Ok(default_socket_path().display().to_string()),
    }
}

fn socket_string(value: &Value) -> Result<&str, String> {
    let Some(value) = value.as_str() else {
        return Err("unix profile socket must be a string".into());
    };
    if value.trim().is_empty() {
        return Err("unix profile socket is empty".into());
    }
    if value.trim() != value {
        return Err("unix profile socket must not have leading or trailing whitespace".into());
    }
    Ok(value)
}

fn invalid_configured_profile(name: &str, detail: impl Into<String>) -> TransportProfileSummary {
    TransportProfileSummary {
        name: name.to_string(),
        transport: "invalid".into(),
        detail: detail.into(),
        source: TransportProfileSource::Configured,
    }
}

fn required_non_empty_string<'a>(
    table: &'a Map<String, Value>,
    field: &'static str,
) -> Result<&'a str, String> {
    match table.get(field) {
        Some(value) => non_empty_string(value, field),
        None => Err(format!("ssh profile is missing {field}")),
    }
}

fn optional_non_empty_string(
    table: &Map<String, Value>,
    field: &'static str,
    default: impl FnOnce() -> String,
) -> Result<String, String> {
    match table.get(field) {
        Some(value) => non_empty_string(value, field).map(str::to_string),
        None => Ok(default()),
    }
}

fn non_empty_string<'a>(value: &'a Value, field: &'static str) -> Result<&'a str, String> {
    let Some(value) = value.as_str() else {
        return Err(format!("ssh profile {field} must be a string"));
    };
    if value.trim().is_empty() {
        return Err(format!("ssh profile {field} is empty"));
    }
    if value.trim() != value {
        return Err(format!(
            "ssh profile {field} must not have leading or trailing whitespace"
        ));
    }
    Ok(value)
}

fn summarize_local_profile(profile: Option<&Value>) -> TransportProfileSummary {
    let Some(profile) = profile else {
        return builtin_local_profile_summary();
    };

    let Some(table) = profile.as_table() else {
        return TransportProfileSummary {
            name: "local".into(),
            transport: "invalid".into(),
            detail: "profile value must be a TOML table".into(),
            source: TransportProfileSource::Configured,
        };
    };

    let Some(transport) = table.get("transport").and_then(Value::as_str) else {
        return TransportProfileSummary {
            name: "local".into(),
            transport: "invalid".into(),
            detail: "local profile is missing transport".into(),
            source: TransportProfileSource::Configured,
        };
    };

    if transport != "unix" {
        return TransportProfileSummary {
            name: "local".into(),
            transport: "invalid".into(),
            detail: "local profile is reserved for unix transport".into(),
            source: TransportProfileSource::Configured,
        };
    }

    let socket = match optional_socket_string(table) {
        Ok(socket) => socket,
        Err(detail) => return invalid_configured_profile("local", detail),
    };

    TransportProfileSummary {
        name: "local".into(),
        transport: "unix".into(),
        detail: format!("socket: {socket}"),
        source: TransportProfileSource::Local,
    }
}

fn builtin_local_profile_summary() -> TransportProfileSummary {
    TransportProfileSummary {
        name: "local".into(),
        transport: "unix".into(),
        detail: format!("socket: {}", default_socket_path().display()),
        source: TransportProfileSource::Local,
    }
}

fn transport_table(document: &Value) -> Option<&Map<String, Value>> {
    document.get("transport")?.as_table()
}

fn transport_auto_detect_ssh(document: &Value) -> Result<bool> {
    let Some(value) =
        transport_table(document).and_then(|transport| transport.get("auto_detect_ssh"))
    else {
        return Ok(true);
    };
    value
        .as_bool()
        .ok_or_else(|| anyhow::anyhow!("transport.auto_detect_ssh must be a boolean"))
}

fn transport_default_profile(document: &Value) -> Result<String> {
    let Some(value) =
        transport_table(document).and_then(|transport| transport.get("default_profile"))
    else {
        return Ok(default_profile_name());
    };
    let profile = value
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("transport.default_profile must be a string"))?;
    validate_default_profile_name(profile)?;
    Ok(profile.to_string())
}

fn transport_profiles_table(document: &Value) -> Result<Option<&Map<String, Value>>> {
    let Some(value) = transport_table(document).and_then(|transport| transport.get("profiles"))
    else {
        return Ok(None);
    };
    let Some(profiles) = value.as_table() else {
        bail!("transport.profiles must be a table");
    };
    validate_transport_profile_names(profiles)?;
    Ok(Some(profiles))
}

fn validate_transport_profile_names(profiles: &Map<String, Value>) -> Result<()> {
    for name in profiles.keys() {
        validate_profile_name(name)?;
    }
    Ok(())
}

fn update_default_profile(document: &mut Value, profile_name: &str) -> Result<()> {
    let root = document
        .as_table_mut()
        .ok_or_else(|| anyhow::anyhow!("config root must be a TOML table"))?;
    let transport = child_table_mut(root, "transport", "transport")?;
    {
        let profiles = child_table_mut(transport, "profiles", "transport.profiles")?;
        if profiles.is_empty() {
            profiles.insert("local".into(), default_local_profile_value());
        }
    }
    transport.insert(
        "default_profile".into(),
        Value::String(profile_name.to_string()),
    );
    Ok(())
}

fn child_table_mut<'a>(
    parent: &'a mut Map<String, Value>,
    key: &str,
    path: &str,
) -> Result<&'a mut Map<String, Value>> {
    if !parent.contains_key(key) {
        parent.insert(key.to_string(), Value::Table(Map::new()));
    }
    parent
        .get_mut(key)
        .expect("entry was inserted above when absent")
        .as_table_mut()
        .ok_or_else(|| anyhow::anyhow!("{path} must be a table"))
}

fn default_local_profile_value() -> Value {
    let mut profile = Map::new();
    profile.insert("transport".into(), Value::String("unix".into()));
    Value::Table(profile)
}

fn transport_settings_write_path(snapshot: &TransportSettingsSnapshot) -> Result<PathBuf> {
    Ok(snapshot.source_path.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_prefers_client_shape_and_summarizes_profiles() {
        let snapshot = parse_transport_snapshot(
            Path::new("client.toml"),
            r#"
[transport]
default_profile = "remote"

[transport.profiles.local]
transport = "unix"
socket = "/tmp/cue.sock"

[transport.profiles.remote]
transport = "ssh"
destination = "devbox"
gateway_command = "cued gateway --stdio --socket /tmp/remote.sock"
"#,
            &Default::default(),
        )
        .unwrap();

        assert_eq!(snapshot.default_profile, "remote");
        assert!(snapshot.auto_detect_ssh);
        assert_eq!(snapshot.profiles.len(), 2);
        assert_eq!(
            snapshot.profiles[0],
            TransportProfileSummary {
                name: "local".into(),
                transport: "unix".into(),
                detail: "socket: /tmp/cue.sock".into(),
                source: TransportProfileSource::Local,
            }
        );
        assert_eq!(
            snapshot.profiles[1],
            TransportProfileSummary {
                name: "remote".into(),
                transport: "ssh".into(),
                detail: "devbox | cued gateway --stdio --socket /tmp/remote.sock".into(),
                source: TransportProfileSource::Configured,
            }
        );
    }

    #[test]
    fn snapshot_surfaces_missing_default_profile() {
        let snapshot = parse_transport_snapshot(
            Path::new("client.toml"),
            r#"
[transport]
default_profile = "remote"

[transport.profiles.local]
transport = "unix"
"#,
            &Default::default(),
        )
        .unwrap();

        assert_eq!(snapshot.default_profile, "remote");
        assert!(
            snapshot
                .profiles
                .iter()
                .any(|profile| profile.name == "remote")
        );
        assert_eq!(
            snapshot
                .profiles
                .iter()
                .find(|profile| profile.name == "remote")
                .map(|profile| profile.transport.as_str()),
            Some("missing")
        );
    }

    #[test]
    fn ssh_profile_without_destination_is_invalid() {
        let snapshot = parse_transport_snapshot(
            Path::new("client.toml"),
            r#"
[transport]
default_profile = "remote"

[transport.profiles.remote]
transport = "ssh"
"#,
            &Default::default(),
        )
        .unwrap();

        assert_eq!(
            snapshot
                .profiles
                .iter()
                .find(|profile| profile.name == "remote")
                .map(|profile| (profile.transport.as_str(), profile.detail.as_str())),
            Some(("invalid", "ssh profile is missing destination"))
        );
    }

    #[test]
    fn unix_profile_with_invalid_socket_is_invalid() {
        for (socket, detail) in [
            (r#""""#, "unix profile socket is empty"),
            (r#""   ""#, "unix profile socket is empty"),
            (
                r#"" /tmp/cue.sock""#,
                "unix profile socket must not have leading or trailing whitespace",
            ),
            ("7", "unix profile socket must be a string"),
        ] {
            let snapshot = parse_transport_snapshot(
                Path::new("client.toml"),
                &format!(
                    r#"
[transport]
default_profile = "remote"

[transport.profiles.remote]
transport = "unix"
socket = {socket}
"#
                ),
                &Default::default(),
            )
            .unwrap();

            let profile = snapshot
                .profiles
                .iter()
                .find(|profile| profile.name == "remote")
                .expect("remote profile is summarized");
            assert_eq!(profile.transport, "invalid");
            assert_eq!(profile.detail, detail);
            assert!(!profile.is_usable_target());
        }
    }

    #[test]
    fn local_profile_with_invalid_socket_is_invalid() {
        let snapshot = parse_transport_snapshot(
            Path::new("client.toml"),
            r#"
[transport]
default_profile = "local"

[transport.profiles.local]
transport = "unix"
socket = " /tmp/cue.sock"
"#,
            &Default::default(),
        )
        .unwrap();

        assert_eq!(
            snapshot.profiles.first(),
            Some(&TransportProfileSummary {
                name: "local".into(),
                transport: "invalid".into(),
                detail: "unix profile socket must not have leading or trailing whitespace".into(),
                source: TransportProfileSource::Configured,
            })
        );
    }

    #[test]
    fn ssh_profile_with_empty_destination_is_invalid() {
        let snapshot = parse_transport_snapshot(
            Path::new("client.toml"),
            r#"
[transport]
default_profile = "remote"

[transport.profiles.remote]
transport = "ssh"
destination = " "
"#,
            &Default::default(),
        )
        .unwrap();

        let profile = snapshot
            .profiles
            .iter()
            .find(|profile| profile.name == "remote")
            .expect("remote profile is summarized");
        assert_eq!(profile.transport, "invalid");
        assert_eq!(profile.detail, "ssh profile destination is empty");
        assert!(!profile.is_usable_target());
    }

    #[test]
    fn ssh_profile_with_padded_connection_field_is_invalid() {
        for (field, field_line) in [
            ("destination", r#"destination = " devbox""#),
            (
                "gateway_command",
                r#"gateway_command = "cued gateway --stdio ""#,
            ),
            ("start_command", r#"start_command = " cued start""#),
        ] {
            let config = if field == "destination" {
                format!(
                    r#"
[transport]
default_profile = "remote"

[transport.profiles.remote]
transport = "ssh"
{field_line}
"#
                )
            } else {
                format!(
                    r#"
[transport]
default_profile = "remote"

[transport.profiles.remote]
transport = "ssh"
destination = "devbox"
{field_line}
"#
                )
            };

            let snapshot =
                parse_transport_snapshot(Path::new("client.toml"), &config, &Default::default())
                    .unwrap();

            let profile = snapshot
                .profiles
                .iter()
                .find(|profile| profile.name == "remote")
                .expect("remote profile is summarized");
            assert_eq!(profile.transport, "invalid");
            assert_eq!(
                profile.detail,
                format!("ssh profile {field} must not have leading or trailing whitespace")
            );
            assert!(!profile.is_usable_target());
        }
    }

    #[test]
    fn ssh_profile_with_invalid_command_field_is_invalid() {
        let snapshot = parse_transport_snapshot(
            Path::new("client.toml"),
            r#"
[transport]
default_profile = "remote"

[transport.profiles.remote]
transport = "ssh"
destination = "devbox"
gateway_command = 7
"#,
            &Default::default(),
        )
        .unwrap();

        let profile = snapshot
            .profiles
            .iter()
            .find(|profile| profile.name == "remote")
            .expect("remote profile is summarized");
        assert_eq!(profile.transport, "invalid");
        assert_eq!(
            profile.detail,
            "ssh profile gateway_command must be a string"
        );
        assert!(!profile.is_usable_target());
    }

    #[test]
    fn configured_profile_without_transport_is_invalid() {
        let snapshot = parse_transport_snapshot(
            Path::new("client.toml"),
            r#"
[transport]
default_profile = "remote"

[transport.profiles.remote]
destination = "devbox"
"#,
            &Default::default(),
        )
        .unwrap();

        assert_eq!(
            snapshot
                .profiles
                .iter()
                .find(|profile| profile.name == "remote")
                .map(|profile| (profile.transport.as_str(), profile.detail.as_str())),
            Some(("invalid", "profile is missing transport"))
        );
    }

    #[test]
    fn local_profile_without_transport_is_invalid() {
        let snapshot = parse_transport_snapshot(
            Path::new("client.toml"),
            r#"
[transport]
default_profile = "local"

[transport.profiles.local]
socket = "/tmp/ignored.sock"
"#,
            &Default::default(),
        )
        .unwrap();

        assert_eq!(
            snapshot.profiles.first(),
            Some(&TransportProfileSummary {
                name: "local".into(),
                transport: "invalid".into(),
                detail: "local profile is missing transport".into(),
                source: TransportProfileSource::Configured,
            })
        );
    }

    #[test]
    fn snapshot_adds_detected_ssh_hosts_and_keeps_local() {
        let snapshot = parse_transport_snapshot(
            Path::new("client.toml"),
            r#"
[transport]
default_profile = "local"

[transport.profiles.remote]
transport = "ssh"
destination = "configured-remote"
"#,
            &["devbox".to_string(), "remote".to_string()]
                .into_iter()
                .collect(),
        )
        .unwrap();

        assert!(
            snapshot
                .profiles
                .iter()
                .any(|profile| profile.name == "local"
                    && profile.transport == "unix"
                    && profile.source == TransportProfileSource::Local)
        );
        assert!(
            snapshot
                .profiles
                .iter()
                .any(|profile| profile.name == "devbox"
                    && profile.transport == "ssh"
                    && profile.source == TransportProfileSource::AutoDetectedSsh)
        );
        assert_eq!(
            snapshot
                .profiles
                .iter()
                .filter(|profile| profile.name == "remote")
                .count(),
            1
        );
    }

    #[test]
    fn local_profile_rejects_non_unix_config() {
        let snapshot = parse_transport_snapshot(
            Path::new("client.toml"),
            r#"
[transport]
default_profile = "local"

[transport.profiles.local]
transport = "ssh"
destination = "bad"
"#,
            &Default::default(),
        )
        .unwrap();

        assert_eq!(
            snapshot.profiles.first(),
            Some(&TransportProfileSummary {
                name: "local".into(),
                transport: "invalid".into(),
                detail: "local profile is reserved for unix transport".into(),
                source: TransportProfileSource::Configured,
            })
        );
    }

    #[test]
    fn snapshot_respects_disabled_auto_detect_ssh() {
        let snapshot = parse_transport_snapshot(
            Path::new("client.toml"),
            r#"
[transport]
auto_detect_ssh = false
"#,
            &["devbox".to_string()].into_iter().collect(),
        )
        .unwrap();

        assert!(!snapshot.auto_detect_ssh);
        assert!(
            snapshot
                .profiles
                .iter()
                .any(|profile| profile.name == "local")
        );
        assert!(
            !snapshot
                .profiles
                .iter()
                .any(|profile| profile.name == "devbox")
        );
    }

    #[test]
    fn snapshot_rejects_invalid_auto_detect_ssh_type() {
        let error = parse_transport_snapshot(
            Path::new("client.toml"),
            r#"
[transport]
auto_detect_ssh = "false"
"#,
            &Default::default(),
        )
        .expect_err("invalid auto_detect_ssh type should fail");

        assert!(format!("{error:#}").contains("transport.auto_detect_ssh must be a boolean"));
    }

    #[test]
    fn snapshot_rejects_invalid_default_profile_type() {
        let error = parse_transport_snapshot(
            Path::new("client.toml"),
            r#"
[transport]
default_profile = 7
"#,
            &Default::default(),
        )
        .expect_err("invalid default_profile type should fail");

        assert!(format!("{error:#}").contains("transport.default_profile must be a string"));
    }

    #[test]
    fn snapshot_rejects_empty_blank_or_padded_default_profile() {
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
            let error = parse_transport_snapshot(
                Path::new("client.toml"),
                &format!(
                    r#"
[transport]
default_profile = {default_profile}
"#
                ),
                &Default::default(),
            )
            .expect_err("explicitly empty default_profile should fail snapshot loading");

            assert!(format!("{error:#}").contains(expected));
        }
    }

    #[test]
    fn snapshot_rejects_empty_blank_or_padded_profile_names() {
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
            let error = parse_transport_snapshot(
                Path::new("client.toml"),
                &format!(
                    r#"
[transport.profiles.{profile_name}]
transport = "unix"
"#
                ),
                &Default::default(),
            )
            .expect_err("explicitly empty profile name should fail snapshot loading");

            assert!(format!("{error:#}").contains(expected));
        }
    }

    #[test]
    fn snapshot_rejects_invalid_profiles_shape() {
        let error = parse_transport_snapshot(
            Path::new("client.toml"),
            r#"
[transport]
profiles = "remote"
"#,
            &Default::default(),
        )
        .expect_err("invalid profiles shape should fail");

        assert!(format!("{error:#}").contains("transport.profiles must be a table"));
    }

    #[test]
    fn update_default_profile_preserves_other_sections() {
        let mut document: Value = toml::from_str(
            r#"
[transport]
default_profile = "local"

[transport.profiles.local]
transport = "unix"

[weft]
socket_path = "./weft.sock"
"#,
        )
        .unwrap();

        update_default_profile(&mut document, "remote").unwrap();

        assert_eq!(
            document
                .get("transport")
                .and_then(Value::as_table)
                .and_then(|transport| transport.get("default_profile"))
                .and_then(Value::as_str),
            Some("remote")
        );
        assert_eq!(
            document
                .get("weft")
                .and_then(Value::as_table)
                .and_then(|weft| weft.get("socket_path"))
                .and_then(Value::as_str),
            Some("./weft.sock")
        );
    }

    #[test]
    fn update_default_profile_seeds_local_profile_when_profiles_are_absent() {
        let mut document: Value = toml::from_str(
            r#"
[transport]
default_profile = "local"
"#,
        )
        .unwrap();

        update_default_profile(&mut document, "devbox").unwrap();

        assert_eq!(
            document
                .get("transport")
                .and_then(Value::as_table)
                .and_then(|transport| transport.get("profiles"))
                .and_then(Value::as_table)
                .and_then(|profiles| profiles.get("local"))
                .and_then(Value::as_table)
                .and_then(|local| local.get("transport"))
                .and_then(Value::as_str),
            Some("unix")
        );
    }

    #[test]
    fn update_default_profile_rejects_non_table_transport_without_rewriting() {
        let mut document: Value = toml::from_str(
            r#"
transport = "bad"
"#,
        )
        .unwrap();
        let original = document.clone();

        let error = update_default_profile(&mut document, "remote")
            .expect_err("non-table transport section should fail");

        assert!(format!("{error:#}").contains("transport must be a table"));
        assert_eq!(document, original);
    }

    #[test]
    fn update_default_profile_rejects_non_table_profiles_without_rewriting() {
        let mut document: Value = toml::from_str(
            r#"
[transport]
profiles = "bad"
"#,
        )
        .unwrap();
        let original = document.clone();

        let error = update_default_profile(&mut document, "remote")
            .expect_err("non-table transport.profiles section should fail");

        assert!(format!("{error:#}").contains("transport.profiles must be a table"));
        assert_eq!(document, original);
    }

    #[test]
    fn save_default_transport_profile_rejects_unknown_profile_before_writing() {
        let snapshot = TransportSettingsSnapshot {
            source_path: PathBuf::from("/tmp/client.toml"),
            auto_detect_ssh: true,
            default_profile: "local".into(),
            profiles: vec![TransportProfileSummary {
                name: "local".into(),
                transport: "unix".into(),
                detail: format!("socket: {}", default_socket_path().display()),
                source: TransportProfileSource::Local,
            }],
        };

        let error = save_default_transport_profile("missing", &snapshot)
            .expect_err("missing profile must be rejected before write");

        assert!(format!("{error:#}").contains("unknown target profile `missing`"));
    }

    #[test]
    fn save_default_transport_profile_rejects_invalid_profile_name_before_writing() {
        for (profile_name, expected) in [
            ("", "transport.default_profile must not be empty"),
            ("   ", "transport.default_profile must not be empty"),
            (
                " remote",
                "transport.default_profile must not have leading or trailing whitespace",
            ),
            (
                "remote ",
                "transport.default_profile must not have leading or trailing whitespace",
            ),
        ] {
            let snapshot = TransportSettingsSnapshot {
                source_path: PathBuf::from("/tmp/client.toml"),
                auto_detect_ssh: true,
                default_profile: "local".into(),
                profiles: vec![TransportProfileSummary {
                    name: profile_name.into(),
                    transport: "unix".into(),
                    detail: format!("socket: {}", default_socket_path().display()),
                    source: TransportProfileSource::Configured,
                }],
            };

            let error = save_default_transport_profile(profile_name, &snapshot)
                .expect_err("invalid profile name must be rejected before write");

            assert!(format!("{error:#}").contains(expected));
        }
    }

    #[test]
    fn save_default_transport_profile_rejects_unusable_profile() {
        let snapshot = TransportSettingsSnapshot {
            source_path: PathBuf::from("/tmp/client.toml"),
            auto_detect_ssh: true,
            default_profile: "local".into(),
            profiles: vec![TransportProfileSummary {
                name: "remote".into(),
                transport: "missing".into(),
                detail: "profile is referenced by default_profile but not defined".into(),
                source: TransportProfileSource::Missing,
            }],
        };

        let error = save_default_transport_profile("remote", &snapshot)
            .expect_err("unusable profile must be rejected before write");

        assert!(format!("{error:#}").contains("target profile `remote` is not usable"));
    }
}
