use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use toml::Value;
use toml::map::Map;

use crate::client::default_socket_path;
use crate::config_paths::{client_config_paths, read_client_config_sources, read_config_source};
use crate::host_discovery::HostDiscoveryConfig;
use crate::transport_discovery::detected_transport_hosts;
use crate::transport_schema::{
    LOCAL_PROFILE_NAME, PROFILE_TRANSPORT_FIELD, SSH_DESTINATION_FIELD, SSH_GATEWAY_COMMAND_FIELD,
    SSH_PROFILE_KEYS, SSH_START_COMMAND_FIELD, SSH_TRANSPORT, TRANSPORT_AUTO_DETECT_SSH_FIELD,
    TRANSPORT_DEFAULT_PROFILE_FIELD, TRANSPORT_DISCOVERY_FIELD, TRANSPORT_KEYS,
    TRANSPORT_PROFILES_FIELD, TRANSPORT_SECTION, UNIX_PROFILE_KEYS, UNIX_SOCKET_FIELD,
    UNIX_TRANSPORT, default_auto_detect_ssh, default_gateway_command, default_profile_name,
    transport_field_path, transport_profiles_path, unknown_field_detail,
    validate_client_config_root_sections, validate_default_profile_name, validate_known_keys,
    validate_profile_name,
};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct TransportProfileSummary {
    pub name: String,
    pub transport: TransportProfileKind,
    pub detail: String,
    pub source: TransportProfileSource,
}

impl TransportProfileSummary {
    pub fn is_usable_target(&self) -> bool {
        self.transport.is_usable_target()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub enum TransportProfileKind {
    Unix,
    Ssh,
    Invalid,
    Missing,
    Unsupported(String),
}

impl TransportProfileKind {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Unix => UNIX_TRANSPORT,
            Self::Ssh => SSH_TRANSPORT,
            Self::Invalid => "invalid",
            Self::Missing => "missing",
            Self::Unsupported(kind) => kind.as_str(),
        }
    }

    fn is_usable_target(&self) -> bool {
        matches!(self, Self::Unix | Self::Ssh)
    }
}

impl fmt::Display for TransportProfileKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum TransportProfileSource {
    Local,
    Configured,
    AutoDetectedSsh,
    Missing,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
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
    load_transport_settings_snapshot_from_sources(
        sources
            .primary()
            .map(|source| (source.path(), source.text())),
        paths.client(),
    )
}

pub fn load_transport_settings_snapshot_from_sources(
    source: Option<(&Path, &str)>,
    default_path: &Path,
) -> Result<TransportSettingsSnapshot> {
    if let Some((path, text)) = source {
        return parse_transport_snapshot_with_config_detection(path, text);
    }

    let detected_hosts = detected_transport_hosts_for_snapshot(&HostDiscoveryConfig::default());
    Ok(TransportSettingsSnapshot {
        source_path: default_path.to_path_buf(),
        auto_detect_ssh: default_auto_detect_ssh(),
        default_profile: default_profile_name(),
        profiles: merged_profile_summaries(None, &detected_hosts),
    })
}

pub fn parse_transport_snapshot(
    path: &Path,
    text: &str,
    detected_hosts: &BTreeSet<String>,
) -> Result<TransportSettingsSnapshot> {
    validate_client_config_root_sections(text, path)?;
    let document: Value =
        toml::from_str(text).with_context(|| format!("parse config {}", path.display()))?;
    validate_transport_section(&document)?;
    snapshot_from_value(path.to_path_buf(), &document, detected_hosts)
        .with_context(|| format!("parse transport settings {}", path.display()))
}

fn parse_transport_snapshot_with_config_detection(
    path: &Path,
    text: &str,
) -> Result<TransportSettingsSnapshot> {
    validate_client_config_root_sections(text, path)?;
    let document: Value =
        toml::from_str(text).with_context(|| format!("parse config {}", path.display()))?;
    validate_transport_section(&document)?;
    let discovery = transport_discovery_config(&document)?;
    let detected_hosts = detected_transport_hosts_for_snapshot(&discovery);
    snapshot_from_value(path.to_path_buf(), &document, &detected_hosts)
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

    let write_path = known_snapshot.source_path.clone();
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
    parse_transport_snapshot_with_config_detection(&write_path, &text)
}

fn detected_transport_hosts_for_snapshot(discovery: &HostDiscoveryConfig) -> BTreeSet<String> {
    match detected_transport_hosts(discovery) {
        Ok(hosts) => hosts,
        Err(error) => {
            tracing::warn!(%error, "failed to auto-detect transport profiles");
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
            transport: TransportProfileKind::Missing,
            detail: "profile is referenced by default_profile but not defined".into(),
            source: TransportProfileSource::Missing,
        });
        profiles.sort_by(|left, right| {
            left.name
                .cmp(&right.name)
                .then_with(|| left.transport.as_str().cmp(right.transport.as_str()))
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
        LOCAL_PROFILE_NAME.to_string(),
        summarize_local_profile(profiles.and_then(|profiles| profiles.get(LOCAL_PROFILE_NAME))),
    );

    if let Some(profiles) = profiles {
        for (name, profile) in profiles {
            if name == LOCAL_PROFILE_NAME {
                continue;
            }
            summaries.insert(name.clone(), summarize_profile(name, profile));
        }
    }

    for host in detected_hosts {
        if host == LOCAL_PROFILE_NAME {
            continue;
        }
        summaries
            .entry(host.clone())
            .or_insert_with(|| auto_detected_ssh_profile_summary(host));
    }

    let mut profiles = Vec::with_capacity(summaries.len());
    if let Some(local) = summaries.remove(LOCAL_PROFILE_NAME) {
        profiles.push(local);
    }
    profiles.extend(summaries.into_values());
    profiles
}

fn auto_detected_ssh_profile_summary(host: &str) -> TransportProfileSummary {
    TransportProfileSummary {
        name: host.to_string(),
        transport: TransportProfileKind::Ssh,
        detail: format!("{host} | {}", default_gateway_command()),
        source: TransportProfileSource::AutoDetectedSsh,
    }
}

fn summarize_profile(name: &str, profile: &Value) -> TransportProfileSummary {
    let Some(table) = profile.as_table() else {
        return TransportProfileSummary {
            name: name.to_string(),
            transport: TransportProfileKind::Invalid,
            detail: "profile value must be a TOML table".into(),
            source: TransportProfileSource::Configured,
        };
    };

    let transport = match profile_transport_string(table, ProfileTransportMessage::Configured) {
        Ok(transport) => transport,
        Err(detail) => {
            return invalid_configured_profile(name, detail);
        }
    };

    match transport {
        UNIX_TRANSPORT => {
            if let Some(detail) = unknown_field_detail(table, UNIX_PROFILE_KEYS) {
                return invalid_configured_profile(name, detail);
            }
            configured_unix_profile_summary(name, table, TransportProfileSource::Configured)
        }
        SSH_TRANSPORT => {
            if let Some(detail) = unknown_field_detail(table, SSH_PROFILE_KEYS) {
                return invalid_configured_profile(name, detail);
            }
            configured_ssh_profile_summary(name, table)
        }
        other => TransportProfileSummary {
            name: name.to_string(),
            transport: TransportProfileKind::Unsupported(other.to_string()),
            detail: "unrecognized transport kind".into(),
            source: TransportProfileSource::Configured,
        },
    }
}

fn configured_ssh_profile_summary(
    name: &str,
    table: &Map<String, Value>,
) -> TransportProfileSummary {
    let destination = match required_non_empty_string(table, SSH_DESTINATION_FIELD) {
        Ok(destination) => destination,
        Err(detail) => return invalid_configured_profile(name, detail),
    };
    let gateway_command = match optional_non_empty_string(
        table,
        SSH_GATEWAY_COMMAND_FIELD,
        default_gateway_command,
    ) {
        Ok(command) => command,
        Err(detail) => return invalid_configured_profile(name, detail),
    };
    if let Err(detail) = validate_optional_non_empty_string(table, SSH_START_COMMAND_FIELD) {
        return invalid_configured_profile(name, detail);
    }
    TransportProfileSummary {
        name: name.to_string(),
        transport: TransportProfileKind::Ssh,
        detail: format!("{destination} | {gateway_command}"),
        source: TransportProfileSource::Configured,
    }
}

fn optional_socket_string(table: &Map<String, Value>) -> Result<String, String> {
    match table.get(UNIX_SOCKET_FIELD) {
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
        transport: TransportProfileKind::Invalid,
        detail: detail.into(),
        source: TransportProfileSource::Configured,
    }
}

enum ProfileTransportMessage {
    Configured,
    Local,
}

impl ProfileTransportMessage {
    fn missing_detail(&self) -> &'static str {
        match self {
            Self::Configured => "profile is missing transport",
            Self::Local => "local profile is missing transport",
        }
    }

    fn wrong_type_detail(&self) -> &'static str {
        match self {
            Self::Configured => "profile transport must be a string",
            Self::Local => "local profile transport must be a string",
        }
    }
}

fn profile_transport_string(
    table: &Map<String, Value>,
    messages: ProfileTransportMessage,
) -> Result<&str, String> {
    match table.get(PROFILE_TRANSPORT_FIELD) {
        Some(value) => value
            .as_str()
            .ok_or_else(|| messages.wrong_type_detail().to_string()),
        None => Err(messages.missing_detail().to_string()),
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

fn validate_optional_non_empty_string(
    table: &Map<String, Value>,
    field: &'static str,
) -> Result<(), String> {
    if let Some(value) = table.get(field) {
        non_empty_string(value, field)?;
    }
    Ok(())
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
            name: LOCAL_PROFILE_NAME.into(),
            transport: TransportProfileKind::Invalid,
            detail: "profile value must be a TOML table".into(),
            source: TransportProfileSource::Configured,
        };
    };

    let transport = match profile_transport_string(table, ProfileTransportMessage::Local) {
        Ok(transport) => transport,
        Err(detail) => {
            return invalid_configured_profile(LOCAL_PROFILE_NAME, detail);
        }
    };

    if transport != UNIX_TRANSPORT {
        return TransportProfileSummary {
            name: LOCAL_PROFILE_NAME.into(),
            transport: TransportProfileKind::Invalid,
            detail: "local profile is reserved for unix transport".into(),
            source: TransportProfileSource::Configured,
        };
    }

    if let Some(detail) = unknown_field_detail(table, UNIX_PROFILE_KEYS) {
        return invalid_configured_profile(LOCAL_PROFILE_NAME, detail);
    }

    configured_unix_profile_summary(LOCAL_PROFILE_NAME, table, TransportProfileSource::Local)
}

fn configured_unix_profile_summary(
    name: &str,
    table: &Map<String, Value>,
    source: TransportProfileSource,
) -> TransportProfileSummary {
    let socket = match optional_socket_string(table) {
        Ok(socket) => socket,
        Err(detail) => return invalid_configured_profile(name, detail),
    };

    TransportProfileSummary {
        name: name.into(),
        transport: TransportProfileKind::Unix,
        detail: format!("socket: {socket}"),
        source,
    }
}

fn builtin_local_profile_summary() -> TransportProfileSummary {
    TransportProfileSummary {
        name: LOCAL_PROFILE_NAME.into(),
        transport: TransportProfileKind::Unix,
        detail: format!("socket: {}", default_socket_path().display()),
        source: TransportProfileSource::Local,
    }
}

fn transport_table(document: &Value) -> Option<&Map<String, Value>> {
    document.get(TRANSPORT_SECTION)?.as_table()
}

fn validate_transport_section(document: &Value) -> Result<()> {
    let Some(value) = document.get(TRANSPORT_SECTION) else {
        return Ok(());
    };
    let Some(transport) = value.as_table() else {
        bail!("transport must be a table");
    };
    validate_known_keys(transport, TRANSPORT_SECTION, TRANSPORT_KEYS)
}

fn transport_auto_detect_ssh(document: &Value) -> Result<bool> {
    let Some(value) = transport_table(document)
        .and_then(|transport| transport.get(TRANSPORT_AUTO_DETECT_SSH_FIELD))
    else {
        return Ok(default_auto_detect_ssh());
    };
    value.as_bool().ok_or_else(|| {
        anyhow::anyhow!(
            "{} must be a boolean",
            transport_field_path(TRANSPORT_AUTO_DETECT_SSH_FIELD)
        )
    })
}

fn transport_discovery_config(document: &Value) -> Result<HostDiscoveryConfig> {
    let Some(value) =
        transport_table(document).and_then(|transport| transport.get(TRANSPORT_DISCOVERY_FIELD))
    else {
        return Ok(HostDiscoveryConfig::default());
    };
    if !value.is_table() {
        bail!(
            "{} must be a table",
            transport_field_path(TRANSPORT_DISCOVERY_FIELD)
        );
    }
    value
        .clone()
        .try_into()
        .with_context(|| format!("parse {}", transport_field_path(TRANSPORT_DISCOVERY_FIELD)))
}

fn transport_default_profile(document: &Value) -> Result<String> {
    let Some(value) = transport_table(document)
        .and_then(|transport| transport.get(TRANSPORT_DEFAULT_PROFILE_FIELD))
    else {
        return Ok(default_profile_name());
    };
    let profile = value.as_str().ok_or_else(|| {
        anyhow::anyhow!(
            "{} must be a string",
            transport_field_path(TRANSPORT_DEFAULT_PROFILE_FIELD)
        )
    })?;
    validate_default_profile_name(profile)?;
    Ok(profile.to_string())
}

fn transport_profiles_table(document: &Value) -> Result<Option<&Map<String, Value>>> {
    let Some(value) =
        transport_table(document).and_then(|transport| transport.get(TRANSPORT_PROFILES_FIELD))
    else {
        return Ok(None);
    };
    let Some(profiles) = value.as_table() else {
        bail!("{} must be a table", transport_profiles_path());
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
    let transport = child_table_mut(root, TRANSPORT_SECTION, TRANSPORT_SECTION)?;
    {
        let profiles = child_table_mut(
            transport,
            TRANSPORT_PROFILES_FIELD,
            &transport_profiles_path(),
        )?;
        if profiles.is_empty() {
            profiles.insert(LOCAL_PROFILE_NAME.into(), default_local_profile_value());
        }
    }
    transport.insert(
        TRANSPORT_DEFAULT_PROFILE_FIELD.into(),
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
    profile.insert(
        PROFILE_TRANSPORT_FIELD.into(),
        Value::String(UNIX_TRANSPORT.into()),
    );
    Value::Table(profile)
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
                transport: TransportProfileKind::Unix,
                detail: "socket: /tmp/cue.sock".into(),
                source: TransportProfileSource::Local,
            }
        );
        assert_eq!(
            snapshot.profiles[1],
            TransportProfileSummary {
                name: "remote".into(),
                transport: TransportProfileKind::Ssh,
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
            assert_eq!(profile.transport, TransportProfileKind::Invalid);
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
                transport: TransportProfileKind::Invalid,
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
        assert_eq!(profile.transport, TransportProfileKind::Invalid);
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
            assert_eq!(profile.transport, TransportProfileKind::Invalid);
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
        assert_eq!(profile.transport, TransportProfileKind::Invalid);
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
    fn configured_profile_with_non_string_transport_is_invalid() {
        let snapshot = parse_transport_snapshot(
            Path::new("client.toml"),
            r#"
[transport]
default_profile = "remote"

[transport.profiles.remote]
transport = 42
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
            Some(("invalid", "profile transport must be a string"))
        );
    }

    #[test]
    fn configured_profile_with_unknown_field_is_invalid() {
        let snapshot = parse_transport_snapshot(
            Path::new("client.toml"),
            r#"
[transport]
default_profile = "remote"

[transport.profiles.remote]
transport = "ssh"
destination = "devbox"
extra_field = "typo"
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
            Some(("invalid", "unknown field `extra_field`"))
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
                transport: TransportProfileKind::Invalid,
                detail: "local profile is missing transport".into(),
                source: TransportProfileSource::Configured,
            })
        );
    }

    #[test]
    fn local_profile_with_non_string_transport_is_invalid() {
        let snapshot = parse_transport_snapshot(
            Path::new("client.toml"),
            r#"
[transport]
default_profile = "local"

[transport.profiles.local]
transport = false
socket = "/tmp/ignored.sock"
"#,
            &Default::default(),
        )
        .unwrap();

        assert_eq!(
            snapshot.profiles.first(),
            Some(&TransportProfileSummary {
                name: "local".into(),
                transport: TransportProfileKind::Invalid,
                detail: "local profile transport must be a string".into(),
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
                    && profile.transport == TransportProfileKind::Unix
                    && profile.source == TransportProfileSource::Local)
        );
        assert!(
            snapshot
                .profiles
                .iter()
                .any(|profile| profile.name == "devbox"
                    && profile.transport == TransportProfileKind::Ssh
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
                transport: TransportProfileKind::Invalid,
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
    fn snapshot_rejects_unknown_transport_field() {
        let error = parse_transport_snapshot(
            Path::new("client.toml"),
            r#"
[transport]
default_profiel = "remote"
"#,
            &Default::default(),
        )
        .expect_err("unknown transport fields should fail settings loading");

        assert!(format!("{error:#}").contains("unknown field `default_profiel` in transport"));
    }

    #[test]
    fn update_default_profile_preserves_other_sections() {
        let mut document: Value = toml::from_str(
            r#"
[transport]
default_profile = "local"

[transport.profiles.local]
transport = "unix"

[extensions]
path_lookup = true
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
                .get("extensions")
                .and_then(Value::as_table)
                .and_then(|extensions| extensions.get("path_lookup"))
                .and_then(Value::as_bool),
            Some(true)
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
                transport: TransportProfileKind::Unix,
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
                    transport: TransportProfileKind::Unix,
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
                transport: TransportProfileKind::Missing,
                detail: "profile is referenced by default_profile but not defined".into(),
                source: TransportProfileSource::Missing,
            }],
        };

        let error = save_default_transport_profile("remote", &snapshot)
            .expect_err("unusable profile must be rejected before write");

        assert!(format!("{error:#}").contains("target profile `remote` is not usable"));
    }
}
