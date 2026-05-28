use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use toml::Value;
use toml::map::Map;

use crate::read_config_source;
use crate::transport_config::{default_gateway_command, default_profile_name};
use crate::{client_config_path, default_socket_path, detected_ssh_hosts, legacy_config_path};

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
    pub using_legacy_config: bool,
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
    let client_path = client_config_path();
    let legacy_path = legacy_config_path();
    let detected_hosts = detected_ssh_hosts();

    if let Some(text) = read_config_source(&client_path)? {
        return parse_transport_snapshot(&client_path, false, &text, &detected_hosts);
    }
    if let Some(text) = read_config_source(&legacy_path)? {
        return parse_transport_snapshot(&legacy_path, true, &text, &detected_hosts);
    }

    Ok(TransportSettingsSnapshot {
        source_path: client_path,
        using_legacy_config: false,
        auto_detect_ssh: true,
        default_profile: default_profile_name(),
        profiles: merged_profile_summaries(None, &detected_hosts),
    })
}

pub fn parse_transport_snapshot(
    path: &Path,
    using_legacy_config: bool,
    text: &str,
    detected_hosts: &BTreeSet<String>,
) -> Result<TransportSettingsSnapshot> {
    let document: Value =
        toml::from_str(text).with_context(|| format!("parse config {}", path.display()))?;
    Ok(snapshot_from_value(
        path.to_path_buf(),
        using_legacy_config,
        &document,
        detected_hosts,
    ))
}

pub fn save_default_transport_profile(
    profile_name: &str,
    known_snapshot: &TransportSettingsSnapshot,
) -> Result<TransportSettingsSnapshot> {
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

    let write_path = transport_settings_write_path(known_snapshot);
    let mut document = match read_config_source(&write_path)? {
        Some(text) => toml::from_str::<Value>(&text)
            .with_context(|| format!("parse config {}", write_path.display()))?,
        None => Value::Table(Map::new()),
    };
    update_default_profile(&mut document, profile_name);

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
    parse_transport_snapshot(&write_path, false, &text, &detected_ssh_hosts())
}

fn snapshot_from_value(
    source_path: PathBuf,
    using_legacy_config: bool,
    document: &Value,
    detected_hosts: &BTreeSet<String>,
) -> TransportSettingsSnapshot {
    let auto_detect_ssh = transport_auto_detect_ssh(document);
    let default_profile = transport_table(document)
        .and_then(|transport| transport.get("default_profile"))
        .and_then(Value::as_str)
        .filter(|profile| !profile.is_empty())
        .unwrap_or("local")
        .to_string();

    let empty_detected = BTreeSet::new();
    let mut profiles = merged_profile_summaries(
        transport_table(document)
            .and_then(|transport| transport.get("profiles"))
            .and_then(Value::as_table),
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

    TransportSettingsSnapshot {
        source_path,
        using_legacy_config,
        auto_detect_ssh,
        default_profile,
        profiles,
    }
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

    match table
        .get("transport")
        .and_then(Value::as_str)
        .unwrap_or("unix")
    {
        "unix" => TransportProfileSummary {
            name: name.to_string(),
            transport: "unix".into(),
            detail: format!(
                "socket: {}",
                table
                    .get("socket")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
                    .unwrap_or_else(|| default_socket_path().display().to_string())
            ),
            source: TransportProfileSource::Configured,
        },
        "ssh" => {
            let Some(destination) = table.get("destination").and_then(Value::as_str) else {
                return TransportProfileSummary {
                    name: name.to_string(),
                    transport: "invalid".into(),
                    detail: "ssh profile is missing destination".into(),
                    source: TransportProfileSource::Configured,
                };
            };
            let gateway_command = table
                .get("gateway_command")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(default_gateway_command);
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

fn summarize_local_profile(profile: Option<&Value>) -> TransportProfileSummary {
    let socket = profile
        .and_then(Value::as_table)
        .filter(|table| {
            table
                .get("transport")
                .and_then(Value::as_str)
                .unwrap_or("unix")
                == "unix"
        })
        .and_then(|table| table.get("socket"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .unwrap_or_else(|| default_socket_path().display().to_string());

    TransportProfileSummary {
        name: "local".into(),
        transport: "unix".into(),
        detail: format!("socket: {socket}"),
        source: TransportProfileSource::Local,
    }
}

fn transport_table(document: &Value) -> Option<&Map<String, Value>> {
    document.get("transport")?.as_table()
}

fn transport_auto_detect_ssh(document: &Value) -> bool {
    transport_table(document)
        .and_then(|transport| transport.get("auto_detect_ssh"))
        .and_then(Value::as_bool)
        .unwrap_or(true)
}

fn update_default_profile(document: &mut Value, profile_name: &str) {
    let root = ensure_table(document);
    let transport = ensure_child_table(root, "transport");
    transport.insert(
        "default_profile".into(),
        Value::String(profile_name.to_string()),
    );
    let profiles = ensure_child_table(transport, "profiles");
    if profiles.is_empty() {
        profiles.insert("local".into(), default_local_profile_value());
    }
}

fn ensure_table(value: &mut Value) -> &mut Map<String, Value> {
    if !value.is_table() {
        *value = Value::Table(Map::new());
    }
    value
        .as_table_mut()
        .expect("value was converted to a table above")
}

fn ensure_child_table<'a>(
    parent: &'a mut Map<String, Value>,
    key: &str,
) -> &'a mut Map<String, Value> {
    let entry = parent
        .entry(key.to_string())
        .or_insert_with(|| Value::Table(Map::new()));
    if !entry.is_table() {
        *entry = Value::Table(Map::new());
    }
    entry
        .as_table_mut()
        .expect("entry was converted to a table above")
}

fn default_local_profile_value() -> Value {
    let mut profile = Map::new();
    profile.insert("transport".into(), Value::String("unix".into()));
    Value::Table(profile)
}

fn transport_settings_write_path(snapshot: &TransportSettingsSnapshot) -> PathBuf {
    if snapshot.using_legacy_config {
        client_config_path()
    } else {
        snapshot.source_path.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_prefers_client_shape_and_summarizes_profiles() {
        let snapshot = parse_transport_snapshot(
            Path::new("client.toml"),
            false,
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
            false,
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
            false,
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
    fn snapshot_adds_detected_ssh_hosts_and_keeps_local() {
        let snapshot = parse_transport_snapshot(
            Path::new("client.toml"),
            false,
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
    fn local_profile_cannot_be_removed_by_non_unix_config() {
        let snapshot = parse_transport_snapshot(
            Path::new("client.toml"),
            false,
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
                transport: "unix".into(),
                detail: format!("socket: {}", default_socket_path().display()),
                source: TransportProfileSource::Local,
            })
        );
    }

    #[test]
    fn snapshot_respects_disabled_auto_detect_ssh() {
        let snapshot = parse_transport_snapshot(
            Path::new("client.toml"),
            false,
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

        update_default_profile(&mut document, "remote");

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

        update_default_profile(&mut document, "devbox");

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
    fn legacy_snapshot_writes_into_client_toml_path() {
        let snapshot = TransportSettingsSnapshot {
            source_path: PathBuf::from("/tmp/config.toml"),
            using_legacy_config: true,
            auto_detect_ssh: true,
            default_profile: "local".into(),
            profiles: vec![TransportProfileSummary {
                name: "local".into(),
                transport: "unix".into(),
                detail: format!("socket: {}", default_socket_path().display()),
                source: TransportProfileSource::Local,
            }],
        };

        assert_eq!(
            transport_settings_write_path(&snapshot),
            client_config_path()
        );
    }

    #[test]
    fn save_default_transport_profile_rejects_unknown_profile_before_writing() {
        let snapshot = TransportSettingsSnapshot {
            source_path: PathBuf::from("/tmp/client.toml"),
            using_legacy_config: false,
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
    fn save_default_transport_profile_rejects_unusable_profile() {
        let snapshot = TransportSettingsSnapshot {
            source_path: PathBuf::from("/tmp/client.toml"),
            using_legacy_config: false,
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
