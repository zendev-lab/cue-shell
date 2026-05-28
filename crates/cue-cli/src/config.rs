use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use cue_client::{
    ResolvedTransport, TransportConfig, client_config_path, legacy_config_path, read_config_source,
};
use serde::Deserialize;

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub transport: TransportConfig,
    #[serde(default)]
    pub extensions: ExtensionsConfig,
}

impl Config {
    pub fn load() -> Result<Self> {
        let client_path = client_config_path();
        let legacy_path = legacy_config_path();
        Self::load_from_sources(
            read_config_source(&client_path)?
                .as_deref()
                .map(|text| (client_path.as_path(), text)),
            read_config_source(&legacy_path)?
                .as_deref()
                .map(|text| (legacy_path.as_path(), text)),
        )
    }

    fn load_from_sources(
        client: Option<(&Path, &str)>,
        legacy: Option<(&Path, &str)>,
    ) -> Result<Self> {
        if let Some((path, text)) = client {
            return Self::parse(text, path);
        }
        if let Some((path, text)) = legacy {
            return Self::parse(text, path);
        }
        Ok(Self::default())
    }

    fn parse(text: &str, path: &Path) -> Result<Self> {
        let mut config: Self =
            toml::from_str(text).with_context(|| format!("parse config {}", path.display()))?;
        config.transport.normalize_defaults();
        Ok(config)
    }

    pub fn resolve_transport(&self, socket_override: Option<PathBuf>) -> Result<ResolvedTransport> {
        self.transport.resolve_transport(socket_override)
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ExtensionsConfig {
    #[serde(default)]
    pub path_lookup: bool,
    #[serde(default)]
    pub commands: BTreeMap<String, ExtensionCommand>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ExtensionCommand {
    pub command: String,
    #[serde(default)]
    pub description: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_external_extensions() {
        let config = Config::load_from_sources(
            Some((
                Path::new("client.toml"),
                r#"
[extensions]
path_lookup = true

[extensions.commands.foo]
command = "cue-foo"
description = "Foo extension"
"#,
            )),
            None,
        )
        .expect("load config");

        assert!(config.extensions.path_lookup);
        assert_eq!(
            config.extensions.commands.get("foo"),
            Some(&ExtensionCommand {
                command: "cue-foo".into(),
                description: Some("Foo extension".into()),
            })
        );
    }
}
