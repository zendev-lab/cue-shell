#[cfg(feature = "extensions")]
use std::collections::BTreeMap;
use std::path::Path;
#[cfg(any(feature = "script", feature = "tui"))]
use std::path::PathBuf;

#[cfg(feature = "extensions")]
use anyhow::bail;
use anyhow::{Context, Result};
#[cfg(any(feature = "script", feature = "tui"))]
use cue_client::ClientConfigSources;
#[cfg(any(feature = "script", feature = "tui"))]
use cue_client::ResolvedTransport;
#[cfg(any(feature = "script", feature = "tui"))]
use cue_client::TransportConfig;
#[cfg(any(feature = "script", feature = "tui"))]
use cue_client::client_config_paths;
#[cfg(feature = "extensions")]
use cue_client::optional_client_config_paths;
use cue_client::{
    ClientConfigPaths, read_client_config_sources, validate_client_config_root_sections,
};
use serde::Deserialize;

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Config {
    #[cfg(any(feature = "script", feature = "tui"))]
    #[serde(default)]
    pub transport: TransportConfig,
    #[cfg(feature = "extensions")]
    #[serde(default)]
    pub extensions: ExtensionsConfig,
}

impl Config {
    #[cfg(any(feature = "script", feature = "tui"))]
    pub fn load() -> Result<Self> {
        Self::load_from_paths(client_config_paths()?)
    }

    #[cfg(feature = "extensions")]
    pub(crate) fn load_for_extension_dispatch() -> Result<Self> {
        Self::load_for_extension_dispatch_from_paths(optional_client_config_paths())
    }

    #[cfg(feature = "extensions")]
    fn load_for_extension_dispatch_from_paths(paths: Option<ClientConfigPaths>) -> Result<Self> {
        let Some(paths) = paths else {
            return Ok(Self::default());
        };
        let sources = read_client_config_sources(&paths)?;
        Self::load_for_extension_dispatch_from_sources(
            sources
                .primary()
                .map(|source| (source.path(), source.text())),
        )
    }

    #[cfg(feature = "extensions")]
    fn load_for_extension_dispatch_from_sources(source: Option<(&Path, &str)>) -> Result<Self> {
        let Some((path, text)) = source else {
            return Ok(Self::default());
        };
        validate_client_config_root_sections(text, path)?;
        let extension_config: ExtensionDispatchConfig =
            toml::from_str(text).with_context(|| format!("parse config {}", path.display()))?;
        extension_config.extensions.validate()?;
        Ok(Self {
            #[cfg(any(feature = "script", feature = "tui"))]
            transport: TransportConfig::default(),
            extensions: extension_config.extensions,
        })
    }

    #[cfg(any(feature = "script", feature = "tui"))]
    fn load_from_paths(paths: ClientConfigPaths) -> Result<Self> {
        let sources = read_client_config_sources(&paths)?;
        Self::load_from_client_sources(&sources)
    }

    #[cfg(any(feature = "script", feature = "tui"))]
    fn load_from_client_sources(sources: &ClientConfigSources) -> Result<Self> {
        Self::load_from_sources(
            sources
                .primary()
                .map(|source| (source.path(), source.text())),
        )
    }

    #[cfg(any(feature = "script", feature = "tui"))]
    fn load_from_sources(source: Option<(&Path, &str)>) -> Result<Self> {
        if let Some((path, text)) = source {
            return Self::parse(text, path);
        }
        Ok(Self::default())
    }

    #[cfg(any(feature = "script", feature = "tui"))]
    fn parse(text: &str, path: &Path) -> Result<Self> {
        validate_client_config_root_sections(text, path)?;
        let config: Self =
            toml::from_str(text).with_context(|| format!("parse config {}", path.display()))?;
        #[cfg(any(feature = "script", feature = "tui"))]
        config.transport.validate()?;
        #[cfg(feature = "extensions")]
        config.extensions.validate()?;
        Ok(config)
    }

    #[cfg(any(feature = "script", feature = "tui"))]
    pub(crate) fn resolve_transport(
        &self,
        socket_override: Option<PathBuf>,
    ) -> Result<ResolvedTransport> {
        self.transport.resolve_transport(socket_override)
    }
}

#[cfg(feature = "extensions")]
#[derive(Debug, Clone, Default, Deserialize)]
struct ExtensionDispatchConfig {
    #[serde(default)]
    extensions: ExtensionsConfig,
}

#[cfg(feature = "extensions")]
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtensionsConfig {
    #[serde(default)]
    pub path_lookup: bool,
    #[serde(default)]
    pub commands: BTreeMap<String, ExtensionCommand>,
}

#[cfg(feature = "extensions")]
impl ExtensionsConfig {
    fn validate(&self) -> Result<()> {
        for (name, command) in &self.commands {
            if is_reserved_extension_name(name) {
                bail!(
                    "extension name `{name}` is reserved for a built-in or first-party cue subcommand"
                );
            }
            validate_extension_name(name, "extension name")?;
            if command.program.trim().is_empty() {
                bail!("extension `{name}` program must not be empty");
            }
            if command.program.trim() != command.program {
                bail!("extension `{name}` program must not have leading or trailing whitespace");
            }
        }
        Ok(())
    }
}

#[cfg(feature = "extensions")]
pub(crate) fn validate_extension_name(name: &str, label: &str) -> Result<()> {
    if !is_valid_extension_name(name) {
        bail!("{label} `{name}` must be kebab-case ASCII, for example `foo` or `foo-bar`");
    }
    Ok(())
}

#[cfg(feature = "extensions")]
fn is_reserved_extension_name(name: &str) -> bool {
    matches!(name, "help" | "run" | "tui" | "version")
}

#[cfg(feature = "extensions")]
fn is_valid_extension_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
        return false;
    }
    let mut previous_was_dash = false;

    for ch in chars {
        match ch {
            'a'..='z' | '0'..='9' => previous_was_dash = false,
            '-' if !previous_was_dash => previous_was_dash = true,
            _ => return false,
        }
    }

    !previous_was_dash
}

#[cfg(feature = "extensions")]
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExtensionCommand {
    pub program: String,
    #[serde(default)]
    pub description: Option<String>,
}

#[cfg(all(test, feature = "extensions"))]
mod tests {
    use super::*;

    #[test]
    fn parses_external_extensions() {
        let config = Config::load_for_extension_dispatch_from_sources(Some((
            Path::new("client.toml"),
            r#"
[extensions]
path_lookup = true

[extensions.commands.foo]
program = "cue-foo"
description = "Foo extension"
"#,
        )))
        .expect("load config");

        assert!(config.extensions.path_lookup);
        assert_eq!(
            config.extensions.commands.get("foo"),
            Some(&ExtensionCommand {
                program: "cue-foo".into(),
                description: Some("Foo extension".into()),
            })
        );
    }

    #[test]
    fn extension_registry_requires_program_field() {
        let error = Config::load_for_extension_dispatch_from_sources(Some((
            Path::new("client.toml"),
            r#"
[extensions.commands.foo]
command = "cue-foo"
"#,
        )))
        .expect_err("command field should not be accepted as a program");

        let message = format!("{error:#}");
        assert!(message.contains("parse config client.toml"));
        assert!(message.contains("unknown field `command`"));
    }

    #[test]
    fn extension_registry_rejects_unknown_extension_fields() {
        let error = Config::load_for_extension_dispatch_from_sources(Some((
            Path::new("client.toml"),
            r#"
[extensions]
path_lookkup = true
"#,
        )))
        .expect_err("unknown extension config keys should fail during config loading");

        let message = format!("{error:#}");
        assert!(message.contains("parse config client.toml"));
        assert!(message.contains("unknown field `path_lookkup`"));
    }

    #[test]
    fn extension_registry_rejects_reserved_names() {
        let error = Config::load_for_extension_dispatch_from_sources(Some((
            Path::new("client.toml"),
            r#"
[extensions.commands.tui]
program = "custom-tui"
"#,
        )))
        .expect_err("first-party and built-in subcommands should not be configurable extensions");

        assert_eq!(
            format!("{error:#}"),
            "extension name `tui` is reserved for a built-in or first-party cue subcommand"
        );
    }

    #[test]
    fn extension_registry_rejects_non_kebab_case_names() {
        let error = Config::load_for_extension_dispatch_from_sources(Some((
            Path::new("client.toml"),
            r#"
[extensions.commands.foo_bar]
program = "cue-foo-bar"
"#,
        )))
        .expect_err("extension names should be stable CLI subcommand names");

        assert_eq!(
            format!("{error:#}"),
            "extension name `foo_bar` must be kebab-case ASCII, for example `foo` or `foo-bar`"
        );
    }

    #[test]
    fn extension_registry_rejects_empty_program() {
        let error = Config::load_for_extension_dispatch_from_sources(Some((
            Path::new("client.toml"),
            r#"
[extensions.commands.foo]
program = "   "
"#,
        )))
        .expect_err("extension program should be validated by config loading");

        assert_eq!(
            format!("{error:#}"),
            "extension `foo` program must not be empty"
        );
    }

    #[test]
    fn extension_registry_rejects_padded_program() {
        for program in [r#"" cue-foo""#, r#""cue-foo ""#] {
            let error = Config::load_for_extension_dispatch_from_sources(Some((
                Path::new("client.toml"),
                &format!(
                    r#"
[extensions.commands.foo]
program = {program}
"#
                ),
            )))
            .expect_err("extension program should be validated by config loading");

            assert_eq!(
                format!("{error:#}"),
                "extension `foo` program must not have leading or trailing whitespace"
            );
        }
    }

    #[test]
    fn extension_dispatch_config_defaults_when_config_root_is_absent() {
        let config = Config::load_for_extension_dispatch_from_paths(None)
            .expect("missing config root should not block extension dispatch");

        assert!(!config.extensions.path_lookup);
        assert!(config.extensions.commands.is_empty());
    }

    #[test]
    fn extension_dispatch_config_ignores_transport_semantics() {
        let config = Config::load_for_extension_dispatch_from_sources(Some((
            Path::new("client.toml"),
            r#"
[transport]
default_profile = " remote"

[extensions.commands.foo]
program = "cue-foo"
"#,
        )))
        .expect("extension dispatch should not validate unrelated transport config");

        assert_eq!(
            config.extensions.commands.get("foo"),
            Some(&ExtensionCommand {
                program: "cue-foo".into(),
                description: None,
            })
        );
    }

    #[test]
    fn extension_dispatch_config_rejects_unknown_top_level_sections() {
        let error = Config::load_for_extension_dispatch_from_sources(Some((
            Path::new("client.toml"),
            r#"
[extensons]
path_lookup = true
"#,
        )))
        .expect_err("top-level extension config typos should not be silently defaulted");

        assert!(
            format!("{error:#}").contains("unknown top-level client config section `extensons`")
        );
    }
}

#[cfg(all(test, any(feature = "script", feature = "tui")))]
mod transport_tests {
    use super::*;

    #[test]
    fn transport_config_is_validated_during_cli_config_load() {
        let error = Config::load_from_sources(Some((
            Path::new("client.toml"),
            r#"
[transport]
default_profile = " remote"
"#,
        )))
        .expect_err("invalid transport config should fail while loading CLI config");

        assert_eq!(
            format!("{error:#}"),
            "transport.default_profile must not have leading or trailing whitespace"
        );
    }

    #[test]
    fn cli_config_rejects_unknown_top_level_sections() {
        let error = Config::load_from_sources(Some((
            Path::new("client.toml"),
            r#"
[transprt]
default_profile = "remote"
"#,
        )))
        .expect_err("top-level client config typos should fail during CLI config loading");

        assert!(
            format!("{error:#}").contains("unknown top-level client config section `transprt`")
        );
    }
}
