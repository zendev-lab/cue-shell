use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::dirs;

const SERVER_CONFIG_FILE: &str = "server.toml";
const LEGACY_CONFIG_FILE: &str = "config.toml";

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub block: BlockConfig,
    #[serde(default)]
    pub aliases: AliasConfig,
    #[serde(default)]
    pub bash_compat: BashCompatConfig,
    #[serde(default)]
    pub retention: RetentionConfig,
    #[serde(default)]
    pub weft: WeftConfig,
    #[serde(default)]
    pub wrapper: WrapperConfig,
}

#[derive(Debug, Clone)]
pub struct AliasEntry {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone, Default)]
pub struct AliasConfig {
    pub entries: Vec<AliasEntry>,
}

impl<'de> Deserialize<'de> for AliasConfig {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let map = BTreeMap::<String, String>::deserialize(deserializer)?;
        let mut entries: Vec<AliasEntry> = map
            .into_iter()
            .map(|(from, to)| AliasEntry { from, to })
            .collect();
        entries.sort_by(|a, b| {
            b.from
                .split_whitespace()
                .count()
                .cmp(&a.from.split_whitespace().count())
        });
        Ok(AliasConfig { entries })
    }
}

impl AliasConfig {
    pub fn apply(&self, input: &str) -> String {
        if self.entries.is_empty() || input.starts_with(':') {
            return input.to_string();
        }
        let input_tokens = token_spans(input);
        for entry in &self.entries {
            let from_tokens: Vec<&str> = entry.from.split_whitespace().collect();
            let n = from_tokens.len();
            if input_tokens.len() < n {
                continue;
            }
            let matches = input_tokens[..n]
                .iter()
                .map(|(start, end)| &input[*start..*end])
                .eq(from_tokens.iter().copied());
            if matches {
                let suffix_start = input_tokens[n - 1].1;
                let suffix = &input[suffix_start..];
                return if suffix.is_empty() {
                    entry.to.clone()
                } else {
                    format!("{}{}", entry.to, suffix)
                };
            }
        }
        input.to_string()
    }
}

fn token_spans(input: &str) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    let mut iter = input.char_indices().peekable();
    while let Some((start, ch)) = iter.next() {
        if ch.is_whitespace() {
            continue;
        }
        let mut end = start + ch.len_utf8();
        while let Some(&(idx, next)) = iter.peek() {
            if next.is_whitespace() {
                break;
            }
            end = idx + next.len_utf8();
            iter.next();
        }
        spans.push((start, end));
    }
    spans
}

/// Forbidden argument patterns for specific commands.
///
/// Configured in `server.toml`:
///
/// ```toml
/// [block.commands]
/// git = ["--no-verify", "--force"]
///
/// [block.warn_commands]
/// cd = "Use `cwd` parameter or `cd /path -> cmd` chain syntax instead"
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct BlockConfig {
    /// Map from command name → list of forbidden argument substrings.
    #[serde(default)]
    pub commands: BTreeMap<String, Vec<String>>,
    /// Map from command name → warning hint (requires user approval).
    #[serde(default)]
    pub warn_commands: BTreeMap<String, String>,
}

impl Default for BlockConfig {
    fn default() -> Self {
        let mut commands = BTreeMap::new();
        commands.insert("git".into(), vec!["--no-verify".into()]);
        let mut warn_commands = BTreeMap::new();
        warn_commands.insert(
            "cd".into(),
            "Use `cwd` parameter or `cd /path -> cmd` chain syntax instead".into(),
        );
        Self {
            commands,
            warn_commands,
        }
    }
}

impl BlockConfig {
    /// Check whether `command_line` is blocked.  Returns `None` if allowed,
    /// `Some(BlockDecision::Block(reason))` if blocked,
    /// `Some(BlockDecision::Warn(hint))` if the command needs approval.
    pub fn check(&self, command_line: &[String]) -> Option<BlockDecision> {
        let cmd_name = command_line.first()?;
        let base = std::path::Path::new(cmd_name)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(cmd_name);

        // Check warn-commands first (whole-command match).
        if let Some(hint) = self.warn_commands.get(base) {
            return Some(BlockDecision::Warn(hint.clone()));
        }

        // Check blocked arguments.
        let forbidden = self.commands.get(base)?;
        for arg in &command_line[1..] {
            for pattern in forbidden {
                if arg == pattern || arg.starts_with(&format!("{pattern}=")) {
                    return Some(BlockDecision::Block(format!(
                        "blocked: `{cmd_name} {pattern}` is forbidden by server config\n  (see [block.commands] in server.toml)"
                    )));
                }
            }
        }
        None
    }
}

#[derive(Debug, Clone)]
pub enum BlockDecision {
    Block(String),
    Warn(String),
}

/// Bash compatibility transform configuration.
///
/// When enabled, `&&` is translated to cue-shell's `->` (serial on success).
/// Other bash operators (`||`, `|`) intentionally stay as cue-shell native
/// operators to avoid semantic conflicts:
/// - `||` = cue-shell parallel-all (bash "or" is cue-shell's `~>`)
/// - `|>` / `|&>` / `|!>` = cue-shell pipe operators
///
/// ```toml
/// [bash_compat]
/// enabled = true
/// ```
#[derive(Debug, Clone, Default, Deserialize)]
pub struct BashCompatConfig {
    #[serde(default)]
    pub enabled: bool,
}

impl BashCompatConfig {
    /// Apply bash compatibility transformations to the input string.
    /// Replaces bash-style operators with cue-shell equivalents.
    pub fn apply(&self, input: &str) -> String {
        if !self.enabled {
            return input.to_string();
        }
        bash_compat_transform(input)
    }
}

/// Pre-tokenization transform: `&&` → cue-shell's `->`.
fn bash_compat_transform(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut result = String::with_capacity(input.len());
    let mut i = 0;

    while i < bytes.len() {
        // `&&` → `->` (only when at word boundary: whitespace before and after)
        if i + 1 < bytes.len()
            && bytes[i] == b'&'
            && bytes[i + 1] == b'&'
            && (i == 0 || bytes[i - 1] == b' ' || bytes[i - 1] == b'\t')
            && (i + 2 >= bytes.len()
                || bytes[i + 2] == b' '
                || bytes[i + 2] == b'\t'
                || bytes[i + 2] == b'\n')
        {
            result.push_str("->");
            i += 2;
            continue;
        }

        result.push(bytes[i] as char);
        i += 1;
    }

    result
}

#[derive(Debug, Clone, Deserialize)]
pub struct RetentionConfig {
    #[serde(default = "default_max_job_history")]
    pub max_job_history: usize,
    #[serde(default = "default_max_script_runs")]
    pub max_script_runs: usize,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            max_job_history: default_max_job_history(),
            max_script_runs: default_max_script_runs(),
        }
    }
}

fn default_max_job_history() -> usize {
    200
}

fn default_max_script_runs() -> usize {
    100
}

impl Config {
    pub fn load() -> Result<Self> {
        let config_dir = dirs::config_dir();
        let server_path = config_dir.join(SERVER_CONFIG_FILE);
        let legacy_path = config_dir.join(LEGACY_CONFIG_FILE);
        Self::load_from_sources(
            read_source(&server_path)?
                .as_deref()
                .map(|text| (server_path.as_path(), text)),
            read_source(&legacy_path)?
                .as_deref()
                .map(|text| (legacy_path.as_path(), text)),
        )
    }

    fn load_from_sources(
        server: Option<(&Path, &str)>,
        legacy: Option<(&Path, &str)>,
    ) -> Result<Self> {
        if let Some((path, text)) = server {
            return Self::parse(text, path);
        }
        if let Some((path, text)) = legacy {
            return Self::parse(text, path);
        }
        Ok(Self::default())
    }

    fn parse(text: &str, path: &Path) -> Result<Self> {
        toml::from_str(text).with_context(|| format!("parse config {}", path.display()))
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct WeftConfig {
    #[serde(default = "default_weft_socket_path")]
    pub socket_path: PathBuf,
}

impl Default for WeftConfig {
    fn default() -> Self {
        Self {
            socket_path: default_weft_socket_path(),
        }
    }
}

fn default_weft_socket_path() -> PathBuf {
    PathBuf::from("./weft.sock")
}

// ────────────────────────────────────────────────────────────────────
// Wrapper config
// ────────────────────────────────────────────────────────────────────

/// Wrapper configuration for binary-prefix injection.
///
/// When enabled, single-segment command spawns are prefixed with `binary`.
/// The wrapper is **idempotent**: if the program already matches `binary`,
/// or is in the denylist, or the spawn is a foreground attach, it is
/// skipped.
#[derive(Debug, Clone, Deserialize)]
pub struct WrapperConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_wrapper_binary")]
    pub binary: String,
    #[serde(default)]
    pub denylist: WrapperDenylist,
}

impl Default for WrapperConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            binary: default_wrapper_binary(),
            denylist: WrapperDenylist::default(),
        }
    }
}

impl WrapperConfig {
    /// Determine whether the wrapper should be applied for a given program.
    pub fn should_wrap(
        &self,
        program: &str,
        is_foreground: bool,
        override_enabled: Option<bool>,
    ) -> bool {
        let enabled = override_enabled.unwrap_or(self.enabled);
        if !enabled {
            return false;
        }
        let base = std::path::Path::new(program)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(program);
        if base == self.binary_base() {
            return false;
        }
        if is_foreground && self.denylist.interactive {
            return false;
        }
        !self.denylist.matches(program)
    }

    fn binary_base(&self) -> &str {
        std::path::Path::new(&self.binary)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(&self.binary)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct WrapperDenylist {
    #[serde(default)]
    pub commands: Vec<String>,
    #[serde(default = "default_true")]
    pub interactive: bool,
}

impl Default for WrapperDenylist {
    fn default() -> Self {
        Self {
            commands: Vec::new(),
            interactive: true,
        }
    }
}

impl WrapperDenylist {
    pub fn matches(&self, program: &str) -> bool {
        let base = std::path::Path::new(program)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(program);
        self.commands.iter().any(|c| c == base)
    }
}

fn default_wrapper_binary() -> String {
    String::new()
}

fn default_true() -> bool {
    true
}

// ────────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────────

fn read_source(path: &Path) -> Result<Option<String>> {
    if !path.exists() {
        return Ok(None);
    }

    let text =
        std::fs::read_to_string(path).with_context(|| format!("read config {}", path.display()))?;
    Ok(Some(text))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn default_config_sets_weft_socket() {
        let config = Config::default();
        assert_eq!(config.weft.socket_path, PathBuf::from("./weft.sock"));
    }

    #[test]
    fn server_toml_takes_precedence_over_legacy_config_toml() {
        let config = Config::load_from_sources(
            Some((
                Path::new("server.toml"),
                r#"
[weft]
socket_path = "/tmp/server.sock"
"#,
            )),
            Some((
                Path::new("config.toml"),
                r#"
[weft]
socket_path = "/tmp/legacy.sock"
"#,
            )),
        )
        .expect("load config");

        assert_eq!(config.weft.socket_path, PathBuf::from("/tmp/server.sock"));
    }

    #[test]
    fn legacy_config_toml_still_loads_weft_config() {
        let config = Config::load_from_sources(
            None,
            Some((
                Path::new("config.toml"),
                r#"
[weft]
socket_path = "/tmp/legacy.sock"
"#,
            )),
        )
        .expect("load config");

        assert_eq!(config.weft.socket_path, PathBuf::from("/tmp/legacy.sock"));
    }

    #[test]
    fn invalid_config_is_not_silently_defaulted() {
        let error = Config::load_from_sources(
            Some((Path::new("server.toml"), "[weft]\nsocket_path = [")),
            None,
        )
        .expect_err("invalid config should fail");

        assert!(error.to_string().contains("parse config server.toml"));
    }

    #[test]
    fn alias_no_match_passthrough() {
        let cfg = AliasConfig::default();
        assert_eq!(cfg.apply("pip install foo"), "pip install foo");
    }

    #[test]
    fn alias_single_word() {
        let cfg: AliasConfig = toml::from_str(r#"pip = "uv pip""#).unwrap();
        assert_eq!(cfg.apply("pip install foo"), "uv pip install foo");
        assert_eq!(cfg.apply("pip"), "uv pip");
    }

    #[test]
    fn alias_multi_word() {
        let cfg: AliasConfig = toml::from_str(r#""git clone" = "ein clone""#).unwrap();
        assert_eq!(
            cfg.apply("git clone https://github.com/foo/bar"),
            "ein clone https://github.com/foo/bar"
        );
    }

    #[test]
    fn alias_longer_match_takes_priority() {
        let cfg: AliasConfig = toml::from_str(
            r#"
git = "alt-git"
"git clone" = "ein clone"
"#,
        )
        .unwrap();
        assert_eq!(
            cfg.apply("git clone https://github.com/foo/bar"),
            "ein clone https://github.com/foo/bar"
        );
        assert_eq!(cfg.apply("git status"), "alt-git status");
    }

    #[test]
    fn alias_no_match_in_middle() {
        let cfg: AliasConfig = toml::from_str(r#"pip = "uv pip""#).unwrap();
        assert_eq!(cfg.apply("run pip install foo"), "run pip install foo");
    }

    #[test]
    fn alias_empty_input() {
        let cfg: AliasConfig = toml::from_str(r#"pip = "uv pip""#).unwrap();
        assert_eq!(cfg.apply(""), "");
    }

    #[test]
    fn alias_preserves_multiline_suffix() {
        let cfg: AliasConfig = toml::from_str(r#"pip = "uv pip""#).unwrap();
        assert_eq!(
            cfg.apply("pip install foo\ncargo test"),
            "uv pip install foo\ncargo test"
        );
    }

    #[test]
    fn alias_parsed_from_server_toml() {
        let config = Config::load_from_sources(
            Some((
                Path::new("server.toml"),
                r#"
[aliases]
"git clone" = "ein clone"
pip = "uv pip"
"#,
            )),
            None,
        )
        .expect("load config");
        assert_eq!(
            config.aliases.apply("git clone https://example.com"),
            "ein clone https://example.com"
        );
        assert_eq!(
            config.aliases.apply("pip install foo"),
            "uv pip install foo"
        );
    }

    #[test]
    fn parses_weft_socket_path() {
        let config = Config::load_from_sources(
            Some((
                Path::new("server.toml"),
                r#"
[weft]
socket_path = "/var/run/weft.sock"
"#,
            )),
            None,
        )
        .expect("load config");

        assert_eq!(config.weft.socket_path, PathBuf::from("/var/run/weft.sock"));
    }

    // ── WrapperConfig ──

    #[test]
    fn wrapper_default_disabled() {
        let cfg = WrapperConfig::default();
        assert!(!cfg.enabled);
        assert!(!cfg.should_wrap("git", false, None));
    }

    #[test]
    fn wrapper_enabled_wraps_command() {
        let cfg = WrapperConfig {
            enabled: true,
            binary: "rtk".into(),
            ..Default::default()
        };
        assert!(cfg.should_wrap("git", false, None));
    }

    #[test]
    fn wrapper_idempotent_already_wrapped() {
        let cfg = WrapperConfig {
            enabled: true,
            binary: "rtk".into(),
            ..Default::default()
        };
        assert!(!cfg.should_wrap("rtk", false, None));
    }

    #[test]
    fn wrapper_denylist_commands() {
        let cfg = WrapperConfig {
            enabled: true,
            binary: "rtk".into(),
            denylist: WrapperDenylist {
                commands: vec!["vim".into()],
                interactive: true,
            },
        };
        assert!(!cfg.should_wrap("vim", false, None));
        assert!(cfg.should_wrap("git", false, None));
    }

    #[test]
    fn wrapper_denylist_interactive() {
        let cfg = WrapperConfig {
            enabled: true,
            binary: "rtk".into(),
            ..Default::default()
        };
        assert!(!cfg.should_wrap("git", true, None));
        assert!(cfg.should_wrap("git", false, None));
    }

    #[test]
    fn wrapper_parsed_from_server_toml() {
        let config = Config::load_from_sources(
            Some((
                Path::new("server.toml"),
                r#"
[wrapper]
enabled = true
binary = "rtk"

[wrapper.denylist]
commands = ["vim", "ssh"]
interactive = false
"#,
            )),
            None,
        )
        .expect("load config");
        assert!(config.wrapper.enabled);
        assert_eq!(config.wrapper.binary, "rtk");
        assert_eq!(config.wrapper.denylist.commands, vec!["vim", "ssh"]);
        assert!(!config.wrapper.denylist.interactive);
    }

    #[test]
    fn wrapper_absent_config_is_default() {
        let config = Config::load_from_sources(
            Some((
                Path::new("server.toml"),
                r#"
[aliases]
pip = "uv pip"
"#,
            )),
            None,
        )
        .expect("load config");
        assert!(!config.wrapper.enabled);
    }

    #[test]
    fn block_config_default_blocks_git_no_verify() {
        let config = Config::default();
        assert!(
            config
                .block
                .check(&["git".into(), "commit".into(), "--no-verify".into()])
                .is_some()
        );
        assert!(config.block.check(&["git".into(), "push".into()]).is_none());
    }

    #[test]
    fn block_config_parses_and_checks() {
        let config = Config::load_from_sources(
            Some((
                Path::new("server.toml"),
                r#"
[block.commands]
git = ["--no-verify"]
npm = ["--force", "--legacy-peer-deps"]
"#,
            )),
            None,
        )
        .expect("load config");

        // Blocked patterns
        assert!(
            config
                .block
                .check(&["git".into(), "push".into(), "--no-verify".into()])
                .is_some()
        );
        assert!(
            config
                .block
                .check(&["git".into(), "commit".into(), "--no-verify".into()])
                .is_some()
        );
        assert!(
            config
                .block
                .check(&["npm".into(), "install".into(), "--force".into()])
                .is_some()
        );

        // Allowed patterns
        assert!(config.block.check(&["git".into(), "push".into()]).is_none());
        assert!(
            config
                .block
                .check(&["git".into(), "commit".into(), "-m".into(), "fix".into()])
                .is_none()
        );
        assert!(
            config
                .block
                .check(&["npm".into(), "install".into()])
                .is_none()
        );
        assert!(
            config
                .block
                .check(&["cargo".into(), "test".into()])
                .is_none()
        );
    }

    #[test]
    fn default_retention_config_is_present() {
        let config = Config::default();
        assert_eq!(config.retention.max_job_history, 200);
        assert_eq!(config.retention.max_script_runs, 100);
    }
}
