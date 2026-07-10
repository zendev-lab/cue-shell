use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use toml::Value;

use crate::dirs;

const DAEMON_CONFIG_FILE: &str = "daemon.toml";
const DAEMON_ROOT_SECTIONS: &[&str] = &[
    "aliases",
    "block",
    "resources",
    "retention",
    "sandbox",
    "warn",
    "wrapper",
];
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub block: BlockConfig,
    #[serde(default)]
    pub warn: WarnConfig,
    #[serde(default)]
    pub aliases: AliasConfig,
    #[serde(default)]
    pub resources: ResourceConfig,
    #[serde(default)]
    pub retention: RetentionConfig,
    #[serde(default)]
    pub sandbox: SandboxConfig,
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

const DEFAULT_GIT_NO_VERIFY_HINT: &str = "Run the commit normally; if hooks fail, inspect and fix the hook/check or ask before any alternative.";
const DEFAULT_DAEMON_CONFIG: &str = r#"# cue-shell daemon runtime configuration.
# This file is created automatically on first daemon startup and is safe to edit.
# Existing files are never overwritten; missing built-in guardrails are still
# merged at load time.

[block.versioned_commands]
python = "Use `script_run`/`script_eval` for Python, or run `uv run python ...` explicitly; direct Python launchers are blocked so Python execution goes through uv."

[block.commands.git]
"--no-verify" = "Run the commit normally; if hooks fail, inspect and fix the hook/check or ask before any alternative."

[retention]
max_job_history = 200
max_script_runs = 100
"#;

/// Hard command guardrails.
///
/// Configured in `daemon.toml`:
///
/// ```toml
/// [block.commands]
/// sh = "Avoid shell wrappers."
///
/// [block.commands.git]
/// "--no-verify" = "Run the commit normally, then fix hook failures."
///
/// [warn.commands]
/// rm = "Careful: this removes files"
/// ```
///
/// Command keys match the exact basename of argv[0].  Argument rules match
/// each argv token independently using literal strings; they are not glob or
/// regex patterns.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlockConfig {
    /// Map from command name → whole-command or argument-level block rule.
    #[serde(default = "default_block_commands")]
    pub commands: BTreeMap<String, BlockCommandRule>,
    /// Map from versioned command stem → whole-command block rule. A stem `python`
    /// matches `python`, `python3`, `python3.12`, etc., but not `python-config`.
    #[serde(default)]
    pub versioned_commands: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum BlockCommandRule {
    /// Block the command whenever argv[0]'s basename matches the map key.
    WholeCommand(String),
    /// Block when any single argv token matches one of these literal patterns.
    Args(BTreeMap<String, String>),
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WarnConfig {
    /// Map from command name → advisory warning hint.
    #[serde(default)]
    pub commands: BTreeMap<String, String>,
}

fn blocked_arg_matches(arg: &str, pattern: &str) -> bool {
    arg == pattern || arg.starts_with(&format!("{pattern}="))
}

fn default_block_commands() -> BTreeMap<String, BlockCommandRule> {
    let mut commands = BTreeMap::new();
    commands.insert(
        "git".into(),
        BlockCommandRule::Args(BTreeMap::from([(
            "--no-verify".into(),
            DEFAULT_GIT_NO_VERIFY_HINT.into(),
        )])),
    );
    commands
}

impl Default for BlockConfig {
    fn default() -> Self {
        Self {
            commands: default_block_commands(),
            versioned_commands: BTreeMap::new(),
        }
    }
}

impl BlockConfig {
    fn ensure_defaults(&mut self) {
        for (command, default_rule) in default_block_commands() {
            match (self.commands.get_mut(&command), default_rule) {
                (Some(BlockCommandRule::Args(rules)), BlockCommandRule::Args(defaults)) => {
                    for (pattern, hint) in defaults {
                        rules.entry(pattern).or_insert(hint);
                    }
                }
                (Some(BlockCommandRule::WholeCommand(_)), _) => {}
                (None, rule) => {
                    self.commands.insert(command, rule);
                }
                _ => {}
            }
        }
    }

    fn check(&self, command_line: &[String]) -> Option<BlockDecision> {
        let cmd_name = command_line.first()?;
        let base = command_base(cmd_name);

        if let Some(rule) = self.commands.get(base) {
            return match rule {
                BlockCommandRule::WholeCommand(hint) => Some(BlockDecision::Block(
                    self.command_block_reason(cmd_name, hint),
                )),
                BlockCommandRule::Args(rules) => {
                    for arg in &command_line[1..] {
                        if let Some((pattern, hint)) = rules
                            .iter()
                            .find(|(pattern, _)| blocked_arg_matches(arg, pattern))
                        {
                            return Some(BlockDecision::Block(
                                self.arg_block_reason(cmd_name, pattern, hint),
                            ));
                        }
                    }
                    None
                }
            };
        }

        self.versioned_commands
            .iter()
            .find(|(stem, _)| versioned_command_matches(base, stem))
            .map(|(_, hint)| BlockDecision::Block(self.command_block_reason(cmd_name, hint)))
    }

    fn command_block_reason(&self, cmd_name: &str, hint: &str) -> String {
        format!(
            "blocked: `{cmd_name}` is forbidden by daemon config\n  hint: {hint}\n  (see [block.commands] in daemon.toml)"
        )
    }

    fn arg_block_reason(&self, cmd_name: &str, pattern: &str, hint: &str) -> String {
        format!(
            "blocked: `{cmd_name} {pattern}` is forbidden by daemon config\n  hint: {hint}\n  (see [block.commands] in daemon.toml)"
        )
    }
}

impl WarnConfig {
    fn check(&self, command_line: &[String]) -> Option<BlockDecision> {
        let cmd_name = command_line.first()?;
        let base = command_base(cmd_name);
        self.commands
            .get(base)
            .map(|hint| BlockDecision::Warn(hint.clone()))
    }
}

fn command_base(cmd_name: &str) -> &str {
    std::path::Path::new(cmd_name)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(cmd_name)
}

fn versioned_command_matches(base: &str, stem: &str) -> bool {
    if stem.is_empty() {
        return false;
    }
    if base == stem {
        return true;
    }
    let Some(suffix) = base.strip_prefix(stem) else {
        return false;
    };
    !suffix.is_empty()
        && suffix
            .split('.')
            .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
}

#[derive(Debug, Clone)]
pub enum BlockDecision {
    Block(String),
    Warn(String),
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceConfig {
    #[serde(default)]
    pub cli: BTreeMap<String, CliResourceProviderConfig>,
    #[serde(default)]
    pub nvidia: NvidiaResourceConfig,
}

impl ResourceConfig {
    fn validate(&self, path: &Path) -> Result<()> {
        for (id, provider) in &self.cli {
            if id.trim() != id || id.is_empty() {
                bail!(
                    "resources.cli provider id `{id}` in {} must be non-empty and must not have leading or trailing whitespace",
                    path.display()
                );
            }
            provider.validate(path, id)?;
        }
        self.nvidia.validate(path)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NvidiaResourceConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_nvidia_provider_id")]
    pub provider_id: String,
    #[serde(default = "default_nvidia_gpu_key")]
    pub gpu_key: String,
    #[serde(default = "default_nvidia_gpu_mem_key")]
    pub gpu_mem_key: String,
    #[serde(default)]
    pub safety_margin_bytes: u64,
    #[serde(default = "default_nvidia_probe_ttl_ms")]
    pub probe_ttl_ms: u64,
}

impl Default for NvidiaResourceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider_id: default_nvidia_provider_id(),
            gpu_key: default_nvidia_gpu_key(),
            gpu_mem_key: default_nvidia_gpu_mem_key(),
            safety_margin_bytes: 0,
            probe_ttl_ms: default_nvidia_probe_ttl_ms(),
        }
    }
}

fn default_nvidia_provider_id() -> String {
    "nvidia".into()
}

fn default_nvidia_gpu_key() -> String {
    "gpu".into()
}

fn default_nvidia_gpu_mem_key() -> String {
    "gpu_mem".into()
}

fn default_nvidia_probe_ttl_ms() -> u64 {
    1_000
}

impl NvidiaResourceConfig {
    fn validate(&self, path: &Path) -> Result<()> {
        for (field, value) in [
            ("provider_id", &self.provider_id),
            ("gpu_key", &self.gpu_key),
            ("gpu_mem_key", &self.gpu_mem_key),
        ] {
            if value.trim() != value || value.is_empty() {
                bail!(
                    "resources.nvidia.{field} in {} must be non-empty and must not have leading or trailing whitespace",
                    path.display()
                );
            }
        }
        if self.gpu_key == self.gpu_mem_key {
            bail!(
                "resources.nvidia.gpu_key and gpu_mem_key in {} must be distinct",
                path.display()
            );
        }
        if self.probe_ttl_ms == 0 {
            bail!(
                "resources.nvidia.probe_ttl_ms in {} must be greater than zero",
                path.display()
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CliResourceProviderConfig {
    pub keys: Vec<String>,
    pub probe: Vec<String>,
    pub reserve: Vec<String>,
    pub release: Vec<String>,
    #[serde(default = "default_resource_cli_timeout_ms")]
    pub timeout_ms: u64,
}

fn default_resource_cli_timeout_ms() -> u64 {
    5_000
}

impl CliResourceProviderConfig {
    fn validate(&self, path: &Path, id: &str) -> Result<()> {
        if self.keys.is_empty() {
            bail!(
                "resources.cli.{id}.keys in {} must contain at least one resource key",
                path.display()
            );
        }
        for key in &self.keys {
            if key.trim() != key || key.is_empty() {
                bail!(
                    "resources.cli.{id}.keys in {} contains an empty or whitespace-padded key",
                    path.display()
                );
            }
        }
        validate_resource_cli_command(path, id, "probe", &self.probe)?;
        validate_resource_cli_command(path, id, "reserve", &self.reserve)?;
        validate_resource_cli_command(path, id, "release", &self.release)?;
        if self.timeout_ms == 0 {
            bail!(
                "resources.cli.{id}.timeout_ms in {} must be greater than zero",
                path.display()
            );
        }
        Ok(())
    }
}

fn validate_resource_cli_command(
    path: &Path,
    id: &str,
    field: &str,
    argv: &[String],
) -> Result<()> {
    let Some(program) = argv.first() else {
        bail!(
            "resources.cli.{id}.{field} in {} must be a non-empty argv array",
            path.display()
        );
    };
    if program.trim() != program || program.is_empty() {
        bail!(
            "resources.cli.{id}.{field}[0] in {} must be non-empty and must not have leading or trailing whitespace",
            path.display()
        );
    }
    Ok(())
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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

/// Filesystem sandbox defaults applied to `:run(sandbox=overlay)` jobs.
///
/// ```toml
/// [sandbox]
/// # Root under which each sandboxed job gets its own <root>/<job-id>/{upper,work}.
/// default_upper_root = "/dev/shm/cue-shell-sandbox"
/// # Refuse to prepare a sandbox when the upper-root filesystem has less than
/// # this fraction of space available (0.0 disables the guard). Protects shared
/// # memory (/dev/shm) from being exhausted by runaway writes.
/// min_free_ratio = 0.1
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxConfig {
    #[serde(default = "default_sandbox_upper_root")]
    pub default_upper_root: PathBuf,
    #[serde(default = "default_sandbox_min_free_ratio")]
    pub min_free_ratio: f64,
}

impl Default for SandboxConfig {
    fn default() -> Self {
        Self {
            default_upper_root: default_sandbox_upper_root(),
            min_free_ratio: default_sandbox_min_free_ratio(),
        }
    }
}

impl SandboxConfig {
    fn validate(&self, path: &Path) -> Result<()> {
        validate_non_empty_path(&self.default_upper_root, "sandbox.default_upper_root", path)?;
        if !self.min_free_ratio.is_finite()
            || self.min_free_ratio < 0.0
            || self.min_free_ratio >= 1.0
        {
            bail!(
                "sandbox.min_free_ratio in {} must be in the range [0.0, 1.0)",
                path.display()
            );
        }
        Ok(())
    }
}

fn default_sandbox_upper_root() -> PathBuf {
    PathBuf::from("/dev/shm/cue-shell-sandbox")
}

fn default_sandbox_min_free_ratio() -> f64 {
    0.1
}

fn validate_non_empty_path(value: &Path, field: &str, config_path: &Path) -> Result<()> {
    let Some(value) = value.to_str() else {
        bail!("{field} in {} must be valid UTF-8", config_path.display());
    };
    if value.trim().is_empty() {
        bail!("{field} in {} must not be empty", config_path.display());
    }
    if value.trim() != value {
        bail!(
            "{field} in {} must not have leading or trailing whitespace",
            config_path.display()
        );
    }
    Ok(())
}

impl Config {
    pub fn load() -> Result<Self> {
        let config_dir = dirs::config_dir()?;
        let daemon_path = config_dir.join(DAEMON_CONFIG_FILE);
        ensure_default_daemon_config(&daemon_path)?;
        Self::load_from_source(
            read_source(&daemon_path)?
                .as_deref()
                .map(|text| (daemon_path.as_path(), text)),
        )
    }

    fn load_from_source(daemon: Option<(&Path, &str)>) -> Result<Self> {
        if let Some((path, text)) = daemon {
            return Self::parse(text, path);
        }
        Ok(Self::default())
    }

    fn parse(text: &str, path: &Path) -> Result<Self> {
        validate_root_config_shape(text, path)?;
        let mut config: Self =
            toml::from_str(text).with_context(|| format!("parse config {}", path.display()))?;
        config.block.ensure_defaults();
        config.validate(path)?;
        Ok(config)
    }

    fn validate(&self, path: &Path) -> Result<()> {
        self.wrapper.validate(path)?;
        self.resources.validate(path)?;
        self.sandbox.validate(path)?;
        Ok(())
    }

    /// Check whether `command_line` is blocked or should warn before running.
    /// Hard block rules take precedence over advisory warnings.
    pub fn check_command_guardrail(&self, command_line: &[String]) -> Option<BlockDecision> {
        self.block
            .check(command_line)
            .or_else(|| self.warn.check(command_line))
    }
}

fn validate_root_config_shape(text: &str, path: &Path) -> Result<()> {
    let value: Value =
        toml::from_str(text).with_context(|| format!("parse config {}", path.display()))?;
    let Some(root) = value.as_table() else {
        bail!("config {} must be a TOML table", path.display());
    };

    for section in root.keys() {
        if !DAEMON_ROOT_SECTIONS.contains(&section.as_str()) {
            bail!(
                "unknown top-level daemon config section `{section}` in {}; expected daemon sections [{}]",
                path.display(),
                DAEMON_ROOT_SECTIONS.join(", ")
            );
        }
    }

    Ok(())
}

// ────────────────────────────────────────────────────────────────────
// Wrapper config
// ────────────────────────────────────────────────────────────────────

/// Wrapper configuration for binary-prefix injection.
///
/// When enabled, command spawns are prefixed with `binary` only when the
/// command basename is explicitly present in `allowlist.commands`. The wrapper
/// is **idempotent**: if the program already matches `binary`, it is skipped.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WrapperConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_wrapper_binary")]
    pub binary: String,
    #[serde(default)]
    pub allowlist: WrapperAllowlist,
}

impl Default for WrapperConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            binary: default_wrapper_binary(),
            allowlist: WrapperAllowlist::default(),
        }
    }
}

impl WrapperConfig {
    fn validate(&self, path: &Path) -> Result<()> {
        if self.binary.trim() != self.binary {
            bail!(
                "wrapper.binary in {} must not have leading or trailing whitespace",
                path.display()
            );
        }
        if self.enabled && self.binary.is_empty() {
            bail!(
                "wrapper.enabled is true in {} but wrapper.binary is empty",
                path.display()
            );
        }
        Ok(())
    }

    /// Determine whether the wrapper should be applied for a given program.
    pub fn should_wrap(
        &self,
        program: &str,
        is_foreground: bool,
        override_enabled: Option<bool>,
    ) -> bool {
        let enabled = override_enabled.unwrap_or(self.enabled);
        if !enabled || is_foreground || self.binary.is_empty() || self.binary.trim() != self.binary
        {
            return false;
        }
        let base = std::path::Path::new(program)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(program);
        if base == self.binary_base() {
            return false;
        }
        self.allowlist.matches(program)
    }

    fn binary_base(&self) -> &str {
        std::path::Path::new(&self.binary)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(&self.binary)
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WrapperAllowlist {
    #[serde(default)]
    pub commands: Vec<String>,
}

impl WrapperAllowlist {
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

// ────────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────────

fn ensure_default_daemon_config(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        dirs::ensure_private_dir(parent)
            .with_context(|| format!("create config directory {}", parent.display()))?;
    }
    match dirs::create_private_file(path) {
        Ok(Some(mut file)) => {
            file.write_all(DEFAULT_DAEMON_CONFIG.as_bytes())
                .with_context(|| format!("write default config {}", path.display()))?;
            Ok(())
        }
        Ok(None) => Ok(()),
        Err(error) => {
            Err(error).with_context(|| format!("create default config {}", path.display()))
        }
    }
}

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
    fn default_daemon_config_text_parses_and_blocks_python() {
        let config =
            Config::load_from_source(Some((Path::new("daemon.toml"), DEFAULT_DAEMON_CONFIG)))
                .expect("default config parses");

        assert!(matches!(
            config.check_command_guardrail(&["python".into(), "script.py".into()]),
            Some(BlockDecision::Block(_))
        ));
        assert!(matches!(
            config.check_command_guardrail(&["python3.13".into(), "script.py".into()]),
            Some(BlockDecision::Block(_))
        ));
        assert!(
            config
                .check_command_guardrail(&[
                    "uv".into(),
                    "run".into(),
                    "python".into(),
                    "script.py".into()
                ])
                .is_none()
        );
    }

    #[test]
    fn missing_daemon_config_is_bootstrapped() {
        let root = std::env::temp_dir().join(format!(
            "cue-daemon-config-bootstrap-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));
        let path = root.join("cue-shell").join("daemon.toml");

        ensure_default_daemon_config(&path).expect("write default config");

        let text = std::fs::read_to_string(&path).expect("read generated config");
        assert!(text.contains("python ="));
        use std::os::unix::fs::PermissionsExt as _;
        assert_eq!(
            std::fs::metadata(path.parent().expect("config parent"))
                .expect("stat config parent")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(&path)
                .expect("stat generated config")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        Config::load_from_source(Some((&path, &text))).expect("generated config parses");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn existing_daemon_config_is_migrated_to_private_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let root = std::env::temp_dir().join(format!(
            "cue-daemon-config-permissions-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));
        let path = root.join("cue-shell/daemon.toml");
        std::fs::create_dir_all(path.parent().expect("config parent"))
            .expect("create config parent");
        std::fs::write(&path, "[warn]\ncommands = []\n").expect("write config");
        std::fs::set_permissions(
            path.parent().expect("config parent"),
            std::fs::Permissions::from_mode(0o755),
        )
        .expect("set wide directory permissions");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
            .expect("set wide config permissions");

        ensure_default_daemon_config(&path).expect("secure existing config");

        assert_eq!(
            std::fs::metadata(path.parent().expect("config parent"))
                .expect("stat config parent")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(&path)
                .expect("stat config")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::read_to_string(&path).expect("read existing config"),
            "[warn]\ncommands = []\n"
        );
        std::fs::remove_dir_all(root).expect("remove temp root");
    }

    #[test]
    fn invalid_config_is_not_silently_defaulted() {
        let error =
            Config::load_from_source(Some((Path::new("daemon.toml"), "[wrapper]\nenabled = [")))
                .expect_err("invalid config should fail");

        assert!(error.to_string().contains("parse config daemon.toml"));
    }

    #[test]
    fn daemon_config_rejects_unknown_fields_inside_fixed_sections() {
        for (section, config, expected) in [
            (
                "block",
                r#"
[block]
commandz = {}
"#,
                "unknown field `commandz`",
            ),
            (
                "retention",
                r#"
[retention]
max_jobs = 10
"#,
                "unknown field `max_jobs`",
            ),
            (
                "warn",
                r#"
[warn]
commandz = {}
"#,
                "unknown field `commandz`",
            ),
            (
                "wrapper",
                r#"
[wrapper]
program = "rtk"
"#,
                "unknown field `program`",
            ),
            (
                "wrapper.allowlist",
                r#"
[wrapper.allowlist]
command = ["git"]
"#,
                "unknown field `command`",
            ),
        ] {
            let error = match Config::load_from_source(Some((Path::new("daemon.toml"), config))) {
                Ok(_) => panic!("unknown {section} field should fail"),
                Err(error) => error,
            };

            let message = format!("{error:#}");
            assert!(
                message.contains("parse config daemon.toml"),
                "missing parse context for {section}: {message}"
            );
            assert!(
                message.contains(expected),
                "wrong error for {section}: expected {expected:?}, got {message}"
            );
        }
    }

    #[test]
    fn daemon_config_rejects_unknown_top_level_sections() {
        let error = Config::load_from_source(Some((
            Path::new("daemon.toml"),
            r#"
[wefft]
socket_path = "/tmp/typo.sock"
"#,
        )))
        .expect_err("unknown top-level daemon sections should fail before defaults apply");

        let message = format!("{error:#}");
        assert!(message.contains("unknown top-level daemon config section `wefft`"));
        assert!(message.contains("daemon.toml"));
        assert!(message.contains("wrapper"));
        assert!(!message.contains("transport"));
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
    fn alias_parsed_from_daemon_toml() {
        let config = Config::load_from_source(Some((
            Path::new("daemon.toml"),
            r#"
[aliases]
"git clone" = "ein clone"
pip = "uv pip"
"#,
        )))
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
    fn resources_nvidia_provider_parsed_from_daemon_toml() {
        let config = Config::load_from_source(Some((
            Path::new("daemon.toml"),
            r#"
[resources.nvidia]
enabled = true
provider_id = "gpu"
gpu_key = "cuda"
gpu_mem_key = "cuda_mem"
safety_margin_bytes = 1048576
probe_ttl_ms = 250
"#,
        )))
        .expect("load config");

        assert!(config.resources.nvidia.enabled);
        assert_eq!(config.resources.nvidia.provider_id, "gpu");
        assert_eq!(config.resources.nvidia.gpu_key, "cuda");
        assert_eq!(config.resources.nvidia.gpu_mem_key, "cuda_mem");
        assert_eq!(config.resources.nvidia.safety_margin_bytes, 1_048_576);
        assert_eq!(config.resources.nvidia.probe_ttl_ms, 250);
    }

    #[test]
    fn resources_nvidia_keys_must_be_distinct() {
        let error = Config::load_from_source(Some((
            Path::new("daemon.toml"),
            r#"
[resources.nvidia]
gpu_key = "gpu"
gpu_mem_key = "gpu"
"#,
        )))
        .expect_err("duplicate keys should fail");

        assert!(
            error.to_string().contains(
                "resources.nvidia.gpu_key and gpu_mem_key in daemon.toml must be distinct"
            ),
            "{error:#}"
        );
    }

    #[test]
    fn resources_cli_provider_parsed_from_daemon_toml() {
        let config = Config::load_from_source(Some((
            Path::new("daemon.toml"),
            r#"
[resources.cli.license]
keys = ["license", "license_mem"]
probe = ["license-helper", "probe"]
reserve = ["license-helper", "reserve"]
release = ["license-helper", "release"]
timeout_ms = 250
"#,
        )))
        .expect("load config");

        let provider = config
            .resources
            .cli
            .get("license")
            .expect("license provider");
        assert_eq!(provider.keys, vec!["license", "license_mem"]);
        assert_eq!(provider.probe, vec!["license-helper", "probe"]);
        assert_eq!(provider.timeout_ms, 250);
    }

    #[test]
    fn resources_cli_provider_requires_non_empty_commands() {
        let error = Config::load_from_source(Some((
            Path::new("daemon.toml"),
            r#"
[resources.cli.license]
keys = ["license"]
probe = []
reserve = ["license-helper", "reserve"]
release = ["license-helper", "release"]
"#,
        )))
        .expect_err("empty probe argv should fail");

        assert!(
            error.to_string().contains(
                "resources.cli.license.probe in daemon.toml must be a non-empty argv array"
            ),
            "{error:#}"
        );
    }

    #[test]
    fn wrapper_default_disabled() {
        let cfg = WrapperConfig::default();
        assert!(!cfg.enabled);
        assert!(!cfg.should_wrap("git", false, None));
    }

    #[test]
    fn wrapper_enabled_requires_allowlist_match() {
        let cfg = WrapperConfig {
            enabled: true,
            binary: "rtk".into(),
            allowlist: WrapperAllowlist {
                commands: vec!["git".into()],
            },
        };
        assert!(cfg.should_wrap("git", false, None));
        assert!(cfg.should_wrap("/usr/bin/git", false, None));
        assert!(!cfg.should_wrap("cargo", false, None));
    }

    #[test]
    fn wrapper_empty_allowlist_wraps_nothing() {
        let cfg = WrapperConfig {
            enabled: true,
            binary: "rtk".into(),
            ..Default::default()
        };
        assert!(!cfg.should_wrap("git", false, None));
    }

    #[test]
    fn wrapper_empty_binary_wraps_nothing_even_when_overridden_on() {
        let cfg = WrapperConfig {
            enabled: false,
            binary: String::new(),
            allowlist: WrapperAllowlist {
                commands: vec!["git".into()],
            },
        };

        assert!(!cfg.should_wrap("git", false, Some(true)));
    }

    #[test]
    fn wrapper_enabled_requires_non_empty_binary() {
        let error = Config::load_from_source(Some((
            Path::new("daemon.toml"),
            r#"
[wrapper]
enabled = true

[wrapper.allowlist]
commands = ["git"]
"#,
        )))
        .expect_err("enabled wrapper without binary should fail config loading");

        assert!(error.to_string().contains("wrapper.binary is empty"));
    }

    #[test]
    fn wrapper_binary_rejects_leading_or_trailing_whitespace() {
        let error = Config::load_from_source(Some((
            Path::new("daemon.toml"),
            r#"
[wrapper]
enabled = false
binary = " rtk"
"#,
        )))
        .expect_err("padded wrapper binary should fail config loading");

        assert!(error.to_string().contains(
            "wrapper.binary in daemon.toml must not have leading or trailing whitespace"
        ));
    }

    #[test]
    fn wrapper_idempotent_already_wrapped() {
        let cfg = WrapperConfig {
            enabled: true,
            binary: "rtk".into(),
            allowlist: WrapperAllowlist {
                commands: vec!["rtk".into()],
            },
        };
        assert!(!cfg.should_wrap("rtk", false, None));
    }

    #[test]
    fn wrapper_skips_foreground_commands() {
        let cfg = WrapperConfig {
            enabled: true,
            binary: "rtk".into(),
            allowlist: WrapperAllowlist {
                commands: vec!["git".into()],
            },
        };
        assert!(!cfg.should_wrap("git", true, None));
        assert!(cfg.should_wrap("git", false, None));
    }

    #[test]
    fn wrapper_parsed_from_daemon_toml() {
        let config = Config::load_from_source(Some((
            Path::new("daemon.toml"),
            r#"
[wrapper]
enabled = true
binary = "rtk"

[wrapper.allowlist]
commands = ["git", "cargo"]
"#,
        )))
        .expect("load config");
        assert!(config.wrapper.enabled);
        assert_eq!(config.wrapper.binary, "rtk");
        assert_eq!(config.wrapper.allowlist.commands, vec!["git", "cargo"]);
    }

    #[test]
    fn wrapper_absent_config_is_default() {
        let config = Config::load_from_source(Some((
            Path::new("daemon.toml"),
            r#"
[aliases]
pip = "uv pip"
"#,
        )))
        .expect("load config");
        assert!(!config.wrapper.enabled);
    }

    #[test]
    fn block_config_default_blocks_git_no_verify_with_hint() {
        let config = Config::default();
        assert!(
            config
                .check_command_guardrail(&["python".into(), "script.py".into()])
                .is_none(),
            "Config::default should not hard-code the generated daemon.toml python block"
        );
        let decision = config
            .check_command_guardrail(&["git".into(), "commit".into(), "--no-verify".into()])
            .expect("git --no-verify should be blocked by default");
        match decision {
            BlockDecision::Block(message) => {
                assert!(message.contains("git --no-verify"));
                assert!(message.contains("hint:"));
                assert!(message.contains("Run the commit normally"));
            }
            BlockDecision::Warn(_) => panic!("expected block decision"),
        }
        assert!(
            config
                .check_command_guardrail(&["git".into(), "push".into()])
                .is_none()
        );
        assert!(
            config
                .check_command_guardrail(&["cd".into(), "/tmp".into()])
                .is_none()
        );
    }

    #[test]
    fn block_config_partial_warn_table_keeps_default_block_commands() {
        let config = Config::load_from_source(Some((
            Path::new("daemon.toml"),
            r#"
[warn.commands]
sh = "Use direct-exec instead."
"#,
        )))
        .expect("load config");

        assert!(
            matches!(
                config.check_command_guardrail(&[
                    "git".into(),
                    "commit".into(),
                    "--no-verify".into()
                ]),
                Some(BlockDecision::Block(_))
            ),
            "configuring only warn.commands must not erase default block.commands"
        );
        assert!(matches!(
            config.check_command_guardrail(&["sh".into(), "-lc".into(), "echo hi".into()]),
            Some(BlockDecision::Warn(_))
        ));
    }

    #[test]
    fn block_config_whole_command_matches_exact_basename() {
        let config = Config::load_from_source(Some((
            Path::new("daemon.toml"),
            r#"
[block.commands]
sh = "Avoid shell wrappers."
"#,
        )))
        .expect("load config");

        for command in ["sh", "/bin/sh"] {
            let decision = config
                .check_command_guardrail(&[command.into(), "-c".into(), "echo hi".into()])
                .expect("sh command should be blocked");
            match decision {
                BlockDecision::Block(message) => {
                    assert!(message.contains(command));
                    assert!(message.contains("Avoid shell wrappers."));
                }
                BlockDecision::Warn(_) => panic!("expected block decision"),
            }
        }

        for command in ["zsh", "/bin/zsh", "shellcheck"] {
            assert!(
                config
                    .check_command_guardrail(&[command.into(), "-c".into(), "echo hi".into()])
                    .is_none(),
                "{command} must not match the literal sh command rule"
            );
        }
    }

    #[test]
    fn block_config_argument_rules_match_each_token_literally() {
        let config = Config::load_from_source(Some((
            Path::new("daemon.toml"),
            r#"
[block.commands.npm]
"--force" = "Use normal install."
"install --unsafe-peer-deps" = "This phrase is not matched across argv tokens."
"#,
        )))
        .expect("load config");

        assert!(matches!(
            config.check_command_guardrail(&["npm".into(), "install".into(), "--force".into()]),
            Some(BlockDecision::Block(_))
        ));
        assert!(matches!(
            config.check_command_guardrail(&[
                "npm".into(),
                "install".into(),
                "--force=true".into()
            ]),
            Some(BlockDecision::Block(_))
        ));
        assert!(
            config
                .check_command_guardrail(&[
                    "npm".into(),
                    "install".into(),
                    "--unsafe-peer-deps".into()
                ])
                .is_none(),
            "argument patterns must not match across joined argv tokens"
        );
    }

    #[test]
    fn block_config_custom_commands_keep_default_git_no_verify() {
        let config = Config::load_from_source(Some((
            Path::new("daemon.toml"),
            r#"
[block.commands.npm]
"--force" = "Use normal install."
"#,
        )))
        .expect("load config");

        assert!(matches!(
            config.check_command_guardrail(&["git".into(), "commit".into(), "--no-verify".into()]),
            Some(BlockDecision::Block(_))
        ));
        assert!(matches!(
            config.check_command_guardrail(&["npm".into(), "install".into(), "--force".into()]),
            Some(BlockDecision::Block(_))
        ));
    }

    #[test]
    fn block_config_prefers_block_over_warning_for_same_command() {
        let config = Config::load_from_source(Some((
            Path::new("daemon.toml"),
            r#"
[block.commands.git]
"--no-verify" = "Run the commit normally."

[warn.commands]
git = "Review git command before running."
"#,
        )))
        .expect("load config");

        let git_no_verify = config
            .check_command_guardrail(&["git".into(), "commit".into(), "--no-verify".into()])
            .expect("git --no-verify should be blocked");
        match git_no_verify {
            BlockDecision::Block(message) => {
                assert!(message.contains("hint:"));
                assert!(message.contains("Run the commit normally"));
            }
            BlockDecision::Warn(_) => panic!("expected block decision"),
        }
        assert!(matches!(
            config.check_command_guardrail(&["git".into(), "status".into()]),
            Some(BlockDecision::Warn(_))
        ));
    }

    #[test]
    fn block_config_parses_and_checks() {
        let config = Config::load_from_source(Some((
            Path::new("daemon.toml"),
            r#"
[block.commands.git]
"--no-verify" = "Run the commit normally."

[block.commands.npm]
"--force" = "Use the lockfile and normal install path instead."
"--unsafe-peer-deps" = "Use the package manager's normal dependency resolution."
"#,
        )))
        .expect("load config");

        // Blocked patterns
        assert!(
            config
                .check_command_guardrail(&["git".into(), "push".into(), "--no-verify".into()])
                .is_some()
        );
        assert!(
            config
                .check_command_guardrail(&["git".into(), "commit".into(), "--no-verify".into()])
                .is_some()
        );
        let npm_force = config
            .check_command_guardrail(&["npm".into(), "install".into(), "--force".into()])
            .expect("npm --force should be blocked");
        match npm_force {
            BlockDecision::Block(message) => {
                assert!(message.contains("npm --force"));
                assert!(message.contains("Use the lockfile and normal install path instead."));
            }
            BlockDecision::Warn(_) => panic!("expected block decision"),
        }

        // Allowed patterns
        assert!(
            config
                .check_command_guardrail(&["git".into(), "push".into()])
                .is_none()
        );
        assert!(
            config
                .check_command_guardrail(&[
                    "git".into(),
                    "commit".into(),
                    "-m".into(),
                    "fix".into()
                ])
                .is_none()
        );
        assert!(
            config
                .check_command_guardrail(&["npm".into(), "install".into()])
                .is_none()
        );
        assert!(
            config
                .check_command_guardrail(&["cargo".into(), "test".into()])
                .is_none()
        );
    }

    #[test]
    fn default_retention_config_is_present() {
        let config = Config::default();
        assert_eq!(config.retention.max_job_history, 200);
        assert_eq!(config.retention.max_script_runs, 100);
    }

    #[test]
    fn default_sandbox_config_uses_shared_memory_upper_root() {
        let config = Config::default();
        assert_eq!(
            config.sandbox.default_upper_root,
            Path::new("/dev/shm/cue-shell-sandbox")
        );
        assert_eq!(config.sandbox.min_free_ratio, 0.1);
    }

    #[test]
    fn sandbox_config_parsed_from_daemon_toml() {
        let config = Config::load_from_source(Some((
            Path::new("daemon.toml"),
            r#"
[sandbox]
default_upper_root = "/mnt/fast/cue-upper"
min_free_ratio = 0.25
"#,
        )))
        .expect("load config");
        assert_eq!(
            config.sandbox.default_upper_root,
            Path::new("/mnt/fast/cue-upper")
        );
        assert_eq!(config.sandbox.min_free_ratio, 0.25);
    }

    #[test]
    fn sandbox_config_rejects_unknown_field() {
        let error = Config::load_from_source(Some((
            Path::new("daemon.toml"),
            r#"
[sandbox]
upper_root = "/tmp/typo"
"#,
        )))
        .expect_err("unknown sandbox field should fail");

        let message = format!("{error:#}");
        assert!(message.contains("parse config daemon.toml"), "{message}");
        assert!(message.contains("unknown field `upper_root`"), "{message}");
    }

    #[test]
    fn sandbox_config_rejects_empty_or_padded_upper_root() {
        let empty = Config::load_from_source(Some((
            Path::new("daemon.toml"),
            "[sandbox]\ndefault_upper_root = \"\"\n",
        )))
        .expect_err("empty upper root should fail");
        assert!(
            format!("{empty:#}")
                .contains("sandbox.default_upper_root in daemon.toml must not be empty"),
            "{empty:#}"
        );

        let padded = Config::load_from_source(Some((
            Path::new("daemon.toml"),
            "[sandbox]\ndefault_upper_root = \" /dev/shm/cue \"\n",
        )))
        .expect_err("padded upper root should fail");
        assert!(
            format!("{padded:#}").contains(
                "sandbox.default_upper_root in daemon.toml must not have leading or trailing whitespace"
            ),
            "{padded:#}"
        );
    }

    #[test]
    fn sandbox_config_rejects_out_of_range_min_free_ratio() {
        for value in ["-0.1", "1.0", "1.5"] {
            let error = Config::load_from_source(Some((
                Path::new("daemon.toml"),
                &format!("[sandbox]\nmin_free_ratio = {value}\n"),
            )))
            .expect_err("out-of-range min_free_ratio should fail");
            assert!(
                format!("{error:#}")
                    .contains("sandbox.min_free_ratio in daemon.toml must be in the range"),
                "min_free_ratio {value} produced unexpected error: {error:#}"
            );
        }
    }
}
