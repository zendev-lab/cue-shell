use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A typed parameter value used in mode params `()`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ParamValue {
    Str(String),
    Bool(bool),
}

/// Mode parameters extracted from `:cmd(k=v, ...)` syntax.
///
/// Per-invocation overrides merged with daemon.toml defaults by the Resolver.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ModeParams {
    pub params: BTreeMap<String, ParamValue>,
}

impl ModeParams {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.params.is_empty()
    }

    pub fn get(&self, key: &str) -> Option<&ParamValue> {
        self.params.get(key)
    }

    pub fn insert(&mut self, key: impl Into<String>, value: ParamValue) {
        self.params.insert(key.into(), value);
    }

    /// Get explicit working directory override, if specified.
    pub fn cwd(&self) -> Option<std::path::PathBuf> {
        match self.get("cwd") {
            Some(ParamValue::Str(s)) => Some(std::path::PathBuf::from(s)),
            _ => None,
        }
    }

    /// Get runtime wrapper override, if specified.
    /// `None` means use the global config default.
    pub fn wrapper_enabled(&self) -> Option<bool> {
        match self.get("wrapper") {
            Some(ParamValue::Bool(b)) => Some(*b),
            _ => None,
        }
    }

    /// Whether the submitted chain may apply scope-transform leaves (`cd`,
    /// `env set`) to its chain scope. Defaults to false when unspecified.
    pub fn scope(&self) -> Option<bool> {
        match self.get("scope") {
            Some(ParamValue::Bool(b)) => Some(*b),
            _ => None,
        }
    }

    /// Whether to allocate a PTY for the spawned command.
    /// Defaults to `true` (PTY). Set `pty=false` for non-interactive
    /// commands that should run with plain pipes.
    pub fn pty_enabled(&self) -> bool {
        match self.get("pty") {
            Some(ParamValue::Bool(b)) => *b,
            _ => true, // default: PTY on
        }
    }
}
