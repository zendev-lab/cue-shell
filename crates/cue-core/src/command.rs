use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// A typed parameter value used in mode params `()`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ParamValue {
    Int(i64),
    Duration(Duration),
    Str(String),
    Bool(bool),
}

/// Mode parameters extracted from `:cmd(k=v, ...)` syntax.
///
/// Per-invocation overrides merged with config.toml defaults by the Resolver.
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

    /// Get retry count, if specified.
    pub fn retry(&self) -> Option<u32> {
        match self.get("retry") {
            Some(ParamValue::Int(n)) => Some(*n as u32),
            _ => None,
        }
    }

    /// Get timeout duration, if specified.
    pub fn timeout(&self) -> Option<Duration> {
        match self.get("timeout") {
            Some(ParamValue::Duration(d)) => Some(*d),
            _ => None,
        }
    }

    /// Get explicit working directory override, if specified.
    pub fn cwd(&self) -> Option<std::path::PathBuf> {
        match self.get("cwd") {
            Some(ParamValue::Str(s)) => Some(std::path::PathBuf::from(s)),
            _ => None,
        }
    }
}
