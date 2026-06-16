use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::id::ScopeHash;
use crate::resource::Need;

/// Immutable, content-addressed environment snapshot.
///
/// Scope ≈ git commit: immutable, content-addressed.
/// `:env set` / `:cd` create a new Scope and move the HEAD pointer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scope {
    /// blake3(canonical_bytes(env + cwd + ...))
    pub hash: ScopeHash,
    /// Parent in the delta chain (None for root scopes).
    pub parent: Option<ScopeHash>,
    /// Incremental changes relative to parent (when parent is Some).
    pub delta: Option<EnvDelta>,
    /// Full environment snapshot (root scopes, or flattened cache).
    pub snapshot: Option<EnvSnapshot>,
}

/// Full scope state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvSnapshot {
    pub env: BTreeMap<String, String>,
    pub cwd: PathBuf,
    /// Execution behavior owned by this scope.
    ///
    /// These are not environment variables, but they are part of scope identity
    /// when explicitly set. The default value preserves legacy scopes and the
    /// historical execution behavior.
    #[serde(default, skip_serializing_if = "ExecutionSettings::is_default")]
    pub execution: ExecutionSettings,
    // Future: umask, aliases, functions, shell_options, traps
}

/// Scope-owned execution behavior derived from run/cron mode params.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionSettings {
    /// Explicit PTY setting. `None` means use the command default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pty_enabled: Option<bool>,
    /// Resource needs declared via `need.<resource>=<quantity>` mode params.
    #[serde(default, skip_serializing_if = "Need::is_empty")]
    pub needs: Need,
    /// Optional filesystem sandbox settings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox: Option<SandboxSettings>,
}

impl ExecutionSettings {
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }
}

/// Scope-owned filesystem sandbox settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxSettings {
    pub mode: SandboxMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upper: Option<SandboxUpper>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxMode {
    Overlay,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxUpper {
    Directory(PathBuf),
    Tmpfs,
}

/// Incremental changes from a parent scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvDelta {
    /// New or modified variables.
    pub set: BTreeMap<String, String>,
    /// Removed variables.
    pub unset: Vec<String>,
    /// Changed cwd (None = inherit from parent).
    pub cwd: Option<PathBuf>,
    /// Replaced execution settings (None = inherit from parent).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<ExecutionSettings>,
}

impl EnvSnapshot {
    /// Compute the content-addressed hash for this snapshot.
    pub fn compute_hash(&self) -> ScopeHash {
        let mut hasher = blake3::Hasher::new();
        // Deterministic: BTreeMap is sorted
        for (k, v) in &self.env {
            hasher.update(k.as_bytes());
            hasher.update(b"=");
            hasher.update(v.as_bytes());
            hasher.update(b"\0");
        }
        hasher.update(self.cwd.to_string_lossy().as_bytes());
        if !self.execution.is_default() {
            hasher.update(b"\0execution\0");
            let execution = serde_json::to_vec(&self.execution)
                .expect("serialize scope execution settings for hashing");
            hasher.update(&execution);
        }
        ScopeHash(*hasher.finalize().as_bytes())
    }

    /// Apply a delta to produce a new snapshot.
    pub fn apply_delta(&self, delta: &EnvDelta) -> Self {
        let mut env = self.env.clone();
        for key in &delta.unset {
            env.remove(key);
        }
        for (k, v) in &delta.set {
            env.insert(k.clone(), v.clone());
        }
        let cwd = delta.cwd.clone().unwrap_or_else(|| self.cwd.clone());
        let execution = delta
            .execution
            .clone()
            .unwrap_or_else(|| self.execution.clone());
        Self {
            env,
            cwd,
            execution,
        }
    }
}

impl Scope {
    /// Create a root scope from a full snapshot.
    pub fn root(snapshot: EnvSnapshot) -> Self {
        let hash = snapshot.compute_hash();
        Self {
            hash,
            parent: None,
            delta: None,
            snapshot: Some(snapshot),
        }
    }

    /// Create a child scope by applying a delta to a parent.
    pub fn fork(parent_hash: ScopeHash, parent_snapshot: &EnvSnapshot, delta: EnvDelta) -> Self {
        let new_snapshot = parent_snapshot.apply_delta(&delta);
        let hash = new_snapshot.compute_hash();
        Self {
            hash,
            parent: Some(parent_hash),
            delta: Some(delta),
            snapshot: Some(new_snapshot),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource::ResourceQuantity;

    fn test_snapshot() -> EnvSnapshot {
        let mut env = BTreeMap::new();
        env.insert("PATH".into(), "/usr/bin".into());
        env.insert("HOME".into(), "/home/test".into());
        EnvSnapshot {
            env,
            cwd: PathBuf::from("/tmp"),
            execution: ExecutionSettings::default(),
        }
    }

    #[test]
    fn same_content_same_hash() {
        let a = test_snapshot();
        let b = test_snapshot();
        assert_eq!(a.compute_hash(), b.compute_hash());
    }

    #[test]
    fn different_content_different_hash() {
        let a = test_snapshot();
        let mut b = test_snapshot();
        b.env.insert("FOO".into(), "bar".into());
        assert_ne!(a.compute_hash(), b.compute_hash());
    }

    #[test]
    fn execution_settings_change_hash() {
        let a = test_snapshot();
        let mut b = test_snapshot();
        b.execution.pty_enabled = Some(false);
        assert_ne!(a.compute_hash(), b.compute_hash());

        let mut c = test_snapshot();
        c.execution.needs.insert("gpu", ResourceQuantity::Count(1));
        assert_ne!(a.compute_hash(), c.compute_hash());

        let mut d = test_snapshot();
        d.execution.sandbox = Some(SandboxSettings {
            mode: SandboxMode::Overlay,
            upper: Some(SandboxUpper::Tmpfs),
        });
        assert_ne!(a.compute_hash(), d.compute_hash());
    }

    #[test]
    fn legacy_snapshot_json_defaults_execution_settings() {
        let json = r#"{"env":{"PATH":"/usr/bin"},"cwd":"/tmp"}"#;
        let snapshot: EnvSnapshot = serde_json::from_str(json).expect("legacy snapshot json");
        assert_eq!(snapshot.execution, ExecutionSettings::default());

        let explicit = EnvSnapshot {
            env: BTreeMap::from([("PATH".into(), "/usr/bin".into())]),
            cwd: PathBuf::from("/tmp"),
            execution: ExecutionSettings::default(),
        };
        assert_eq!(snapshot.compute_hash(), explicit.compute_hash());
    }

    #[test]
    fn apply_delta() {
        let base = test_snapshot();
        let delta = EnvDelta {
            set: BTreeMap::from([("FOO".into(), "bar".into())]),
            unset: vec!["HOME".into()],
            cwd: Some(PathBuf::from("/home")),
            execution: Some(ExecutionSettings {
                pty_enabled: Some(false),
                needs: Need::new(),
                sandbox: None,
            }),
        };
        let result = base.apply_delta(&delta);
        assert_eq!(result.env.get("FOO"), Some(&"bar".to_string()));
        assert!(!result.env.contains_key("HOME"));
        assert_eq!(result.cwd, PathBuf::from("/home"));
        assert_eq!(result.execution.pty_enabled, Some(false));
    }

    #[test]
    fn delta_without_execution_inherits_parent_execution() {
        let mut base = test_snapshot();
        base.execution.pty_enabled = Some(false);
        let delta = EnvDelta {
            set: BTreeMap::new(),
            unset: vec![],
            cwd: Some(PathBuf::from("/home")),
            execution: None,
        };
        let result = base.apply_delta(&delta);
        assert_eq!(result.execution, base.execution);
    }

    #[test]
    fn root_scope() {
        let snap = test_snapshot();
        let scope = Scope::root(snap.clone());
        assert_eq!(scope.hash, snap.compute_hash());
        assert!(scope.parent.is_none());
    }

    #[test]
    fn fork_scope() {
        let base = test_snapshot();
        let root = Scope::root(base.clone());
        let delta = EnvDelta {
            set: BTreeMap::from([("NEW".into(), "val".into())]),
            unset: vec![],
            cwd: None,
            execution: None,
        };
        let child = Scope::fork(root.hash, &base, delta);
        assert!(child.parent.is_some());
        assert_ne!(child.hash, root.hash);
    }
}
