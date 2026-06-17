use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::id::ScopeHash;

/// Immutable, content-addressed environment snapshot.
///
/// Scope ≈ git commit: immutable, content-addressed.
/// `:env set` / `:cd` create a new Scope and move the owning session cursor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scope {
    /// blake3(canonical_bytes(env + cwd))
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
    // Future: umask, aliases, functions, shell_options, traps
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
        Self { env, cwd }
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
    use insta::assert_json_snapshot;

    fn test_snapshot() -> EnvSnapshot {
        let mut env = BTreeMap::new();
        env.insert("PATH".into(), "/usr/bin".into());
        env.insert("HOME".into(), "/home/test".into());
        EnvSnapshot {
            env,
            cwd: PathBuf::from("/tmp"),
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
    fn snapshot_json_contains_only_env_and_cwd() {
        assert_json_snapshot!(test_snapshot(), @r###"
        {
          "env": {
            "HOME": "/home/test",
            "PATH": "/usr/bin"
          },
          "cwd": "/tmp"
        }
        "###);
    }

    #[test]
    fn apply_delta() {
        let base = test_snapshot();
        let delta = EnvDelta {
            set: BTreeMap::from([("FOO".into(), "bar".into())]),
            unset: vec!["HOME".into()],
            cwd: Some(PathBuf::from("/home")),
        };
        let result = base.apply_delta(&delta);
        assert_eq!(result.env.get("FOO"), Some(&"bar".to_string()));
        assert!(!result.env.contains_key("HOME"));
        assert_eq!(result.cwd, PathBuf::from("/home"));
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
        };
        let child = Scope::fork(root.hash, &base, delta);
        assert!(child.parent.is_some());
        assert_ne!(child.hash, root.hash);
    }
}
