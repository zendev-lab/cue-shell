use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::path::PathBuf;

use cue_core::scope::EnvSnapshot;

const AMBIENT_OVERRIDE_KEYS: &[&str] = &[
    "COLORTERM",
    "DBUS_SESSION_BUS_ADDRESS",
    "DISPLAY",
    "GH_TOKEN",
    "GITHUB_TOKEN",
    "GPG_AGENT_INFO",
    "GPG_TTY",
    "SSH_AGENT_PID",
    "SSH_AUTH_SOCK",
    "TERM",
    "TERM_PROGRAM",
    "TMPDIR",
    "WAYLAND_DISPLAY",
    "XAUTHORITY",
];

pub fn effective_snapshot(snapshot: &EnvSnapshot) -> EnvSnapshot {
    let ambient: BTreeMap<String, String> = std::env::vars().collect();
    EnvSnapshot {
        env: merge_snapshot_env(snapshot, &ambient),
        cwd: snapshot.cwd.clone(),
    }
}

fn merge_snapshot_env(
    snapshot: &EnvSnapshot,
    ambient: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut merged = snapshot.env.clone();
    for (key, value) in ambient {
        if is_path_like_key(key) {
            match merged.get(key) {
                Some(existing) => {
                    merged.insert(key.clone(), merge_path_like(existing, value));
                }
                None => {
                    merged.insert(key.clone(), value.clone());
                }
            }
            continue;
        }
        if AMBIENT_OVERRIDE_KEYS.contains(&key.as_str()) {
            merged.insert(key.clone(), value.clone());
            continue;
        }
        merged.entry(key.clone()).or_insert_with(|| value.clone());
    }
    merged
}

fn is_path_like_key(key: &str) -> bool {
    key == "PATH" || key.ends_with("PATH")
}

fn merge_path_like(existing: &str, ambient: &str) -> String {
    if existing.is_empty() {
        return ambient.to_string();
    }
    if ambient.is_empty() {
        return existing.to_string();
    }

    let mut ordered = Vec::new();
    let mut seen = BTreeSet::<PathBuf>::new();
    for path in std::env::split_paths(&OsString::from(existing))
        .chain(std::env::split_paths(&OsString::from(ambient)))
    {
        if seen.insert(path.clone()) {
            ordered.push(path);
        }
    }

    std::env::join_paths(&ordered)
        .ok()
        .and_then(|joined| joined.into_string().ok())
        .unwrap_or_else(|| existing.to_string())
}

#[cfg(test)]
mod tests {
    use super::merge_snapshot_env;
    use cue_core::scope::EnvSnapshot;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    #[test]
    fn merges_missing_ambient_variables() {
        let snapshot = EnvSnapshot {
            env: BTreeMap::from([(String::from("FOO"), String::from("bar"))]),
            cwd: PathBuf::from("/tmp"),
        };
        let ambient = BTreeMap::from([(String::from("BAR"), String::from("baz"))]);

        let merged = merge_snapshot_env(&snapshot, &ambient);

        assert_eq!(merged.get("FOO").map(String::as_str), Some("bar"));
        assert_eq!(merged.get("BAR").map(String::as_str), Some("baz"));
    }

    #[test]
    fn appends_missing_path_entries_from_ambient_env() {
        let snapshot = EnvSnapshot {
            env: BTreeMap::from([(
                String::from("PATH"),
                String::from("/snapshot/bin:/shared/bin"),
            )]),
            cwd: PathBuf::from("/tmp"),
        };
        let ambient = BTreeMap::from([(
            String::from("PATH"),
            String::from("/ambient/bin:/shared/bin"),
        )]);

        let merged = merge_snapshot_env(&snapshot, &ambient);

        assert_eq!(
            merged.get("PATH").map(String::as_str),
            Some("/snapshot/bin:/shared/bin:/ambient/bin")
        );
    }

    #[test]
    fn refreshes_ambient_session_variables() {
        let snapshot = EnvSnapshot {
            env: BTreeMap::from([(String::from("SSH_AUTH_SOCK"), String::from("/stale.sock"))]),
            cwd: PathBuf::from("/tmp"),
        };
        let ambient =
            BTreeMap::from([(String::from("SSH_AUTH_SOCK"), String::from("/fresh.sock"))]);

        let merged = merge_snapshot_env(&snapshot, &ambient);

        assert_eq!(
            merged.get("SSH_AUTH_SOCK").map(String::as_str),
            Some("/fresh.sock")
        );
    }
}
