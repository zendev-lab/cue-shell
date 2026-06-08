use std::ffi::OsString;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use anyhow::Context;
use cue_core::Mode;
use cue_core::command_spec::command_names;

pub(crate) fn builtin_command_candidates(word: &str) -> Vec<String> {
    let prefix = word.strip_prefix(':').unwrap_or(word);
    command_names()
        .chain(["restart"])
        .filter(|command| command.starts_with(prefix))
        .map(|command| format!(":{command}"))
        .collect()
}

pub(crate) fn bare_completion_candidates(
    mode: Mode,
    line_prefix: &str,
    word: &str,
) -> anyhow::Result<Vec<String>> {
    match mode {
        Mode::Job => shell_segment_completion_candidates(line_prefix, word),
        Mode::Cron => cron_completion_candidates(line_prefix, word),
    }
}

fn cron_completion_candidates(line_prefix: &str, word: &str) -> anyhow::Result<Vec<String>> {
    const KEYWORDS: &[&str] = &[
        "every", "in", "at", "on", "daily", "hourly", "weekly", "monthly", "cron",
    ];

    if let Some(command_start) = cron_command_start(line_prefix) {
        return shell_segment_completion_candidates(&line_prefix[command_start..], word);
    }

    Ok(KEYWORDS
        .iter()
        .filter(|keyword| keyword.starts_with(word))
        .map(|keyword| keyword.to_string())
        .collect())
}

fn cron_command_start(line_prefix: &str) -> Option<usize> {
    let trimmed = line_prefix.trim_start();
    let leading = line_prefix.len().saturating_sub(trimmed.len());
    let tokens = token_spans(trimmed);
    let first = tokens.first()?.0;

    let start_after = match first {
        "daily" | "hourly" | "weekly" | "monthly" => 1,
        "every" | "in" => 2,
        "cron" => 6,
        "at" => {
            if tokens.len() >= 4 && tokens.get(2).is_some_and(|token| token.0 == "on") {
                4
            } else {
                2
            }
        }
        "on" => {
            if tokens.len() >= 4 && tokens.get(2).is_some_and(|token| token.0 == "at") {
                4
            } else {
                2
            }
        }
        _ => return None,
    };

    if tokens.len() < start_after {
        return None;
    }
    let (_, _, end) = tokens[start_after - 1];
    Some(leading + end + 1)
}

fn shell_segment_completion_candidates(
    line_prefix: &str,
    word: &str,
) -> anyhow::Result<Vec<String>> {
    let tokens = line_prefix.split_whitespace().collect::<Vec<_>>();
    let segment_start = tokens
        .iter()
        .rposition(|token| is_chain_operator(token))
        .map_or(0, |index| index + 1);
    let segment_token_count = tokens.len().saturating_sub(segment_start);
    let ends_with_whitespace = line_prefix.chars().last().is_some_and(char::is_whitespace);
    let completing_command = if ends_with_whitespace {
        line_prefix.trim().is_empty()
            || tokens.last().is_some_and(|token| is_chain_operator(token))
            || segment_token_count == 0
    } else {
        line_prefix.trim().is_empty() || segment_token_count <= 1
    };

    let mut candidates = path_completion_candidates(word)?;
    if completing_command {
        candidates.extend(command_completion_candidates(word));
    }
    candidates.sort();
    candidates.dedup();
    Ok(candidates)
}

fn is_chain_operator(token: &str) -> bool {
    matches!(token, "->" | "~>" | "|||" | "|?|")
}

fn command_completion_candidates(prefix: &str) -> Vec<String> {
    if prefix.contains('/') || prefix.starts_with('~') {
        return Vec::new();
    }

    let Some(paths) = std::env::var_os("PATH") else {
        return Vec::new();
    };
    command_completion_candidates_from_paths(prefix, std::env::split_paths(&paths))
}

fn command_completion_candidates_from_paths(
    prefix: &str,
    paths: impl IntoIterator<Item = PathBuf>,
) -> Vec<String> {
    let mut candidates = Vec::new();
    for dir in paths {
        let Ok(entries) = fs::read_dir(dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if name.starts_with(prefix) && is_executable_file(&path) {
                candidates.push(name.to_string());
            }
        }
    }
    candidates
}

#[cfg(unix)]
fn is_executable_file(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;

    fs::metadata(path)
        .map(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn is_executable_file(path: &Path) -> bool {
    path.is_file()
}

fn path_completion_candidates(prefix: &str) -> anyhow::Result<Vec<String>> {
    let Some((base_dir, partial, display_prefix)) = path_completion_context(prefix)? else {
        return Ok(Vec::new());
    };
    let entries = match fs::read_dir(&base_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("read completion directory {}", base_dir.display()));
        }
    };

    let mut candidates = Vec::new();
    for entry in entries {
        let entry =
            entry.with_context(|| format!("read completion entry in {}", base_dir.display()))?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !name.starts_with(&partial) {
            continue;
        }
        let suffix = if path.is_dir() { "/" } else { "" };
        candidates.push(format!("{display_prefix}{name}{suffix}"));
    }
    Ok(candidates)
}

fn path_completion_context(prefix: &str) -> anyhow::Result<Option<(PathBuf, String, String)>> {
    path_completion_context_from_sources(prefix, std::env::var_os("HOME"), std::env::current_dir)
}

fn path_completion_context_from_sources(
    prefix: &str,
    home: Option<OsString>,
    mut current_dir: impl FnMut() -> io::Result<PathBuf>,
) -> anyhow::Result<Option<(PathBuf, String, String)>> {
    if prefix == "~" {
        return Ok(home_path_from_env(home).map(|home| (home, String::new(), "~/".into())));
    }
    if let Some(rest) = prefix.strip_prefix("~/") {
        let Some(home) = home_path_from_env(home) else {
            return Ok(None);
        };
        return path_completion_context_from_expanded(prefix, home.join(rest), &mut current_dir);
    }

    path_completion_context_from_expanded(prefix, PathBuf::from(prefix), &mut current_dir)
}

fn path_completion_context_from_expanded<F>(
    prefix: &str,
    expanded: PathBuf,
    current_dir: &mut F,
) -> anyhow::Result<Option<(PathBuf, String, String)>>
where
    F: FnMut() -> io::Result<PathBuf>,
{
    let path = Path::new(&expanded);

    if prefix.ends_with('/') {
        let base = if expanded.as_os_str().is_empty() {
            current_dir().context("resolve current directory for path completion")?
        } else {
            expanded
        };
        return Ok(Some((base, String::new(), prefix.to_string())));
    }

    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    let base_dir = match parent {
        Some(parent) => PathBuf::from(parent),
        None => current_dir().context("resolve current directory for path completion")?,
    };
    let partial = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_string();
    let display_prefix = prefix
        .rfind('/')
        .map(|index| prefix[..=index].to_string())
        .unwrap_or_default();

    Ok(Some((base_dir, partial, display_prefix)))
}

fn home_path_from_env(home: Option<OsString>) -> Option<PathBuf> {
    non_empty_env(home).map(PathBuf::from)
}

fn non_empty_env(value: Option<OsString>) -> Option<OsString> {
    value.filter(|value| !value.is_empty())
}

fn token_spans(input: &str) -> Vec<(&str, usize, usize)> {
    let mut tokens = Vec::new();
    let mut start = None;
    for (index, ch) in input.char_indices() {
        if ch.is_whitespace() {
            if let Some(token_start) = start.take() {
                tokens.push((&input[token_start..index], token_start, index));
            }
        } else if start.is_none() {
            start = Some(index);
        }
    }
    if let Some(token_start) = start {
        tokens.push((&input[token_start..], token_start, input.len()));
    }
    tokens
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn command_completion_ignores_non_executable_files_on_unix() {
        use std::os::unix::fs::PermissionsExt;
        use std::sync::atomic::{AtomicU64, Ordering};

        static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

        let dir = std::env::temp_dir().join(format!(
            "cue-tui-command-completion-{}-{}",
            std::process::id(),
            TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&dir).expect("create temp completion dir");
        let executable = dir.join("cue-demo");
        let non_executable = dir.join("cue-draft");
        fs::write(&executable, "#!/bin/sh\n").expect("write executable candidate");
        fs::write(&non_executable, "#!/bin/sh\n").expect("write non-executable candidate");
        let mut permissions = fs::metadata(&executable)
            .expect("stat executable candidate")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&executable, permissions).expect("chmod executable candidate");

        let candidates = command_completion_candidates_from_paths("cue-d", [dir.clone()]);

        assert_eq!(candidates, vec!["cue-demo".to_string()]);

        fs::remove_dir_all(dir).expect("remove temp completion dir");
    }

    #[test]
    fn path_completion_context_uses_home_for_tilde_prefixes() {
        let (base, partial, display_prefix) = path_completion_context_from_sources(
            "~/src/app",
            Some(OsString::from("/home/test")),
            || Ok(PathBuf::from("/work")),
        )
        .expect("tilde path should not fail")
        .expect("tilde path should resolve with HOME");

        assert_eq!(base, PathBuf::from("/home/test/src"));
        assert_eq!(partial, "app");
        assert_eq!(display_prefix, "~/src/");

        let (base, partial, display_prefix) =
            path_completion_context_from_sources("~", Some(OsString::from("/home/test")), || {
                Ok(PathBuf::from("/work"))
            })
            .expect("bare tilde should not fail")
            .expect("bare tilde should resolve with HOME");

        assert_eq!(base, PathBuf::from("/home/test"));
        assert_eq!(partial, "");
        assert_eq!(display_prefix, "~/");
    }

    #[test]
    fn path_completion_context_rejects_tilde_without_home() {
        assert!(
            path_completion_context_from_sources("~/src", None, || Ok(PathBuf::from("/work")))
                .expect("missing HOME is not a filesystem error")
                .is_none()
        );
        assert!(
            path_completion_context_from_sources("~/src", Some(OsString::new()), || Ok(
                PathBuf::from("/work")
            ),)
            .expect("empty HOME is not a filesystem error")
            .is_none()
        );
    }

    #[test]
    fn relative_path_completion_context_does_not_require_home() {
        let (base, partial, display_prefix) =
            path_completion_context_from_sources("src/app", None, || {
                panic!("relative path with a parent should not read current_dir")
            })
            .expect("relative path should not need HOME")
            .expect("relative path should have completion context");

        assert_eq!(base, PathBuf::from("src"));
        assert_eq!(partial, "app");
        assert_eq!(display_prefix, "src/");
    }

    #[test]
    fn bare_relative_path_completion_requires_current_dir() {
        let (base, partial, display_prefix) =
            path_completion_context_from_sources("app", None, || Ok(PathBuf::from("/work")))
                .expect("bare relative path should use current dir")
                .expect("bare relative path should have completion context");

        assert_eq!(base, PathBuf::from("/work"));
        assert_eq!(partial, "app");
        assert_eq!(display_prefix, "");
    }

    #[test]
    fn bare_relative_path_completion_reports_current_dir_failure() {
        let error = path_completion_context_from_sources("app", None, || {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                "current directory was removed",
            ))
        })
        .expect_err("bare relative path completion should not hide current_dir errors");

        let message = format!("{error:#}");
        assert!(message.contains("resolve current directory for path completion"));
        assert!(message.contains("current directory was removed"));
    }
}
