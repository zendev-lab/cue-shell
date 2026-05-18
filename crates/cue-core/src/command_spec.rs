//! Shared metadata for daemon-handled `:` commands.
//!
//! Keep command names, parser argument shape, help grouping, and completion
//! details in one place so frontends and the daemon do not drift apart.

/// High-level command grouping used by help and documentation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandCategory {
    Job,
    Cron,
    Scope,
    System,
}

/// Parser-facing argument classification for a `:` command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandArgKind {
    Chain,
    Cron,
    Id,
    Tail,
    Text,
    OptionalId,
    OptionalText,
    Empty,
}

/// Static command metadata shared by parser, help, and completions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommandSpec {
    pub name: &'static str,
    pub category: CommandCategory,
    pub arg_kind: CommandArgKind,
    pub usage: &'static str,
    pub detail: &'static str,
    pub accepts_mode_params: bool,
    pub documented: bool,
}

/// Static mode-parameter metadata for completion and docs checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModeParamSpec {
    pub name: &'static str,
    pub value_hint: &'static str,
    pub detail: &'static str,
}

pub const MODE_PARAM_SPECS: &[ModeParamSpec] = &[
    ModeParamSpec {
        name: "cwd",
        value_hint: "/path",
        detail: "Run from this working directory without moving HEAD",
    },
    ModeParamSpec {
        name: "retry",
        value_hint: "3",
        detail: "Retry failed jobs up to this count",
    },
    ModeParamSpec {
        name: "retry_delay",
        value_hint: "5s",
        detail: "Delay between retry attempts",
    },
    ModeParamSpec {
        name: "timeout",
        value_hint: "30s",
        detail: "Fail the job after this duration",
    },
    ModeParamSpec {
        name: "wrapper",
        value_hint: "true",
        detail: "Override the runtime wrapper for this invocation",
    },
    ModeParamSpec {
        name: "scope",
        value_hint: "true",
        detail: "Allow run jobs to update the chain scope",
    },
];

pub const COMMAND_SPECS: &[CommandSpec] = &[
    CommandSpec {
        name: "run",
        category: CommandCategory::Job,
        arg_kind: CommandArgKind::Chain,
        usage: ":run <command>",
        detail: "Run a command chain as durable job(s)",
        accepts_mode_params: true,
        documented: true,
    },
    CommandSpec {
        name: "cron",
        category: CommandCategory::Cron,
        arg_kind: CommandArgKind::Cron,
        usage: ":cron <schedule> <command>",
        detail: "Register a scheduled command chain",
        accepts_mode_params: true,
        documented: true,
    },
    CommandSpec {
        name: "kill",
        category: CommandCategory::Job,
        arg_kind: CommandArgKind::Id,
        usage: ":kill <id>",
        detail: "Stop a running job or remove a cron entry",
        accepts_mode_params: false,
        documented: true,
    },
    CommandSpec {
        name: "retry",
        category: CommandCategory::Job,
        arg_kind: CommandArgKind::Id,
        usage: ":retry J<n>",
        detail: "Rerun a failed job from its original start scope",
        accepts_mode_params: false,
        documented: true,
    },
    CommandSpec {
        name: "out",
        category: CommandCategory::Job,
        arg_kind: CommandArgKind::Id,
        usage: ":out J<n>",
        detail: "Read a stdout snapshot",
        accepts_mode_params: false,
        documented: true,
    },
    CommandSpec {
        name: "tail",
        category: CommandCategory::Job,
        arg_kind: CommandArgKind::Tail,
        usage: ":tail J<n> [bytes]",
        detail: "Follow live stdout, optionally capped to bytes",
        accepts_mode_params: false,
        documented: true,
    },
    CommandSpec {
        name: "err",
        category: CommandCategory::Job,
        arg_kind: CommandArgKind::Id,
        usage: ":err J<n>",
        detail: "Read stderr or merged PTY output",
        accepts_mode_params: false,
        documented: true,
    },
    CommandSpec {
        name: "fg",
        category: CommandCategory::Job,
        arg_kind: CommandArgKind::Id,
        usage: ":fg J<n>",
        detail: "Attach a running PTY job in the foreground",
        accepts_mode_params: false,
        documented: true,
    },
    CommandSpec {
        name: "wait",
        category: CommandCategory::Job,
        arg_kind: CommandArgKind::Id,
        usage: ":wait J<n>",
        detail: "Wait until a job reaches a terminal state",
        accepts_mode_params: false,
        documented: true,
    },
    CommandSpec {
        name: "send",
        category: CommandCategory::Job,
        arg_kind: CommandArgKind::Text,
        usage: ":send J<n> <input>",
        detail: "Write stdin to a running job",
        accepts_mode_params: false,
        documented: true,
    },
    CommandSpec {
        name: "cancel",
        category: CommandCategory::Job,
        arg_kind: CommandArgKind::Id,
        usage: ":cancel J<n>",
        detail: "Cancel a pending or running job",
        accepts_mode_params: false,
        documented: true,
    },
    CommandSpec {
        name: "pause",
        category: CommandCategory::Cron,
        arg_kind: CommandArgKind::Id,
        usage: ":pause C<n>",
        detail: "Pause a scheduled cron entry",
        accepts_mode_params: false,
        documented: true,
    },
    CommandSpec {
        name: "resume",
        category: CommandCategory::Cron,
        arg_kind: CommandArgKind::Id,
        usage: ":resume C<n>",
        detail: "Resume a paused cron entry",
        accepts_mode_params: false,
        documented: true,
    },
    CommandSpec {
        name: "log",
        category: CommandCategory::Job,
        arg_kind: CommandArgKind::OptionalId,
        usage: ":log [id]",
        detail: "Show historical job and cron activity",
        accepts_mode_params: false,
        documented: true,
    },
    CommandSpec {
        name: "jobs",
        category: CommandCategory::Job,
        arg_kind: CommandArgKind::Empty,
        usage: ":jobs",
        detail: "List known jobs",
        accepts_mode_params: false,
        documented: true,
    },
    CommandSpec {
        name: "crons",
        category: CommandCategory::Cron,
        arg_kind: CommandArgKind::Empty,
        usage: ":crons",
        detail: "List persisted cron entries",
        accepts_mode_params: false,
        documented: true,
    },
    CommandSpec {
        name: "scopes",
        category: CommandCategory::Scope,
        arg_kind: CommandArgKind::Empty,
        usage: ":scopes",
        detail: "List known scope snapshots",
        accepts_mode_params: false,
        documented: true,
    },
    CommandSpec {
        name: "env",
        category: CommandCategory::Scope,
        arg_kind: CommandArgKind::OptionalText,
        usage: ":env [subcommand]",
        detail: "Inspect or update the persisted HEAD environment",
        accepts_mode_params: false,
        documented: true,
    },
    CommandSpec {
        name: "cd",
        category: CommandCategory::Scope,
        arg_kind: CommandArgKind::OptionalText,
        usage: ":cd <path>",
        detail: "Move the persisted HEAD working directory",
        accepts_mode_params: false,
        documented: true,
    },
    CommandSpec {
        name: "scope",
        category: CommandCategory::Scope,
        arg_kind: CommandArgKind::OptionalText,
        usage: ":scope [subcommand]",
        detail: "Inspect or manage scope snapshots",
        accepts_mode_params: true,
        documented: true,
    },
    CommandSpec {
        name: "help",
        category: CommandCategory::System,
        arg_kind: CommandArgKind::OptionalText,
        usage: ":help [topic]",
        detail: "Show command and mode help",
        accepts_mode_params: false,
        documented: true,
    },
    CommandSpec {
        name: "config",
        category: CommandCategory::System,
        arg_kind: CommandArgKind::OptionalText,
        usage: ":config [subcommand]",
        detail: "Inspect runtime configuration",
        accepts_mode_params: false,
        documented: true,
    },
    CommandSpec {
        name: "wrap",
        category: CommandCategory::System,
        arg_kind: CommandArgKind::OptionalText,
        usage: ":wrap [on|off|status]",
        detail: "Override or inspect the runtime wrapper",
        accepts_mode_params: false,
        documented: true,
    },
    CommandSpec {
        name: "clear",
        category: CommandCategory::System,
        arg_kind: CommandArgKind::Empty,
        usage: ":clear",
        detail: "Clear the frontend input/output view",
        accepts_mode_params: false,
        documented: true,
    },
    CommandSpec {
        name: "quit",
        category: CommandCategory::System,
        arg_kind: CommandArgKind::Empty,
        usage: ":quit",
        detail: "Quit the frontend",
        accepts_mode_params: false,
        documented: true,
    },
    CommandSpec {
        name: "exit",
        category: CommandCategory::System,
        arg_kind: CommandArgKind::Empty,
        usage: ":exit",
        detail: "Alias for :quit",
        accepts_mode_params: false,
        documented: true,
    },
];

pub fn command_spec(name: &str) -> Option<&'static CommandSpec> {
    COMMAND_SPECS.iter().find(|spec| spec.name == name)
}

pub fn command_names() -> impl Iterator<Item = &'static str> {
    COMMAND_SPECS.iter().map(|spec| spec.name)
}

pub fn command_suggestions(name: &str) -> Vec<&'static str> {
    COMMAND_SPECS
        .iter()
        .filter(|spec| {
            spec.name.starts_with(&name[..1.min(name.len())]) || edit_distance(name, spec.name) <= 2
        })
        .map(|spec| spec.name)
        .collect()
}

fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut dp = vec![vec![0usize; b.len() + 1]; a.len() + 1];
    for (i, row) in dp.iter_mut().enumerate() {
        row[0] = i;
    }
    for (j, cell) in dp[0].iter_mut().enumerate() {
        *cell = j;
    }
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            dp[i][j] = (dp[i - 1][j] + 1)
                .min(dp[i][j - 1] + 1)
                .min(dp[i - 1][j - 1] + cost);
        }
    }
    dp[a.len()][b.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_names_are_unique() {
        let mut names = std::collections::BTreeSet::new();
        for spec in COMMAND_SPECS {
            assert!(names.insert(spec.name), "duplicate command `{}`", spec.name);
        }
    }

    #[test]
    fn suggestions_include_close_matches() {
        assert!(command_suggestions("rn").contains(&"run"));
        assert!(command_suggestions("crn").contains(&"cron"));
    }

    #[test]
    fn parser_design_docs_cover_public_commands() {
        let docs = include_str!("../../../docs/design/parser.md");
        for spec in COMMAND_SPECS.iter().filter(|spec| spec.documented) {
            assert!(
                docs.contains(&format!("`:{}{}", spec.name, "`")),
                "docs/design/parser.md is missing `:{}`",
                spec.name
            );
        }
    }

    #[test]
    fn parser_design_docs_cover_mode_params() {
        let docs = include_str!("../../../docs/design/parser.md");
        for spec in MODE_PARAM_SPECS {
            assert!(
                docs.contains(spec.name),
                "docs/design/parser.md is missing mode param `{}`",
                spec.name
            );
        }
    }
}
