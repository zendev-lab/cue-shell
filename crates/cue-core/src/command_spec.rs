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
    Id(CommandIdKind),
    Tail(CommandIdKind),
    Text,
    TargetText(CommandIdKind),
    OptionalId(CommandIdKind),
    OptionalText,
    Empty,
}

/// Entity ID shape accepted by a command argument.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandIdKind {
    Job,
    Cron,
    JobOrCron,
}

impl CommandIdKind {
    pub fn accepts_job(self) -> bool {
        matches!(self, Self::Job | Self::JobOrCron)
    }

    pub fn accepts_cron(self) -> bool {
        matches!(self, Self::Cron | Self::JobOrCron)
    }

    pub fn display(self) -> &'static str {
        match self {
            Self::Job => "J<n>",
            Self::Cron => "C<n>",
            Self::JobOrCron => "J<n> or C<n>",
        }
    }

    pub fn first_example(self) -> &'static str {
        match self {
            Self::Job | Self::JobOrCron => "J1",
            Self::Cron => "C1",
        }
    }
}

/// Static command metadata shared by parser, help, and completions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommandSpec {
    pub name: &'static str,
    pub category: CommandCategory,
    pub arg_kind: CommandArgKind,
    pub usage: &'static str,
    pub detail: &'static str,
    pub documented: bool,
}

impl CommandSpec {
    pub fn visible_in_category(&self, category: CommandCategory) -> bool {
        if self.category == category {
            return true;
        }

        matches!(
            (self.arg_kind, category),
            (
                CommandArgKind::Id(CommandIdKind::JobOrCron)
                    | CommandArgKind::OptionalId(CommandIdKind::JobOrCron),
                CommandCategory::Job | CommandCategory::Cron
            )
        )
    }

    pub fn accepts_mode_params(&self) -> bool {
        mode_param_specs_for_command(self.name).next().is_some()
    }
}

/// Static mode-parameter metadata for completion and docs checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModeParamSpec {
    pub name: &'static str,
    pub commands: &'static [&'static str],
    pub value_kind: ModeParamValueKind,
    pub value_hint: &'static str,
    pub detail: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModeParamValueKind {
    String,
    Bool,
}

const MODE_PARAM_SPECS: &[ModeParamSpec] = &[
    ModeParamSpec {
        name: "cwd",
        commands: &["run", "cron"],
        value_kind: ModeParamValueKind::String,
        value_hint: "/path",
        detail: "Run from this working directory without moving HEAD",
    },
    ModeParamSpec {
        name: "wrapper",
        commands: &["run", "cron"],
        value_kind: ModeParamValueKind::Bool,
        value_hint: "true",
        detail: "Override the runtime wrapper for this invocation",
    },
    ModeParamSpec {
        name: "scope",
        commands: &["run", "cron"],
        value_kind: ModeParamValueKind::Bool,
        value_hint: "true",
        detail: "Allow run jobs to update the chain scope",
    },
    ModeParamSpec {
        name: "pty",
        commands: &["run"],
        value_kind: ModeParamValueKind::Bool,
        value_hint: "false",
        detail: "Run the job without allocating a PTY",
    },
];

impl ModeParamSpec {
    pub fn applies_to(&self, command: &str) -> bool {
        self.commands.contains(&command)
    }
}

pub const COMMAND_SPECS: &[CommandSpec] = &[
    CommandSpec {
        name: "run",
        category: CommandCategory::Job,
        arg_kind: CommandArgKind::Chain,
        usage: ":run <command>",
        detail: "Run a command chain as durable job(s)",
        documented: true,
    },
    CommandSpec {
        name: "cron",
        category: CommandCategory::Cron,
        arg_kind: CommandArgKind::Cron,
        usage: ":cron <schedule> <command>",
        detail: "Register a scheduled command chain",
        documented: true,
    },
    CommandSpec {
        name: "kill",
        category: CommandCategory::Job,
        arg_kind: CommandArgKind::Id(CommandIdKind::JobOrCron),
        usage: ":kill <id>",
        detail: "Stop a running job or remove a cron entry",
        documented: true,
    },
    CommandSpec {
        name: "retry",
        category: CommandCategory::Job,
        arg_kind: CommandArgKind::Id(CommandIdKind::Job),
        usage: ":retry J<n>",
        detail: "Rerun a failed job from its original start scope",
        documented: true,
    },
    CommandSpec {
        name: "out",
        category: CommandCategory::Job,
        arg_kind: CommandArgKind::Id(CommandIdKind::Job),
        usage: ":out J<n>",
        detail: "Read a stdout snapshot",
        documented: true,
    },
    CommandSpec {
        name: "tail",
        category: CommandCategory::Job,
        arg_kind: CommandArgKind::Tail(CommandIdKind::Job),
        usage: ":tail J<n> [bytes]",
        detail: "Follow live stdout, optionally capped to bytes",
        documented: true,
    },
    CommandSpec {
        name: "err",
        category: CommandCategory::Job,
        arg_kind: CommandArgKind::Id(CommandIdKind::Job),
        usage: ":err J<n>",
        detail: "Read stderr or merged PTY output",
        documented: true,
    },
    CommandSpec {
        name: "fg",
        category: CommandCategory::Job,
        arg_kind: CommandArgKind::Id(CommandIdKind::Job),
        usage: ":fg J<n>",
        detail: "Attach a running PTY job in the foreground",
        documented: true,
    },
    CommandSpec {
        name: "wait",
        category: CommandCategory::Job,
        arg_kind: CommandArgKind::Id(CommandIdKind::Job),
        usage: ":wait J<n>",
        detail: "Wait until a job reaches a terminal state",
        documented: true,
    },
    CommandSpec {
        name: "send",
        category: CommandCategory::Job,
        arg_kind: CommandArgKind::TargetText(CommandIdKind::Job),
        usage: ":send J<n> <input>",
        detail: "Write stdin to a running job",
        documented: true,
    },
    CommandSpec {
        name: "cancel",
        category: CommandCategory::Job,
        arg_kind: CommandArgKind::Id(CommandIdKind::Job),
        usage: ":cancel J<n>",
        detail: "Cancel a pending or running job",
        documented: true,
    },
    CommandSpec {
        name: "pause",
        category: CommandCategory::Cron,
        arg_kind: CommandArgKind::Id(CommandIdKind::Cron),
        usage: ":pause C<n>",
        detail: "Pause a scheduled cron entry",
        documented: true,
    },
    CommandSpec {
        name: "resume",
        category: CommandCategory::Cron,
        arg_kind: CommandArgKind::Id(CommandIdKind::Cron),
        usage: ":resume C<n>",
        detail: "Resume a paused cron entry",
        documented: true,
    },
    CommandSpec {
        name: "log",
        category: CommandCategory::Job,
        arg_kind: CommandArgKind::OptionalId(CommandIdKind::JobOrCron),
        usage: ":log [id]",
        detail: "Show historical job and cron activity",
        documented: true,
    },
    CommandSpec {
        name: "jobs",
        category: CommandCategory::Job,
        arg_kind: CommandArgKind::Empty,
        usage: ":jobs",
        detail: "List known jobs",
        documented: true,
    },
    CommandSpec {
        name: "crons",
        category: CommandCategory::Cron,
        arg_kind: CommandArgKind::Empty,
        usage: ":crons",
        detail: "List persisted cron entries",
        documented: true,
    },
    CommandSpec {
        name: "scopes",
        category: CommandCategory::Scope,
        arg_kind: CommandArgKind::Empty,
        usage: ":scopes",
        detail: "List known scope snapshots",
        documented: true,
    },
    CommandSpec {
        name: "env",
        category: CommandCategory::Scope,
        arg_kind: CommandArgKind::OptionalText,
        usage: ":env [subcommand]",
        detail: "Inspect or update the persisted HEAD environment",
        documented: true,
    },
    CommandSpec {
        name: "cd",
        category: CommandCategory::Scope,
        arg_kind: CommandArgKind::OptionalText,
        usage: ":cd <path>",
        detail: "Move the persisted HEAD working directory",
        documented: true,
    },
    CommandSpec {
        name: "scope",
        category: CommandCategory::Scope,
        arg_kind: CommandArgKind::OptionalText,
        usage: ":scope list",
        detail: "List scope snapshots",
        documented: true,
    },
    CommandSpec {
        name: "help",
        category: CommandCategory::System,
        arg_kind: CommandArgKind::OptionalText,
        usage: ":help [topic]",
        detail: "Show command and mode help",
        documented: true,
    },
    CommandSpec {
        name: "config",
        category: CommandCategory::System,
        arg_kind: CommandArgKind::OptionalText,
        usage: ":config [subcommand]",
        detail: "Inspect runtime configuration",
        documented: true,
    },
    CommandSpec {
        name: "wrap",
        category: CommandCategory::System,
        arg_kind: CommandArgKind::OptionalText,
        usage: ":wrap [on|off|status]",
        detail: "Override or inspect the runtime wrapper",
        documented: true,
    },
    CommandSpec {
        name: "clear",
        category: CommandCategory::System,
        arg_kind: CommandArgKind::Empty,
        usage: ":clear",
        detail: "Clear the frontend input/output view",
        documented: true,
    },
    CommandSpec {
        name: "quit",
        category: CommandCategory::System,
        arg_kind: CommandArgKind::Empty,
        usage: ":quit",
        detail: "Quit the frontend",
        documented: true,
    },
    CommandSpec {
        name: "exit",
        category: CommandCategory::System,
        arg_kind: CommandArgKind::Empty,
        usage: ":exit",
        detail: "Alias for :quit",
        documented: true,
    },
];

pub fn command_spec(name: &str) -> Option<&'static CommandSpec> {
    COMMAND_SPECS.iter().find(|spec| spec.name == name)
}

pub fn mode_param_spec(name: &str) -> Option<&'static ModeParamSpec> {
    MODE_PARAM_SPECS.iter().find(|spec| spec.name == name)
}

pub fn mode_param_spec_for_command(command: &str, name: &str) -> Option<&'static ModeParamSpec> {
    MODE_PARAM_SPECS
        .iter()
        .find(|spec| spec.name == name && spec.applies_to(command))
}

pub fn mode_param_specs_for_command(
    command: &str,
) -> impl Iterator<Item = &'static ModeParamSpec> + '_ {
    MODE_PARAM_SPECS
        .iter()
        .filter(move |spec| spec.applies_to(command))
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
    fn mode_param_names_are_unique() {
        let mut names = std::collections::BTreeSet::new();
        for spec in MODE_PARAM_SPECS {
            assert!(
                names.insert(spec.name),
                "duplicate mode parameter `{}`",
                spec.name
            );
        }
    }

    #[test]
    fn mode_param_command_boundaries_are_explicit() {
        assert!(mode_param_spec_for_command("run", "pty").is_some());
        assert!(mode_param_spec_for_command("cron", "pty").is_none());
        assert!(mode_param_spec_for_command("cron", "cwd").is_some());
        assert!(command_spec("run").is_some_and(CommandSpec::accepts_mode_params));
        assert!(command_spec("cron").is_some_and(CommandSpec::accepts_mode_params));
        assert!(!command_spec("kill").is_some_and(CommandSpec::accepts_mode_params));
    }

    #[test]
    fn id_command_boundaries_are_explicit() {
        assert_eq!(
            command_spec("fg").map(|spec| spec.arg_kind),
            Some(CommandArgKind::Id(CommandIdKind::Job))
        );
        assert_eq!(
            command_spec("pause").map(|spec| spec.arg_kind),
            Some(CommandArgKind::Id(CommandIdKind::Cron))
        );
        assert_eq!(
            command_spec("kill").map(|spec| spec.arg_kind),
            Some(CommandArgKind::Id(CommandIdKind::JobOrCron))
        );
        assert_eq!(
            command_spec("log").map(|spec| spec.arg_kind),
            Some(CommandArgKind::OptionalId(CommandIdKind::JobOrCron))
        );
        assert_eq!(
            command_spec("send").map(|spec| spec.arg_kind),
            Some(CommandArgKind::TargetText(CommandIdKind::Job))
        );
    }

    #[test]
    fn cross_entity_commands_are_visible_in_each_help_category() {
        let kill = command_spec("kill").expect("kill command spec");
        assert!(kill.visible_in_category(CommandCategory::Job));
        assert!(kill.visible_in_category(CommandCategory::Cron));

        let log = command_spec("log").expect("log command spec");
        assert!(log.visible_in_category(CommandCategory::Job));
        assert!(log.visible_in_category(CommandCategory::Cron));

        let pause = command_spec("pause").expect("pause command spec");
        assert!(!pause.visible_in_category(CommandCategory::Job));
        assert!(pause.visible_in_category(CommandCategory::Cron));
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
