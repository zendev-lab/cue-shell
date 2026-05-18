//! Resolver: Ast → validated execution request.
//!
//! Responsibilities:
//! 1. Mode injection: BareInput → wraps with default command per mode
//! 2. Argument type validation
//! 3. Mode params merge with config defaults
//! 4. AST → cue_core types conversion

use cue_core::command::{ModeParams, ParamValue};
use cue_core::cron::{CronPreset, CronSchedule, parse_day_filter, parse_time_of_day};
use cue_core::mode::Mode;
use cue_core::pipeline::{self as core_pipeline};

use super::ast::{Argument, Ast, ChainNode, CronScheduleAst, Pipeline, ScriptItemAst};
use super::parse::{ParseError, ParseErrorKind, parse_duration_str};
use super::token::{Span, Value};

/// Resolved command ready for execution.
#[derive(Debug, Clone)]
pub enum ResolvedCommand {
    /// One multiline script submission containing multiple top-level commands.
    Script {
        mode: Mode,
        items: Vec<ResolvedScriptItem>,
    },
    /// Run a chain of jobs.
    Run {
        chain: core_pipeline::ChainNode,
        params: ModeParams,
    },
    /// Add a cron job.
    Cron {
        schedule: CronSchedule,
        chain: core_pipeline::ChainNode,
        params: ModeParams,
    },
    /// Kill a job/session.
    Kill { id: String },
    /// Retry a failed job.
    Retry { id: String },
    /// View stdout.
    Out {
        id: String,
        tail_bytes: Option<usize>,
    },
    /// View stderr.
    Err { id: String },
    /// Foreground attach.
    Fg { id: String },
    /// Wait for job completion.
    Wait { id: String },
    /// Send stdin.
    Send { id: String, data: String },
    /// Cancel a pending job.
    Cancel { id: String },
    /// Pause cron/session.
    Pause { id: String },
    /// Resume cron/session.
    Resume { id: String },
    /// View log.
    Log { id: Option<String> },
    /// List jobs.
    Jobs,
    /// List crons.
    Crons,
    /// List scopes.
    Scopes,
    /// Environment operations.
    Env { subcommand: Option<String> },
    /// Change directory.
    Cd { path: String },
    /// Scope operations.
    Scope { subcommand: Option<String> },
    /// Help.
    Help { topic: Option<String> },
    /// Config operations.
    Config { subcommand: Option<String> },
    /// Clear REPL.
    Clear,
    /// Quit.
    Quit,
    /// Wrapper control: on / off / status.
    Wrap { subcommand: Option<String> },
}

#[derive(Debug, Clone)]
pub struct ResolvedScriptItem {
    pub source: String,
    pub command: Box<ResolvedCommand>,
}

/// Resolve an AST into a command ready for execution.
pub struct Resolver;

impl Resolver {
    pub fn resolve(ast: Ast, mode: Mode) -> Result<ResolvedCommand, ParseError> {
        match ast {
            Ast::Script { items, .. } => Ok(ResolvedCommand::Script {
                mode,
                items: items
                    .into_iter()
                    .map(|item| Self::resolve_script_item(item, mode))
                    .collect::<Result<Vec<_>, _>>()?,
            }),
            Ast::BareInput { argument, span } => Self::resolve_bare(argument, mode, span),
            Ast::Command {
                name,
                mode_params,
                argument,
                span,
            } => Self::resolve_command(&name, mode_params, argument, span),
        }
    }

    fn resolve_script_item(
        item: ScriptItemAst,
        mode: Mode,
    ) -> Result<ResolvedScriptItem, ParseError> {
        Ok(ResolvedScriptItem {
            source: item.source,
            command: Box::new(Self::resolve(*item.statement, mode)?),
        })
    }

    fn resolve_bare(
        argument: Argument,
        mode: Mode,
        span: Span,
    ) -> Result<ResolvedCommand, ParseError> {
        if is_mode_help_request(&argument) {
            return Ok(ResolvedCommand::Help {
                topic: Some(mode_help_topic(mode).to_string()),
            });
        }

        match mode {
            Mode::Job => match argument {
                Argument::Chain(chain) => Ok(ResolvedCommand::Run {
                    chain: convert_chain(chain),
                    params: ModeParams::new(),
                }),
                Argument::Empty => Err(ParseError {
                    span,
                    message: "empty input".into(),
                    kind: super::parse::ParseErrorKind::MissingArgument,
                    suggestions: vec![],
                }),
                _ => Err(ParseError {
                    span,
                    message: "JOB mode expects a command to run".into(),
                    kind: super::parse::ParseErrorKind::UnexpectedToken,
                    suggestions: vec![],
                }),
            },
            Mode::Cron => resolve_bare_cron(argument, span),
        }
    }

    fn resolve_command(
        name: &str,
        mode_params: Vec<(String, Value)>,
        argument: Argument,
        span: Span,
    ) -> Result<ResolvedCommand, ParseError> {
        let params = convert_mode_params(mode_params);

        Ok(match name {
            "run" => match argument {
                Argument::Chain(chain) => ResolvedCommand::Run {
                    chain: convert_chain(chain),
                    params,
                },
                _ => unreachable!("parser guarantees Chain for :run"),
            },
            "cron" => match argument {
                Argument::Chain(chain) => {
                    let (schedule_ast, body) = split_bare_cron_chain(chain, span)?;
                    let schedule =
                        convert_cron_schedule(&schedule_ast).ok_or_else(|| ParseError {
                            span,
                            message: format!(
                                "cannot parse cron schedule from: {}",
                                cron_schedule_display(&schedule_ast)
                            ),
                            kind: ParseErrorKind::InvalidCronSchedule,
                            suggestions: vec!["every 5m".into(), "at 09:00".into(), "daily".into()],
                        })?;
                    ResolvedCommand::Cron {
                        schedule,
                        chain: convert_chain(body),
                        params,
                    }
                }
                _ => unreachable!("parser guarantees chain-like input for :cron"),
            },
            "kill" => ResolvedCommand::Kill {
                id: extract_id(argument, span, "kill")?,
            },
            "retry" => ResolvedCommand::Retry {
                id: extract_id(argument, span, "retry")?,
            },
            "out" => ResolvedCommand::Out {
                id: extract_id(argument, span, "out")?,
                tail_bytes: None,
            },
            "tail" => {
                let (id, bytes) = extract_tail_ref(argument);
                ResolvedCommand::Out {
                    id,
                    tail_bytes: Some(bytes.unwrap_or(8192)),
                }
            }
            "err" => ResolvedCommand::Err {
                id: extract_id(argument, span, "err")?,
            },
            "fg" => ResolvedCommand::Fg {
                id: extract_id(argument, span, "fg")?,
            },
            "wait" => ResolvedCommand::Wait {
                id: extract_id(argument, span, "wait")?,
            },
            "send" => {
                let (id, data) = extract_target_and_text(argument, span, "send")?;
                ResolvedCommand::Send { id, data }
            }
            "cancel" => ResolvedCommand::Cancel {
                id: extract_id(argument, span, "cancel")?,
            },
            "pause" => ResolvedCommand::Pause {
                id: extract_id(argument, span, "pause")?,
            },
            "resume" => ResolvedCommand::Resume {
                id: extract_id(argument, span, "resume")?,
            },
            "log" => ResolvedCommand::Log {
                id: match argument {
                    Argument::IdRef(k, n) => Some(format!("{k}{n}")),
                    _ => None,
                },
            },
            "jobs" => ResolvedCommand::Jobs,
            "crons" => ResolvedCommand::Crons,
            "scopes" => ResolvedCommand::Scopes,
            "env" => ResolvedCommand::Env {
                subcommand: extract_optional_text(argument),
            },
            "cd" => ResolvedCommand::Cd {
                path: extract_text(argument),
            },
            "scope" => ResolvedCommand::Scope {
                subcommand: extract_optional_text(argument),
            },
            "help" => ResolvedCommand::Help {
                topic: extract_optional_text(argument),
            },
            "config" => ResolvedCommand::Config {
                subcommand: extract_optional_text(argument),
            },
            "clear" => ResolvedCommand::Clear,
            "quit" | "exit" => ResolvedCommand::Quit,
            "wrap" => ResolvedCommand::Wrap {
                subcommand: extract_optional_text(argument),
            },
            _ => unreachable!("parser rejects unknown commands"),
        })
    }
}

// ── Conversion helpers ──

fn convert_chain(node: ChainNode) -> core_pipeline::ChainNode {
    match node {
        ChainNode::Leaf(p) => core_pipeline::ChainNode::Leaf(convert_pipeline(p)),
        ChainNode::Serial { op, left, right } => core_pipeline::ChainNode::Serial {
            left: Box::new(convert_chain(*left)),
            op,
            right: Box::new(convert_chain(*right)),
        },
        ChainNode::Parallel { op, left, right } => core_pipeline::ChainNode::Parallel {
            left: Box::new(convert_chain(*left)),
            op,
            right: Box::new(convert_chain(*right)),
        },
    }
}

fn convert_pipeline(p: Pipeline) -> core_pipeline::Pipeline {
    core_pipeline::Pipeline {
        segments: p
            .segments
            .into_iter()
            .map(|s| core_pipeline::PipeSegment {
                command: s.command,
                pipe_to_next: s.pipe_to_next,
            })
            .collect(),
    }
}

fn convert_mode_params(params: Vec<(String, Value)>) -> ModeParams {
    let mut mp = ModeParams::new();
    for (key, value) in params {
        let pv = match value {
            Value::Int(n) => ParamValue::Int(n),
            Value::Duration(d) => ParamValue::Duration(d),
            Value::Str(s) => ParamValue::Str(s),
            Value::Bool(b) => ParamValue::Bool(b),
        };
        mp.insert(key, pv);
    }
    mp
}

fn extract_id(arg: Argument, span: Span, command: &str) -> Result<String, ParseError> {
    match arg {
        Argument::IdRef(k, n) => Ok(format!("{k}{n}")),
        _ => Err(ParseError {
            span,
            message: format!("`:{command}` requires an ID (e.g. J1, C1)"),
            kind: ParseErrorKind::InvalidIdRef,
            suggestions: vec![format!(":{command} J1")],
        }),
    }
}

fn extract_tail_ref(arg: Argument) -> (String, Option<usize>) {
    match arg {
        Argument::TailRef(k, n, bytes) => (format!("{k}{n}"), bytes),
        Argument::IdRef(k, n) => (format!("{k}{n}"), None),
        _ => (String::new(), None),
    }
}

fn extract_text(arg: Argument) -> String {
    match arg {
        Argument::Text(t) => t,
        Argument::Chain(chain) => chain_to_text(&chain),
        _ => String::new(),
    }
}

fn extract_target_and_text(
    arg: Argument,
    span: Span,
    command: &str,
) -> Result<(String, String), ParseError> {
    let text = match arg {
        Argument::Text(text) => text,
        _ => String::new(),
    };
    let trimmed = text.trim();
    let Some((id, rest)) = split_first_word(trimmed) else {
        return Err(ParseError {
            span,
            message: format!("`:{command}` requires a target and input"),
            kind: ParseErrorKind::MissingArgument,
            suggestions: vec![format!(":{command} J1 your input")],
        });
    };
    if rest.trim().is_empty() {
        return Err(ParseError {
            span,
            message: format!("`:{command}` requires input after the target"),
            kind: ParseErrorKind::MissingArgument,
            suggestions: vec![format!(":{command} J1 your input")],
        });
    }
    Ok((id.to_string(), rest.trim().to_string()))
}

fn split_first_word(text: &str) -> Option<(&str, &str)> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Some(index) = trimmed.find(char::is_whitespace) {
        Some((&trimmed[..index], &trimmed[index..]))
    } else {
        Some((trimmed, ""))
    }
}

fn extract_optional_text(arg: Argument) -> Option<String> {
    match arg {
        Argument::Text(t) if !t.is_empty() => Some(t),
        Argument::Empty => None,
        _ => None,
    }
}

/// Convert `CronScheduleAst` → `CronSchedule`.  Returns `None` if the
/// schedule time cannot be parsed (e.g. invalid time format).
fn convert_cron_schedule(ast: &CronScheduleAst) -> Option<CronSchedule> {
    match ast {
        CronScheduleAst::Every(d) => Some(CronSchedule::Interval(*d)),
        CronScheduleAst::At { time, days } => {
            let time_secs = parse_time_of_day(time)?;
            let day_filter = days.as_ref().and_then(|d| parse_day_filter(d));
            Some(CronSchedule::TimeOfDay {
                time_secs,
                days: day_filter,
            })
        }
        CronScheduleAst::In(d) => Some(CronSchedule::Delay(*d)),
        CronScheduleAst::Crontab(expr) => Some(CronSchedule::Crontab(expr.clone())),
        CronScheduleAst::Preset(name) => {
            let preset = match name.as_str() {
                "daily" => CronPreset::Daily,
                "hourly" => CronPreset::Hourly,
                "weekly" => CronPreset::Weekly,
                "monthly" => CronPreset::Monthly,
                _ => return None,
            };
            Some(CronSchedule::Preset(preset))
        }
    }
}

/// Display string for a `CronScheduleAst` (used in error messages).
fn cron_schedule_display(ast: &CronScheduleAst) -> String {
    match ast {
        CronScheduleAst::Every(d) => format!("every {}", format_duration(*d)),
        CronScheduleAst::At { time, days } => match days {
            Some(d) => format!("at {time} on {d}"),
            None => format!("at {time}"),
        },
        CronScheduleAst::In(d) => format!("in {}", format_duration(*d)),
        CronScheduleAst::Crontab(expr) => format!("cron {expr}"),
        CronScheduleAst::Preset(name) => name.clone(),
    }
}

fn format_duration(duration: std::time::Duration) -> String {
    let seconds = duration.as_secs();
    if seconds == 0 {
        return "0s".into();
    }
    if seconds.is_multiple_of(86_400) {
        return format!("{}d", seconds / 86_400);
    }
    if seconds.is_multiple_of(3_600) {
        return format!("{}h", seconds / 3_600);
    }
    if seconds.is_multiple_of(60) {
        return format!("{}m", seconds / 60);
    }
    format!("{seconds}s")
}

/// Convert a chain AST back to text (for legacy prompt-mode bare input).
fn chain_to_text(node: &ChainNode) -> String {
    match node {
        ChainNode::Leaf(p) => p
            .segments
            .iter()
            .map(|s| {
                let cmd = s.command.join(" ");
                match s.pipe_to_next {
                    Some(core_pipeline::PipeOp::Stdout) => format!("{cmd} |>"),
                    Some(core_pipeline::PipeOp::StdoutStderr) => format!("{cmd} |&>"),
                    Some(core_pipeline::PipeOp::StderrOnly) => format!("{cmd} |!>"),
                    None => cmd,
                }
            })
            .collect::<Vec<_>>()
            .join(" "),
        ChainNode::Serial { left, op, right } => {
            let op_str = match op {
                core_pipeline::SerialOp::Then => "->",
                core_pipeline::SerialOp::Always => "~>",
            };
            format!("{} {op_str} {}", chain_to_text(left), chain_to_text(right))
        }
        ChainNode::Parallel { left, op, right } => {
            let op_str = match op {
                core_pipeline::ParallelOp::All => "||",
                core_pipeline::ParallelOp::Race => "||?",
            };
            format!("{} {op_str} {}", chain_to_text(left), chain_to_text(right))
        }
    }
}

fn resolve_bare_cron(argument: Argument, span: Span) -> Result<ResolvedCommand, ParseError> {
    let chain = match argument {
        Argument::Chain(chain) => chain,
        Argument::Empty => {
            return Err(ParseError {
                span,
                message: "`:cron` requires a schedule expression".into(),
                kind: ParseErrorKind::MissingArgument,
                suggestions: vec![":cron every 5m cargo test".into()],
            });
        }
        _ => {
            return Err(ParseError {
                span,
                message: "CRON mode expects a schedule expression and command body".into(),
                kind: ParseErrorKind::UnexpectedToken,
                suggestions: vec![],
            });
        }
    };

    let (schedule_ast, body) = split_bare_cron_chain(chain, span)?;
    let schedule = convert_cron_schedule(&schedule_ast).ok_or_else(|| ParseError {
        span,
        message: format!(
            "cannot parse cron schedule from: {}",
            cron_schedule_display(&schedule_ast)
        ),
        kind: ParseErrorKind::InvalidCronSchedule,
        suggestions: vec!["every 5m".into(), "at 09:00".into(), "daily".into()],
    })?;
    Ok(ResolvedCommand::Cron {
        schedule,
        chain: convert_chain(body),
        params: ModeParams::new(),
    })
}

fn split_bare_cron_chain(
    mut chain: ChainNode,
    span: Span,
) -> Result<(CronScheduleAst, ChainNode), ParseError> {
    let words = leftmost_command_words_mut(&mut chain).ok_or_else(|| ParseError {
        span,
        message: "`:cron` requires a schedule expression".into(),
        kind: ParseErrorKind::MissingArgument,
        suggestions: vec![":cron every 5m cargo test".into()],
    })?;

    let (schedule, consumed) = parse_bare_cron_schedule(words, span)?;
    if words.len() == consumed {
        return Err(ParseError {
            span,
            message: "`:cron` requires a command body after schedule".into(),
            kind: ParseErrorKind::MissingArgument,
            suggestions: vec![],
        });
    }

    words.drain(..consumed);
    Ok((schedule, chain))
}

fn leftmost_command_words_mut(node: &mut ChainNode) -> Option<&mut Vec<String>> {
    match node {
        ChainNode::Leaf(pipeline) => pipeline
            .segments
            .first_mut()
            .map(|segment| &mut segment.command),
        ChainNode::Serial { left, .. } | ChainNode::Parallel { left, .. } => {
            leftmost_command_words_mut(left)
        }
    }
}

fn parse_bare_cron_schedule(
    words: &[String],
    span: Span,
) -> Result<(CronScheduleAst, usize), ParseError> {
    let Some(keyword) = words.first().map(String::as_str) else {
        return Err(ParseError {
            span,
            message: "`:cron` requires a schedule expression".into(),
            kind: ParseErrorKind::MissingArgument,
            suggestions: vec![":cron every 5m cargo test".into()],
        });
    };

    let (schedule, mut consumed) = match keyword {
        "every" => {
            let duration = words
                .get(1)
                .and_then(|word| parse_duration_str(word))
                .ok_or_else(|| ParseError {
                    span,
                    message: "expected duration (e.g. 5m, 30s) after `every`".into(),
                    kind: ParseErrorKind::InvalidCronSchedule,
                    suggestions: vec!["every 5m".into()],
                })?;
            (CronScheduleAst::Every(duration), 2)
        }
        "at" => {
            let time = words.get(1).cloned().ok_or_else(|| ParseError {
                span,
                message: "expected time after `at`".into(),
                kind: ParseErrorKind::InvalidCronSchedule,
                suggestions: vec!["at 9:00".into()],
            })?;
            if words.get(2).is_some_and(|word| word == "on") {
                let days = words.get(3).cloned().ok_or_else(|| ParseError {
                    span,
                    message: "expected day filter after `on`".into(),
                    kind: ParseErrorKind::InvalidCronSchedule,
                    suggestions: vec!["at 9am on weekdays".into()],
                })?;
                (
                    CronScheduleAst::At {
                        time,
                        days: Some(days),
                    },
                    4,
                )
            } else {
                (CronScheduleAst::At { time, days: None }, 2)
            }
        }
        "on" => {
            let days = words.get(1).cloned().ok_or_else(|| ParseError {
                span,
                message: "expected day filter after `on`".into(),
                kind: ParseErrorKind::InvalidCronSchedule,
                suggestions: vec!["on weekdays at 9am".into()],
            })?;
            if words.get(2).map(String::as_str) != Some("at") {
                return Err(ParseError {
                    span,
                    message: "expected `at <time>` after day filter".into(),
                    kind: ParseErrorKind::InvalidCronSchedule,
                    suggestions: vec!["on weekdays at 9am".into()],
                });
            }
            let time = words.get(3).cloned().ok_or_else(|| ParseError {
                span,
                message: "expected time after `at`".into(),
                kind: ParseErrorKind::InvalidCronSchedule,
                suggestions: vec!["on weekdays at 9am".into()],
            })?;
            (
                CronScheduleAst::At {
                    time,
                    days: Some(days),
                },
                4,
            )
        }
        "in" => {
            let duration = words
                .get(1)
                .and_then(|word| parse_duration_str(word))
                .ok_or_else(|| ParseError {
                    span,
                    message: "expected duration (e.g. 5m, 30s) after `in`".into(),
                    kind: ParseErrorKind::InvalidCronSchedule,
                    suggestions: vec!["in 30s".into()],
                })?;
            (CronScheduleAst::In(duration), 2)
        }
        "cron" => {
            let fields = words.get(1..6).ok_or_else(|| ParseError {
                span,
                message: "expected five cron fields after `cron`".into(),
                kind: ParseErrorKind::InvalidCronSchedule,
                suggestions: vec!["cron */5 * * * *".into()],
            })?;
            (CronScheduleAst::Crontab(fields.join(" ")), 6)
        }
        "daily" | "hourly" | "weekly" | "monthly" => {
            (CronScheduleAst::Preset(keyword.to_string()), 1)
        }
        _ => {
            if let Some(do_idx) = words.iter().position(|word| word == "do") {
                if do_idx == 0 {
                    return Err(ParseError {
                        span,
                        message: "expected schedule text before `do`".into(),
                        kind: ParseErrorKind::InvalidCronSchedule,
                        suggestions: vec!["*/5 * * * * do curl api/health".into()],
                    });
                }
                if do_idx == 5 {
                    (
                        CronScheduleAst::Crontab(words[..do_idx].join(" ")),
                        do_idx + 1,
                    )
                } else {
                    return Err(ParseError {
                        span,
                        message: "`do` currently supports five-field crontab schedules only".into(),
                        kind: ParseErrorKind::InvalidCronSchedule,
                        suggestions: vec![
                            "*/5 * * * * do curl api/health".into(),
                            "cron */5 * * * * curl api/health".into(),
                        ],
                    });
                }
            } else {
                return Err(ParseError {
                    span,
                    message:
                        "expected schedule keyword (every, at, on, in, cron, daily, ...) or `<5-field-crontab> do <cmd>`"
                            .into(),
                    kind: ParseErrorKind::InvalidCronSchedule,
                    suggestions: vec![
                        "every 5m".into(),
                        "at 9:00".into(),
                        "on weekdays at 9am".into(),
                        "*/5 * * * * do curl api/health".into(),
                    ],
                });
            }
        }
    };

    if words.get(consumed).is_some_and(|word| word == "do") {
        consumed += 1;
    }

    Ok((schedule, consumed))
}

fn is_mode_help_request(argument: &Argument) -> bool {
    matches!(
        argument,
        Argument::Chain(ChainNode::Leaf(Pipeline { segments }))
            if segments.len() == 1
                && segments[0].pipe_to_next.is_none()
                && segments[0].command == ["?"]
    )
}

fn mode_help_topic(mode: Mode) -> &'static str {
    match mode {
        Mode::Job => "job",
        Mode::Cron => "cron",
    }
}

#[cfg(test)]
mod tests {
    use cue_core::command_spec::{COMMAND_SPECS, CommandArgKind};

    use super::super::parse::Parser as CueParser;
    use super::*;

    fn resolve(input: &str, mode: Mode) -> ResolvedCommand {
        let ast = CueParser::parse(input).unwrap();
        Resolver::resolve(ast, mode).unwrap()
    }

    #[test]
    fn resolve_run() {
        let cmd = resolve(":run cargo test", Mode::Job);
        assert!(matches!(cmd, ResolvedCommand::Run { .. }));
    }

    #[test]
    fn resolve_bare_job() {
        let cmd = resolve("cargo test --release", Mode::Job);
        assert!(matches!(cmd, ResolvedCommand::Run { .. }));
    }

    #[test]
    fn resolve_bare_question_in_job_mode_to_help() {
        let cmd = resolve("?", Mode::Job);
        assert!(matches!(
            cmd,
            ResolvedCommand::Help {
                topic: Some(ref topic)
            } if topic == "job"
        ));
    }

    #[test]
    fn resolve_bare_cron() {
        let cmd = resolve("every 5m cargo test -> cargo clippy", Mode::Cron);
        match cmd {
            ResolvedCommand::Cron {
                schedule: _,
                chain,
                params,
            } => {
                // schedule_text replaced by schedule: CronSchedule; display check removed
                assert_eq!(params.retry(), None);
                match chain {
                    core_pipeline::ChainNode::Serial { left, right, .. } => {
                        match *left {
                            core_pipeline::ChainNode::Leaf(pipeline) => {
                                assert_eq!(pipeline.segments[0].command, vec!["cargo", "test"]);
                            }
                            _ => panic!("expected left leaf"),
                        }
                        match *right {
                            core_pipeline::ChainNode::Leaf(pipeline) => {
                                assert_eq!(pipeline.segments[0].command, vec!["cargo", "clippy"]);
                            }
                            _ => panic!("expected right leaf"),
                        }
                    }
                    _ => panic!("expected serial chain"),
                }
            }
            _ => panic!("expected Cron"),
        }
    }

    #[test]
    fn resolve_bare_question_in_cron_mode_to_help() {
        let cmd = resolve("?", Mode::Cron);
        assert!(matches!(
            cmd,
            ResolvedCommand::Help {
                topic: Some(ref topic)
            } if topic == "cron"
        ));
    }

    #[test]
    fn resolve_bare_cron_supports_do_keyword() {
        let cmd = resolve("daily do echo hello", Mode::Cron);
        match cmd {
            ResolvedCommand::Cron {
                schedule: _, chain, ..
            } => {
                // schedule_text replaced by schedule: CronSchedule; display check removed
                match chain {
                    core_pipeline::ChainNode::Leaf(pipeline) => {
                        assert_eq!(pipeline.segments[0].command, vec!["echo", "hello"]);
                    }
                    _ => panic!("expected leaf"),
                }
            }
            _ => panic!("expected Cron"),
        }
    }

    #[test]
    fn resolve_bare_cron_supports_days_and_time() {
        let cmd = resolve("on weekdays at 9am cargo test", Mode::Cron);
        match cmd {
            ResolvedCommand::Cron { schedule: _, .. } => {
                // schedule_text replaced by schedule: CronSchedule; display check removed
            }
            _ => panic!("expected Cron"),
        }
    }

    #[test]
    fn resolve_bare_cron_supports_hh_mm_time() {
        let cmd = resolve("at 14:30 cargo test", Mode::Cron);
        match cmd {
            ResolvedCommand::Cron { schedule, .. } => {
                assert!(matches!(
                    schedule,
                    CronSchedule::TimeOfDay {
                        time_secs: 52200,
                        days: None
                    }
                ));
            }
            _ => panic!("expected Cron"),
        }
    }

    #[test]
    fn resolve_bare_cron_supports_five_field_do_crontab() {
        let cmd = resolve("*/5 * * * * do curl api/health", Mode::Cron);
        match cmd {
            ResolvedCommand::Cron {
                schedule, chain, ..
            } => {
                assert!(matches!(schedule, CronSchedule::Crontab(e) if e == "*/5 * * * *"));
                match chain {
                    core_pipeline::ChainNode::Leaf(pipeline) => {
                        assert_eq!(pipeline.segments[0].command, vec!["curl", "api/health"]);
                    }
                    _ => panic!("expected leaf"),
                }
            }
            _ => panic!("expected Cron"),
        }
    }

    #[test]
    fn resolve_bare_cron_rejects_unimplemented_freeform_do() {
        let ast = CueParser::parse("$MY_SCHEDULE do $MY_CMD").unwrap();
        let error = Resolver::resolve(ast, Mode::Cron).expect_err("freeform do is unsupported");

        assert_eq!(error.kind, ParseErrorKind::InvalidCronSchedule);
        assert!(error.message.contains("five-field crontab"));
    }

    #[test]
    fn resolve_bare_cron_supports_named_time_and_long_weekday() {
        let cmd = resolve("at midnight on monday cargo test", Mode::Cron);
        match cmd {
            ResolvedCommand::Cron { schedule, .. } => {
                assert!(matches!(
                    schedule,
                    CronSchedule::TimeOfDay {
                        time_secs: 0,
                        days: Some(_)
                    }
                ));
            }
            _ => panic!("expected Cron"),
        }
    }

    #[test]
    fn resolve_kill() {
        let cmd = resolve(":kill J1", Mode::Job);
        match cmd {
            ResolvedCommand::Kill { id } => assert_eq!(id, "J1"),
            _ => panic!("expected Kill"),
        }
    }

    #[test]
    fn resolve_with_params() {
        let cmd = resolve(":run(retry=3) cargo test", Mode::Job);
        match cmd {
            ResolvedCommand::Run { params, .. } => {
                assert_eq!(params.retry(), Some(3));
            }
            _ => panic!("expected Run"),
        }
    }

    #[test]
    fn resolve_jobs() {
        let cmd = resolve(":jobs", Mode::Job);
        assert!(matches!(cmd, ResolvedCommand::Jobs));
    }

    #[test]
    fn resolve_env_set() {
        let cmd = resolve(":env set FOO=bar FOO=baz", Mode::Job);
        match cmd {
            ResolvedCommand::Env { subcommand } => {
                assert_eq!(subcommand.as_deref(), Some("set FOO=bar FOO=baz"));
            }
            _ => panic!("expected Env"),
        }
    }

    #[test]
    fn resolve_send_target_and_data() {
        let cmd = resolve(":send J1 continue with the fix", Mode::Job);
        match cmd {
            ResolvedCommand::Send { id, data } => {
                assert_eq!(id, "J1");
                assert_eq!(data, "continue with the fix");
            }
            _ => panic!("expected Send"),
        }
    }

    #[test]
    fn resolve_cron_keeps_scheduler_text() {
        let cmd = resolve(":cron every 1h echo hello", Mode::Job);
        match cmd {
            ResolvedCommand::Cron { schedule: _, .. } => {
                // schedule_text replaced by schedule: CronSchedule; display check removed
            }
            _ => panic!("expected Cron"),
        }
    }

    #[test]
    fn resolve_cron_keeps_five_field_scheduler_text() {
        let cmd = resolve(":cron cron */5 * * * * echo hello", Mode::Job);
        match cmd {
            ResolvedCommand::Cron { schedule, .. } => {
                assert!(matches!(schedule, CronSchedule::Crontab(e) if e == "*/5 * * * *"));
            }
            _ => panic!("expected Cron"),
        }
    }

    #[test]
    fn resolver_covers_all_registered_commands() {
        for spec in COMMAND_SPECS {
            let input = match spec.arg_kind {
                CommandArgKind::Chain => format!(":{} echo ok", spec.name),
                CommandArgKind::Cron => format!(":{} every 5m echo ok", spec.name),
                CommandArgKind::Id => format!(":{} J1", spec.name),
                CommandArgKind::Tail => format!(":{} J1 1024", spec.name),
                CommandArgKind::Text => format!(":{} J1 hello", spec.name),
                CommandArgKind::OptionalId => format!(":{} J1", spec.name),
                CommandArgKind::OptionalText => format!(":{} status", spec.name),
                CommandArgKind::Empty => format!(":{}", spec.name),
            };
            let ast = CueParser::parse(&input).unwrap_or_else(|error| {
                panic!(
                    "registered command `{}` failed to parse: {error}",
                    spec.name
                )
            });
            Resolver::resolve(ast, Mode::Job).unwrap_or_else(|error| {
                panic!(
                    "registered command `{}` failed to resolve from `{input}`: {error}",
                    spec.name
                )
            });
        }
    }
}
