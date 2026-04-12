//! Resolver: Ast → validated execution request.
//!
//! Responsibilities:
//! 1. Mode injection: BareInput → wraps with default command per mode
//! 2. Argument type validation
//! 3. Mode params merge with config defaults
//! 4. AST → cue_core types conversion

use cue_core::command::{ModeParams, ParamValue};
use cue_core::mode::Mode;
use cue_core::pipeline::{self as core_pipeline};

use super::ast::{Argument, Ast, ChainNode, Pipeline};
use super::parse::ParseError;
use super::token::{Span, Value};

/// Resolved command ready for execution.
#[derive(Debug, Clone)]
pub enum ResolvedCommand {
    /// Run a chain of jobs.
    Run {
        chain: core_pipeline::ChainNode,
        params: ModeParams,
    },
    /// Send a prompt to the agent.
    Ask { text: String, params: ModeParams },
    /// Add a cron job.
    Cron {
        schedule_text: String,
        chain: core_pipeline::ChainNode,
        params: ModeParams,
    },
    /// Spawn an executor agent.
    Spawn { text: String, params: ModeParams },
    /// Kill a job/agent.
    Kill { id: String },
    /// Retry a failed job.
    Retry { id: String },
    /// View stdout.
    Out { id: String },
    /// View stderr.
    Err { id: String },
    /// Foreground attach.
    Fg { id: String },
    /// Wait for job completion.
    Wait { id: String },
    /// Send stdin.
    Send { id: String },
    /// Cancel a pending job.
    Cancel { id: String },
    /// Pause cron/agent.
    Pause { id: String },
    /// Resume cron/agent.
    Resume { id: String },
    /// Probe (planner light query).
    Probe { id: String },
    /// View log.
    Log { id: Option<String> },
    /// List jobs.
    Jobs,
    /// List agents.
    Agents,
    /// List crons.
    Crons,
    /// List scopes.
    Scopes,
    /// Confirm prompt.
    Confirm { text: String },
    /// Escalate from executor.
    Escalate { text: String },
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
}

/// Resolve an AST into a command ready for execution.
pub struct Resolver;

impl Resolver {
    pub fn resolve(ast: Ast, mode: Mode) -> Result<ResolvedCommand, ParseError> {
        match ast {
            Ast::BareInput { argument, span } => Self::resolve_bare(argument, mode, span),
            Ast::Command {
                name,
                mode_params,
                argument,
                span,
            } => Self::resolve_command(&name, mode_params, argument, span),
        }
    }

    fn resolve_bare(
        argument: Argument,
        mode: Mode,
        span: Span,
    ) -> Result<ResolvedCommand, ParseError> {
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
            Mode::Agent => {
                let text = match argument {
                    Argument::Chain(chain) => chain_to_text(&chain),
                    Argument::Text(t) => t,
                    Argument::Empty => {
                        return Err(ParseError {
                            span,
                            message: "empty input".into(),
                            kind: super::parse::ParseErrorKind::MissingArgument,
                            suggestions: vec![],
                        });
                    }
                    _ => {
                        return Err(ParseError {
                            span,
                            message: "AGENT mode expects a prompt".into(),
                            kind: super::parse::ParseErrorKind::UnexpectedToken,
                            suggestions: vec![],
                        });
                    }
                };
                Ok(ResolvedCommand::Ask {
                    text,
                    params: ModeParams::new(),
                })
            }
            Mode::Cron => Err(ParseError {
                span,
                message: "CRON mode requires a schedule expression (e.g. `every 5m cargo test`)"
                    .into(),
                kind: super::parse::ParseErrorKind::MissingArgument,
                suggestions: vec![],
            }),
        }
    }

    fn resolve_command(
        name: &str,
        mode_params: Vec<(String, Value)>,
        argument: Argument,
        _span: Span,
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
            "ask" => ResolvedCommand::Ask {
                text: extract_text(argument),
                params,
            },
            "cron" => match argument {
                Argument::CronExpr { schedule, body } => ResolvedCommand::Cron {
                    schedule_text: format!("{schedule:?}"),
                    chain: convert_chain(body),
                    params,
                },
                _ => unreachable!("parser guarantees CronExpr for :cron"),
            },
            "spawn" => ResolvedCommand::Spawn {
                text: extract_text(argument),
                params,
            },
            "kill" => ResolvedCommand::Kill {
                id: extract_id(argument),
            },
            "retry" => ResolvedCommand::Retry {
                id: extract_id(argument),
            },
            "out" => ResolvedCommand::Out {
                id: extract_id(argument),
            },
            "err" => ResolvedCommand::Err {
                id: extract_id(argument),
            },
            "fg" => ResolvedCommand::Fg {
                id: extract_id(argument),
            },
            "wait" => ResolvedCommand::Wait {
                id: extract_id(argument),
            },
            "send" => ResolvedCommand::Send {
                id: extract_id(argument),
            },
            "cancel" => ResolvedCommand::Cancel {
                id: extract_id(argument),
            },
            "pause" => ResolvedCommand::Pause {
                id: extract_id(argument),
            },
            "resume" => ResolvedCommand::Resume {
                id: extract_id(argument),
            },
            "probe" => ResolvedCommand::Probe {
                id: extract_id(argument),
            },
            "log" => ResolvedCommand::Log {
                id: match argument {
                    Argument::IdRef(k, n) => Some(format!("{k}{n}")),
                    _ => None,
                },
            },
            "jobs" => ResolvedCommand::Jobs,
            "agents" => ResolvedCommand::Agents,
            "crons" => ResolvedCommand::Crons,
            "scopes" => ResolvedCommand::Scopes,
            "confirm" => ResolvedCommand::Confirm {
                text: extract_text(argument),
            },
            "escalate" => ResolvedCommand::Escalate {
                text: extract_text(argument),
            },
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
            "quit" => ResolvedCommand::Quit,
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

fn extract_id(arg: Argument) -> String {
    match arg {
        Argument::IdRef(k, n) => format!("{k}{n}"),
        _ => String::new(),
    }
}

fn extract_text(arg: Argument) -> String {
    match arg {
        Argument::Text(t) => t,
        Argument::Chain(chain) => chain_to_text(&chain),
        _ => String::new(),
    }
}

fn extract_optional_text(arg: Argument) -> Option<String> {
    match arg {
        Argument::Text(t) if !t.is_empty() => Some(t),
        Argument::Empty => None,
        _ => None,
    }
}

/// Convert a chain AST back to text (for bare input in Agent mode).
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

#[cfg(test)]
mod tests {
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
    fn resolve_bare_agent() {
        let cmd = resolve("explain this error", Mode::Agent);
        match cmd {
            ResolvedCommand::Ask { text, .. } => {
                assert_eq!(text, "explain this error");
            }
            _ => panic!("expected Ask"),
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
    fn resolve_ask() {
        let cmd = resolve(":ask explain this error", Mode::Job);
        match cmd {
            ResolvedCommand::Ask { text, .. } => {
                assert_eq!(text, "explain this error");
            }
            _ => panic!("expected Ask"),
        }
    }
}
