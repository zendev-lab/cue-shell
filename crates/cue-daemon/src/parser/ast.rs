//! AST types produced by the Parser (unresolved).

use std::time::Duration;

use cue_core::pipeline::{ParallelOp, PipeOp, SerialOp};

use super::token::{IdKind, Span, Value};

/// Top-level parsed input.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Ast {
    /// File-script body containing one or more top-level statements.
    Script {
        items: Vec<ScriptItemAst>,
        span: Span,
    },
    /// Explicit builtin command (starts with `:`).
    Command {
        name: String,
        mode_params: Vec<(String, Value)>,
        argument: Argument,
        span: Span,
    },
    /// Bare input (no `:` prefix) — mode determines the command.
    BareInput { argument: Argument, span: Span },
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct ScriptItemAst {
    pub(super) source: String,
    pub(super) statement: Box<Ast>,
    pub(super) span: Span,
}

/// Argument types — which variant is valid depends on the command.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Argument {
    /// Chain expression (for `:run`, bare input in JOB/CRON mode).
    Chain(ChainNode),
    /// Entity ID reference (for `:kill`, `:out`, `:fg`, `:retry`).
    IdRef(IdKind, u32),
    /// Free-form text arguments for builtins like `:send`.
    Text(String),
    /// Entity ID with optional byte count (for `:tail J3 1024`).
    TailRef(IdKind, u32, Option<usize>),
    /// No argument (`:jobs`, `:crons`, `:help`).
    Empty,
}

/// Chain AST — tree structure of job-level operations.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum ChainNode {
    Leaf(JobExpr),
    Serial {
        op: SerialOp,
        left: Box<ChainNode>,
        right: Box<ChainNode>,
    },
    Parallel {
        op: ParallelOp,
        left: Box<ChainNode>,
        right: Box<ChainNode>,
    },
}

/// Job-internal expression. This is one cue Job even when it contains
/// shell-style logical operators.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum JobExpr {
    Pipeline(Pipeline),
    And {
        left: Box<JobExpr>,
        right: Box<JobExpr>,
    },
    Or {
        left: Box<JobExpr>,
        right: Box<JobExpr>,
    },
}

/// Pipeline = one Job's process chain.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Pipeline {
    pub(super) segments: Vec<PipeSegment>,
}

/// One process in a pipeline.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct PipeSegment {
    /// Command words, e.g. `["cargo", "test", "--release"]`.
    pub(super) command: Vec<String>,
    /// Pipe to next segment (None for last).
    pub(super) pipe_to_next: Option<PipeOp>,
}

/// Cron schedule AST (before resolution).
#[derive(Debug, Clone, PartialEq)]
pub(super) enum CronScheduleAst {
    /// `every 5m`
    Every(Duration),
    /// `at 09:00 [on weekdays]` / `on weekdays at 09:00`
    At { time: String, days: Option<String> },
    /// `in 30s`
    In(Duration),
    /// `cron "*/5 * * * *"`
    Crontab(String),
    /// `daily`, `hourly`, etc.
    Preset(String),
}
