//! AST types produced by the Parser (unresolved).

use std::time::Duration;

use cue_core::pipeline::{ParallelOp, PipeOp, SerialOp};

use super::token::{IdKind, Span, Value};

/// Top-level parsed input.
#[derive(Debug, Clone, PartialEq)]
pub enum Ast {
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

/// Argument types — which variant is valid depends on the command.
#[derive(Debug, Clone, PartialEq)]
pub enum Argument {
    /// Chain expression (for `:run`, bare input in JOB/CRON mode).
    Chain(ChainNode),
    /// Entity ID reference (for `:kill`, `:out`, `:fg`, `:retry`).
    IdRef(IdKind, u32),
    /// Free-form text (for `:ask`, `:confirm`).
    Text(String),
    /// Cron expression: schedule + body.
    CronExpr {
        schedule: CronScheduleAst,
        body: ChainNode,
    },
    /// No argument (`:jobs`, `:agents`, `:help`).
    Empty,
}

/// Chain AST — tree structure of job-level operations.
#[derive(Debug, Clone, PartialEq)]
pub enum ChainNode {
    Leaf(Pipeline),
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

/// Pipeline = one Job's process chain.
#[derive(Debug, Clone, PartialEq)]
pub struct Pipeline {
    pub segments: Vec<PipeSegment>,
}

/// One process in a pipeline.
#[derive(Debug, Clone, PartialEq)]
pub struct PipeSegment {
    /// Command words, e.g. `["cargo", "test", "--release"]`.
    pub command: Vec<String>,
    /// Pipe to next segment (None for last).
    pub pipe_to_next: Option<PipeOp>,
}

/// Cron schedule AST (before resolution).
#[derive(Debug, Clone, PartialEq)]
pub enum CronScheduleAst {
    /// `every 5m`
    Every(Duration),
    /// `at 09:00`
    At(String),
    /// `in 30s`
    In(Duration),
    /// `cron "*/5 * * * *"`
    Crontab(String),
    /// `daily`, `hourly`, etc.
    Preset(String),
}

impl Pipeline {
    pub fn simple(command: Vec<String>) -> Self {
        Self {
            segments: vec![PipeSegment {
                command,
                pipe_to_next: None,
            }],
        }
    }
}
