use std::fmt;

use serde::{Deserialize, Serialize};

// ── Job-internal execution plan ──

/// A single Job's execution plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobPlan {
    Pipeline(Pipeline),
    And {
        left: Box<JobPlan>,
        right: Box<JobPlan>,
    },
    Or {
        left: Box<JobPlan>,
        right: Box<JobPlan>,
    },
}

/// A single Job's command, possibly a multi-process pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pipeline {
    /// At least one segment.
    pub segments: Vec<PipeSegment>,
}

/// One process in a pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PipeSegment {
    /// Command words, e.g. `["cargo", "test", "--release"]`.
    pub command: Vec<String>,
    /// How this segment's output connects to the next (None for last segment).
    pub pipe_to_next: Option<PipeOp>,
}

/// Pipe operator connecting two processes within a Job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PipeOp {
    /// `|>` — stdout → next stdin.
    Stdout,
    /// `|&>` — stdout + stderr → next stdin.
    StdoutStderr,
    /// `|!>` — stderr only → next stdin.
    StderrOnly,
}

// ── Chain (Job-level orchestration) ──

/// Tree-shaped AST for chaining multiple Jobs.
///
/// Leaf nodes are Job plans (each becomes one Job).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChainNode {
    Leaf(JobPlan),
    Serial {
        left: Box<ChainNode>,
        op: SerialOp,
        right: Box<ChainNode>,
    },
    Parallel {
        left: Box<ChainNode>,
        op: ParallelOp,
        right: Box<ChainNode>,
    },
}

/// Serial chain operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SerialOp {
    /// `->` — continue only if predecessor exits 0.
    Then,
    /// `~>` — continue regardless of predecessor's exit code.
    Always,
}

/// Parallel chain operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ParallelOp {
    /// `|||` — fire all branches simultaneously, wait for all.
    All,
    /// `|?|` — fire all branches, succeed when any one succeeds.
    Race,
}

/// Overall chain execution status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChainStatus {
    Running,
    Done,
    Failed,
    /// A step failed and the chain was aborted (with `->` semantics).
    Aborted,
}

impl Pipeline {
    /// Create a simple single-command pipeline.
    pub fn simple(command: Vec<String>) -> Self {
        Self {
            segments: vec![PipeSegment {
                command,
                pipe_to_next: None,
            }],
        }
    }
}

impl fmt::Display for PipeOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Stdout => "|>",
            Self::StdoutStderr => "|&>",
            Self::StderrOnly => "|!>",
        })
    }
}

impl fmt::Display for SerialOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Then => "->",
            Self::Always => "~>",
        })
    }
}

impl fmt::Display for ParallelOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::All => "|||",
            Self::Race => "|?|",
        })
    }
}

impl fmt::Display for Pipeline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (idx, segment) in self.segments.iter().enumerate() {
            if idx > 0 {
                f.write_str(" ")?;
            }

            let cmd = segment.command.join(" ");
            match segment.pipe_to_next {
                Some(op) => write!(f, "{cmd} {op}")?,
                None => f.write_str(&cmd)?,
            }
        }
        Ok(())
    }
}

impl fmt::Display for JobPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pipeline(pipeline) => write!(f, "{pipeline}"),
            Self::And { left, right } => write!(f, "{left} && {right}"),
            Self::Or { left, right } => write!(f, "{left} || {right}"),
        }
    }
}

impl fmt::Display for ChainNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Leaf(plan) => write!(f, "{plan}"),
            Self::Serial { left, op, right } => write!(f, "{left} {op} {right}"),
            Self::Parallel { left, op, right } => write!(f, "{left} {op} {right}"),
        }
    }
}

/// Return true when a command is likely to need immediate foreground/TTY use.
pub fn command_prefers_foreground(command_line: &[String]) -> bool {
    let Some(command_word) = command_line.first() else {
        return false;
    };
    let command = std::path::Path::new(command_word)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(command_word.as_str());
    let args: Vec<&str> = command_line.iter().skip(1).map(String::as_str).collect();

    match command {
        "vim" | "nvim" | "vi" | "nano" | "less" | "more" | "man" | "top" | "htop" | "watch"
        | "fzf" | "tig" | "lazygit" | "tmux" | "zellij" => true,
        "bash" | "zsh" | "sh" | "fish" => {
            args.is_empty()
                || args.contains(&"-i")
                || args.contains(&"--interactive")
                || args.contains(&"-l")
        }
        "python" | "python3" | "node" | "ipython" | "bpython" | "irb" => {
            args.is_empty()
                || args
                    .first()
                    .is_some_and(|arg| matches!(*arg, "-i" | "--interactive"))
        }
        "ssh" | "psql" | "mysql" | "sqlite3" => true,
        _ => false,
    }
}

impl JobPlan {
    pub fn simple(command: Vec<String>) -> Self {
        Self::Pipeline(Pipeline::simple(command))
    }

    pub fn first_command(&self) -> &[String] {
        match self {
            Self::Pipeline(pipeline) => pipeline
                .segments
                .first()
                .map(|segment| segment.command.as_slice())
                .unwrap_or(&[]),
            Self::And { left, .. } | Self::Or { left, .. } => left.first_command(),
        }
    }

    pub fn pipelines(&self) -> Vec<&Pipeline> {
        let mut pipelines = Vec::new();
        self.collect_pipelines(&mut pipelines);
        pipelines
    }

    fn collect_pipelines<'a>(&'a self, pipelines: &mut Vec<&'a Pipeline>) {
        match self {
            Self::Pipeline(pipeline) => pipelines.push(pipeline),
            Self::And { left, right } | Self::Or { left, right } => {
                left.collect_pipelines(pipelines);
                right.collect_pipelines(pipelines);
            }
        }
    }
}

impl ChainNode {
    /// Count the number of leaf pipelines (= number of Jobs).
    pub fn leaf_count(&self) -> usize {
        match self {
            Self::Leaf(_) => 1,
            Self::Serial { left, right, .. } | Self::Parallel { left, right, .. } => {
                left.leaf_count() + right.leaf_count()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_pipeline() {
        let p = Pipeline::simple(vec!["cargo".into(), "test".into()]);
        assert_eq!(p.segments.len(), 1);
        assert!(p.segments[0].pipe_to_next.is_none());
    }

    #[test]
    fn chain_leaf_count() {
        let a = ChainNode::Leaf(JobPlan::simple(vec!["a".into()]));
        let b = ChainNode::Leaf(JobPlan::simple(vec!["b".into()]));
        let c = ChainNode::Leaf(JobPlan::simple(vec!["c".into()]));
        let chain = ChainNode::Serial {
            left: Box::new(a),
            op: SerialOp::Then,
            right: Box::new(ChainNode::Parallel {
                left: Box::new(b),
                op: ParallelOp::All,
                right: Box::new(c),
            }),
        };
        assert_eq!(chain.leaf_count(), 3);
    }

    #[test]
    fn foreground_command_detection() {
        assert!(command_prefers_foreground(&[
            "vim".into(),
            "src/main.rs".into()
        ]));
        assert!(command_prefers_foreground(&[
            "/usr/bin/ssh".into(),
            "host".into()
        ]));
        assert!(command_prefers_foreground(&["python".into()]));
        assert!(command_prefers_foreground(&[
            "bash".into(),
            "--interactive".into(),
        ]));
        assert!(!command_prefers_foreground(&[
            "cargo".into(),
            "test".into(),
        ]));
        assert!(!command_prefers_foreground(&[
            "python".into(),
            "script.py".into(),
        ]));
    }

    #[test]
    fn display_pipeline_job_plan_and_chain() {
        let pipeline = Pipeline {
            segments: vec![
                PipeSegment {
                    command: vec!["printf".into(), "hi".into()],
                    pipe_to_next: Some(PipeOp::Stdout),
                },
                PipeSegment {
                    command: vec!["grep".into(), "h".into()],
                    pipe_to_next: Some(PipeOp::StderrOnly),
                },
                PipeSegment {
                    command: vec!["wc".into(), "-l".into()],
                    pipe_to_next: None,
                },
            ],
        };
        assert_eq!(pipeline.to_string(), "printf hi |> grep h |!> wc -l");

        let plan = JobPlan::And {
            left: Box::new(JobPlan::simple(vec!["cargo".into(), "test".into()])),
            right: Box::new(JobPlan::simple(vec!["cargo".into(), "clippy".into()])),
        };
        assert_eq!(plan.to_string(), "cargo test && cargo clippy");

        let chain = ChainNode::Serial {
            left: Box::new(ChainNode::Leaf(JobPlan::simple(vec!["build".into()]))),
            op: SerialOp::Then,
            right: Box::new(ChainNode::Parallel {
                left: Box::new(ChainNode::Leaf(JobPlan::simple(vec!["test".into()]))),
                op: ParallelOp::All,
                right: Box::new(ChainNode::Leaf(JobPlan::simple(vec!["lint".into()]))),
            }),
        };
        assert_eq!(chain.to_string(), "build -> test ||| lint");
    }
}
