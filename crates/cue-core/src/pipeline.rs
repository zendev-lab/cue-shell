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
}
