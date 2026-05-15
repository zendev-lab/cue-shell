use serde::{Deserialize, Serialize};

/// Job lifecycle state (unidirectional state machine).
///
/// ```text
///                     ┌─────────┐
///       :cancel ──→   │Cancelled│
///                     │(reason) │
///                     └─────────┘
///                          ↑
/// ┌───────┐  sched  ┌───────┐  done   ┌──────┐
/// │Pending│ ────→   │Running│ ────→   │ Done │  (exit 0)
/// └───────┘         └───────┘         └──────┘
///                       │             ┌──────┐
///                       ├───────────→ │Failed│  (exit != 0)
///                       │             └──────┘
///                       │             ┌──────┐
///                       └───────────→ │Killed│  (:kill)
///                                     └──────┘
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobStatus {
    /// Queued, waiting for execution slot.
    Pending,
    /// Currently executing.
    Running,
    /// Completed successfully (exit code 0).
    Done,
    /// Completed with non-zero exit code.
    Failed,
    /// Terminated by `:kill`.
    Killed,
    /// Cancelled before or during execution.
    Cancelled(CancelReason),
}

/// Why a job was cancelled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CancelReason {
    /// User issued `:cancel`.
    User,
    /// Preceding step in a chain failed (with `->` operator).
    ChainAborted,
    /// Reserved for future timeout enforcement.
    Timeout,
}

impl JobStatus {
    /// Whether the job has reached a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Done | Self::Failed | Self::Killed | Self::Cancelled(_)
        )
    }

    /// Short label for TUI display.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Done => "done",
            Self::Failed => "failed",
            Self::Killed => "killed",
            Self::Cancelled(_) => "cancelled",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_states() {
        assert!(!JobStatus::Pending.is_terminal());
        assert!(!JobStatus::Running.is_terminal());
        assert!(JobStatus::Done.is_terminal());
        assert!(JobStatus::Failed.is_terminal());
        assert!(JobStatus::Killed.is_terminal());
        assert!(JobStatus::Cancelled(CancelReason::User).is_terminal());
    }
}
