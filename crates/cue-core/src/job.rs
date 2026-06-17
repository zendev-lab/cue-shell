use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::resource::Need;

/// Exit code used when a job has no process-provided exit status.
///
/// This covers spawn failures, explicit cancellation, explicit kill handling,
/// and rare platform cases where the OS wait status cannot be represented as a
/// numeric exit code.
pub const EXIT_CODE_UNAVAILABLE: i32 = -1;

/// Per-run options that affect how a job is launched but are not part of the
/// content-addressed scope identity.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaunchOptions {
    /// Explicit PTY setting. `None` means use the session/config default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pty: Option<bool>,
    /// Resource needs declared via `need.<resource>=<quantity>` mode params.
    #[serde(default, skip_serializing_if = "Need::is_empty")]
    pub needs: Need,
    /// Optional filesystem sandbox settings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox: Option<SandboxSettings>,
}

/// Per-run filesystem sandbox settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxSettings {
    pub mode: SandboxMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upper: Option<SandboxUpper>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxMode {
    Overlay,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxUpper {
    Directory(PathBuf),
    Tmpfs,
}

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
    use crate::ResourceQuantity;
    use insta::assert_json_snapshot;

    #[test]
    fn terminal_states() {
        assert!(!JobStatus::Pending.is_terminal());
        assert!(!JobStatus::Running.is_terminal());
        assert!(JobStatus::Done.is_terminal());
        assert!(JobStatus::Failed.is_terminal());
        assert!(JobStatus::Killed.is_terminal());
        assert!(JobStatus::Cancelled(CancelReason::User).is_terminal());
    }

    #[test]
    fn launch_options_json_skips_empty_defaults() {
        assert_json_snapshot!(LaunchOptions::default(), @r###"
        {}
        "###);
    }

    #[test]
    fn launch_options_json_includes_explicit_values() {
        let options = LaunchOptions {
            pty: Some(false),
            needs: Need::from_pairs([
                ("gpu", ResourceQuantity::Count(1)),
                ("gpu_mem", ResourceQuantity::Bytes(24 * 1024 * 1024 * 1024)),
            ]),
            sandbox: Some(SandboxSettings {
                mode: SandboxMode::Overlay,
                upper: Some(SandboxUpper::Tmpfs),
            }),
        };

        assert_json_snapshot!(options, @r###"
        {
          "pty": false,
          "needs": {
            "gpu": {
              "kind": "count",
              "value": 1
            },
            "gpu_mem": {
              "kind": "bytes",
              "value": 25769803776
            }
          },
          "sandbox": {
            "mode": "overlay",
            "upper": "tmpfs"
          }
        }
        "###);
    }
}
