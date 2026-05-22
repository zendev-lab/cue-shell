//! Actor system for cued.
//!
//! Five actors communicate via bounded `mpsc` channels:
//!
//! ```text
//! Gateway  ──→  Scheduler  ──→  ProcessMgr
//!    │              │
//!    │         ScopeStore
//!    │
//!    └────────  EventBus  ←── (all actors publish)
//! ```

pub mod event_bus;
pub mod gateway;
pub mod process_mgr;
pub mod scheduler;
pub mod scope_store;

use anyhow::Result;
use tokio::sync::mpsc;

use cue_core::ScopeHash;
use cue_core::ipc::{EventPayload, OkPayload, ResponsePayload, ScopeInfo};
use cue_core::scope::{EnvDelta, EnvSnapshot, Scope};

use crate::parser::resolver::ResolvedCommand;

/// Default bounded channel capacity for actor mailboxes.
pub const ACTOR_CHANNEL_CAP: usize = 256;

/// Per-client event channel capacity.
pub const CLIENT_EVENT_CAP: usize = 64;

// ── Per-actor message types ──

/// Messages handled by the Gateway actor.
pub enum GatewayMsg {
    /// Deliver a response to a specific client.
    SendResponse {
        client_id: u64,
        request_id: u32,
        payload: ResponsePayload,
    },
    /// Deliver an event directly to a specific client.
    SendEvent {
        client_id: u64,
        payload: EventPayload,
    },
    /// Push an event to all subscribers of `channel`.
    PushEvent {
        payload: EventPayload,
        channel: String,
    },
    /// Graceful shutdown.
    Shutdown,
}

/// Messages handled by the Scheduler actor.
pub enum SchedulerMsg {
    /// Evaluate a resolved command on behalf of a client.
    Eval {
        client_id: u64,
        request_id: u32,
        command: ResolvedCommand,
    },
    /// A job has finished execution.
    JobFinished {
        job_id: cue_core::JobId,
        exit_code: i32,
    },
    /// Graceful shutdown.
    Shutdown,
}

/// Messages handled by the ProcessManager actor.
pub enum ProcessMgrMsg {
    /// Spawn a child process, pipeline, or job-local expression for the given job.
    SpawnJob {
        job_id: cue_core::JobId,
        /// Full job plan. A simple single-segment pipeline can use PTY; compound
        /// plans run as one JobId with stream output.
        plan: cue_core::pipeline::JobPlan,
        scope_hash: ScopeHash,
        /// Override the scope's cwd for this specific invocation.
        cwd_override: Option<std::path::PathBuf>,
        /// Whether the wrapper binary should be prepended to each segment.
        wrapper_enabled: bool,
        /// Whether to allocate a PTY. `false` uses pipes (stdout/stderr).
        pty_enabled: bool,
    },
    /// Kill a running job.
    KillJob { job_id: cue_core::JobId },
    /// Read the tail of a running job's output ring buffer.
    GetOutput {
        job_id: cue_core::JobId,
        tail_bytes: usize,
        reply: tokio::sync::oneshot::Sender<Option<OutputSnapshot>>,
    },
    /// Read the stderr tail of a running job.
    /// Returns `None` when the job is not in the live map (completed or unknown).
    GetStderr {
        job_id: cue_core::JobId,
        tail_bytes: usize,
        reply: tokio::sync::oneshot::Sender<Option<StderrSnapshot>>,
    },
    /// Send raw input bytes to a specific running job.
    SendJobInput {
        job_id: cue_core::JobId,
        data: Vec<u8>,
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    /// Attach a client to a job's live foreground stream.
    AttachFg {
        client_id: u64,
        job_id: cue_core::JobId,
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    /// Detach a client from any foreground-attached job.
    DetachFg { client_id: u64, reason: String },
    /// Send raw input bytes to the currently foreground-attached job.
    FgInput {
        client_id: u64,
        data: Vec<u8>,
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    /// Resize the foreground session.
    FgResize {
        client_id: u64,
        cols: u16,
        rows: u16,
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    /// Graceful shutdown.
    Shutdown,
}

/// Snapshot of a job output stream, as returned by `ProcessMgrMsg::GetOutput`.
pub struct OutputSnapshot {
    /// Captured bytes (tail of the ring buffer, or empty).
    pub data: Vec<u8>,
    /// True when older bytes were omitted by ring-buffer overflow or tail limit.
    pub truncated: bool,
}

/// Snapshot of a job's stderr, as returned by `ProcessMgrMsg::GetStderr`.
pub struct StderrSnapshot {
    /// True when the job used a PTY (stdout and stderr are merged).
    pub pty_merged: bool,
    /// Captured bytes (tail of the ring buffer, or empty).
    pub data: Vec<u8>,
    /// True when older bytes were omitted by ring-buffer overflow or tail limit.
    pub truncated: bool,
}

/// Messages handled by the ScopeStore actor.
pub enum ScopeStoreMsg {
    /// Get the current HEAD scope hash.
    GetHead {
        reply: tokio::sync::oneshot::Sender<ScopeHash>,
    },
    /// Get a scope by hash.
    GetScope {
        hash: ScopeHash,
        reply: tokio::sync::oneshot::Sender<Option<Scope>>,
    },
    /// Get the current HEAD snapshot.
    GetHeadSnapshot {
        reply: tokio::sync::oneshot::Sender<Option<EnvSnapshot>>,
    },
    /// Create a root scope from a full snapshot.
    CreateRoot {
        snapshot: EnvSnapshot,
        reply: tokio::sync::oneshot::Sender<ScopeHash>,
    },
    /// Fork a child scope from the current HEAD.
    Fork {
        delta: EnvDelta,
        reply: tokio::sync::oneshot::Sender<Result<ScopeHash>>,
    },
    /// Derive a child scope from a specific base without moving HEAD.
    Derive {
        base: ScopeHash,
        delta: EnvDelta,
        reply: tokio::sync::oneshot::Sender<Result<ScopeHash>>,
    },
    /// Graceful shutdown.
    Shutdown,
    /// List all known scopes, returning (head_hash, scope_infos).
    ListScopes {
        reply: tokio::sync::oneshot::Sender<(ScopeHash, Vec<ScopeInfo>)>,
    },
}

/// Messages handled by the EventBus actor.
pub enum EventBusMsg {
    /// Register a client for a channel.
    Subscribe {
        client_id: u64,
        channel: String,
        sender: mpsc::Sender<EventPayload>,
    },
    /// Remove a client from a channel.
    Unsubscribe { client_id: u64, channel: String },
    /// Remove a client from ALL channels (on disconnect).
    UnsubscribeAll { client_id: u64 },
    /// Broadcast an event to all subscribers of a channel.
    Publish {
        payload: EventPayload,
        channel: String,
    },
    /// Graceful shutdown.
    Shutdown,
}

// ── Actor handle bundle ──

/// Holds all actor sender handles.  Cheaply cloneable.
#[derive(Clone)]
pub struct ActorSystem {
    pub gateway: mpsc::Sender<GatewayMsg>,
    pub scheduler: mpsc::Sender<SchedulerMsg>,
    pub process_mgr: mpsc::Sender<ProcessMgrMsg>,
    pub scope_store: mpsc::Sender<ScopeStoreMsg>,
    pub event_bus: mpsc::Sender<EventBusMsg>,
    pub config: crate::config::Config,
}

impl ActorSystem {
    /// Send `Shutdown` to every actor.
    pub async fn shutdown(&self) {
        // Ignore errors — the actor may already have stopped.
        let _ = self.gateway.send(GatewayMsg::Shutdown).await;
        let _ = self.scheduler.send(SchedulerMsg::Shutdown).await;
        let _ = self.process_mgr.send(ProcessMgrMsg::Shutdown).await;
        let _ = self.scope_store.send(ScopeStoreMsg::Shutdown).await;
        let _ = self.event_bus.send(EventBusMsg::Shutdown).await;
    }
}

/// Spawn all five actors, returning the [`ActorSystem`] handle bundle.
pub async fn spawn_all(
    socket_path: std::path::PathBuf,
    scope_db: rusqlite::Connection,
    scheduler_db: rusqlite::Connection,
    config: crate::config::Config,
) -> Result<ActorSystem> {
    // Create channels.
    let (gw_tx, gw_rx) = mpsc::channel::<GatewayMsg>(ACTOR_CHANNEL_CAP);
    let (sched_tx, sched_rx) = mpsc::channel::<SchedulerMsg>(ACTOR_CHANNEL_CAP);
    let (pm_tx, pm_rx) = mpsc::channel::<ProcessMgrMsg>(ACTOR_CHANNEL_CAP);
    let (ss_tx, ss_rx) = mpsc::channel::<ScopeStoreMsg>(ACTOR_CHANNEL_CAP);
    let (eb_tx, eb_rx) = mpsc::channel::<EventBusMsg>(ACTOR_CHANNEL_CAP);

    let sys = ActorSystem {
        gateway: gw_tx,
        scheduler: sched_tx,
        process_mgr: pm_tx,
        scope_store: ss_tx,
        event_bus: eb_tx,
        config,
    };

    // Spawn actors.
    event_bus::spawn(eb_rx);
    scope_store::spawn(ss_rx, scope_db, sys.clone());
    process_mgr::spawn(pm_rx, sys.clone());
    scheduler::spawn(sched_rx, scheduler_db, sys.clone());
    gateway::spawn(gw_rx, socket_path, sys.clone()).await?;

    // Publish DaemonReady event.
    let _ = sys
        .event_bus
        .send(EventBusMsg::Publish {
            payload: EventPayload::DaemonReady {},
            channel: "system".into(),
        })
        .await;

    Ok(sys)
}

/// Helper to build a success response.
pub fn ok_response(payload: OkPayload) -> ResponsePayload {
    ResponsePayload::Ok(payload)
}
