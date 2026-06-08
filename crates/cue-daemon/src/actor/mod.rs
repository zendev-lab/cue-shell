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

mod cron_schedule;
mod event_bus;
pub(crate) mod gateway;
mod process_mgr;
mod scheduler;
mod scope_store;
mod script_record;

use anyhow::Result;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use cue_core::ipc::{EventPayload, ResponsePayload, ScopeInfo};
use cue_core::scope::{EnvDelta, EnvSnapshot, Scope};
use cue_core::{EventChannel, ScopeHash};

use crate::parser::ResolvedCommand;

/// Default bounded channel capacity for actor mailboxes.
pub(crate) const ACTOR_CHANNEL_CAP: usize = 256;

/// Per-client event channel capacity.
pub(crate) const CLIENT_EVENT_CAP: usize = 64;

#[derive(Clone, Debug)]
pub(crate) struct ProcessJobOptions {
    /// Override the scope's cwd for this specific invocation.
    pub cwd_override: Option<std::path::PathBuf>,
    /// Whether the wrapper binary should be prepended to each segment.
    pub wrapper_enabled: bool,
    /// Whether to allocate a PTY. `false` uses pipes (stdout/stderr).
    pub pty_enabled: bool,
    /// Client that should receive this job's output directly, independent of
    /// output-channel subscriptions.
    pub direct_output_client: Option<u64>,
}

// ── Per-actor message types ──

/// Messages handled by the Gateway actor.
pub(crate) enum GatewayMsg {
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
    /// Graceful shutdown.
    Shutdown,
}

/// Messages handled by the Scheduler actor.
pub(crate) enum SchedulerMsg {
    /// Evaluate a resolved command on behalf of a client.
    Eval {
        client_id: u64,
        request_id: u32,
        command: Box<ResolvedCommand>,
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
pub(crate) enum ProcessMgrMsg {
    /// Spawn a child process, pipeline, or job-local expression for the given job.
    SpawnJob {
        job_id: cue_core::JobId,
        /// Full job plan. A simple single-segment pipeline can use PTY; compound
        /// plans run as one JobId with stream output.
        plan: cue_core::pipeline::JobPlan,
        scope_hash: ScopeHash,
        options: ProcessJobOptions,
    },
    /// Request cancellation of a running job.
    KillJob {
        job_id: cue_core::JobId,
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
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
pub(crate) struct OutputSnapshot {
    /// Captured bytes (tail of the ring buffer, or empty).
    pub data: Vec<u8>,
    /// True when older bytes were omitted by ring-buffer overflow or tail limit.
    pub truncated: bool,
}

/// Snapshot of a job's stderr, as returned by `ProcessMgrMsg::GetStderr`.
pub(crate) struct StderrSnapshot {
    /// True when the job used a PTY (stdout and stderr are merged).
    pub pty_merged: bool,
    /// Captured bytes (tail of the ring buffer, or empty).
    pub data: Vec<u8>,
    /// True when older bytes were omitted by ring-buffer overflow or tail limit.
    pub truncated: bool,
}

/// Messages handled by the ScopeStore actor.
pub(crate) enum ScopeStoreMsg {
    /// Get the current HEAD scope hash.
    GetHead {
        reply: tokio::sync::oneshot::Sender<ScopeHash>,
    },
    /// Get a scope by hash.
    GetScope {
        hash: ScopeHash,
        reply: tokio::sync::oneshot::Sender<Result<Option<Scope>>>,
    },
    /// Get the current HEAD snapshot.
    GetHeadSnapshot {
        reply: tokio::sync::oneshot::Sender<Result<EnvSnapshot>>,
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
        reply: tokio::sync::oneshot::Sender<Result<(ScopeHash, Vec<ScopeInfo>)>>,
    },
}

/// Messages handled by the EventBus actor.
pub(crate) enum EventBusMsg {
    /// Register a client for a channel.
    Subscribe {
        client_id: u64,
        channel: EventChannel,
        sender: mpsc::Sender<EventPayload>,
    },
    /// Remove a client from a channel.
    Unsubscribe {
        client_id: u64,
        channel: EventChannel,
    },
    /// Remove a client from ALL channels (on disconnect).
    UnsubscribeAll { client_id: u64 },
    /// Broadcast an event to all subscribers of a channel.
    Publish {
        payload: EventPayload,
        channel: EventChannel,
    },
    /// Broadcast an event to all subscribers of a channel except one client.
    PublishExcept {
        payload: EventPayload,
        channel: EventChannel,
        excluded_client_id: u64,
    },
    /// Graceful shutdown.
    Shutdown,
}

pub(crate) async fn publish_event(
    actor: &'static str,
    event_bus: &mpsc::Sender<EventBusMsg>,
    channel: EventChannel,
    payload: EventPayload,
) {
    if let Err(error) = event_bus
        .send(EventBusMsg::Publish {
            payload,
            channel: channel.clone(),
        })
        .await
    {
        warn!(%actor, %channel, "actor: failed to publish event: {error}");
    }
}

pub(crate) async fn publish_event_except(
    actor: &'static str,
    event_bus: &mpsc::Sender<EventBusMsg>,
    channel: EventChannel,
    payload: EventPayload,
    excluded_client_id: u64,
) {
    if let Err(error) = event_bus
        .send(EventBusMsg::PublishExcept {
            payload,
            channel: channel.clone(),
            excluded_client_id,
        })
        .await
    {
        warn!(%actor, %channel, %excluded_client_id, "actor: failed to publish event: {error}");
    }
}

pub(crate) async fn send_gateway_event(
    actor: &'static str,
    sys: &ActorSystem,
    client_id: u64,
    payload: EventPayload,
) {
    if let Err(error) = sys
        .gateway
        .send(GatewayMsg::SendEvent { client_id, payload })
        .await
    {
        warn!(%actor, %client_id, "actor: failed to send gateway event: {error}");
    }
}

// ── Actor handle bundle ──

/// Holds all actor sender handles.  Cheaply cloneable.
#[derive(Clone)]
pub(crate) struct ActorSystem {
    gateway: mpsc::Sender<GatewayMsg>,
    scheduler: mpsc::Sender<SchedulerMsg>,
    process_mgr: mpsc::Sender<ProcessMgrMsg>,
    scope_store: mpsc::Sender<ScopeStoreMsg>,
    event_bus: mpsc::Sender<EventBusMsg>,
    config: crate::config::Config,
}

impl ActorSystem {
    /// Send `Shutdown` to every actor.
    pub(crate) async fn shutdown(&self) {
        self.shutdown_with_reason("shutdown requested").await;
    }

    /// Notify clients about shutdown, then send `Shutdown` to every actor.
    pub(crate) async fn shutdown_with_reason(&self, reason: impl Into<String>) {
        publish_event(
            "actor_system",
            &self.event_bus,
            EventChannel::System,
            EventPayload::ShuttingDown {
                reason: reason.into(),
            },
        )
        .await;
        send_shutdown("gateway", &self.gateway, GatewayMsg::Shutdown).await;
        send_shutdown("scheduler", &self.scheduler, SchedulerMsg::Shutdown).await;
        send_shutdown("process_mgr", &self.process_mgr, ProcessMgrMsg::Shutdown).await;
        send_shutdown("scope_store", &self.scope_store, ScopeStoreMsg::Shutdown).await;
        send_shutdown("event_bus", &self.event_bus, EventBusMsg::Shutdown).await;
    }
}

async fn send_shutdown<T>(actor: &'static str, sender: &mpsc::Sender<T>, message: T) {
    if sender.send(message).await.is_err() {
        debug!(%actor, "actor: shutdown message was not delivered");
    }
}

/// Spawn all five actors, returning the [`ActorSystem`] handle bundle.
pub(crate) async fn spawn_all(
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

    // ScopeStore restores persisted state before the daemon is considered ready.
    scope_store::spawn(ss_rx, scope_db, sys.clone()).await?;
    event_bus::spawn(eb_rx);
    process_mgr::spawn(pm_rx, sys.clone());
    if let Err(error) = scheduler::spawn(sched_rx, scheduler_db, sys.clone()).await {
        send_shutdown("scope_store", &sys.scope_store, ScopeStoreMsg::Shutdown).await;
        send_shutdown("process_mgr", &sys.process_mgr, ProcessMgrMsg::Shutdown).await;
        send_shutdown("event_bus", &sys.event_bus, EventBusMsg::Shutdown).await;
        return Err(anyhow::anyhow!("initialize scheduler: {error}"));
    }
    if let Err(error) = gateway::spawn(gw_rx, socket_path, sys.clone()).await {
        sys.shutdown().await;
        return Err(error);
    }

    Ok(sys)
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::*;
    use crate::storage;

    fn in_memory_db() -> rusqlite::Connection {
        storage::open_db(Path::new(":memory:")).expect("open in-memory db")
    }

    #[tokio::test]
    async fn spawn_all_reports_scope_store_initialization_failure() {
        let scope_db = in_memory_db();
        let missing = ScopeHash([7; 32]);
        storage::set_head(&scope_db, &missing).expect("set missing head");
        let scheduler_db = in_memory_db();

        let result = spawn_all(
            PathBuf::from("/tmp/cue-spawn-all-should-not-bind.sock"),
            scope_db,
            scheduler_db,
            crate::config::Config::default(),
        )
        .await;
        let Err(error) = result else {
            panic!("scope store initialization failure should stop daemon startup");
        };

        assert!(error.to_string().contains("persisted head scope"));
        assert!(error.to_string().contains("is missing"));
    }

    #[tokio::test]
    async fn shutdown_publishes_system_notice_before_stopping_event_bus() {
        let (gateway, mut gateway_rx) = mpsc::channel(1);
        let (scheduler, _scheduler_rx) = mpsc::channel(1);
        let (process_mgr, _process_mgr_rx) = mpsc::channel(1);
        let (scope_store, _scope_store_rx) = mpsc::channel(1);
        let (event_bus, mut event_bus_rx) = mpsc::channel(2);
        let sys = ActorSystem {
            gateway,
            scheduler,
            process_mgr,
            scope_store,
            event_bus,
            config: crate::config::Config::default(),
        };

        sys.shutdown_with_reason("SIGTERM").await;

        match event_bus_rx.recv().await.expect("shutdown notice") {
            EventBusMsg::Publish {
                channel,
                payload: EventPayload::ShuttingDown { reason },
            } => {
                assert_eq!(channel, EventChannel::System);
                assert_eq!(reason, "SIGTERM");
            }
            _ => panic!("expected ShuttingDown publish"),
        }
        assert!(matches!(gateway_rx.try_recv(), Ok(GatewayMsg::Shutdown)));
        assert!(matches!(
            event_bus_rx.recv().await.expect("event bus shutdown"),
            EventBusMsg::Shutdown
        ));
    }

    #[tokio::test]
    async fn spawn_all_reports_gateway_initialization_failure() {
        let socket_path = std::env::temp_dir().join(format!(
            "cue-spawn-all-gateway-failure-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&socket_path).expect("create socket-blocking directory");

        let result = spawn_all(
            socket_path.clone(),
            in_memory_db(),
            in_memory_db(),
            crate::config::Config::default(),
        )
        .await;
        let Err(error) = result else {
            panic!("gateway initialization failure should stop daemon startup");
        };

        assert!(error.to_string().contains("remove stale socket"));
        std::fs::remove_dir_all(socket_path).expect("remove socket-blocking directory");
    }

    #[tokio::test]
    async fn spawn_all_reports_scheduler_initialization_failure() {
        let scope_db = in_memory_db();
        let scheduler_db = in_memory_db();
        scheduler_db
            .execute_batch("DROP TABLE crons;")
            .expect("drop crons table");

        let result = spawn_all(
            PathBuf::from("/tmp/cue-spawn-all-scheduler-init-fails.sock"),
            scope_db,
            scheduler_db,
            crate::config::Config::default(),
        )
        .await;
        let Err(error) = result else {
            panic!("scheduler initialization failure should stop daemon startup");
        };

        let message = error.to_string();
        assert!(message.contains("initialize scheduler"));
        assert!(message.contains("load persisted crons"));
    }
}
