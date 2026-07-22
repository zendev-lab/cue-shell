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
mod operation_ledger;
mod process_mgr;
mod scheduler;
mod scope_store;
mod script_record;

use anyhow::Result;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, warn};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionBinding {
    /// Scheduler-internal logical session key. Named sessions use their durable
    /// id so every attachment shares one idempotency namespace.
    pub session_id: String,
    /// Durable named-session owner used to isolate realtime resource events.
    /// Anonymous handshake sessions leave this unset for compatibility.
    pub named_session_id: Option<String>,
    pub scope: ScopeHash,
    pub incarnation: u64,
}

#[derive(Debug)]
pub(crate) enum SessionCommand {
    Create { name: String },
    List,
    ListArchived,
    ListAll,
    Archive { selector: String },
    Restore { selector: String },
    Attach { selector: String, refresh: bool },
    Info { selector: Option<String> },
}

pub(crate) struct SessionCommandResult {
    pub payload: ResponsePayload,
    /// Present when the command changed the calling client's binding.
    pub binding: Option<SessionBinding>,
}

use cue_core::ipc::{
    EventPayload, ForegroundAttachmentInfo, ForegroundRole, ResponsePayload, ScopeInfo,
};
use cue_core::scope::{EnvDelta, EnvSnapshot, Scope};
use cue_core::{EventChannel, ScopeHash};

use crate::parser::ResolvedCommand;
use crate::resource::ProviderRegistry;

/// Default bounded channel capacity for actor mailboxes.
pub(crate) const ACTOR_CHANNEL_CAP: usize = 256;

/// Per-client event channel capacity.
pub(crate) const CLIENT_EVENT_CAP: usize = 64;

/// One outbound event plus the audience that authorized its delivery.
///
/// Keeping the audience beside the payload lets the gateway revalidate events
/// that were queued before a transport switched named sessions. `Global`
/// events are not resource-owned; `Session(None)` is an anonymous resource and
/// `Session(Some(id))` belongs to one durable named session.
#[derive(Clone, Debug)]
pub(crate) enum ClientEventAudience {
    Global,
    Session(Option<String>),
}

#[derive(Clone, Debug)]
pub(crate) struct ClientEvent {
    pub payload: EventPayload,
    pub audience: ClientEventAudience,
}

impl ClientEvent {
    pub(crate) fn global(payload: EventPayload) -> Self {
        Self {
            payload,
            audience: ClientEventAudience::Global,
        }
    }

    pub(crate) fn session(payload: EventPayload, session_id: Option<String>) -> Self {
        Self {
            payload,
            audience: ClientEventAudience::Session(session_id),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ProcessJobOptions {
    /// Override the scope's cwd for this specific invocation.
    pub cwd_override: Option<std::path::PathBuf>,
    /// Optional per-run filesystem sandbox configuration.
    pub sandbox: Option<crate::sandbox::SandboxConfig>,
    /// Whether the wrapper binary should be prepended to each segment.
    pub wrapper_enabled: bool,
    /// Whether to allocate a PTY. `false` uses pipes (stdout/stderr).
    pub pty_enabled: bool,
    /// Client that should receive this job's output directly, independent of
    /// output-channel subscriptions.
    pub direct_output_client: Option<u64>,
    /// Durable named-session owner for state and output event isolation.
    pub session_id: Option<String>,
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
        /// Owning named session. `None` denotes an anonymous resource; the
        /// gateway still treats the event as resource-scoped, not global.
        session_id: Option<String>,
    },
    /// Graceful shutdown.
    Shutdown,
}

/// Messages handled by the Scheduler actor.
pub(crate) enum SchedulerMsg {
    /// Arm execution activation while leaving jobs and crons paused. The
    /// scheduler observes the lifecycle's durable-completion signal before it
    /// opens execution and publishes readiness.
    Activate {
        reply: tokio::sync::oneshot::Sender<Result<()>>,
    },
    /// Close scheduler admission and acknowledge after the gate is visible.
    BeginDrain {
        reply: tokio::sync::oneshot::Sender<()>,
    },
    /// Bind a transport client id to a stable logical session.
    Connect {
        client_id: u64,
        session_id: String,
        snapshot: EnvSnapshot,
        /// Explicitly refresh an existing session cursor from the handshake snapshot.
        refresh: bool,
        reply: tokio::sync::oneshot::Sender<Result<SessionBinding>>,
    },
    /// Create, list, attach, or inspect durable named sessions.
    Session {
        client_id: u64,
        command: SessionCommand,
        reply: tokio::sync::oneshot::Sender<SessionCommandResult>,
    },
    /// Mark a transport client as disconnected and start session TTL handling.
    Disconnect { client_id: u64 },
    /// Evaluate a resolved command on behalf of a client.
    Eval {
        client_id: u64,
        request_id: u32,
        command: Box<ResolvedCommand>,
    },
    /// Recover a daemon-lifetime script snapshot after a client reconnect.
    ScriptInfo {
        client_id: u64,
        request_id: u32,
        id: String,
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
        client_id: u64,
        job_id: cue_core::JobId,
        data: Vec<u8>,
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    /// Attach a client to a job's live foreground stream.
    AttachFg {
        client_id: u64,
        job_id: cue_core::JobId,
        role: ForegroundRole,
        reply: tokio::sync::oneshot::Sender<Result<ForegroundAttachmentInfo, String>>,
    },
    /// Acquire the free controller lease for the client's observed PTY job.
    ClaimFgControl {
        client_id: u64,
        reply: tokio::sync::oneshot::Sender<Result<ForegroundRoleUpdate, String>>,
    },
    /// Release the controller lease while keeping the client's observer attachment.
    ReleaseFgControl {
        client_id: u64,
        reply: tokio::sync::oneshot::Sender<Result<ForegroundRoleUpdate, String>>,
    },
    /// Detach a client from any foreground-attached job.
    DetachFg {
        client_id: u64,
        reason: String,
        /// Present when the caller must not acknowledge until the foreground
        /// lease has actually been cleared.
        reply: Option<tokio::sync::oneshot::Sender<()>>,
    },
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

/// Internal controller-lease transition result used to build typed IPC replies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ForegroundRoleUpdate {
    pub id: String,
    pub attachment_id: u64,
    pub role: ForegroundRole,
    pub control_available: bool,
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
    /// Insert a full scope snapshot if it is not already present.
    Insert {
        scope: Scope,
        reply: tokio::sync::oneshot::Sender<Result<ScopeHash>>,
    },
    /// Get a scope by hash.
    GetScope {
        hash: ScopeHash,
        reply: tokio::sync::oneshot::Sender<Result<Option<Scope>>>,
    },
    /// Derive a child scope from a specific base without moving a global cursor.
    Derive {
        base: ScopeHash,
        delta: EnvDelta,
        reply: tokio::sync::oneshot::Sender<Result<ScopeHash>>,
    },
    /// Retain the supplied roots and every ancestor they reference, removing
    /// all other persisted and process-local scopes.
    GarbageCollect {
        roots: HashSet<ScopeHash>,
        reply: tokio::sync::oneshot::Sender<Result<ScopeGcReport>>,
    },
    /// Graceful shutdown.
    Shutdown,
    /// List all known scopes.
    ListScopes {
        reply: tokio::sync::oneshot::Sender<Result<Vec<ScopeInfo>>>,
    },
}

/// Result of one scope mark-and-sweep pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ScopeGcReport {
    pub retained: usize,
    pub removed_cached: usize,
    pub removed_persisted: usize,
}

/// Messages handled by the EventBus actor.
pub(crate) enum EventBusMsg {
    /// Update the named-session audience for every subscription owned by a
    /// client. `None` preserves the legacy anonymous/global view.
    SetClientSession {
        client_id: u64,
        named_session_id: Option<String>,
    },
    /// Register a client for a channel.
    Subscribe {
        client_id: u64,
        channel: EventChannel,
        sender: mpsc::Sender<ClientEvent>,
        /// Signals the gateway to close the whole client connection when event
        /// delivery can no longer be lossless.
        disconnect: tokio::sync::watch::Sender<bool>,
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
    #[allow(dead_code)]
    PublishExcept {
        payload: EventPayload,
        channel: EventChannel,
        excluded_client_id: u64,
    },
    /// Publish a resource event only to legacy anonymous clients and clients
    /// attached to the owning named session.
    PublishSession {
        payload: EventPayload,
        channel: EventChannel,
        session_id: Option<String>,
    },
    /// Session-scoped publish with one directly-notified client excluded.
    PublishSessionExcept {
        payload: EventPayload,
        channel: EventChannel,
        session_id: Option<String>,
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

#[allow(dead_code)]
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

pub(crate) async fn publish_session_event(
    actor: &'static str,
    event_bus: &mpsc::Sender<EventBusMsg>,
    channel: EventChannel,
    payload: EventPayload,
    session_id: Option<String>,
) {
    if let Err(error) = event_bus
        .send(EventBusMsg::PublishSession {
            payload,
            channel: channel.clone(),
            session_id,
        })
        .await
    {
        warn!(%actor, %channel, "actor: failed to publish session event: {error}");
    }
}

pub(crate) async fn publish_session_event_except(
    actor: &'static str,
    event_bus: &mpsc::Sender<EventBusMsg>,
    channel: EventChannel,
    payload: EventPayload,
    session_id: Option<String>,
    excluded_client_id: u64,
) {
    if let Err(error) = event_bus
        .send(EventBusMsg::PublishSessionExcept {
            payload,
            channel: channel.clone(),
            session_id,
            excluded_client_id,
        })
        .await
    {
        warn!(%actor, %channel, %excluded_client_id, "actor: failed to publish session event: {error}");
    }
}

pub(crate) async fn send_gateway_event(
    actor: &'static str,
    sys: &ActorSystem,
    client_id: u64,
    payload: EventPayload,
    session_id: Option<String>,
) {
    if let Err(error) = sys
        .gateway
        .send(GatewayMsg::SendEvent {
            client_id,
            payload,
            session_id,
        })
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
    /// Resource provider registry for `:run(need.X=Y)` admission gating.
    /// Defaults to an empty registry; populated from `daemon.toml` when
    /// providers are configured. Wrapped in `Arc` so cheap clone in the
    /// scheduler's hot path stays cheap.
    resources: Arc<ProviderRegistry>,
}

impl ActorSystem {
    /// Prove the scheduler can activate, but keep execution paused until the
    /// lifecycle publishes durable restart completion.
    pub(crate) async fn activate_restart_successor(&self) -> Result<()> {
        let (reply, activated) = tokio::sync::oneshot::channel();
        self.scheduler
            .send(SchedulerMsg::Activate { reply })
            .await
            .map_err(|_| anyhow::anyhow!("scheduler stopped before successor activation"))?;
        activated.await.map_err(|_| {
            anyhow::anyhow!("scheduler dropped successor activation acknowledgement")
        })?
    }

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
    lifecycle: std::sync::Arc<crate::lifecycle::DaemonLifecycle>,
) -> Result<ActorSystem> {
    // Create channels.
    let (gw_tx, gw_rx) = mpsc::channel::<GatewayMsg>(ACTOR_CHANNEL_CAP);
    let (sched_tx, sched_rx) = mpsc::channel::<SchedulerMsg>(ACTOR_CHANNEL_CAP);
    let (pm_tx, pm_rx) = mpsc::channel::<ProcessMgrMsg>(ACTOR_CHANNEL_CAP);
    let (ss_tx, ss_rx) = mpsc::channel::<ScopeStoreMsg>(ACTOR_CHANNEL_CAP);
    let (eb_tx, eb_rx) = mpsc::channel::<EventBusMsg>(ACTOR_CHANNEL_CAP);

    let resources = Arc::new(crate::resource::registry_from_config(&config.resources)?);

    let sys = ActorSystem {
        gateway: gw_tx,
        scheduler: sched_tx,
        process_mgr: pm_tx,
        scope_store: ss_tx,
        event_bus: eb_tx,
        config,
        resources,
    };

    // ScopeStore restores persisted state before the daemon is considered ready.
    scope_store::spawn(ss_rx, scope_db, sys.clone()).await?;
    event_bus::spawn(eb_rx);
    process_mgr::spawn(pm_rx, sys.clone());
    if let Err(error) =
        scheduler::spawn(sched_rx, scheduler_db, sys.clone(), lifecycle.clone()).await
    {
        send_shutdown("scope_store", &sys.scope_store, ScopeStoreMsg::Shutdown).await;
        send_shutdown("process_mgr", &sys.process_mgr, ProcessMgrMsg::Shutdown).await;
        send_shutdown("event_bus", &sys.event_bus, EventBusMsg::Shutdown).await;
        return Err(anyhow::anyhow!("initialize scheduler: {error}"));
    }
    if let Err(error) = gateway::spawn(gw_rx, socket_path, sys.clone(), lifecycle).await {
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
            resources: std::sync::Arc::new(crate::resource::ProviderRegistry::empty()),
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
            std::sync::Arc::new(crate::lifecycle::DaemonLifecycle::new(
                socket_path.clone(),
                crate::lifecycle::RestartOwnership::Standalone,
            )),
        )
        .await;
        let Err(error) = result else {
            panic!("gateway initialization failure should stop daemon startup");
        };

        assert!(
            error.to_string().contains("bind socket"),
            "unexpected gateway init error: {error}"
        );
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
            std::sync::Arc::new(crate::lifecycle::DaemonLifecycle::new(
                PathBuf::from("/tmp/cue-spawn-all-scheduler-init-fails.sock"),
                crate::lifecycle::RestartOwnership::Standalone,
            )),
        )
        .await;
        let Err(error) = result else {
            panic!("scheduler initialization failure should stop daemon startup");
        };

        let message = error.to_string();
        assert!(message.contains("initialize scheduler"));
        assert!(message.contains("load persisted crons"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn starting_scheduler_rejects_direct_execution_message() {
        let dir = PathBuf::from(format!("/tmp/csg-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("create scheduler gate test dir");
        let socket = dir.join("cued.sock");
        let marker = dir.join("scheduler-bypass-ran");
        let startup = crate::lifecycle::RestartRecord {
            restart_id: "scheduler-gate".into(),
            daemon_instance_id: "predecessor".into(),
            protocol_version: cue_core::ipc::IPC_PROTOCOL_VERSION,
            target_generation: "target".into(),
            phase: crate::lifecycle::RestartPhase::Armed,
            supervisor_restart: false,
        };
        let lifecycle = std::sync::Arc::new(crate::lifecycle::DaemonLifecycle::new_with_startup(
            socket.clone(),
            crate::lifecycle::RestartOwnership::Standalone,
            Some(startup),
        ));
        let sys = spawn_all(
            socket,
            in_memory_db(),
            in_memory_db(),
            crate::config::Config::default(),
            lifecycle.clone(),
        )
        .await
        .expect("spawn starting actor system");

        let (reply, connected) = tokio::sync::oneshot::channel();
        sys.scheduler
            .send(SchedulerMsg::Connect {
                client_id: 4242,
                session_id: "scheduler-gate".into(),
                snapshot: cue_core::scope::EnvSnapshot {
                    env: std::collections::BTreeMap::from([(
                        "PATH".into(),
                        "/usr/bin:/bin".into(),
                    )]),
                    cwd: dir.clone(),
                },
                refresh: false,
                reply,
            })
            .await
            .expect("send direct scheduler connect");
        connected
            .await
            .expect("receive scheduler connect response")
            .expect("connect direct scheduler client");
        let command = crate::parser::parse_command(
            &format!("/usr/bin/touch {}", marker.display()),
            cue_core::mode::Mode::Job,
        )
        .expect("parse scheduler bypass command");
        sys.scheduler
            .send(SchedulerMsg::Eval {
                client_id: 4242,
                request_id: 1,
                command: Box::new(command),
            })
            .await
            .expect("send direct scheduler Eval");

        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        assert!(
            !marker.exists(),
            "scheduler startup gate must reject execution even when gateway is bypassed"
        );

        sys.activate_restart_successor()
            .await
            .expect("arm successor activation");
        let flood_tx = sys.scheduler.clone();
        let flood = tokio::spawn(async move {
            let mut client_id = 10_000;
            loop {
                if flood_tx
                    .send(SchedulerMsg::Disconnect { client_id })
                    .await
                    .is_err()
                {
                    break;
                }
                client_id += 1;
            }
        });
        lifecycle.mark_startup_restart_completed();
        tokio::time::timeout(
            std::time::Duration::from_millis(500),
            lifecycle.wait_for_execution_ready(),
        )
        .await
        .expect("startup activation must outrank a continuously ready control queue");
        flood.abort();
        let _ = flood.await;

        sys.shutdown_with_reason("scheduler startup gate test complete")
            .await;
        drop(sys);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        std::fs::remove_dir_all(dir).expect("remove scheduler gate test dir");
    }
}
