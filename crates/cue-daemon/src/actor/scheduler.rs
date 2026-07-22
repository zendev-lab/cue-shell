//! Scheduler actor — command routing, ID assignment, chain execution, cron timer heap.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use rusqlite::Connection;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use cue_core::chain::{
    LeafStatus, advance_chain, aggregate_chain_exit_code, flatten_leaves, initially_ready,
    is_chain_terminal,
};
use cue_core::command::ModeParams;
use cue_core::command_spec::{COMMAND_SPECS, CommandCategory, CommandSpec, command_spec};
use cue_core::cron::{CronSchedule, CronStatus, parse_schedule_text};
#[cfg(test)]
use cue_core::ipc::ForegroundRole;
use cue_core::ipc::{
    ChainInfo, ChainJobInfo, CronInfo, EventPayload, JobInfo, JobOpenHint, OkPayload,
    OutputEncoding, PageInfo, ResponsePayload, ScriptInfo, ScriptInfoStatus, ScriptItemInfo,
    ScriptItemResult, ScriptRunStatus, ScriptSource, ScriptSubmitError, SessionInfo,
    SessionScopeState, StreamText, error_code,
};
use cue_core::job::{CancelReason, EXIT_CODE_UNAVAILABLE, JobStatus, LaunchOptions};
use cue_core::mode::Mode;
use cue_core::pipeline::{ChainNode, command_prefers_foreground};
#[cfg(test)]
use cue_core::pipeline::{ParallelOp, SerialOp};
use cue_core::resource::Need;
use cue_core::scope::{EnvDelta, EnvSnapshot, Scope};
use cue_core::{ChainId, CronId, EventChannel, JobId, ScopeHash, ScriptId};

use crate::config::{BlockDecision, Config};
use crate::parser::{ResolvedCommand, ResolvedScriptItem, Token, Tokenizer, parse_command};
use crate::resource::RejectGroup;
use crate::storage;
use crate::word_expansion::expand_command_line;

use super::cron_schedule::next_trigger_instant;
use super::script_record::{
    ScriptFinish, persist_finished as persist_script_finished,
    persist_submission as persist_script_submission,
};
use super::{
    ActorSystem, GatewayMsg, ProcessJobOptions, ProcessMgrMsg, SchedulerMsg, ScopeStoreMsg,
    SessionBinding, SessionCommand, SessionCommandResult, StderrSnapshot,
    publish_session_event as publish_actor_session_event,
    publish_session_event_except as publish_actor_session_event_except,
    send_gateway_event as send_actor_gateway_event,
};

const MAX_OUTPUT_TAIL_BYTES: usize = cue_core::ipc::MAX_MESSAGE_SIZE / 4;
const SCOPE_GC_INTERVAL: Duration = Duration::from_secs(60);
const SCRIPT_SNAPSHOT_IDENTITY_CAPACITY: usize = 65_536;
const SCRIPT_SNAPSHOT_RESPONSE_CAPACITY: usize = 1_024;
const SCRIPT_SNAPSHOT_MAX_ITEM_BYTES: usize = 8 * 1024 * 1024;
const SCRIPT_SNAPSHOT_MAX_TOTAL_BYTES: usize = 64 * 1024 * 1024;
const SCRIPT_SNAPSHOT_TTL: Duration = Duration::from_secs(15 * 60);

// ── Chain state ─────────────────────────────────────────────────────────────

/// Tracks a running chain's execution state.
struct ChainState {
    node: ChainNode,
    /// Maps each leaf index (0-based, left-to-right DFS) to its `JobId`.
    leaf_jobs: HashMap<usize, JobId>,
    /// Maps each leaf index to its current status.
    leaf_status: HashMap<usize, LeafStatus>,
    scope_hash: ScopeHash,
    pipeline_text: String,
    /// Process execution options shared by jobs in this chain.
    process: ProcessJobContext,
    /// Whether scope-transform leaves may derive a new scope for later leaves.
    scope_enabled: bool,
    /// Durable named-session owner inherited by every leaf.
    session_id: Option<String>,
}

// ── Job tracking ────────────────────────────────────────────────────────────

/// Scheduler-side view of every spawned job.
struct JobEntry {
    job_id: JobId,
    session_id: Option<String>,
    pipeline_text: String,
    status: JobStatus,
    exit_code: Option<i32>,
    start_scope: Option<ScopeHash>,
    end_scope: Option<ScopeHash>,
    open_hint: JobOpenHint,
    chain_id: Option<ChainId>,
    chain_index: Option<usize>,
    chain_total: Option<usize>,
    /// Human-readable reason a job is held in `Pending` status — currently
    /// used by resource admission to surface why a `:run(need.X=Y)` chain
    /// was rejected by `ProviderRegistry::try_reserve`. `None` for jobs
    /// that are running or already terminal.
    pending_reason: Option<String>,
}

// ── Cron entry ──────────────────────────────────────────────────────────────

/// A registered cron / timer entry.
#[derive(Clone)]
struct CronEntry {
    cron_id: CronId,
    schedule: CronSchedule,
    chain: ChainNode,
    scope_hash: ScopeHash,
    status: CronStatus,
    next_trigger: Instant,
    /// Legacy cwd override restored from older cron records.
    ///
    /// New cron `cwd` mode params are captured in `scope_hash` instead.
    cwd_override: Option<std::path::PathBuf>,
    /// Whether scope-transform leaves may derive a new scope for later leaves.
    scope_enabled: bool,
    /// Whether the wrapper binary is enabled for jobs spawned by this cron.
    wrapper_enabled: bool,
    /// Durable named-session owner inherited by triggered jobs.
    session_id: Option<String>,
}

#[derive(Debug, Clone, Copy)]
struct PendingWait {
    client_id: u64,
    request_id: u32,
}

struct PendingScriptRun {
    client_id: u64,
    script_id: ScriptId,
    mode: Mode,
    source: ScriptSource,
    items: VecDeque<ResolvedScriptItem>,
    next_index: usize,
    item_scope: ScopeHash,
    created_items: Vec<ScriptItemInfo>,
    last_exit_code: i32,
    waiting_index: Option<usize>,
    /// Captured at submission so later items do not depend on a live transport.
    session_id: Option<String>,
}

struct CompletedScriptSnapshot {
    info: Option<ScriptInfo>,
    /// Durable named-session owner captured when the script was submitted.
    session_id: Option<String>,
    completed_at: Instant,
    response_bytes: usize,
}

#[derive(Clone, Default)]
struct CommandExecutionContext {
    scope_override: Option<ScopeHash>,
    direct_output_client: Option<u64>,
    session_id: Option<String>,
}

#[derive(Clone, Default)]
struct LaunchDefaults {
    pty: Option<bool>,
    wrapper_enabled: Option<bool>,
}

struct SessionState {
    scope: ScopeHash,
    incarnation: u64,
    defaults: LaunchDefaults,
    connected_clients: usize,
    disconnected_at: Option<Instant>,
    /// Present only for daemon-owned durable named sessions. Anonymous
    /// handshake sessions keep using the bounded disconnect TTL.
    named: Option<NamedSessionMeta>,
}

#[derive(Clone)]
struct NamedSessionMeta {
    id: String,
    name: String,
    scope_durable: bool,
    created_at_ms: i64,
    updated_at_ms: i64,
    archived_at_ms: Option<i64>,
}

/// Durable identity restored without a cursor because its live scope contained
/// credential-like environment names and intentionally stayed process-local.
#[derive(Clone)]
struct UnavailableNamedSession {
    meta: NamedSessionMeta,
    defaults: LaunchDefaults,
}

const SESSION_GC_TTL: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, Copy)]
struct ChainCompletion {
    exit_code: i32,
    end_scope: Option<ScopeHash>,
}

#[derive(Clone)]
struct PendingResourceAdmission {
    plan: cue_core::pipeline::JobPlan,
    base_scope: ScopeHash,
    options: ProcessJobOptions,
    needs: Need,
}

// ── Scheduler state (all mutable state lives here) ──────────────────────────

struct SchedulerState {
    next_job: u32,
    next_cron: u32,
    next_chain: u32,
    next_script: u32,
    next_session_incarnation: u64,

    /// Active chains keyed by `ChainId`.
    chains: HashMap<ChainId, ChainState>,
    /// Reverse lookup: `JobId` → `(ChainId, leaf_index)`.
    job_to_chain: HashMap<JobId, (ChainId, usize)>,
    /// All jobs the scheduler knows about.
    jobs: HashMap<JobId, JobEntry>,
    /// Registered cron entries.
    crons: HashMap<CronId, CronEntry>,
    /// Deferred `:wait` responses keyed by job ID.
    job_waiters: HashMap<JobId, Vec<PendingWait>>,
    /// File script runs waiting for item completion.
    pending_scripts: HashMap<ScriptId, PendingScriptRun>,
    pending_script_jobs: HashMap<JobId, ScriptId>,
    pending_script_chains: HashMap<ChainId, ScriptId>,
    /// Completed chain results retained only until their owning script consumes them.
    completed_chains: HashMap<ChainId, ChainCompletion>,
    /// Bounded daemon-lifetime recovery snapshots for completed scripts.
    completed_script_snapshots: HashMap<ScriptId, CompletedScriptSnapshot>,
    completed_script_snapshot_order: VecDeque<ScriptId>,
    completed_script_snapshot_responses: usize,
    completed_script_snapshot_bytes: usize,
    /// Logical sessions keyed by stable client-provided session id.
    sessions: HashMap<String, SessionState>,
    /// Named sessions whose volatile scope did not survive a daemon restart.
    unavailable_named_sessions: HashMap<String, UnavailableNamedSession>,
    /// Transport client id to logical session id.
    client_sessions: HashMap<u64, String>,
    /// Jobs waiting for resource admission, preserved in FIFO retry order.
    pending_resource_jobs: VecDeque<JobId>,
    /// Spawn context for each resource-pending job.
    pending_resource: HashMap<JobId, PendingResourceAdmission>,
}

#[derive(Clone, Copy)]
struct SchedulerIo<'a> {
    db: &'a Arc<Mutex<Connection>>,
    sys: &'a ActorSystem,
}

impl<'a> SchedulerIo<'a> {
    fn new(db: &'a Arc<Mutex<Connection>>, sys: &'a ActorSystem) -> Self {
        Self { db, sys }
    }
}

#[derive(Clone, Copy)]
struct SchedulerRuntime<'a> {
    io: SchedulerIo<'a>,
    config: &'a Config,
}

impl<'a> SchedulerRuntime<'a> {
    fn new(db: &'a Arc<Mutex<Connection>>, config: &'a Config, sys: &'a ActorSystem) -> Self {
        Self {
            io: SchedulerIo::new(db, sys),
            config,
        }
    }
}

impl SchedulerState {
    fn new() -> Self {
        Self {
            next_job: 1,
            next_cron: 1,
            next_chain: 1,
            next_script: 1,
            next_session_incarnation: 1,
            chains: HashMap::new(),
            job_to_chain: HashMap::new(),
            jobs: HashMap::new(),
            crons: HashMap::new(),
            job_waiters: HashMap::new(),
            pending_scripts: HashMap::new(),
            pending_script_jobs: HashMap::new(),
            pending_script_chains: HashMap::new(),
            completed_chains: HashMap::new(),
            completed_script_snapshots: HashMap::new(),
            completed_script_snapshot_order: VecDeque::new(),
            completed_script_snapshot_responses: 0,
            completed_script_snapshot_bytes: 0,
            sessions: HashMap::new(),
            unavailable_named_sessions: HashMap::new(),
            client_sessions: HashMap::new(),
            pending_resource_jobs: VecDeque::new(),
            pending_resource: HashMap::new(),
        }
    }

    fn alloc_job(&mut self) -> JobId {
        let id = JobId(self.next_job);
        self.next_job += 1;
        id
    }

    fn alloc_cron(&mut self) -> CronId {
        let id = CronId(self.next_cron);
        self.next_cron += 1;
        id
    }

    fn alloc_chain(&mut self) -> ChainId {
        let id = ChainId(self.next_chain);
        self.next_chain += 1;
        id
    }

    fn session_for_client(&self, client_id: u64) -> Option<&SessionState> {
        self.client_sessions
            .get(&client_id)
            .and_then(|session_id| self.sessions.get(session_id))
    }

    #[cfg(test)]
    fn session_for_client_mut(&mut self, client_id: u64) -> Option<&mut SessionState> {
        let session_id = self.client_sessions.get(&client_id)?.clone();
        self.sessions.get_mut(&session_id)
    }

    fn client_scope(&self, client_id: u64) -> Option<ScopeHash> {
        self.session_for_client(client_id)
            .map(|session| session.scope)
    }

    fn named_session_id_for_client(&self, client_id: u64) -> Option<&str> {
        self.session_for_client(client_id)
            .and_then(|session| session.named.as_ref())
            .map(|named| named.id.as_str())
    }

    fn has_named_session_id(&self, session_id: &str) -> bool {
        self.sessions.values().any(|session| {
            session
                .named
                .as_ref()
                .is_some_and(|named| named.id == session_id)
        }) || self.unavailable_named_sessions.contains_key(session_id)
    }

    fn wrapper_enabled(&self, client_id: u64, config: &Config) -> bool {
        self.session_for_client(client_id)
            .and_then(|session| session.defaults.wrapper_enabled)
            .unwrap_or(config.wrapper.enabled)
    }

    fn pty_default(&self, client_id: u64) -> bool {
        self.session_for_client(client_id)
            .and_then(|session| session.defaults.pty)
            .unwrap_or(true)
    }

    fn alloc_script(&mut self) -> ScriptId {
        let id = ScriptId(self.next_script);
        self.next_script += 1;
        id
    }

    fn alloc_session_incarnation(&mut self) -> anyhow::Result<u64> {
        let incarnation = self.next_session_incarnation;
        self.next_session_incarnation = self
            .next_session_incarnation
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("session incarnation space exhausted"))?;
        Ok(incarnation)
    }
}

/// A scheduler identity whose visibility and mutation rights follow its
/// durable named-session owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionOwnedTarget {
    Job(JobId),
    Chain(ChainId),
    Script(ScriptId),
    Cron(CronId),
}

impl SessionOwnedTarget {
    fn display(self) -> String {
        match self {
            Self::Job(id) => format!("job {id}"),
            Self::Chain(id) => format!("chain {id}"),
            Self::Script(id) => format!("script {id}"),
            Self::Cron(id) => format!("cron {id}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SessionAccessDenied(SessionOwnedTarget);

impl SessionAccessDenied {
    fn into_response(self) -> ResponsePayload {
        ResponsePayload::err(
            error_code::NOT_FOUND,
            format!("{} not found", self.0.display()),
        )
    }
}

/// Resolve the identity targeted by a command. Keeping this mapping in one
/// place makes it difficult for a newly added ID operation to accidentally
/// bypass the named-session boundary.
fn session_owned_target_for_command(command: &ResolvedCommand) -> Option<SessionOwnedTarget> {
    match command {
        ResolvedCommand::Kill { id } => parse_job_id(id)
            .map(SessionOwnedTarget::Job)
            .or_else(|| parse_cron_id(id).map(SessionOwnedTarget::Cron)),
        ResolvedCommand::KillJob { id }
        | ResolvedCommand::Retry { id }
        | ResolvedCommand::Out { id, .. }
        | ResolvedCommand::Err { id }
        | ResolvedCommand::JobOutput { id, .. }
        | ResolvedCommand::Fg { id, .. }
        | ResolvedCommand::Wait { id }
        | ResolvedCommand::Send { id, .. }
        | ResolvedCommand::Cancel { id } => parse_job_id(id).map(SessionOwnedTarget::Job),
        ResolvedCommand::CancelExecution { id } => parse_job_id(id)
            .map(SessionOwnedTarget::Job)
            .or_else(|| parse_chain_id(id).map(SessionOwnedTarget::Chain))
            .or_else(|| parse_script_id(id).map(SessionOwnedTarget::Script)),
        ResolvedCommand::RemoveCron { id }
        | ResolvedCommand::Pause { id }
        | ResolvedCommand::Resume { id } => parse_cron_id(id).map(SessionOwnedTarget::Cron),
        ResolvedCommand::Log { id: Some(id) } | ResolvedCommand::ShowLog { id: Some(id), .. } => {
            parse_job_id(id)
                .map(SessionOwnedTarget::Job)
                .or_else(|| parse_cron_id(id).map(SessionOwnedTarget::Cron))
        }
        _ => None,
    }
}

/// Enforce the named-session ownership boundary without changing legacy
/// anonymous behavior. Cross-session and unowned targets are reported as not
/// found so their existence is not disclosed to another named session.
fn authorize_session_owned_target(
    state: &SchedulerState,
    requester_session_id: Option<&str>,
    target: SessionOwnedTarget,
) -> Result<(), SessionAccessDenied> {
    let Some(requester_session_id) = requester_session_id else {
        return Ok(());
    };
    let authorized = match target {
        SessionOwnedTarget::Job(id) => state
            .jobs
            .get(&id)
            .is_some_and(|entry| entry.session_id.as_deref() == Some(requester_session_id)),
        SessionOwnedTarget::Chain(id) => state
            .chains
            .get(&id)
            .is_some_and(|entry| entry.session_id.as_deref() == Some(requester_session_id)),
        SessionOwnedTarget::Script(id) => state
            .pending_scripts
            .get(&id)
            .map(|entry| entry.session_id.as_deref())
            .or_else(|| {
                state
                    .completed_script_snapshots
                    .get(&id)
                    .map(|snapshot| snapshot.session_id.as_deref())
            })
            .is_some_and(|owner| owner == Some(requester_session_id)),
        SessionOwnedTarget::Cron(id) => state
            .crons
            .get(&id)
            .is_some_and(|entry| entry.session_id.as_deref() == Some(requester_session_id)),
    };
    if authorized {
        Ok(())
    } else {
        Err(SessionAccessDenied(target))
    }
}

/// Compute the complete set of live scope leaves owned by scheduler state.
///
/// `jobs` contains both active jobs and retained durable history, while `crons`
/// contains every durable cron record restored by the daemon. ScopeStore owns
/// ancestor traversal; the scheduler owns only these domain-level references.
fn scope_gc_roots(state: &SchedulerState) -> HashSet<ScopeHash> {
    let mut roots = HashSet::new();
    roots.extend(state.sessions.values().map(|session| session.scope));
    roots.extend(state.chains.values().map(|chain| chain.scope_hash));
    roots.extend(
        state
            .jobs
            .values()
            .flat_map(|job| [job.start_scope, job.end_scope])
            .flatten(),
    );
    roots.extend(state.crons.values().map(|cron| cron.scope_hash));
    roots.extend(
        state
            .pending_scripts
            .values()
            .map(|script| script.item_scope),
    );
    roots.extend(
        state
            .completed_chains
            .values()
            .filter_map(|chain| chain.end_scope),
    );
    roots.extend(
        state
            .pending_resource
            .values()
            .map(|pending| pending.base_scope),
    );
    roots
}

// ── Spawn the actor ─────────────────────────────────────────────────────────

fn command_starts_execution(command: &ResolvedCommand) -> bool {
    matches!(
        command,
        ResolvedCommand::Script { .. }
            | ResolvedCommand::Run { .. }
            | ResolvedCommand::Cron { .. }
            | ResolvedCommand::Retry { .. }
            | ResolvedCommand::Resume { .. }
    )
}

fn scheduler_is_idle_for_restart(state: &SchedulerState) -> bool {
    state.jobs.values().all(|job| job.status.is_terminal())
        && state.chains.is_empty()
        && state.pending_scripts.is_empty()
        && state.pending_resource.is_empty()
}

/// Restore durable Scheduler state and spawn the actor task.
pub(super) async fn spawn(
    mut rx: mpsc::Receiver<SchedulerMsg>,
    conn: Connection,
    sys: ActorSystem,
    lifecycle: Arc<crate::lifecycle::DaemonLifecycle>,
) -> anyhow::Result<()> {
    let db = storage::shared_connection(conn);
    let config = sys.config.clone();
    let mut state = SchedulerState::new();
    restore_named_sessions(&db, &mut state).await?;
    restore_jobs(&db, &mut state).await?;
    restore_crons(&db, &mut state).await?;
    restore_script_counter(&db, &mut state).await?;

    tokio::spawn(async move {
        prune_retained_job_history(&mut state, &db, &config, &sys).await;
        garbage_collect_scopes(&state, &sys).await;
        let mut next_scope_gc = Instant::now() + SCOPE_GC_INTERVAL;
        // A fenced successor may answer control-plane Ping while it proves its
        // exact identity, but it must not execute restored crons or new work
        // until the Armed -> Completed CAS has committed.
        let mut execution_paused = lifecycle.is_starting();
        let mut startup_activation_armed = false;
        let mut draining = false;
        debug!("scheduler: started");

        loop {
            if sweep_disconnected_sessions(&mut state) > 0 {
                garbage_collect_scopes(&state, &sys).await;
            }
            // Compute the sleep deadline from the nearest enabled cron trigger.
            let next_cron_deadline = state
                .crons
                .values()
                .filter(|c| !execution_paused && !draining && c.status.is_runnable())
                .map(|c| c.next_trigger)
                .min();
            let next_session_gc_deadline = state
                .sessions
                .values()
                .filter(|session| session.named.is_none())
                .filter(|session| session.connected_clients == 0)
                .filter_map(|session| session.disconnected_at)
                .map(|disconnected_at| disconnected_at + SESSION_GC_TTL)
                .min();
            let wake_deadline = [
                next_cron_deadline,
                next_session_gc_deadline,
                Some(next_scope_gc),
            ]
            .into_iter()
            .flatten()
            .min()
            .expect("periodic scope GC always supplies a scheduler deadline");
            let sleep = tokio::time::sleep_until(wake_deadline);
            tokio::pin!(sleep);

            tokio::select! {
                biased;

                // Once the durable completion signal is published, startup
                // activation must outrank an arbitrarily busy control queue.
                // Otherwise a stream of status/output traffic could keep a
                // valid successor in Starting until its readiness timeout.
                _ = lifecycle.wait_for_startup_activation(), if execution_paused && startup_activation_armed => {
                    execution_paused = false;
                    lifecycle.mark_startup_execution_ready();
                    debug!("scheduler: restart successor execution activated");
                }

                msg = rx.recv() => {
                    let Some(msg) = msg else { break };
                    match msg {
                        SchedulerMsg::Activate { reply } => {
                            let result = if draining || !execution_paused || !lifecycle.is_starting() {
                                Err(anyhow::anyhow!(
                                    "scheduler cannot arm successor activation in its current lifecycle state"
                                ))
                            } else {
                                startup_activation_armed = true;
                                Ok(())
                            };
                            let _ = reply.send(result);
                        }

                        SchedulerMsg::BeginDrain { reply } => {
                            draining = true;
                            let _ = reply.send(());
                        }

                        SchedulerMsg::Connect { client_id, session_id, snapshot, refresh, reply } => {
                            let result = match connect_session(client_id, session_id, snapshot, refresh, &mut state, &sys).await {
                                Ok(binding) => match set_client_event_session(&sys, client_id, &binding).await {
                                    Ok(()) => Ok(binding),
                                    Err(error) => Err(error),
                                },
                                Err(error) => Err(error),
                            };
                            let _ = reply.send(result);
                        }

                        SchedulerMsg::Session { client_id, command, reply } => {
                            let mut result = handle_session_command(
                                client_id,
                                command,
                                &mut state,
                                &db,
                            )
                            .await;
                            if let Some(binding) = result.binding.as_ref()
                                && let Err(error) = set_client_event_session(&sys, client_id, binding).await
                            {
                                result = SessionCommandResult {
                                    payload: ResponsePayload::err(error_code::INTERNAL, error.to_string()),
                                    binding: None,
                                };
                            }
                            let _ = reply.send(result);
                        }

                        SchedulerMsg::Disconnect { client_id } => {
                            disconnect_session(client_id, &mut state);
                        }

                        SchedulerMsg::ScriptInfo { client_id, request_id, id } => {
                            let response = if state.session_for_client(client_id).is_none() {
                                ResponsePayload::err(
                                    error_code::INVALID_REQUEST,
                                    "client session handshake required",
                                )
                            } else {
                                script_info_response(&id, client_id, &mut state)
                            };
                            send_gateway_response(&sys, client_id, request_id, response).await;
                        }

                        SchedulerMsg::Eval { client_id, request_id, command } => {
                            if state.session_for_client(client_id).is_none() {
                                send_gateway_response(
                                    &sys,
                                    client_id,
                                    request_id,
                                    ResponsePayload::err(error_code::INVALID_REQUEST, "client session handshake required"),
                                )
                                .await;
                                continue;
                            }
                            if (execution_paused || draining) && command_starts_execution(&command) {
                                send_gateway_response(
                                    &sys,
                                    client_id,
                                    request_id,
                                    ResponsePayload::err(
                                        error_code::DAEMON_DRAINING,
                                        "daemon is draining; new execution admission is closed",
                                    ),
                                )
                                .await;
                                continue;
                            }
                            match *command {
                                ResolvedCommand::Wait { id } => {
                                    if let Some(response) = handle_wait_command(
                                        id,
                                        client_id,
                                        request_id,
                                        &mut state,
                                    )
                                    .await
                                    {
                                        send_gateway_response(
                                            &sys,
                                            client_id,
                                            request_id,
                                            response,
                                        )
                                        .await;
                                    }
                                }
                                ResolvedCommand::Script {
                                    mode,
                                    source: source @ ScriptSource::File { .. },
                                    items,
                                } => {
                                    if let Some(response) = start_pending_script_run(
                                        mode,
                                        source,
                                        items,
                                        client_id,
                                        &mut state,
                                        SchedulerRuntime::new(&db, &config, &sys),
                                    )
                                    .await
                                    {
                                        send_gateway_response(
                                            &sys,
                                            client_id,
                                            request_id,
                                            response,
                                        )
                                        .await;
                                    }
                                }
                                other => {
                                    let response =
                                        handle_command(other, client_id, &mut state, &db, &config, &sys)
                                            .await;
                                    send_gateway_response(&sys, client_id, request_id, response)
                                        .await;
                                }
                            }
                        }

                        SchedulerMsg::JobFinished { job_id, exit_code } => {
                            handle_job_finished(job_id, exit_code, &mut state, &db, &sys).await;
                            advance_pending_scripts_after_terminal_job(
                                job_id,
                                exit_code,
                                &mut state,
                                SchedulerRuntime::new(&db, &config, &sys),
                            )
                            .await;
                        }

                        SchedulerMsg::Shutdown => {
                            debug!("scheduler: shutting down");

                            // Cancel all active chain jobs before shutting down.
                            let mut jobs_to_persist = Vec::new();
                            let chain_ids: Vec<ChainId> =
                                state.chains.keys().copied().collect();
                            for chain_id in chain_ids {
                                if let Some(chain) = state.chains.get(&chain_id) {
                                    let leaf_indices: Vec<usize> =
                                        chain.leaf_status.keys().copied().collect();
                                    for idx in leaf_indices {
                                        let Some(chain) = state.chains.get(&chain_id) else {
                                            break;
                                        };
                                        let status = chain.leaf_status.get(&idx).cloned();
                                        let leaf_job = chain.leaf_jobs.get(&idx).copied();
                                        match status {
                                            Some(LeafStatus::Running) => {
                                                let mut kill_accepted = false;
                                                if let Some(jid) = leaf_job {
                                                    match kill_process_job(&sys, jid).await {
                                                        Ok(()) => {
                                                            kill_accepted = true;
                                                            if let Some(entry) =
                                                                state.jobs.get_mut(&jid)
                                                            {
                                                                entry.status =
                                                                    JobStatus::Cancelled(
                                                                        CancelReason::ChainAborted,
                                                                    );
                                                                jobs_to_persist
                                                                    .push(stored_job_from_entry(entry));
                                                            }
                                                        }
                                                        Err(error) => {
                                                            warn!(%jid, "scheduler: failed to kill chain leaf during shutdown: {error}");
                                                        }
                                                    }
                                                }
                                                if kill_accepted
                                                    && let Some(chain) =
                                                        state.chains.get_mut(&chain_id)
                                                {
                                                    chain
                                                        .leaf_status
                                                        .insert(idx, LeafStatus::Cancelled);
                                                }
                                            }
                                            Some(LeafStatus::Pending) => {
                                                if let Some(chain) = state.chains.get_mut(&chain_id) {
                                                    chain
                                                        .leaf_status
                                                        .insert(idx, LeafStatus::Cancelled);
                                                }
                                                if let Some(jid) = leaf_job
                                                    && let Some(entry) = state.jobs.get_mut(&jid)
                                                {
                                                    entry.status = JobStatus::Cancelled(
                                                        CancelReason::ChainAborted,
                                                    );
                                                    jobs_to_persist
                                                        .push(stored_job_from_entry(entry));
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                                // Remove chain tracking.
                                if let Some(finished) = state.chains.remove(&chain_id) {
                                    for jid in finished.leaf_jobs.values() {
                                        state.job_to_chain.remove(jid);
                                    }
                                }
                            }

                            for entry in state.jobs.values_mut() {
                                if !entry.status.is_terminal() {
                                    entry.status = JobStatus::Killed;
                                    jobs_to_persist.push(stored_job_from_entry(entry));
                                }
                            }
                            for stored in jobs_to_persist {
                                if let Err(error) = persist_job_entry(&db, stored).await {
                                    warn!("scheduler: failed to persist shutdown job state: {error}");
                                }
                            }
                            fail_pending_scripts_on_shutdown(
                                &mut state,
                                SchedulerRuntime::new(&db, &config, &sys),
                            )
                            .await;

                            break;
                        }
                    }
                    if draining && scheduler_is_idle_for_restart(&state) {
                        lifecycle.mark_drained();
                    }
                    if prune_retained_job_history(&mut state, &db, &config, &sys).await {
                        garbage_collect_scopes(&state, &sys).await;
                    }
                }

                () = &mut sleep => {
                    let now = Instant::now();
                    if !execution_paused
                        && !draining
                        && next_cron_deadline.is_some_and(|deadline| deadline <= now)
                    {
                        fire_due_crons(&mut state, &db, &config, &sys).await;
                    }
                    if next_scope_gc <= now {
                        garbage_collect_scopes(&state, &sys).await;
                        next_scope_gc = now + SCOPE_GC_INTERVAL;
                    }
                }
            }
        }

        debug!("scheduler: stopped");
    });

    Ok(())
}

fn named_session_key(id: &str) -> String {
    format!("named:{id}")
}

fn ephemeral_session_key(id: &str) -> String {
    format!("ephemeral:{id}")
}

async fn restore_named_sessions(
    db: &storage::SharedConnection,
    state: &mut SchedulerState,
) -> anyhow::Result<()> {
    let restored = storage::with_connection(db, |conn| {
        let sessions = storage::load_sessions(conn)?;
        for session in &sessions {
            if let Some(scope_hash) = session.scope_hash
                && storage::get_scope(conn, &scope_hash)?.is_none()
            {
                return Err(anyhow::anyhow!(
                    "named session {} references missing scope {}",
                    session.id,
                    scope_hash
                ));
            }
        }
        Ok(sessions)
    })
    .await
    .map_err(|error| anyhow::anyhow!("load persisted named sessions: {error}"))?;

    for session in restored {
        let incarnation = state.alloc_session_incarnation()?;
        let meta = NamedSessionMeta {
            id: session.id.clone(),
            name: session.name,
            scope_durable: session.scope_hash.is_some(),
            created_at_ms: session.created_at_ms,
            updated_at_ms: session.updated_at_ms,
            archived_at_ms: session.archived_at_ms,
        };
        let defaults = LaunchDefaults {
            pty: session.pty_default,
            wrapper_enabled: session.wrapper_enabled,
        };
        if let Some(scope) = session.scope_hash {
            state.sessions.insert(
                named_session_key(&session.id),
                SessionState {
                    scope,
                    incarnation,
                    defaults,
                    connected_clients: 0,
                    disconnected_at: None,
                    named: Some(meta),
                },
            );
        } else {
            state
                .unavailable_named_sessions
                .insert(session.id, UnavailableNamedSession { meta, defaults });
        }
    }

    let ready = state
        .sessions
        .values()
        .filter(|session| session.named.is_some())
        .count();
    if ready > 0 || !state.unavailable_named_sessions.is_empty() {
        info!(
            ready,
            needs_refresh = state.unavailable_named_sessions.len(),
            "scheduler: restored named sessions"
        );
    }
    Ok(())
}

async fn restore_jobs(
    db: &storage::SharedConnection,
    state: &mut SchedulerState,
) -> anyhow::Result<()> {
    let restored = storage::with_connection(db, storage::load_job_history)
        .await
        .map_err(|error| anyhow::anyhow!("load persisted job history: {error}"))?;

    let mut max_job = 0;
    let mut interrupted = 0usize;
    for mut job in restored {
        if let Some(session_id) = job.session_id.as_deref()
            && !state.has_named_session_id(session_id)
        {
            return Err(anyhow::anyhow!(
                "persisted job {} references missing named session {}",
                job.id,
                session_id
            ));
        }
        let Some(job_id) = parse_job_id(&job.id) else {
            return Err(anyhow::anyhow!("invalid persisted job id {}", job.id));
        };
        if !job.status.is_terminal() {
            interrupted += 1;
            job.status = JobStatus::Killed;
            job.exit_code = None;
            let repaired = job.clone();
            storage::with_connection(db, move |conn| storage::upsert_job_history(conn, &repaired))
                .await
                .map_err(|error| {
                    anyhow::anyhow!("persist interrupted job {} during restore: {error}", job.id)
                })?;
        }
        max_job = max_job.max(job_id.0);
        state.jobs.insert(
            job_id,
            JobEntry {
                job_id,
                session_id: job.session_id,
                pipeline_text: job.pipeline,
                status: job.status,
                exit_code: job.exit_code,
                start_scope: job.start_scope,
                end_scope: job.end_scope,
                open_hint: JobOpenHint::Stream,
                chain_id: None,
                chain_index: None,
                chain_total: None,
                pending_reason: None,
            },
        );
    }

    if max_job > 0 {
        state.next_job = max_job + 1;
        info!(
            restored = state.jobs.len(),
            next_job = state.next_job,
            "scheduler: restored job history"
        );
    }
    if interrupted > 0 {
        warn!(
            interrupted,
            "scheduler: marked nonterminal history as killed because no process ownership survived"
        );
    }

    Ok(())
}

async fn restore_crons(
    db: &storage::SharedConnection,
    state: &mut SchedulerState,
) -> anyhow::Result<()> {
    let restored = storage::with_connection(db, storage::load_crons)
        .await
        .map_err(|error| anyhow::anyhow!("load persisted crons: {error}"))?;

    let mut max_cron = 0;
    for loaded in restored {
        let storage::LoadedCron {
            record: cron,
            elapsed,
        } = loaded;
        if let Some(session_id) = cron.session_id.as_deref()
            && !state.has_named_session_id(session_id)
        {
            return Err(anyhow::anyhow!(
                "persisted cron {} references missing named session {}",
                cron.id,
                session_id
            ));
        }
        let Some(cron_id) = parse_cron_id(&cron.id) else {
            return Err(anyhow::anyhow!("invalid persisted cron id {}", cron.id));
        };
        max_cron = max_cron.max(cron_id.0);

        let Some(scope_hash) = cron.scope_hash else {
            if cron.status == CronStatus::Paused {
                warn!(
                    cron_id = %cron.id,
                    "scheduler: skipping disabled cron whose sensitive scope was removed"
                );
                continue;
            }
            return Err(anyhow::anyhow!(
                "persisted runnable cron {} has no scope hash",
                cron.id
            ));
        };

        let Some(schedule) = parse_schedule_text(&cron.schedule) else {
            return Err(anyhow::anyhow!(
                "persisted cron {} has invalid schedule {}",
                cron.id,
                cron.schedule
            ));
        };

        let chain = parse_chain_text(&cron.command).map_err(|error| {
            anyhow::anyhow!("persisted cron {} has invalid command: {error}", cron.id)
        })?;

        let mut status = cron.status;
        if status.is_runnable()
            && let CronSchedule::Delay(duration) = &schedule
            && elapsed >= *duration
        {
            status = CronStatus::Expired;
            let stored = storage::StoredCron {
                id: cron.id.clone(),
                session_id: cron.session_id.clone(),
                schedule: cron.schedule.clone(),
                command: cron.command.clone(),
                status,
                scope_hash: cron.scope_hash,
                cwd_override: cron.cwd_override.clone(),
                scope_enabled: cron.scope_enabled,
                wrapper_enabled: cron.wrapper_enabled,
            };
            if let Err(e) =
                storage::with_connection(db, move |conn| storage::upsert_cron(conn, &stored)).await
            {
                return Err(anyhow::anyhow!(
                    "persist expired cron {} during restore: {e}",
                    cron.id
                ));
            }
        }
        let next_trigger = if status.is_terminal() {
            Instant::now()
        } else {
            let Some(next_trigger) = next_trigger_instant(&schedule, elapsed) else {
                return Err(anyhow::anyhow!(
                    "persisted cron {} has unreachable next trigger for schedule {}",
                    cron.id,
                    cron.schedule
                ));
            };
            next_trigger
        };

        state.crons.insert(
            cron_id,
            CronEntry {
                cron_id,
                schedule,
                chain,
                scope_hash,
                status,
                next_trigger,
                cwd_override: cron.cwd_override,
                scope_enabled: cron.scope_enabled,
                wrapper_enabled: cron.wrapper_enabled,
                session_id: cron.session_id,
            },
        );
    }

    if max_cron > 0 {
        state.next_cron = max_cron + 1;
        info!(
            restored = state.crons.len(),
            next_cron = state.next_cron,
            "scheduler: restored crons"
        );
    }

    Ok(())
}

async fn restore_script_counter(
    db: &storage::SharedConnection,
    state: &mut SchedulerState,
) -> anyhow::Result<()> {
    match storage::with_connection(db, storage::max_script_run_id).await {
        Ok(Some(max_id)) => {
            state.next_script = max_id + 1;
        }
        Ok(None) => {}
        Err(error) => return Err(anyhow::anyhow!("restore script counter: {error}")),
    }
    Ok(())
}

async fn prune_retained_job_history(
    state: &mut SchedulerState,
    db: &storage::SharedConnection,
    config: &Config,
    sys: &ActorSystem,
) -> bool {
    let keep = config.retention.max_job_history;
    let removed = match storage::with_connection(db, move |conn| {
        storage::prune_job_history(conn, keep)
    })
    .await
    {
        Ok(removed) => removed,
        Err(error) => {
            warn!("scheduler: failed to prune job history: {error}");
            return false;
        }
    };

    let removed_any = !removed.is_empty();
    for id in removed {
        if let Some(job_id) = parse_job_id(&id) {
            if let Some(entry) = state.jobs.remove(&job_id) {
                publish_session_event(
                    sys,
                    EventChannel::Jobs,
                    EventPayload::JobRemoved {
                        job_id: job_id.to_string(),
                    },
                    entry.session_id,
                )
                .await;
            }
            remove_job_logs(job_id).await;
        }
    }
    removed_any
}

async fn garbage_collect_scopes(state: &SchedulerState, sys: &ActorSystem) {
    let roots = scope_gc_roots(state);
    let (reply, result) = tokio::sync::oneshot::channel();
    if let Err(error) = sys
        .scope_store
        .send(ScopeStoreMsg::GarbageCollect { roots, reply })
        .await
    {
        warn!("scheduler: failed to request scope garbage collection: {error}");
        return;
    }
    match result.await {
        Ok(Ok(report)) => {
            debug!(
                retained = report.retained,
                removed_cached = report.removed_cached,
                removed_persisted = report.removed_persisted,
                "scheduler: scope garbage collection complete"
            );
        }
        Ok(Err(error)) => warn!("scheduler: scope garbage collection failed: {error}"),
        Err(error) => warn!("scheduler: scope garbage collection reply dropped: {error}"),
    }
}

async fn publish_session_event(
    sys: &ActorSystem,
    channel: EventChannel,
    payload: EventPayload,
    session_id: Option<String>,
) {
    publish_actor_session_event("scheduler", &sys.event_bus, channel, payload, session_id).await;
}

/// Publish the binding from the scheduler actor before it acknowledges the
/// handshake/session command. Messages sent by this actor are ordered, so a
/// later cron or job event cannot overtake the audience update.
async fn set_client_event_session(
    sys: &ActorSystem,
    client_id: u64,
    binding: &SessionBinding,
) -> anyhow::Result<()> {
    sys.event_bus
        .send(super::EventBusMsg::SetClientSession {
            client_id,
            named_session_id: binding.named_session_id.clone(),
        })
        .await
        .map_err(|error| anyhow::anyhow!("event bus unreachable during session bind: {error}"))
}

async fn publish_session_event_except(
    sys: &ActorSystem,
    channel: EventChannel,
    payload: EventPayload,
    session_id: Option<String>,
    excluded_client_id: u64,
) {
    publish_actor_session_event_except(
        "scheduler",
        &sys.event_bus,
        channel,
        payload,
        session_id,
        excluded_client_id,
    )
    .await;
}

async fn send_gateway_response(
    sys: &ActorSystem,
    client_id: u64,
    request_id: u32,
    payload: ResponsePayload,
) {
    if let Err(error) = sys
        .gateway
        .send(GatewayMsg::SendResponse {
            client_id,
            request_id,
            payload,
        })
        .await
    {
        warn!(%client_id, request_id, "scheduler: failed to send gateway response: {error}");
    }
}

async fn prune_retained_script_runs(db: &storage::SharedConnection, config: &Config) {
    let keep = config.retention.max_script_runs;
    if let Err(error) =
        storage::with_connection(db, move |conn| storage::prune_script_runs(conn, keep)).await
    {
        warn!("scheduler: failed to prune script runs: {error}");
    }
}

async fn persist_script_finished_with_retention(
    script_id: ScriptId,
    mode: Mode,
    created_items: &[ScriptItemInfo],
    finish: ScriptFinish,
    submit_error: Option<&ScriptSubmitError>,
    db: &storage::SharedConnection,
    config: &Config,
) -> anyhow::Result<()> {
    persist_script_finished(script_id, mode, created_items, finish, submit_error, db).await?;
    prune_retained_script_runs(db, config).await;
    Ok(())
}

fn stored_job_from_entry(entry: &JobEntry) -> storage::StoredJob {
    storage::StoredJob {
        id: entry.job_id.to_string(),
        session_id: entry.session_id.clone(),
        pipeline: entry.pipeline_text.clone(),
        status: entry.status.clone(),
        exit_code: entry.exit_code,
        start_scope: entry.start_scope,
        end_scope: entry.end_scope,
        chain_id: entry.chain_id.map(|id| id.to_string()),
        stderr: String::new(),
    }
}

async fn persist_job_entry(
    db: &storage::SharedConnection,
    stored: storage::StoredJob,
) -> anyhow::Result<()> {
    let job_id = stored.id.clone();
    storage::with_connection(db, move |conn| storage::upsert_job_history(conn, &stored))
        .await
        .map_err(|error| anyhow::anyhow!("persist job {job_id} history: {error}"))
}

fn stored_cron_from_entry(entry: &CronEntry) -> storage::StoredCron {
    storage::StoredCron {
        id: entry.cron_id.to_string(),
        session_id: entry.session_id.clone(),
        schedule: entry.schedule.display(),
        command: entry.chain.to_string(),
        status: entry.status,
        scope_hash: Some(entry.scope_hash),
        cwd_override: entry.cwd_override.clone(),
        scope_enabled: entry.scope_enabled,
        wrapper_enabled: entry.wrapper_enabled,
    }
}

async fn persist_cron_entry(
    db: &storage::SharedConnection,
    entry: &CronEntry,
) -> anyhow::Result<()> {
    persist_cron_record(db, stored_cron_from_entry(entry)).await
}

async fn persist_cron_record(
    db: &storage::SharedConnection,
    cron: storage::StoredCron,
) -> anyhow::Result<()> {
    let cron_id = cron.id.clone();
    storage::with_connection(db, move |conn| storage::upsert_cron(conn, &cron))
        .await
        .map_err(|error| anyhow::anyhow!("persist cron {cron_id}: {error}"))
}

async fn remove_cron_from_db(db: &storage::SharedConnection, cid: CronId) -> anyhow::Result<()> {
    let cron_id = cid.to_string();
    let cid_for_db = cron_id.clone();
    storage::with_connection(db, move |conn| storage::delete_cron(conn, &cid_for_db))
        .await
        .map_err(|error| anyhow::anyhow!("remove cron {cron_id}: {error}"))
}

async fn remove_cron_entry(
    state: &mut SchedulerState,
    db: &storage::SharedConnection,
    sys: &ActorSystem,
    cid: CronId,
) -> anyhow::Result<()> {
    remove_cron_from_db(db, cid).await?;
    if let Some(entry) = state.crons.remove(&cid) {
        info!(%cid, "scheduler: cron removed");
        publish_session_event(
            sys,
            EventChannel::Crons,
            EventPayload::CronRemoved {
                cron_id: cid.to_string(),
            },
            entry.session_id,
        )
        .await;
    }
    Ok(())
}

async fn mark_cron_failed(
    state: &mut SchedulerState,
    db: &storage::SharedConnection,
    cron_id: CronId,
    reason: &str,
) {
    warn!(%cron_id, reason = %reason, "scheduler: cron trigger failed");
    let Some(entry) = state.crons.get_mut(&cron_id) else {
        return;
    };
    entry.status = CronStatus::Failed;
    let stored = stored_cron_from_entry(entry);
    if let Err(error) = persist_cron_record(db, stored).await {
        warn!(%cron_id, "scheduler: failed to persist failed cron: {error}");
    }
}

async fn connect_session(
    client_id: u64,
    session_id: String,
    snapshot: EnvSnapshot,
    refresh: bool,
    state: &mut SchedulerState,
    sys: &ActorSystem,
) -> anyhow::Result<SessionBinding> {
    let public_session_id = session_id;
    let session_id = ephemeral_session_key(&public_session_id);
    let mut old_session_id = state.client_sessions.get(&client_id).cloned();
    let mut same_client_session = old_session_id.as_deref() == Some(session_id.as_str());

    if same_client_session && !state.sessions.contains_key(&session_id) {
        state.client_sessions.remove(&client_id);
        old_session_id = None;
        same_client_session = false;
    }

    if state.sessions.contains_key(&session_id) {
        let refreshed_scope = if refresh {
            Some(insert_scope(sys, Scope::root(snapshot)).await?)
        } else {
            None
        };
        let session = state
            .sessions
            .get_mut(&session_id)
            .expect("session exists after contains_key");
        if !same_client_session {
            session.connected_clients += 1;
        }
        session.disconnected_at = None;
        if let Some(scope) = refreshed_scope {
            session.scope = scope;
        }
        let scope = session.scope;
        let incarnation = session.incarnation;
        state.client_sessions.insert(client_id, session_id.clone());
        mark_replaced_session_disconnected(state, old_session_id, &session_id);
        return Ok(SessionBinding {
            session_id: public_session_id,
            named_session_id: None,
            scope,
            incarnation,
        });
    }

    let incarnation = state.alloc_session_incarnation()?;
    let scope = Scope::root(snapshot);
    let hash = insert_scope(sys, scope).await?;
    state.sessions.insert(
        session_id.clone(),
        SessionState {
            scope: hash,
            incarnation,
            defaults: LaunchDefaults::default(),
            connected_clients: 1,
            disconnected_at: None,
            named: None,
        },
    );
    state.client_sessions.insert(client_id, session_id.clone());
    mark_replaced_session_disconnected(state, old_session_id, &session_id);
    Ok(SessionBinding {
        session_id: public_session_id,
        named_session_id: None,
        scope: hash,
        incarnation,
    })
}

#[derive(Debug, Clone)]
enum NamedSessionLocation {
    Ready(String),
    NeedsRefresh(String),
}

#[derive(Debug, Clone, Copy)]
enum NamedSessionListFilter {
    Active,
    Archived,
    All,
}

async fn handle_session_command(
    client_id: u64,
    command: SessionCommand,
    state: &mut SchedulerState,
    db: &storage::SharedConnection,
) -> SessionCommandResult {
    if state.session_for_client(client_id).is_none() {
        return session_command_response(ResponsePayload::err(
            error_code::INVALID_REQUEST,
            "client session handshake required",
        ));
    }

    match command {
        SessionCommand::Create { name } => {
            if let Err(message) = validate_session_name(&name) {
                return session_command_response(ResponsePayload::err(
                    error_code::INVALID_REQUEST,
                    message,
                ));
            }
            if find_named_session(state, &name).is_some() {
                return session_command_response(ResponsePayload::err(
                    error_code::ALREADY_EXISTS,
                    format!("named session `{name}` already exists"),
                ));
            }

            let Some(current) = state.session_for_client(client_id) else {
                return session_command_response(missing_session_response());
            };
            let scope = current.scope;
            let defaults = current.defaults.clone();
            let now = unix_time_ms();
            let id = format!("SS-{}", uuid::Uuid::new_v4());
            let mut meta = NamedSessionMeta {
                id: id.clone(),
                name,
                scope_durable: false,
                created_at_ms: now,
                updated_at_ms: now,
                archived_at_ms: None,
            };
            let incarnation = match state.alloc_session_incarnation() {
                Ok(incarnation) => incarnation,
                Err(error) => {
                    return session_command_response(ResponsePayload::err(
                        error_code::INTERNAL,
                        error.to_string(),
                    ));
                }
            };
            let durable = match persist_named_session(db, &meta, scope, &defaults).await {
                Ok(durable) => durable,
                Err(error) => {
                    return session_command_response(ResponsePayload::err(
                        error_code::INTERNAL,
                        format!("persist named session: {error}"),
                    ));
                }
            };
            meta.scope_durable = durable;
            let key = named_session_key(&id);
            state.sessions.insert(
                key.clone(),
                SessionState {
                    scope,
                    incarnation,
                    defaults,
                    connected_clients: 0,
                    disconnected_at: None,
                    named: Some(meta),
                },
            );
            let binding = bind_client_to_ready_session(state, client_id, &key)
                .expect("newly inserted named session must be bindable");
            let info = ready_session_info(state, &key, client_id)
                .expect("newly inserted named session must have metadata");
            SessionCommandResult {
                payload: ResponsePayload::Ok(OkPayload::SessionInfo(Box::new(info))),
                binding: Some(binding),
            }
        }

        SessionCommand::List => {
            let sessions = named_session_list(state, client_id, NamedSessionListFilter::Active);
            session_command_response(ResponsePayload::Ok(OkPayload::SessionList(sessions)))
        }

        SessionCommand::ListArchived => {
            let sessions = named_session_list(state, client_id, NamedSessionListFilter::Archived);
            session_command_response(ResponsePayload::Ok(OkPayload::SessionList(sessions)))
        }

        SessionCommand::ListAll => {
            let sessions = named_session_list(state, client_id, NamedSessionListFilter::All);
            session_command_response(ResponsePayload::Ok(OkPayload::SessionList(sessions)))
        }

        SessionCommand::Archive { selector } => {
            set_named_session_archived(state, db, client_id, &selector, true).await
        }

        SessionCommand::Restore { selector } => {
            set_named_session_archived(state, db, client_id, &selector, false).await
        }

        SessionCommand::Attach { selector, refresh } => {
            let Some(location) = find_named_session(state, &selector) else {
                return session_command_response(ResponsePayload::err(
                    error_code::NOT_FOUND,
                    format!("named session `{selector}` not found"),
                ));
            };
            let archived_at_ms = match &location {
                NamedSessionLocation::Ready(key) => state
                    .sessions
                    .get(key)
                    .and_then(|session| session.named.as_ref())
                    .and_then(|meta| meta.archived_at_ms),
                NamedSessionLocation::NeedsRefresh(id) => state
                    .unavailable_named_sessions
                    .get(id)
                    .and_then(|session| session.meta.archived_at_ms),
            };
            if archived_at_ms.is_some() {
                return session_command_response(ResponsePayload::err(
                    error_code::INVALID_STATE,
                    format!("named session `{selector}` is archived; restore it before attaching"),
                ));
            }
            match location {
                NamedSessionLocation::Ready(key) => {
                    let replacement_scope = if refresh {
                        state.client_scope(client_id)
                    } else {
                        state.sessions.get(&key).map(|session| session.scope)
                    };
                    let Some(replacement_scope) = replacement_scope else {
                        return session_command_response(missing_session_response());
                    };
                    let Some(target) = state.sessions.get(&key) else {
                        return session_command_response(ResponsePayload::err(
                            error_code::NOT_FOUND,
                            format!("named session `{selector}` not found"),
                        ));
                    };
                    let mut meta = target
                        .named
                        .clone()
                        .expect("ready named session must have metadata");
                    let defaults = target.defaults.clone();
                    meta.updated_at_ms = unix_time_ms();
                    let durable = match persist_named_session(
                        db,
                        &meta,
                        replacement_scope,
                        &defaults,
                    )
                    .await
                    {
                        Ok(durable) => durable,
                        Err(error) => {
                            return session_command_response(ResponsePayload::err(
                                error_code::INTERNAL,
                                format!("persist named session attach: {error}"),
                            ));
                        }
                    };
                    meta.scope_durable = durable;
                    if let Some(target) = state.sessions.get_mut(&key) {
                        target.scope = replacement_scope;
                        target.named = Some(meta);
                    }
                    let Some(binding) = bind_client_to_ready_session(state, client_id, &key) else {
                        return session_command_response(ResponsePayload::err(
                            error_code::INTERNAL,
                            "named session disappeared while attaching",
                        ));
                    };
                    let info = ready_session_info(state, &key, client_id)
                        .expect("attached named session must have metadata");
                    SessionCommandResult {
                        payload: ResponsePayload::Ok(OkPayload::SessionInfo(Box::new(info))),
                        binding: Some(binding),
                    }
                }
                NamedSessionLocation::NeedsRefresh(id) => {
                    if !refresh {
                        return session_command_response(ResponsePayload::err(
                            error_code::INVALID_STATE,
                            format!(
                                "named session `{selector}` lost its volatile scope during daemon restart; attach with refresh=true to replace it explicitly"
                            ),
                        ));
                    }
                    let Some(scope) = state.client_scope(client_id) else {
                        return session_command_response(missing_session_response());
                    };
                    let Some(unavailable) = state.unavailable_named_sessions.get(&id).cloned()
                    else {
                        return session_command_response(ResponsePayload::err(
                            error_code::NOT_FOUND,
                            format!("named session `{selector}` not found"),
                        ));
                    };
                    let mut meta = unavailable.meta;
                    meta.updated_at_ms = unix_time_ms();
                    let incarnation = match state.alloc_session_incarnation() {
                        Ok(incarnation) => incarnation,
                        Err(error) => {
                            return session_command_response(ResponsePayload::err(
                                error_code::INTERNAL,
                                error.to_string(),
                            ));
                        }
                    };
                    let durable = match persist_named_session(
                        db,
                        &meta,
                        scope,
                        &unavailable.defaults,
                    )
                    .await
                    {
                        Ok(durable) => durable,
                        Err(error) => {
                            return session_command_response(ResponsePayload::err(
                                error_code::INTERNAL,
                                format!("refresh named session: {error}"),
                            ));
                        }
                    };
                    meta.scope_durable = durable;
                    state.unavailable_named_sessions.remove(&id);
                    let key = named_session_key(&id);
                    state.sessions.insert(
                        key.clone(),
                        SessionState {
                            scope,
                            incarnation,
                            defaults: unavailable.defaults,
                            connected_clients: 0,
                            disconnected_at: None,
                            named: Some(meta),
                        },
                    );
                    let binding = bind_client_to_ready_session(state, client_id, &key)
                        .expect("refreshed named session must be bindable");
                    let info = ready_session_info(state, &key, client_id)
                        .expect("refreshed named session must have metadata");
                    SessionCommandResult {
                        payload: ResponsePayload::Ok(OkPayload::SessionInfo(Box::new(info))),
                        binding: Some(binding),
                    }
                }
            }
        }

        SessionCommand::Info { selector } => {
            let location = if let Some(selector) = selector {
                find_named_session(state, &selector)
            } else {
                let current_key = state.client_sessions.get(&client_id);
                current_key.and_then(|key| {
                    state
                        .sessions
                        .get(key)
                        .and_then(|session| session.named.as_ref())
                        .map(|_| NamedSessionLocation::Ready(key.clone()))
                })
            };
            let Some(location) = location else {
                return session_command_response(ResponsePayload::err(
                    error_code::NOT_FOUND,
                    "client is not attached to a named session",
                ));
            };
            let info = match location {
                NamedSessionLocation::Ready(key) => ready_session_info(state, &key, client_id),
                NamedSessionLocation::NeedsRefresh(id) => {
                    unavailable_session_info(state, &id, client_id)
                }
            };
            match info {
                Some(info) => session_command_response(ResponsePayload::Ok(
                    OkPayload::SessionInfo(Box::new(info)),
                )),
                None => session_command_response(ResponsePayload::err(
                    error_code::NOT_FOUND,
                    "named session not found",
                )),
            }
        }
    }
}

fn session_command_response(payload: ResponsePayload) -> SessionCommandResult {
    SessionCommandResult {
        payload,
        binding: None,
    }
}

async fn set_named_session_archived(
    state: &mut SchedulerState,
    db: &storage::SharedConnection,
    client_id: u64,
    selector: &str,
    archive: bool,
) -> SessionCommandResult {
    let Some(location) = find_named_session(state, selector) else {
        return session_command_response(ResponsePayload::err(
            error_code::NOT_FOUND,
            format!("named session `{selector}` not found"),
        ));
    };
    let (id, archived_at_ms) = match &location {
        NamedSessionLocation::Ready(key) => {
            let Some(meta) = state
                .sessions
                .get(key)
                .and_then(|session| session.named.as_ref())
            else {
                return session_command_response(ResponsePayload::err(
                    error_code::INTERNAL,
                    "named session metadata disappeared",
                ));
            };
            (meta.id.clone(), meta.archived_at_ms)
        }
        NamedSessionLocation::NeedsRefresh(id) => {
            let Some(session) = state.unavailable_named_sessions.get(id) else {
                return session_command_response(ResponsePayload::err(
                    error_code::INTERNAL,
                    "named session metadata disappeared",
                ));
            };
            (session.meta.id.clone(), session.meta.archived_at_ms)
        }
    };

    if archive == archived_at_ms.is_some() {
        return session_info_for_location(state, &location, client_id).map_or_else(
            || {
                session_command_response(ResponsePayload::err(
                    error_code::INTERNAL,
                    "named session metadata disappeared",
                ))
            },
            |info| {
                session_command_response(ResponsePayload::Ok(OkPayload::SessionInfo(Box::new(
                    info,
                ))))
            },
        );
    }

    if archive && let Some(blocker) = named_session_archive_blocker(state, &id) {
        return session_command_response(ResponsePayload::err(error_code::INVALID_STATE, blocker));
    }

    let now = unix_time_ms();
    let next_archived_at_ms = archive.then_some(now);
    let stored_id = id.clone();
    if let Err(error) = storage::with_connection(db, move |conn| {
        storage::set_session_archived_at(conn, &stored_id, next_archived_at_ms, now)
    })
    .await
    {
        return session_command_response(ResponsePayload::err(
            error_code::INTERNAL,
            format!("persist named session lifecycle: {error}"),
        ));
    }

    match &location {
        NamedSessionLocation::Ready(key) => {
            let meta = state
                .sessions
                .get_mut(key)
                .and_then(|session| session.named.as_mut())
                .expect("persisted ready named session must still exist");
            meta.archived_at_ms = next_archived_at_ms;
            meta.updated_at_ms = now;
        }
        NamedSessionLocation::NeedsRefresh(id) => {
            let meta = &mut state
                .unavailable_named_sessions
                .get_mut(id)
                .expect("persisted unavailable named session must still exist")
                .meta;
            meta.archived_at_ms = next_archived_at_ms;
            meta.updated_at_ms = now;
        }
    }

    let info = session_info_for_location(state, &location, client_id)
        .expect("updated named session must remain queryable");
    session_command_response(ResponsePayload::Ok(OkPayload::SessionInfo(Box::new(info))))
}

fn session_info_for_location(
    state: &SchedulerState,
    location: &NamedSessionLocation,
    client_id: u64,
) -> Option<SessionInfo> {
    match location {
        NamedSessionLocation::Ready(key) => ready_session_info(state, key, client_id),
        NamedSessionLocation::NeedsRefresh(id) => unavailable_session_info(state, id, client_id),
    }
}

fn named_session_archive_blocker(state: &SchedulerState, session_id: &str) -> Option<String> {
    if let Some(connected_clients) = state.sessions.values().find_map(|session| {
        session
            .named
            .as_ref()
            .filter(|meta| meta.id == session_id)
            .map(|_| session.connected_clients)
    }) && connected_clients > 0
    {
        return Some(format!(
            "named session has {connected_clients} connected client(s); detach them before archiving"
        ));
    }
    if let Some(job) = state
        .jobs
        .values()
        .find(|job| job.session_id.as_deref() == Some(session_id) && !job.status.is_terminal())
    {
        return Some(format!(
            "named session has non-terminal job {}; wait for or cancel it before archiving",
            job.job_id
        ));
    }
    if state
        .pending_scripts
        .values()
        .any(|script| script.session_id.as_deref() == Some(session_id))
    {
        return Some(
            "named session has pending script work; wait for or cancel it before archiving".into(),
        );
    }
    if state
        .chains
        .values()
        .any(|chain| chain.session_id.as_deref() == Some(session_id))
    {
        return Some(
            "named session has pending chain work; wait for or cancel it before archiving".into(),
        );
    }
    if state
        .crons
        .values()
        .any(|cron| cron.session_id.as_deref() == Some(session_id))
    {
        return Some("named session owns a cron; remove it explicitly before archiving".into());
    }
    None
}

fn validate_session_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("session name must not be empty".into());
    }
    if name.trim() != name {
        return Err("session name must not have leading or trailing whitespace".into());
    }
    if name.chars().count() > 64 {
        return Err("session name must be at most 64 characters".into());
    }
    if name.chars().any(char::is_control) {
        return Err("session name must not contain control characters".into());
    }
    if name.starts_with("SS-") {
        return Err("session names beginning with `SS-` are reserved for session ids".into());
    }
    Ok(())
}

fn find_named_session(state: &SchedulerState, selector: &str) -> Option<NamedSessionLocation> {
    for (key, session) in &state.sessions {
        let Some(named) = &session.named else {
            continue;
        };
        if named.id == selector || named.name == selector {
            return Some(NamedSessionLocation::Ready(key.clone()));
        }
    }
    state
        .unavailable_named_sessions
        .iter()
        .find(|(id, session)| *id == selector || session.meta.name == selector)
        .map(|(id, _)| NamedSessionLocation::NeedsRefresh(id.clone()))
}

fn bind_client_to_ready_session(
    state: &mut SchedulerState,
    client_id: u64,
    key: &str,
) -> Option<SessionBinding> {
    let old_session_id = state.client_sessions.get(&client_id).cloned();
    let same_session = old_session_id.as_deref() == Some(key);
    let target = state.sessions.get_mut(key)?;
    if !same_session {
        target.connected_clients += 1;
    }
    target.disconnected_at = None;
    let named_id = target.named.as_ref()?.id.clone();
    let scope = target.scope;
    let incarnation = target.incarnation;
    state.client_sessions.insert(client_id, key.to_string());
    mark_replaced_session_disconnected(state, old_session_id, key);
    Some(SessionBinding {
        session_id: named_id.clone(),
        named_session_id: Some(named_id),
        scope,
        incarnation,
    })
}

async fn persist_named_session(
    db: &storage::SharedConnection,
    meta: &NamedSessionMeta,
    scope: ScopeHash,
    defaults: &LaunchDefaults,
) -> anyhow::Result<bool> {
    let stored = storage::StoredSession {
        id: meta.id.clone(),
        name: meta.name.clone(),
        scope_hash: Some(scope),
        pty_default: defaults.pty,
        wrapper_enabled: defaults.wrapper_enabled,
        created_at_ms: meta.created_at_ms,
        updated_at_ms: meta.updated_at_ms,
        archived_at_ms: meta.archived_at_ms,
    };
    storage::with_connection(db, move |conn| storage::upsert_session(conn, &stored))
        .await
        .map_err(|error| anyhow::anyhow!("persist session {}: {error}", meta.id))
}

async fn update_client_session_scope(
    state: &mut SchedulerState,
    client_id: u64,
    scope: ScopeHash,
    db: &storage::SharedConnection,
) -> anyhow::Result<()> {
    let key = state
        .client_sessions
        .get(&client_id)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("client session handshake required"))?;
    let session = state
        .sessions
        .get(&key)
        .ok_or_else(|| anyhow::anyhow!("client session is unavailable"))?;
    let Some(mut meta) = session.named.clone() else {
        state
            .sessions
            .get_mut(&key)
            .expect("anonymous session exists")
            .scope = scope;
        return Ok(());
    };
    let defaults = session.defaults.clone();
    meta.updated_at_ms = unix_time_ms();
    let durable = persist_named_session(db, &meta, scope, &defaults).await?;
    meta.scope_durable = durable;
    let session = state
        .sessions
        .get_mut(&key)
        .ok_or_else(|| anyhow::anyhow!("named session disappeared during scope update"))?;
    session.scope = scope;
    session.named = Some(meta);
    Ok(())
}

fn named_session_list(
    state: &SchedulerState,
    client_id: u64,
    filter: NamedSessionListFilter,
) -> Vec<SessionInfo> {
    let mut sessions = Vec::new();
    for key in state.sessions.keys() {
        if let Some(info) = ready_session_info(state, key, client_id) {
            sessions.push(info);
        }
    }
    for id in state.unavailable_named_sessions.keys() {
        if let Some(info) = unavailable_session_info(state, id, client_id) {
            sessions.push(info);
        }
    }
    sessions.retain(|session| match filter {
        NamedSessionListFilter::Active => session.archived_at_ms.is_none(),
        NamedSessionListFilter::Archived => session.archived_at_ms.is_some(),
        NamedSessionListFilter::All => true,
    });
    sessions.sort_by(|left, right| {
        left.created_at_ms
            .cmp(&right.created_at_ms)
            .then_with(|| left.name.cmp(&right.name))
            .then_with(|| left.id.cmp(&right.id))
    });
    sessions
}

fn ready_session_info(state: &SchedulerState, key: &str, client_id: u64) -> Option<SessionInfo> {
    let session = state.sessions.get(key)?;
    let named = session.named.as_ref()?;
    Some(SessionInfo {
        id: named.id.clone(),
        name: named.name.clone(),
        scope_state: if named.scope_durable {
            SessionScopeState::ReadyDurable
        } else {
            SessionScopeState::ReadyVolatile
        },
        scope_hash: Some(session.scope.to_string()),
        connected_clients: session.connected_clients,
        restart_safe: named.scope_durable,
        current: state
            .client_sessions
            .get(&client_id)
            .is_some_and(|id| id == key),
        created_at_ms: named.created_at_ms,
        updated_at_ms: named.updated_at_ms,
        archived_at_ms: named.archived_at_ms,
    })
}

fn unavailable_session_info(
    state: &SchedulerState,
    id: &str,
    _client_id: u64,
) -> Option<SessionInfo> {
    let session = state.unavailable_named_sessions.get(id)?;
    Some(SessionInfo {
        id: session.meta.id.clone(),
        name: session.meta.name.clone(),
        scope_state: SessionScopeState::NeedsRefresh,
        scope_hash: None,
        connected_clients: 0,
        restart_safe: false,
        current: false,
        created_at_ms: session.meta.created_at_ms,
        updated_at_ms: session.meta.updated_at_ms,
        archived_at_ms: session.meta.archived_at_ms,
    })
}

fn unix_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn mark_replaced_session_disconnected(
    state: &mut SchedulerState,
    old_session_id: Option<String>,
    new_session_id: &str,
) {
    if let Some(old_session_id) = old_session_id
        && old_session_id != new_session_id
        && let Some(old_session) = state.sessions.get_mut(&old_session_id)
    {
        old_session.connected_clients = old_session.connected_clients.saturating_sub(1);
        if old_session.connected_clients == 0 {
            old_session.disconnected_at = Some(Instant::now());
        }
    }
}

fn disconnect_session(client_id: u64, state: &mut SchedulerState) {
    let Some(session_id) = state.client_sessions.remove(&client_id) else {
        return;
    };
    let Some(session) = state.sessions.get_mut(&session_id) else {
        return;
    };
    session.connected_clients = session.connected_clients.saturating_sub(1);
    if session.connected_clients == 0 {
        session.disconnected_at = Some(Instant::now());
    }
}

fn sweep_disconnected_sessions(state: &mut SchedulerState) -> usize {
    let now = Instant::now();
    let before = state.sessions.len();
    state.sessions.retain(|_, session| {
        session.named.is_some()
            || session.connected_clients > 0
            || session
                .disconnected_at
                .is_none_or(|disconnected_at| now.duration_since(disconnected_at) < SESSION_GC_TTL)
    });
    before.saturating_sub(state.sessions.len())
}

async fn insert_scope(sys: &ActorSystem, scope: Scope) -> anyhow::Result<ScopeHash> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    sys.scope_store
        .send(ScopeStoreMsg::Insert { scope, reply: tx })
        .await
        .map_err(|_| anyhow::anyhow!("scope_store unreachable"))?;
    rx.await
        .map_err(|_| anyhow::anyhow!("scope_store reply dropped"))?
}

async fn get_session_snapshot(
    sys: &ActorSystem,
    state: &SchedulerState,
    client_id: u64,
) -> Result<cue_core::scope::EnvSnapshot, ResponsePayload> {
    let Some(scope) = state.client_scope(client_id) else {
        return Err(ResponsePayload::err(
            error_code::INVALID_REQUEST,
            "client session handshake required",
        ));
    };
    get_scope_snapshot_by_hash(sys, scope)
        .await
        .map_err(|error| ResponsePayload::err(error_code::INTERNAL, error))
}

// ── Chain helpers ────────────────────────────────────────────────────────────

fn parse_chain_text(text: &str) -> Result<ChainNode, String> {
    match parse_command(&format!(":run {text}"), cue_core::Mode::Job).map_err(|err| err.message)? {
        ResolvedCommand::Run { chain, .. } => Ok(chain),
        other => Err(format!("unexpected restore command: {other:?}")),
    }
}

fn chain_final_scope(chain: &ChainState, state: &SchedulerState) -> Option<ScopeHash> {
    (0..chain.node.leaf_count()).rev().find_map(|idx| {
        chain
            .leaf_jobs
            .get(&idx)
            .and_then(|job_id| state.jobs.get(job_id))
            .and_then(|entry| entry.end_scope.or(entry.start_scope))
    })
}

async fn publish_job_created(
    sys: &ActorSystem,
    state: &SchedulerState,
    job_id: JobId,
    pipeline_text: &str,
    start_scope: ScopeHash,
    open_hint: JobOpenHint,
) {
    let (chain_id, chain_index, chain_total, session_id) = state
        .jobs
        .get(&job_id)
        .map(|entry| {
            (
                entry.chain_id.map(|id| id.to_string()),
                entry.chain_index,
                entry.chain_total,
                entry.session_id.clone(),
            )
        })
        .unwrap_or((None, None, None, None));
    publish_session_event(
        sys,
        EventChannel::Jobs,
        EventPayload::JobCreated {
            job_id: job_id.to_string(),
            pipeline: pipeline_text.to_string(),
            start_scope: Some(start_scope.to_string()),
            open_hint,
            chain_id,
            chain_index,
            chain_total,
        },
        session_id,
    )
    .await;
}

async fn publish_job_state_changed(
    sys: &ActorSystem,
    state: &SchedulerState,
    job_id: JobId,
    old_state: JobStatus,
    new_state: JobStatus,
    end_scope: Option<ScopeHash>,
) {
    let (chain_id, chain_index, session_id) = state
        .jobs
        .get(&job_id)
        .map(|entry| {
            (
                entry.chain_id.map(|id| id.to_string()),
                entry.chain_index,
                entry.session_id.clone(),
            )
        })
        .unwrap_or((None, None, None));
    publish_session_event(
        sys,
        EventChannel::Jobs,
        EventPayload::JobStateChanged {
            job_id: job_id.to_string(),
            old_state,
            new_state,
            end_scope: end_scope.map(|hash| hash.to_string()),
            chain_id,
            chain_index,
        },
        session_id,
    )
    .await;
}

fn build_chain_info(state: &SchedulerState, chain_id: ChainId) -> Option<ChainInfo> {
    let chain = state.chains.get(&chain_id)?;
    let leaves = flatten_leaves(&chain.node);
    Some(ChainInfo {
        id: chain_id.to_string(),
        pipeline: chain.pipeline_text.clone(),
        total_jobs: leaves.len(),
        jobs: leaves
            .into_iter()
            .map(|leaf| {
                let job_id = chain.leaf_jobs.get(&leaf.index).copied();
                let job_entry = job_id.and_then(|jid| state.jobs.get(&jid));
                ChainJobInfo {
                    index: leaf.index,
                    pipeline: leaf.pipeline_text,
                    status: chain
                        .leaf_status
                        .get(&leaf.index)
                        .cloned()
                        .map(leaf_status_to_job_status)
                        .unwrap_or(JobStatus::Pending),
                    job_id: job_id.map(|id| id.to_string()),
                    start_scope: job_entry
                        .and_then(|entry| entry.start_scope)
                        .map(|hash| hash.to_string()),
                    end_scope: job_entry
                        .and_then(|entry| entry.end_scope)
                        .map(|hash| hash.to_string()),
                    open_hint: job_entry.map(|entry| entry.open_hint),
                }
            })
            .collect(),
    })
}

fn leaf_status_to_job_status(status: LeafStatus) -> JobStatus {
    match status {
        LeafStatus::Pending => JobStatus::Pending,
        LeafStatus::Running => JobStatus::Running,
        LeafStatus::Done(_) => JobStatus::Done,
        LeafStatus::Failed(_) => JobStatus::Failed,
        LeafStatus::Cancelled => JobStatus::Cancelled(CancelReason::ChainAborted),
    }
}

async fn publish_chain_progress(sys: &ActorSystem, state: &mut SchedulerState, chain_id: ChainId) {
    let Some(chain) = build_chain_info(state, chain_id) else {
        return;
    };
    let session_id = state
        .chains
        .get(&chain_id)
        .and_then(|entry| entry.session_id.clone());
    synchronize_pending_script_chain(state, chain_id, &chain);
    publish_session_event(
        sys,
        EventChannel::Jobs,
        EventPayload::ChainProgress { chain },
        session_id,
    )
    .await;
}

fn synchronize_pending_script_chain(
    state: &mut SchedulerState,
    chain_id: ChainId,
    chain: &ChainInfo,
) {
    let Some(script_id) = state.pending_script_chains.get(&chain_id).copied() else {
        return;
    };
    let Some(pending) = state.pending_scripts.get_mut(&script_id) else {
        return;
    };
    let Some(item) = pending.created_items.iter_mut().find(|item| {
        matches!(
            &item.result,
            ScriptItemResult::Chain {
                chain_id: item_chain_id,
                ..
            } if item_chain_id == &chain_id.to_string()
        )
    }) else {
        return;
    };
    let ScriptItemResult::Chain {
        job_ids,
        chain: item_chain,
        ..
    } = &mut item.result
    else {
        return;
    };
    *job_ids = chain
        .jobs
        .iter()
        .filter_map(|job| job.job_id.clone())
        .collect();
    *item_chain = chain.clone();
}

#[derive(Debug, Clone)]
enum ScopeTransform {
    Cd { path: String },
    EnvSet { assignments: Vec<String> },
}

fn scope_transform_from_command(words: &[String]) -> Result<Option<ScopeTransform>, String> {
    let Some(command) = words.first().map(String::as_str) else {
        return Ok(None);
    };

    match command {
        "cd" => {
            if words.len() != 2 {
                return Err(
                    "`cd` inside `:run` only accepts a single path (e.g. `cd /some/dir`).\n\
                     To combine `cd` with other commands, use a chain: `cd /some/dir -> cargo build`.\n\
                     Or pass cwd via mode param: `:run(cwd=/some/dir) cargo build`."
                        .into(),
                );
            }
            Ok(Some(ScopeTransform::Cd {
                path: words[1].clone(),
            }))
        }
        "env" if words.get(1).map(String::as_str) == Some("set") => {
            if words.len() < 3 {
                return Err(
                    "`env set` inside `:run` needs at least one KEY=VALUE pair.\n\
                     Example: `env set RUST_BACKTRACE=1 -> cargo test`."
                        .into(),
                );
            }
            Ok(Some(ScopeTransform::EnvSet {
                assignments: words[2..].to_vec(),
            }))
        }
        _ => Ok(None),
    }
}

fn scope_transform_from_pipeline(
    pipeline: &cue_core::pipeline::Pipeline,
) -> Result<Option<ScopeTransform>, String> {
    let mut found = None;
    for segment in &pipeline.segments {
        if let Some(transform) = scope_transform_from_command(&segment.command)? {
            if pipeline.segments.len() != 1 {
                return Err(
                    "scope-transform steps are not supported inside pipelines yet".to_string(),
                );
            }
            found = Some(transform);
        }
    }
    Ok(found)
}

fn scope_transform_from_job_plan(
    plan: &cue_core::pipeline::JobPlan,
) -> Result<Option<ScopeTransform>, String> {
    match plan {
        cue_core::pipeline::JobPlan::Pipeline(pipeline) => scope_transform_from_pipeline(pipeline),
        cue_core::pipeline::JobPlan::And { left, right }
        | cue_core::pipeline::JobPlan::Or { left, right } => {
            let left_transform = scope_transform_from_job_plan(left)?;
            let right_transform = scope_transform_from_job_plan(right)?;
            if left_transform.is_some() || right_transform.is_some() {
                return Err(
                    "scope-transform steps are not supported inside job-local &&/|| expressions yet"
                        .into(),
                );
            }
            Ok(None)
        }
    }
}

fn subtree_contains_scope_transform(node: &ChainNode) -> Result<bool, String> {
    match node {
        ChainNode::Leaf(plan) => Ok(scope_transform_from_job_plan(plan)?.is_some()),
        ChainNode::Serial { left, right, .. } | ChainNode::Parallel { left, right, .. } => {
            Ok(subtree_contains_scope_transform(left)? || subtree_contains_scope_transform(right)?)
        }
    }
}

fn validate_scope_transform_support(node: &ChainNode) -> Result<(), String> {
    match node {
        ChainNode::Leaf(plan) => {
            let _ = scope_transform_from_job_plan(plan)?;
            Ok(())
        }
        ChainNode::Serial { left, right, .. } => {
            validate_scope_transform_support(left)?;
            validate_scope_transform_support(right)
        }
        ChainNode::Parallel { left, right, .. } => {
            if subtree_contains_scope_transform(left)? || subtree_contains_scope_transform(right)? {
                return Err(
                    "scope-transform jobs are not supported inside parallel chains yet".into(),
                );
            }
            Ok(())
        }
    }
}

async fn get_scope_snapshot_by_hash(
    sys: &ActorSystem,
    hash: ScopeHash,
) -> Result<cue_core::scope::EnvSnapshot, String> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    if sys
        .scope_store
        .send(ScopeStoreMsg::GetScope { hash, reply: tx })
        .await
        .is_err()
    {
        return Err("scope_store unreachable".into());
    }
    match rx.await {
        Ok(Ok(Some(scope))) => scope
            .snapshot
            .ok_or_else(|| format!("scope {hash} has no snapshot")),
        Ok(Ok(None)) => Err(format!("scope {hash} not found")),
        Ok(Err(error)) => Err(format!("scope {hash} lookup failed: {error}")),
        Err(_) => Err("scope_store reply dropped".into()),
    }
}

async fn derive_scope(
    sys: &ActorSystem,
    base: ScopeHash,
    delta: cue_core::scope::EnvDelta,
) -> Result<ScopeHash, String> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    if sys
        .scope_store
        .send(ScopeStoreMsg::Derive {
            base,
            delta,
            reply: tx,
        })
        .await
        .is_err()
    {
        return Err("scope_store unreachable".into());
    }
    match rx.await {
        Ok(Ok(hash)) => Ok(hash),
        Ok(Err(error)) => Err(error.to_string()),
        Err(_) => Err("scope_store reply dropped".into()),
    }
}

fn resolve_cd_target(
    snapshot: &cue_core::scope::EnvSnapshot,
    path: &str,
) -> Result<std::path::PathBuf, String> {
    let requested = std::path::PathBuf::from(path);
    let target = if requested.is_absolute() {
        requested
    } else {
        snapshot.cwd.join(requested)
    };
    let resolved = std::fs::canonicalize(&target)
        .map_err(|error| format!("cannot cd to `{}`: {error}", target.display()))?;
    if !resolved.is_dir() {
        return Err(format!(
            "cannot cd to `{}`: not a directory",
            resolved.display()
        ));
    }
    Ok(resolved)
}

fn launch_options_from_params(params: &ModeParams) -> Result<LaunchOptions, String> {
    Ok(LaunchOptions {
        pty: match params.get("pty") {
            Some(cue_core::command::ParamValue::Bool(value)) => Some(*value),
            _ => None,
        },
        needs: params.needs(),
        sandbox: crate::sandbox::SandboxConfig::from_params(params)?.map(Into::into),
    })
}

fn mode_params_cwd_delta(params: &ModeParams) -> Option<EnvDelta> {
    params.cwd().map(|cwd| EnvDelta {
        set: std::collections::BTreeMap::new(),
        unset: Vec::new(),
        cwd: Some(cwd),
    })
}

async fn derive_mode_params_scope(
    sys: &ActorSystem,
    base_scope: ScopeHash,
    params: &ModeParams,
) -> Result<ScopeHash, String> {
    match mode_params_cwd_delta(params) {
        Some(delta) => derive_scope(sys, base_scope, delta).await,
        None => Ok(base_scope),
    }
}

async fn apply_scope_transform(
    sys: &ActorSystem,
    start_scope: ScopeHash,
    command_line: &[String],
) -> Result<ScopeHash, String> {
    let snapshot = get_scope_snapshot_by_hash(sys, start_scope).await?;
    let expanded = expand_command_line(command_line, Some(&snapshot));
    let Some(transform) = scope_transform_from_command(&expanded)? else {
        return Err("not a scope transform".into());
    };

    let delta = match transform {
        ScopeTransform::Cd { path } => cue_core::scope::EnvDelta {
            set: std::collections::BTreeMap::new(),
            unset: vec![],
            cwd: Some(resolve_cd_target(&snapshot, &path)?),
        },
        ScopeTransform::EnvSet { assignments } => {
            let mut set = std::collections::BTreeMap::new();
            for assignment in assignments {
                let Some((key, value)) = assignment.split_once('=') else {
                    return Err(format!(
                        "`env set` inside `:run` expects KEY=VALUE, got `{assignment}`"
                    ));
                };
                if key.is_empty() {
                    return Err("`env set` inside `:run` requires a non-empty variable name".into());
                }
                set.insert(key.to_string(), value.to_string());
            }
            cue_core::scope::EnvDelta {
                set,
                unset: vec![],
                cwd: None,
            }
        }
    };

    derive_scope(sys, start_scope, delta).await
}

fn classify_job_plan_open_hint(plan: &cue_core::pipeline::JobPlan) -> JobOpenHint {
    match plan {
        cue_core::pipeline::JobPlan::Pipeline(pipeline)
            if pipeline.segments.len() == 1
                && command_prefers_foreground(&pipeline.segments[0].command) =>
        {
            JobOpenHint::Fg
        }
        _ => JobOpenHint::Stream,
    }
}

async fn spawn_process_job(
    sys: &ActorSystem,
    job_id: JobId,
    plan: cue_core::pipeline::JobPlan,
    scope_hash: ScopeHash,
    options: ProcessJobOptions,
) -> Result<(), String> {
    sys.process_mgr
        .send(ProcessMgrMsg::SpawnJob {
            job_id,
            plan,
            scope_hash,
            options,
        })
        .await
        .map_err(|_| "process_mgr unreachable".to_string())
}

enum ResourceAdmission {
    Granted(ScopeHash),
    Pending(String),
}

fn pending_reason_from_reject(reject: RejectGroup) -> String {
    if reject.provider_id.as_str() == "core" {
        reject.reject.reason
    } else {
        reject.to_string()
    }
}

async fn admit_resource_scope(
    sys: &ActorSystem,
    job_id: JobId,
    base_scope: ScopeHash,
    needs: &Need,
) -> Result<ResourceAdmission, String> {
    if needs.is_empty() {
        return Ok(ResourceAdmission::Granted(base_scope));
    }

    let grants = match sys.resources.try_reserve(job_id, needs) {
        Ok(grants) => grants,
        Err(reject) => {
            return Ok(ResourceAdmission::Pending(pending_reason_from_reject(
                reject,
            )));
        }
    };

    let mut set = std::collections::BTreeMap::new();
    for grant in grants {
        set.extend(grant.env);
    }
    if set.is_empty() {
        return Ok(ResourceAdmission::Granted(base_scope));
    }

    match derive_scope(
        sys,
        base_scope,
        EnvDelta {
            set,
            unset: Vec::new(),
            cwd: None,
        },
    )
    .await
    {
        Ok(scope) => Ok(ResourceAdmission::Granted(scope)),
        Err(error) => {
            let released = sys.resources.release(job_id);
            warn!(%job_id, released, "scheduler: released resource grants after scope derive failure");
            Err(format!("derive resource scope: {error}"))
        }
    }
}

fn record_resource_pending(
    state: &mut SchedulerState,
    job_id: JobId,
    reason: String,
    pending: PendingResourceAdmission,
) {
    if let Some(entry) = state.jobs.get_mut(&job_id) {
        entry.status = JobStatus::Pending;
        entry.pending_reason = Some(reason);
    }
    state.pending_resource.insert(job_id, pending);
    if !state.pending_resource_jobs.contains(&job_id) {
        state.pending_resource_jobs.push_back(job_id);
    }
}

fn forget_resource_pending(state: &mut SchedulerState, job_id: JobId) {
    state.pending_resource.remove(&job_id);
    state
        .pending_resource_jobs
        .retain(|queued| *queued != job_id);
}

async fn retry_pending_resource_admissions(state: &mut SchedulerState, io: SchedulerIo<'_>) {
    let retry_len = state.pending_resource_jobs.len();
    for _ in 0..retry_len {
        let Some(job_id) = state.pending_resource_jobs.pop_front() else {
            break;
        };
        let Some(pending) = state.pending_resource.get(&job_id).cloned() else {
            continue;
        };
        if !state
            .jobs
            .get(&job_id)
            .is_some_and(|entry| entry.status == JobStatus::Pending)
        {
            state.pending_resource.remove(&job_id);
            continue;
        }

        match admit_resource_scope(io.sys, job_id, pending.base_scope, &pending.needs).await {
            Ok(ResourceAdmission::Pending(reason)) => {
                if let Some(entry) = state.jobs.get_mut(&job_id) {
                    entry.pending_reason = Some(reason);
                }
                state.pending_resource_jobs.push_back(job_id);
            }
            Ok(ResourceAdmission::Granted(spawn_scope)) => {
                state.pending_resource.remove(&job_id);
                let (old_state, chain_id) = match state.jobs.get_mut(&job_id) {
                    Some(entry) => {
                        let old_state = entry.status.clone();
                        entry.status = JobStatus::Running;
                        entry.pending_reason = None;
                        entry.start_scope = Some(spawn_scope);
                        (old_state, entry.chain_id)
                    }
                    None => continue,
                };
                if let Some((cid, idx)) = state.job_to_chain.get(&job_id).copied()
                    && let Some(chain) = state.chains.get_mut(&cid)
                {
                    chain.leaf_status.insert(idx, LeafStatus::Running);
                }
                publish_job_state_changed(
                    io.sys,
                    state,
                    job_id,
                    old_state,
                    JobStatus::Running,
                    None,
                )
                .await;
                if let Some(chain_id) = chain_id {
                    publish_chain_progress(io.sys, state, chain_id).await;
                }
                if let Err(error) =
                    spawn_process_job(io.sys, job_id, pending.plan, spawn_scope, pending.options)
                        .await
                {
                    warn!(%job_id, "scheduler: failed to spawn resource-admitted job: {error}");
                    let terminal = set_job_terminal_state(
                        job_id,
                        TerminalStateUpdate {
                            status: JobStatus::Failed,
                            exit_code: EXIT_CODE_UNAVAILABLE,
                            end_scope: Some(spawn_scope),
                            advance_chain: true,
                        },
                        state,
                        io.db,
                        io.sys,
                    )
                    .await;
                    if let Some(error) = terminal.persist_error {
                        warn!(%job_id, "scheduler: failed to persist spawn failure after resource admission: {error}");
                    }
                }
            }
            Err(error) => {
                state.pending_resource.remove(&job_id);
                if let Some(entry) = state.jobs.get_mut(&job_id) {
                    entry.pending_reason = Some(error.clone());
                }
                let terminal = set_job_terminal_state(
                    job_id,
                    TerminalStateUpdate {
                        status: JobStatus::Failed,
                        exit_code: EXIT_CODE_UNAVAILABLE,
                        end_scope: Some(pending.base_scope),
                        advance_chain: true,
                    },
                    state,
                    io.db,
                    io.sys,
                )
                .await;
                if let Some(error) = terminal.persist_error {
                    warn!(%job_id, "scheduler: failed to persist resource admission internal failure: {error}");
                }
            }
        }
    }
}

async fn kill_process_job(sys: &ActorSystem, job_id: JobId) -> Result<(), String> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    sys.process_mgr
        .send(ProcessMgrMsg::KillJob { job_id, reply: tx })
        .await
        .map_err(|_| "process_mgr unreachable".to_string())?;
    rx.await
        .map_err(|_| "process_mgr reply dropped".to_string())?
}

async fn cancel_process_job(sys: &ActorSystem, job_id: JobId) -> Result<(), String> {
    match kill_process_job(sys, job_id).await {
        Ok(()) => Ok(()),
        // The process reader may have already reaped the child and queued its
        // scheduler notification. For idempotent cancellation, absence at the
        // process owner is equivalent to already stopped.
        Err(error) if error == format!("job {job_id} not found") => Ok(()),
        Err(error) => Err(error),
    }
}

struct TerminalStateUpdate {
    status: JobStatus,
    exit_code: i32,
    end_scope: Option<ScopeHash>,
    advance_chain: bool,
}

#[derive(Clone)]
struct ProcessJobContext {
    cwd_override: Option<std::path::PathBuf>,
    launch: LaunchOptions,
    wrapper_enabled: bool,
    pty_default: bool,
    direct_output_client: Option<u64>,
}

impl ProcessJobContext {
    fn process_job_options(&self, session_id: Option<String>) -> ProcessJobOptions {
        ProcessJobOptions {
            cwd_override: self.cwd_override.clone(),
            sandbox: self
                .launch
                .sandbox
                .as_ref()
                .map(crate::sandbox::SandboxConfig::from),
            wrapper_enabled: self.wrapper_enabled,
            pty_enabled: self.launch.pty.unwrap_or(self.pty_default),
            direct_output_client: self.direct_output_client,
            session_id,
        }
    }

    fn needs(&self) -> &Need {
        &self.launch.needs
    }
}

struct ChainExecutionOptions {
    process: ProcessJobContext,
    scope_enabled: bool,
}

impl ChainExecutionOptions {
    fn from_params(
        params: &ModeParams,
        state: &SchedulerState,
        client_id: u64,
        config: &Config,
        direct_output_client: Option<u64>,
    ) -> Result<Self, String> {
        Ok(Self {
            process: ProcessJobContext {
                cwd_override: None,
                launch: launch_options_from_params(params)?,
                wrapper_enabled: params
                    .wrapper_enabled()
                    .unwrap_or_else(|| state.wrapper_enabled(client_id, config)),
                pty_default: state.pty_default(client_id),
                direct_output_client,
            },
            scope_enabled: params.scope().unwrap_or(false),
        })
    }

    fn from_cron_entry(entry: &CronEntry) -> Self {
        Self {
            process: ProcessJobContext {
                cwd_override: entry.cwd_override.clone(),
                launch: LaunchOptions::default(),
                wrapper_enabled: entry.wrapper_enabled,
                pty_default: true,
                direct_output_client: None,
            },
            scope_enabled: entry.scope_enabled,
        }
    }

    fn retry_default(config: &Config) -> Self {
        Self {
            process: ProcessJobContext {
                cwd_override: None,
                launch: LaunchOptions::default(),
                wrapper_enabled: config.wrapper.enabled,
                pty_default: true,
                direct_output_client: None,
            },
            scope_enabled: false,
        }
    }

    fn process_job_options(&self, session_id: Option<String>) -> ProcessJobOptions {
        self.process.process_job_options(session_id)
    }
}

struct SpawnChainRequest {
    chain: ChainNode,
    scope_hash: ScopeHash,
    options: ChainExecutionOptions,
    warnings: Vec<String>,
    retain_completed_chain: bool,
    session_id: Option<String>,
}

struct ChainAdvance {
    chain_id: ChainId,
    newly_ready: Vec<(usize, ScopeHash)>,
    to_cancel: Vec<usize>,
}

struct ChainAdvanceRequest {
    chain_id: ChainId,
    newly_ready: Vec<(usize, ScopeHash)>,
    to_cancel: Vec<usize>,
    capture_first: usize,
    retain_completed_chain: bool,
}

impl ChainExecutionOptions {
    fn from_chain(chain: &ChainState) -> Self {
        Self {
            process: chain.process.clone(),
            scope_enabled: chain.scope_enabled,
        }
    }
}

#[derive(Default)]
struct TerminalStateOutcome {
    chain_advance: Option<ChainAdvance>,
    persist_error: Option<String>,
}

#[derive(Default)]
struct ChainAdvanceOutcome {
    captured_job_ids: Vec<JobId>,
    completed_chain: Option<ChainInfo>,
    spawn_error: Option<String>,
    persist_error: Option<String>,
}

impl ChainAdvanceOutcome {
    fn record_terminal_state(&mut self, terminal: &TerminalStateOutcome) {
        if let Some(error) = terminal.persist_error.as_ref() {
            self.record_persist_error(error.clone());
        }
    }

    fn record_persist_error(&mut self, error: String) {
        self.persist_error.get_or_insert(error);
    }
}

async fn set_job_terminal_state(
    job_id: JobId,
    update: TerminalStateUpdate,
    state: &mut SchedulerState,
    db: &Arc<Mutex<Connection>>,
    sys: &ActorSystem,
) -> TerminalStateOutcome {
    let TerminalStateUpdate {
        status: new_status,
        exit_code,
        end_scope,
        advance_chain: advance_chain_state,
    } = update;
    let mut stored_job = None;
    let transition = {
        let Some(entry) = state.jobs.get_mut(&job_id) else {
            return TerminalStateOutcome::default();
        };
        if entry.status.is_terminal() {
            let existing_status = entry.status.clone();
            if entry.end_scope.is_none()
                && let Some(scope) = end_scope.or(entry.start_scope)
            {
                entry.end_scope = Some(scope);
                stored_job = Some(stored_job_from_entry(entry));
            }
            debug!(
                %job_id,
                ?existing_status,
                ?new_status,
                reported_exit_code = exit_code,
                "scheduler: ignoring terminal job state update"
            );
            None
        } else {
            let old_state = entry.status.clone();
            entry.status = new_status.clone();
            entry.exit_code = Some(exit_code);
            entry.end_scope = end_scope.or(entry.start_scope);
            let effective_end_scope = entry.end_scope.or(entry.start_scope);
            stored_job = Some(stored_job_from_entry(entry));
            Some((old_state, effective_end_scope))
        }
    };

    let persist_error = match stored_job {
        Some(stored) => match persist_job_entry(db, stored).await {
            Ok(()) => None,
            Err(error) => {
                let message = error.to_string();
                warn!(%job_id, "scheduler: failed to persist terminal job state: {message}");
                Some(message)
            }
        },
        None => None,
    };

    let Some((old_state, effective_end_scope)) = transition else {
        return TerminalStateOutcome {
            chain_advance: None,
            persist_error,
        };
    };

    if new_status.is_terminal() {
        forget_resource_pending(state, job_id);
        let released = sys.resources.release(job_id);
        if released > 0 {
            debug!(%job_id, released, "scheduler: released resource reservations for terminal job");
        }
    }

    publish_job_state_changed(
        sys,
        state,
        job_id,
        old_state,
        new_status.clone(),
        effective_end_scope,
    )
    .await;

    notify_job_waiters(state, sys, job_id).await;

    let Some((chain_id, leaf_idx)) = state.job_to_chain.get(&job_id).copied() else {
        return TerminalStateOutcome {
            chain_advance: None,
            persist_error,
        };
    };

    if let Some(chain) = state.chains.get_mut(&chain_id) {
        let leaf_status = match &new_status {
            JobStatus::Done => LeafStatus::Done(exit_code),
            JobStatus::Failed | JobStatus::Killed => LeafStatus::Failed(exit_code),
            JobStatus::Cancelled(_) => LeafStatus::Cancelled,
            JobStatus::Pending => LeafStatus::Pending,
            JobStatus::Running => LeafStatus::Running,
        };
        chain.leaf_status.insert(leaf_idx, leaf_status);
    }

    if !advance_chain_state {
        return TerminalStateOutcome {
            chain_advance: None,
            persist_error,
        };
    }

    let Some(next_scope) =
        effective_end_scope.or_else(|| state.chains.get(&chain_id).map(|chain| chain.scope_hash))
    else {
        return TerminalStateOutcome {
            chain_advance: None,
            persist_error,
        };
    };
    let Some(chain) = state.chains.get(&chain_id) else {
        return TerminalStateOutcome {
            chain_advance: None,
            persist_error,
        };
    };
    let transition = advance_chain(&chain.node, leaf_idx, &chain.leaf_status);
    TerminalStateOutcome {
        chain_advance: Some(ChainAdvance {
            chain_id,
            newly_ready: transition
                .newly_ready
                .into_iter()
                .map(|idx| (idx, next_scope))
                .collect(),
            to_cancel: transition.to_cancel,
        }),
        persist_error,
    }
}

fn job_info_from_entry(entry: &JobEntry) -> JobInfo {
    JobInfo {
        id: entry.job_id.to_string(),
        session_id: entry.session_id.clone(),
        status: entry.status.clone(),
        pipeline: entry.pipeline_text.clone(),
        exit_code: entry.exit_code,
        start_scope: entry.start_scope.map(|hash| hash.to_string()),
        end_scope: entry.end_scope.map(|hash| hash.to_string()),
        open_hint: entry.open_hint,
        chain_id: entry.chain_id.map(|id| id.to_string()),
        chain_index: entry.chain_index,
        chain_total: entry.chain_total,
        pending_reason: entry.pending_reason.clone(),
    }
}

async fn notify_job_waiters(state: &mut SchedulerState, sys: &ActorSystem, job_id: JobId) {
    let Some(waiters) = state.job_waiters.remove(&job_id) else {
        return;
    };
    let Some(entry) = state.jobs.get(&job_id) else {
        return;
    };
    let payload = ResponsePayload::Ok(OkPayload::JobInfo(job_info_from_entry(entry)));
    for waiter in waiters {
        send_gateway_response(sys, waiter.client_id, waiter.request_id, payload.clone()).await;
    }
}

async fn cancel_chain_leaves(
    chain_id: ChainId,
    to_cancel: &[usize],
    state: &mut SchedulerState,
    db: &Arc<Mutex<Connection>>,
    sys: &ActorSystem,
) -> Option<String> {
    let mut persist_error = None;
    for &idx in to_cancel {
        let jid = state
            .chains
            .get(&chain_id)
            .and_then(|chain| chain.leaf_jobs.get(&idx).copied());

        if let Some(jid) = jid {
            let is_running = state
                .jobs
                .get(&jid)
                .is_some_and(|entry| entry.status == JobStatus::Running);
            if is_running && let Err(error) = cancel_process_job(sys, jid).await {
                warn!(%chain_id, %jid, "scheduler: failed to kill chain leaf: {error}");
                continue;
            }
            let terminal = set_job_terminal_state(
                jid,
                TerminalStateUpdate {
                    status: JobStatus::Cancelled(CancelReason::ChainAborted),
                    exit_code: EXIT_CODE_UNAVAILABLE,
                    end_scope: None,
                    advance_chain: false,
                },
                state,
                db,
                sys,
            )
            .await;
            if persist_error.is_none() {
                persist_error = terminal.persist_error;
            }
        } else if let Some(chain) = state.chains.get_mut(&chain_id) {
            chain.leaf_status.insert(idx, LeafStatus::Cancelled);
        }
    }
    persist_error
}

// ── Cron trigger logic ──────────────────────────────────────────────────────

/// Fire all crons whose `next_trigger` has passed.
async fn fire_due_crons(
    state: &mut SchedulerState,
    db: &Arc<Mutex<Connection>>,
    config: &Config,
    sys: &ActorSystem,
) {
    let now = Instant::now();
    // Collect cron IDs to fire (avoid borrow conflict).
    let due: Vec<CronId> = state
        .crons
        .values()
        .filter(|c| c.status.is_runnable() && c.next_trigger <= now)
        .map(|c| c.cron_id)
        .collect();

    for cron_id in due {
        let Some(entry) = state.crons.get(&cron_id) else {
            continue;
        };
        let chain = entry.chain.clone();
        let scope_hash = entry.scope_hash;
        let schedule = entry.schedule.clone();
        let is_oneshot = schedule.is_oneshot();
        let options = ChainExecutionOptions::from_cron_entry(entry);
        let session_id = entry.session_id.clone();

        info!(%cron_id, "scheduler: cron triggered");
        let warnings = match check_chain_guardrails(&chain, config) {
            Ok(warnings) => warnings,
            Err(reason) => {
                mark_cron_failed(state, db, cron_id, &reason).await;
                continue;
            }
        };

        // Spawn the chain just like `:run`.
        let response = spawn_chain(
            SpawnChainRequest {
                chain,
                scope_hash,
                options,
                warnings,
                retain_completed_chain: false,
                session_id: session_id.clone(),
            },
            state,
            SchedulerIo::new(db, sys),
        )
        .await;
        let first_job_id = match &response {
            ResponsePayload::Ok(OkPayload::JobCreated { job_id, .. }) => Some(job_id.clone()),
            ResponsePayload::Ok(OkPayload::ChainCreated { chain, .. }) => {
                chain.jobs.iter().find_map(|job| job.job_id.clone())
            }
            ResponsePayload::Err { code, message } => {
                let reason = format!("{code}: {message}");
                mark_cron_failed(state, db, cron_id, &reason).await;
                continue;
            }
            _ => None,
        };
        if let Some(job_id) = first_job_id {
            publish_session_event(
                sys,
                EventChannel::Crons,
                EventPayload::CronTriggered {
                    cron_id: cron_id.to_string(),
                    job_id,
                },
                session_id,
            )
            .await;
        }

        if is_oneshot {
            if let Some(entry) = state.crons.get_mut(&cron_id) {
                entry.status = CronStatus::Completed;
                let stored = stored_cron_from_entry(entry);
                if let Err(error) = persist_cron_record(db, stored).await {
                    warn!(%cron_id, "scheduler: failed to persist completed cron: {error}");
                }
            }
            debug!(%cron_id, "scheduler: one-shot cron completed");
        } else if let Some(next_trigger) = next_trigger_instant(&schedule, Duration::ZERO)
            && let Some(entry) = state.crons.get_mut(&cron_id)
        {
            entry.next_trigger = next_trigger;
        }
    }
}

// ── Spawn chain / single job ────────────────────────────────────────────────

/// Check whether any pipeline in the chain contains blocked command patterns.
/// Warn-only rules are returned as advisory messages and do not prevent execution.
fn check_chain_guardrails(chain: &ChainNode, config: &Config) -> Result<Vec<String>, String> {
    let mut warnings = Vec::new();
    let leaves = flatten_leaves(chain);
    for leaf in &leaves {
        for pipeline in leaf.plan.pipelines() {
            for segment in &pipeline.segments {
                match config.check_command_guardrail(&segment.command) {
                    Some(BlockDecision::Block(reason)) => return Err(reason),
                    Some(BlockDecision::Warn(hint)) => warnings.push(hint),
                    None => {}
                }
            }
        }
    }
    Ok(warnings)
}

/// Spawn a chain (or a single job) from a `ChainNode`, returning the response payload.
async fn spawn_chain(
    request: SpawnChainRequest,
    state: &mut SchedulerState,
    io: SchedulerIo<'_>,
) -> ResponsePayload {
    let SpawnChainRequest {
        chain,
        scope_hash,
        options,
        warnings,
        retain_completed_chain,
        session_id,
    } = request;

    if options.scope_enabled
        && let Err(message) = validate_scope_transform_support(&chain)
    {
        return ResponsePayload::err(error_code::INVALID_SYNTAX, message);
    }

    let leaves = flatten_leaves(&chain);

    if leaves.len() == 1 {
        let leaf = &leaves[0];
        let jid = state.alloc_job();
        let open_hint = classify_job_plan_open_hint(&leaf.plan);

        let scope_transform = match options
            .scope_enabled
            .then(|| scope_transform_from_command(leaf.command()))
            .transpose()
        {
            Ok(value) => value,
            Err(message) => {
                state.jobs.insert(
                    jid,
                    JobEntry {
                        job_id: jid,
                        session_id: session_id.clone(),
                        pipeline_text: leaf.pipeline_text.clone(),
                        status: JobStatus::Running,
                        exit_code: None,
                        start_scope: Some(scope_hash),
                        end_scope: None,
                        open_hint,
                        chain_id: None,
                        chain_index: None,
                        chain_total: None,
                        pending_reason: None,
                    },
                );
                publish_job_created(
                    io.sys,
                    state,
                    jid,
                    &leaf.pipeline_text,
                    scope_hash,
                    open_hint,
                )
                .await;
                let terminal = set_job_terminal_state(
                    jid,
                    TerminalStateUpdate {
                        status: JobStatus::Failed,
                        exit_code: EXIT_CODE_UNAVAILABLE,
                        end_scope: Some(scope_hash),
                        advance_chain: true,
                    },
                    state,
                    io.db,
                    io.sys,
                )
                .await;
                if let Some(error) = terminal.persist_error {
                    return ResponsePayload::err(error_code::INTERNAL, error);
                }
                return ResponsePayload::err(error_code::INVALID_SYNTAX, message);
            }
        };

        match scope_transform {
            Some(_) => {
                state.jobs.insert(
                    jid,
                    JobEntry {
                        job_id: jid,
                        session_id: session_id.clone(),
                        pipeline_text: leaf.pipeline_text.clone(),
                        status: JobStatus::Running,
                        exit_code: None,
                        start_scope: Some(scope_hash),
                        end_scope: None,
                        open_hint,
                        chain_id: None,
                        chain_index: None,
                        chain_total: None,
                        pending_reason: None,
                    },
                );
                publish_job_created(
                    io.sys,
                    state,
                    jid,
                    &leaf.pipeline_text,
                    scope_hash,
                    open_hint,
                )
                .await;
                info!(%jid, pipeline = %leaf.pipeline_text, "scheduler: applying single scope-transform job");
                match apply_scope_transform(io.sys, scope_hash, leaf.command()).await {
                    Ok(end_scope) => {
                        let terminal = set_job_terminal_state(
                            jid,
                            TerminalStateUpdate {
                                status: JobStatus::Done,
                                exit_code: 0,
                                end_scope: Some(end_scope),
                                advance_chain: true,
                            },
                            state,
                            io.db,
                            io.sys,
                        )
                        .await;
                        if let Some(error) = terminal.persist_error {
                            return ResponsePayload::err(error_code::INTERNAL, error);
                        }
                    }
                    Err(error) => {
                        warn!(%jid, pipeline = %leaf.pipeline_text, "scheduler: scope-transform failed: {error}");
                        let terminal = set_job_terminal_state(
                            jid,
                            TerminalStateUpdate {
                                status: JobStatus::Failed,
                                exit_code: EXIT_CODE_UNAVAILABLE,
                                end_scope: Some(scope_hash),
                                advance_chain: true,
                            },
                            state,
                            io.db,
                            io.sys,
                        )
                        .await;
                        if let Some(error) = terminal.persist_error {
                            return ResponsePayload::err(error_code::INTERNAL, error);
                        }
                    }
                }

                return ResponsePayload::Ok(OkPayload::JobCreated {
                    job_id: jid.to_string(),
                    start_scope: Some(scope_hash.to_string()),
                    open_hint,
                    chain_id: None,
                    chain_index: None,
                    chain_total: None,
                    warnings,
                });
            }
            None => {
                let needs = options.process.needs().clone();
                match admit_resource_scope(io.sys, jid, scope_hash, &needs).await {
                    Ok(ResourceAdmission::Pending(reason)) => {
                        state.jobs.insert(
                            jid,
                            JobEntry {
                                job_id: jid,
                                session_id: session_id.clone(),
                                pipeline_text: leaf.pipeline_text.clone(),
                                status: JobStatus::Pending,
                                exit_code: None,
                                start_scope: Some(scope_hash),
                                end_scope: None,
                                open_hint,
                                chain_id: None,
                                chain_index: None,
                                chain_total: None,
                                pending_reason: Some(reason.clone()),
                            },
                        );
                        record_resource_pending(
                            state,
                            jid,
                            reason,
                            PendingResourceAdmission {
                                plan: leaf.plan.clone(),
                                base_scope: scope_hash,
                                options: options.process_job_options(session_id.clone()),
                                needs: needs.clone(),
                            },
                        );
                        publish_job_created(
                            io.sys,
                            state,
                            jid,
                            &leaf.pipeline_text,
                            scope_hash,
                            open_hint,
                        )
                        .await;
                        return ResponsePayload::Ok(OkPayload::JobCreated {
                            job_id: jid.to_string(),
                            start_scope: Some(scope_hash.to_string()),
                            open_hint,
                            chain_id: None,
                            chain_index: None,
                            chain_total: None,
                            warnings,
                        });
                    }
                    Ok(ResourceAdmission::Granted(spawn_scope)) => {
                        state.jobs.insert(
                            jid,
                            JobEntry {
                                job_id: jid,
                                session_id: session_id.clone(),
                                pipeline_text: leaf.pipeline_text.clone(),
                                status: JobStatus::Running,
                                exit_code: None,
                                start_scope: Some(spawn_scope),
                                end_scope: None,
                                open_hint,
                                chain_id: None,
                                chain_index: None,
                                chain_total: None,
                                pending_reason: None,
                            },
                        );
                        publish_job_created(
                            io.sys,
                            state,
                            jid,
                            &leaf.pipeline_text,
                            spawn_scope,
                            open_hint,
                        )
                        .await;
                        info!(%jid, pipeline = %leaf.pipeline_text, "scheduler: spawning single job");
                        if let Err(message) = spawn_process_job(
                            io.sys,
                            jid,
                            leaf.plan.clone(),
                            spawn_scope,
                            options.process_job_options(session_id.clone()),
                        )
                        .await
                        {
                            let terminal = set_job_terminal_state(
                                jid,
                                TerminalStateUpdate {
                                    status: JobStatus::Failed,
                                    exit_code: EXIT_CODE_UNAVAILABLE,
                                    end_scope: Some(spawn_scope),
                                    advance_chain: true,
                                },
                                state,
                                io.db,
                                io.sys,
                            )
                            .await;
                            if let Some(error) = terminal.persist_error {
                                return ResponsePayload::err(
                                    error_code::INTERNAL,
                                    format!("{message}; {error}"),
                                );
                            }
                            return ResponsePayload::err(error_code::INTERNAL, message);
                        }
                        return ResponsePayload::Ok(OkPayload::JobCreated {
                            job_id: jid.to_string(),
                            start_scope: Some(spawn_scope.to_string()),
                            open_hint,
                            chain_id: None,
                            chain_index: None,
                            chain_total: None,
                            warnings,
                        });
                    }
                    Err(message) => {
                        state.jobs.insert(
                            jid,
                            JobEntry {
                                job_id: jid,
                                session_id: session_id.clone(),
                                pipeline_text: leaf.pipeline_text.clone(),
                                status: JobStatus::Running,
                                exit_code: None,
                                start_scope: Some(scope_hash),
                                end_scope: None,
                                open_hint,
                                chain_id: None,
                                chain_index: None,
                                chain_total: None,
                                pending_reason: Some(message.clone()),
                            },
                        );
                        publish_job_created(
                            io.sys,
                            state,
                            jid,
                            &leaf.pipeline_text,
                            scope_hash,
                            open_hint,
                        )
                        .await;
                        let terminal = set_job_terminal_state(
                            jid,
                            TerminalStateUpdate {
                                status: JobStatus::Failed,
                                exit_code: EXIT_CODE_UNAVAILABLE,
                                end_scope: Some(scope_hash),
                                advance_chain: true,
                            },
                            state,
                            io.db,
                            io.sys,
                        )
                        .await;
                        if let Some(error) = terminal.persist_error {
                            return ResponsePayload::err(
                                error_code::INTERNAL,
                                format!("{message}; {error}"),
                            );
                        }
                        return ResponsePayload::err(error_code::INTERNAL, message);
                    }
                }
            }
        }
    }

    let chain_text = chain.to_string();
    let chain_id = state.alloc_chain();
    let ready_indices = initially_ready(&chain);
    let mut leaf_status: HashMap<usize, LeafStatus> = HashMap::new();

    for leaf in &leaves {
        leaf_status.insert(leaf.index, LeafStatus::Pending);
    }

    let chain_state = ChainState {
        node: chain,
        leaf_jobs: HashMap::new(),
        leaf_status,
        scope_hash,
        pipeline_text: chain_text,
        process: options.process.clone(),
        scope_enabled: options.scope_enabled,
        session_id,
    };
    state.chains.insert(chain_id, chain_state);

    let outcome = process_chain_advance(
        ChainAdvanceRequest {
            chain_id,
            newly_ready: ready_indices
                .iter()
                .copied()
                .map(|idx| (idx, scope_hash))
                .collect(),
            to_cancel: Vec::new(),
            capture_first: ready_indices.len(),
            retain_completed_chain,
        },
        state,
        io,
    )
    .await;
    if let Some(error) = outcome.spawn_error {
        let message = match outcome.persist_error {
            Some(persist_error) => format!("{error}; {persist_error}"),
            None => error,
        };
        return ResponsePayload::err(error_code::INTERNAL, message);
    }
    if let Some(error) = outcome.persist_error {
        return ResponsePayload::err(error_code::INTERNAL, error);
    }
    let Some(chain_info) = build_chain_info(state, chain_id).or(outcome.completed_chain) else {
        return ResponsePayload::err(
            error_code::INTERNAL,
            format!("{chain_id}: chain state unavailable after creation"),
        );
    };

    ResponsePayload::Ok(OkPayload::ChainCreated {
        chain_id: chain_id.to_string(),
        job_ids: outcome
            .captured_job_ids
            .iter()
            .map(|j| j.to_string())
            .collect(),
        chain: chain_info,
        warnings,
    })
}

// ── Job finished handler ────────────────────────────────────────────────────

async fn handle_job_finished(
    job_id: JobId,
    exit_code: i32,
    state: &mut SchedulerState,
    db: &Arc<Mutex<Connection>>,
    sys: &ActorSystem,
) {
    info!(%job_id, exit_code, "scheduler: job finished");

    let new_status = if exit_code == 0 {
        JobStatus::Done
    } else {
        JobStatus::Failed
    };
    let outcome = set_job_terminal_state(
        job_id,
        TerminalStateUpdate {
            status: new_status,
            exit_code,
            end_scope: None,
            advance_chain: true,
        },
        state,
        db,
        sys,
    )
    .await;
    if let Some(chain_advance) = outcome.chain_advance {
        let chain_id = chain_advance.chain_id;
        let advance = process_chain_advance(
            ChainAdvanceRequest {
                chain_id,
                newly_ready: chain_advance.newly_ready,
                to_cancel: chain_advance.to_cancel,
                capture_first: 0,
                retain_completed_chain: false,
            },
            state,
            SchedulerIo::new(db, sys),
        )
        .await;
        if let Some(error) = advance.persist_error {
            warn!(%chain_id, "scheduler: chain advance reported a persistence error: {error}");
        }
    }
    retry_pending_resource_admissions(state, SchedulerIo::new(db, sys)).await;
}

async fn apply_user_terminal_job_update(
    job_id: JobId,
    update: TerminalStateUpdate,
    state: &mut SchedulerState,
    runtime: SchedulerRuntime<'_>,
) -> Option<String> {
    let reported_exit_code = update.exit_code;
    let outcome =
        set_job_terminal_state(job_id, update, state, runtime.io.db, runtime.io.sys).await;
    let mut persist_error = outcome.persist_error.clone();
    if let Some(chain_advance) = outcome.chain_advance {
        let chain_id = chain_advance.chain_id;
        let advance = process_chain_advance(
            ChainAdvanceRequest {
                chain_id,
                newly_ready: chain_advance.newly_ready,
                to_cancel: chain_advance.to_cancel,
                capture_first: 0,
                retain_completed_chain: false,
            },
            state,
            runtime.io,
        )
        .await;
        if persist_error.is_none() {
            persist_error = advance.persist_error;
        }
    }
    retry_pending_resource_admissions(state, runtime.io).await;
    advance_pending_scripts_after_terminal_job(job_id, reported_exit_code, state, runtime).await;
    persist_error
}

async fn cancel_job_execution(
    job_id: JobId,
    state: &mut SchedulerState,
    runtime: SchedulerRuntime<'_>,
) -> Result<(), String> {
    let status = state.jobs.get(&job_id).map(|entry| entry.status.clone());
    match status {
        Some(JobStatus::Pending) | Some(JobStatus::Running) => {
            if matches!(status, Some(JobStatus::Running)) {
                cancel_process_job(runtime.io.sys, job_id).await?;
            }
            if let Some(error) = apply_user_terminal_job_update(
                job_id,
                TerminalStateUpdate {
                    status: JobStatus::Cancelled(CancelReason::User),
                    exit_code: EXIT_CODE_UNAVAILABLE,
                    end_scope: None,
                    advance_chain: true,
                },
                state,
                runtime,
            )
            .await
            {
                return Err(error);
            }
            Ok(())
        }
        // Cancellation is deliberately idempotent. A terminal or retained
        // execution is already quiescent and therefore satisfies the request.
        Some(_) => Ok(()),
        None if job_id.0 < state.next_job => Ok(()),
        None => Err(format!("job {job_id} not found")),
    }
}

async fn cancel_chain_execution(
    chain_id: ChainId,
    state: &mut SchedulerState,
    runtime: SchedulerRuntime<'_>,
) -> Result<(), String> {
    let Some(chain) = state.chains.get(&chain_id) else {
        return if chain_id.0 < state.next_chain {
            Ok(())
        } else {
            Err(format!("chain {chain_id} not found"))
        };
    };
    let to_cancel = flatten_leaves(&chain.node)
        .into_iter()
        .map(|leaf| leaf.index)
        .collect::<Vec<_>>();
    let outcome = process_chain_advance(
        ChainAdvanceRequest {
            chain_id,
            newly_ready: Vec::new(),
            to_cancel,
            capture_first: 0,
            retain_completed_chain: false,
        },
        state,
        runtime.io,
    )
    .await;
    if let Some(error) = outcome.spawn_error.or(outcome.persist_error) {
        return Err(error);
    }
    advance_pending_scripts_after_completed_chains(state, runtime).await;
    Ok(())
}

async fn cancel_script_execution(
    script_id: ScriptId,
    state: &mut SchedulerState,
    runtime: SchedulerRuntime<'_>,
) -> Result<(), String> {
    let Some(pending) = state.pending_scripts.remove(&script_id) else {
        return if script_id.0 < state.next_script {
            Ok(())
        } else {
            Err(format!("script {script_id} not found"))
        };
    };

    // Remove ownership links before cancelling the active item so terminal
    // callbacks cannot advance this script to its next item.
    let current_job = state
        .pending_script_jobs
        .iter()
        .find_map(|(job_id, owner)| (*owner == script_id).then_some(*job_id));
    let current_chain = state
        .pending_script_chains
        .iter()
        .find_map(|(chain_id, owner)| (*owner == script_id).then_some(*chain_id));
    if let Some(job_id) = current_job {
        state.pending_script_jobs.remove(&job_id);
    }
    if let Some(chain_id) = current_chain {
        state.pending_script_chains.remove(&chain_id);
    }

    let cancel_result = if let Some(job_id) = current_job {
        cancel_job_execution(job_id, state, runtime).await
    } else if let Some(chain_id) = current_chain {
        cancel_chain_execution(chain_id, state, runtime).await
    } else {
        Ok(())
    };

    finish_pending_script_failed(pending, EXIT_CODE_UNAVAILABLE, state, runtime).await;
    cancel_result
}

/// Shared logic for processing chain advancement results (cancels + spawns + cleanup).
///
/// Used by `handle_job_finished`, `:kill`, and `:cancel` handlers.
async fn process_chain_advance(
    request: ChainAdvanceRequest,
    state: &mut SchedulerState,
    io: SchedulerIo<'_>,
) -> ChainAdvanceOutcome {
    let ChainAdvanceRequest {
        chain_id,
        newly_ready,
        to_cancel,
        capture_first,
        retain_completed_chain,
    } = request;
    let mut outcome = ChainAdvanceOutcome::default();
    if let Some(error) = cancel_chain_leaves(chain_id, &to_cancel, state, io.db, io.sys).await {
        outcome.record_persist_error(error);
    }

    let (leaves, chain_context, session_id) = {
        let Some(chain) = state.chains.get(&chain_id) else {
            return outcome;
        };
        (
            flatten_leaves(&chain.node),
            ChainExecutionOptions::from_chain(chain),
            chain.session_id.clone(),
        )
    };

    let mut queue: VecDeque<(usize, ScopeHash)> = newly_ready.into();

    while let Some((idx, start_scope)) = queue.pop_front() {
        let jid = state.alloc_job();
        let open_hint = classify_job_plan_open_hint(&leaves[idx].plan);
        if outcome.captured_job_ids.len() < capture_first {
            outcome.captured_job_ids.push(jid);
        }

        if let Some(chain) = state.chains.get_mut(&chain_id) {
            chain.leaf_jobs.insert(idx, jid);
            chain.leaf_status.insert(idx, LeafStatus::Running);
        } else {
            break;
        }

        state.job_to_chain.insert(jid, (chain_id, idx));
        state.jobs.insert(
            jid,
            JobEntry {
                job_id: jid,
                session_id: session_id.clone(),
                pipeline_text: leaves[idx].pipeline_text.clone(),
                status: JobStatus::Running,
                exit_code: None,
                start_scope: Some(start_scope),
                end_scope: None,
                open_hint,
                chain_id: Some(chain_id),
                chain_index: Some(idx),
                chain_total: Some(leaves.len()),
                pending_reason: None,
            },
        );

        info!(%chain_id, %jid, leaf_idx = idx, "scheduler: spawning next chain leaf");
        publish_job_created(
            io.sys,
            state,
            jid,
            &leaves[idx].pipeline_text,
            start_scope,
            open_hint,
        )
        .await;

        match chain_context
            .scope_enabled
            .then(|| scope_transform_from_command(leaves[idx].command()))
            .transpose()
        {
            Ok(Some(_)) => {
                match apply_scope_transform(io.sys, start_scope, leaves[idx].command()).await {
                    Ok(end_scope) => {
                        let terminal = set_job_terminal_state(
                            jid,
                            TerminalStateUpdate {
                                status: JobStatus::Done,
                                exit_code: 0,
                                end_scope: Some(end_scope),
                                advance_chain: true,
                            },
                            state,
                            io.db,
                            io.sys,
                        )
                        .await;
                        apply_terminal_chain_advance(
                            chain_id,
                            terminal,
                            &mut outcome,
                            &mut queue,
                            state,
                            io,
                        )
                        .await;
                    }
                    Err(error) => {
                        warn!(%jid, pipeline = %leaves[idx].pipeline_text, "scheduler: scope-transform failed: {error}");
                        let terminal = set_job_terminal_state(
                            jid,
                            TerminalStateUpdate {
                                status: JobStatus::Failed,
                                exit_code: EXIT_CODE_UNAVAILABLE,
                                end_scope: Some(start_scope),
                                advance_chain: true,
                            },
                            state,
                            io.db,
                            io.sys,
                        )
                        .await;
                        apply_terminal_chain_advance(
                            chain_id,
                            terminal,
                            &mut outcome,
                            &mut queue,
                            state,
                            io,
                        )
                        .await;
                    }
                }
            }
            Ok(None) => {
                let proc_options = chain_context.process_job_options(session_id.clone());
                let needs = chain_context.process.needs().clone();
                match admit_resource_scope(io.sys, jid, start_scope, &needs).await {
                    Ok(ResourceAdmission::Pending(reason)) => {
                        if let Some(chain) = state.chains.get_mut(&chain_id) {
                            chain.leaf_status.insert(idx, LeafStatus::Pending);
                        }
                        record_resource_pending(
                            state,
                            jid,
                            reason,
                            PendingResourceAdmission {
                                plan: leaves[idx].plan.clone(),
                                base_scope: start_scope,
                                options: proc_options,
                                needs: needs.clone(),
                            },
                        );
                        debug!(%chain_id, %jid, leaf_idx = idx, "scheduler: chain leaf waiting for resources");
                    }
                    Ok(ResourceAdmission::Granted(spawn_scope)) => {
                        if let Some(entry) = state.jobs.get_mut(&jid) {
                            entry.start_scope = Some(spawn_scope);
                        }
                        if let Err(error) = spawn_process_job(
                            io.sys,
                            jid,
                            leaves[idx].plan.clone(),
                            spawn_scope,
                            proc_options,
                        )
                        .await
                        {
                            warn!(%chain_id, %jid, pipeline = %leaves[idx].pipeline_text, "scheduler: failed to spawn chain leaf: {error}");
                            outcome.spawn_error.get_or_insert_with(|| error.clone());
                            let terminal = set_job_terminal_state(
                                jid,
                                TerminalStateUpdate {
                                    status: JobStatus::Failed,
                                    exit_code: EXIT_CODE_UNAVAILABLE,
                                    end_scope: Some(spawn_scope),
                                    advance_chain: true,
                                },
                                state,
                                io.db,
                                io.sys,
                            )
                            .await;
                            apply_terminal_chain_advance(
                                chain_id,
                                terminal,
                                &mut outcome,
                                &mut queue,
                                state,
                                io,
                            )
                            .await;
                        }
                    }
                    Err(error) => {
                        warn!(%chain_id, %jid, pipeline = %leaves[idx].pipeline_text, "scheduler: resource admission failed internally: {error}");
                        if let Some(entry) = state.jobs.get_mut(&jid) {
                            entry.pending_reason = Some(error.clone());
                        }
                        let terminal = set_job_terminal_state(
                            jid,
                            TerminalStateUpdate {
                                status: JobStatus::Failed,
                                exit_code: EXIT_CODE_UNAVAILABLE,
                                end_scope: Some(start_scope),
                                advance_chain: true,
                            },
                            state,
                            io.db,
                            io.sys,
                        )
                        .await;
                        apply_terminal_chain_advance(
                            chain_id,
                            terminal,
                            &mut outcome,
                            &mut queue,
                            state,
                            io,
                        )
                        .await;
                    }
                }
            }
            Err(error) => {
                warn!(%jid, pipeline = %leaves[idx].pipeline_text, "scheduler: invalid scope-transform leaf: {error}");
                let terminal = set_job_terminal_state(
                    jid,
                    TerminalStateUpdate {
                        status: JobStatus::Failed,
                        exit_code: EXIT_CODE_UNAVAILABLE,
                        end_scope: Some(start_scope),
                        advance_chain: true,
                    },
                    state,
                    io.db,
                    io.sys,
                )
                .await;
                apply_terminal_chain_advance(
                    chain_id,
                    terminal,
                    &mut outcome,
                    &mut queue,
                    state,
                    io,
                )
                .await;
            }
        }
    }

    publish_chain_progress(io.sys, state, chain_id).await;

    if let Some(chain) = state.chains.get(&chain_id)
        && is_chain_terminal(&chain.node, &chain.leaf_status)
    {
        outcome.completed_chain = build_chain_info(state, chain_id);
        let completion = ChainCompletion {
            exit_code: aggregate_chain_exit_code(&chain.node, &chain.leaf_status),
            end_scope: chain_final_scope(chain, state),
        };
        let exit_code = completion.exit_code;
        info!(%chain_id, exit_code, "scheduler: chain complete");
        if retain_completed_chain || state.pending_script_chains.contains_key(&chain_id) {
            state.completed_chains.insert(chain_id, completion);
        }
        if let Some(finished) = state.chains.remove(&chain_id) {
            for jid in finished.leaf_jobs.values() {
                state.job_to_chain.remove(jid);
            }
        } else {
            warn!(%chain_id, "scheduler: completed chain disappeared before cleanup");
        }
    }

    outcome
}

async fn apply_terminal_chain_advance(
    chain_id: ChainId,
    terminal: TerminalStateOutcome,
    outcome: &mut ChainAdvanceOutcome,
    queue: &mut VecDeque<(usize, ScopeHash)>,
    state: &mut SchedulerState,
    io: SchedulerIo<'_>,
) {
    outcome.record_terminal_state(&terminal);
    let Some(chain_advance) = terminal.chain_advance else {
        return;
    };

    debug_assert_eq!(chain_advance.chain_id, chain_id);
    if let Some(error) =
        cancel_chain_leaves(chain_id, &chain_advance.to_cancel, state, io.db, io.sys).await
    {
        outcome.record_persist_error(error);
    }
    queue.extend(chain_advance.newly_ready);
}

// ── Command dispatch ────────────────────────────────────────────────────────

async fn handle_wait_command(
    id: String,
    client_id: u64,
    request_id: u32,
    state: &mut SchedulerState,
) -> Option<ResponsePayload> {
    if let Some(job_id) = parse_job_id(&id) {
        let requester_session_id = state
            .named_session_id_for_client(client_id)
            .map(str::to_owned);
        if let Err(response) = authorize_session_owned_target(
            state,
            requester_session_id.as_deref(),
            SessionOwnedTarget::Job(job_id),
        ) {
            return Some(response.into_response());
        }
        let Some(entry) = state.jobs.get(&job_id) else {
            return Some(ResponsePayload::err(
                error_code::NOT_FOUND,
                format!("job {id} not found"),
            ));
        };
        if entry.status.is_terminal() {
            return Some(ResponsePayload::Ok(OkPayload::JobInfo(
                job_info_from_entry(entry),
            )));
        }
        state
            .job_waiters
            .entry(job_id)
            .or_default()
            .push(PendingWait {
                client_id,
                request_id,
            });
        return None;
    }

    if id.starts_with('S') {
        return Some(ResponsePayload::err(
            error_code::NOT_SUPPORTED,
            "`:wait` currently supports job IDs only",
        ));
    }

    Some(ResponsePayload::err(
        error_code::NOT_FOUND,
        format!("{id} not found"),
    ))
}

async fn start_pending_script_run(
    mode: Mode,
    source: ScriptSource,
    items: Vec<ResolvedScriptItem>,
    client_id: u64,
    state: &mut SchedulerState,
    runtime: SchedulerRuntime<'_>,
) -> Option<ResponsePayload> {
    #[cfg(test)]
    ensure_test_session(state, runtime.io.sys, client_id).await;
    prune_completed_script_snapshots(state, Instant::now());
    if state.pending_scripts.len() + state.completed_script_snapshots.len()
        >= SCRIPT_SNAPSHOT_IDENTITY_CAPACITY
    {
        return Some(ResponsePayload::err(
            error_code::INVALID_STATE,
            "script recovery identity ledger is saturated; refusing a new script",
        ));
    }
    let script_id = state.alloc_script();
    let item_scope = match create_isolated_script_scope(state, client_id, runtime.io.sys).await {
        Ok(scope) => scope,
        Err(response) => return Some(response),
    };
    let pending = PendingScriptRun {
        client_id,
        script_id,
        mode,
        source,
        items: items.into(),
        next_index: 0,
        item_scope,
        created_items: Vec::new(),
        last_exit_code: 0,
        waiting_index: None,
        session_id: state
            .named_session_id_for_client(client_id)
            .map(str::to_owned),
    };
    submit_pending_script_next(pending, true, state, runtime).await
}

async fn continue_pending_script(
    pending: PendingScriptRun,
    state: &mut SchedulerState,
    runtime: SchedulerRuntime<'_>,
) {
    if let Some(response) = submit_pending_script_next(pending, false, state, runtime).await {
        warn!(
            ?response,
            "scheduler: script continuation produced an unexpected client response"
        );
    }
}

async fn submit_pending_script_next(
    mut pending: PendingScriptRun,
    respond_created: bool,
    state: &mut SchedulerState,
    runtime: SchedulerRuntime<'_>,
) -> Option<ResponsePayload> {
    while let Some(item) = pending.items.pop_front() {
        let index = pending.next_index;
        pending.next_index += 1;
        let source_text = item.source;
        let response = Box::pin(handle_command_with_scope(
            *item.command,
            pending.client_id,
            state,
            runtime.io.db,
            runtime.config,
            runtime.io.sys,
            CommandExecutionContext {
                scope_override: Some(pending.item_scope),
                direct_output_client: Some(pending.client_id),
                session_id: pending.session_id.clone(),
            },
        ))
        .await;

        match response {
            ResponsePayload::Err { code, message } => {
                let submit_error = Some(ScriptSubmitError {
                    index,
                    source: source_text,
                    code,
                    message,
                });
                if let Err(error) = persist_script_finished_with_retention(
                    pending.script_id,
                    pending.mode,
                    &pending.created_items,
                    ScriptFinish::failed(EXIT_CODE_UNAVAILABLE, Some(index)),
                    submit_error.as_ref(),
                    runtime.io.db,
                    runtime.config,
                )
                .await
                {
                    let message = error.to_string();
                    warn!(script = %pending.script_id, "scheduler: failed to persist script submission: {message}");
                    if respond_created {
                        return Some(ResponsePayload::err(error_code::INTERNAL, message));
                    }
                }
                publish_script_finished(
                    state,
                    runtime.io.sys,
                    pending.client_id,
                    pending.script_id,
                    ScriptCompletion {
                        status: ScriptRunStatus::Failed,
                        exit_code: EXIT_CODE_UNAVAILABLE,
                        failed_item_index: Some(index),
                        items: &pending.created_items,
                        submit_error: submit_error.as_ref(),
                        session_id: pending.session_id.as_deref(),
                    },
                )
                .await;
                return respond_created.then(|| {
                    ResponsePayload::Ok(OkPayload::ScriptCreated {
                        script_id: pending.script_id.to_string(),
                        source: pending.source,
                        items: pending.created_items,
                        submit_error,
                    })
                });
            }
            ResponsePayload::Ok(payload) => {
                if let Some(next_scope) = script_item_end_scope_from_ok(&payload, state) {
                    pending.item_scope = next_scope;
                }
                let result = script_item_result_from_ok(&payload);
                let created_item = ScriptItemInfo {
                    index,
                    source: source_text,
                    result,
                };
                pending.created_items.push(created_item.clone());
                if !respond_created {
                    publish_script_item_created(
                        state,
                        runtime.io.sys,
                        pending.client_id,
                        pending.script_id,
                        created_item,
                        pending.session_id.as_deref(),
                    )
                    .await;
                }

                if let Some(exit_code) = immediate_script_item_exit_code(&payload, state) {
                    pending.last_exit_code = exit_code;
                    if exit_code != 0 {
                        if let Err(error) = persist_script_finished_with_retention(
                            pending.script_id,
                            pending.mode,
                            &pending.created_items,
                            ScriptFinish::failed(exit_code, Some(index)),
                            None,
                            runtime.io.db,
                            runtime.config,
                        )
                        .await
                        {
                            let message = error.to_string();
                            warn!(script = %pending.script_id, "scheduler: failed to persist script submission: {message}");
                            if respond_created {
                                return Some(ResponsePayload::err(error_code::INTERNAL, message));
                            }
                        }
                        publish_script_finished(
                            state,
                            runtime.io.sys,
                            pending.client_id,
                            pending.script_id,
                            ScriptCompletion {
                                status: ScriptRunStatus::Failed,
                                exit_code,
                                failed_item_index: Some(index),
                                items: &pending.created_items,
                                submit_error: None,
                                session_id: pending.session_id.as_deref(),
                            },
                        )
                        .await;
                        return respond_created.then(|| script_created_response(&pending, None));
                    }
                    continue;
                }

                match pending.created_items.last().map(|item| &item.result) {
                    Some(ScriptItemResult::Job { job_id, .. }) => {
                        let Some(job_id) = parse_job_id(job_id) else {
                            if let Err(error) = persist_script_finished_with_retention(
                                pending.script_id,
                                pending.mode,
                                &pending.created_items,
                                ScriptFinish::failed(EXIT_CODE_UNAVAILABLE, Some(index)),
                                None,
                                runtime.io.db,
                                runtime.config,
                            )
                            .await
                            {
                                let message = error.to_string();
                                warn!(script = %pending.script_id, "scheduler: failed to persist script completion: {message}");
                                if respond_created {
                                    return Some(ResponsePayload::err(
                                        error_code::INTERNAL,
                                        message,
                                    ));
                                }
                            }
                            publish_script_finished(
                                state,
                                runtime.io.sys,
                                pending.client_id,
                                pending.script_id,
                                ScriptCompletion {
                                    status: ScriptRunStatus::Failed,
                                    exit_code: EXIT_CODE_UNAVAILABLE,
                                    failed_item_index: Some(index),
                                    items: &pending.created_items,
                                    submit_error: None,
                                    session_id: pending.session_id.as_deref(),
                                },
                            )
                            .await;
                            return respond_created
                                .then(|| script_created_response(&pending, None));
                        };
                        pending.waiting_index = Some(index);
                        let response =
                            respond_created.then(|| script_created_response(&pending, None));
                        state.pending_script_jobs.insert(job_id, pending.script_id);
                        let script_id = pending.script_id;
                        let persist_error = persist_script_submission(
                            pending.script_id,
                            pending.mode,
                            &pending.created_items,
                            None,
                            runtime.io.db,
                        )
                        .await
                        .err()
                        .map(|error| error.to_string());
                        state.pending_scripts.insert(pending.script_id, pending);
                        if let Some(message) = persist_error {
                            warn!(script = %script_id, "scheduler: failed to persist script submission: {message}");
                            if respond_created {
                                return Some(ResponsePayload::err(error_code::INTERNAL, message));
                            }
                        }
                        return response;
                    }
                    Some(ScriptItemResult::Chain { chain_id, .. }) => {
                        let Some(chain_id) = parse_chain_id(chain_id) else {
                            if let Err(error) = persist_script_finished_with_retention(
                                pending.script_id,
                                pending.mode,
                                &pending.created_items,
                                ScriptFinish::failed(EXIT_CODE_UNAVAILABLE, Some(index)),
                                None,
                                runtime.io.db,
                                runtime.config,
                            )
                            .await
                            {
                                let message = error.to_string();
                                warn!(script = %pending.script_id, "scheduler: failed to persist script completion: {message}");
                                if respond_created {
                                    return Some(ResponsePayload::err(
                                        error_code::INTERNAL,
                                        message,
                                    ));
                                }
                            }
                            publish_script_finished(
                                state,
                                runtime.io.sys,
                                pending.client_id,
                                pending.script_id,
                                ScriptCompletion {
                                    status: ScriptRunStatus::Failed,
                                    exit_code: EXIT_CODE_UNAVAILABLE,
                                    failed_item_index: Some(index),
                                    items: &pending.created_items,
                                    submit_error: None,
                                    session_id: pending.session_id.as_deref(),
                                },
                            )
                            .await;
                            return respond_created
                                .then(|| script_created_response(&pending, None));
                        };
                        pending.waiting_index = Some(index);
                        let response =
                            respond_created.then(|| script_created_response(&pending, None));
                        if let Some(completion) = take_completed_chain(state, chain_id) {
                            if let Some(scope) = completion.end_scope {
                                pending.item_scope = scope;
                            }
                            pending.last_exit_code = completion.exit_code;
                            if completion.exit_code != 0 {
                                finish_pending_script_failed(
                                    pending,
                                    completion.exit_code,
                                    state,
                                    runtime,
                                )
                                .await;
                                return response;
                            }
                            continue;
                        }
                        state
                            .pending_script_chains
                            .insert(chain_id, pending.script_id);
                        let script_id = pending.script_id;
                        let persist_error = persist_script_submission(
                            pending.script_id,
                            pending.mode,
                            &pending.created_items,
                            None,
                            runtime.io.db,
                        )
                        .await
                        .err()
                        .map(|error| error.to_string());
                        state.pending_scripts.insert(pending.script_id, pending);
                        if let Some(message) = persist_error {
                            warn!(script = %script_id, "scheduler: failed to persist script submission: {message}");
                            if respond_created {
                                return Some(ResponsePayload::err(error_code::INTERNAL, message));
                            }
                        }
                        return response;
                    }
                    _ => continue,
                }
            }
        }
    }

    if let Err(error) = persist_script_finished_with_retention(
        pending.script_id,
        pending.mode,
        &pending.created_items,
        ScriptFinish::done(pending.last_exit_code),
        None,
        runtime.io.db,
        runtime.config,
    )
    .await
    {
        let message = error.to_string();
        warn!(script = %pending.script_id, "scheduler: failed to persist script submission: {message}");
        if respond_created {
            return Some(ResponsePayload::err(error_code::INTERNAL, message));
        }
    }
    publish_script_finished(
        state,
        runtime.io.sys,
        pending.client_id,
        pending.script_id,
        ScriptCompletion {
            status: ScriptRunStatus::Done,
            exit_code: pending.last_exit_code,
            failed_item_index: None,
            items: &pending.created_items,
            submit_error: None,
            session_id: pending.session_id.as_deref(),
        },
    )
    .await;
    respond_created.then(|| script_created_response(&pending, None))
}

fn take_completed_chain(state: &mut SchedulerState, chain_id: ChainId) -> Option<ChainCompletion> {
    state.completed_chains.remove(&chain_id)
}

async fn advance_pending_scripts_after_terminal_job(
    job_id: JobId,
    reported_exit_code: i32,
    state: &mut SchedulerState,
    runtime: SchedulerRuntime<'_>,
) {
    if let Some(script_id) = state.pending_script_jobs.remove(&job_id)
        && let Some(mut pending) = state.pending_scripts.remove(&script_id)
    {
        let exit_code = script_exit_code_for_job(state, job_id, reported_exit_code);
        if let Some(entry) = state.jobs.get(&job_id)
            && let Some(scope) = entry.end_scope.or(entry.start_scope)
        {
            pending.item_scope = scope;
        }
        pending.last_exit_code = exit_code;
        if exit_code != 0 {
            finish_pending_script_failed(pending, exit_code, state, runtime).await;
        } else {
            continue_pending_script(pending, state, runtime).await;
        }
    }

    advance_pending_scripts_after_completed_chains(state, runtime).await;
}

async fn advance_pending_scripts_after_completed_chains(
    state: &mut SchedulerState,
    runtime: SchedulerRuntime<'_>,
) {
    let finished_chains = state
        .pending_script_chains
        .keys()
        .filter(|chain_id| state.completed_chains.contains_key(chain_id))
        .copied()
        .collect::<Vec<_>>();
    for chain_id in finished_chains {
        let Some(completion) = take_completed_chain(state, chain_id) else {
            continue;
        };
        let Some(script_id) = state.pending_script_chains.remove(&chain_id) else {
            continue;
        };
        let Some(mut pending) = state.pending_scripts.remove(&script_id) else {
            continue;
        };
        if let Some(scope) = completion.end_scope {
            pending.item_scope = scope;
        }
        pending.last_exit_code = completion.exit_code;
        if completion.exit_code != 0 {
            finish_pending_script_failed(pending, completion.exit_code, state, runtime).await;
        } else {
            continue_pending_script(pending, state, runtime).await;
        }
    }
}

fn script_exit_code_for_job(state: &SchedulerState, job_id: JobId, reported_exit_code: i32) -> i32 {
    let Some(entry) = state.jobs.get(&job_id) else {
        return reported_exit_code;
    };
    match entry.status {
        JobStatus::Done => entry.exit_code.unwrap_or(reported_exit_code),
        JobStatus::Failed | JobStatus::Killed | JobStatus::Cancelled(_) => {
            entry.exit_code.unwrap_or(reported_exit_code)
        }
        JobStatus::Pending | JobStatus::Running => reported_exit_code,
    }
}

async fn finish_pending_script_failed(
    pending: PendingScriptRun,
    exit_code: i32,
    state: &mut SchedulerState,
    runtime: SchedulerRuntime<'_>,
) {
    let failed_index = pending.waiting_index;
    if let Err(error) = persist_script_finished_with_retention(
        pending.script_id,
        pending.mode,
        &pending.created_items,
        ScriptFinish::failed(exit_code, failed_index),
        None,
        runtime.io.db,
        runtime.config,
    )
    .await
    {
        warn!(script = %pending.script_id, "scheduler: failed to persist script completion: {error}");
    }
    publish_script_finished(
        state,
        runtime.io.sys,
        pending.client_id,
        pending.script_id,
        ScriptCompletion {
            status: ScriptRunStatus::Failed,
            exit_code,
            failed_item_index: failed_index,
            items: &pending.created_items,
            submit_error: None,
            session_id: pending.session_id.as_deref(),
        },
    )
    .await;
}

async fn fail_pending_scripts_on_shutdown(
    state: &mut SchedulerState,
    runtime: SchedulerRuntime<'_>,
) {
    let mut pending = std::mem::take(&mut state.pending_scripts)
        .into_values()
        .collect::<Vec<_>>();
    pending.sort_by_key(|pending| pending.script_id.0);
    state.pending_script_jobs.clear();
    state.pending_script_chains.clear();
    state.completed_chains.clear();

    for pending in pending {
        finish_pending_script_failed(pending, EXIT_CODE_UNAVAILABLE, state, runtime).await;
    }
}

fn immediate_script_item_exit_code(payload: &OkPayload, state: &SchedulerState) -> Option<i32> {
    match payload {
        OkPayload::JobCreated { job_id, .. } => {
            let job_id = parse_job_id(job_id)?;
            let entry = state.jobs.get(&job_id)?;
            entry
                .status
                .is_terminal()
                .then_some(entry.exit_code.unwrap_or(EXIT_CODE_UNAVAILABLE))
        }
        OkPayload::CronAdded { .. }
        | OkPayload::EvalText { .. }
        | OkPayload::TextOutput { .. }
        | OkPayload::ScopeCreated { .. }
        | OkPayload::Ack {} => Some(0),
        _ => None,
    }
}

fn script_info_response(id: &str, client_id: u64, state: &mut SchedulerState) -> ResponsePayload {
    let Some(script_id) = parse_script_id(id) else {
        return ResponsePayload::err(
            error_code::INVALID_REQUEST,
            format!("invalid script id {id}"),
        );
    };
    prune_completed_script_snapshots(state, Instant::now());
    let requester_session_id = state
        .named_session_id_for_client(client_id)
        .map(str::to_owned);
    if let Err(response) = authorize_session_owned_target(
        state,
        requester_session_id.as_deref(),
        SessionOwnedTarget::Script(script_id),
    ) {
        return response.into_response();
    }
    if let Some(pending) = state.pending_scripts.get(&script_id) {
        let info = ScriptInfo {
            script_id: script_id.to_string(),
            status: ScriptInfoStatus::Running,
            items: pending.created_items.clone(),
            exit_code: None,
            failed_item_index: None,
            submit_error: None,
        };
        if serialized_script_info_size(&info) > SCRIPT_SNAPSHOT_MAX_ITEM_BYTES {
            return ResponsePayload::err(
                error_code::INVALID_STATE,
                format!("script {script_id} recovery snapshot exceeds the replay limit"),
            );
        }
        return ResponsePayload::Ok(OkPayload::ScriptInfo(info));
    }
    if let Some(snapshot) = state.completed_script_snapshots.get(&script_id) {
        return match &snapshot.info {
            Some(info) => ResponsePayload::Ok(OkPayload::ScriptInfo(info.clone())),
            None => ResponsePayload::err(
                error_code::INVALID_STATE,
                format!("script {script_id} completed, but its recovery snapshot is unavailable"),
            ),
        };
    }
    if script_id.0 < state.next_script {
        ResponsePayload::err(
            error_code::INVALID_STATE,
            format!("script {script_id} recovery snapshot expired or is unavailable"),
        )
    } else {
        ResponsePayload::err(
            error_code::NOT_FOUND,
            format!("script {script_id} not found"),
        )
    }
}

fn record_completed_script_snapshot(
    state: &mut SchedulerState,
    info: ScriptInfo,
    session_id: Option<String>,
) {
    prune_completed_script_snapshots(state, Instant::now());
    let script_id = parse_script_id(&info.script_id).expect("scheduler created valid script id");
    let response_bytes = serialized_script_info_size(&info);
    let (info, response_bytes) = if response_bytes <= SCRIPT_SNAPSHOT_MAX_ITEM_BYTES {
        (Some(info), response_bytes)
    } else {
        (None, 0)
    };
    if let Some(previous) = state.completed_script_snapshots.insert(
        script_id,
        CompletedScriptSnapshot {
            info,
            session_id,
            completed_at: Instant::now(),
            response_bytes,
        },
    ) {
        if previous.info.is_some() {
            state.completed_script_snapshot_responses =
                state.completed_script_snapshot_responses.saturating_sub(1);
            state.completed_script_snapshot_bytes = state
                .completed_script_snapshot_bytes
                .saturating_sub(previous.response_bytes);
        }
        state
            .completed_script_snapshot_order
            .retain(|existing| *existing != script_id);
    }
    if response_bytes > 0 {
        state.completed_script_snapshot_responses += 1;
        state.completed_script_snapshot_bytes += response_bytes;
    }
    state.completed_script_snapshot_order.push_back(script_id);
    while state.completed_script_snapshot_responses > SCRIPT_SNAPSHOT_RESPONSE_CAPACITY
        || state.completed_script_snapshot_bytes > SCRIPT_SNAPSHOT_MAX_TOTAL_BYTES
    {
        let Some(oldest) = state
            .completed_script_snapshot_order
            .iter()
            .copied()
            .find(|id| {
                state
                    .completed_script_snapshots
                    .get(id)
                    .is_some_and(|snapshot| snapshot.info.is_some())
            })
        else {
            break;
        };
        let snapshot = state
            .completed_script_snapshots
            .get_mut(&oldest)
            .expect("snapshot exists after lookup");
        snapshot.info = None;
        state.completed_script_snapshot_responses =
            state.completed_script_snapshot_responses.saturating_sub(1);
        state.completed_script_snapshot_bytes = state
            .completed_script_snapshot_bytes
            .saturating_sub(snapshot.response_bytes);
        snapshot.response_bytes = 0;
    }
}

fn serialized_script_info_size(info: &ScriptInfo) -> usize {
    serde_json::to_vec(info)
        .map(|encoded| encoded.len())
        .unwrap_or(usize::MAX)
}

fn prune_completed_script_snapshots(state: &mut SchedulerState, now: Instant) {
    while let Some(oldest) = state.completed_script_snapshot_order.front().copied() {
        let expired = state
            .completed_script_snapshots
            .get(&oldest)
            .is_none_or(|snapshot| {
                now.saturating_duration_since(snapshot.completed_at) >= SCRIPT_SNAPSHOT_TTL
            });
        if !expired {
            break;
        }
        state.completed_script_snapshot_order.pop_front();
        if let Some(snapshot) = state.completed_script_snapshots.remove(&oldest)
            && snapshot.info.is_some()
        {
            state.completed_script_snapshot_responses =
                state.completed_script_snapshot_responses.saturating_sub(1);
            state.completed_script_snapshot_bytes = state
                .completed_script_snapshot_bytes
                .saturating_sub(snapshot.response_bytes);
        }
    }
}

fn script_created_response(
    pending: &PendingScriptRun,
    submit_error: Option<ScriptSubmitError>,
) -> ResponsePayload {
    ResponsePayload::Ok(OkPayload::ScriptCreated {
        script_id: pending.script_id.to_string(),
        source: pending.source.clone(),
        items: pending.created_items.clone(),
        submit_error,
    })
}

async fn publish_script_item_created(
    state: &SchedulerState,
    sys: &ActorSystem,
    client_id: u64,
    script_id: ScriptId,
    item: ScriptItemInfo,
    session_id: Option<&str>,
) {
    let payload = EventPayload::ScriptItemCreated {
        script_id: script_id.to_string(),
        item,
    };
    if direct_client_can_receive_resource_event(state, client_id, session_id) {
        send_actor_gateway_event(
            "scheduler",
            sys,
            client_id,
            payload.clone(),
            session_id.map(str::to_owned),
        )
        .await;
    }
    publish_session_event_except(
        sys,
        EventChannel::Jobs,
        payload,
        session_id.map(str::to_owned),
        client_id,
    )
    .await;
}

fn direct_client_can_receive_resource_event(
    state: &SchedulerState,
    client_id: u64,
    session_id: Option<&str>,
) -> bool {
    state
        .named_session_id_for_client(client_id)
        .is_none_or(|attached| session_id == Some(attached))
}

struct ScriptCompletion<'a> {
    status: ScriptRunStatus,
    exit_code: i32,
    failed_item_index: Option<usize>,
    items: &'a [ScriptItemInfo],
    submit_error: Option<&'a ScriptSubmitError>,
    session_id: Option<&'a str>,
}

async fn publish_script_finished(
    state: &mut SchedulerState,
    sys: &ActorSystem,
    client_id: u64,
    script_id: ScriptId,
    completion: ScriptCompletion<'_>,
) {
    record_completed_script_snapshot(
        state,
        ScriptInfo {
            script_id: script_id.to_string(),
            status: match completion.status {
                ScriptRunStatus::Done => ScriptInfoStatus::Done,
                ScriptRunStatus::Failed => ScriptInfoStatus::Failed,
            },
            items: completion.items.to_vec(),
            exit_code: Some(completion.exit_code),
            failed_item_index: completion.failed_item_index,
            submit_error: completion.submit_error.cloned(),
        },
        completion.session_id.map(str::to_owned),
    );
    let payload = EventPayload::ScriptFinished {
        script_id: script_id.to_string(),
        status: completion.status,
        exit_code: completion.exit_code,
        failed_item_index: completion.failed_item_index,
    };
    if direct_client_can_receive_resource_event(state, client_id, completion.session_id) {
        send_actor_gateway_event(
            "scheduler",
            sys,
            client_id,
            payload.clone(),
            completion.session_id.map(str::to_owned),
        )
        .await;
    }
    publish_session_event_except(
        sys,
        EventChannel::Jobs,
        payload,
        completion.session_id.map(str::to_owned),
        client_id,
    )
    .await;
}

async fn handle_command(
    cmd: ResolvedCommand,
    client_id: u64,
    state: &mut SchedulerState,
    db: &Arc<Mutex<Connection>>,
    config: &Config,
    sys: &ActorSystem,
) -> ResponsePayload {
    if matches!(cmd, ResolvedCommand::Script { .. }) {
        return handle_command_with_scope(
            cmd,
            client_id,
            state,
            db,
            config,
            sys,
            CommandExecutionContext::default(),
        )
        .await;
    }

    #[cfg(test)]
    ensure_test_session(state, sys, client_id).await;
    handle_command_with_scope(
        cmd,
        client_id,
        state,
        db,
        config,
        sys,
        CommandExecutionContext::default(),
    )
    .await
}

async fn handle_command_with_scope(
    cmd: ResolvedCommand,
    client_id: u64,
    state: &mut SchedulerState,
    db: &Arc<Mutex<Connection>>,
    config: &Config,
    sys: &ActorSystem,
    context: CommandExecutionContext,
) -> ResponsePayload {
    let requester_session_id = context.session_id.clone().or_else(|| {
        state
            .named_session_id_for_client(client_id)
            .map(str::to_owned)
    });
    if let Some(target) = session_owned_target_for_command(&cmd)
        && let Err(response) =
            authorize_session_owned_target(state, requester_session_id.as_deref(), target)
    {
        return response.into_response();
    }

    match cmd {
        ResolvedCommand::Script { .. } => ResponsePayload::err(
            error_code::NOT_SUPPORTED,
            "script commands must enter the scheduler through the file-script runner",
        ),
        ResolvedCommand::Run { chain, params } => {
            let Some(base_scope) = resolve_command_scope(state, client_id, context.scope_override)
            else {
                return missing_session_response();
            };
            let scope_hash = match derive_mode_params_scope(sys, base_scope, &params).await {
                Ok(scope) => scope,
                Err(reason) => return ResponsePayload::err(error_code::INVALID_SYNTAX, reason),
            };
            let warnings = match check_chain_guardrails(&chain, config) {
                Ok(warnings) => warnings,
                Err(reason) => return ResponsePayload::err(error_code::BLOCKED, reason),
            };
            let options = match ChainExecutionOptions::from_params(
                &params,
                state,
                client_id,
                config,
                context.direct_output_client,
            ) {
                Ok(options) => options,
                Err(reason) => return ResponsePayload::err(error_code::INVALID_SYNTAX, reason),
            };
            spawn_chain(
                SpawnChainRequest {
                    chain,
                    scope_hash,
                    options,
                    warnings,
                    retain_completed_chain: context.scope_override.is_some(),
                    session_id: requester_session_id.clone(),
                },
                state,
                SchedulerIo::new(db, sys),
            )
            .await
        }

        ResolvedCommand::Cron {
            schedule,
            chain,
            params,
        } => {
            let display_text = schedule.display();
            let Some(base_scope) = resolve_command_scope(state, client_id, context.scope_override)
            else {
                return missing_session_response();
            };
            let scope_hash = match derive_mode_params_scope(sys, base_scope, &params).await {
                Ok(scope) => scope,
                Err(reason) => return ResponsePayload::err(error_code::INVALID_SYNTAX, reason),
            };
            if let Err(reason) = check_chain_guardrails(&chain, config) {
                return ResponsePayload::err(error_code::BLOCKED, reason);
            }

            let cron_id = state.alloc_cron();
            let Some(next_trigger) = next_trigger_instant(&schedule, Duration::ZERO) else {
                return ResponsePayload::err(
                    error_code::INVALID_SYNTAX,
                    format!("cannot compute next trigger for schedule: {display_text}"),
                );
            };
            let options =
                match ChainExecutionOptions::from_params(&params, state, client_id, config, None) {
                    Ok(options) => options,
                    Err(reason) => return ResponsePayload::err(error_code::INVALID_SYNTAX, reason),
                };
            let entry = CronEntry {
                cron_id,
                schedule,
                chain,
                scope_hash,
                status: CronStatus::Scheduled,
                next_trigger,
                cwd_override: None,
                scope_enabled: options.scope_enabled,
                wrapper_enabled: options.process.wrapper_enabled,
                session_id: requester_session_id.clone(),
            };
            if let Err(error) = persist_cron_entry(db, &entry).await {
                return ResponsePayload::err(error_code::INTERNAL, error.to_string());
            }
            state.crons.insert(cron_id, entry);
            info!(%cron_id, "scheduler: cron added");

            ResponsePayload::Ok(OkPayload::CronAdded {
                cron_id: cron_id.to_string(),
            })
        }

        ResolvedCommand::Fg { id, role } => {
            if let Some(job_id) = parse_job_id(&id) {
                let Some(entry) = state.jobs.get(&job_id) else {
                    return ResponsePayload::err(
                        error_code::NOT_FOUND,
                        format!("job {id} not found"),
                    );
                };
                if entry.status != JobStatus::Running {
                    return ResponsePayload::err(
                        error_code::INVALID_STATE,
                        format!("job {job_id} is not running"),
                    );
                }

                let (tx, rx) = tokio::sync::oneshot::channel();
                if sys
                    .process_mgr
                    .send(ProcessMgrMsg::AttachFg {
                        client_id,
                        job_id,
                        role,
                        reply: tx,
                    })
                    .await
                    .is_err()
                {
                    return ResponsePayload::err(error_code::INTERNAL, "process_mgr unreachable");
                }

                match rx.await {
                    Ok(Ok(info)) => ResponsePayload::Ok(OkPayload::FgAttached(Box::new(info))),
                    Ok(Err(message)) => ResponsePayload::err(error_code::INVALID_STATE, message),
                    Err(_) => {
                        ResponsePayload::err(error_code::INTERNAL, "process_mgr reply dropped")
                    }
                }
            } else {
                ResponsePayload::err(error_code::NOT_FOUND, format!("{id} not found"))
            }
        }

        ResolvedCommand::Kill { id } => {
            if let Some(jid) = parse_job_id(&id) {
                let status = state.jobs.get(&jid).map(|entry| entry.status.clone());
                match status {
                    Some(JobStatus::Running) => {
                        if let Err(error) = kill_process_job(sys, jid).await {
                            return ResponsePayload::err(error_code::INTERNAL, error);
                        }
                        info!(%jid, "scheduler: job killed");
                        if let Some(error) = apply_user_terminal_job_update(
                            jid,
                            TerminalStateUpdate {
                                status: JobStatus::Killed,
                                exit_code: EXIT_CODE_UNAVAILABLE,
                                end_scope: None,
                                advance_chain: true,
                            },
                            state,
                            SchedulerRuntime::new(db, config, sys),
                        )
                        .await
                        {
                            return ResponsePayload::err(error_code::INTERNAL, error);
                        }
                        ResponsePayload::ack()
                    }
                    Some(_) => ResponsePayload::err(
                        error_code::INVALID_STATE,
                        format!("job {jid} is not running"),
                    ),
                    None => {
                        ResponsePayload::err(error_code::NOT_FOUND, format!("job {id} not found"))
                    }
                }
            } else if let Some(cid) = parse_cron_id(&id) {
                if state.crons.contains_key(&cid) {
                    if let Err(error) = remove_cron_entry(state, db, sys, cid).await {
                        return ResponsePayload::err(error_code::INTERNAL, error.to_string());
                    }
                    ResponsePayload::ack()
                } else {
                    ResponsePayload::err(error_code::NOT_FOUND, format!("cron {id} not found"))
                }
            } else {
                warn!(%id, "scheduler: kill target not found");
                ResponsePayload::err(error_code::NOT_FOUND, format!("{id} not found"))
            }
        }

        ResolvedCommand::KillJob { id } => {
            let Some(jid) = parse_job_id(&id) else {
                return ResponsePayload::err(
                    error_code::NOT_SUPPORTED,
                    "KillJob only supports job IDs (J<n>)",
                );
            };
            let status = state.jobs.get(&jid).map(|entry| entry.status.clone());
            match status {
                Some(JobStatus::Running) => {
                    if let Err(error) = kill_process_job(sys, jid).await {
                        return ResponsePayload::err(error_code::INTERNAL, error);
                    }
                    info!(%jid, "scheduler: job killed");
                    if let Some(error) = apply_user_terminal_job_update(
                        jid,
                        TerminalStateUpdate {
                            status: JobStatus::Killed,
                            exit_code: EXIT_CODE_UNAVAILABLE,
                            end_scope: None,
                            advance_chain: true,
                        },
                        state,
                        SchedulerRuntime::new(db, config, sys),
                    )
                    .await
                    {
                        return ResponsePayload::err(error_code::INTERNAL, error);
                    }
                    ResponsePayload::ack()
                }
                Some(_) => ResponsePayload::err(
                    error_code::INVALID_STATE,
                    format!("job {jid} is not running"),
                ),
                None => ResponsePayload::err(error_code::NOT_FOUND, format!("job {id} not found")),
            }
        }

        ResolvedCommand::CancelExecution { id } => {
            let runtime = SchedulerRuntime::new(db, config, sys);
            let result = if let Some(job_id) = parse_job_id(&id) {
                cancel_job_execution(job_id, state, runtime).await
            } else if let Some(chain_id) = parse_chain_id(&id) {
                cancel_chain_execution(chain_id, state, runtime).await
            } else if let Some(script_id) = parse_script_id(&id) {
                cancel_script_execution(script_id, state, runtime).await
            } else {
                return ResponsePayload::err(
                    error_code::INVALID_REQUEST,
                    format!("unsupported execution id {id}; expected J<n>, CH<n>, or R<n>"),
                );
            };
            match result {
                Ok(()) => ResponsePayload::ack(),
                Err(message) if message.ends_with("not found") => {
                    ResponsePayload::err(error_code::NOT_FOUND, message)
                }
                Err(message) => ResponsePayload::err(error_code::INTERNAL, message),
            }
        }

        ResolvedCommand::RemoveCron { id } => {
            let Some(cid) = parse_cron_id(&id) else {
                return ResponsePayload::err(
                    error_code::NOT_SUPPORTED,
                    "RemoveCron only supports cron IDs (C<n>)",
                );
            };
            if state.crons.contains_key(&cid) {
                if let Err(error) = remove_cron_entry(state, db, sys, cid).await {
                    return ResponsePayload::err(error_code::INTERNAL, error.to_string());
                }
                ResponsePayload::ack()
            } else {
                ResponsePayload::err(error_code::NOT_FOUND, format!("cron {id} not found"))
            }
        }

        ResolvedCommand::Cancel { id } => {
            if let Some(jid) = parse_job_id(&id) {
                let status = state.jobs.get(&jid).map(|entry| entry.status.clone());
                match status {
                    Some(JobStatus::Pending) | Some(JobStatus::Running) => {
                        if matches!(status, Some(JobStatus::Running))
                            && let Err(error) = kill_process_job(sys, jid).await
                        {
                            return ResponsePayload::err(error_code::INTERNAL, error);
                        }
                        info!(%jid, "scheduler: job cancelled");
                        if let Some(error) = apply_user_terminal_job_update(
                            jid,
                            TerminalStateUpdate {
                                status: JobStatus::Cancelled(CancelReason::User),
                                exit_code: EXIT_CODE_UNAVAILABLE,
                                end_scope: None,
                                advance_chain: true,
                            },
                            state,
                            SchedulerRuntime::new(db, config, sys),
                        )
                        .await
                        {
                            return ResponsePayload::err(error_code::INTERNAL, error);
                        }
                        ResponsePayload::ack()
                    }
                    Some(_) => ResponsePayload::err(
                        error_code::INVALID_STATE,
                        format!("job {jid} is already terminal"),
                    ),
                    None => {
                        ResponsePayload::err(error_code::NOT_FOUND, format!("job {id} not found"))
                    }
                }
            } else {
                ResponsePayload::err(error_code::NOT_FOUND, format!("{id} not found"))
            }
        }

        ResolvedCommand::Pause { id } => {
            if let Some(cid) = parse_cron_id(&id) {
                if let Some(entry) = state.crons.get(&cid) {
                    if entry.status.is_terminal() {
                        return ResponsePayload::err(
                            error_code::INVALID_STATE,
                            format!("cron {cid} is already terminal"),
                        );
                    }
                    let mut updated = entry.clone();
                    updated.status = CronStatus::Paused;
                    if let Err(error) = persist_cron_entry(db, &updated).await {
                        return ResponsePayload::err(error_code::INTERNAL, error.to_string());
                    }
                    state.crons.insert(cid, updated);
                    info!(%cid, "scheduler: cron paused");
                    return ResponsePayload::ack();
                }
                ResponsePayload::err(error_code::NOT_FOUND, format!("cron {id} not found"))
            } else {
                ResponsePayload::err(
                    error_code::NOT_SUPPORTED,
                    "pause only supports cron IDs (C<n>)",
                )
            }
        }

        ResolvedCommand::Resume { id } => {
            if let Some(cid) = parse_cron_id(&id) {
                if let Some(entry) = state.crons.get(&cid) {
                    if entry.status.is_terminal() {
                        return ResponsePayload::err(
                            error_code::INVALID_STATE,
                            format!("cron {cid} is already terminal"),
                        );
                    }
                    let mut updated = entry.clone();
                    updated.status = CronStatus::Scheduled;
                    if let Some(next_trigger) =
                        next_trigger_instant(&updated.schedule, Duration::ZERO)
                    {
                        updated.next_trigger = next_trigger;
                    }
                    if let Err(error) = persist_cron_entry(db, &updated).await {
                        return ResponsePayload::err(error_code::INTERNAL, error.to_string());
                    }
                    state.crons.insert(cid, updated);
                    info!(%cid, "scheduler: cron resumed");
                    return ResponsePayload::ack();
                }
                ResponsePayload::err(error_code::NOT_FOUND, format!("cron {id} not found"))
            } else {
                ResponsePayload::err(
                    error_code::NOT_SUPPORTED,
                    "resume only supports cron IDs (C<n>)",
                )
            }
        }

        ResolvedCommand::Jobs => {
            let list = sorted_job_list_for_client(state, client_id);
            ResponsePayload::Ok(OkPayload::JobList(list))
        }

        ResolvedCommand::ListJobs { limit } => {
            let list = sorted_job_list_for_client(state, client_id);
            let (jobs, page) = page_items(list, limit);
            ResponsePayload::Ok(OkPayload::JobListPage { jobs, page })
        }

        ResolvedCommand::Crons => {
            let list = sorted_cron_list_for_client(state, client_id);
            ResponsePayload::Ok(OkPayload::CronList(list))
        }

        ResolvedCommand::ListCrons { limit } => {
            let list = sorted_cron_list_for_client(state, client_id);
            let (crons, page) = page_items(list, limit);
            ResponsePayload::Ok(OkPayload::CronListPage { crons, page })
        }

        ResolvedCommand::Scopes => handle_list_scopes(sys).await,

        ResolvedCommand::Providers => ResponsePayload::Ok(OkPayload::EvalText {
            text: format_resource_providers(sys.resources.as_ref()),
        }),

        ResolvedCommand::Resources => ResponsePayload::Ok(OkPayload::EvalText {
            text: format_resource_snapshots(sys.resources.as_ref()),
        }),

        ResolvedCommand::ListScopes { limit } => handle_list_scopes_page(sys, limit).await,

        ResolvedCommand::Env { subcommand } => {
            let snapshot = match get_session_snapshot(sys, state, client_id).await {
                Ok(snapshot) => snapshot,
                Err(response) => return response,
            };
            match parse_env_command(subcommand.as_deref()) {
                Ok(EnvCommand::Show) => ResponsePayload::Ok(OkPayload::EvalText {
                    text: format_snapshot_env(&snapshot),
                }),
                Ok(EnvCommand::Set { assignments }) => {
                    let mut set = std::collections::BTreeMap::new();
                    for assignment in assignments {
                        let Some((key, value)) = assignment.split_once('=') else {
                            return ResponsePayload::err(
                                error_code::INVALID_SYNTAX,
                                format!("`:env set` expects KEY=VALUE, got `{assignment}`"),
                            );
                        };
                        if key.is_empty() {
                            return ResponsePayload::err(
                                error_code::INVALID_SYNTAX,
                                "`:env set` requires a non-empty variable name",
                            );
                        }
                        set.insert(key.to_string(), value.to_string());
                    }
                    let delta = cue_core::scope::EnvDelta {
                        set,
                        unset: vec![],
                        cwd: None,
                    };
                    let Some(base) = state.client_scope(client_id) else {
                        return ResponsePayload::err(
                            error_code::INVALID_REQUEST,
                            "client session handshake required",
                        );
                    };
                    match derive_scope(sys, base, delta).await {
                        Ok(hash) => {
                            if let Err(error) =
                                update_client_session_scope(state, client_id, hash, db).await
                            {
                                return ResponsePayload::err(
                                    error_code::INTERNAL,
                                    error.to_string(),
                                );
                            }
                            match get_scope_snapshot_by_hash(sys, hash).await {
                                Ok(updated) => ResponsePayload::Ok(OkPayload::ScopeCreated {
                                    hash: hash.to_string(),
                                    summary: format_scope_change_summary(hash, &snapshot, &updated),
                                }),
                                Err(message) => ResponsePayload::err(error_code::INTERNAL, message),
                            }
                        }
                        Err(message) => ResponsePayload::err(error_code::INTERNAL, message),
                    }
                }
                Ok(EnvCommand::Unset { keys }) => {
                    let delta = cue_core::scope::EnvDelta {
                        set: std::collections::BTreeMap::new(),
                        unset: keys,
                        cwd: None,
                    };
                    let Some(base) = state.client_scope(client_id) else {
                        return ResponsePayload::err(
                            error_code::INVALID_REQUEST,
                            "client session handshake required",
                        );
                    };
                    match derive_scope(sys, base, delta).await {
                        Ok(hash) => {
                            if let Err(error) =
                                update_client_session_scope(state, client_id, hash, db).await
                            {
                                return ResponsePayload::err(
                                    error_code::INTERNAL,
                                    error.to_string(),
                                );
                            }
                            match get_scope_snapshot_by_hash(sys, hash).await {
                                Ok(updated) => ResponsePayload::Ok(OkPayload::ScopeCreated {
                                    hash: hash.to_string(),
                                    summary: format_scope_change_summary(hash, &snapshot, &updated),
                                }),
                                Err(message) => ResponsePayload::err(error_code::INTERNAL, message),
                            }
                        }
                        Err(message) => ResponsePayload::err(error_code::INTERNAL, message),
                    }
                }
                Err(message) => ResponsePayload::err(error_code::INVALID_SYNTAX, message),
            }
        }

        ResolvedCommand::ShowEnv { tail_bytes } => {
            let snapshot = match get_session_snapshot(sys, state, client_id).await {
                Ok(snapshot) => snapshot,
                Err(response) => return response,
            };
            if let Some(response) = invalid_tail_bytes_response("tail_bytes", tail_bytes) {
                return response;
            }
            let text = format_snapshot_env(&snapshot);
            let (text, truncated) = limit_text(text, None, tail_bytes);
            text_output_response(text, truncated)
        }

        ResolvedCommand::Help { topic } => {
            let text = render_help_text(topic.as_deref());
            ResponsePayload::Ok(OkPayload::EvalText { text })
        }

        ResolvedCommand::Clear => ResponsePayload::ack(),

        ResolvedCommand::Quit => ResponsePayload::ack(),

        ResolvedCommand::Wrap { subcommand } => {
            handle_session_bool_default(
                state,
                client_id,
                db,
                subcommand.as_deref(),
                "wrapper",
                config.wrapper.enabled,
                |defaults| &mut defaults.wrapper_enabled,
            )
            .await
        }

        ResolvedCommand::Pty { subcommand } => {
            handle_session_bool_default(
                state,
                client_id,
                db,
                subcommand.as_deref(),
                "pty",
                true,
                |defaults| &mut defaults.pty,
            )
            .await
        }

        ResolvedCommand::Cd { path } => {
            let snapshot = match get_session_snapshot(sys, state, client_id).await {
                Ok(snapshot) => snapshot,
                Err(response) => return response,
            };
            let requested = std::path::PathBuf::from(&path);
            let target = if requested.is_absolute() {
                requested
            } else {
                snapshot.cwd.join(requested)
            };
            let resolved = match std::fs::canonicalize(&target) {
                Ok(path) => path,
                Err(error) => {
                    return ResponsePayload::err(
                        error_code::NOT_FOUND,
                        format!("cannot cd to `{}`: {error}", target.display()),
                    );
                }
            };
            if !resolved.is_dir() {
                return ResponsePayload::err(
                    error_code::INVALID_STATE,
                    format!("cannot cd to `{}`: not a directory", resolved.display()),
                );
            }
            let delta = cue_core::scope::EnvDelta {
                set: std::collections::BTreeMap::new(),
                unset: vec![],
                cwd: Some(resolved.clone()),
            };
            let Some(base) = state.client_scope(client_id) else {
                return ResponsePayload::err(
                    error_code::INVALID_REQUEST,
                    "client session handshake required",
                );
            };
            match derive_scope(sys, base, delta).await {
                Ok(hash) => {
                    if let Err(error) =
                        update_client_session_scope(state, client_id, hash, db).await
                    {
                        return ResponsePayload::err(error_code::INTERNAL, error.to_string());
                    }
                    match get_scope_snapshot_by_hash(sys, hash).await {
                        Ok(updated) => ResponsePayload::Ok(OkPayload::ScopeCreated {
                            hash: hash.to_string(),
                            summary: format_scope_change_summary(hash, &snapshot, &updated),
                        }),
                        Err(message) => ResponsePayload::err(error_code::INTERNAL, message),
                    }
                }
                Err(message) => ResponsePayload::err(error_code::INTERNAL, message),
            }
        }

        // ── :out / :tail / :err → read job output ──
        ResolvedCommand::Out { id, tail_bytes } => {
            let Some(job_id) = parse_job_id(&id) else {
                return ResponsePayload::err(
                    error_code::NOT_FOUND,
                    format!("invalid job id: {id}"),
                );
            };
            if let Some(response) = invalid_tail_bytes_response("tail_bytes", tail_bytes) {
                return response;
            }
            let request_bytes = tail_bytes.unwrap_or(crate::ring_buffer::DEFAULT_CAPACITY);
            read_job_output(sys, job_id, &id, request_bytes).await
        }

        ResolvedCommand::Err { id } => {
            let Some(job_id) = parse_job_id(&id) else {
                return ResponsePayload::err(
                    error_code::NOT_FOUND,
                    format!("invalid job id: {id}"),
                );
            };
            read_job_stderr(sys, job_id, &id, crate::ring_buffer::DEFAULT_CAPACITY).await
        }

        ResolvedCommand::JobOutput {
            id,
            stdout_bytes,
            stderr_bytes,
        } => {
            let Some(job_id) = parse_job_id(&id) else {
                return ResponsePayload::err(
                    error_code::NOT_FOUND,
                    format!("invalid job id: {id}"),
                );
            };
            read_job_output_pair(sys, job_id, &id, stdout_bytes, stderr_bytes).await
        }

        ResolvedCommand::Send { id, data } => {
            if let Some(job_id) = parse_job_id(&id) {
                let (tx, rx) = tokio::sync::oneshot::channel();
                if sys
                    .process_mgr
                    .send(ProcessMgrMsg::SendJobInput {
                        client_id,
                        job_id,
                        data: data.into_bytes(),
                        reply: tx,
                    })
                    .await
                    .is_err()
                {
                    return ResponsePayload::err(error_code::INTERNAL, "process_mgr unreachable");
                }
                match rx.await {
                    Ok(Ok(())) => ResponsePayload::ack(),
                    Ok(Err(message)) => ResponsePayload::err(error_code::INVALID_STATE, message),
                    Err(_) => {
                        ResponsePayload::err(error_code::INTERNAL, "process_mgr reply dropped")
                    }
                }
            } else {
                ResponsePayload::err(error_code::NOT_FOUND, format!("{id} not found"))
            }
        }

        ResolvedCommand::Retry { id } => {
            let Some(job_id) = parse_job_id(&id) else {
                return ResponsePayload::err(
                    error_code::NOT_FOUND,
                    format!("invalid job id: {id}"),
                );
            };
            let Some(entry) = state.jobs.get(&job_id) else {
                return ResponsePayload::err(error_code::NOT_FOUND, format!("job {id} not found"));
            };
            if !entry.status.is_terminal() {
                return ResponsePayload::err(
                    error_code::INVALID_STATE,
                    format!("job {job_id} is not terminal"),
                );
            };
            let Some(start_scope) = entry.start_scope else {
                return ResponsePayload::err(
                    error_code::INVALID_SCOPE,
                    format!("job {job_id} has no recorded start scope"),
                );
            };
            let retry_session_id = entry.session_id.clone();
            let chain = match parse_chain_text(&entry.pipeline_text) {
                Ok(chain) => chain,
                Err(error) => {
                    return ResponsePayload::err(
                        error_code::INTERNAL,
                        format!("cannot reconstruct job pipeline: {error}"),
                    );
                }
            };
            let delay = std::time::Duration::from_millis(500);
            info!(%job_id, ?delay, "scheduler: retrying job with delay");
            tokio::time::sleep(delay).await;
            spawn_chain(
                SpawnChainRequest {
                    chain,
                    scope_hash: start_scope,
                    options: ChainExecutionOptions::retry_default(config),
                    warnings: Vec::new(),
                    retain_completed_chain: false,
                    // A retry is a new execution of the original job, not a
                    // transfer to whichever session happens to issue it.
                    session_id: retry_session_id,
                },
                state,
                SchedulerIo::new(db, sys),
            )
            .await
        }

        ResolvedCommand::Wait { .. } => ResponsePayload::err(
            error_code::INTERNAL,
            "`:wait` should be handled by the scheduler loop",
        ),

        ResolvedCommand::Log { id } => {
            let text = format_log_text(state, id.as_deref(), requester_session_id.as_deref());
            ResponsePayload::Ok(OkPayload::EvalText { text })
        }

        ResolvedCommand::ShowLog {
            id,
            limit,
            tail_bytes,
        } => {
            if let Some(response) = invalid_tail_bytes_response("tail_bytes", tail_bytes) {
                return response;
            }
            let text = format_log_text(state, id.as_deref(), requester_session_id.as_deref());
            let (text, truncated) = limit_text(text, limit, tail_bytes);
            text_output_response(text, truncated)
        }

        ResolvedCommand::Scope { subcommand } => {
            match subcommand.as_deref().map(str::trim).unwrap_or("list") {
                "" | "list" => handle_list_scopes(sys).await,
                other => ResponsePayload::err(
                    error_code::NOT_SUPPORTED,
                    format!("`:scope {other}` is not yet implemented; supported: `:scope list`"),
                ),
            }
        }

        ResolvedCommand::Config { subcommand } => {
            match subcommand.as_deref().map(str::trim).unwrap_or("show") {
                "" | "show" => ResponsePayload::Ok(OkPayload::EvalText {
                    text: format_config_text(config),
                }),
                other => ResponsePayload::err(
                    error_code::NOT_SUPPORTED,
                    format!("`:config {other}` is not supported; try `:config` or `:config show`"),
                ),
            }
        }

        ResolvedCommand::ShowConfig { tail_bytes } => {
            if let Some(response) = invalid_tail_bytes_response("tail_bytes", tail_bytes) {
                return response;
            }
            let text = format_config_text(config);
            let (text, truncated) = limit_text(text, None, tail_bytes);
            text_output_response(text, truncated)
        }
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn script_item_result_from_ok(payload: &OkPayload) -> ScriptItemResult {
    match payload {
        OkPayload::JobCreated {
            job_id,
            start_scope,
            open_hint,
            ..
        } => ScriptItemResult::Job {
            job_id: job_id.clone(),
            start_scope: start_scope.clone(),
            open_hint: *open_hint,
        },
        OkPayload::ChainCreated {
            chain_id,
            job_ids,
            chain,
            ..
        } => ScriptItemResult::Chain {
            chain_id: chain_id.clone(),
            job_ids: job_ids.clone(),
            chain: chain.clone(),
        },
        OkPayload::CronAdded { cron_id } => ScriptItemResult::Cron {
            cron_id: cron_id.clone(),
        },
        OkPayload::EvalText { text } => ScriptItemResult::Message { text: text.clone() },
        OkPayload::Ack {} => ScriptItemResult::Message { text: "ok".into() },
        OkPayload::ScopeCreated { hash, summary } => ScriptItemResult::Message {
            text: format!("{hash}\n{summary}"),
        },
        OkPayload::Output { id, truncated, .. } => ScriptItemResult::Message {
            text: if *truncated {
                format!("opened output snapshot for {id} (truncated)")
            } else {
                format!("opened output snapshot for {id}")
            },
        },
        other => ScriptItemResult::Message {
            text: format!("{other:?}"),
        },
    }
}

fn script_item_end_scope_from_ok(payload: &OkPayload, state: &SchedulerState) -> Option<ScopeHash> {
    match payload {
        OkPayload::JobCreated { job_id, .. } => {
            let job_id = parse_job_id(job_id)?;
            let entry = state.jobs.get(&job_id)?;
            entry
                .status
                .is_terminal()
                .then_some(())
                .and(entry.end_scope.or(entry.start_scope))
        }
        _ => None,
    }
}

fn resolve_command_scope(
    state: &SchedulerState,
    client_id: u64,
    scope_override: Option<ScopeHash>,
) -> Option<ScopeHash> {
    scope_override.or_else(|| state.client_scope(client_id))
}

fn missing_session_response() -> ResponsePayload {
    ResponsePayload::err(
        error_code::INVALID_REQUEST,
        "client session handshake required",
    )
}

async fn create_isolated_script_scope(
    state: &SchedulerState,
    client_id: u64,
    sys: &ActorSystem,
) -> Result<ScopeHash, ResponsePayload> {
    let base = state.client_scope(client_id).ok_or_else(|| {
        ResponsePayload::err(
            error_code::INVALID_REQUEST,
            "client session handshake required",
        )
    })?;
    derive_scope(
        sys,
        base,
        cue_core::scope::EnvDelta {
            set: std::collections::BTreeMap::new(),
            unset: vec![],
            cwd: None,
        },
    )
    .await
    .map_err(|error| ResponsePayload::err(error_code::INTERNAL, error))
}

#[cfg(test)]
async fn ensure_test_session(state: &mut SchedulerState, _sys: &ActorSystem, client_id: u64) {
    if state.session_for_client(client_id).is_some() {
        return;
    }
    let session_id = format!("test-session-{client_id}");
    state.client_sessions.insert(client_id, session_id.clone());
    state.sessions.insert(
        session_id,
        SessionState {
            scope: ScopeHash([0; 32]),
            incarnation: 1,
            defaults: LaunchDefaults::default(),
            connected_clients: 1,
            disconnected_at: None,
            named: None,
        },
    );
}

async fn handle_session_bool_default(
    state: &mut SchedulerState,
    client_id: u64,
    db: &storage::SharedConnection,
    subcommand: Option<&str>,
    name: &str,
    config_default: bool,
    field: impl FnOnce(&mut LaunchDefaults) -> &mut Option<bool>,
) -> ResponsePayload {
    let Some(key) = state.client_sessions.get(&client_id).cloned() else {
        return ResponsePayload::err(
            error_code::INVALID_REQUEST,
            "client session handshake required",
        );
    };
    let Some(session) = state.sessions.get(&key) else {
        return ResponsePayload::err(error_code::INVALID_STATE, "client session unavailable");
    };
    let mut defaults = session.defaults.clone();
    let scope = session.scope;
    let mut meta = session.named.clone();
    let default = field(&mut defaults);
    let response_text = match subcommand.unwrap_or("status") {
        "on" => {
            *default = Some(true);
            format!("{name} enabled for this session")
        }
        "off" => {
            *default = Some(false);
            format!("{name} disabled for this session")
        }
        "" | "status" => {
            let effective = default.unwrap_or(config_default);
            let source = match default {
                Some(true) => "session override: on",
                Some(false) => "session override: off",
                None => "config",
            };
            return ResponsePayload::Ok(OkPayload::EvalText {
                text: format!(
                    "{name} status: {}\n  source: {source}",
                    if effective { "enabled" } else { "disabled" }
                ),
            });
        }
        other => {
            return ResponsePayload::err(
                error_code::INVALID_SYNTAX,
                format!(":{name} {other} — expected 'on', 'off', or 'status'"),
            );
        }
    };

    if let Some(mut named) = meta.take() {
        named.updated_at_ms = unix_time_ms();
        match persist_named_session(db, &named, scope, &defaults).await {
            Ok(durable) => {
                named.scope_durable = durable;
                meta = Some(named);
            }
            Err(error) => {
                return ResponsePayload::err(error_code::INTERNAL, error.to_string());
            }
        }
    }
    let Some(session) = state.sessions.get_mut(&key) else {
        return ResponsePayload::err(
            error_code::INTERNAL,
            "session disappeared while updating defaults",
        );
    };
    session.defaults = defaults;
    session.named = meta;
    ResponsePayload::Ok(OkPayload::EvalText {
        text: response_text,
    })
}

/// Parse a string like `"J5"` into a `JobId`.
fn parse_job_id(s: &str) -> Option<JobId> {
    s.trim().parse().ok()
}

/// Parse a string like `"CH3"` into a `ChainId`.
fn parse_chain_id(s: &str) -> Option<ChainId> {
    s.trim().parse().ok()
}

/// Parse a string like `"R3"` into a `ScriptId`.
fn parse_script_id(s: &str) -> Option<ScriptId> {
    s.trim().parse().ok()
}

/// Parse a string like `"C3"` into a `CronId`.
fn parse_cron_id(s: &str) -> Option<CronId> {
    s.trim().parse().ok()
}

async fn remove_job_logs(job_id: JobId) {
    if let Err(error) = tokio::task::spawn_blocking(move || {
        let dir = match crate::dirs::output_dir() {
            Ok(dir) => dir,
            Err(error) => {
                warn!(%job_id, err = %error, "scheduler: cannot resolve output dir for cleanup");
                return;
            }
        };
        for suffix in [".log", ".stderr"] {
            let path = dir.join(format!("{job_id}{suffix}"));
            if let Err(error) = std::fs::remove_file(&path)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                warn!(%job_id, path = %path.display(), "scheduler: failed to remove output log: {error}");
            }
        }
    })
    .await
    {
        warn!(%job_id, err = %error, "scheduler: output log cleanup task failed");
    }
}

enum EnvCommand {
    Show,
    Set { assignments: Vec<String> },
    Unset { keys: Vec<String> },
}

fn parse_env_command(subcommand: Option<&str>) -> Result<EnvCommand, String> {
    let Some(subcommand) = subcommand.map(str::trim) else {
        return Ok(EnvCommand::Show);
    };
    if subcommand.is_empty() || subcommand == "list" {
        return Ok(EnvCommand::Show);
    }
    let words = tokenize_words(subcommand)?;
    let Some((verb, rest)) = words.split_first() else {
        return Ok(EnvCommand::Show);
    };
    match verb.as_str() {
        "set" => {
            if rest.is_empty() {
                return Err("`:env set` expects at least one KEY=VALUE assignment".into());
            }
            Ok(EnvCommand::Set {
                assignments: rest.to_vec(),
            })
        }
        "unset" => {
            if rest.is_empty() {
                return Err("`:env unset` expects at least one variable name".into());
            }
            if let Some(key) = rest.iter().find(|key| key.is_empty() || key.contains('=')) {
                return Err(format!("`:env unset` expects variable names, got `{key}`"));
            }
            Ok(EnvCommand::Unset {
                keys: rest.to_vec(),
            })
        }
        other => Err(format!("unsupported `:env` subcommand `{other}`")),
    }
}

fn tokenize_words(input: &str) -> Result<Vec<String>, String> {
    let mut words = Vec::new();
    let tokens = Tokenizer::tokenize(input).map_err(|error| error.to_string())?;
    for token in tokens {
        match token.token {
            Token::Word(word) | Token::Command(word) => words.push(word),
            Token::IdRef(kind, n) => words.push(format!("{kind}{n}")),
            Token::Whitespace(_) | Token::Eof => {}
            other => {
                return Err(format!("unsupported token `{other}` in `:env` command"));
            }
        }
    }
    Ok(words)
}

fn format_snapshot_env(snapshot: &EnvSnapshot) -> String {
    let mut lines = vec![format!("cwd={}", snapshot.cwd.display())];
    lines.extend(
        snapshot
            .env
            .iter()
            .map(|(key, value)| format!("{key}={}", value.escape_default())),
    );
    lines.join("\n")
}

fn format_scope_change_summary(
    hash: ScopeHash,
    before: &EnvSnapshot,
    after: &EnvSnapshot,
) -> String {
    let mut lines = vec![hash.to_string()];
    if before.cwd != after.cwd {
        lines.push(format!(
            "cwd: {} -> {}",
            before.cwd.display(),
            after.cwd.display()
        ));
    }

    let mut env_changes = Vec::new();
    for (key, after_value) in &after.env {
        let before_value = before.env.get(key);
        if before_value != Some(after_value) {
            env_changes.push(format!(
                "env: {key}: {} -> {}",
                before_value
                    .map(|value| value.escape_default().to_string())
                    .unwrap_or_else(|| "<unset>".into()),
                after_value.escape_default()
            ));
        }
    }
    for (key, before_value) in &before.env {
        if !after.env.contains_key(key) {
            env_changes.push(format!(
                "env: {key}: {} -> <unset>",
                before_value.escape_default()
            ));
        }
    }
    lines.extend(env_changes);
    if lines.len() == 1 {
        lines.push("no persistent scope changes".into());
    }
    lines.join("\n")
}

fn render_help_text(topic: Option<&str>) -> String {
    match topic
        .map(str::trim)
        .filter(|topic| !topic.is_empty())
        .map(|topic| topic.to_ascii_lowercase())
        .as_deref()
    {
        None => general_help_text(),
        Some(topic) if is_job_help_topic(topic) => job_help_text(),
        Some(topic) if is_cron_help_topic(topic) => cron_help_text(),
        Some(topic) => format!(
            "Unknown help topic `{topic}`.\n\nAvailable help topics: job, cron.\nUse bare `?` to show detailed help for the current mode."
        ),
    }
}

fn is_job_help_topic(topic: &str) -> bool {
    topic == "job"
        || command_spec(topic).is_some_and(|spec| {
            spec.visible_in_category(CommandCategory::Job)
                || spec.visible_in_category(CommandCategory::Scope)
                || spec.visible_in_category(CommandCategory::System)
        })
}

fn is_cron_help_topic(topic: &str) -> bool {
    topic == "cron"
        || command_spec(topic).is_some_and(|spec| spec.visible_in_category(CommandCategory::Cron))
}

fn general_help_text() -> String {
    format!(
        concat!(
            "cue-shell help\n",
            "\n",
            "Modes:\n",
            "- JOB: run shell commands and inspect output / scopes.\n",
            "- CRON: define scheduled commands.\n",
            "\n",
            "Quick tips:\n",
            "- Enter bare `?` to show detailed help for the current mode.\n",
            "- Use `:help job` or `:help cron` for mode-specific help.\n",
            "- Builtins start with `:` and are executed by `cued`.\n",
            "- Modes only change how bare input is interpreted.\n",
            "\n",
            "Builtins:\n",
            "{}"
        ),
        format_command_list(COMMAND_SPECS)
    )
}

fn job_help_text() -> String {
    format!(
        concat!(
            "JOB mode\n",
            "\n",
            "Bare input runs a job using the current scope.\n",
            "Examples:\n",
            "- `cargo test`\n",
            "- `git status -> cargo test`\n",
            "- `cargo test ||| cargo clippy`\n",
            "\n",
            "Useful builtins:\n",
            "{}"
        ),
        format_command_list_by_category(&[
            CommandCategory::Job,
            CommandCategory::Scope,
            CommandCategory::System,
        ])
    )
}

fn cron_help_text() -> String {
    format!(
        concat!(
            "CRON mode\n",
            "\n",
            "Bare input defines a schedule plus command body.\n",
            "Examples:\n",
            "- `every 5m cargo test`\n",
            "- `in 30s echo hello`\n",
            "- `at 09:00 on weekdays cargo check`\n",
            "- `on weekends at 10am backup.sh`\n",
            "- `cron */5 * * * * do curl api/health`\n",
            "\n",
            "Useful builtins:\n",
            "{}"
        ),
        format_command_list_by_category(&[CommandCategory::Cron])
    )
}

fn format_command_list_by_category(categories: &[CommandCategory]) -> String {
    let specs: Vec<&CommandSpec> = COMMAND_SPECS
        .iter()
        .filter(|spec| {
            categories
                .iter()
                .any(|category| spec.visible_in_category(*category))
        })
        .collect();
    format_command_list(specs)
}

fn format_command_list<'a>(specs: impl IntoIterator<Item = &'a CommandSpec>) -> String {
    specs
        .into_iter()
        .map(|spec| format!("- `{}` — {}", spec.usage, spec.detail))
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_resource_providers(registry: &crate::resource::ProviderRegistry) -> String {
    let provider_ids = registry.provider_ids();
    if provider_ids.is_empty() {
        return "no resource providers configured".into();
    }

    let mut keys_by_provider: HashMap<String, Vec<String>> = HashMap::new();
    for (key, provider_id) in registry.key_routes() {
        keys_by_provider
            .entry(provider_id.to_string())
            .or_default()
            .push(key);
    }

    let mut lines = vec![format!(
        "resource providers: {} (active reservations: {})",
        provider_ids.len(),
        registry.active_reservation_count()
    )];
    for provider_id in provider_ids {
        let keys = keys_by_provider
            .remove(provider_id.as_str())
            .filter(|keys| !keys.is_empty())
            .map(|keys| keys.join(", "))
            .unwrap_or_else(|| "<no keys>".into());
        lines.push(format!("- {provider_id}: {keys}"));
    }
    lines.join("\n")
}

fn format_resource_snapshots(registry: &crate::resource::ProviderRegistry) -> String {
    let snapshots = registry.snapshot();
    if snapshots.is_empty() {
        return "no resource providers configured".into();
    }

    let mut lines = Vec::new();
    for (provider_id, snapshot) in snapshots {
        if !lines.is_empty() {
            lines.push(String::new());
        }
        lines.push(format!("provider {provider_id}"));
        if snapshot.units.is_empty() {
            lines.push("  (no units reported)".into());
            continue;
        }
        for unit in snapshot.units {
            if unit.attrs.is_empty() {
                lines.push(format!("  unit {}", unit.id));
                continue;
            }
            let attrs = unit
                .attrs
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join(", ");
            lines.push(format!("  unit {}: {attrs}", unit.id));
        }
    }
    lines.join("\n")
}

fn sorted_job_list(state: &SchedulerState) -> Vec<JobInfo> {
    let mut entries: Vec<&JobEntry> = state.jobs.values().collect();
    entries.sort_by_key(|entry| entry.job_id.0);
    entries.into_iter().map(job_info_from_entry).collect()
}

fn sorted_job_list_for_client(state: &SchedulerState, client_id: u64) -> Vec<JobInfo> {
    let Some(session_id) = state.named_session_id_for_client(client_id) else {
        // Anonymous v2 clients retain the daemon-global compatibility view.
        return sorted_job_list(state);
    };
    let mut entries = state
        .jobs
        .values()
        .filter(|entry| entry.session_id.as_deref() == Some(session_id))
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.job_id.0);
    entries.into_iter().map(job_info_from_entry).collect()
}

fn sorted_cron_list(state: &SchedulerState) -> Vec<CronInfo> {
    let mut entries: Vec<&CronEntry> = state.crons.values().collect();
    entries.sort_by_key(|entry| entry.cron_id.0);
    entries
        .into_iter()
        .map(|cron| CronInfo {
            id: cron.cron_id.to_string(),
            session_id: cron.session_id.clone(),
            schedule: cron.schedule.display(),
            command: cron.chain.to_string(),
            status: cron.status,
        })
        .collect()
}

fn sorted_cron_list_for_client(state: &SchedulerState, client_id: u64) -> Vec<CronInfo> {
    let Some(session_id) = state.named_session_id_for_client(client_id) else {
        return sorted_cron_list(state);
    };
    let mut entries = state
        .crons
        .values()
        .filter(|entry| entry.session_id.as_deref() == Some(session_id))
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.cron_id.0);
    entries
        .into_iter()
        .map(|cron| CronInfo {
            id: cron.cron_id.to_string(),
            session_id: cron.session_id.clone(),
            schedule: cron.schedule.display(),
            command: cron.chain.to_string(),
            status: cron.status,
        })
        .collect()
}

fn page_items<T>(items: Vec<T>, limit: Option<usize>) -> (Vec<T>, PageInfo) {
    let total = items.len();
    let shown = limit.map_or(total, |limit| total.min(limit));
    let truncated = shown < total;
    let page = PageInfo {
        total,
        shown,
        limit,
        truncated,
    };
    (items.into_iter().take(shown).collect(), page)
}

/// Send `ListScopes` to the scope store and return a `ScopeList` response.
async fn handle_list_scopes(sys: &ActorSystem) -> ResponsePayload {
    let (tx, rx) = tokio::sync::oneshot::channel();
    if sys
        .scope_store
        .send(ScopeStoreMsg::ListScopes { reply: tx })
        .await
        .is_err()
    {
        return ResponsePayload::err(error_code::INTERNAL, "scope_store unreachable");
    }
    match rx.await {
        Ok(Ok(scopes)) => ResponsePayload::Ok(OkPayload::ScopeList(scopes)),
        Ok(Err(error)) => ResponsePayload::err(error_code::INTERNAL, error.to_string()),
        Err(_) => ResponsePayload::err(error_code::INTERNAL, "scope_store reply dropped"),
    }
}

async fn handle_list_scopes_page(sys: &ActorSystem, limit: Option<usize>) -> ResponsePayload {
    let (tx, rx) = tokio::sync::oneshot::channel();
    if sys
        .scope_store
        .send(ScopeStoreMsg::ListScopes { reply: tx })
        .await
        .is_err()
    {
        return ResponsePayload::err(error_code::INTERNAL, "scope_store unreachable");
    }
    match rx.await {
        Ok(Ok(scopes)) => {
            let (scopes, page) = page_items(scopes, limit);
            ResponsePayload::Ok(OkPayload::ScopeListPage { scopes, page })
        }
        Ok(Err(error)) => ResponsePayload::err(error_code::INTERNAL, error.to_string()),
        Err(_) => ResponsePayload::err(error_code::INTERNAL, "scope_store reply dropped"),
    }
}

fn limit_text(
    text: String,
    line_limit: Option<usize>,
    tail_bytes: Option<usize>,
) -> (String, bool) {
    let (text, byte_truncated) = if let Some(max) = tail_bytes {
        tail_utf8(&text, max)
    } else {
        (text, false)
    };
    let Some(limit) = line_limit else {
        return (text, byte_truncated);
    };
    let lines: Vec<&str> = text.lines().collect();
    if lines.len() <= limit {
        return (text, byte_truncated);
    }
    let start = lines.len().saturating_sub(limit);
    (lines[start..].join("\n"), true)
}

fn invalid_tail_bytes_response(field: &str, tail_bytes: Option<usize>) -> Option<ResponsePayload> {
    if let Some(bytes) = tail_bytes
        && bytes > MAX_OUTPUT_TAIL_BYTES
    {
        return Some(ResponsePayload::err(
            error_code::INVALID_SYNTAX,
            format!("{field} must be <= {MAX_OUTPUT_TAIL_BYTES} bytes"),
        ));
    }
    None
}

fn tail_utf8(text: &str, max_bytes: usize) -> (String, bool) {
    if max_bytes == 0 {
        return (String::new(), !text.is_empty());
    }
    if text.len() <= max_bytes {
        return (text.to_string(), false);
    }
    let mut start = text.len() - max_bytes;
    while !text.is_char_boundary(start) {
        start += 1;
    }
    (text[start..].to_string(), true)
}

/// Build a human-readable log of jobs and crons.
///
/// If `id` is given, only log for that specific job or cron is shown.
fn format_log_text(
    state: &SchedulerState,
    id: Option<&str>,
    requester_session_id: Option<&str>,
) -> String {
    if let Some(id) = id {
        if let Some(job_id) = parse_job_id(id) {
            return state
                .jobs
                .get(&job_id)
                .map(|entry| {
                    let scope = entry
                        .start_scope
                        .map(|h| h.to_string())
                        .unwrap_or_else(|| "<none>".into());
                    format!(
                        "{}: [{}] {:?} (scope: {scope})",
                        entry.job_id, entry.pipeline_text, entry.status
                    )
                })
                .unwrap_or_else(|| format!("{id}: job not found"));
        }
        if let Some(cron_id) = parse_cron_id(id) {
            return state
                .crons
                .get(&cron_id)
                .map(|entry| {
                    format!(
                        "{}: {} [{:?}]",
                        entry.cron_id,
                        entry.schedule.display(),
                        entry.status
                    )
                })
                .unwrap_or_else(|| format!("{id}: cron not found"));
        }
        return format!("{id}: unrecognised ID (expected J<n> or C<n>)");
    }

    let mut lines = Vec::new();

    let mut jobs: Vec<&JobEntry> = state
        .jobs
        .values()
        .filter(|entry| {
            requester_session_id.is_none() || entry.session_id.as_deref() == requester_session_id
        })
        .collect();
    jobs.sort_by_key(|j| j.job_id.0);
    if jobs.is_empty() {
        lines.push("jobs: none".into());
    } else {
        lines.push("=== Jobs ===".into());
        for entry in jobs {
            lines.push(format!(
                "  {}: [{}] {:?}",
                entry.job_id, entry.pipeline_text, entry.status
            ));
        }
    }

    let mut crons: Vec<&CronEntry> = state
        .crons
        .values()
        .filter(|entry| {
            requester_session_id.is_none() || entry.session_id.as_deref() == requester_session_id
        })
        .collect();
    crons.sort_by_key(|c| c.cron_id.0);
    if crons.is_empty() {
        lines.push("crons: none".into());
    } else {
        lines.push("=== Crons ===".into());
        for entry in crons {
            lines.push(format!(
                "  {}: {} [{:?}]",
                entry.cron_id,
                entry.schedule.display(),
                entry.status
            ));
        }
    }

    lines.join("\n")
}

/// Format the active daemon config as human-readable text.
fn format_config_text(config: &Config) -> String {
    let mut lines = Vec::new();
    lines.push(format!(
        "retention.max_job_history = {}",
        config.retention.max_job_history
    ));
    lines.push(format!(
        "retention.max_script_runs = {}",
        config.retention.max_script_runs
    ));
    lines.push(format!(
        "resources.nvidia.enabled = {}",
        config.resources.nvidia.enabled
    ));
    lines.push(format!("wrapper.enabled = {}", config.wrapper.enabled));
    lines.push(format!("wrapper.binary = {:?}", config.wrapper.binary));
    lines.push(format!(
        "wrapper.allowlist.commands = [{}]",
        config.wrapper.allowlist.commands.join(", ")
    ));
    lines.push(format!(
        "sandbox.default_upper_root = {}",
        config.sandbox.default_upper_root.display()
    ));
    lines.push(format!(
        "sandbox.min_free_ratio = {}",
        config.sandbox.min_free_ratio
    ));
    lines.join("\n")
}

fn encode_output(data: Vec<u8>, truncated: bool) -> StreamText {
    match String::from_utf8(data) {
        Ok(data) => StreamText {
            data,
            truncated,
            encoding: OutputEncoding::Utf8,
            base64: None,
        },
        Err(error) => {
            let bytes = error.into_bytes();
            StreamText {
                data: String::from_utf8_lossy(&bytes).into_owned(),
                truncated,
                encoding: OutputEncoding::Base64,
                base64: Some(BASE64_STANDARD.encode(bytes)),
            }
        }
    }
}

fn output_response(id: String, data: Vec<u8>, truncated: bool) -> ResponsePayload {
    let output = encode_output(data, truncated);
    ResponsePayload::Ok(OkPayload::Output {
        id,
        data: output.data,
        truncated: output.truncated,
        encoding: output.encoding,
        base64: output.base64,
    })
}

fn text_output_response(text: String, truncated: bool) -> ResponsePayload {
    ResponsePayload::Ok(OkPayload::TextOutput {
        text,
        truncated,
        encoding: OutputEncoding::Utf8,
        base64: None,
    })
}

async fn read_job_output(
    sys: &ActorSystem,
    job_id: JobId,
    display_id: &str,
    tail_bytes: usize,
) -> ResponsePayload {
    let id = display_id.to_owned();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let sent = sys
        .process_mgr
        .send(ProcessMgrMsg::GetOutput {
            job_id,
            tail_bytes,
            reply: tx,
        })
        .await;
    if sent.is_err() {
        return ResponsePayload::err(error_code::INTERNAL, "process_mgr unreachable");
    }

    match rx.await {
        Ok(Some(snapshot)) => output_response(id, snapshot.data, snapshot.truncated),
        Ok(None) => read_output_from_log(job_id, &id, tail_bytes).await,
        Err(_) => ResponsePayload::err(error_code::INTERNAL, "process_mgr reply dropped"),
    }
}

/// Fall back to reading a completed job's log file from disk.
///
/// The log lives at `<output_dir>/J<n>.log`.  File I/O is offloaded to the
/// blocking thread-pool so the async runtime is not stalled.
async fn read_output_from_log(
    job_id: JobId,
    display_id: &str,
    tail_bytes: usize,
) -> ResponsePayload {
    let id = display_id.to_owned();
    let output_dir = match crate::dirs::output_dir() {
        Ok(dir) => dir,
        Err(error) => {
            return ResponsePayload::err(
                error_code::INTERNAL,
                format!("resolve output directory: {error:#}"),
            );
        }
    };
    match tokio::task::spawn_blocking(move || {
        let path = output_dir.join(format!("{job_id}.log"));
        read_log_tail(path, tail_bytes)
    })
    .await
    {
        Ok(result) => output_from_log_result(id, result),
        Err(_) => ResponsePayload::err(error_code::INTERNAL, "blocking task panicked"),
    }
}

async fn read_job_output_pair(
    sys: &ActorSystem,
    job_id: JobId,
    display_id: &str,
    stdout_bytes: Option<usize>,
    stderr_bytes: Option<usize>,
) -> ResponsePayload {
    if let Some(response) = invalid_tail_bytes_response("stdout_bytes", stdout_bytes) {
        return response;
    }
    if let Some(response) = invalid_tail_bytes_response("stderr_bytes", stderr_bytes) {
        return response;
    }
    let stdout_limit = stdout_bytes.unwrap_or(crate::ring_buffer::DEFAULT_CAPACITY);
    let stderr_limit = stderr_bytes.unwrap_or(crate::ring_buffer::DEFAULT_CAPACITY);
    let stdout = match read_job_output(sys, job_id, display_id, stdout_limit).await {
        ResponsePayload::Ok(OkPayload::Output {
            data,
            truncated,
            encoding,
            base64,
            ..
        }) => StreamText {
            data,
            truncated,
            encoding,
            base64,
        },
        error => return error,
    };
    let stderr = match read_job_stderr(sys, job_id, display_id, stderr_limit).await {
        ResponsePayload::Ok(OkPayload::Output {
            data,
            truncated,
            encoding,
            base64,
            ..
        }) => StreamText {
            data,
            truncated,
            encoding,
            base64,
        },
        error => return error,
    };
    let stderr_pty_merged = stderr
        .data
        .starts_with("[PTY: stdout and stderr are merged]");
    ResponsePayload::Ok(OkPayload::JobOutput {
        id: display_id.to_string(),
        stdout,
        stderr,
        stderr_pty_merged,
    })
}

/// Return stderr for a job — real pipe-mode bytes, or merged PTY output with a notice.
async fn read_job_stderr(
    sys: &ActorSystem,
    job_id: JobId,
    display_id: &str,
    tail_bytes: usize,
) -> ResponsePayload {
    let id = display_id.to_owned();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let sent = sys
        .process_mgr
        .send(ProcessMgrMsg::GetStderr {
            job_id,
            tail_bytes,
            reply: tx,
        })
        .await;
    if sent.is_err() {
        return ResponsePayload::err(error_code::INTERNAL, "process_mgr unreachable");
    }

    match rx.await {
        // Live pipe-mode job: return real stderr.
        Ok(Some(StderrSnapshot {
            pty_merged: false,
            data,
            truncated,
        })) => output_response(id, data, truncated),
        // Live PTY job: streams are merged — fall back to combined log with notice.
        Ok(Some(StderrSnapshot {
            pty_merged: true, ..
        })) => prepend_pty_notice(read_job_output(sys, job_id, &id, tail_bytes).await),
        // Job not in live map (completed) — try dedicated stderr log, then combined log.
        Ok(None) => read_stderr_from_log(job_id, &id, tail_bytes).await,
        Err(_) => ResponsePayload::err(error_code::INTERNAL, "process_mgr reply dropped"),
    }
}

/// Prepend a PTY-merged notice to an `Output` response.
fn prepend_pty_notice(mut resp: ResponsePayload) -> ResponsePayload {
    if let ResponsePayload::Ok(OkPayload::Output {
        ref mut data,
        ref mut encoding,
        ref mut base64,
        ..
    }) = resp
    {
        const NOTICE: &[u8] = b"[PTY: stdout and stderr are merged]\n";
        if *encoding == OutputEncoding::Base64 {
            let mut bytes = NOTICE.to_vec();
            if let Some(encoded) = base64.as_deref()
                && let Ok(decoded) = BASE64_STANDARD.decode(encoded)
            {
                bytes.extend_from_slice(&decoded);
            }
            *data = String::from_utf8_lossy(&bytes).into_owned();
            *base64 = Some(BASE64_STANDARD.encode(bytes));
        } else {
            *data = format!("{}{}", String::from_utf8_lossy(NOTICE), data);
        }
    }
    resp
}

/// Read stderr for a completed job from disk.
///
/// Checks `<output_dir>/J<n>.stderr` first (pipe-mode jobs), then falls back
/// to `<output_dir>/J<n>.log` (PTY-mode combined output) with a notice.
async fn read_stderr_from_log(
    job_id: JobId,
    display_id: &str,
    tail_bytes: usize,
) -> ResponsePayload {
    let id = display_id.to_owned();
    let output_dir = match crate::dirs::output_dir() {
        Ok(dir) => dir,
        Err(error) => {
            return ResponsePayload::err(
                error_code::INTERNAL,
                format!("resolve output directory: {error:#}"),
            );
        }
    };

    // Try the dedicated stderr log (pipe-mode jobs).
    let stderr_dir = output_dir.clone();
    let stderr_data = tokio::task::spawn_blocking(move || {
        let path = stderr_dir.join(format!("{job_id}.stderr"));
        read_log_tail(path, tail_bytes)
    })
    .await;
    match stderr_data {
        Ok(Ok(LogTail { data, truncated })) => return output_response(id, data, truncated),
        Ok(Err(error)) if error.kind() == io::ErrorKind::NotFound => {}
        Ok(Err(error)) => return output_log_error_response(&id, error),
        Err(error) => {
            return ResponsePayload::err(
                error_code::INTERNAL,
                format!("stderr log read task failed: {error}"),
            );
        }
    }

    // No dedicated stderr — return combined PTY log with notice.
    let id2 = id.clone();
    match tokio::task::spawn_blocking(move || {
        let path = output_dir.join(format!("{job_id}.log"));
        read_log_tail(path, tail_bytes)
    })
    .await
    {
        Ok(Ok(LogTail { data, truncated })) => {
            prepend_pty_notice(output_response(id2, data, truncated))
        }
        Ok(Err(error)) if error.kind() == io::ErrorKind::NotFound => {
            ResponsePayload::err(error_code::NOT_FOUND, format!("no output found for {id}"))
        }
        Ok(Err(error)) => output_log_error_response(&id, error),
        Err(_) => ResponsePayload::err(error_code::INTERNAL, "blocking task panicked"),
    }
}

struct LogTail {
    data: Vec<u8>,
    truncated: bool,
}

fn read_log_tail(path: std::path::PathBuf, tail_bytes: usize) -> io::Result<LogTail> {
    let mut file = std::fs::File::open(path)?;
    let len = file.metadata()?.len();
    if tail_bytes == 0 {
        return Ok(LogTail {
            data: Vec::new(),
            truncated: len > 0,
        });
    }

    let read_len = len.min(tail_bytes as u64);
    let truncated = len > read_len;
    file.seek(SeekFrom::Start(len - read_len))?;

    let mut data = Vec::with_capacity(read_len as usize);
    file.take(read_len).read_to_end(&mut data)?;
    Ok(LogTail { data, truncated })
}

fn output_from_log_result(id: String, result: io::Result<LogTail>) -> ResponsePayload {
    match result {
        Ok(LogTail { data, truncated }) => output_response(id, data, truncated),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            ResponsePayload::err(error_code::NOT_FOUND, format!("no output found for {id}"))
        }
        Err(error) => output_log_error_response(&id, error),
    }
}

fn output_log_error_response(id: &str, error: io::Error) -> ResponsePayload {
    ResponsePayload::err(
        error_code::INTERNAL,
        format!("read job log for {id}: {error}"),
    )
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::super::EventBusMsg;
    use super::*;
    use cue_core::ipc::ScriptSource;
    use cue_core::pipeline::{JobPlan, PipeSegment, Pipeline};
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    /// Helper: build a simple leaf from a command string.
    fn leaf(cmd: &str) -> ChainNode {
        ChainNode::Leaf(JobPlan::Pipeline(Pipeline {
            segments: vec![PipeSegment {
                command: cmd.split_whitespace().map(String::from).collect(),
                pipe_to_next: None,
            }],
        }))
    }

    type TestActorSystem = (
        ActorSystem,
        mpsc::Receiver<GatewayMsg>,
        mpsc::Receiver<SchedulerMsg>,
        mpsc::Receiver<ProcessMgrMsg>,
        mpsc::Receiver<ScopeStoreMsg>,
        mpsc::Receiver<super::super::EventBusMsg>,
    );

    /// Create an `ActorSystem` wired to test receivers.
    fn test_actor_system() -> TestActorSystem {
        test_actor_system_with_resources(std::sync::Arc::new(
            crate::resource::ProviderRegistry::empty(),
        ))
    }

    fn test_actor_system_with_resources(
        resources: std::sync::Arc<crate::resource::ProviderRegistry>,
    ) -> TestActorSystem {
        let (gw_tx, gw_rx) = mpsc::channel(64);
        let (sched_tx, sched_rx) = mpsc::channel(64);
        let (pm_tx, pm_rx) = mpsc::channel(64);
        let (ss_tx, ss_rx) = mpsc::channel(64);
        let (eb_tx, eb_rx) = mpsc::channel(64);
        let sys = ActorSystem {
            gateway: gw_tx,
            scheduler: sched_tx,
            process_mgr: pm_tx,
            scope_store: ss_tx,
            event_bus: eb_tx,
            config: crate::config::Config::default(),
            resources,
        };
        (sys, gw_rx, sched_rx, pm_rx, ss_rx, eb_rx)
    }

    fn test_db() -> Arc<Mutex<Connection>> {
        Arc::new(Mutex::new(
            storage::open_db(Path::new(":memory:")).expect("open test db"),
        ))
    }

    fn insert_anonymous_test_client(state: &mut SchedulerState, client_id: u64, scope: ScopeHash) {
        let key = ephemeral_session_key(&format!("test-{client_id}"));
        state.sessions.insert(
            key.clone(),
            SessionState {
                scope,
                incarnation: client_id,
                defaults: LaunchDefaults::default(),
                connected_clients: 1,
                disconnected_at: None,
                named: None,
            },
        );
        state.client_sessions.insert(client_id, key);
    }

    fn insert_ready_named_test_session(
        conn: &Arc<Mutex<Connection>>,
        state: &mut SchedulerState,
        id: &str,
        name: &str,
        scope: ScopeHash,
        connected_clients: usize,
    ) {
        storage::upsert_session(
            &conn.lock().unwrap(),
            &storage::StoredSession {
                id: id.into(),
                name: name.into(),
                scope_hash: Some(scope),
                pty_default: None,
                wrapper_enabled: None,
                created_at_ms: 1,
                updated_at_ms: 1,
                archived_at_ms: None,
            },
        )
        .expect("persist named session fixture");
        state.sessions.insert(
            named_session_key(id),
            SessionState {
                scope,
                incarnation: 1,
                defaults: LaunchDefaults::default(),
                connected_clients,
                disconnected_at: None,
                named: Some(NamedSessionMeta {
                    id: id.into(),
                    name: name.into(),
                    scope_durable: true,
                    created_at_ms: 1,
                    updated_at_ms: 1,
                    archived_at_ms: None,
                }),
            },
        );
    }

    async fn archive_test_session(
        state: &mut SchedulerState,
        conn: &storage::SharedConnection,
        client_id: u64,
        selector: &str,
    ) -> SessionCommandResult {
        handle_session_command(
            client_id,
            SessionCommand::Archive {
                selector: selector.into(),
            },
            state,
            conn,
        )
        .await
    }

    fn insert_test_scope(conn: &Arc<Mutex<Connection>>, name: &str) -> ScopeHash {
        let scope = Scope::root(EnvSnapshot {
            env: std::collections::BTreeMap::from([("PATH".into(), "/usr/bin".into())]),
            cwd: std::path::PathBuf::from(format!("/tmp/cue-scheduler-{name}")),
        });
        assert_eq!(
            storage::insert_scope(&conn.lock().unwrap(), &scope).expect("insert test scope"),
            storage::ScopePersistence::Persisted
        );
        scope.hash
    }

    fn test_runtime<'a>(
        conn: &'a Arc<Mutex<Connection>>,
        config: &'a Config,
        sys: &'a ActorSystem,
    ) -> SchedulerRuntime<'a> {
        SchedulerRuntime::new(conn, config, sys)
    }

    fn test_chain_spawn(chain: ChainNode, scope_hash: ScopeHash) -> SpawnChainRequest {
        test_chain_spawn_with_options(
            chain,
            scope_hash,
            ChainExecutionOptions {
                process: ProcessJobContext {
                    cwd_override: None,
                    launch: LaunchOptions::default(),
                    wrapper_enabled: false,
                    pty_default: true,
                    direct_output_client: None,
                },
                scope_enabled: false,
            },
        )
    }

    fn test_scope_chain_spawn(chain: ChainNode, scope_hash: ScopeHash) -> SpawnChainRequest {
        test_chain_spawn_with_options(
            chain,
            scope_hash,
            ChainExecutionOptions {
                process: ProcessJobContext {
                    cwd_override: None,
                    launch: LaunchOptions::default(),
                    wrapper_enabled: false,
                    pty_default: true,
                    direct_output_client: None,
                },
                scope_enabled: true,
            },
        )
    }

    fn test_chain_spawn_with_options(
        chain: ChainNode,
        scope_hash: ScopeHash,
        options: ChainExecutionOptions,
    ) -> SpawnChainRequest {
        SpawnChainRequest {
            chain,
            scope_hash,
            options,
            warnings: Vec::new(),
            retain_completed_chain: false,
            session_id: None,
        }
    }

    fn drop_crons_table(conn: &Arc<Mutex<Connection>>) {
        conn.lock()
            .unwrap()
            .execute_batch("DROP TABLE crons;")
            .expect("drop crons table");
    }

    fn drop_jobs_history_table(conn: &Arc<Mutex<Connection>>) {
        conn.lock()
            .unwrap()
            .execute_batch("DROP TABLE jobs_history;")
            .expect("drop jobs_history table");
    }

    fn drop_script_items_table(conn: &Arc<Mutex<Connection>>) {
        conn.lock()
            .unwrap()
            .execute_batch("DROP TABLE script_items;")
            .expect("drop script_items table");
    }

    fn persisted_script_state(
        conn: &Arc<Mutex<Connection>>,
        script_id: &str,
    ) -> (String, Option<i32>, Option<i64>, Option<String>) {
        conn.lock()
            .unwrap()
            .query_row(
                "SELECT status, exit_code, failed_item_index, finished_at
                 FROM script_runs WHERE id = ?1",
                rusqlite::params![script_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .expect("script run exists")
    }

    fn persisted_script_ids(conn: &Arc<Mutex<Connection>>) -> Vec<String> {
        let guard = conn.lock().unwrap();
        let mut stmt = guard
            .prepare("SELECT id FROM script_runs ORDER BY id")
            .expect("prepare script id query");
        stmt.query_map([], |row| row.get::<_, String>(0))
            .expect("query script ids")
            .map(|row| row.expect("read script id"))
            .collect()
    }

    /// Spawn a fake scope_store that preserves derived snapshots.
    fn spawn_fake_scope_store(mut rx: mpsc::Receiver<ScopeStoreMsg>) {
        tokio::spawn(async move {
            let root_snapshot = cue_core::scope::EnvSnapshot {
                env: std::collections::BTreeMap::new(),
                cwd: std::env::current_dir().expect("current dir"),
            };
            let root = cue_core::scope::Scope::root(root_snapshot.clone());
            let root_hash = root.hash;
            let mut scopes = HashMap::from([(root_hash, root)]);

            while let Some(msg) = rx.recv().await {
                match msg {
                    ScopeStoreMsg::Insert { scope, reply } => {
                        let hash = scope.hash;
                        scopes.insert(hash, scope);
                        let _ = reply.send(Ok(hash));
                    }
                    ScopeStoreMsg::GetScope { hash, reply } => {
                        let scope =
                            scopes
                                .get(&hash)
                                .cloned()
                                .unwrap_or_else(|| cue_core::scope::Scope {
                                    hash,
                                    parent: None,
                                    delta: None,
                                    snapshot: Some(root_snapshot.clone()),
                                });
                        let _ = reply.send(Ok(Some(scope)));
                    }
                    ScopeStoreMsg::Derive { base, delta, reply } => {
                        let parent_snapshot = scopes
                            .get(&base)
                            .and_then(|scope| scope.snapshot.as_ref())
                            .cloned()
                            .unwrap_or_else(|| root_snapshot.clone());
                        let child = cue_core::scope::Scope::fork(base, &parent_snapshot, delta);
                        let hash = child.hash;
                        scopes.insert(hash, child);
                        let _ = reply.send(Ok(hash));
                    }
                    ScopeStoreMsg::GarbageCollect { roots, reply } => {
                        let mut reachable = HashSet::new();
                        let mut pending = roots.into_iter().collect::<Vec<_>>();
                        while let Some(hash) = pending.pop() {
                            if !reachable.insert(hash) {
                                continue;
                            }
                            if let Some(parent) = scopes.get(&hash).and_then(|scope| scope.parent) {
                                pending.push(parent);
                            }
                        }
                        let before = scopes.len();
                        scopes.retain(|hash, _| reachable.contains(hash));
                        let _ = reply.send(Ok(super::super::ScopeGcReport {
                            retained: reachable.len(),
                            removed_cached: before.saturating_sub(scopes.len()),
                            removed_persisted: 0,
                        }));
                    }
                    ScopeStoreMsg::ListScopes { reply } => {
                        let mut infos = scopes
                            .values()
                            .map(|scope| {
                                let snapshot =
                                    scope.snapshot.as_ref().expect("test scope snapshot");
                                cue_core::ipc::ScopeInfo {
                                    hash: scope.hash.to_string(),
                                    parent: scope.parent.map(|hash| hash.to_string()),
                                    cwd: snapshot.cwd.display().to_string(),
                                    env_count: snapshot.env.len(),
                                }
                            })
                            .collect::<Vec<_>>();
                        if infos.len() == 1 {
                            infos.push(cue_core::ipc::ScopeInfo {
                                hash: ScopeHash([1u8; 32]).to_string(),
                                parent: None,
                                cwd: root_snapshot.cwd.display().to_string(),
                                env_count: 0,
                            });
                        }
                        let _ = reply.send(Ok(infos));
                    }
                    ScopeStoreMsg::Shutdown => break,
                }
            }
        });
    }

    async fn bind_test_session(state: &mut SchedulerState, sys: &ActorSystem, client_id: u64) {
        let snapshot = cue_core::scope::EnvSnapshot {
            env: std::collections::BTreeMap::new(),
            cwd: std::env::current_dir().expect("current dir"),
        };
        connect_session(
            client_id,
            format!("test-session-{client_id}"),
            snapshot,
            false,
            state,
            sys,
        )
        .await
        .expect("bind test session");
    }

    fn test_handshake_snapshot() -> cue_core::scope::EnvSnapshot {
        cue_core::scope::EnvSnapshot {
            env: std::collections::BTreeMap::new(),
            cwd: std::env::current_dir().expect("current dir"),
        }
    }

    fn test_handshake_snapshot_with_env(pairs: &[(&str, &str)]) -> cue_core::scope::EnvSnapshot {
        cue_core::scope::EnvSnapshot {
            env: pairs
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect(),
            cwd: std::env::current_dir().expect("current dir"),
        }
    }

    #[test]
    fn scope_gc_roots_cover_live_and_retained_scheduler_state() {
        let hashes = (1u8..=8)
            .map(|byte| ScopeHash([byte; 32]))
            .collect::<Vec<_>>();
        let mut state = SchedulerState::new();
        state.sessions.insert(
            "session".into(),
            SessionState {
                scope: hashes[0],
                incarnation: 1,
                defaults: LaunchDefaults::default(),
                connected_clients: 1,
                disconnected_at: None,
                named: None,
            },
        );
        state.chains.insert(
            ChainId(1),
            ChainState {
                node: leaf("echo chain"),
                leaf_jobs: HashMap::new(),
                leaf_status: HashMap::new(),
                scope_hash: hashes[1],
                pipeline_text: "echo chain".into(),
                process: ProcessJobContext {
                    cwd_override: None,
                    launch: LaunchOptions::default(),
                    wrapper_enabled: false,
                    pty_default: false,
                    direct_output_client: None,
                },
                scope_enabled: false,
                session_id: None,
            },
        );
        state.jobs.insert(
            JobId(1),
            JobEntry {
                job_id: JobId(1),
                session_id: None,
                pipeline_text: "echo retained".into(),
                status: JobStatus::Done,
                exit_code: Some(0),
                start_scope: Some(hashes[2]),
                end_scope: Some(hashes[3]),
                open_hint: JobOpenHint::Stream,
                chain_id: None,
                chain_index: None,
                chain_total: None,
                pending_reason: None,
            },
        );
        state.crons.insert(
            CronId(1),
            CronEntry {
                cron_id: CronId(1),
                schedule: parse_schedule_text("every 1m").expect("valid schedule"),
                chain: leaf("echo cron"),
                scope_hash: hashes[4],
                status: CronStatus::Paused,
                next_trigger: Instant::now(),
                cwd_override: None,
                scope_enabled: false,
                wrapper_enabled: false,
                session_id: None,
            },
        );
        state.pending_scripts.insert(
            ScriptId(1),
            PendingScriptRun {
                client_id: 1,
                script_id: ScriptId(1),
                mode: Mode::Job,
                source: ScriptSource::Inline,
                items: VecDeque::new(),
                next_index: 0,
                item_scope: hashes[5],
                created_items: Vec::new(),
                last_exit_code: 0,
                waiting_index: None,
                session_id: None,
            },
        );
        state.completed_chains.insert(
            ChainId(2),
            ChainCompletion {
                exit_code: 0,
                end_scope: Some(hashes[6]),
            },
        );
        state.pending_resource.insert(
            JobId(2),
            PendingResourceAdmission {
                plan: match leaf("echo resource") {
                    ChainNode::Leaf(plan) => plan,
                    _ => unreachable!("leaf helper returns a leaf"),
                },
                base_scope: hashes[7],
                options: ProcessJobOptions {
                    cwd_override: None,
                    sandbox: None,
                    wrapper_enabled: false,
                    pty_enabled: false,
                    direct_output_client: None,
                    session_id: None,
                },
                needs: Need::new(),
            },
        );

        assert_eq!(scope_gc_roots(&state), hashes.into_iter().collect());
    }

    #[test]
    fn session_ttl_sweep_reports_removed_roots() {
        let mut state = SchedulerState::new();
        state.sessions.insert(
            "expired".into(),
            SessionState {
                scope: ScopeHash([1; 32]),
                incarnation: 1,
                defaults: LaunchDefaults::default(),
                connected_clients: 0,
                disconnected_at: Instant::now().checked_sub(SESSION_GC_TTL),
                named: None,
            },
        );
        state.sessions.insert(
            "connected".into(),
            SessionState {
                scope: ScopeHash([2; 32]),
                incarnation: 2,
                defaults: LaunchDefaults::default(),
                connected_clients: 1,
                disconnected_at: None,
                named: None,
            },
        );

        assert_eq!(sweep_disconnected_sessions(&mut state), 1);
        assert!(!state.sessions.contains_key("expired"));
        assert!(state.sessions.contains_key("connected"));
    }

    #[tokio::test]
    async fn failed_initial_session_connect_does_not_record_client_mapping() {
        let (sys, _gw_rx, _sched_rx, _pm_rx, ss_rx, _eb_rx) = test_actor_system();
        drop(ss_rx);
        let mut state = SchedulerState::new();

        let error = connect_session(
            7,
            "failed-session".into(),
            test_handshake_snapshot(),
            false,
            &mut state,
            &sys,
        )
        .await
        .expect_err("scope store should be unreachable");

        assert!(error.to_string().contains("scope_store unreachable"));
        assert!(!state.client_sessions.contains_key(&7));
        assert!(!state.sessions.contains_key("failed-session"));
        assert!(state.session_for_client(7).is_none());
    }

    #[tokio::test]
    async fn failed_session_switch_preserves_existing_client_binding() {
        let (sys, _gw_rx, _sched_rx, _pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);
        let mut state = SchedulerState::new();
        connect_session(
            7,
            "old-session".into(),
            test_handshake_snapshot(),
            false,
            &mut state,
            &sys,
        )
        .await
        .expect("initial session connects");
        let old_scope = state.client_scope(7).expect("old session scope");

        let (broken_sys, _gw_rx, _sched_rx, _pm_rx, broken_ss_rx, _eb_rx) = test_actor_system();
        drop(broken_ss_rx);
        let error = connect_session(
            7,
            "new-session".into(),
            test_handshake_snapshot(),
            false,
            &mut state,
            &broken_sys,
        )
        .await
        .expect_err("new session scope insert should fail");

        assert!(error.to_string().contains("scope_store unreachable"));
        assert_eq!(
            state.client_sessions.get(&7).map(String::as_str),
            Some(ephemeral_session_key("old-session").as_str())
        );
        assert_eq!(state.client_scope(7), Some(old_scope));
        assert_eq!(
            state.sessions[&ephemeral_session_key("old-session")].connected_clients,
            1
        );
        assert!(
            state.sessions[&ephemeral_session_key("old-session")]
                .disconnected_at
                .is_none()
        );
        assert!(!state.sessions.contains_key("new-session"));
    }

    #[tokio::test]
    async fn reconnect_without_refresh_keeps_existing_session_scope() {
        let (sys, _gw_rx, _sched_rx, _pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);
        let mut state = SchedulerState::new();
        let initial = connect_session(
            7,
            "sticky-session".into(),
            test_handshake_snapshot_with_env(&[("NODE_VERSION", "24")]),
            false,
            &mut state,
            &sys,
        )
        .await
        .expect("initial session connects");

        let reconnected = connect_session(
            7,
            "sticky-session".into(),
            test_handshake_snapshot_with_env(&[("NODE_VERSION", "26")]),
            false,
            &mut state,
            &sys,
        )
        .await
        .expect("ordinary reconnect reuses session");

        assert_eq!(reconnected, initial);
        let snapshot = get_scope_snapshot_by_hash(&sys, reconnected.scope)
            .await
            .expect("scope snapshot");
        assert_eq!(
            snapshot.env.get("NODE_VERSION").map(String::as_str),
            Some("24")
        );
        assert_eq!(
            state.sessions[&ephemeral_session_key("sticky-session")].connected_clients,
            1
        );
    }

    #[tokio::test]
    async fn session_id_gets_new_incarnation_after_ttl_gc() {
        let (sys, _gw_rx, _sched_rx, _pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);
        let mut state = SchedulerState::new();
        let first = connect_session(
            7,
            "reused-session".into(),
            test_handshake_snapshot(),
            false,
            &mut state,
            &sys,
        )
        .await
        .expect("first incarnation");
        disconnect_session(7, &mut state);
        state
            .sessions
            .get_mut(&ephemeral_session_key("reused-session"))
            .unwrap()
            .disconnected_at = Instant::now().checked_sub(SESSION_GC_TTL);
        assert_eq!(sweep_disconnected_sessions(&mut state), 1);

        let second = connect_session(
            8,
            "reused-session".into(),
            test_handshake_snapshot(),
            false,
            &mut state,
            &sys,
        )
        .await
        .expect("second incarnation");
        assert_ne!(first.incarnation, second.incarnation);
    }

    #[tokio::test]
    async fn session_incarnation_exhaustion_fails_closed() {
        let (sys, _gw_rx, _sched_rx, _pm_rx, _ss_rx, _eb_rx) = test_actor_system();
        let mut state = SchedulerState::new();
        state.next_session_incarnation = u64::MAX;

        let error = connect_session(
            7,
            "exhausted-session".into(),
            test_handshake_snapshot(),
            false,
            &mut state,
            &sys,
        )
        .await
        .expect_err("incarnation reuse must fail closed");
        assert!(error.to_string().contains("incarnation space exhausted"));
        assert!(!state.sessions.contains_key("exhausted-session"));
    }

    #[tokio::test]
    async fn explicit_refresh_replaces_existing_session_scope() {
        let (sys, _gw_rx, _sched_rx, _pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);
        let mut state = SchedulerState::new();
        let initial = connect_session(
            7,
            "refreshable-session".into(),
            test_handshake_snapshot_with_env(&[("NODE_VERSION", "24")]),
            false,
            &mut state,
            &sys,
        )
        .await
        .expect("initial session connects");

        let refreshed = connect_session(
            7,
            "refreshable-session".into(),
            test_handshake_snapshot_with_env(&[("NODE_VERSION", "26")]),
            true,
            &mut state,
            &sys,
        )
        .await
        .expect("explicit refresh replaces session scope");

        assert_ne!(refreshed, initial);
        assert_eq!(state.client_scope(7), Some(refreshed.scope));
        assert_eq!(
            state.sessions[&ephemeral_session_key("refreshable-session")].connected_clients,
            1
        );
        let snapshot = get_scope_snapshot_by_hash(&sys, refreshed.scope)
            .await
            .expect("refreshed scope snapshot");
        assert_eq!(
            snapshot.env.get("NODE_VERSION").map(String::as_str),
            Some("26")
        );
    }

    #[tokio::test]
    async fn env_unset_moves_session_cursor_and_future_scope() {
        let (sys, _gw_rx, _sched_rx, _pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);
        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();
        connect_session(
            7,
            "env-session".into(),
            test_handshake_snapshot_with_env(&[("REMOVE_ME", "1"), ("KEEP_ME", "yes")]),
            false,
            &mut state,
            &sys,
        )
        .await
        .expect("initial session connects");

        let response = handle_command(
            ResolvedCommand::Env {
                subcommand: Some("unset REMOVE_ME".into()),
            },
            7,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        assert!(matches!(
            response,
            ResponsePayload::Ok(OkPayload::ScopeCreated { .. })
        ));
        let scope = state.client_scope(7).expect("session scope after unset");
        let snapshot = get_scope_snapshot_by_hash(&sys, scope)
            .await
            .expect("unset scope snapshot");
        assert!(!snapshot.env.contains_key("REMOVE_ME"));
        assert_eq!(snapshot.env.get("KEEP_ME").map(String::as_str), Some("yes"));
    }

    fn spawn_fake_process_mgr(mut rx: mpsc::Receiver<ProcessMgrMsg>) {
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                match msg {
                    ProcessMgrMsg::GetOutput {
                        tail_bytes, reply, ..
                    } => {
                        let data = b"stdout-data";
                        let shown = data.len().min(tail_bytes);
                        let _ = reply.send(Some(crate::actor::OutputSnapshot {
                            data: data[data.len() - shown..].to_vec(),
                            truncated: shown < data.len(),
                        }));
                    }
                    ProcessMgrMsg::GetStderr {
                        tail_bytes, reply, ..
                    } => {
                        let data = b"stderr-data";
                        let shown = data.len().min(tail_bytes);
                        let _ = reply.send(Some(StderrSnapshot {
                            pty_merged: false,
                            data: data[data.len() - shown..].to_vec(),
                            truncated: shown < data.len(),
                        }));
                    }
                    _ => {}
                }
            }
        });
    }

    fn insert_running_test_job(state: &mut SchedulerState, job_id: JobId) {
        state.jobs.insert(
            job_id,
            JobEntry {
                job_id,
                session_id: None,
                pipeline_text: "sleep 60".into(),
                status: JobStatus::Running,
                exit_code: None,
                start_scope: Some(ScopeHash([0; 32])),
                end_scope: None,
                open_hint: JobOpenHint::Stream,
                chain_id: None,
                chain_index: None,
                chain_total: None,
                pending_reason: None,
            },
        );
    }

    #[test]
    fn script_exit_code_uses_reported_code_when_terminal_entry_lacks_exit_code() {
        let mut state = SchedulerState::new();
        insert_running_test_job(&mut state, JobId(7));
        let entry = state.jobs.get_mut(&JobId(7)).expect("test job");
        entry.status = JobStatus::Done;
        entry.exit_code = None;

        assert_eq!(script_exit_code_for_job(&state, JobId(7), 9), 9);

        let entry = state.jobs.get_mut(&JobId(7)).expect("test job");
        entry.status = JobStatus::Failed;
        entry.exit_code = None;

        assert_eq!(script_exit_code_for_job(&state, JobId(7), 11), 11);
    }

    #[test]
    fn sorted_job_list_uses_internal_job_id_order() {
        let mut state = SchedulerState::new();
        insert_running_test_job(&mut state, JobId(12));
        insert_running_test_job(&mut state, JobId(3));

        let list = sorted_job_list(&state);

        assert_eq!(
            list.iter().map(|job| job.id.as_str()).collect::<Vec<_>>(),
            ["J3", "J12"]
        );
    }

    #[test]
    fn sorted_cron_list_uses_internal_cron_id_order() {
        let mut state = SchedulerState::new();
        for cron_id in [CronId(8), CronId(2)] {
            state.crons.insert(
                cron_id,
                CronEntry {
                    cron_id,
                    schedule: CronSchedule::Interval(std::time::Duration::from_secs(60)),
                    chain: leaf("echo tick"),
                    scope_hash: ScopeHash([0; 32]),
                    status: CronStatus::Scheduled,
                    next_trigger: Instant::now(),
                    cwd_override: None,
                    scope_enabled: false,
                    wrapper_enabled: false,
                    session_id: None,
                },
            );
        }

        let list = sorted_cron_list(&state);

        assert_eq!(
            list.iter().map(|cron| cron.id.as_str()).collect::<Vec<_>>(),
            ["C2", "C8"]
        );
    }

    /// Drain all `SpawnJob` messages from the ProcessMgr receiver.
    async fn drain_spawn_jobs(rx: &mut mpsc::Receiver<ProcessMgrMsg>) -> Vec<JobId> {
        let mut ids = Vec::new();
        // Yield to let messages propagate.
        tokio::task::yield_now().await;
        while let Ok(msg) = rx.try_recv() {
            if let ProcessMgrMsg::SpawnJob { job_id, .. } = msg {
                ids.push(job_id);
            }
        }
        ids
    }

    async fn ack_next_kill(rx: &mut mpsc::Receiver<ProcessMgrMsg>) -> JobId {
        loop {
            if let ProcessMgrMsg::KillJob { job_id, reply } =
                rx.recv().await.expect("process manager message")
            {
                reply.send(Ok(())).expect("send kill ack");
                return job_id;
            }
        }
    }

    async fn drain_spawn_scopes(rx: &mut mpsc::Receiver<ProcessMgrMsg>) -> Vec<ScopeHash> {
        let mut scopes = Vec::new();
        tokio::task::yield_now().await;
        while let Ok(msg) = rx.try_recv() {
            if let ProcessMgrMsg::SpawnJob { scope_hash, .. } = msg {
                scopes.push(scope_hash);
            }
        }
        scopes
    }

    async fn recv_gateway_msg(rx: &mut mpsc::Receiver<GatewayMsg>) -> GatewayMsg {
        tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .expect("gateway message timeout")
            .expect("gateway channel closed")
    }

    fn gpu_need_params() -> cue_core::command::ModeParams {
        let mut params = cue_core::command::ModeParams::new();
        params.insert("need.gpu", cue_core::command::ParamValue::Str("1".into()));
        params
    }

    async fn drain_script_finished_events(
        rx: &mut mpsc::Receiver<EventBusMsg>,
    ) -> Vec<(String, ScriptRunStatus, i32, Option<usize>)> {
        let mut events = Vec::new();
        tokio::task::yield_now().await;
        while let Ok(msg) = rx.try_recv() {
            match msg {
                EventBusMsg::Publish {
                    payload:
                        EventPayload::ScriptFinished {
                            script_id,
                            status,
                            exit_code,
                            failed_item_index,
                        },
                    channel,
                }
                | EventBusMsg::PublishExcept {
                    payload:
                        EventPayload::ScriptFinished {
                            script_id,
                            status,
                            exit_code,
                            failed_item_index,
                        },
                    channel,
                    excluded_client_id: _,
                }
                | EventBusMsg::PublishSession {
                    payload:
                        EventPayload::ScriptFinished {
                            script_id,
                            status,
                            exit_code,
                            failed_item_index,
                        },
                    channel,
                    session_id: _,
                }
                | EventBusMsg::PublishSessionExcept {
                    payload:
                        EventPayload::ScriptFinished {
                            script_id,
                            status,
                            exit_code,
                            failed_item_index,
                        },
                    channel,
                    session_id: _,
                    excluded_client_id: _,
                } => {
                    assert_eq!(channel, EventChannel::Jobs);
                    events.push((script_id, status, exit_code, failed_item_index));
                }
                _ => {}
            }
        }
        events
    }

    #[tokio::test]
    async fn resource_admission_grant_derives_scope_env_and_releases_on_finish() {
        let provider = crate::resource::mock_provider("gpu", &["gpu"]);
        provider.set_env(std::collections::BTreeMap::from([(
            "CUDA_VISIBLE_DEVICES".to_string(),
            "2".to_string(),
        )]));
        let registry = crate::resource::ProviderRegistry::from_providers(vec![
            provider.clone() as std::sync::Arc<dyn crate::resource::Provider>
        ])
        .expect("resource registry");
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) =
            test_actor_system_with_resources(std::sync::Arc::new(registry));
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();
        let resp = handle_command(
            ResolvedCommand::Run {
                chain: leaf("echo gpu"),
                params: gpu_need_params(),
            },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;

        assert!(matches!(
            resp,
            ResponsePayload::Ok(OkPayload::JobCreated { .. })
        ));
        let scopes = drain_spawn_scopes(&mut pm_rx).await;
        let spawn_scope = *scopes.first().expect("spawn scope");
        let snapshot = get_scope_snapshot_by_hash(&sys, spawn_scope)
            .await
            .expect("spawn scope snapshot");
        assert_eq!(
            snapshot.env.get("CUDA_VISIBLE_DEVICES").map(String::as_str),
            Some("2")
        );
        assert_eq!(provider.reserve_calls(), 1);
        assert_eq!(provider.release_calls(), 0);

        handle_job_finished(JobId(1), 0, &mut state, &conn, &sys).await;
        assert_eq!(provider.release_calls(), 1);
    }

    #[tokio::test]
    async fn resource_admission_reject_keeps_job_pending_with_reason() {
        let provider = std::sync::Arc::new(crate::resource::MockProvider::with_behaviour(
            "gpu",
            &["gpu"],
            crate::resource::MockBehaviour::AlwaysReject(cue_core::resource::Reject::new(
                "gpu unavailable",
            )),
        ));
        let registry = crate::resource::ProviderRegistry::from_providers(vec![
            provider.clone() as std::sync::Arc<dyn crate::resource::Provider>
        ])
        .expect("resource registry");
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) =
            test_actor_system_with_resources(std::sync::Arc::new(registry));
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();
        let resp = handle_command(
            ResolvedCommand::Run {
                chain: leaf("echo gpu"),
                params: gpu_need_params(),
            },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;

        assert!(matches!(
            resp,
            ResponsePayload::Ok(OkPayload::JobCreated { .. })
        ));
        assert!(drain_spawn_jobs(&mut pm_rx).await.is_empty());
        let entry = state.jobs.get(&JobId(1)).expect("pending job");
        assert_eq!(entry.status, JobStatus::Pending);
        assert_eq!(
            entry.pending_reason.as_deref(),
            Some("gpu: gpu unavailable")
        );
        let list = sorted_job_list(&state);
        assert_eq!(
            list[0].pending_reason.as_deref(),
            Some("gpu: gpu unavailable")
        );
        assert_eq!(provider.reserve_calls(), 1);
        assert_eq!(provider.release_calls(), 0);
    }

    #[tokio::test]
    async fn resource_pending_job_retries_after_any_job_finishes() {
        let provider = std::sync::Arc::new(crate::resource::MockProvider::with_behaviour(
            "gpu",
            &["gpu"],
            crate::resource::MockBehaviour::Scripted(vec![
                Err(cue_core::resource::Reject::new("busy")),
                Ok(()),
            ]),
        ));
        let registry = crate::resource::ProviderRegistry::from_providers(vec![
            provider.clone() as std::sync::Arc<dyn crate::resource::Provider>
        ])
        .expect("resource registry");
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) =
            test_actor_system_with_resources(std::sync::Arc::new(registry));
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();
        let _ = handle_command(
            ResolvedCommand::Run {
                chain: leaf("echo waits"),
                params: gpu_need_params(),
            },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        assert!(drain_spawn_jobs(&mut pm_rx).await.is_empty());
        assert_eq!(state.jobs[&JobId(1)].status, JobStatus::Pending);

        let _ = spawn_chain(
            test_chain_spawn(leaf("echo trigger"), ScopeHash([0; 32])),
            &mut state,
            SchedulerIo::new(&conn, &sys),
        )
        .await;
        assert_eq!(drain_spawn_jobs(&mut pm_rx).await, vec![JobId(2)]);

        handle_job_finished(JobId(2), 0, &mut state, &conn, &sys).await;
        assert_eq!(drain_spawn_jobs(&mut pm_rx).await, vec![JobId(1)]);
        let entry = state.jobs.get(&JobId(1)).expect("retried job");
        assert_eq!(entry.status, JobStatus::Running);
        assert_eq!(entry.pending_reason, None);
        assert_eq!(provider.reserve_calls(), 2);
    }

    #[test]
    fn resource_provider_formatter_lists_routes_and_active_reservations() {
        let provider = crate::resource::mock_provider("gpu", &["gpu", "gpu_mem"]);
        let registry = crate::resource::ProviderRegistry::from_providers(vec![
            provider as Arc<dyn crate::resource::Provider>,
        ])
        .expect("registry");

        let text = format_resource_providers(&registry);
        assert!(text.contains("resource providers: 1 (active reservations: 0)"));
        assert!(text.contains("- gpu: gpu, gpu_mem"));
    }

    #[test]
    fn resource_snapshot_formatter_lists_units_and_attrs() {
        let provider = crate::resource::mock_provider("gpu", &["gpu_mem"]);
        provider.set_snapshot_units(vec![cue_core::resource::ResourceUnit::new("2").with_attr(
            "effective_free_mem",
            cue_core::resource::ResourceQuantity::Bytes(24 * 1024 * 1024 * 1024),
        )]);
        let registry = crate::resource::ProviderRegistry::from_providers(vec![
            provider as Arc<dyn crate::resource::Provider>,
        ])
        .expect("registry");

        let text = format_resource_snapshots(&registry);
        assert!(text.contains("provider gpu"));
        assert!(text.contains("unit 2: effective_free_mem=24GiB"));
    }

    #[tokio::test]
    async fn resource_commands_return_text() {
        let provider = crate::resource::mock_provider("license", &["license"]);
        provider.set_snapshot_units(vec![
            cue_core::resource::ResourceUnit::new("pool")
                .with_attr("free", cue_core::resource::ResourceQuantity::Count(3)),
        ]);
        let registry = crate::resource::ProviderRegistry::from_providers(vec![
            provider as Arc<dyn crate::resource::Provider>,
        ])
        .expect("registry");
        let (sys, _gw_rx, _sched_rx, _pm_rx, _ss_rx, _eb_rx) =
            test_actor_system_with_resources(Arc::new(registry));
        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();

        let providers = handle_command(
            ResolvedCommand::Providers,
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        assert!(matches!(
            providers,
            ResponsePayload::Ok(OkPayload::EvalText { ref text }) if text.contains("- license: license")
        ));

        let resources = handle_command(
            ResolvedCommand::Resources,
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        assert!(matches!(
            resources,
            ResponsePayload::Ok(OkPayload::EvalText { ref text }) if text.contains("unit pool: free=3")
        ));
    }

    #[test]
    fn help_renderer_supports_mode_topics() {
        let job = render_help_text(Some("job"));
        assert!(job.contains("JOB mode"));
        assert!(job.contains(":tail J<n>"));

        let cron = render_help_text(Some("cron"));
        assert!(cron.contains("CRON mode"));
        assert!(cron.contains("every 5m cargo test"));
        assert!(cron.contains(":kill <id>"));
        assert!(cron.contains(":log [id]"));
    }

    #[test]
    fn help_renderer_maps_command_aliases_to_modes() {
        assert!(render_help_text(Some("run")).contains("JOB mode"));
        assert!(render_help_text(Some("ask")).contains("Unknown help topic"));
        assert!(render_help_text(Some("pause")).contains("CRON mode"));
    }

    #[tokio::test]
    async fn serial_then_chain_spawns_left_first() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let mut state = SchedulerState::new();
        let chain = ChainNode::Serial {
            left: Box::new(leaf("echo a")),
            op: SerialOp::Then,
            right: Box::new(leaf("echo b")),
        };

        let resp = spawn_chain(
            test_chain_spawn(chain, ScopeHash([0; 32])),
            &mut state,
            SchedulerIo::new(&conn, &sys),
        )
        .await;

        // Should create a chain, not a single job.
        assert!(matches!(
            resp,
            ResponsePayload::Ok(OkPayload::ChainCreated { .. })
        ));

        // Only one job should be spawned initially (the left leaf).
        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(spawned.len(), 1);

        // Left leaf should be Running, right should be Pending.
        let chain_st = state.chains.values().next().unwrap();
        assert!(matches!(chain_st.leaf_status[&0], LeafStatus::Running));
        assert!(matches!(chain_st.leaf_status[&1], LeafStatus::Pending));
    }

    #[tokio::test]
    async fn serial_then_left_fail_cancels_right() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let mut state = SchedulerState::new();
        let chain = ChainNode::Serial {
            left: Box::new(leaf("false")),
            op: SerialOp::Then,
            right: Box::new(leaf("echo b")),
        };

        let _ = spawn_chain(
            test_chain_spawn(chain, ScopeHash([0; 32])),
            &mut state,
            SchedulerIo::new(&conn, &sys),
        )
        .await;
        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(spawned.len(), 1);
        let left_jid = spawned[0];

        // Simulate left failing.
        handle_job_finished(left_jid, 1, &mut state, &conn, &sys).await;

        // Right should NOT have been spawned.
        let after_finish = drain_spawn_jobs(&mut pm_rx).await;
        assert!(after_finish.is_empty());

        // Chain should be cleaned up (complete).
        assert!(state.chains.is_empty());
    }

    #[tokio::test]
    async fn serial_then_left_success_spawns_right() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let mut state = SchedulerState::new();
        let chain = ChainNode::Serial {
            left: Box::new(leaf("echo a")),
            op: SerialOp::Then,
            right: Box::new(leaf("echo b")),
        };

        let _ = spawn_chain(
            test_chain_spawn(chain, ScopeHash([0; 32])),
            &mut state,
            SchedulerIo::new(&conn, &sys),
        )
        .await;
        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        let left_jid = spawned[0];

        // Simulate left succeeding.
        handle_job_finished(left_jid, 0, &mut state, &conn, &sys).await;

        // Right should be spawned now.
        let after_finish = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(after_finish.len(), 1);
    }

    #[tokio::test]
    async fn serial_always_runs_right_after_left_fails() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let mut state = SchedulerState::new();
        let chain = ChainNode::Serial {
            left: Box::new(leaf("false")),
            op: SerialOp::Always,
            right: Box::new(leaf("cleanup")),
        };

        let _ = spawn_chain(
            test_chain_spawn(chain, ScopeHash([0; 32])),
            &mut state,
            SchedulerIo::new(&conn, &sys),
        )
        .await;
        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        let left_jid = spawned[0];

        // Left fails.
        handle_job_finished(left_jid, 1, &mut state, &conn, &sys).await;

        // Right should still spawn (Always semantics).
        let after = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(after.len(), 1);
    }

    #[tokio::test]
    async fn parallel_all_spawns_both_immediately() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let mut state = SchedulerState::new();
        let chain = ChainNode::Parallel {
            left: Box::new(leaf("cargo test")),
            op: ParallelOp::All,
            right: Box::new(leaf("cargo clippy")),
        };

        let _ = spawn_chain(
            test_chain_spawn(chain, ScopeHash([0; 32])),
            &mut state,
            SchedulerIo::new(&conn, &sys),
        )
        .await;
        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(spawned.len(), 2);
    }

    #[tokio::test]
    async fn single_scope_transform_defaults_to_regular_job() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let mut state = SchedulerState::new();
        let resp = spawn_chain(
            test_chain_spawn(leaf("cd ."), ScopeHash([0; 32])),
            &mut state,
            SchedulerIo::new(&conn, &sys),
        )
        .await;
        assert!(matches!(
            resp,
            ResponsePayload::Ok(OkPayload::JobCreated { .. })
        ));

        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(spawned.len(), 1);
        let entry = state.jobs.get(&spawned[0]).expect("job entry");
        assert_eq!(entry.status, JobStatus::Running);
        assert_eq!(entry.end_scope, None);
    }

    #[tokio::test]
    async fn single_scope_transform_requires_scope_param() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let mut state = SchedulerState::new();
        let resp = spawn_chain(
            test_scope_chain_spawn(leaf("cd ."), ScopeHash([0; 32])),
            &mut state,
            SchedulerIo::new(&conn, &sys),
        )
        .await;
        assert!(matches!(
            resp,
            ResponsePayload::Ok(OkPayload::JobCreated { .. })
        ));

        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        assert!(spawned.is_empty());
        let entry = state.jobs.get(&JobId(1)).expect("job entry");
        assert_eq!(entry.status, JobStatus::Done);
        assert!(entry.end_scope.is_some());
    }

    #[tokio::test]
    async fn single_scope_transform_reports_terminal_persist_failure() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        drop_jobs_history_table(&conn);
        let mut state = SchedulerState::new();
        let temp_dir = std::env::temp_dir();
        let resp = spawn_chain(
            test_scope_chain_spawn(
                leaf(&format!("cd {}", temp_dir.display())),
                ScopeHash([0; 32]),
            ),
            &mut state,
            SchedulerIo::new(&conn, &sys),
        )
        .await;

        match resp {
            ResponsePayload::Err { code, message } => {
                assert_eq!(code, error_code::INTERNAL);
                assert!(message.contains("persist job J1 history"));
                assert!(message.contains("no such table"));
            }
            other => panic!("expected job history persist failure, got {other:?}"),
        }
        assert!(drain_spawn_jobs(&mut pm_rx).await.is_empty());
        let entry = state.jobs.get(&JobId(1)).expect("job entry");
        assert_eq!(entry.status, JobStatus::Done);
        assert!(entry.end_scope.is_some());
    }

    #[tokio::test]
    async fn single_job_spawn_failure_is_reported_without_running_job() {
        let (sys, _gw_rx, _sched_rx, pm_rx, ss_rx, _eb_rx) = test_actor_system();
        drop(pm_rx);
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let mut state = SchedulerState::new();
        let resp = spawn_chain(
            test_chain_spawn(leaf("echo hello"), ScopeHash([0; 32])),
            &mut state,
            SchedulerIo::new(&conn, &sys),
        )
        .await;

        match resp {
            ResponsePayload::Err { code, message } => {
                assert_eq!(code, error_code::INTERNAL);
                assert_eq!(message, "process_mgr unreachable");
            }
            other => panic!("expected process_mgr error, got {other:?}"),
        }
        let entry = state.jobs.get(&JobId(1)).expect("job entry");
        assert_eq!(entry.status, JobStatus::Failed);
        assert_eq!(entry.exit_code, Some(EXIT_CODE_UNAVAILABLE));
    }

    #[tokio::test]
    async fn chain_spawn_failure_is_reported_and_terminalizes_leaf() {
        let (sys, _gw_rx, _sched_rx, pm_rx, ss_rx, _eb_rx) = test_actor_system();
        drop(pm_rx);
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let mut state = SchedulerState::new();
        let chain = ChainNode::Serial {
            left: Box::new(leaf("echo a")),
            op: SerialOp::Then,
            right: Box::new(leaf("echo b")),
        };

        let resp = spawn_chain(
            test_chain_spawn(chain, ScopeHash([0; 32])),
            &mut state,
            SchedulerIo::new(&conn, &sys),
        )
        .await;

        match resp {
            ResponsePayload::Err { code, message } => {
                assert_eq!(code, error_code::INTERNAL);
                assert_eq!(message, "process_mgr unreachable");
            }
            other => panic!("expected process_mgr error, got {other:?}"),
        }
        let entry = state.jobs.get(&JobId(1)).expect("job entry");
        assert_eq!(entry.status, JobStatus::Failed);
        assert_eq!(entry.exit_code, Some(EXIT_CODE_UNAVAILABLE));
        assert!(state.chains.is_empty());
        assert!(state.completed_chains.is_empty());
    }

    #[tokio::test]
    async fn scope_transform_chain_reports_terminal_persist_failure() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        drop_jobs_history_table(&conn);
        let mut state = SchedulerState::new();
        let temp_dir = std::env::temp_dir();
        let chain = ChainNode::Serial {
            left: Box::new(leaf(&format!("cd {}", temp_dir.display()))),
            op: SerialOp::Then,
            right: Box::new(leaf("env set CUE_SCOPE_TEST=1")),
        };

        let resp = spawn_chain(
            test_scope_chain_spawn(chain, ScopeHash([0; 32])),
            &mut state,
            SchedulerIo::new(&conn, &sys),
        )
        .await;

        match resp {
            ResponsePayload::Err { code, message } => {
                assert_eq!(code, error_code::INTERNAL);
                assert!(message.contains("persist job J1 history"));
                assert!(message.contains("no such table"));
            }
            other => panic!("expected job history persist failure, got {other:?}"),
        }
        assert!(drain_spawn_jobs(&mut pm_rx).await.is_empty());
        assert!(state.chains.is_empty());
    }

    #[tokio::test]
    async fn scope_transform_chain_can_complete_before_creation_response() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let mut state = SchedulerState::new();
        let temp_dir = std::env::temp_dir();
        let chain = ChainNode::Serial {
            left: Box::new(leaf(&format!("cd {}", temp_dir.display()))),
            op: SerialOp::Then,
            right: Box::new(leaf("env set CUE_SCOPE_TEST=1")),
        };

        let resp = spawn_chain(
            test_scope_chain_spawn(chain, ScopeHash([0; 32])),
            &mut state,
            SchedulerIo::new(&conn, &sys),
        )
        .await;

        match resp {
            ResponsePayload::Ok(OkPayload::ChainCreated { chain, .. }) => {
                assert_eq!(chain.jobs.len(), 2);
                assert!(chain.jobs.iter().all(|job| job.status == JobStatus::Done));
            }
            other => panic!("expected completed ChainCreated response, got {other:?}"),
        }
        assert!(drain_spawn_jobs(&mut pm_rx).await.is_empty());
        assert!(state.chains.is_empty());
        assert!(state.completed_chains.is_empty());
    }

    #[tokio::test]
    async fn direct_script_command_is_rejected_before_scheduler_execution() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, _ss_rx, _eb_rx) = test_actor_system();

        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();
        let resp = handle_command(
            ResolvedCommand::Script {
                mode: Mode::Job,
                source: ScriptSource::Inline,
                items: vec![ResolvedScriptItem {
                    source: "echo hi".into(),
                    command: Box::new(ResolvedCommand::Run {
                        chain: leaf("echo hi"),
                        params: cue_core::command::ModeParams::new(),
                    }),
                }],
            },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;

        match resp {
            ResponsePayload::Err { code, message } => {
                assert_eq!(code, error_code::NOT_SUPPORTED);
                assert!(message.contains("file-script runner"));
            }
            other => panic!("expected script command rejection, got {other:?}"),
        }
        assert!(state.jobs.is_empty());
        assert!(drain_spawn_jobs(&mut pm_rx).await.is_empty());
    }

    #[tokio::test]
    async fn pending_file_script_consumes_synchronously_completed_chain_scope() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();
        let mut scope_params = cue_core::command::ModeParams::new();
        scope_params.insert("scope", cue_core::command::ParamValue::Bool(true));
        let temp_dir = std::env::temp_dir();
        let cd_command = format!("cd {}", temp_dir.display());
        let chain = ChainNode::Serial {
            left: Box::new(leaf(&cd_command)),
            op: SerialOp::Then,
            right: Box::new(leaf("env set CUE_CHAIN_SCOPE=1")),
        };

        let response = start_pending_script_run(
            Mode::Job,
            ScriptSource::File {
                path: "sync-chain.cue".into(),
            },
            vec![
                ResolvedScriptItem {
                    source: format!("{cd_command} -> env set CUE_CHAIN_SCOPE=1"),
                    command: Box::new(ResolvedCommand::Run {
                        chain,
                        params: scope_params,
                    }),
                },
                ResolvedScriptItem {
                    source: "echo after".into(),
                    command: Box::new(ResolvedCommand::Run {
                        chain: leaf("echo after"),
                        params: cue_core::command::ModeParams::new(),
                    }),
                },
            ],
            0,
            &mut state,
            test_runtime(&conn, &config, &sys),
        )
        .await
        .expect("created response");

        assert!(matches!(
            response,
            ResponsePayload::Ok(OkPayload::ScriptCreated { .. })
        ));
        let chain_end_scope = state
            .jobs
            .get(&JobId(2))
            .and_then(|entry| entry.end_scope)
            .expect("second scope-transform end scope");
        let scopes = drain_spawn_scopes(&mut pm_rx).await;
        assert_eq!(scopes, vec![chain_end_scope]);
        assert!(state.completed_chains.is_empty());
        assert!(state.pending_script_chains.is_empty());
    }

    #[tokio::test]
    async fn pending_file_script_reports_submission_persist_failure_without_losing_job_tracking() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        drop_script_items_table(&conn);
        let config = Config::default();
        let mut state = SchedulerState::new();

        let response = start_pending_script_run(
            Mode::Job,
            ScriptSource::File {
                path: "persist-fails.cue".into(),
            },
            vec![ResolvedScriptItem {
                source: "long-running".into(),
                command: Box::new(ResolvedCommand::Run {
                    chain: leaf("long-running"),
                    params: cue_core::command::ModeParams::new(),
                }),
            }],
            0,
            &mut state,
            test_runtime(&conn, &config, &sys),
        )
        .await
        .expect("response");

        match response {
            ResponsePayload::Err { code, message } => {
                assert_eq!(code, error_code::INTERNAL);
                assert!(message.contains("persist script R1 submission"));
                assert!(message.contains("delete existing script items"));
            }
            other => panic!("expected script persistence error, got {other:?}"),
        }

        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(spawned, vec![JobId(1)]);
        assert_eq!(state.pending_script_jobs.get(&JobId(1)), Some(&ScriptId(1)));
        assert!(state.pending_scripts.contains_key(&ScriptId(1)));
        let count: i64 = conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM script_runs WHERE id = 'R1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn pending_file_script_spawns_jobs_with_direct_output_client() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();
        let response = start_pending_script_run(
            Mode::Job,
            ScriptSource::File {
                path: "direct-output.cue".into(),
            },
            vec![ResolvedScriptItem {
                source: "echo direct".into(),
                command: Box::new(ResolvedCommand::Run {
                    chain: leaf("echo direct"),
                    params: cue_core::command::ModeParams::new(),
                }),
            }],
            42,
            &mut state,
            test_runtime(&conn, &config, &sys),
        )
        .await
        .expect("created response");

        assert!(matches!(
            response,
            ResponsePayload::Ok(OkPayload::ScriptCreated { .. })
        ));
        match pm_rx.recv().await.expect("spawn job") {
            ProcessMgrMsg::SpawnJob {
                job_id, options, ..
            } => {
                assert_eq!(job_id, JobId(1));
                assert_eq!(options.direct_output_client, Some(42));
            }
            _ => panic!("expected script job spawn"),
        }
    }

    #[tokio::test]
    async fn pending_file_script_immediate_completion_sends_direct_finish_event() {
        let (sys, mut gw_rx, _sched_rx, mut pm_rx, ss_rx, mut eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();
        let response = start_pending_script_run(
            Mode::Job,
            ScriptSource::File {
                path: "immediate.cue".into(),
            },
            vec![ResolvedScriptItem {
                source: ":help".into(),
                command: Box::new(ResolvedCommand::Help { topic: None }),
            }],
            42,
            &mut state,
            test_runtime(&conn, &config, &sys),
        )
        .await
        .expect("created response");

        assert!(matches!(
            response,
            ResponsePayload::Ok(OkPayload::ScriptCreated { .. })
        ));
        assert!(drain_spawn_jobs(&mut pm_rx).await.is_empty());
        match recv_gateway_msg(&mut gw_rx).await {
            GatewayMsg::SendEvent {
                client_id,
                session_id: None,
                payload:
                    EventPayload::ScriptFinished {
                        script_id,
                        status,
                        exit_code,
                        failed_item_index,
                    },
            } => {
                assert_eq!(client_id, 42);
                assert_eq!(script_id, "R1");
                assert_eq!(status, ScriptRunStatus::Done);
                assert_eq!(exit_code, 0);
                assert_eq!(failed_item_index, None);
            }
            _ => panic!("expected direct script finished event"),
        }
        let publish = tokio::time::timeout(std::time::Duration::from_secs(5), eb_rx.recv())
            .await
            .expect("script finished publish timeout")
            .expect("event bus channel closed");
        match publish {
            EventBusMsg::PublishSessionExcept {
                channel,
                session_id,
                excluded_client_id,
                payload:
                    EventPayload::ScriptFinished {
                        script_id,
                        status,
                        exit_code,
                        failed_item_index,
                    },
            } => {
                assert_eq!(channel, EventChannel::Jobs);
                assert_eq!(session_id, None);
                assert_eq!(excluded_client_id, 42);
                assert_eq!(script_id, "R1");
                assert_eq!(status, ScriptRunStatus::Done);
                assert_eq!(exit_code, 0);
                assert_eq!(failed_item_index, None);
            }
            _ => panic!("expected script finished publish excluding requester"),
        }
    }

    #[tokio::test]
    async fn pending_file_script_finish_applies_retention_policy() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let mut config = Config::default();
        config.retention.max_script_runs = 1;
        let mut state = SchedulerState::new();

        for path in ["first.cue", "second.cue"] {
            let response = start_pending_script_run(
                Mode::Job,
                ScriptSource::File { path: path.into() },
                vec![ResolvedScriptItem {
                    source: ":help".into(),
                    command: Box::new(ResolvedCommand::Help { topic: None }),
                }],
                42,
                &mut state,
                test_runtime(&conn, &config, &sys),
            )
            .await
            .expect("created response");
            assert!(matches!(
                response,
                ResponsePayload::Ok(OkPayload::ScriptCreated { .. })
            ));
        }

        assert!(drain_spawn_jobs(&mut pm_rx).await.is_empty());
        assert_eq!(persisted_script_ids(&conn), vec!["R2"]);
    }

    #[tokio::test]
    async fn pending_file_script_fail_fast_stops_following_items() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, mut eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();
        let response = start_pending_script_run(
            Mode::Job,
            ScriptSource::File {
                path: "fail.cue".into(),
            },
            vec![
                ResolvedScriptItem {
                    source: "false".into(),
                    command: Box::new(ResolvedCommand::Run {
                        chain: leaf("false"),
                        params: cue_core::command::ModeParams::new(),
                    }),
                },
                ResolvedScriptItem {
                    source: "echo never".into(),
                    command: Box::new(ResolvedCommand::Run {
                        chain: leaf("echo never"),
                        params: cue_core::command::ModeParams::new(),
                    }),
                },
            ],
            0,
            &mut state,
            test_runtime(&conn, &config, &sys),
        )
        .await
        .expect("created response");
        assert!(matches!(
            response,
            ResponsePayload::Ok(OkPayload::ScriptCreated { .. })
        ));

        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(spawned.len(), 1);
        handle_job_finished(spawned[0], 7, &mut state, &conn, &sys).await;
        advance_pending_scripts_after_terminal_job(
            spawned[0],
            7,
            &mut state,
            test_runtime(&conn, &config, &sys),
        )
        .await;

        assert!(drain_spawn_jobs(&mut pm_rx).await.is_empty());
        let events = drain_script_finished_events(&mut eb_rx).await;
        assert_eq!(
            events,
            vec![("R1".into(), ScriptRunStatus::Failed, 7, Some(0))]
        );
        let (status, exit_code, failed_item_index, finished_at) =
            persisted_script_state(&conn, "R1");
        assert_eq!(status, "failed");
        assert_eq!(exit_code, Some(7));
        assert_eq!(failed_item_index, Some(0));
        assert!(finished_at.is_some());
    }

    #[tokio::test]
    async fn pending_file_script_cancel_fails_without_waiting_for_late_process_exit() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, mut eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();
        let _ = start_pending_script_run(
            Mode::Job,
            ScriptSource::File {
                path: "cancel.cue".into(),
            },
            vec![
                ResolvedScriptItem {
                    source: "long-running".into(),
                    command: Box::new(ResolvedCommand::Run {
                        chain: leaf("long-running"),
                        params: cue_core::command::ModeParams::new(),
                    }),
                },
                ResolvedScriptItem {
                    source: "echo never".into(),
                    command: Box::new(ResolvedCommand::Run {
                        chain: leaf("echo never"),
                        params: cue_core::command::ModeParams::new(),
                    }),
                },
            ],
            0,
            &mut state,
            test_runtime(&conn, &config, &sys),
        )
        .await
        .expect("created response");

        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(spawned.len(), 1);
        let jid = spawned[0];

        let response_fut = handle_command(
            ResolvedCommand::Cancel {
                id: jid.to_string(),
            },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        );
        let (response, killed) = tokio::join!(response_fut, ack_next_kill(&mut pm_rx));
        assert_eq!(killed, jid);
        assert!(matches!(response, ResponsePayload::Ok(OkPayload::Ack {})));
        assert_eq!(
            state.jobs[&jid].status,
            JobStatus::Cancelled(CancelReason::User)
        );
        assert_eq!(state.jobs[&jid].exit_code, Some(EXIT_CODE_UNAVAILABLE));
        assert!(state.pending_script_jobs.is_empty());
        assert!(state.pending_scripts.is_empty());
        assert!(drain_spawn_jobs(&mut pm_rx).await.is_empty());
        assert_eq!(
            drain_script_finished_events(&mut eb_rx).await,
            vec![(
                "R1".into(),
                ScriptRunStatus::Failed,
                EXIT_CODE_UNAVAILABLE,
                Some(0)
            )]
        );

        handle_job_finished(jid, 0, &mut state, &conn, &sys).await;
        advance_pending_scripts_after_terminal_job(
            jid,
            0,
            &mut state,
            test_runtime(&conn, &config, &sys),
        )
        .await;

        assert_eq!(
            state.jobs[&jid].status,
            JobStatus::Cancelled(CancelReason::User)
        );
        assert_eq!(state.jobs[&jid].exit_code, Some(EXIT_CODE_UNAVAILABLE));
        assert!(drain_spawn_jobs(&mut pm_rx).await.is_empty());
        assert!(drain_script_finished_events(&mut eb_rx).await.is_empty());
    }

    #[tokio::test]
    async fn shutdown_fails_pending_file_scripts_and_clears_tracking() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, mut eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();
        let _ = start_pending_script_run(
            Mode::Job,
            ScriptSource::File {
                path: "shutdown.cue".into(),
            },
            vec![
                ResolvedScriptItem {
                    source: "long-running".into(),
                    command: Box::new(ResolvedCommand::Run {
                        chain: leaf("long-running"),
                        params: cue_core::command::ModeParams::new(),
                    }),
                },
                ResolvedScriptItem {
                    source: "echo never".into(),
                    command: Box::new(ResolvedCommand::Run {
                        chain: leaf("echo never"),
                        params: cue_core::command::ModeParams::new(),
                    }),
                },
            ],
            0,
            &mut state,
            test_runtime(&conn, &config, &sys),
        )
        .await
        .expect("created response");

        assert_eq!(drain_spawn_jobs(&mut pm_rx).await, vec![JobId(1)]);
        assert_eq!(state.pending_script_jobs.get(&JobId(1)), Some(&ScriptId(1)));
        assert!(state.pending_scripts.contains_key(&ScriptId(1)));

        fail_pending_scripts_on_shutdown(&mut state, test_runtime(&conn, &config, &sys)).await;

        assert!(state.pending_script_jobs.is_empty());
        assert!(state.pending_script_chains.is_empty());
        assert!(state.pending_scripts.is_empty());
        assert!(state.completed_chains.is_empty());
        assert_eq!(
            drain_script_finished_events(&mut eb_rx).await,
            vec![(
                "R1".into(),
                ScriptRunStatus::Failed,
                EXIT_CODE_UNAVAILABLE,
                Some(0)
            )]
        );
        let (status, exit_code, failed_item_index, finished_at) =
            persisted_script_state(&conn, "R1");
        assert_eq!(status, "failed");
        assert_eq!(exit_code, Some(EXIT_CODE_UNAVAILABLE));
        assert_eq!(failed_item_index, Some(0));
        assert!(finished_at.is_some());
    }

    #[tokio::test]
    async fn pending_file_script_success_advances_to_next_item_and_finishes() {
        let (sys, mut gw_rx, _sched_rx, mut pm_rx, ss_rx, mut eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();
        let _ = start_pending_script_run(
            Mode::Job,
            ScriptSource::File {
                path: "ok.cue".into(),
            },
            vec![
                ResolvedScriptItem {
                    source: "echo one".into(),
                    command: Box::new(ResolvedCommand::Run {
                        chain: leaf("echo one"),
                        params: cue_core::command::ModeParams::new(),
                    }),
                },
                ResolvedScriptItem {
                    source: "echo two".into(),
                    command: Box::new(ResolvedCommand::Run {
                        chain: leaf("echo two"),
                        params: cue_core::command::ModeParams::new(),
                    }),
                },
            ],
            42,
            &mut state,
            test_runtime(&conn, &config, &sys),
        )
        .await
        .expect("created response");

        let first = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(first.len(), 1);
        handle_job_finished(first[0], 0, &mut state, &conn, &sys).await;
        advance_pending_scripts_after_terminal_job(
            first[0],
            0,
            &mut state,
            test_runtime(&conn, &config, &sys),
        )
        .await;

        let second = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(second.len(), 1);
        match recv_gateway_msg(&mut gw_rx).await {
            GatewayMsg::SendEvent {
                client_id,
                session_id: None,
                payload: EventPayload::ScriptItemCreated { script_id, item },
            } => {
                assert_eq!(client_id, 42);
                assert_eq!(script_id, "R1");
                assert_eq!(item.index, 1);
                assert_eq!(item.source, "echo two");
                assert!(matches!(
                    item.result,
                    ScriptItemResult::Job { ref job_id, .. } if job_id == "J2"
                ));
            }
            _ => panic!("expected authoritative script item event"),
        }
        handle_job_finished(second[0], 0, &mut state, &conn, &sys).await;
        advance_pending_scripts_after_terminal_job(
            second[0],
            0,
            &mut state,
            test_runtime(&conn, &config, &sys),
        )
        .await;

        match recv_gateway_msg(&mut gw_rx).await {
            GatewayMsg::SendEvent {
                client_id,
                session_id: None,
                payload:
                    EventPayload::ScriptFinished {
                        script_id,
                        status,
                        exit_code,
                        failed_item_index,
                    },
            } => {
                assert_eq!(client_id, 42);
                assert_eq!(script_id, "R1");
                assert_eq!(status, ScriptRunStatus::Done);
                assert_eq!(exit_code, 0);
                assert_eq!(failed_item_index, None);
            }
            _ => panic!("expected direct script finished event"),
        }
        let events = drain_script_finished_events(&mut eb_rx).await;
        assert_eq!(events, vec![("R1".into(), ScriptRunStatus::Done, 0, None)]);
        let (status, exit_code, failed_item_index, finished_at) =
            persisted_script_state(&conn, "R1");
        assert_eq!(status, "done");
        assert_eq!(exit_code, Some(0));
        assert_eq!(failed_item_index, None);
        assert!(finished_at.is_some());
    }

    #[tokio::test]
    async fn warned_run_commands_still_execute() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let mut config = Config::default();
        config
            .warn
            .commands
            .insert("cd".into(), "review before changing directory".into());
        let mut state = SchedulerState::new();
        let chain = ChainNode::Serial {
            left: Box::new(leaf("cd /tmp")),
            op: SerialOp::Then,
            right: Box::new(leaf("pwd")),
        };

        let resp = handle_command(
            ResolvedCommand::Run {
                chain,
                params: cue_core::command::ModeParams::new(),
            },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;

        match resp {
            ResponsePayload::Ok(OkPayload::ChainCreated { warnings, .. }) => {
                assert_eq!(warnings, vec!["review before changing directory"]);
            }
            other => panic!("expected warned chain to execute, got {other:?}"),
        }
        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(spawned.len(), 1);
    }

    #[tokio::test]
    async fn run_mode_params_are_split_between_scope_and_launch_options() {
        let provider = crate::resource::mock_provider("gpu", &["gpu", "gpu_mem"]);
        provider.set_env(std::collections::BTreeMap::from([(
            "CUDA_VISIBLE_DEVICES".to_string(),
            "mock".to_string(),
        )]));
        let registry = crate::resource::ProviderRegistry::from_providers(vec![
            provider.clone() as std::sync::Arc<dyn crate::resource::Provider>
        ])
        .expect("resource registry");
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) =
            test_actor_system_with_resources(std::sync::Arc::new(registry));
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();
        let cwd = std::env::temp_dir();
        let mut params = cue_core::command::ModeParams::new();
        params.insert(
            "cwd",
            cue_core::command::ParamValue::Str(cwd.display().to_string()),
        );
        params.insert("pty", cue_core::command::ParamValue::Bool(false));
        params.insert("need.gpu", cue_core::command::ParamValue::Str("1".into()));
        params.insert(
            "need.gpu_mem",
            cue_core::command::ParamValue::Str("24GiB".into()),
        );
        params.insert(
            "sandbox",
            cue_core::command::ParamValue::Str("overlay".into()),
        );
        params.insert(
            "sandbox.upper",
            cue_core::command::ParamValue::Str("tmpfs".into()),
        );

        let resp = handle_command(
            ResolvedCommand::Run {
                chain: leaf("echo hi"),
                params,
            },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        assert!(matches!(
            resp,
            ResponsePayload::Ok(OkPayload::JobCreated { .. })
        ));

        let msg = pm_rx.try_recv().expect("spawn job");
        let (scope, options) = match msg {
            ProcessMgrMsg::SpawnJob {
                scope_hash,
                options,
                ..
            } => (scope_hash, options),
            _ => panic!("expected SpawnJob"),
        };
        let snapshot = get_scope_snapshot_by_hash(&sys, scope)
            .await
            .expect("spawn scope snapshot");
        assert_eq!(snapshot.cwd, cwd);
        assert_eq!(
            snapshot.env.get("CUDA_VISIBLE_DEVICES"),
            Some(&"mock".to_string())
        );
        let snapshot_json = serde_json::to_value(&snapshot).expect("serialize snapshot");
        assert!(snapshot_json.get("execution").is_none());
        assert!(!options.pty_enabled);
        assert_eq!(
            options.sandbox,
            Some(crate::sandbox::SandboxConfig {
                mode: crate::sandbox::SandboxMode::Overlay,
                upper: Some(crate::sandbox::SandboxUpper::Tmpfs),
            })
        );
        let request = provider.last_request().expect("resource request");
        assert_eq!(
            request.need.get("gpu"),
            Some(cue_core::resource::ResourceQuantity::Count(1))
        );
        assert_eq!(
            request.need.get("gpu_mem"),
            Some(cue_core::resource::ResourceQuantity::Bytes(
                24 * 1024 * 1024 * 1024
            ))
        );
    }

    #[tokio::test]
    async fn run_wrapper_param_overrides_session_and_config() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let mut config = Config::default();
        config.wrapper.enabled = false;
        let mut state = SchedulerState::new();
        bind_test_session(&mut state, &sys, 0).await;
        state
            .session_for_client_mut(0)
            .expect("test session")
            .defaults
            .wrapper_enabled = Some(false);
        let mut params = cue_core::command::ModeParams::new();
        params.insert("wrapper", cue_core::command::ParamValue::Bool(true));

        let resp = handle_command(
            ResolvedCommand::Run {
                chain: leaf("echo hi"),
                params,
            },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        assert!(matches!(
            resp,
            ResponsePayload::Ok(OkPayload::JobCreated { .. })
        ));

        let msg = pm_rx.try_recv().expect("spawn job");
        match msg {
            ProcessMgrMsg::SpawnJob { options, .. } => assert!(options.wrapper_enabled),
            _ => panic!("expected SpawnJob"),
        }
    }

    #[tokio::test]
    async fn run_uses_session_pty_default_without_mutating_scope() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();
        bind_test_session(&mut state, &sys, 0).await;
        let original_scope = state.client_scope(0).expect("session scope");
        state
            .session_for_client_mut(0)
            .expect("test session")
            .defaults
            .pty = Some(false);

        let resp = handle_command(
            ResolvedCommand::Run {
                chain: leaf("echo hi"),
                params: cue_core::command::ModeParams::new(),
            },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        assert!(matches!(
            resp,
            ResponsePayload::Ok(OkPayload::JobCreated { .. })
        ));
        assert_eq!(state.client_scope(0), Some(original_scope));

        let msg = pm_rx.try_recv().expect("spawn job");
        match msg {
            ProcessMgrMsg::SpawnJob { options, .. } => assert!(!options.pty_enabled),
            _ => panic!("expected SpawnJob"),
        }
    }

    #[tokio::test]
    async fn cron_cwd_param_is_captured_in_registered_scope() {
        let (sys, _gw_rx, _sched_rx, _pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();
        let cwd = std::env::temp_dir();
        let mut params = cue_core::command::ModeParams::new();
        params.insert(
            "cwd",
            cue_core::command::ParamValue::Str(cwd.display().to_string()),
        );
        let cmd = ResolvedCommand::Cron {
            schedule: CronSchedule::Interval(std::time::Duration::from_secs(300)),
            chain: leaf("cargo clippy"),
            params,
        };

        let resp = handle_command(cmd, 0, &mut state, &conn, &config, &sys).await;
        assert!(matches!(
            resp,
            ResponsePayload::Ok(OkPayload::CronAdded { .. })
        ));
        let entry = &state.crons[&CronId(1)];
        assert_eq!(entry.cwd_override, None);
        let snapshot = get_scope_snapshot_by_hash(&sys, entry.scope_hash)
            .await
            .expect("cron scope snapshot");
        assert_eq!(snapshot.cwd, cwd);
    }

    #[tokio::test]
    async fn cron_wrapper_param_is_stored_on_entry() {
        let (sys, _gw_rx, _sched_rx, _pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let mut config = Config::default();
        config.wrapper.enabled = false;
        let mut state = SchedulerState::new();
        let mut params = cue_core::command::ModeParams::new();
        params.insert("wrapper", cue_core::command::ParamValue::Bool(true));
        let cmd = ResolvedCommand::Cron {
            schedule: CronSchedule::Interval(std::time::Duration::from_secs(300)),
            chain: leaf("backup.sh"),
            params,
        };
        let resp = handle_command(cmd, 0, &mut state, &conn, &config, &sys).await;
        assert!(matches!(
            resp,
            ResponsePayload::Ok(OkPayload::CronAdded { .. })
        ));
        assert!(state.crons[&CronId(1)].wrapper_enabled);
    }

    #[tokio::test]
    async fn cron_add_and_list() {
        let (sys, _gw_rx, _sched_rx, _pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();
        let cmd = ResolvedCommand::Cron {
            schedule: CronSchedule::Interval(std::time::Duration::from_secs(300)),
            chain: leaf("backup.sh"),
            params: cue_core::command::ModeParams::new(),
        };
        let resp = handle_command(cmd, 0, &mut state, &conn, &config, &sys).await;
        assert!(matches!(
            resp,
            ResponsePayload::Ok(OkPayload::CronAdded { .. })
        ));
        assert_eq!(state.crons.len(), 1);

        // List crons.
        let list_resp =
            handle_command(ResolvedCommand::Crons, 0, &mut state, &conn, &config, &sys).await;
        if let ResponsePayload::Ok(OkPayload::CronList(list)) = list_resp {
            assert_eq!(list.len(), 1);
            assert_eq!(list[0].schedule, "every 5m");
            assert_eq!(list[0].status, CronStatus::Scheduled);
        } else {
            panic!("expected CronList");
        }
    }

    #[tokio::test]
    async fn cron_add_rejects_blocked_chain_without_registering() {
        let (sys, _gw_rx, _sched_rx, _pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();
        let cmd = ResolvedCommand::Cron {
            schedule: CronSchedule::Interval(std::time::Duration::from_secs(300)),
            chain: leaf("git commit --no-verify"),
            params: cue_core::command::ModeParams::new(),
        };

        let resp = handle_command(cmd, 0, &mut state, &conn, &config, &sys).await;

        match resp {
            ResponsePayload::Err { code, message } => {
                assert_eq!(code, error_code::BLOCKED);
                assert!(message.contains("git --no-verify"));
            }
            other => panic!("expected blocked cron response, got {other:?}"),
        }
        assert!(state.crons.is_empty());
    }

    #[tokio::test]
    async fn due_cron_blocked_by_guardrail_fails_without_spawning_job() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let scope_hash = insert_test_scope(&conn, "guardrail-cron");
        let config = Config::default();
        let mut state = SchedulerState::new();
        state.crons.insert(
            CronId(1),
            CronEntry {
                cron_id: CronId(1),
                schedule: CronSchedule::Delay(std::time::Duration::from_secs(1)),
                chain: leaf("git commit --no-verify"),
                scope_hash,
                status: CronStatus::Scheduled,
                next_trigger: Instant::now(),
                cwd_override: None,
                scope_enabled: false,
                wrapper_enabled: false,
                session_id: None,
            },
        );

        fire_due_crons(&mut state, &conn, &config, &sys).await;

        assert_eq!(state.crons[&CronId(1)].status, CronStatus::Failed);
        assert!(drain_spawn_jobs(&mut pm_rx).await.is_empty());
    }

    #[tokio::test]
    async fn due_one_shot_cron_spawn_failure_marks_failed_not_completed() {
        let (sys, _gw_rx, _sched_rx, pm_rx, ss_rx, _eb_rx) = test_actor_system();
        drop(pm_rx);
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let scope_hash = insert_test_scope(&conn, "spawn-failure-cron");
        let config = Config::default();
        let mut state = SchedulerState::new();
        state.crons.insert(
            CronId(1),
            CronEntry {
                cron_id: CronId(1),
                schedule: CronSchedule::Delay(std::time::Duration::from_secs(1)),
                chain: leaf("echo due"),
                scope_hash,
                status: CronStatus::Scheduled,
                next_trigger: Instant::now(),
                cwd_override: None,
                scope_enabled: false,
                wrapper_enabled: false,
                session_id: None,
            },
        );

        fire_due_crons(&mut state, &conn, &config, &sys).await;

        assert_eq!(state.crons[&CronId(1)].status, CronStatus::Failed);
        let persisted = storage::with_connection(&conn, storage::load_crons)
            .await
            .expect("load crons");
        assert_eq!(persisted[0].record.status, CronStatus::Failed);
    }

    #[tokio::test]
    async fn cron_add_reports_persist_failure_without_registering() {
        let (sys, _gw_rx, _sched_rx, _pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        drop_crons_table(&conn);
        let config = Config::default();
        let mut state = SchedulerState::new();
        let cmd = ResolvedCommand::Cron {
            schedule: CronSchedule::Interval(std::time::Duration::from_secs(300)),
            chain: leaf("backup.sh"),
            params: cue_core::command::ModeParams::new(),
        };
        let resp = handle_command(cmd, 0, &mut state, &conn, &config, &sys).await;

        match resp {
            ResponsePayload::Err { code, message } => {
                assert_eq!(code, error_code::INTERNAL);
                assert!(message.contains("persist cron C1"));
            }
            other => panic!("expected cron persist failure, got {other:?}"),
        }
        assert!(state.crons.is_empty());
    }

    #[tokio::test]
    async fn remove_cron_reports_persist_failure_without_removing_state() {
        let (sys, _gw_rx, _sched_rx, _pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();
        let cmd = ResolvedCommand::Cron {
            schedule: CronSchedule::Interval(std::time::Duration::from_secs(300)),
            chain: leaf("backup.sh"),
            params: cue_core::command::ModeParams::new(),
        };
        let added = handle_command(cmd, 0, &mut state, &conn, &config, &sys).await;
        assert!(matches!(
            added,
            ResponsePayload::Ok(OkPayload::CronAdded { .. })
        ));
        drop_crons_table(&conn);

        let removed = handle_command(
            ResolvedCommand::RemoveCron { id: "C1".into() },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        match removed {
            ResponsePayload::Err { code, message } => {
                assert_eq!(code, error_code::INTERNAL);
                assert!(message.contains("remove cron C1"));
            }
            other => panic!("expected cron remove failure, got {other:?}"),
        }
        assert!(state.crons.contains_key(&CronId(1)));
    }

    #[tokio::test]
    async fn pause_cron_reports_persist_failure_without_mutating_status() {
        let (sys, _gw_rx, _sched_rx, _pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();
        let cmd = ResolvedCommand::Cron {
            schedule: CronSchedule::Interval(std::time::Duration::from_secs(300)),
            chain: leaf("backup.sh"),
            params: cue_core::command::ModeParams::new(),
        };
        let added = handle_command(cmd, 0, &mut state, &conn, &config, &sys).await;
        assert!(matches!(
            added,
            ResponsePayload::Ok(OkPayload::CronAdded { .. })
        ));
        drop_crons_table(&conn);

        let paused = handle_command(
            ResolvedCommand::Pause { id: "C1".into() },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        match paused {
            ResponsePayload::Err { code, message } => {
                assert_eq!(code, error_code::INTERNAL);
                assert!(message.contains("persist cron C1"));
            }
            other => panic!("expected cron pause failure, got {other:?}"),
        }
        assert_eq!(state.crons[&CronId(1)].status, CronStatus::Scheduled);
    }

    #[tokio::test]
    async fn cron_pause_and_resume() {
        let (sys, _gw_rx, _sched_rx, _pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();
        let cmd = ResolvedCommand::Cron {
            schedule: CronSchedule::Interval(std::time::Duration::from_secs(3600)),
            chain: leaf("check.sh"),
            params: cue_core::command::ModeParams::new(),
        };
        let _ = handle_command(cmd, 0, &mut state, &conn, &config, &sys).await;

        // Pause.
        let pause = handle_command(
            ResolvedCommand::Pause { id: "C1".into() },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        assert!(matches!(pause, ResponsePayload::Ok(OkPayload::Ack {})));
        assert_eq!(state.crons[&CronId(1)].status, CronStatus::Paused);

        // Resume.
        let resume = handle_command(
            ResolvedCommand::Resume { id: "C1".into() },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        assert!(matches!(resume, ResponsePayload::Ok(OkPayload::Ack {})));
        assert_eq!(state.crons[&CronId(1)].status, CronStatus::Scheduled);
    }

    #[tokio::test]
    async fn job_tracking_after_spawn_and_finish() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();
        let chain = leaf("ls -la");

        let resp = spawn_chain(
            test_chain_spawn(chain, ScopeHash([0; 32])),
            &mut state,
            SchedulerIo::new(&conn, &sys),
        )
        .await;
        assert!(matches!(
            resp,
            ResponsePayload::Ok(OkPayload::JobCreated { .. })
        ));

        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(spawned.len(), 1);
        let jid = spawned[0];

        // Job should appear in :jobs listing as Running.
        let list_resp =
            handle_command(ResolvedCommand::Jobs, 0, &mut state, &conn, &config, &sys).await;
        if let ResponsePayload::Ok(OkPayload::JobList(list)) = &list_resp {
            assert_eq!(list.len(), 1);
            assert_eq!(list[0].status, JobStatus::Running);
        } else {
            panic!("expected JobList");
        }

        // Finish the job.
        handle_job_finished(jid, 0, &mut state, &conn, &sys).await;

        // Job should now be Done.
        let list_resp2 =
            handle_command(ResolvedCommand::Jobs, 0, &mut state, &conn, &config, &sys).await;
        if let ResponsePayload::Ok(OkPayload::JobList(list)) = &list_resp2 {
            assert_eq!(list[0].status, JobStatus::Done);
            assert_eq!(list[0].exit_code, Some(0));
        } else {
            panic!("expected JobList");
        }
    }

    #[tokio::test]
    async fn typed_list_jobs_returns_page_metadata() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();
        for command in ["echo one", "echo two"] {
            let _ = spawn_chain(
                test_chain_spawn(leaf(command), ScopeHash([0; 32])),
                &mut state,
                SchedulerIo::new(&conn, &sys),
            )
            .await;
        }
        let _ = drain_spawn_jobs(&mut pm_rx).await;

        let resp = handle_command(
            ResolvedCommand::ListJobs { limit: Some(1) },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        match resp {
            ResponsePayload::Ok(OkPayload::JobListPage { jobs, page }) => {
                assert_eq!(jobs.len(), 1);
                assert_eq!(page.total, 2);
                assert_eq!(page.shown, 1);
                assert_eq!(page.limit, Some(1));
                assert!(page.truncated);
            }
            other => panic!("expected paged job list, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn typed_list_scopes_returns_page_metadata() {
        let (sys, _gw_rx, _sched_rx, _pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let resp = handle_list_scopes_page(&sys, Some(1)).await;
        match resp {
            ResponsePayload::Ok(OkPayload::ScopeListPage { scopes, page }) => {
                assert_eq!(scopes.len(), 1);
                assert_eq!(page.total, 2);
                assert_eq!(page.shown, 1);
                assert_eq!(page.limit, Some(1));
                assert!(page.truncated);
            }
            other => panic!("expected paged scope list, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn typed_cron_remove_is_separate_from_job_kill() {
        let (sys, _gw_rx, _sched_rx, _pm_rx, ss_rx, mut eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();
        let cmd = ResolvedCommand::Cron {
            schedule: CronSchedule::Interval(std::time::Duration::from_secs(300)),
            chain: leaf("backup.sh"),
            params: cue_core::command::ModeParams::new(),
        };
        let _ = handle_command(cmd, 0, &mut state, &conn, &config, &sys).await;
        let list_resp = handle_command(
            ResolvedCommand::ListCrons { limit: Some(1) },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        match list_resp {
            ResponsePayload::Ok(OkPayload::CronListPage { crons, page }) => {
                assert_eq!(crons.len(), 1);
                assert_eq!(page.total, 1);
                assert!(!page.truncated);
            }
            other => panic!("expected paged cron list, got {other:?}"),
        }

        let wrong_kind = handle_command(
            ResolvedCommand::KillJob { id: "C1".into() },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        assert!(
            matches!(wrong_kind, ResponsePayload::Err { code, .. } if code == error_code::NOT_SUPPORTED)
        );
        assert_eq!(state.crons.len(), 1);

        let removed = handle_command(
            ResolvedCommand::RemoveCron { id: "C1".into() },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        assert!(matches!(removed, ResponsePayload::Ok(OkPayload::Ack {})));
        assert!(state.crons.is_empty());

        let event = tokio::time::timeout(std::time::Duration::from_secs(5), eb_rx.recv())
            .await
            .expect("cron removed event timeout")
            .expect("event bus channel closed");
        match event {
            EventBusMsg::PublishSession {
                channel,
                session_id,
                payload: EventPayload::CronRemoved { cron_id },
            } => {
                assert_eq!(channel, EventChannel::Crons);
                assert_eq!(session_id, None);
                assert_eq!(cron_id, "C1");
            }
            _ => panic!("expected CronRemoved event"),
        }
    }

    #[tokio::test]
    async fn typed_job_output_uses_independent_stdout_stderr_limits() {
        let (sys, _gw_rx, _sched_rx, pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);
        spawn_fake_process_mgr(pm_rx);

        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();
        let resp = handle_command(
            ResolvedCommand::JobOutput {
                id: "J1".into(),
                stdout_bytes: Some(4),
                stderr_bytes: Some(6),
            },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        match resp {
            ResponsePayload::Ok(OkPayload::JobOutput { stdout, stderr, .. }) => {
                assert_eq!(stdout.data, "data");
                assert!(stdout.truncated);
                assert_eq!(stderr.data, "r-data");
                assert!(stderr.truncated);
            }
            other => panic!("expected job output, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn out_rejects_tail_limit_above_response_boundary_before_process_lookup() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, _ss_rx, _eb_rx) = test_actor_system();
        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();

        let resp = handle_command(
            ResolvedCommand::Out {
                id: "J1".into(),
                tail_bytes: Some(MAX_OUTPUT_TAIL_BYTES + 1),
            },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;

        assert_invalid_tail_limit(resp, "tail_bytes");
        assert!(
            pm_rx.try_recv().is_err(),
            "invalid output tail request must not reach process manager"
        );
    }

    #[tokio::test]
    async fn typed_job_output_rejects_tail_limits_above_response_boundary_before_process_lookup() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, _ss_rx, _eb_rx) = test_actor_system();
        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();

        let oversized_stdout = handle_command(
            ResolvedCommand::JobOutput {
                id: "J1".into(),
                stdout_bytes: Some(MAX_OUTPUT_TAIL_BYTES + 1),
                stderr_bytes: Some(1),
            },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        assert_invalid_tail_limit(oversized_stdout, "stdout_bytes");

        let oversized_stderr = handle_command(
            ResolvedCommand::JobOutput {
                id: "J1".into(),
                stdout_bytes: Some(1),
                stderr_bytes: Some(MAX_OUTPUT_TAIL_BYTES + 1),
            },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        assert_invalid_tail_limit(oversized_stderr, "stderr_bytes");
        assert!(
            pm_rx.try_recv().is_err(),
            "invalid typed output tail requests must not reach process manager"
        );
    }

    fn assert_invalid_tail_limit(resp: ResponsePayload, field: &str) {
        match resp {
            ResponsePayload::Err { code, message } => {
                assert_eq!(code, error_code::INVALID_SYNTAX);
                assert!(message.contains(field), "{message}");
                assert!(
                    message.contains(&MAX_OUTPUT_TAIL_BYTES.to_string()),
                    "{message}"
                );
            }
            other => panic!("expected invalid tail limit error, got {other:?}"),
        }
    }

    #[test]
    fn buffered_binary_output_keeps_exact_base64_and_explicit_encoding() {
        let encoded = encode_output(vec![0xff, b'b', b'i', b'n'], true);

        assert_eq!(encoded.encoding, OutputEncoding::Base64);
        assert_eq!(encoded.base64.as_deref(), Some("/2Jpbg=="));
        assert_eq!(encoded.data, "�bin");
        assert!(encoded.truncated);
    }

    #[test]
    fn log_result_reports_missing_output_only_for_not_found() {
        let missing = output_from_log_result(
            "J7".into(),
            Err(io::Error::new(io::ErrorKind::NotFound, "missing")),
        );
        assert!(
            matches!(missing, ResponsePayload::Err { code, message } if code == error_code::NOT_FOUND && message.contains("no output found"))
        );

        let denied = output_from_log_result(
            "J7".into(),
            Err(io::Error::new(io::ErrorKind::PermissionDenied, "denied")),
        );
        assert!(
            matches!(denied, ResponsePayload::Err { code, message } if code == error_code::INTERNAL && message.contains("read job log for J7") && message.contains("denied"))
        );
    }

    #[test]
    fn read_log_tail_reads_requested_suffix_without_loading_full_file_contract() {
        let path = std::env::temp_dir().join(format!(
            "cue-read-log-tail-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, b"abcdefghij").expect("write temp log");

        let tail = read_log_tail(path.clone(), 4).expect("read tail");
        assert_eq!(tail.data, b"ghij");
        assert!(tail.truncated);

        let all = read_log_tail(path.clone(), 20).expect("read full log through tail helper");
        assert_eq!(all.data, b"abcdefghij");
        assert!(!all.truncated);

        let empty_tail = read_log_tail(path.clone(), 0).expect("read empty tail");
        assert_eq!(empty_tail.data, b"");
        assert!(empty_tail.truncated);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn typed_text_limit_applies_tail_then_line_limit() {
        let (text, truncated) = limit_text("a\nb\nc\nd".to_string(), Some(2), Some(5));
        assert_eq!(text, "c\nd");
        assert!(truncated);
    }

    #[test]
    fn typed_text_tail_zero_returns_empty_text() {
        let (text, truncated) = limit_text("abc".to_string(), None, Some(0));
        assert_eq!(text, "");
        assert!(truncated);

        let (text, truncated) = limit_text(String::new(), None, Some(0));
        assert_eq!(text, "");
        assert!(!truncated);
    }

    #[test]
    fn restore_jobs_resumes_next_job_id() {
        let conn = test_db();
        let scope_hash = insert_test_scope(&conn, "restored-job");
        let guard = conn.lock().unwrap();
        storage::upsert_job_history(
            &guard,
            &storage::StoredJob {
                id: "J7".into(),
                session_id: None,
                pipeline: "cargo test".into(),
                status: JobStatus::Done,
                exit_code: Some(0),
                start_scope: Some(scope_hash),
                end_scope: Some(scope_hash),
                chain_id: None,
                stderr: String::new(),
            },
        )
        .unwrap();
        drop(guard);

        let mut state = SchedulerState::new();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(restore_jobs(&conn, &mut state)).unwrap();

        assert_eq!(state.next_job, 8);
        assert_eq!(state.jobs[&JobId(7)].pipeline_text, "cargo test");
        assert_eq!(state.jobs[&JobId(7)].status, JobStatus::Done);
    }

    #[test]
    fn restore_jobs_fails_closed_for_nonterminal_history_without_process_ownership() {
        let conn = test_db();
        let scope_hash = insert_test_scope(&conn, "interrupted-job");
        for (id, status) in [("J3", JobStatus::Running), ("J4", JobStatus::Pending)] {
            storage::upsert_job_history(
                &conn.lock().unwrap(),
                &storage::StoredJob {
                    id: id.into(),
                    session_id: None,
                    pipeline: "sleep 30".into(),
                    status,
                    exit_code: None,
                    start_scope: Some(scope_hash),
                    end_scope: None,
                    chain_id: None,
                    stderr: String::new(),
                },
            )
            .unwrap();
        }

        let mut state = SchedulerState::new();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(restore_jobs(&conn, &mut state)).unwrap();

        assert_eq!(state.jobs[&JobId(3)].status, JobStatus::Killed);
        assert_eq!(state.jobs[&JobId(4)].status, JobStatus::Killed);
        let persisted = storage::load_job_history(&conn.lock().unwrap()).unwrap();
        assert!(persisted.iter().all(|job| job.status == JobStatus::Killed));
    }

    #[test]
    fn restore_jobs_rejects_invalid_persisted_job_id() {
        let conn = test_db();
        conn.lock()
            .unwrap()
            .execute(
                "INSERT INTO jobs_history (id, pipeline, status) VALUES ('not-a-job', 'echo hi', '\"Done\"')",
                [],
            )
            .unwrap();

        let mut state = SchedulerState::new();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let error = rt
            .block_on(restore_jobs(&conn, &mut state))
            .expect_err("invalid job ids must not be silently skipped");

        assert!(error.to_string().contains("load persisted job history"));
        assert!(error.to_string().contains("invalid job history id"));
        assert!(state.jobs.is_empty());
    }

    #[test]
    fn restore_script_counter_rejects_invalid_persisted_script_id() {
        let conn = test_db();
        conn.lock()
            .unwrap()
            .execute(
                "INSERT INTO script_runs (id, mode, input, status, item_count)
                 VALUES ('not-a-script', 'job', 'echo hi', 'submitted', 1)",
                [],
            )
            .unwrap();

        let mut state = SchedulerState::new();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let error = rt
            .block_on(restore_script_counter(&conn, &mut state))
            .expect_err("invalid script ids must not be silently skipped");

        assert!(error.to_string().contains("restore script counter"));
        assert!(error.to_string().contains("invalid script run id"));
        assert_eq!(state.next_script, 1);
    }

    #[test]
    fn restore_crons_resumes_next_cron_id() {
        let conn = test_db();
        let scope_hash = insert_test_scope(&conn, "restored-cron");
        let guard = conn.lock().unwrap();
        storage::upsert_cron(
            &guard,
            &storage::StoredCron {
                id: "C4".into(),
                session_id: None,
                schedule: "every 5m".into(),
                command: "echo hello".into(),
                status: CronStatus::Scheduled,
                scope_hash: Some(scope_hash),
                cwd_override: Some(std::path::PathBuf::from("/tmp/cue-cron-cwd")),
                scope_enabled: true,
                wrapper_enabled: true,
            },
        )
        .unwrap();
        drop(guard);

        let mut state = SchedulerState::new();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(restore_crons(&conn, &mut state)).unwrap();

        assert_eq!(state.next_cron, 5);
        assert!(state.crons.contains_key(&CronId(4)));
        assert_eq!(state.crons[&CronId(4)].schedule.display(), "every 5m");
        assert_eq!(state.crons[&CronId(4)].status, CronStatus::Scheduled);
        assert_eq!(
            state.crons[&CronId(4)].cwd_override.as_deref(),
            Some(std::path::Path::new("/tmp/cue-cron-cwd"))
        );
        assert!(state.crons[&CronId(4)].scope_enabled);
        assert!(state.crons[&CronId(4)].wrapper_enabled);
    }

    #[test]
    fn restore_crons_expires_overdue_enabled_one_shot() {
        let conn = test_db();
        let scope_hash = insert_test_scope(&conn, "overdue-cron");
        let guard = conn.lock().unwrap();
        storage::upsert_cron(
            &guard,
            &storage::StoredCron {
                id: "C1".into(),
                session_id: None,
                schedule: "in 1s".into(),
                command: "echo late".into(),
                status: CronStatus::Scheduled,
                scope_hash: Some(scope_hash),
                cwd_override: None,
                scope_enabled: false,
                wrapper_enabled: false,
            },
        )
        .unwrap();
        guard
            .execute(
                "UPDATE crons
                 SET created_at_ms = CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER) - 5000
                 WHERE id = 'C1'",
                [],
            )
            .unwrap();
        drop(guard);

        let mut state = SchedulerState::new();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(restore_crons(&conn, &mut state)).unwrap();

        assert_eq!(state.crons.len(), 1);
        assert_eq!(state.crons[&CronId(1)].status, CronStatus::Expired);
        let guard = conn.lock().unwrap();
        let crons = storage::load_crons(&guard).unwrap();
        assert_eq!(crons.len(), 1);
        assert_eq!(crons[0].record.status, CronStatus::Expired);
    }

    #[test]
    fn restore_crons_preserves_fresh_subsecond_one_shot() {
        let conn = test_db();
        let scope_hash = insert_test_scope(&conn, "fresh-cron");
        let guard = conn.lock().unwrap();
        storage::upsert_cron(
            &guard,
            &storage::StoredCron {
                id: "C1".into(),
                session_id: None,
                schedule: "in 500ms".into(),
                command: "echo soon".into(),
                status: CronStatus::Scheduled,
                scope_hash: Some(scope_hash),
                cwd_override: None,
                scope_enabled: false,
                wrapper_enabled: false,
            },
        )
        .unwrap();
        drop(guard);

        let mut state = SchedulerState::new();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(restore_crons(&conn, &mut state)).unwrap();

        assert_eq!(state.crons.len(), 1);
        assert_eq!(state.crons[&CronId(1)].status, CronStatus::Scheduled);
        assert_eq!(state.crons[&CronId(1)].schedule.display(), "in 500ms");
        let guard = conn.lock().unwrap();
        let crons = storage::load_crons(&guard).unwrap();
        assert_eq!(crons[0].record.status, CronStatus::Scheduled);
    }

    #[test]
    fn restore_crons_expires_millisecond_overdue_one_shot() {
        let conn = test_db();
        let scope_hash = insert_test_scope(&conn, "millisecond-overdue-cron");
        let guard = conn.lock().unwrap();
        storage::upsert_cron(
            &guard,
            &storage::StoredCron {
                id: "C1".into(),
                session_id: None,
                schedule: "in 1500ms".into(),
                command: "echo late".into(),
                status: CronStatus::Scheduled,
                scope_hash: Some(scope_hash),
                cwd_override: None,
                scope_enabled: false,
                wrapper_enabled: false,
            },
        )
        .unwrap();
        guard
            .execute(
                "UPDATE crons
                 SET created_at_ms = CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER) - 1800
                 WHERE id = 'C1'",
                [],
            )
            .unwrap();
        drop(guard);

        let mut state = SchedulerState::new();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(restore_crons(&conn, &mut state)).unwrap();

        assert_eq!(state.crons.len(), 1);
        assert_eq!(state.crons[&CronId(1)].status, CronStatus::Expired);
    }

    #[test]
    fn restore_crons_skips_paused_cron_with_removed_sensitive_scope() {
        let conn = test_db();
        storage::upsert_cron(
            &conn.lock().unwrap(),
            &storage::StoredCron {
                id: "C1".into(),
                session_id: None,
                schedule: "every 5m".into(),
                command: "echo secret".into(),
                status: CronStatus::Paused,
                scope_hash: None,
                cwd_override: None,
                scope_enabled: true,
                wrapper_enabled: false,
            },
        )
        .unwrap();

        let mut state = SchedulerState::new();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(restore_crons(&conn, &mut state)).unwrap();

        assert!(state.crons.is_empty());
        assert_eq!(state.next_cron, 2);
    }

    #[test]
    fn restore_crons_rejects_invalid_persisted_cron_id() {
        let conn = test_db();
        conn.lock()
            .unwrap()
            .execute(
                "INSERT INTO crons (id, schedule, command, enabled, scope_hash, status, created_at_ms)
                 VALUES ('not-a-cron', 'every 5m', 'echo hi', 1, ?1, '\"scheduled\"', CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))",
                rusqlite::params![vec![9u8; 32]],
            )
            .unwrap();

        let mut state = SchedulerState::new();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let error = rt
            .block_on(restore_crons(&conn, &mut state))
            .expect_err("invalid cron ids must not be silently skipped");

        assert!(error.to_string().contains("load persisted crons"));
        assert!(error.to_string().contains("invalid cron id"));
        assert!(state.crons.is_empty());
    }

    #[tokio::test]
    async fn single_leaf_no_chain_tracking() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let mut state = SchedulerState::new();
        let chain = leaf("echo hello");

        let resp = spawn_chain(
            test_chain_spawn(chain, ScopeHash([0; 32])),
            &mut state,
            SchedulerIo::new(&conn, &sys),
        )
        .await;
        // Single leaf → JobCreated, not ChainCreated.
        assert!(matches!(
            resp,
            ResponsePayload::Ok(OkPayload::JobCreated { .. })
        ));
        assert!(state.chains.is_empty());

        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(spawned.len(), 1);
    }

    #[tokio::test]
    async fn chain_created_response_includes_snapshot() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let mut state = SchedulerState::new();
        let chain = ChainNode::Serial {
            left: Box::new(leaf("echo a")),
            op: SerialOp::Then,
            right: Box::new(leaf("echo b")),
        };

        let resp = spawn_chain(
            test_chain_spawn(chain, ScopeHash([0; 32])),
            &mut state,
            SchedulerIo::new(&conn, &sys),
        )
        .await;
        let chain = match resp {
            ResponsePayload::Ok(OkPayload::ChainCreated { chain, .. }) => chain,
            other => panic!("expected ChainCreated, got {other:?}"),
        };
        assert_eq!(chain.total_jobs, 2);
        assert_eq!(chain.jobs[0].job_id.as_deref(), Some("J1"));
        assert_eq!(chain.jobs[1].job_id, None);
        assert_eq!(chain.jobs[0].status, JobStatus::Running);
        assert_eq!(chain.jobs[1].status, JobStatus::Pending);

        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(spawned.len(), 1);
    }

    #[tokio::test]
    async fn wait_job_response_is_deferred_until_terminal() {
        let (sys, mut gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let mut state = SchedulerState::new();
        let resp = spawn_chain(
            test_chain_spawn(leaf("echo hello"), ScopeHash([0; 32])),
            &mut state,
            SchedulerIo::new(&conn, &sys),
        )
        .await;
        let job_id = match resp {
            ResponsePayload::Ok(OkPayload::JobCreated { job_id, .. }) => job_id,
            other => panic!("expected JobCreated, got {other:?}"),
        };
        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        let jid = spawned[0];

        assert!(
            handle_wait_command(job_id.clone(), 7, 42, &mut state)
                .await
                .is_none()
        );

        handle_job_finished(jid, 0, &mut state, &conn, &sys).await;

        loop {
            if let GatewayMsg::SendResponse {
                client_id,
                request_id,
                payload: ResponsePayload::Ok(OkPayload::JobInfo(info)),
            } = recv_gateway_msg(&mut gw_rx).await
            {
                assert_eq!(client_id, 7);
                assert_eq!(request_id, 42);
                assert_eq!(info.id, job_id);
                assert_eq!(info.status, JobStatus::Done);
                break;
            }
        }
    }

    #[tokio::test]
    async fn retry_respawns_terminal_job() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();

        let resp = spawn_chain(
            test_chain_spawn(leaf("echo hello"), ScopeHash([0; 32])),
            &mut state,
            SchedulerIo::new(&conn, &sys),
        )
        .await;
        let job_id = match resp {
            ResponsePayload::Ok(OkPayload::JobCreated { job_id, .. }) => job_id,
            other => panic!("expected JobCreated, got {other:?}"),
        };
        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        let jid = spawned[0];
        handle_job_finished(jid, 1, &mut state, &conn, &sys).await;

        let retry = handle_command(
            ResolvedCommand::Retry { id: job_id },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        assert!(matches!(
            retry,
            ResponsePayload::Ok(OkPayload::JobCreated { .. })
        ));
        let retried = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(retried.len(), 1);
    }

    #[test]
    fn parse_job_id_valid() {
        assert_eq!(parse_job_id("J1"), Some(JobId(1)));
        assert_eq!(parse_job_id("J42"), Some(JobId(42)));
    }

    #[test]
    fn parse_job_id_invalid() {
        assert_eq!(parse_job_id("C1"), None);
        assert_eq!(parse_job_id("J+1"), None);
        assert_eq!(parse_job_id("foo"), None);
    }

    #[test]
    fn parse_cron_id_valid() {
        assert_eq!(parse_cron_id("C1"), Some(CronId(1)));
        assert_eq!(parse_cron_id("C99"), Some(CronId(99)));
    }

    #[test]
    fn parse_cron_id_invalid() {
        assert_eq!(parse_cron_id("J1"), None);
        assert_eq!(parse_cron_id("C+1"), None);
    }

    #[test]
    fn parse_chain_id_uses_core_id_parser() {
        assert_eq!(parse_chain_id("CH7"), Some(ChainId(7)));
        assert_eq!(parse_chain_id("C7"), None);
        assert_eq!(parse_chain_id("CH+7"), None);
    }

    #[test]
    fn flatten_leaves_serial() {
        let chain = ChainNode::Serial {
            left: Box::new(leaf("a")),
            op: SerialOp::Then,
            right: Box::new(leaf("b")),
        };
        let leaves = flatten_leaves(&chain);
        assert_eq!(leaves.len(), 2);
        assert_eq!(leaves[0].index, 0);
        assert_eq!(leaves[1].index, 1);
    }

    #[test]
    fn initially_ready_serial() {
        let chain = ChainNode::Serial {
            left: Box::new(leaf("a")),
            op: SerialOp::Then,
            right: Box::new(leaf("b")),
        };
        let ready = initially_ready(&chain);
        assert_eq!(ready, vec![0]); // Only left is ready.
    }

    #[test]
    fn initially_ready_parallel() {
        let chain = ChainNode::Parallel {
            left: Box::new(leaf("a")),
            op: ParallelOp::All,
            right: Box::new(leaf("b")),
        };
        let ready = initially_ready(&chain);
        assert_eq!(ready, vec![0, 1]); // Both ready.
    }

    // ── Race + Serial: cancelled leaf must not be re-spawned ──

    /// `(a -> b) |?| c` — when `c` succeeds, Race should cancel both `a`/`b`.
    /// When `a` also succeeds, `b` should NOT be spawned because it was cancelled.
    #[tokio::test]
    async fn race_serial_cancelled_leaf_not_respawned() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let mut state = SchedulerState::new();
        // (a -> b) |?| c
        // Leaves: 0=a, 1=b, 2=c
        let chain = ChainNode::Parallel {
            left: Box::new(ChainNode::Serial {
                left: Box::new(leaf("a")),
                op: SerialOp::Then,
                right: Box::new(leaf("b")),
            }),
            op: ParallelOp::Race,
            right: Box::new(leaf("c")),
        };

        let _ = spawn_chain(
            test_chain_spawn(chain, ScopeHash([0; 32])),
            &mut state,
            SchedulerIo::new(&conn, &sys),
        )
        .await;
        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        // Initially ready: a (idx 0) and c (idx 2).
        assert_eq!(spawned.len(), 2);
        let a_jid = spawned[0]; // leaf 0 = a
        let c_jid = spawned[1]; // leaf 2 = c

        // c succeeds first → Race fires, cancels a (running) and b (pending).
        let finish_fut = handle_job_finished(c_jid, 0, &mut state, &conn, &sys);
        let (_, killed) = tokio::join!(finish_fut, ack_next_kill(&mut pm_rx));
        assert_eq!(killed, a_jid, "a should have been killed");

        // Now a finishes (process exits after kill signal).
        handle_job_finished(a_jid, 0, &mut state, &conn, &sys).await;

        // b should NOT be spawned — it was already cancelled by Race.
        let after = drain_spawn_jobs(&mut pm_rx).await;
        assert!(after.is_empty(), "b must not be spawned after cancellation");

        // Chain should be complete.
        assert!(state.chains.is_empty(), "chain should be cleaned up");
    }

    // ── Race waits for entire branch, not single leaf ──

    /// `(compile -> test) |?| lint`
    /// When `compile` succeeds but `test` hasn't run yet, Race should NOT fire.
    #[tokio::test]
    async fn race_does_not_fire_on_partial_branch_success() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let mut state = SchedulerState::new();
        // (compile -> test) |?| lint
        // Leaves: 0=compile, 1=test, 2=lint
        let chain = ChainNode::Parallel {
            left: Box::new(ChainNode::Serial {
                left: Box::new(leaf("compile")),
                op: SerialOp::Then,
                right: Box::new(leaf("test")),
            }),
            op: ParallelOp::Race,
            right: Box::new(leaf("lint")),
        };

        let _ = spawn_chain(
            test_chain_spawn(chain, ScopeHash([0; 32])),
            &mut state,
            SchedulerIo::new(&conn, &sys),
        )
        .await;
        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        // Initially ready: compile (idx 0) and lint (idx 2).
        assert_eq!(spawned.len(), 2);
        let compile_jid = spawned[0]; // leaf 0 = compile

        // compile succeeds → test should become ready, Race must NOT fire yet.
        handle_job_finished(compile_jid, 0, &mut state, &conn, &sys).await;

        // test should have been spawned.
        let after_compile = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(after_compile.len(), 1, "test should be spawned");

        // lint should still be running (not cancelled by Race).
        let chain_st = state.chains.values().next().unwrap();
        assert!(
            matches!(chain_st.leaf_status.get(&2), Some(LeafStatus::Running)),
            "lint should still be running — Race should not have fired yet"
        );
    }

    // ── :cancel updates chain leaf_status and advances chain ──

    #[tokio::test]
    async fn kill_running_job_reports_process_mgr_rejection_without_marking_killed() {
        let (sys, _gw_rx, _sched_rx, pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);
        crate::actor::process_mgr::spawn(pm_rx, sys.clone());

        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();
        let jid = JobId(1);
        insert_running_test_job(&mut state, jid);

        let resp = handle_command(
            ResolvedCommand::Kill {
                id: jid.to_string(),
            },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;

        match resp {
            ResponsePayload::Err { code, message } => {
                assert_eq!(code, error_code::INTERNAL);
                assert!(message.contains("not found"));
            }
            other => panic!("expected process_mgr rejection, got {other:?}"),
        }
        assert_eq!(state.jobs[&jid].status, JobStatus::Running);
        assert_eq!(state.jobs[&jid].exit_code, None);

        sys.shutdown().await;
    }

    #[tokio::test]
    async fn cancel_running_job_reports_process_mgr_unreachable_without_marking_cancelled() {
        let (sys, _gw_rx, _sched_rx, pm_rx, ss_rx, _eb_rx) = test_actor_system();
        drop(pm_rx);
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();
        let jid = JobId(1);
        insert_running_test_job(&mut state, jid);

        let resp = handle_command(
            ResolvedCommand::Cancel {
                id: jid.to_string(),
            },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;

        match resp {
            ResponsePayload::Err { code, message } => {
                assert_eq!(code, error_code::INTERNAL);
                assert_eq!(message, "process_mgr unreachable");
            }
            other => panic!("expected process_mgr unreachable, got {other:?}"),
        }
        assert_eq!(state.jobs[&jid].status, JobStatus::Running);
        assert_eq!(state.jobs[&jid].exit_code, None);
    }

    #[tokio::test]
    async fn cancel_running_job_reports_history_persist_failure_after_kill_ack() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        drop_jobs_history_table(&conn);
        let config = Config::default();
        let mut state = SchedulerState::new();
        let jid = JobId(1);
        insert_running_test_job(&mut state, jid);

        let cancel_fut = handle_command(
            ResolvedCommand::Cancel {
                id: jid.to_string(),
            },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        );
        let (resp, killed) = tokio::join!(cancel_fut, ack_next_kill(&mut pm_rx));

        assert_eq!(killed, jid);
        match resp {
            ResponsePayload::Err { code, message } => {
                assert_eq!(code, error_code::INTERNAL);
                assert!(message.contains("persist job J1 history"));
                assert!(message.contains("no such table"));
            }
            other => panic!("expected job history persist failure, got {other:?}"),
        }
        assert_eq!(
            state.jobs[&jid].status,
            JobStatus::Cancelled(CancelReason::User)
        );
        assert_eq!(state.jobs[&jid].exit_code, Some(EXIT_CODE_UNAVAILABLE));
    }

    #[tokio::test]
    async fn cancel_chain_leaf_updates_leaf_status() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();
        // a -> b
        let chain = ChainNode::Serial {
            left: Box::new(leaf("a")),
            op: SerialOp::Always,
            right: Box::new(leaf("b")),
        };

        let _ = spawn_chain(
            test_chain_spawn(chain, ScopeHash([0; 32])),
            &mut state,
            SchedulerIo::new(&conn, &sys),
        )
        .await;
        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(spawned.len(), 1);
        let a_jid = spawned[0];

        // Cancel a via :cancel.
        let cancel_fut = handle_command(
            ResolvedCommand::Cancel {
                id: format!("J{}", a_jid.0),
            },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        );
        let (resp, killed) = tokio::join!(cancel_fut, ack_next_kill(&mut pm_rx));
        assert_eq!(killed, a_jid);
        assert!(matches!(resp, ResponsePayload::Ok(OkPayload::Ack {})));

        // Since the op is Always, b should become ready after a is cancelled.
        // The process_chain_advance sends both KillJob and SpawnJob to pm_rx.
        // Drain all messages and check.
        let after = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(
            after.len(),
            1,
            "b should be spawned via Always after cancel"
        );
    }

    // ── :kill does not get overwritten by later JobFinished ──

    #[tokio::test]
    async fn kill_status_not_overwritten_by_job_finished() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();
        let chain = leaf("long-running");

        let _ = spawn_chain(
            test_chain_spawn(chain, ScopeHash([0; 32])),
            &mut state,
            SchedulerIo::new(&conn, &sys),
        )
        .await;
        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        let jid = spawned[0];

        // Kill the job.
        let kill_fut = handle_command(
            ResolvedCommand::Kill {
                id: format!("J{}", jid.0),
            },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        );
        let (resp, killed) = tokio::join!(kill_fut, ack_next_kill(&mut pm_rx));
        assert_eq!(killed, jid);
        assert!(matches!(resp, ResponsePayload::Ok(OkPayload::Ack {})));
        assert_eq!(state.jobs[&jid].status, JobStatus::Killed);
        assert_eq!(state.jobs[&jid].exit_code, Some(EXIT_CODE_UNAVAILABLE));

        // Now the process exits (JobFinished arrives).
        handle_job_finished(jid, -9, &mut state, &conn, &sys).await;

        // Status should still be Killed, not overwritten to Failed.
        assert_eq!(
            state.jobs[&jid].status,
            JobStatus::Killed,
            "Killed status must not be overwritten by JobFinished"
        );
        assert_eq!(
            state.jobs[&jid].exit_code,
            Some(EXIT_CODE_UNAVAILABLE),
            "Killed exit code must not be overwritten by JobFinished"
        );
    }

    #[tokio::test]
    async fn named_session_archive_is_reversible_and_preserves_terminal_history() {
        let conn = test_db();
        let scope = insert_test_scope(&conn, "archive-lifecycle");
        let owner = "SS-archive";
        let mut state = SchedulerState::new();
        insert_anonymous_test_client(&mut state, 9, scope);
        insert_ready_named_test_session(&conn, &mut state, owner, "archive-me", scope, 0);
        state.jobs.insert(
            JobId(1),
            JobEntry {
                job_id: JobId(1),
                session_id: Some(owner.into()),
                pipeline_text: "echo retained".into(),
                status: JobStatus::Done,
                exit_code: Some(0),
                start_scope: Some(scope),
                end_scope: Some(scope),
                open_hint: JobOpenHint::Stream,
                chain_id: None,
                chain_index: None,
                chain_total: None,
                pending_reason: None,
            },
        );
        storage::upsert_job_history(
            &conn.lock().unwrap(),
            &storage::StoredJob {
                id: "J1".into(),
                session_id: Some(owner.into()),
                pipeline: "echo retained".into(),
                status: JobStatus::Done,
                exit_code: Some(0),
                start_scope: Some(scope),
                end_scope: Some(scope),
                chain_id: None,
                stderr: String::new(),
            },
        )
        .expect("persist terminal history");

        let archived = handle_session_command(
            9,
            SessionCommand::Archive {
                selector: "archive-me".into(),
            },
            &mut state,
            &conn,
        )
        .await;
        let archived_info = match archived.payload {
            ResponsePayload::Ok(OkPayload::SessionInfo(info)) => *info,
            other => panic!("unexpected archive response: {other:?}"),
        };
        assert!(archived_info.archived_at_ms.is_some());
        assert_eq!(state.jobs[&JobId(1)].status, JobStatus::Done);

        let repeated = handle_session_command(
            9,
            SessionCommand::Archive {
                selector: owner.into(),
            },
            &mut state,
            &conn,
        )
        .await;
        assert!(matches!(
            repeated.payload,
            ResponsePayload::Ok(OkPayload::SessionInfo(info))
                if info.archived_at_ms == archived_info.archived_at_ms
        ));

        for (command, expected_len) in [
            (SessionCommand::List, 0),
            (SessionCommand::ListArchived, 1),
            (SessionCommand::ListAll, 1),
        ] {
            let listed = handle_session_command(9, command, &mut state, &conn).await;
            assert!(matches!(
                listed.payload,
                ResponsePayload::Ok(OkPayload::SessionList(sessions))
                    if sessions.len() == expected_len
            ));
        }
        let info = handle_session_command(
            9,
            SessionCommand::Info {
                selector: Some(owner.into()),
            },
            &mut state,
            &conn,
        )
        .await;
        assert!(matches!(
            info.payload,
            ResponsePayload::Ok(OkPayload::SessionInfo(info)) if info.archived_at_ms.is_some()
        ));
        let attach = handle_session_command(
            9,
            SessionCommand::Attach {
                selector: owner.into(),
                refresh: false,
            },
            &mut state,
            &conn,
        )
        .await;
        assert!(matches!(
            attach.payload,
            ResponsePayload::Err { ref code, .. } if code == error_code::INVALID_STATE
        ));

        let mut restored = SchedulerState::new();
        restore_named_sessions(&conn, &mut restored)
            .await
            .expect("restore archived session");
        restore_jobs(&conn, &mut restored)
            .await
            .expect("restore terminal history");
        assert_eq!(restored.jobs[&JobId(1)].session_id.as_deref(), Some(owner));
        insert_anonymous_test_client(&mut restored, 10, scope);
        let still_archived = handle_session_command(
            10,
            SessionCommand::Attach {
                selector: owner.into(),
                refresh: false,
            },
            &mut restored,
            &conn,
        )
        .await;
        assert!(matches!(
            still_archived.payload,
            ResponsePayload::Err { ref code, .. } if code == error_code::INVALID_STATE
        ));

        let restored_response = handle_session_command(
            10,
            SessionCommand::Restore {
                selector: "archive-me".into(),
            },
            &mut restored,
            &conn,
        )
        .await;
        assert!(matches!(
            restored_response.payload,
            ResponsePayload::Ok(OkPayload::SessionInfo(info)) if info.archived_at_ms.is_none()
        ));
        let attached = handle_session_command(
            10,
            SessionCommand::Attach {
                selector: owner.into(),
                refresh: false,
            },
            &mut restored,
            &conn,
        )
        .await;
        assert!(matches!(
            attached.payload,
            ResponsePayload::Ok(OkPayload::SessionInfo(info)) if info.current
        ));
        assert_eq!(
            storage::load_sessions(&conn.lock().unwrap()).expect("load restored lifecycle")[0]
                .archived_at_ms,
            None
        );
    }

    #[tokio::test]
    async fn named_session_archive_rejects_every_live_work_category() {
        let conn = test_db();
        let scope = insert_test_scope(&conn, "archive-blockers");
        let owner = "SS-blocked";
        let key = named_session_key(owner);
        let mut state = SchedulerState::new();
        insert_anonymous_test_client(&mut state, 9, scope);
        insert_ready_named_test_session(&conn, &mut state, owner, "blocked", scope, 1);

        assert!(matches!(
            archive_test_session(&mut state, &conn, 9, owner)
                .await
                .payload,
            ResponsePayload::Err { ref code, .. } if code == error_code::INVALID_STATE
        ));
        state.sessions.get_mut(&key).unwrap().connected_clients = 0;

        state.jobs.insert(
            JobId(1),
            JobEntry {
                job_id: JobId(1),
                session_id: Some(owner.into()),
                pipeline_text: "sleep 1".into(),
                status: JobStatus::Pending,
                exit_code: None,
                start_scope: Some(scope),
                end_scope: None,
                open_hint: JobOpenHint::Stream,
                chain_id: None,
                chain_index: None,
                chain_total: None,
                pending_reason: Some("fixture".into()),
            },
        );
        assert!(matches!(
            archive_test_session(&mut state, &conn, 9, owner)
                .await
                .payload,
            ResponsePayload::Err { ref code, .. } if code == error_code::INVALID_STATE
        ));
        state.jobs.get_mut(&JobId(1)).unwrap().status = JobStatus::Running;
        assert!(matches!(
            archive_test_session(&mut state, &conn, 9, owner)
                .await
                .payload,
            ResponsePayload::Err { ref code, .. } if code == error_code::INVALID_STATE
        ));
        state.jobs.get_mut(&JobId(1)).unwrap().status = JobStatus::Done;

        state.chains.insert(
            ChainId(1),
            ChainState {
                node: leaf("echo chain"),
                leaf_jobs: HashMap::new(),
                leaf_status: HashMap::new(),
                scope_hash: scope,
                pipeline_text: "echo chain".into(),
                process: ProcessJobContext {
                    cwd_override: None,
                    launch: LaunchOptions::default(),
                    wrapper_enabled: false,
                    pty_default: false,
                    direct_output_client: None,
                },
                scope_enabled: false,
                session_id: Some(owner.into()),
            },
        );
        assert!(matches!(
            archive_test_session(&mut state, &conn, 9, owner)
                .await
                .payload,
            ResponsePayload::Err { ref code, .. } if code == error_code::INVALID_STATE
        ));
        state.chains.clear();

        state.pending_scripts.insert(
            ScriptId(1),
            PendingScriptRun {
                client_id: 9,
                script_id: ScriptId(1),
                mode: Mode::Job,
                source: ScriptSource::Inline,
                items: VecDeque::new(),
                next_index: 0,
                item_scope: scope,
                created_items: Vec::new(),
                last_exit_code: 0,
                waiting_index: None,
                session_id: Some(owner.into()),
            },
        );
        assert!(matches!(
            archive_test_session(&mut state, &conn, 9, owner)
                .await
                .payload,
            ResponsePayload::Err { ref code, .. } if code == error_code::INVALID_STATE
        ));
        state.pending_scripts.clear();

        state.crons.insert(
            CronId(1),
            CronEntry {
                cron_id: CronId(1),
                schedule: parse_schedule_text("every 1m").expect("valid schedule"),
                chain: leaf("echo cron"),
                scope_hash: scope,
                status: CronStatus::Paused,
                next_trigger: Instant::now(),
                cwd_override: None,
                scope_enabled: false,
                wrapper_enabled: false,
                session_id: Some(owner.into()),
            },
        );
        assert!(matches!(
            archive_test_session(&mut state, &conn, 9, owner)
                .await
                .payload,
            ResponsePayload::Err { ref code, .. } if code == error_code::INVALID_STATE
        ));
        assert!(
            state.sessions[&key]
                .named
                .as_ref()
                .unwrap()
                .archived_at_ms
                .is_none()
        );
        state.crons.clear();

        assert!(matches!(
            archive_test_session(&mut state, &conn, 9, owner)
                .await
                .payload,
            ResponsePayload::Ok(OkPayload::SessionInfo(info)) if info.archived_at_ms.is_some()
        ));
    }

    #[tokio::test]
    async fn needs_refresh_session_archive_is_safe_and_restore_does_not_refresh_scope() {
        let conn = test_db();
        let scope = insert_test_scope(&conn, "archive-needs-refresh");
        let owner = "SS-needs-refresh";
        storage::upsert_session(
            &conn.lock().unwrap(),
            &storage::StoredSession {
                id: owner.into(),
                name: "needs-refresh-archive".into(),
                scope_hash: None,
                pty_default: None,
                wrapper_enabled: None,
                created_at_ms: 1,
                updated_at_ms: 1,
                archived_at_ms: None,
            },
        )
        .expect("persist unavailable session");
        let mut state = SchedulerState::new();
        restore_named_sessions(&conn, &mut state)
            .await
            .expect("restore unavailable session");
        insert_anonymous_test_client(&mut state, 9, scope);
        state.crons.insert(
            CronId(1),
            CronEntry {
                cron_id: CronId(1),
                schedule: parse_schedule_text("every 1m").expect("valid schedule"),
                chain: leaf("echo cron"),
                scope_hash: scope,
                status: CronStatus::Paused,
                next_trigger: Instant::now(),
                cwd_override: None,
                scope_enabled: false,
                wrapper_enabled: false,
                session_id: Some(owner.into()),
            },
        );
        let blocked = handle_session_command(
            9,
            SessionCommand::Archive {
                selector: owner.into(),
            },
            &mut state,
            &conn,
        )
        .await;
        assert!(matches!(
            blocked.payload,
            ResponsePayload::Err { ref code, .. } if code == error_code::INVALID_STATE
        ));
        state.crons.clear();

        let archived = handle_session_command(
            9,
            SessionCommand::Archive {
                selector: owner.into(),
            },
            &mut state,
            &conn,
        )
        .await;
        assert!(matches!(
            archived.payload,
            ResponsePayload::Ok(OkPayload::SessionInfo(info))
                if info.archived_at_ms.is_some()
                    && info.scope_state == SessionScopeState::NeedsRefresh
        ));
        let archived_attach = handle_session_command(
            9,
            SessionCommand::Attach {
                selector: owner.into(),
                refresh: true,
            },
            &mut state,
            &conn,
        )
        .await;
        assert!(matches!(
            archived_attach.payload,
            ResponsePayload::Err { ref code, .. } if code == error_code::INVALID_STATE
        ));

        let restored = handle_session_command(
            9,
            SessionCommand::Restore {
                selector: owner.into(),
            },
            &mut state,
            &conn,
        )
        .await;
        assert!(matches!(
            restored.payload,
            ResponsePayload::Ok(OkPayload::SessionInfo(info))
                if info.archived_at_ms.is_none()
                    && info.scope_state == SessionScopeState::NeedsRefresh
        ));
        let no_refresh = handle_session_command(
            9,
            SessionCommand::Attach {
                selector: owner.into(),
                refresh: false,
            },
            &mut state,
            &conn,
        )
        .await;
        assert!(matches!(
            no_refresh.payload,
            ResponsePayload::Err { ref code, .. } if code == error_code::INVALID_STATE
        ));
        let refreshed = handle_session_command(
            9,
            SessionCommand::Attach {
                selector: owner.into(),
                refresh: true,
            },
            &mut state,
            &conn,
        )
        .await;
        assert!(matches!(
            refreshed.payload,
            ResponsePayload::Ok(OkPayload::SessionInfo(info))
                if info.scope_state == SessionScopeState::ReadyDurable && info.current
        ));
    }

    #[tokio::test]
    async fn archive_storage_failure_leaves_scheduler_state_active() {
        let conn = test_db();
        let scope = insert_test_scope(&conn, "archive-storage-failure");
        let owner = "SS-storage-failure";
        let key = named_session_key(owner);
        let mut state = SchedulerState::new();
        insert_anonymous_test_client(&mut state, 9, scope);
        insert_ready_named_test_session(&conn, &mut state, owner, "storage-failure", scope, 0);
        conn.lock()
            .unwrap()
            .execute_batch("DROP TABLE sessions;")
            .expect("break session storage");

        let response = handle_session_command(
            9,
            SessionCommand::Archive {
                selector: owner.into(),
            },
            &mut state,
            &conn,
        )
        .await;

        assert!(matches!(
            response.payload,
            ResponsePayload::Err { ref code, .. } if code == error_code::INTERNAL
        ));
        let meta = state.sessions[&key].named.as_ref().unwrap();
        assert!(meta.archived_at_ms.is_none());
        assert_eq!(meta.updated_at_ms, 1);
    }

    #[tokio::test]
    async fn named_session_is_shared_and_survives_disconnect_gc() {
        let conn = test_db();
        let scope = insert_test_scope(&conn, "named-shared");
        let mut state = SchedulerState::new();
        for (client_id, key) in [(1, "ephemeral-1"), (2, "ephemeral-2")] {
            state.client_sessions.insert(client_id, key.into());
            state.sessions.insert(
                key.into(),
                SessionState {
                    scope,
                    incarnation: client_id,
                    defaults: LaunchDefaults::default(),
                    connected_clients: 1,
                    disconnected_at: None,
                    named: None,
                },
            );
        }

        let created = handle_session_command(
            1,
            SessionCommand::Create {
                name: "shared-dev".into(),
            },
            &mut state,
            &conn,
        )
        .await;
        let created_info = match created.payload {
            ResponsePayload::Ok(OkPayload::SessionInfo(info)) => *info,
            other => panic!("unexpected create response: {other:?}"),
        };
        assert_eq!(created_info.scope_state, SessionScopeState::ReadyDurable);
        assert!(created_info.current);

        let attached = handle_session_command(
            2,
            SessionCommand::Attach {
                selector: "shared-dev".into(),
                refresh: false,
            },
            &mut state,
            &conn,
        )
        .await;
        let attached_info = match attached.payload {
            ResponsePayload::Ok(OkPayload::SessionInfo(info)) => *info,
            other => panic!("unexpected attach response: {other:?}"),
        };
        assert_eq!(attached_info.id, created_info.id);
        assert_eq!(attached_info.connected_clients, 2);
        assert_eq!(attached_info.scope_hash, Some(scope.to_string()));

        disconnect_session(1, &mut state);
        disconnect_session(2, &mut state);
        for session in state.sessions.values_mut() {
            if session.connected_clients == 0 {
                session.disconnected_at = Instant::now().checked_sub(SESSION_GC_TTL);
            }
        }
        assert_eq!(sweep_disconnected_sessions(&mut state), 2);
        let key = named_session_key(&created_info.id);
        assert!(state.sessions.contains_key(&key));
        assert_eq!(state.sessions[&key].connected_clients, 0);

        let stored = storage::load_sessions(&conn.lock().unwrap()).expect("load named session");
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].id, created_info.id);
    }

    #[tokio::test]
    async fn volatile_named_session_requires_explicit_refresh_after_restore() {
        let conn = test_db();
        let volatile_scope = ScopeHash([91; 32]);
        let mut state = SchedulerState::new();
        state.client_sessions.insert(1, "ephemeral".into());
        state.sessions.insert(
            "ephemeral".into(),
            SessionState {
                scope: volatile_scope,
                incarnation: 1,
                defaults: LaunchDefaults::default(),
                connected_clients: 1,
                disconnected_at: None,
                named: None,
            },
        );
        let created = handle_session_command(
            1,
            SessionCommand::Create {
                name: "volatile-agent".into(),
            },
            &mut state,
            &conn,
        )
        .await;
        let id = match created.payload {
            ResponsePayload::Ok(OkPayload::SessionInfo(info)) => {
                let info = *info;
                assert_eq!(info.scope_state, SessionScopeState::ReadyVolatile);
                info.id
            }
            other => panic!("unexpected create response: {other:?}"),
        };

        let mut restored = SchedulerState::new();
        restore_named_sessions(&conn, &mut restored)
            .await
            .expect("restore session metadata");
        assert!(restored.unavailable_named_sessions.contains_key(&id));
        restored.client_sessions.insert(9, "fresh-client".into());
        restored.sessions.insert(
            "fresh-client".into(),
            SessionState {
                scope: ScopeHash([92; 32]),
                incarnation: 9,
                defaults: LaunchDefaults::default(),
                connected_clients: 1,
                disconnected_at: None,
                named: None,
            },
        );

        let refused = handle_session_command(
            9,
            SessionCommand::Attach {
                selector: id.clone(),
                refresh: false,
            },
            &mut restored,
            &conn,
        )
        .await;
        assert!(matches!(
            refused.payload,
            ResponsePayload::Err { ref code, .. } if code == error_code::INVALID_STATE
        ));

        let refreshed = handle_session_command(
            9,
            SessionCommand::Attach {
                selector: id,
                refresh: true,
            },
            &mut restored,
            &conn,
        )
        .await;
        assert!(matches!(
            refreshed.payload,
            ResponsePayload::Ok(OkPayload::SessionInfo(info))
                if matches!(
                    *info,
                    SessionInfo {
                        scope_state: SessionScopeState::ReadyVolatile,
                        current: true,
                        ..
                    }
                )
        ));
    }

    #[tokio::test]
    async fn spawned_job_persists_named_session_owner() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);
        let conn = test_db();
        let scope = insert_test_scope(&conn, "owned-job");
        let owner = "SS-owned".to_string();
        storage::upsert_session(
            &conn.lock().unwrap(),
            &storage::StoredSession {
                id: owner.clone(),
                name: "owned".into(),
                scope_hash: Some(scope),
                pty_default: None,
                wrapper_enabled: None,
                created_at_ms: 1,
                updated_at_ms: 1,
                archived_at_ms: None,
            },
        )
        .expect("persist owner session");

        let mut request = test_chain_spawn(leaf("echo owned"), scope);
        request.session_id = Some(owner.clone());
        let mut state = SchedulerState::new();
        let response = spawn_chain(request, &mut state, SchedulerIo::new(&conn, &sys)).await;
        assert!(matches!(
            response,
            ResponsePayload::Ok(OkPayload::JobCreated { .. })
        ));
        let job_id = drain_spawn_jobs(&mut pm_rx).await[0];
        assert_eq!(
            state.jobs[&job_id].session_id.as_deref(),
            Some(owner.as_str())
        );

        handle_job_finished(job_id, 0, &mut state, &conn, &sys).await;
        let stored = storage::load_job_history(&conn.lock().unwrap()).expect("load job history");
        assert_eq!(stored[0].session_id.as_deref(), Some(owner.as_str()));
        assert_eq!(
            job_info_from_entry(&state.jobs[&job_id]).session_id,
            Some(owner)
        );
    }

    #[test]
    fn every_id_targeted_command_maps_to_a_session_owned_target() {
        let commands = vec![
            (
                ResolvedCommand::Kill { id: "J1".into() },
                SessionOwnedTarget::Job(JobId(1)),
            ),
            (
                ResolvedCommand::Kill { id: "C1".into() },
                SessionOwnedTarget::Cron(CronId(1)),
            ),
            (
                ResolvedCommand::KillJob { id: "J1".into() },
                SessionOwnedTarget::Job(JobId(1)),
            ),
            (
                ResolvedCommand::CancelExecution { id: "J1".into() },
                SessionOwnedTarget::Job(JobId(1)),
            ),
            (
                ResolvedCommand::CancelExecution { id: "CH1".into() },
                SessionOwnedTarget::Chain(ChainId(1)),
            ),
            (
                ResolvedCommand::CancelExecution { id: "R1".into() },
                SessionOwnedTarget::Script(ScriptId(1)),
            ),
            (
                ResolvedCommand::RemoveCron { id: "C1".into() },
                SessionOwnedTarget::Cron(CronId(1)),
            ),
            (
                ResolvedCommand::Retry { id: "J1".into() },
                SessionOwnedTarget::Job(JobId(1)),
            ),
            (
                ResolvedCommand::Out {
                    id: "J1".into(),
                    tail_bytes: None,
                },
                SessionOwnedTarget::Job(JobId(1)),
            ),
            (
                ResolvedCommand::Err { id: "J1".into() },
                SessionOwnedTarget::Job(JobId(1)),
            ),
            (
                ResolvedCommand::JobOutput {
                    id: "J1".into(),
                    stdout_bytes: None,
                    stderr_bytes: None,
                },
                SessionOwnedTarget::Job(JobId(1)),
            ),
            (
                ResolvedCommand::Fg {
                    id: "J1".into(),
                    role: ForegroundRole::Controller,
                },
                SessionOwnedTarget::Job(JobId(1)),
            ),
            (
                ResolvedCommand::Fg {
                    id: "J1".into(),
                    role: ForegroundRole::Observer,
                },
                SessionOwnedTarget::Job(JobId(1)),
            ),
            (
                ResolvedCommand::Wait { id: "J1".into() },
                SessionOwnedTarget::Job(JobId(1)),
            ),
            (
                ResolvedCommand::Send {
                    id: "J1".into(),
                    data: "input".into(),
                },
                SessionOwnedTarget::Job(JobId(1)),
            ),
            (
                ResolvedCommand::Cancel { id: "J1".into() },
                SessionOwnedTarget::Job(JobId(1)),
            ),
            (
                ResolvedCommand::Pause { id: "C1".into() },
                SessionOwnedTarget::Cron(CronId(1)),
            ),
            (
                ResolvedCommand::Resume { id: "C1".into() },
                SessionOwnedTarget::Cron(CronId(1)),
            ),
            (
                ResolvedCommand::Log {
                    id: Some("J1".into()),
                },
                SessionOwnedTarget::Job(JobId(1)),
            ),
            (
                ResolvedCommand::ShowLog {
                    id: Some("C1".into()),
                    limit: None,
                    tail_bytes: None,
                },
                SessionOwnedTarget::Cron(CronId(1)),
            ),
        ];

        for (command, expected) in commands {
            assert_eq!(session_owned_target_for_command(&command), Some(expected));
        }
    }

    #[test]
    fn named_session_target_access_is_owner_scoped_but_anonymous_stays_compatible() {
        let scope = ScopeHash([77; 32]);
        let owner = "SS-owner";
        let other = "SS-other";
        let mut state = SchedulerState::new();
        state.jobs.insert(
            JobId(1),
            JobEntry {
                job_id: JobId(1),
                session_id: Some(owner.into()),
                pipeline_text: "echo owned".into(),
                status: JobStatus::Done,
                exit_code: Some(0),
                start_scope: Some(scope),
                end_scope: Some(scope),
                open_hint: JobOpenHint::Stream,
                chain_id: None,
                chain_index: None,
                chain_total: None,
                pending_reason: None,
            },
        );
        state.chains.insert(
            ChainId(1),
            ChainState {
                node: leaf("echo owned"),
                leaf_jobs: HashMap::new(),
                leaf_status: HashMap::new(),
                scope_hash: scope,
                pipeline_text: "echo owned".into(),
                process: ProcessJobContext {
                    cwd_override: None,
                    launch: LaunchOptions::default(),
                    wrapper_enabled: false,
                    pty_default: true,
                    direct_output_client: None,
                },
                scope_enabled: false,
                session_id: Some(owner.into()),
            },
        );
        state.pending_scripts.insert(
            ScriptId(1),
            PendingScriptRun {
                client_id: 1,
                script_id: ScriptId(1),
                mode: Mode::Job,
                source: ScriptSource::Inline,
                items: VecDeque::new(),
                next_index: 0,
                item_scope: scope,
                created_items: Vec::new(),
                last_exit_code: 0,
                waiting_index: None,
                session_id: Some(owner.into()),
            },
        );
        state.completed_script_snapshots.insert(
            ScriptId(2),
            CompletedScriptSnapshot {
                info: None,
                session_id: Some(owner.into()),
                completed_at: Instant::now(),
                response_bytes: 0,
            },
        );
        state.crons.insert(
            CronId(1),
            CronEntry {
                cron_id: CronId(1),
                schedule: parse_schedule_text("every 1m").expect("valid schedule"),
                chain: leaf("echo owned"),
                scope_hash: scope,
                status: CronStatus::Paused,
                next_trigger: Instant::now(),
                cwd_override: None,
                scope_enabled: false,
                wrapper_enabled: false,
                session_id: Some(owner.into()),
            },
        );

        let targets = [
            SessionOwnedTarget::Job(JobId(1)),
            SessionOwnedTarget::Chain(ChainId(1)),
            SessionOwnedTarget::Script(ScriptId(1)),
            SessionOwnedTarget::Script(ScriptId(2)),
            SessionOwnedTarget::Cron(CronId(1)),
        ];
        for target in targets {
            assert!(authorize_session_owned_target(&state, Some(owner), target).is_ok());
            let denied = authorize_session_owned_target(&state, Some(other), target)
                .expect_err("foreign named session must be denied");
            assert!(matches!(
                denied.into_response(),
                ResponsePayload::Err { ref code, .. } if code == error_code::NOT_FOUND
            ));
            assert!(authorize_session_owned_target(&state, None, target).is_ok());
        }

        // Anonymous clients retain the legacy idempotent/missing-ID behavior;
        // named sessions fail closed when an owner can no longer be proven.
        let missing = SessionOwnedTarget::Job(JobId(999));
        assert!(authorize_session_owned_target(&state, None, missing).is_ok());
        let denied = authorize_session_owned_target(&state, Some(owner), missing)
            .expect_err("missing owned target must fail closed");
        assert!(matches!(
            denied.into_response(),
            ResponsePayload::Err { ref code, .. } if code == error_code::NOT_FOUND
        ));
    }

    #[tokio::test]
    async fn cross_named_session_commands_and_recovery_reads_fail_closed() {
        let (sys, _gw_rx, _sched_rx, _pm_rx, _ss_rx, _eb_rx) = test_actor_system();
        let conn = test_db();
        let config = Config::default();
        let scope = ScopeHash([78; 32]);
        let owner = "SS-owner";
        let other = "SS-other";
        let other_key = named_session_key(other);
        let mut state = SchedulerState::new();
        state.client_sessions.insert(2, other_key.clone());
        state.sessions.insert(
            other_key,
            SessionState {
                scope,
                incarnation: 2,
                defaults: LaunchDefaults::default(),
                connected_clients: 1,
                disconnected_at: None,
                named: Some(NamedSessionMeta {
                    id: other.into(),
                    name: "other".into(),
                    scope_durable: true,
                    created_at_ms: 1,
                    updated_at_ms: 1,
                    archived_at_ms: None,
                }),
            },
        );
        state.jobs.insert(
            JobId(1),
            JobEntry {
                job_id: JobId(1),
                session_id: Some(owner.into()),
                pipeline_text: "echo secret".into(),
                status: JobStatus::Done,
                exit_code: Some(0),
                start_scope: Some(scope),
                end_scope: Some(scope),
                open_hint: JobOpenHint::Stream,
                chain_id: None,
                chain_index: None,
                chain_total: None,
                pending_reason: None,
            },
        );
        state.crons.insert(
            CronId(1),
            CronEntry {
                cron_id: CronId(1),
                schedule: parse_schedule_text("every 1m").expect("valid schedule"),
                chain: leaf("echo secret"),
                scope_hash: scope,
                status: CronStatus::Scheduled,
                next_trigger: Instant::now(),
                cwd_override: None,
                scope_enabled: false,
                wrapper_enabled: false,
                session_id: Some(owner.into()),
            },
        );
        state.pending_scripts.insert(
            ScriptId(1),
            PendingScriptRun {
                client_id: 1,
                script_id: ScriptId(1),
                mode: Mode::Job,
                source: ScriptSource::Inline,
                items: VecDeque::new(),
                next_index: 0,
                item_scope: scope,
                created_items: Vec::new(),
                last_exit_code: 0,
                waiting_index: None,
                session_id: Some(owner.into()),
            },
        );

        for command in [
            ResolvedCommand::Log {
                id: Some("J1".into()),
            },
            ResolvedCommand::Pause { id: "C1".into() },
            ResolvedCommand::CancelExecution { id: "R1".into() },
        ] {
            let response = handle_command(command, 2, &mut state, &conn, &config, &sys).await;
            assert!(matches!(
                response,
                ResponsePayload::Err { ref code, .. } if code == error_code::NOT_FOUND
            ));
        }
        assert!(matches!(
            handle_wait_command("J1".into(), 2, 9, &mut state).await,
            Some(ResponsePayload::Err { ref code, .. }) if code == error_code::NOT_FOUND
        ));
        assert!(matches!(
            script_info_response("R1", 2, &mut state),
            ResponsePayload::Err { ref code, .. } if code == error_code::NOT_FOUND
        ));

        let global_log = handle_command(
            ResolvedCommand::Log { id: None },
            2,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        assert!(matches!(
            global_log,
            ResponsePayload::Ok(OkPayload::EvalText { ref text })
                if text.contains("jobs: none") && text.contains("crons: none")
        ));
    }

    #[tokio::test]
    async fn retry_by_anonymous_client_preserves_original_named_session_owner() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);
        let conn = test_db();
        let config = Config::default();
        let scope = insert_test_scope(&conn, "retry-owner");
        let owner = "SS-retry-owner".to_string();
        storage::upsert_session(
            &conn.lock().unwrap(),
            &storage::StoredSession {
                id: owner.clone(),
                name: "retry-owner".into(),
                scope_hash: Some(scope),
                pty_default: None,
                wrapper_enabled: None,
                created_at_ms: 1,
                updated_at_ms: 1,
                archived_at_ms: None,
            },
        )
        .expect("persist owner session");

        let mut initial = test_chain_spawn(leaf("echo retry-owner"), scope);
        initial.session_id = Some(owner.clone());
        let mut state = SchedulerState::new();
        let response = spawn_chain(initial, &mut state, SchedulerIo::new(&conn, &sys)).await;
        let job_id = match response {
            ResponsePayload::Ok(OkPayload::JobCreated { job_id, .. }) => job_id,
            other => panic!("expected JobCreated, got {other:?}"),
        };
        let original_job = match pm_rx.recv().await.expect("original spawn") {
            ProcessMgrMsg::SpawnJob {
                job_id, options, ..
            } => {
                assert_eq!(options.session_id.as_deref(), Some(owner.as_str()));
                job_id
            }
            _ => panic!("expected original SpawnJob"),
        };
        handle_job_finished(original_job, 1, &mut state, &conn, &sys).await;

        let retry = handle_command(
            ResolvedCommand::Retry { id: job_id },
            99,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        assert!(matches!(
            retry,
            ResponsePayload::Ok(OkPayload::JobCreated { .. })
        ));
        let retried_job = match pm_rx.recv().await.expect("retry spawn") {
            ProcessMgrMsg::SpawnJob {
                job_id, options, ..
            } => {
                assert_eq!(options.session_id.as_deref(), Some(owner.as_str()));
                job_id
            }
            _ => panic!("expected retry SpawnJob"),
        };
        assert_eq!(
            state.jobs[&retried_job].session_id.as_deref(),
            Some(owner.as_str())
        );
    }
}
