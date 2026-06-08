//! Scheduler actor — command routing, ID assignment, chain execution, cron timer heap.

use std::collections::{HashMap, VecDeque};
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rusqlite::Connection;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use cue_core::command_spec::{COMMAND_SPECS, CommandCategory, CommandSpec, command_spec};
use cue_core::cron::{CronSchedule, CronStatus, parse_schedule_text};
use cue_core::ipc::{
    ChainInfo, ChainJobInfo, CronInfo, EventPayload, JobInfo, JobOpenHint, OkPayload, PageInfo,
    ResponsePayload, ScriptItemInfo, ScriptItemResult, ScriptRunStatus, ScriptSource,
    ScriptSubmitError, StreamText, error_code,
};
use cue_core::job::{CancelReason, EXIT_CODE_UNAVAILABLE, JobStatus};
use cue_core::mode::Mode;
use cue_core::pipeline::{ChainNode, ParallelOp, SerialOp, command_prefers_foreground};
use cue_core::scope::EnvSnapshot;
use cue_core::{ChainId, CronId, EventChannel, JobId, ScopeHash, ScriptId};

use crate::config::{BlockDecision, Config};
use crate::parser::{ResolvedCommand, ResolvedScriptItem, Token, Tokenizer, parse_command};
use crate::storage;
use crate::word_expansion::expand_command_line;

use super::cron_schedule::next_trigger_instant;
use super::script_record::{
    ScriptFinish, persist_finished as persist_script_finished,
    persist_submission as persist_script_submission,
};
use super::{
    ActorSystem, GatewayMsg, ProcessJobOptions, ProcessMgrMsg, SchedulerMsg, ScopeStoreMsg,
    StderrSnapshot, publish_event as publish_actor_event,
    publish_event_except as publish_actor_event_except,
    send_gateway_event as send_actor_gateway_event,
};

const MAX_OUTPUT_TAIL_BYTES: usize = cue_core::ipc::MAX_MESSAGE_SIZE / 4;

// ── Leaf status within a chain ──────────────────────────────────────────────

/// Status of a single leaf (pipeline) within a chain.
#[derive(Debug, Clone, PartialEq, Eq)]
enum LeafStatus {
    Pending,
    Running,
    Done(i32),
    Failed(i32),
    Cancelled,
}

impl LeafStatus {
    /// Returns `true` if the leaf has reached a final state.
    fn is_terminal(&self) -> bool {
        matches!(
            self,
            LeafStatus::Done(_) | LeafStatus::Failed(_) | LeafStatus::Cancelled
        )
    }
}

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
    /// Explicit working directory override for all jobs in this chain.
    cwd_override: Option<std::path::PathBuf>,
    /// Whether scope-transform leaves may derive a new scope for later leaves.
    scope_enabled: bool,
    /// Whether the wrapper binary is enabled for this chain's jobs.
    wrapper_enabled: bool,
    /// Whether to allocate a PTY for spawned leaf commands.
    pty_enabled: bool,
    /// Client that should receive output for spawned leaves directly.
    direct_output_client: Option<u64>,
}

/// Flattened representation of a chain leaf for easy lookup.
struct FlatLeaf {
    /// Index in the DFS-order leaf list.
    index: usize,
    /// Full job plan.
    plan: cue_core::pipeline::JobPlan,
    /// Human-readable pipeline text.
    pipeline_text: String,
}

impl FlatLeaf {
    /// First segment's command words (for scope-transform detection and open hint).
    fn command(&self) -> &[String] {
        self.plan.first_command()
    }
}

// ── Job tracking ────────────────────────────────────────────────────────────

/// Scheduler-side view of every spawned job.
struct JobEntry {
    job_id: JobId,
    pipeline_text: String,
    status: JobStatus,
    exit_code: Option<i32>,
    start_scope: Option<ScopeHash>,
    end_scope: Option<ScopeHash>,
    open_hint: JobOpenHint,
    chain_id: Option<ChainId>,
    chain_index: Option<usize>,
    chain_total: Option<usize>,
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
    /// Explicit working directory override for jobs spawned by this cron.
    cwd_override: Option<std::path::PathBuf>,
    /// Whether scope-transform leaves may derive a new scope for later leaves.
    scope_enabled: bool,
    /// Whether the wrapper binary is enabled for jobs spawned by this cron.
    wrapper_enabled: bool,
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
}

#[derive(Clone, Copy, Default)]
struct CommandExecutionContext {
    scope_override: Option<ScopeHash>,
    direct_output_client: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct ChainCompletion {
    exit_code: i32,
    end_scope: Option<ScopeHash>,
}

// ── Scheduler state (all mutable state lives here) ──────────────────────────

struct SchedulerState {
    next_job: u32,
    next_cron: u32,
    next_chain: u32,
    next_script: u32,

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
    /// Runtime wrapper toggle set by `:wrap on` / `:wrap off`.
    wrapper_enabled: Option<bool>,
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
            chains: HashMap::new(),
            job_to_chain: HashMap::new(),
            jobs: HashMap::new(),
            crons: HashMap::new(),
            job_waiters: HashMap::new(),
            pending_scripts: HashMap::new(),
            pending_script_jobs: HashMap::new(),
            pending_script_chains: HashMap::new(),
            completed_chains: HashMap::new(),
            wrapper_enabled: None,
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

    /// Resolve the effective wrapper_enabled from session override or config.
    fn wrapper_enabled(&self, config: &Config) -> bool {
        self.wrapper_enabled.unwrap_or(config.wrapper.enabled)
    }

    fn alloc_script(&mut self) -> ScriptId {
        let id = ScriptId(self.next_script);
        self.next_script += 1;
        id
    }
}

// ── Spawn the actor ─────────────────────────────────────────────────────────

/// Restore durable Scheduler state and spawn the actor task.
pub(super) async fn spawn(
    mut rx: mpsc::Receiver<SchedulerMsg>,
    conn: Connection,
    sys: ActorSystem,
) -> anyhow::Result<()> {
    let db = storage::shared_connection(conn);
    let config = sys.config.clone();
    let mut state = SchedulerState::new();
    restore_jobs(&db, &mut state).await?;
    restore_crons(&db, &mut state).await?;
    restore_script_counter(&db, &mut state).await?;

    tokio::spawn(async move {
        prune_retained_job_history(&mut state, &db, &config, &sys).await;
        debug!("scheduler: started");

        loop {
            // Compute the sleep deadline from the nearest enabled cron trigger.
            let next_cron_deadline = state
                .crons
                .values()
                .filter(|c| c.status.is_runnable())
                .map(|c| c.next_trigger)
                .min();

            let sleep = match next_cron_deadline {
                Some(deadline) => tokio::time::sleep_until(deadline),
                // No crons → sleep "forever" (will be cancelled by select).
                None => tokio::time::sleep(std::time::Duration::from_secs(86400 * 365)),
            };
            tokio::pin!(sleep);

            tokio::select! {
                biased;

                msg = rx.recv() => {
                    let Some(msg) = msg else { break };
                    match msg {
                        SchedulerMsg::Eval { client_id, request_id, command } => {
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
                    prune_retained_job_history(&mut state, &db, &config, &sys).await;
                }

                () = &mut sleep => {
                    // A cron timer has fired.
                    fire_due_crons(&mut state, &db, &config, &sys).await;
                }
            }
        }

        debug!("scheduler: stopped");
    });

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
    for job in restored {
        let Some(job_id) = parse_job_id(&job.id) else {
            return Err(anyhow::anyhow!("invalid persisted job id {}", job.id));
        };
        max_job = max_job.max(job_id.0);
        state.jobs.insert(
            job_id,
            JobEntry {
                job_id,
                pipeline_text: job.pipeline,
                status: job.status,
                exit_code: job.exit_code,
                start_scope: job.start_scope,
                end_scope: job.end_scope,
                open_hint: JobOpenHint::Stream,
                chain_id: None,
                chain_index: None,
                chain_total: None,
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
        let Some(cron_id) = parse_cron_id(&cron.id) else {
            return Err(anyhow::anyhow!("invalid persisted cron id {}", cron.id));
        };
        max_cron = max_cron.max(cron_id.0);

        let Some(scope_hash) = cron.scope_hash else {
            return Err(anyhow::anyhow!(
                "persisted cron {} has no scope hash",
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
) {
    let keep = config.retention.max_job_history;
    let removed = match storage::with_connection(db, move |conn| {
        storage::prune_job_history(conn, keep)
    })
    .await
    {
        Ok(removed) => removed,
        Err(error) => {
            warn!("scheduler: failed to prune job history: {error}");
            return;
        }
    };

    for id in removed {
        if let Some(job_id) = parse_job_id(&id) {
            state.jobs.remove(&job_id);
            publish_event(
                sys,
                EventChannel::Jobs,
                EventPayload::JobRemoved {
                    job_id: job_id.to_string(),
                },
            )
            .await;
            remove_job_logs(job_id).await;
        }
    }
}

async fn publish_event(sys: &ActorSystem, channel: EventChannel, payload: EventPayload) {
    publish_actor_event("scheduler", &sys.event_bus, channel, payload).await;
}

async fn publish_event_except(
    sys: &ActorSystem,
    channel: EventChannel,
    payload: EventPayload,
    excluded_client_id: u64,
) {
    publish_actor_event_except(
        "scheduler",
        &sys.event_bus,
        channel,
        payload,
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
    if state.crons.remove(&cid).is_some() {
        info!(%cid, "scheduler: cron removed");
        publish_event(
            sys,
            EventChannel::Crons,
            EventPayload::CronRemoved {
                cron_id: cid.to_string(),
            },
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

async fn get_head_snapshot(
    sys: &ActorSystem,
) -> Result<cue_core::scope::EnvSnapshot, ResponsePayload> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    if sys
        .scope_store
        .send(ScopeStoreMsg::GetHeadSnapshot { reply: tx })
        .await
        .is_err()
    {
        return Err(ResponsePayload::err(
            error_code::INTERNAL,
            "scope_store unreachable",
        ));
    }
    match rx.await {
        Ok(Ok(snapshot)) => Ok(snapshot),
        Ok(Err(error)) => Err(ResponsePayload::err(
            error_code::INTERNAL,
            error.to_string(),
        )),
        Err(_) => Err(ResponsePayload::err(
            error_code::INTERNAL,
            "scope_store reply dropped",
        )),
    }
}

// ── Chain helpers ────────────────────────────────────────────────────────────

/// Count the number of leaf nodes (Pipelines) in a `ChainNode`.
fn leaf_count(node: &ChainNode) -> usize {
    match node {
        ChainNode::Leaf(_) => 1,
        ChainNode::Serial { left, right, .. } | ChainNode::Parallel { left, right, .. } => {
            leaf_count(left) + leaf_count(right)
        }
    }
}

/// Flatten a `ChainNode` into a list of `FlatLeaf` entries (DFS, left-to-right).
fn flatten_leaves(node: &ChainNode) -> Vec<FlatLeaf> {
    let mut out = Vec::new();
    flatten_leaves_inner(node, &mut out);
    out
}

fn flatten_leaves_inner(node: &ChainNode, out: &mut Vec<FlatLeaf>) {
    match node {
        ChainNode::Leaf(plan) => {
            let idx = out.len();
            let pipeline_text = plan.to_string();
            out.push(FlatLeaf {
                index: idx,
                plan: plan.clone(),
                pipeline_text,
            });
        }
        ChainNode::Serial { left, right, .. } | ChainNode::Parallel { left, right, .. } => {
            flatten_leaves_inner(left, out);
            flatten_leaves_inner(right, out);
        }
    }
}

fn parse_chain_text(text: &str) -> Result<ChainNode, String> {
    match parse_command(&format!(":run {text}"), cue_core::Mode::Job).map_err(|err| err.message)? {
        ResolvedCommand::Run { chain, .. } => Ok(chain),
        other => Err(format!("unexpected restore command: {other:?}")),
    }
}

/// Determine which leaf indices are *initially ready* given the chain structure.
///
/// Returns a `Vec<usize>` of leaf indices that should be spawned immediately.
fn initially_ready(node: &ChainNode) -> Vec<usize> {
    let mut ready = Vec::new();
    initially_ready_inner(node, 0, &mut ready);
    ready
}

fn initially_ready_inner(node: &ChainNode, offset: usize, ready: &mut Vec<usize>) {
    match node {
        ChainNode::Leaf(_) => {
            ready.push(offset);
        }
        ChainNode::Serial { left, .. } => {
            // Only the left subtree is ready initially.
            initially_ready_inner(left, offset, ready);
        }
        ChainNode::Parallel { left, right, .. } => {
            // Both subtrees are ready.
            let left_count = leaf_count(left);
            initially_ready_inner(left, offset, ready);
            initially_ready_inner(right, offset + left_count, ready);
        }
    }
}

/// After a leaf finishes, determine which new leaves become ready
/// and whether any should be cancelled.
///
/// Returns `(newly_ready, to_cancel)` leaf indices.
fn advance_chain(
    node: &ChainNode,
    finished_idx: usize,
    statuses: &HashMap<usize, LeafStatus>,
) -> (Vec<usize>, Vec<usize>) {
    let mut ready = Vec::new();
    let mut cancel = Vec::new();
    advance_inner(node, 0, finished_idx, statuses, &mut ready, &mut cancel);
    (ready, cancel)
}

fn advance_inner(
    node: &ChainNode,
    offset: usize,
    finished_idx: usize,
    statuses: &HashMap<usize, LeafStatus>,
    ready: &mut Vec<usize>,
    cancel: &mut Vec<usize>,
) {
    match node {
        ChainNode::Leaf(_) => {
            // Nothing to advance for a bare leaf.
        }
        ChainNode::Serial { left, op, right } => {
            let left_count = leaf_count(left);
            let left_range = offset..offset + left_count;
            let right_offset = offset + left_count;

            if left_range.contains(&finished_idx) {
                // Finished leaf is in the left subtree. Recurse into left.
                advance_inner(left, offset, finished_idx, statuses, ready, cancel);

                // Check if the entire left subtree is complete.
                if all_leaves_terminal(left, offset, statuses) {
                    match op {
                        SerialOp::Then => {
                            // Right runs only if all left leaves succeeded (exit 0).
                            if all_leaves_succeeded(left, offset, statuses) {
                                mark_ready(right, right_offset, statuses, ready);
                            } else {
                                mark_cancelled(right, right_offset, statuses, cancel);
                            }
                        }
                        SerialOp::Always => {
                            // Right always runs after left completes.
                            mark_ready(right, right_offset, statuses, ready);
                        }
                    }
                }
            } else {
                // Finished leaf is in the right subtree. Recurse into right.
                advance_inner(right, right_offset, finished_idx, statuses, ready, cancel);
            }
        }
        ChainNode::Parallel { left, right, op } => {
            let left_count = leaf_count(left);
            let right_offset = offset + left_count;

            // Recurse into the subtree that owns the finished leaf.
            if finished_idx < right_offset {
                advance_inner(left, offset, finished_idx, statuses, ready, cancel);
            } else {
                advance_inner(right, right_offset, finished_idx, statuses, ready, cancel);
            }

            // For Race, check entire branch success (subtree terminal + all ok),
            // not individual leaf success.
            if *op == ParallelOp::Race {
                let right_count = leaf_count(right);
                let left_terminal = (offset..offset + left_count)
                    .all(|i| statuses.get(&i).is_some_and(|s| s.is_terminal()));
                let left_ok = left_terminal
                    && (offset..offset + left_count)
                        .all(|i| matches!(statuses.get(&i), Some(LeafStatus::Done(0))));

                let right_terminal = (right_offset..right_offset + right_count)
                    .all(|i| statuses.get(&i).is_some_and(|s| s.is_terminal()));
                let right_ok = right_terminal
                    && (right_offset..right_offset + right_count)
                        .all(|i| matches!(statuses.get(&i), Some(LeafStatus::Done(0))));

                if left_ok || right_ok {
                    // Cancel the OTHER branch's pending/running leaves.
                    let cancel_range = if left_ok {
                        right_offset..right_offset + right_count
                    } else {
                        offset..offset + left_count
                    };
                    for i in cancel_range {
                        if !statuses.get(&i).is_none_or(|s| s.is_terminal()) {
                            cancel.push(i);
                        }
                    }
                }
            }
        }
    }
}

/// Check whether every leaf in the subtree has reached a terminal state.
fn all_leaves_terminal(
    node: &ChainNode,
    offset: usize,
    statuses: &HashMap<usize, LeafStatus>,
) -> bool {
    match node {
        ChainNode::Leaf(_) => matches!(
            statuses.get(&offset),
            Some(LeafStatus::Done(_) | LeafStatus::Failed(_) | LeafStatus::Cancelled)
        ),
        ChainNode::Serial { left, right, .. } | ChainNode::Parallel { left, right, .. } => {
            let left_count = leaf_count(left);
            all_leaves_terminal(left, offset, statuses)
                && all_leaves_terminal(right, offset + left_count, statuses)
        }
    }
}

fn aggregate_chain_exit_code(chain: &ChainState) -> i32 {
    let mut last = 0;
    for idx in 0..leaf_count(&chain.node) {
        match chain.leaf_status.get(&idx) {
            Some(LeafStatus::Done(code)) => last = *code,
            Some(LeafStatus::Failed(code)) => return *code,
            Some(LeafStatus::Cancelled) => return EXIT_CODE_UNAVAILABLE,
            Some(LeafStatus::Pending | LeafStatus::Running) | None => return EXIT_CODE_UNAVAILABLE,
        }
    }
    last
}

fn chain_final_scope(chain: &ChainState, state: &SchedulerState) -> Option<ScopeHash> {
    (0..leaf_count(&chain.node)).rev().find_map(|idx| {
        chain
            .leaf_jobs
            .get(&idx)
            .and_then(|job_id| state.jobs.get(job_id))
            .and_then(|entry| entry.end_scope.or(entry.start_scope))
    })
}

/// Check whether every leaf in the subtree succeeded (exit code 0).
fn all_leaves_succeeded(
    node: &ChainNode,
    offset: usize,
    statuses: &HashMap<usize, LeafStatus>,
) -> bool {
    match node {
        ChainNode::Leaf(_) => matches!(statuses.get(&offset), Some(LeafStatus::Done(0))),
        ChainNode::Serial { left, right, .. } | ChainNode::Parallel { left, right, .. } => {
            let left_count = leaf_count(left);
            all_leaves_succeeded(left, offset, statuses)
                && all_leaves_succeeded(right, offset + left_count, statuses)
        }
    }
}

/// Mark all pending leaves in the subtree as ready.
fn mark_ready(
    node: &ChainNode,
    offset: usize,
    statuses: &HashMap<usize, LeafStatus>,
    ready: &mut Vec<usize>,
) {
    match node {
        ChainNode::Leaf(_) => {
            if matches!(statuses.get(&offset), Some(LeafStatus::Pending) | None) {
                ready.push(offset);
            }
        }
        ChainNode::Serial { left, .. } => {
            // Only the left side is initially ready.
            mark_ready(left, offset, statuses, ready);
        }
        ChainNode::Parallel { left, right, .. } => {
            let left_count = leaf_count(left);
            mark_ready(left, offset, statuses, ready);
            mark_ready(right, offset + left_count, statuses, ready);
        }
    }
}

/// Mark all pending leaves in the subtree as cancelled.
fn mark_cancelled(
    node: &ChainNode,
    offset: usize,
    statuses: &HashMap<usize, LeafStatus>,
    cancel: &mut Vec<usize>,
) {
    match node {
        ChainNode::Leaf(_) => {
            if matches!(statuses.get(&offset), Some(LeafStatus::Pending) | None) {
                cancel.push(offset);
            }
        }
        ChainNode::Serial { left, right, .. } | ChainNode::Parallel { left, right, .. } => {
            let left_count = leaf_count(left);
            mark_cancelled(left, offset, statuses, cancel);
            mark_cancelled(right, offset + left_count, statuses, cancel);
        }
    }
}

async fn publish_job_created(
    sys: &ActorSystem,
    state: &SchedulerState,
    job_id: JobId,
    pipeline_text: &str,
    start_scope: ScopeHash,
    open_hint: JobOpenHint,
) {
    let (chain_id, chain_index, chain_total) = state
        .jobs
        .get(&job_id)
        .map(|entry| {
            (
                entry.chain_id.map(|id| id.to_string()),
                entry.chain_index,
                entry.chain_total,
            )
        })
        .unwrap_or((None, None, None));
    publish_event(
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
    let (chain_id, chain_index) = state
        .jobs
        .get(&job_id)
        .map(|entry| (entry.chain_id.map(|id| id.to_string()), entry.chain_index))
        .unwrap_or((None, None));
    publish_event(
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

async fn publish_chain_progress(sys: &ActorSystem, state: &SchedulerState, chain_id: ChainId) {
    let Some(chain) = build_chain_info(state, chain_id) else {
        return;
    };
    publish_event(
        sys,
        EventChannel::Jobs,
        EventPayload::ChainProgress { chain },
    )
    .await;
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

async fn fork_scope(
    sys: &ActorSystem,
    delta: cue_core::scope::EnvDelta,
) -> Result<ScopeHash, ResponsePayload> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    if sys
        .scope_store
        .send(ScopeStoreMsg::Fork { delta, reply: tx })
        .await
        .is_err()
    {
        return Err(ResponsePayload::err(
            error_code::INTERNAL,
            "scope_store unreachable",
        ));
    }
    match rx.await {
        Ok(Ok(hash)) => Ok(hash),
        Ok(Err(error)) => Err(ResponsePayload::err(
            error_code::INTERNAL,
            error.to_string(),
        )),
        Err(_) => Err(ResponsePayload::err(
            error_code::INTERNAL,
            "scope_store reply dropped",
        )),
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

fn process_job_options(
    cwd_override: Option<std::path::PathBuf>,
    wrapper_enabled: bool,
    pty_enabled: bool,
    direct_output_client: Option<u64>,
) -> ProcessJobOptions {
    ProcessJobOptions {
        cwd_override,
        wrapper_enabled,
        pty_enabled,
        direct_output_client,
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

struct TerminalStateUpdate {
    status: JobStatus,
    exit_code: i32,
    end_scope: Option<ScopeHash>,
    advance_chain: bool,
}

struct ChainSpawnOptions {
    cwd_override: Option<std::path::PathBuf>,
    scope_enabled: bool,
    wrapper_enabled: bool,
    pty_enabled: bool,
    direct_output_client: Option<u64>,
}

impl ChainSpawnOptions {
    fn process_job_options(&self) -> ProcessJobOptions {
        ProcessJobOptions {
            cwd_override: self.cwd_override.clone(),
            wrapper_enabled: self.wrapper_enabled,
            pty_enabled: self.pty_enabled,
            direct_output_client: self.direct_output_client,
        }
    }
}

struct SpawnChainRequest {
    chain: ChainNode,
    scope_hash: ScopeHash,
    options: ChainSpawnOptions,
    warnings: Vec<String>,
    retain_completed_chain: bool,
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
    cwd_override: Option<std::path::PathBuf>,
    retain_completed_chain: bool,
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
    let (newly_ready, to_cancel) = advance_chain(&chain.node, leaf_idx, &chain.leaf_status);
    TerminalStateOutcome {
        chain_advance: Some(ChainAdvance {
            chain_id,
            newly_ready: newly_ready
                .into_iter()
                .map(|idx| (idx, next_scope))
                .collect(),
            to_cancel,
        }),
        persist_error,
    }
}

fn job_info_from_entry(entry: &JobEntry) -> JobInfo {
    JobInfo {
        id: entry.job_id.to_string(),
        status: entry.status.clone(),
        pipeline: entry.pipeline_text.clone(),
        exit_code: entry.exit_code,
        start_scope: entry.start_scope.map(|hash| hash.to_string()),
        end_scope: entry.end_scope.map(|hash| hash.to_string()),
        open_hint: entry.open_hint,
        chain_id: entry.chain_id.map(|id| id.to_string()),
        chain_index: entry.chain_index,
        chain_total: entry.chain_total,
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
            if is_running && let Err(error) = kill_process_job(sys, jid).await {
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
        let cwd_override = entry.cwd_override.clone();
        let scope_enabled = entry.scope_enabled;
        let wrapper_enabled = entry.wrapper_enabled;

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
                options: ChainSpawnOptions {
                    cwd_override,
                    scope_enabled,
                    wrapper_enabled,
                    pty_enabled: true,
                    direct_output_client: None,
                },
                warnings,
                retain_completed_chain: false,
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
            publish_event(
                sys,
                EventChannel::Crons,
                EventPayload::CronTriggered {
                    cron_id: cron_id.to_string(),
                    job_id,
                },
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

        state.jobs.insert(
            jid,
            JobEntry {
                job_id: jid,
                pipeline_text: leaf.pipeline_text.clone(),
                status: JobStatus::Running,
                exit_code: None,
                start_scope: Some(scope_hash),
                end_scope: None,
                open_hint,
                chain_id: None,
                chain_index: None,
                chain_total: None,
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

        match options
            .scope_enabled
            .then(|| scope_transform_from_command(leaf.command()))
            .transpose()
        {
            Ok(Some(_)) => {
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
            }
            Ok(None) => {
                info!(%jid, pipeline = %leaf.pipeline_text, "scheduler: spawning single job");
                if let Err(message) = spawn_process_job(
                    io.sys,
                    jid,
                    leaf.plan.clone(),
                    scope_hash,
                    options.process_job_options(),
                )
                .await
                {
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
            Err(message) => {
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
        cwd_override: options.cwd_override.clone(),
        scope_enabled: options.scope_enabled,
        wrapper_enabled: options.wrapper_enabled,
        pty_enabled: options.pty_enabled,
        direct_output_client: options.direct_output_client,
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
            cwd_override: options.cwd_override,
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
        let cwd_override = state
            .chains
            .get(&chain_id)
            .and_then(|c| c.cwd_override.clone());
        let advance = process_chain_advance(
            ChainAdvanceRequest {
                chain_id,
                newly_ready: chain_advance.newly_ready,
                to_cancel: chain_advance.to_cancel,
                capture_first: 0,
                cwd_override,
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
        let cwd_override = state
            .chains
            .get(&chain_id)
            .and_then(|c| c.cwd_override.clone());
        let advance = process_chain_advance(
            ChainAdvanceRequest {
                chain_id,
                newly_ready: chain_advance.newly_ready,
                to_cancel: chain_advance.to_cancel,
                capture_first: 0,
                cwd_override,
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
    advance_pending_scripts_after_terminal_job(job_id, reported_exit_code, state, runtime).await;
    persist_error
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
        cwd_override,
        retain_completed_chain,
    } = request;
    let mut outcome = ChainAdvanceOutcome::default();
    if let Some(error) = cancel_chain_leaves(chain_id, &to_cancel, state, io.db, io.sys).await {
        outcome.record_persist_error(error);
    }

    let (leaves, wrapper_enabled, scope_enabled, pty_enabled, direct_output_client) = {
        let Some(chain) = state.chains.get(&chain_id) else {
            return outcome;
        };
        (
            flatten_leaves(&chain.node),
            chain.wrapper_enabled,
            chain.scope_enabled,
            chain.pty_enabled,
            chain.direct_output_client,
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
                pipeline_text: leaves[idx].pipeline_text.clone(),
                status: JobStatus::Running,
                exit_code: None,
                start_scope: Some(start_scope),
                end_scope: None,
                open_hint,
                chain_id: Some(chain_id),
                chain_index: Some(idx),
                chain_total: Some(leaves.len()),
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

        match scope_enabled
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
                if let Err(error) = spawn_process_job(
                    io.sys,
                    jid,
                    leaves[idx].plan.clone(),
                    start_scope,
                    process_job_options(
                        cwd_override.clone(),
                        wrapper_enabled,
                        pty_enabled,
                        direct_output_client,
                    ),
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
        && all_leaves_terminal(&chain.node, 0, &chain.leaf_status)
    {
        outcome.completed_chain = build_chain_info(state, chain_id);
        let completion = ChainCompletion {
            exit_code: aggregate_chain_exit_code(chain),
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
    let script_id = state.alloc_script();
    let item_scope = match create_isolated_script_scope(runtime.io.sys).await {
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
                    runtime.io.sys,
                    pending.client_id,
                    pending.script_id,
                    ScriptRunStatus::Failed,
                    EXIT_CODE_UNAVAILABLE,
                    Some(index),
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
                pending.created_items.push(ScriptItemInfo {
                    index,
                    source: source_text,
                    result,
                });

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
                            runtime.io.sys,
                            pending.client_id,
                            pending.script_id,
                            ScriptRunStatus::Failed,
                            exit_code,
                            Some(index),
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
                                runtime.io.sys,
                                pending.client_id,
                                pending.script_id,
                                ScriptRunStatus::Failed,
                                EXIT_CODE_UNAVAILABLE,
                                Some(index),
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
                                runtime.io.sys,
                                pending.client_id,
                                pending.script_id,
                                ScriptRunStatus::Failed,
                                EXIT_CODE_UNAVAILABLE,
                                Some(index),
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
        runtime.io.sys,
        pending.client_id,
        pending.script_id,
        ScriptRunStatus::Done,
        pending.last_exit_code,
        None,
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
            finish_pending_script_failed(pending, exit_code, runtime).await;
        } else {
            continue_pending_script(pending, state, runtime).await;
        }
    }

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
            finish_pending_script_failed(pending, completion.exit_code, runtime).await;
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
        runtime.io.sys,
        pending.client_id,
        pending.script_id,
        ScriptRunStatus::Failed,
        exit_code,
        failed_index,
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
        finish_pending_script_failed(pending, EXIT_CODE_UNAVAILABLE, runtime).await;
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

async fn publish_script_finished(
    sys: &ActorSystem,
    client_id: u64,
    script_id: ScriptId,
    status: ScriptRunStatus,
    exit_code: i32,
    failed_item_index: Option<usize>,
) {
    let payload = EventPayload::ScriptFinished {
        script_id: script_id.to_string(),
        status,
        exit_code,
        failed_item_index,
    };
    send_actor_gateway_event("scheduler", sys, client_id, payload.clone()).await;
    publish_event_except(sys, EventChannel::Jobs, payload, client_id).await;
}

async fn handle_command(
    cmd: ResolvedCommand,
    client_id: u64,
    state: &mut SchedulerState,
    db: &Arc<Mutex<Connection>>,
    config: &Config,
    sys: &ActorSystem,
) -> ResponsePayload {
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
    match cmd {
        ResolvedCommand::Script { .. } => ResponsePayload::err(
            error_code::NOT_SUPPORTED,
            "script commands must enter the scheduler through the file-script runner",
        ),
        ResolvedCommand::Run { chain, params } => {
            let scope_hash = match resolve_command_scope(sys, context.scope_override).await {
                Ok(h) => h,
                Err(resp) => return resp,
            };
            let warnings = match check_chain_guardrails(&chain, config) {
                Ok(warnings) => warnings,
                Err(reason) => return ResponsePayload::err(error_code::BLOCKED, reason),
            };
            let cwd_override = params.cwd();
            let scope_enabled = params.scope().unwrap_or(false);
            let wrapper_enabled = params
                .wrapper_enabled()
                .unwrap_or_else(|| state.wrapper_enabled(config));
            let pty_enabled = params.pty_enabled();
            spawn_chain(
                SpawnChainRequest {
                    chain,
                    scope_hash,
                    options: ChainSpawnOptions {
                        cwd_override,
                        scope_enabled,
                        wrapper_enabled,
                        pty_enabled,
                        direct_output_client: context.direct_output_client,
                    },
                    warnings,
                    retain_completed_chain: context.scope_override.is_some(),
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
            let scope_hash = match resolve_command_scope(sys, context.scope_override).await {
                Ok(h) => h,
                Err(resp) => return resp,
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
            let entry = CronEntry {
                cron_id,
                schedule,
                chain,
                scope_hash,
                status: CronStatus::Scheduled,
                next_trigger,
                cwd_override: params.cwd(),
                scope_enabled: params.scope().unwrap_or(false),
                wrapper_enabled: params
                    .wrapper_enabled()
                    .unwrap_or_else(|| state.wrapper_enabled(config)),
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

        ResolvedCommand::Fg { id } => {
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
                        reply: tx,
                    })
                    .await
                    .is_err()
                {
                    return ResponsePayload::err(error_code::INTERNAL, "process_mgr unreachable");
                }

                match rx.await {
                    Ok(Ok(())) => ResponsePayload::Ok(OkPayload::FgAttached { id }),
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
            let list = sorted_job_list(state);
            ResponsePayload::Ok(OkPayload::JobList(list))
        }

        ResolvedCommand::ListJobs { limit } => {
            let list = sorted_job_list(state);
            let (jobs, page) = page_items(list, limit);
            ResponsePayload::Ok(OkPayload::JobListPage { jobs, page })
        }

        ResolvedCommand::Crons => {
            let list = sorted_cron_list(state);
            ResponsePayload::Ok(OkPayload::CronList(list))
        }

        ResolvedCommand::ListCrons { limit } => {
            let list = sorted_cron_list(state);
            let (crons, page) = page_items(list, limit);
            ResponsePayload::Ok(OkPayload::CronListPage { crons, page })
        }

        ResolvedCommand::Scopes => handle_list_scopes(sys).await,

        ResolvedCommand::ListScopes { limit } => handle_list_scopes_page(sys, limit).await,

        ResolvedCommand::Env { subcommand } => {
            let snapshot = match get_head_snapshot(sys).await {
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
                    match fork_scope(sys, delta).await {
                        Ok(hash) => match get_scope_snapshot_by_hash(sys, hash).await {
                            Ok(updated) => ResponsePayload::Ok(OkPayload::ScopeCreated {
                                hash: hash.to_string(),
                                summary: format_scope_change_summary(hash, &snapshot, &updated),
                            }),
                            Err(message) => ResponsePayload::err(error_code::INTERNAL, message),
                        },
                        Err(response) => response,
                    }
                }
                Err(message) => ResponsePayload::err(error_code::INVALID_SYNTAX, message),
            }
        }

        ResolvedCommand::ShowEnv { tail_bytes } => {
            let snapshot = match get_head_snapshot(sys).await {
                Ok(snapshot) => snapshot,
                Err(response) => return response,
            };
            if let Some(response) = invalid_tail_bytes_response("tail_bytes", tail_bytes) {
                return response;
            }
            let text = format_snapshot_env(&snapshot);
            let (text, truncated) = limit_text(text, None, tail_bytes);
            ResponsePayload::Ok(OkPayload::TextOutput { text, truncated })
        }

        ResolvedCommand::Help { topic } => {
            let text = render_help_text(topic.as_deref());
            ResponsePayload::Ok(OkPayload::EvalText { text })
        }

        ResolvedCommand::Clear => ResponsePayload::ack(),

        ResolvedCommand::Quit => ResponsePayload::ack(),

        ResolvedCommand::Wrap { subcommand } => {
            let sub = subcommand.as_deref().unwrap_or("status");
            match sub {
                "on" => {
                    state.wrapper_enabled = Some(true);
                    let text = if config.wrapper.enabled {
                        "wrapper enabled (already on in config)".into()
                    } else {
                        "wrapper enabled for this session".into()
                    };
                    ResponsePayload::Ok(OkPayload::EvalText { text })
                }
                "off" => {
                    state.wrapper_enabled = Some(false);
                    let text = if config.wrapper.enabled {
                        "wrapper disabled for this session".into()
                    } else {
                        "wrapper disabled (already off in config)".into()
                    };
                    ResponsePayload::Ok(OkPayload::EvalText { text })
                }
                "" | "status" => {
                    let effective = state.wrapper_enabled.unwrap_or(config.wrapper.enabled);
                    let binary = &config.wrapper.binary;
                    let source = if let Some(ov) = state.wrapper_enabled {
                        if ov {
                            "session override: on"
                        } else {
                            "session override: off"
                        }
                    } else {
                        "config"
                    };
                    let lines = [
                        format!(
                            "wrapper status: {}",
                            if effective { "enabled" } else { "disabled" }
                        ),
                        format!(
                            "  binary: {}",
                            if binary.is_empty() { "(none)" } else { binary }
                        ),
                        format!("  source: {source}"),
                        format!(
                            "  allowlist: {}",
                            if config.wrapper.allowlist.commands.is_empty() {
                                "(empty; wraps nothing)".into()
                            } else {
                                config.wrapper.allowlist.commands.join(", ")
                            }
                        ),
                    ];
                    ResponsePayload::Ok(OkPayload::EvalText {
                        text: lines.join("\n"),
                    })
                }
                other => ResponsePayload::err(
                    error_code::INVALID_SYNTAX,
                    format!(":wrap {other} — expected 'on', 'off', or 'status'"),
                ),
            }
        }

        ResolvedCommand::Cd { path } => {
            let snapshot = match get_head_snapshot(sys).await {
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
            match fork_scope(sys, delta).await {
                Ok(hash) => match get_scope_snapshot_by_hash(sys, hash).await {
                    Ok(updated) => ResponsePayload::Ok(OkPayload::ScopeCreated {
                        hash: hash.to_string(),
                        summary: format_scope_change_summary(hash, &snapshot, &updated),
                    }),
                    Err(message) => ResponsePayload::err(error_code::INTERNAL, message),
                },
                Err(response) => response,
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
            let wrapper_enabled = state.wrapper_enabled(config);
            spawn_chain(
                SpawnChainRequest {
                    chain,
                    scope_hash: start_scope,
                    options: ChainSpawnOptions {
                        cwd_override: None,
                        scope_enabled: false,
                        wrapper_enabled,
                        pty_enabled: true,
                        direct_output_client: None,
                    },
                    warnings: Vec::new(),
                    retain_completed_chain: false,
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
            let text = format_log_text(state, id.as_deref());
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
            let text = format_log_text(state, id.as_deref());
            let (text, truncated) = limit_text(text, limit, tail_bytes);
            ResponsePayload::Ok(OkPayload::TextOutput { text, truncated })
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
            ResponsePayload::Ok(OkPayload::TextOutput { text, truncated })
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

async fn resolve_command_scope(
    sys: &ActorSystem,
    scope_override: Option<ScopeHash>,
) -> Result<ScopeHash, ResponsePayload> {
    match scope_override {
        Some(scope) => Ok(scope),
        None => get_head_scope(sys).await,
    }
}

async fn create_isolated_script_scope(sys: &ActorSystem) -> Result<ScopeHash, ResponsePayload> {
    let head = get_head_scope(sys).await?;
    derive_scope(
        sys,
        head,
        cue_core::scope::EnvDelta {
            set: std::collections::BTreeMap::new(),
            unset: vec![],
            cwd: None,
        },
    )
    .await
    .map_err(|error| ResponsePayload::err(error_code::INTERNAL, error))
}

/// Get the HEAD scope hash from the scope store.
async fn get_head_scope(sys: &ActorSystem) -> Result<ScopeHash, ResponsePayload> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    if sys
        .scope_store
        .send(ScopeStoreMsg::GetHead { reply: tx })
        .await
        .is_err()
    {
        return Err(ResponsePayload::err(
            error_code::INTERNAL,
            "scope_store unreachable",
        ));
    }
    rx.await
        .map_err(|_| ResponsePayload::err(error_code::INTERNAL, "scope_store reply dropped"))
}

/// Parse a string like `"J5"` into a `JobId`.
fn parse_job_id(s: &str) -> Option<JobId> {
    s.trim().parse().ok()
}

/// Parse a string like `"CH3"` into a `ChainId`.
fn parse_chain_id(s: &str) -> Option<ChainId> {
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

fn sorted_job_list(state: &SchedulerState) -> Vec<JobInfo> {
    let mut entries: Vec<&JobEntry> = state.jobs.values().collect();
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
        Ok(Ok((head, mut scopes))) => {
            let head_str = head.to_string();
            scopes.sort_by(|a, b| {
                let a_head = a.hash == head_str;
                let b_head = b.hash == head_str;
                b_head.cmp(&a_head).then(a.hash.cmp(&b.hash))
            });
            ResponsePayload::Ok(OkPayload::ScopeList(scopes))
        }
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
        Ok(Ok((head, mut scopes))) => {
            let head_str = head.to_string();
            scopes.sort_by(|a, b| {
                let a_head = a.hash == head_str;
                let b_head = b.hash == head_str;
                b_head.cmp(&a_head).then(a.hash.cmp(&b.hash))
            });
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
fn format_log_text(state: &SchedulerState, id: Option<&str>) -> String {
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

    let mut jobs: Vec<&JobEntry> = state.jobs.values().collect();
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

    let mut crons: Vec<&CronEntry> = state.crons.values().collect();
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

/// Format the active config as human-readable text.
fn format_config_text(config: &Config) -> String {
    format!("weft.socket_path = {}", config.weft.socket_path.display())
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
        Ok(Some(snapshot)) => {
            let text = String::from_utf8_lossy(&snapshot.data).into_owned();
            ResponsePayload::Ok(OkPayload::Output {
                id,
                data: text,
                truncated: snapshot.truncated,
            })
        }
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
            data, truncated, ..
        }) => StreamText { data, truncated },
        error => return error,
    };
    let stderr = match read_job_stderr(sys, job_id, display_id, stderr_limit).await {
        ResponsePayload::Ok(OkPayload::Output {
            data, truncated, ..
        }) => StreamText { data, truncated },
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
        })) => {
            let text = String::from_utf8_lossy(&data).into_owned();
            ResponsePayload::Ok(OkPayload::Output {
                id,
                data: text,
                truncated,
            })
        }
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
    if let ResponsePayload::Ok(OkPayload::Output { ref mut data, .. }) = resp {
        *data = format!("[PTY: stdout and stderr are merged]\n{data}");
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
        Ok(Ok(LogTail { data, truncated })) => {
            return ResponsePayload::Ok(OkPayload::Output {
                id,
                data: String::from_utf8_lossy(&data).into_owned(),
                truncated,
            });
        }
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
            let body = String::from_utf8_lossy(&data).into_owned();
            ResponsePayload::Ok(OkPayload::Output {
                id: id2,
                data: format!("[PTY: stdout and stderr are merged]\n{body}"),
                truncated,
            })
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
        Ok(LogTail { data, truncated }) => ResponsePayload::Ok(OkPayload::Output {
            id,
            data: String::from_utf8_lossy(&data).into_owned(),
            truncated,
        }),
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
        };
        (sys, gw_rx, sched_rx, pm_rx, ss_rx, eb_rx)
    }

    fn test_db() -> Arc<Mutex<Connection>> {
        Arc::new(Mutex::new(
            storage::open_db(Path::new(":memory:")).expect("open test db"),
        ))
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
            ChainSpawnOptions {
                cwd_override: None,
                scope_enabled: false,
                wrapper_enabled: false,
                pty_enabled: true,
                direct_output_client: None,
            },
        )
    }

    fn test_scope_chain_spawn(chain: ChainNode, scope_hash: ScopeHash) -> SpawnChainRequest {
        test_chain_spawn_with_options(
            chain,
            scope_hash,
            ChainSpawnOptions {
                cwd_override: None,
                scope_enabled: true,
                wrapper_enabled: false,
                pty_enabled: true,
                direct_output_client: None,
            },
        )
    }

    fn test_chain_spawn_with_options(
        chain: ChainNode,
        scope_hash: ScopeHash,
        options: ChainSpawnOptions,
    ) -> SpawnChainRequest {
        SpawnChainRequest {
            chain,
            scope_hash,
            options,
            warnings: Vec::new(),
            retain_completed_chain: false,
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

    /// Spawn a fake scope_store that always replies with a zero hash.
    fn spawn_fake_scope_store(mut rx: mpsc::Receiver<ScopeStoreMsg>) {
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                match msg {
                    ScopeStoreMsg::GetHead { reply } => {
                        let _ = reply.send(ScopeHash([0u8; 32]));
                    }
                    ScopeStoreMsg::GetHeadSnapshot { reply } => {
                        let _ = reply.send(Ok(cue_core::scope::EnvSnapshot {
                            env: std::collections::BTreeMap::new(),
                            cwd: std::env::current_dir().expect("current dir"),
                        }));
                    }
                    ScopeStoreMsg::GetScope { hash, reply } => {
                        let _ = reply.send(Ok(Some(cue_core::scope::Scope {
                            hash,
                            parent: None,
                            delta: None,
                            snapshot: Some(cue_core::scope::EnvSnapshot {
                                env: std::collections::BTreeMap::new(),
                                cwd: std::env::current_dir().expect("current dir"),
                            }),
                        })));
                    }
                    ScopeStoreMsg::Derive { base, delta, reply } => {
                        let snapshot = cue_core::scope::EnvSnapshot {
                            env: std::collections::BTreeMap::new(),
                            cwd: std::env::current_dir().expect("current dir"),
                        };
                        let child = cue_core::scope::Scope::fork(base, &snapshot, delta);
                        let _ = reply.send(Ok(child.hash));
                    }
                    ScopeStoreMsg::ListScopes { reply } => {
                        let cwd = std::env::current_dir()
                            .expect("current dir")
                            .display()
                            .to_string();
                        let _ = reply.send(Ok((
                            ScopeHash([0u8; 32]),
                            vec![
                                cue_core::ipc::ScopeInfo {
                                    hash: ScopeHash([0u8; 32]).to_string(),
                                    parent: None,
                                    cwd: cwd.clone(),
                                    env_count: 0,
                                },
                                cue_core::ipc::ScopeInfo {
                                    hash: ScopeHash([1u8; 32]).to_string(),
                                    parent: None,
                                    cwd,
                                    env_count: 0,
                                },
                            ],
                        )));
                    }
                    ScopeStoreMsg::Shutdown => break,
                    _ => {}
                }
            }
        });
    }

    fn spawn_scope_store_with_head_snapshot_error(mut rx: mpsc::Receiver<ScopeStoreMsg>) {
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                match msg {
                    ScopeStoreMsg::GetHeadSnapshot { reply } => {
                        let _ = reply.send(Err(anyhow::anyhow!("head snapshot unavailable")));
                    }
                    ScopeStoreMsg::Shutdown => break,
                    _ => {}
                }
            }
        });
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
                pipeline_text: "sleep 60".into(),
                status: JobStatus::Running,
                exit_code: None,
                start_scope: Some(ScopeHash([0; 32])),
                end_scope: None,
                open_hint: JobOpenHint::Stream,
                chain_id: None,
                chain_index: None,
                chain_total: None,
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
                } => {
                    assert_eq!(channel, EventChannel::Jobs);
                    events.push((script_id, status, exit_code, failed_item_index));
                }
                _ => {}
            }
        }
        events
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
            EventBusMsg::PublishExcept {
                channel,
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
    async fn run_wrapper_param_overrides_session_and_config() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let mut config = Config::default();
        config.wrapper.enabled = false;
        let mut state = SchedulerState::new();
        state.wrapper_enabled = Some(false);
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
    async fn head_snapshot_errors_are_reported_to_scheduler_callers() {
        let (sys, _gw_rx, _sched_rx, _pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_scope_store_with_head_snapshot_error(ss_rx);

        let error = get_head_snapshot(&sys)
            .await
            .expect_err("head snapshot error should be reported");

        match error {
            ResponsePayload::Err { code, message } => {
                assert_eq!(code, error_code::INTERNAL);
                assert!(message.contains("head snapshot unavailable"));
            }
            other => panic!("expected error response, got {other:?}"),
        }
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
        let config = Config::default();
        let mut state = SchedulerState::new();
        state.crons.insert(
            CronId(1),
            CronEntry {
                cron_id: CronId(1),
                schedule: CronSchedule::Delay(std::time::Duration::from_secs(1)),
                chain: leaf("git commit --no-verify"),
                scope_hash: ScopeHash([0; 32]),
                status: CronStatus::Scheduled,
                next_trigger: Instant::now(),
                cwd_override: None,
                scope_enabled: false,
                wrapper_enabled: false,
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
        let config = Config::default();
        let mut state = SchedulerState::new();
        state.crons.insert(
            CronId(1),
            CronEntry {
                cron_id: CronId(1),
                schedule: CronSchedule::Delay(std::time::Duration::from_secs(1)),
                chain: leaf("echo due"),
                scope_hash: ScopeHash([0; 32]),
                status: CronStatus::Scheduled,
                next_trigger: Instant::now(),
                cwd_override: None,
                scope_enabled: false,
                wrapper_enabled: false,
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
            EventBusMsg::Publish {
                channel,
                payload: EventPayload::CronRemoved { cron_id },
            } => {
                assert_eq!(channel, EventChannel::Crons);
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
        let guard = conn.lock().unwrap();
        storage::upsert_job_history(
            &guard,
            &storage::StoredJob {
                id: "J7".into(),
                pipeline: "cargo test".into(),
                status: JobStatus::Done,
                exit_code: Some(0),
                start_scope: Some(ScopeHash([3; 32])),
                end_scope: Some(ScopeHash([3; 32])),
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
        let guard = conn.lock().unwrap();
        storage::upsert_cron(
            &guard,
            &storage::StoredCron {
                id: "C4".into(),
                schedule: "every 5m".into(),
                command: "echo hello".into(),
                status: CronStatus::Scheduled,
                scope_hash: Some(ScopeHash([5; 32])),
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
        let guard = conn.lock().unwrap();
        storage::upsert_cron(
            &guard,
            &storage::StoredCron {
                id: "C1".into(),
                schedule: "in 1s".into(),
                command: "echo late".into(),
                status: CronStatus::Scheduled,
                scope_hash: Some(ScopeHash([8; 32])),
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
        let guard = conn.lock().unwrap();
        storage::upsert_cron(
            &guard,
            &storage::StoredCron {
                id: "C1".into(),
                schedule: "in 500ms".into(),
                command: "echo soon".into(),
                status: CronStatus::Scheduled,
                scope_hash: Some(ScopeHash([8; 32])),
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
        let guard = conn.lock().unwrap();
        storage::upsert_cron(
            &guard,
            &storage::StoredCron {
                id: "C1".into(),
                schedule: "in 1500ms".into(),
                command: "echo late".into(),
                status: CronStatus::Scheduled,
                scope_hash: Some(ScopeHash([8; 32])),
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
}
