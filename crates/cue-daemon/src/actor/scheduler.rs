//! Scheduler actor — command routing, ID assignment, chain execution, cron timer heap.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use chrono::{
    DateTime, Datelike, Duration as ChronoDuration, Local, LocalResult, NaiveTime, TimeZone,
    Timelike,
};
use rusqlite::Connection;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use cue_core::command_spec::{COMMAND_SPECS, CommandCategory, CommandSpec, command_spec};
use cue_core::cron::{
    CronPreset, CronSchedule, CronStatus, DayFilter, Weekday, parse_day_filter, parse_time_of_day,
};
use cue_core::ipc::{
    ChainInfo, ChainJobInfo, CronInfo, EventPayload, JobInfo, JobOpenHint, OkPayload,
    ResponsePayload, ScriptItemInfo, ScriptItemResult, ScriptSubmitError, error_code,
};
use cue_core::job::{CancelReason, JobStatus};
use cue_core::mode::Mode;
use cue_core::pipeline::{ChainNode, ParallelOp, SerialOp};
use cue_core::scope::EnvSnapshot;
use cue_core::{ChainId, CronId, JobId, ScopeHash, ScriptId};

use crate::config::{BlockDecision, Config};
use crate::parser::parse::Parser as CueParser;
use crate::parser::resolver::{ResolvedCommand, Resolver};
use crate::parser::token::Token;
use crate::parser::tokenizer::Tokenizer;
use crate::storage;
use crate::word_expansion::expand_command_line;

use super::{ActorSystem, GatewayMsg, ProcessMgrMsg, SchedulerMsg, ScopeStoreMsg, StderrSnapshot};

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
    /// Reserved for auto-retry: maximum retry attempts (0 = no retry).
    #[allow(dead_code)]
    retry_max: u32,
    /// Reserved for auto-retry: delay between retry attempts.
    retry_delay: Option<std::time::Duration>,
    /// Reserved for auto-retry: attempts made so far.
    #[allow(dead_code)]
    retry_attempts: u32,
    /// Maps each leaf index (0-based, left-to-right DFS) to its `JobId`.
    leaf_jobs: HashMap<usize, JobId>,
    /// Maps each leaf index to its current status.
    leaf_status: HashMap<usize, LeafStatus>,
    scope_hash: ScopeHash,
    pipeline_text: String,
    /// Explicit working directory override for all jobs in this chain.
    cwd_override: Option<std::path::PathBuf>,
    /// Whether the wrapper binary is enabled for this chain's jobs.
    wrapper_enabled: bool,
}

/// Flattened representation of a chain leaf for easy lookup.
struct FlatLeaf {
    /// Index in the DFS-order leaf list.
    index: usize,
    /// Full pipeline (may be multi-segment with pipe operators).
    pipeline: cue_core::pipeline::Pipeline,
    /// Human-readable pipeline text.
    pipeline_text: String,
}

impl FlatLeaf {
    /// First segment's command words (for scope-transform detection and open hint).
    fn command(&self) -> &[String] {
        self.pipeline
            .segments
            .first()
            .map(|s| s.command.as_slice())
            .unwrap_or(&[])
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
struct CronEntry {
    cron_id: CronId,
    schedule_text: String,
    schedule: CronSchedule,
    chain: ChainNode,
    scope_hash: ScopeHash,
    status: CronStatus,
    next_trigger: Instant,
    /// Explicit working directory override for jobs spawned by this cron.
    cwd_override: Option<std::path::PathBuf>,
}

#[derive(Debug, Clone, Copy)]
struct PendingWait {
    client_id: u64,
    request_id: u32,
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
    /// Runtime wrapper toggle set by `:wrap on` / `:wrap off`.
    wrapper_enabled: Option<bool>,
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

/// Spawn the Scheduler actor task.
pub fn spawn(mut rx: mpsc::Receiver<SchedulerMsg>, conn: Connection, sys: ActorSystem) {
    tokio::spawn(async move {
        let db = storage::shared_connection(conn);
        let config = sys.config.clone();
        let mut state = SchedulerState::new();
        restore_jobs(&db, &mut state).await;
        restore_crons(&db, &mut state).await;
        restore_script_counter(&db, &mut state).await;
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
                            match command {
                                ResolvedCommand::Wait { id } => {
                                    if let Some(response) = handle_wait_command(
                                        id,
                                        client_id,
                                        request_id,
                                        &mut state,
                                        &sys,
                                    )
                                    .await
                                    {
                                        let _ = sys.gateway.send(GatewayMsg::SendResponse {
                                            client_id,
                                            request_id,
                                            payload: response,
                                        }).await;
                                    }
                                }
                                other => {
                                    let response =
                                        handle_command(other, client_id, &mut state, &db, &config, &sys)
                                            .await;
                                    let _ = sys.gateway.send(GatewayMsg::SendResponse {
                                        client_id,
                                        request_id,
                                        payload: response,
                                    }).await;
                                }
                            }
                        }

                        SchedulerMsg::JobFinished { job_id, exit_code } => {
                            handle_job_finished(job_id, exit_code, &mut state, &db, &sys).await;
                        }

                        SchedulerMsg::Shutdown => {
                            debug!("scheduler: shutting down");

                            // FIX 4: Cancel all active chain jobs before shutting down.
                            let chain_ids: Vec<ChainId> =
                                state.chains.keys().copied().collect();
                            for chain_id in chain_ids {
                                if let Some(chain) = state.chains.get(&chain_id) {
                                    let leaf_indices: Vec<usize> =
                                        chain.leaf_status.keys().copied().collect();
                                    for idx in leaf_indices {
                                        let chain = state.chains.get(&chain_id).unwrap();
                                        let status = chain.leaf_status.get(&idx).cloned();
                                        match status {
                                            Some(LeafStatus::Running) => {
                                                if let Some(&jid) =
                                                    chain.leaf_jobs.get(&idx)
                                                {
                                                    let _ = sys
                                                        .process_mgr
                                                        .send(ProcessMgrMsg::KillJob {
                                                            job_id: jid,
                                                        })
                                                        .await;
                                                    if let Some(entry) =
                                                        state.jobs.get_mut(&jid)
                                                    {
                                                        entry.status =
                                                            JobStatus::Cancelled(
                                                                CancelReason::ChainAborted,
                                                            );
                                                        persist_job_entry(&db, entry);
                                                    }
                                                }
                                                let chain =
                                                    state.chains.get_mut(&chain_id).unwrap();
                                                chain.leaf_status
                                                    .insert(idx, LeafStatus::Cancelled);
                                            }
                                            Some(LeafStatus::Pending) => {
                                                let chain =
                                                    state.chains.get_mut(&chain_id).unwrap();
                                                chain.leaf_status
                                                    .insert(idx, LeafStatus::Cancelled);
                                                if let Some(&jid) =
                                                    chain.leaf_jobs.get(&idx)
                                                && let Some(entry) =
                                                    state.jobs.get_mut(&jid)
                                                {
                                                    entry.status =
                                                        JobStatus::Cancelled(
                                                            CancelReason::ChainAborted,
                                                        );
                                                    persist_job_entry(&db, entry);
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
                                    persist_job_entry(&db, entry);
                                }
                            }

                            break;
                        }
                    }
                    prune_retained_job_history(&mut state, &db, &config, &sys).await;
                }

                () = &mut sleep => {
                    // A cron timer has fired.
                    fire_due_crons(&mut state, &db, &sys).await;
                }
            }
        }

        debug!("scheduler: stopped");
    });
}

async fn restore_jobs(db: &storage::SharedConnection, state: &mut SchedulerState) {
    let restored = match storage::with_connection(db, storage::load_job_history).await {
        Ok(jobs) => jobs,
        Err(e) => {
            warn!("scheduler: failed to load job history: {e}");
            return;
        }
    };

    let mut max_job = 0;
    for job in restored {
        let Some(job_id) = parse_job_id(&job.id) else {
            warn!(id = %job.id, "scheduler: skipping invalid persisted job id");
            continue;
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
}

async fn restore_crons(db: &storage::SharedConnection, state: &mut SchedulerState) {
    let restored = match storage::with_connection(db, storage::load_crons).await {
        Ok(crons) => crons,
        Err(e) => {
            warn!("scheduler: failed to load crons: {e}");
            return;
        }
    };

    let mut max_cron = 0;
    for cron in restored {
        let Some(cron_id) = parse_cron_id(&cron.id) else {
            warn!(id = %cron.id, "scheduler: skipping invalid persisted cron id");
            continue;
        };
        max_cron = max_cron.max(cron_id.0);

        let Some(scope_hash) = cron.scope_hash else {
            warn!(id = %cron.id, "scheduler: skipping cron without persisted scope");
            continue;
        };

        let Some(schedule) = parse_schedule(&cron.schedule) else {
            warn!(id = %cron.id, schedule = %cron.schedule, "scheduler: skipping cron with invalid schedule");
            continue;
        };

        let chain = match parse_chain_text(&cron.command) {
            Ok(chain) => chain,
            Err(error) => {
                warn!(id = %cron.id, command = %cron.command, "scheduler: skipping cron with invalid command: {error}");
                continue;
            }
        };

        let mut status = cron.status;
        if status.is_runnable()
            && let CronSchedule::Delay(duration) = &schedule
            && cron.age_secs.max(0) as u64 >= duration.as_secs()
        {
            status = CronStatus::Expired;
            let stored = storage::StoredCron {
                id: cron.id.clone(),
                schedule: cron.schedule.clone(),
                command: cron.command.clone(),
                status,
                scope_hash: cron.scope_hash,
                cwd_override: cron.cwd_override.clone(),
                age_secs: cron.age_secs,
            };
            if let Err(e) =
                storage::with_connection(db, move |conn| storage::upsert_cron(conn, &stored)).await
            {
                warn!(id = %cron.id, "scheduler: failed to persist expired cron history: {e}");
            }
        }
        let next_trigger = if status.is_terminal() {
            Instant::now()
        } else {
            let Some(next_trigger) = next_trigger_instant(&schedule, cron.age_secs) else {
                warn!(id = %cron.id, schedule = %cron.schedule, "scheduler: skipping cron with unreachable next trigger");
                continue;
            };
            next_trigger
        };

        state.crons.insert(
            cron_id,
            CronEntry {
                cron_id,
                schedule_text: cron.schedule,
                schedule,
                chain,
                scope_hash,
                status,
                next_trigger,
                cwd_override: cron.cwd_override,
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
}

async fn restore_script_counter(db: &storage::SharedConnection, state: &mut SchedulerState) {
    match storage::with_connection(db, storage::max_script_run_id).await {
        Ok(Some(max_id)) => {
            state.next_script = max_id + 1;
        }
        Ok(None) => {}
        Err(error) => warn!("scheduler: failed to restore script counter: {error}"),
    }
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
            let _ = sys
                .gateway
                .send(GatewayMsg::PushEvent {
                    payload: EventPayload::JobRemoved {
                        job_id: job_id.to_string(),
                    },
                    channel: "jobs".to_string(),
                })
                .await;
            remove_job_logs(job_id).await;
        }
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

fn persist_job_entry(db: &storage::SharedConnection, entry: &JobEntry) {
    if !entry.status.is_terminal() {
        return;
    }

    let job_id = entry.job_id.to_string();
    let stored = storage::StoredJob {
        id: job_id.clone(),
        pipeline: entry.pipeline_text.clone(),
        status: entry.status.clone(),
        exit_code: entry.exit_code,
        start_scope: entry.start_scope,
        end_scope: entry.end_scope,
        chain_id: entry.chain_id.map(|id| id.to_string()),
        stderr: String::new(),
    };
    let db = Arc::clone(db);
    tokio::spawn(async move {
        if let Err(error) =
            storage::with_connection(&db, move |conn| storage::upsert_job_history(conn, &stored))
                .await
        {
            warn!(job = %job_id, "scheduler: failed to persist job history: {error}");
        }
    });
}

fn persist_cron_entry(db: &storage::SharedConnection, entry: &CronEntry) {
    persist_cron_record(
        db,
        &storage::StoredCron {
            id: entry.cron_id.to_string(),
            schedule: entry.schedule_text.clone(),
            command: chain_to_text(&entry.chain),
            status: entry.status,
            scope_hash: Some(entry.scope_hash),
            cwd_override: entry.cwd_override.clone(),
            age_secs: 0,
        },
    );
}

fn persist_cron_record(db: &storage::SharedConnection, cron: &storage::StoredCron) {
    let cron_id = cron.id.clone();
    let stored = cron.clone();
    let db = Arc::clone(db);
    tokio::spawn(async move {
        if let Err(error) =
            storage::with_connection(&db, move |conn| storage::upsert_cron(conn, &stored)).await
        {
            warn!(cron = %cron_id, "scheduler: failed to persist cron: {error}");
        }
    });
}

fn remove_cron_from_db(db: &storage::SharedConnection, cid: CronId) {
    let cron_id = cid.to_string();
    let db = Arc::clone(db);
    tokio::spawn(async move {
        let cid_for_db = cron_id.clone();
        if let Err(error) =
            storage::with_connection(&db, move |conn| storage::delete_cron(conn, &cid_for_db)).await
        {
            warn!(cron = %cron_id, "scheduler: failed to remove cron from db: {error}");
        }
    });
}

async fn get_head_snapshot(
    sys: &ActorSystem,
) -> Result<cue_core::scope::EnvSnapshot, ResponsePayload> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let _ = sys
        .scope_store
        .send(ScopeStoreMsg::GetHeadSnapshot { reply: tx })
        .await;
    match rx.await {
        Ok(Some(snapshot)) => Ok(snapshot),
        Ok(None) => Err(ResponsePayload::err(
            error_code::INTERNAL,
            "head scope has no snapshot",
        )),
        Err(_) => Err(ResponsePayload::err(
            error_code::INTERNAL,
            "scope_store unreachable",
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
        ChainNode::Leaf(pipeline) => {
            let idx = out.len();
            let pipeline_text = pipeline_to_text(pipeline);
            out.push(FlatLeaf {
                index: idx,
                pipeline: pipeline.clone(),
                pipeline_text,
            });
        }
        ChainNode::Serial { left, right, .. } | ChainNode::Parallel { left, right, .. } => {
            flatten_leaves_inner(left, out);
            flatten_leaves_inner(right, out);
        }
    }
}

/// Convert a `Pipeline` to a human-readable string.
fn pipeline_to_text(pipeline: &cue_core::pipeline::Pipeline) -> String {
    pipeline
        .segments
        .iter()
        .map(|s| {
            let cmd = s.command.join(" ");
            match s.pipe_to_next {
                Some(cue_core::pipeline::PipeOp::Stdout) => format!("{cmd} |>"),
                Some(cue_core::pipeline::PipeOp::StdoutStderr) => format!("{cmd} |&>"),
                Some(cue_core::pipeline::PipeOp::StderrOnly) => format!("{cmd} |!>"),
                None => cmd,
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Convert a full `ChainNode` to text.
fn chain_to_text(node: &ChainNode) -> String {
    match node {
        ChainNode::Leaf(p) => pipeline_to_text(p),
        ChainNode::Serial { left, op, right } => {
            let op_str = match op {
                SerialOp::Then => "->",
                SerialOp::Always => "~>",
            };
            format!("{} {op_str} {}", chain_to_text(left), chain_to_text(right))
        }
        ChainNode::Parallel { left, op, right } => {
            let op_str = match op {
                ParallelOp::All => "||",
                ParallelOp::Race => "||?",
            };
            format!("{} {op_str} {}", chain_to_text(left), chain_to_text(right))
        }
    }
}

fn parse_chain_text(text: &str) -> Result<ChainNode, String> {
    let ast = CueParser::parse(&format!(":run {text}")).map_err(|err| err.message)?;
    match Resolver::resolve(ast, cue_core::Mode::Job).map_err(|err| err.message)? {
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

            // FIX 3: For Race, check entire branch success (subtree terminal + all ok),
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

// ── Cron schedule parsing ───────────────────────────────────────────────────

fn parse_schedule(text: &str) -> Option<CronSchedule> {
    let words: Vec<&str> = text.split_whitespace().collect();
    let keyword = *words.first()?;
    match keyword {
        "every" if words.len() == 2 => Some(CronSchedule::Interval(parse_duration(words.get(1)?)?)),
        "in" if words.len() == 2 => Some(CronSchedule::Delay(parse_duration(words.get(1)?)?)),
        "at" => {
            let time_secs = parse_time_of_day(words.get(1)?)?;
            let days = if words.get(2) == Some(&"on") {
                Some(parse_day_filter(words.get(3)?)?)
            } else {
                None
            };
            if !(words.len() == 2 || words.len() == 4 && words.get(2) == Some(&"on")) {
                return None;
            }
            Some(CronSchedule::TimeOfDay { time_secs, days })
        }
        "daily" if words.len() == 1 => Some(CronSchedule::Preset(CronPreset::Daily)),
        "hourly" if words.len() == 1 => Some(CronSchedule::Preset(CronPreset::Hourly)),
        "weekly" if words.len() == 1 => Some(CronSchedule::Preset(CronPreset::Weekly)),
        "monthly" if words.len() == 1 => Some(CronSchedule::Preset(CronPreset::Monthly)),
        "cron" if words.len() == 6 => {
            let expr = words.get(1..6)?.join(" ");
            validate_crontab(&expr)?;
            Some(CronSchedule::Crontab(expr))
        }
        _ => {
            validate_crontab(text)?;
            Some(CronSchedule::Crontab(text.to_string()))
        }
    }
}

fn next_trigger_instant(schedule: &CronSchedule, age_secs: i64) -> Option<Instant> {
    match schedule {
        CronSchedule::Interval(duration) => Some(Instant::now() + *duration),
        CronSchedule::Delay(duration) => {
            let remaining =
                duration.saturating_sub(std::time::Duration::from_secs(age_secs.max(0) as u64));
            if remaining.is_zero() && age_secs > 0 {
                None
            } else {
                Some(Instant::now() + remaining)
            }
        }
        CronSchedule::TimeOfDay { time_secs, days } => instant_from_local(
            next_time_of_day_occurrence(Local::now(), *time_secs, days.as_ref())?,
        ),
        CronSchedule::Preset(preset) => {
            instant_from_local(next_preset_occurrence(Local::now(), *preset)?)
        }
        CronSchedule::Crontab(expr) => {
            instant_from_local(next_crontab_occurrence(Local::now(), expr)?)
        }
    }
}

fn instant_from_local(target: DateTime<Local>) -> Option<Instant> {
    let delay = (target - Local::now()).to_std().ok()?;
    Some(Instant::now() + delay)
}

fn next_time_of_day_occurrence(
    now: DateTime<Local>,
    time_secs: u32,
    days: Option<&DayFilter>,
) -> Option<DateTime<Local>> {
    let time = NaiveTime::from_num_seconds_from_midnight_opt(time_secs, 0)?;
    for day_offset in 0..14 {
        let date = now.date_naive() + ChronoDuration::days(day_offset);
        let weekday = chrono_weekday_to_core(date.weekday());
        if days.is_none_or(|filter| filter.days.contains(&weekday)) {
            let candidate = local_datetime(
                date.year(),
                date.month(),
                date.day(),
                time.hour(),
                time.minute(),
            )?;
            if candidate > now {
                return Some(candidate);
            }
        }
    }
    None
}

fn next_preset_occurrence(now: DateTime<Local>, preset: CronPreset) -> Option<DateTime<Local>> {
    match preset {
        CronPreset::Hourly => {
            let next =
                now.with_minute(0)?.with_second(0)?.with_nanosecond(0)? + ChronoDuration::hours(1);
            Some(next)
        }
        CronPreset::Daily => {
            let date = now.date_naive() + ChronoDuration::days(1);
            local_datetime(date.year(), date.month(), date.day(), 0, 0)
        }
        CronPreset::Weekly => {
            let today = now.date_naive();
            let days_until_monday = (8 - today.weekday().number_from_monday()) % 7;
            let offset = if days_until_monday == 0 {
                7
            } else {
                days_until_monday
            };
            let date = today + ChronoDuration::days(offset.into());
            local_datetime(date.year(), date.month(), date.day(), 0, 0)
        }
        CronPreset::Monthly => {
            let (year, month) = if now.month() == 12 {
                (now.year() + 1, 1)
            } else {
                (now.year(), now.month() + 1)
            };
            local_datetime(year, month, 1, 0, 0)
        }
    }
}

fn next_crontab_occurrence(now: DateTime<Local>, expr: &str) -> Option<DateTime<Local>> {
    let matcher = parse_crontab(expr)?;
    let mut candidate = now.with_second(0)?.with_nanosecond(0)? + ChronoDuration::minutes(1);
    for _ in 0..(366 * 24 * 60) {
        if matcher.matches(candidate) {
            return Some(candidate);
        }
        candidate += ChronoDuration::minutes(1);
    }
    None
}

/// Parse a bare duration like `5m`, `30s`, `1h`, `2d`.
fn parse_duration(s: &str) -> Option<std::time::Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let (num_part, unit) = s.split_at(s.len() - 1);
    let n: u64 = num_part.parse().ok()?;
    let secs = match unit {
        "s" => n,
        "m" => n * 60,
        "h" => n * 3600,
        "d" => n * 86400,
        _ => return None,
    };
    Some(std::time::Duration::from_secs(secs))
}

fn chrono_weekday_to_core(day: chrono::Weekday) -> Weekday {
    match day {
        chrono::Weekday::Mon => Weekday::Mon,
        chrono::Weekday::Tue => Weekday::Tue,
        chrono::Weekday::Wed => Weekday::Wed,
        chrono::Weekday::Thu => Weekday::Thu,
        chrono::Weekday::Fri => Weekday::Fri,
        chrono::Weekday::Sat => Weekday::Sat,
        chrono::Weekday::Sun => Weekday::Sun,
    }
}

fn local_datetime(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
) -> Option<DateTime<Local>> {
    match Local.with_ymd_and_hms(year, month, day, hour, minute, 0) {
        LocalResult::Single(dt) => Some(dt),
        LocalResult::Ambiguous(early, _) => Some(early),
        LocalResult::None => None,
    }
}

#[derive(Clone)]
struct CrontabMatcher {
    minute: Vec<u32>,
    hour: Vec<u32>,
    day_of_month: Vec<u32>,
    month: Vec<u32>,
    day_of_week: Vec<u32>,
}

impl CrontabMatcher {
    fn matches(&self, dt: DateTime<Local>) -> bool {
        let weekday = match dt.weekday() {
            chrono::Weekday::Sun => 0,
            other => other.number_from_monday(),
        };
        self.minute.contains(&dt.minute())
            && self.hour.contains(&dt.hour())
            && self.day_of_month.contains(&dt.day())
            && self.month.contains(&dt.month())
            && self.day_of_week.contains(&weekday)
    }
}

fn validate_crontab(expr: &str) -> Option<()> {
    parse_crontab(expr).map(|_| ())
}

fn parse_crontab(expr: &str) -> Option<CrontabMatcher> {
    let fields: Vec<&str> = expr.split_whitespace().collect();
    if fields.len() != 5 {
        return None;
    }
    Some(CrontabMatcher {
        minute: parse_cron_field(fields[0], 0, 59, &[])?,
        hour: parse_cron_field(fields[1], 0, 23, &[])?,
        day_of_month: parse_cron_field(fields[2], 1, 31, &[])?,
        month: parse_cron_field(
            fields[3],
            1,
            12,
            &[
                ("jan", 1),
                ("feb", 2),
                ("mar", 3),
                ("apr", 4),
                ("may", 5),
                ("jun", 6),
                ("jul", 7),
                ("aug", 8),
                ("sep", 9),
                ("oct", 10),
                ("nov", 11),
                ("dec", 12),
            ],
        )?,
        day_of_week: parse_cron_field(
            fields[4],
            0,
            7,
            &[
                ("sun", 0),
                ("mon", 1),
                ("tue", 2),
                ("wed", 3),
                ("thu", 4),
                ("fri", 5),
                ("sat", 6),
            ],
        )?
        .into_iter()
        .map(|value| if value == 7 { 0 } else { value })
        .collect(),
    })
}

fn parse_cron_field(field: &str, min: u32, max: u32, names: &[(&str, u32)]) -> Option<Vec<u32>> {
    let normalized = field.trim().to_ascii_lowercase();
    let mut values = Vec::new();
    for part in normalized.split(',') {
        let part = part.trim();
        if part.is_empty() {
            return None;
        }
        let expanded = if part == "*" {
            (min..=max).collect::<Vec<_>>()
        } else if let Some(step_text) = part.strip_prefix("*/") {
            let step = step_text.parse::<u32>().ok()?;
            if step == 0 {
                return None;
            }
            (min..=max).step_by(step as usize).collect::<Vec<_>>()
        } else {
            parse_cron_part(part, min, max, names)?
        };
        values.extend(expanded);
    }
    values.sort_unstable();
    values.dedup();
    Some(values)
}

fn parse_cron_part(part: &str, min: u32, max: u32, names: &[(&str, u32)]) -> Option<Vec<u32>> {
    let (range_part, step) = if let Some((range, step)) = part.split_once('/') {
        let step = step.parse::<u32>().ok()?;
        if step == 0 {
            return None;
        }
        (range, Some(step))
    } else {
        (part, None)
    };

    let mut values = if let Some((start, end)) = range_part.split_once('-') {
        let start = parse_cron_value(start, names)?;
        let end = parse_cron_value(end, names)?;
        if start > end || start < min || end > max {
            return None;
        }
        (start..=end).collect::<Vec<_>>()
    } else {
        let value = parse_cron_value(range_part, names)?;
        if value < min || value > max {
            return None;
        }
        vec![value]
    };

    if let Some(step) = step {
        values = values
            .into_iter()
            .enumerate()
            .filter_map(|(idx, value)| (idx as u32).is_multiple_of(step).then_some(value))
            .collect();
    }
    Some(values)
}

fn parse_cron_value(input: &str, names: &[(&str, u32)]) -> Option<u32> {
    input.parse::<u32>().ok().or_else(|| {
        names
            .iter()
            .find_map(|(name, value)| (*name == input).then_some(*value))
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
    let _ = sys
        .gateway
        .send(GatewayMsg::PushEvent {
            payload: EventPayload::JobCreated {
                job_id: job_id.to_string(),
                pipeline: pipeline_text.to_string(),
                start_scope: Some(start_scope.to_string()),
                open_hint,
                chain_id,
                chain_index,
                chain_total,
            },
            channel: "jobs".to_string(),
        })
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
    let _ = sys
        .gateway
        .send(GatewayMsg::PushEvent {
            payload: EventPayload::JobStateChanged {
                job_id: job_id.to_string(),
                old_state,
                new_state,
                end_scope: end_scope.map(|hash| hash.to_string()),
                chain_id,
                chain_index,
            },
            channel: "jobs".to_string(),
        })
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
    let _ = sys
        .gateway
        .send(GatewayMsg::PushEvent {
            payload: EventPayload::ChainProgress { chain },
            channel: "jobs".to_string(),
        })
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

fn subtree_contains_scope_transform(node: &ChainNode) -> Result<bool, String> {
    match node {
        ChainNode::Leaf(pipeline) => Ok(scope_transform_from_pipeline(pipeline)?.is_some()),
        ChainNode::Serial { left, right, .. } | ChainNode::Parallel { left, right, .. } => {
            Ok(subtree_contains_scope_transform(left)? || subtree_contains_scope_transform(right)?)
        }
    }
}

fn validate_scope_transform_support(node: &ChainNode) -> Result<(), String> {
    match node {
        ChainNode::Leaf(pipeline) => {
            let _ = scope_transform_from_pipeline(pipeline)?;
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
        Ok(Some(scope)) => scope
            .snapshot
            .ok_or_else(|| format!("scope {hash} has no snapshot")),
        Ok(None) => Err(format!("scope {hash} not found")),
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

fn classify_job_open_hint(pipeline: &cue_core::pipeline::Pipeline) -> JobOpenHint {
    if pipeline.segments.len() != 1 {
        return JobOpenHint::Stream;
    }
    let command_line = &pipeline.segments[0].command;
    let Some(command_word) = command_line.first() else {
        return JobOpenHint::Stream;
    };
    let command = std::path::Path::new(command_word)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(command_word.as_str());
    let args: Vec<&str> = command_line.iter().skip(1).map(String::as_str).collect();

    let prefers_fg = match command {
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
    };

    if prefers_fg {
        JobOpenHint::Fg
    } else {
        JobOpenHint::Stream
    }
}

struct TerminalStateUpdate {
    status: JobStatus,
    exit_code: i32,
    end_scope: Option<ScopeHash>,
    advance_chain: bool,
}

async fn set_job_terminal_state(
    job_id: JobId,
    update: TerminalStateUpdate,
    state: &mut SchedulerState,
    db: &Arc<Mutex<Connection>>,
    sys: &ActorSystem,
) -> Option<(ChainId, Vec<(usize, ScopeHash)>, Vec<usize>)> {
    let TerminalStateUpdate {
        status: new_status,
        exit_code,
        end_scope,
        advance_chain: advance_chain_state,
    } = update;
    let (old_state, effective_end_scope) = {
        let entry = state.jobs.get_mut(&job_id)?;
        if entry.status.is_terminal() {
            entry.exit_code = Some(exit_code);
            if entry.end_scope.is_none() {
                entry.end_scope = end_scope.or(entry.start_scope);
            }
            persist_job_entry(db, entry);
            return None;
        }

        let old_state = entry.status.clone();
        entry.status = new_status.clone();
        entry.exit_code = Some(exit_code);
        entry.end_scope = end_scope.or(entry.start_scope);
        let effective_end_scope = entry.end_scope.or(entry.start_scope);
        persist_job_entry(db, entry);
        (old_state, effective_end_scope)
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

    let (chain_id, leaf_idx) = state.job_to_chain.get(&job_id).copied()?;

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
        return None;
    }

    let next_scope = effective_end_scope
        .or_else(|| state.chains.get(&chain_id).map(|chain| chain.scope_hash))?;
    let (newly_ready, to_cancel) = {
        let chain = state.chains.get(&chain_id)?;
        advance_chain(&chain.node, leaf_idx, &chain.leaf_status)
    };
    Some((
        chain_id,
        newly_ready
            .into_iter()
            .map(|idx| (idx, next_scope))
            .collect(),
        to_cancel,
    ))
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
        let _ = sys
            .gateway
            .send(GatewayMsg::SendResponse {
                client_id: waiter.client_id,
                request_id: waiter.request_id,
                payload: payload.clone(),
            })
            .await;
    }
}

async fn cancel_chain_leaves(
    chain_id: ChainId,
    to_cancel: &[usize],
    state: &mut SchedulerState,
    db: &Arc<Mutex<Connection>>,
    sys: &ActorSystem,
) {
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
            if is_running {
                let _ = sys
                    .process_mgr
                    .send(ProcessMgrMsg::KillJob { job_id: jid })
                    .await;
            }
            let _ = set_job_terminal_state(
                jid,
                TerminalStateUpdate {
                    status: JobStatus::Cancelled(CancelReason::ChainAborted),
                    exit_code: -1,
                    end_scope: None,
                    advance_chain: false,
                },
                state,
                db,
                sys,
            )
            .await;
        } else if let Some(chain) = state.chains.get_mut(&chain_id) {
            chain.leaf_status.insert(idx, LeafStatus::Cancelled);
        }
    }
}

// ── Cron trigger logic ──────────────────────────────────────────────────────

/// Fire all crons whose `next_trigger` has passed.
async fn fire_due_crons(
    state: &mut SchedulerState,
    db: &Arc<Mutex<Connection>>,
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

        info!(%cron_id, "scheduler: cron triggered");

        // Spawn the chain just like `:run`.
        let wrapper_enabled = state.wrapper_enabled(&sys.config);
        let response = spawn_chain(
            chain,
            scope_hash,
            0,
            0,
            cwd_override,
            wrapper_enabled,
            0,
            None,
            state,
            db,
            sys,
        )
        .await;
        let first_job_id = match &response {
            ResponsePayload::Ok(OkPayload::JobCreated { job_id, .. }) => Some(job_id.clone()),
            ResponsePayload::Ok(OkPayload::ChainCreated { chain, .. }) => {
                chain.jobs.iter().find_map(|job| job.job_id.clone())
            }
            _ => None,
        };
        if let Some(job_id) = first_job_id {
            let _ = sys
                .gateway
                .send(GatewayMsg::PushEvent {
                    payload: EventPayload::CronTriggered {
                        cron_id: cron_id.to_string(),
                        job_id,
                    },
                    channel: "crons".into(),
                })
                .await;
        }

        if is_oneshot {
            if let Some(entry) = state.crons.get_mut(&cron_id) {
                entry.status = CronStatus::Completed;
                persist_cron_entry(db, entry);
            }
            debug!(%cron_id, "scheduler: one-shot cron completed");
        } else if let Some(next_trigger) = next_trigger_instant(&schedule, 0)
            && let Some(entry) = state.crons.get_mut(&cron_id)
        {
            entry.next_trigger = next_trigger;
        }
    }
}

// ── Spawn chain / single job ────────────────────────────────────────────────

/// Check whether any pipeline in the chain contains blocked or warned command patterns.
fn check_chain_blocked(chain: &ChainNode, config: &Config) -> Option<BlockDecision> {
    let leaves = flatten_leaves(chain);
    for leaf in &leaves {
        for segment in &leaf.pipeline.segments {
            if let Some(decision) = config.block.check(&segment.command) {
                return Some(decision);
            }
        }
    }
    None
}

/// Spawn a chain (or a single job) from a `ChainNode`, returning the response payload.
#[allow(clippy::too_many_arguments)]
async fn spawn_chain(
    chain: ChainNode,
    scope_hash: ScopeHash,
    _client_id: u64,
    _request_id: u32,
    cwd_override: Option<std::path::PathBuf>,
    wrapper_enabled: bool,
    retry_max: u32,
    retry_delay: Option<std::time::Duration>,
    state: &mut SchedulerState,
    db: &Arc<Mutex<Connection>>,
    sys: &ActorSystem,
) -> ResponsePayload {
    if let Err(message) = validate_scope_transform_support(&chain) {
        return ResponsePayload::err(error_code::INVALID_SYNTAX, message);
    }

    let leaves = flatten_leaves(&chain);

    if leaves.len() == 1 {
        let leaf = &leaves[0];
        let jid = state.alloc_job();
        let open_hint = classify_job_open_hint(&leaf.pipeline);

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

        publish_job_created(sys, state, jid, &leaf.pipeline_text, scope_hash, open_hint).await;

        match scope_transform_from_command(leaf.command()) {
            Ok(Some(_)) => {
                info!(%jid, pipeline = %leaf.pipeline_text, "scheduler: applying single scope-transform job");
                match apply_scope_transform(sys, scope_hash, leaf.command()).await {
                    Ok(end_scope) => {
                        let _ = set_job_terminal_state(
                            jid,
                            TerminalStateUpdate {
                                status: JobStatus::Done,
                                exit_code: 0,
                                end_scope: Some(end_scope),
                                advance_chain: true,
                            },
                            state,
                            db,
                            sys,
                        )
                        .await;
                    }
                    Err(error) => {
                        warn!(%jid, pipeline = %leaf.pipeline_text, "scheduler: scope-transform failed: {error}");
                        let _ = set_job_terminal_state(
                            jid,
                            TerminalStateUpdate {
                                status: JobStatus::Failed,
                                exit_code: -1,
                                end_scope: Some(scope_hash),
                                advance_chain: true,
                            },
                            state,
                            db,
                            sys,
                        )
                        .await;
                    }
                }
            }
            Ok(None) => {
                info!(%jid, pipeline = %leaf.pipeline_text, "scheduler: spawning single job");
                let _ = sys
                    .process_mgr
                    .send(ProcessMgrMsg::SpawnJob {
                        job_id: jid,
                        pipeline: leaf.pipeline.clone(),
                        scope_hash,
                        cwd_override: cwd_override.clone(),
                        wrapper_enabled,
                    })
                    .await;
            }
            Err(message) => {
                let _ = set_job_terminal_state(
                    jid,
                    TerminalStateUpdate {
                        status: JobStatus::Failed,
                        exit_code: -1,
                        end_scope: Some(scope_hash),
                        advance_chain: true,
                    },
                    state,
                    db,
                    sys,
                )
                .await;
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
        });
    }

    let chain_text = chain_to_text(&chain);
    let chain_id = state.alloc_chain();
    let ready_indices = initially_ready(&chain);
    let mut leaf_status: HashMap<usize, LeafStatus> = HashMap::new();

    for leaf in &leaves {
        leaf_status.insert(leaf.index, LeafStatus::Pending);
    }

    let chain_state = ChainState {
        node: chain,
        retry_max,
        retry_delay,
        retry_attempts: 0,
        leaf_jobs: HashMap::new(),
        leaf_status,
        scope_hash,
        pipeline_text: chain_text,
        cwd_override: cwd_override.clone(),
        wrapper_enabled,
    };
    state.chains.insert(chain_id, chain_state);

    let spawned_job_ids = process_chain_advance(
        chain_id,
        ready_indices
            .iter()
            .copied()
            .map(|idx| (idx, scope_hash))
            .collect(),
        &[],
        ready_indices.len(),
        cwd_override,
        state,
        db,
        sys,
    )
    .await;

    ResponsePayload::Ok(OkPayload::ChainCreated {
        chain_id: chain_id.to_string(),
        job_ids: spawned_job_ids.iter().map(|j| j.to_string()).collect(),
        chain: build_chain_info(state, chain_id).expect("chain info after creation"),
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
    if let Some((chain_id, ready_queue, to_cancel)) = set_job_terminal_state(
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
    .await
    {
        let cwd_override = state
            .chains
            .get(&chain_id)
            .and_then(|c| c.cwd_override.clone());
        process_chain_advance(
            chain_id,
            ready_queue,
            &to_cancel,
            0,
            cwd_override,
            state,
            db,
            sys,
        )
        .await;
    }
}

/// Shared logic for processing chain advancement results (cancels + spawns + cleanup).
///
/// Used by `handle_job_finished`, `:kill`, and `:cancel` handlers.
#[allow(clippy::too_many_arguments)]
async fn process_chain_advance(
    chain_id: ChainId,
    newly_ready: Vec<(usize, ScopeHash)>,
    to_cancel: &[usize],
    capture_first: usize,
    cwd_override: Option<std::path::PathBuf>,
    state: &mut SchedulerState,
    db: &Arc<Mutex<Connection>>,
    sys: &ActorSystem,
) -> Vec<JobId> {
    cancel_chain_leaves(chain_id, to_cancel, state, db, sys).await;

    let (leaves, wrapper_enabled) = {
        let Some(chain) = state.chains.get(&chain_id) else {
            return Vec::new();
        };
        (flatten_leaves(&chain.node), chain.wrapper_enabled)
    };

    let mut queue: VecDeque<(usize, ScopeHash)> = newly_ready.into();
    let mut captured = Vec::new();

    while let Some((idx, start_scope)) = queue.pop_front() {
        let jid = state.alloc_job();
        let open_hint = classify_job_open_hint(&leaves[idx].pipeline);
        if captured.len() < capture_first {
            captured.push(jid);
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
            sys,
            state,
            jid,
            &leaves[idx].pipeline_text,
            start_scope,
            open_hint,
        )
        .await;

        match scope_transform_from_command(leaves[idx].command()) {
            Ok(Some(_)) => {
                match apply_scope_transform(sys, start_scope, leaves[idx].command()).await {
                    Ok(end_scope) => {
                        if let Some((_, ready_queue, more_cancel)) = set_job_terminal_state(
                            jid,
                            TerminalStateUpdate {
                                status: JobStatus::Done,
                                exit_code: 0,
                                end_scope: Some(end_scope),
                                advance_chain: true,
                            },
                            state,
                            db,
                            sys,
                        )
                        .await
                        {
                            cancel_chain_leaves(chain_id, &more_cancel, state, db, sys).await;
                            queue.extend(ready_queue);
                        }
                    }
                    Err(error) => {
                        warn!(%jid, pipeline = %leaves[idx].pipeline_text, "scheduler: scope-transform failed: {error}");
                        if let Some((_, ready_queue, more_cancel)) = set_job_terminal_state(
                            jid,
                            TerminalStateUpdate {
                                status: JobStatus::Failed,
                                exit_code: -1,
                                end_scope: Some(start_scope),
                                advance_chain: true,
                            },
                            state,
                            db,
                            sys,
                        )
                        .await
                        {
                            cancel_chain_leaves(chain_id, &more_cancel, state, db, sys).await;
                            queue.extend(ready_queue);
                        }
                    }
                }
            }
            Ok(None) => {
                let _ = sys
                    .process_mgr
                    .send(ProcessMgrMsg::SpawnJob {
                        job_id: jid,
                        pipeline: leaves[idx].pipeline.clone(),
                        scope_hash: start_scope,
                        cwd_override: cwd_override.clone(),
                        wrapper_enabled,
                    })
                    .await;
            }
            Err(error) => {
                warn!(%jid, pipeline = %leaves[idx].pipeline_text, "scheduler: invalid scope-transform leaf: {error}");
                if let Some((_, ready_queue, more_cancel)) = set_job_terminal_state(
                    jid,
                    TerminalStateUpdate {
                        status: JobStatus::Failed,
                        exit_code: -1,
                        end_scope: Some(start_scope),
                        advance_chain: true,
                    },
                    state,
                    db,
                    sys,
                )
                .await
                {
                    cancel_chain_leaves(chain_id, &more_cancel, state, db, sys).await;
                    queue.extend(ready_queue);
                }
            }
        }
    }

    publish_chain_progress(sys, state, chain_id).await;

    if let Some(chain) = state.chains.get(&chain_id)
        && all_leaves_terminal(&chain.node, 0, &chain.leaf_status)
    {
        info!(%chain_id, "scheduler: chain complete");
        let finished = state.chains.remove(&chain_id).unwrap();
        for jid in finished.leaf_jobs.values() {
            state.job_to_chain.remove(jid);
        }
    }

    captured
}

// ── Command dispatch ────────────────────────────────────────────────────────

async fn handle_wait_command(
    id: String,
    client_id: u64,
    request_id: u32,
    state: &mut SchedulerState,
    sys: &ActorSystem,
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

    let _ = sys;
    Some(ResponsePayload::err(
        error_code::NOT_FOUND,
        format!("{id} not found"),
    ))
}

async fn handle_command(
    cmd: ResolvedCommand,
    client_id: u64,
    state: &mut SchedulerState,
    db: &Arc<Mutex<Connection>>,
    config: &Config,
    sys: &ActorSystem,
) -> ResponsePayload {
    match cmd {
        ResolvedCommand::Script { mode, items } => {
            let script_id = state.alloc_script();
            let total_items = items.len();
            let mut created_items = Vec::with_capacity(total_items);
            let mut submit_error = None;

            for (index, item) in items.into_iter().enumerate() {
                let source = item.source;
                let response = Box::pin(handle_command(
                    *item.command,
                    client_id,
                    state,
                    db,
                    config,
                    sys,
                ))
                .await;
                match response {
                    ResponsePayload::Ok(payload) => {
                        created_items.push(ScriptItemInfo {
                            index,
                            source,
                            result: script_item_result_from_ok(&payload),
                        });
                    }
                    ResponsePayload::Err { code, message } => {
                        submit_error = Some(ScriptSubmitError {
                            index,
                            source,
                            code,
                            message,
                        });
                        break;
                    }
                }
            }

            persist_script_submission(script_id, mode, &created_items, submit_error.as_ref(), db);
            prune_retained_script_runs(db, config).await;

            ResponsePayload::Ok(OkPayload::ScriptCreated {
                script_id: script_id.to_string(),
                items: created_items,
                submit_error,
            })
        }
        ResolvedCommand::Run { chain, params } => {
            let scope_hash = match get_head_scope(sys).await {
                Ok(h) => h,
                Err(resp) => return resp,
            };
            // Check block rules before executing.
            if let Some(decision) = check_chain_blocked(&chain, config) {
                return match decision {
                    BlockDecision::Block(reason) => {
                        ResponsePayload::err(error_code::BLOCKED, reason)
                    }
                    BlockDecision::Warn(hint) => ResponsePayload::err(error_code::WARNED, hint),
                };
            }
            let cwd_override = params.cwd();
            let wrapper_enabled = state.wrapper_enabled(config);
            let retry_max = params.retry().unwrap_or(0);
            let retry_delay = params.retry_delay();
            spawn_chain(
                chain,
                scope_hash,
                0,
                0,
                cwd_override,
                wrapper_enabled,
                retry_max,
                retry_delay,
                state,
                db,
                sys,
            )
            .await
        }

        ResolvedCommand::Cron {
            schedule,
            chain,
            params,
        } => {
            let display_text = schedule.display();
            let scope_hash = match get_head_scope(sys).await {
                Ok(h) => h,
                Err(resp) => return resp,
            };

            let cron_id = state.alloc_cron();
            let Some(next_trigger) = next_trigger_instant(&schedule, 0) else {
                return ResponsePayload::err(
                    error_code::INVALID_SYNTAX,
                    format!("cannot compute next trigger for schedule: {display_text}"),
                );
            };
            info!(%cron_id, %display_text, "scheduler: cron added");

            let entry = CronEntry {
                cron_id,
                schedule_text: display_text,
                schedule,
                chain,
                scope_hash,
                status: CronStatus::Scheduled,
                next_trigger,
                cwd_override: params.cwd(),
            };
            persist_cron_entry(db, &entry);
            state.crons.insert(cron_id, entry);

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
                        let _ = sys
                            .process_mgr
                            .send(ProcessMgrMsg::KillJob { job_id: jid })
                            .await;
                        info!(%jid, "scheduler: job killed");
                        if let Some((chain_id, ready_queue, to_cancel)) = set_job_terminal_state(
                            jid,
                            TerminalStateUpdate {
                                status: JobStatus::Killed,
                                exit_code: -1,
                                end_scope: None,
                                advance_chain: true,
                            },
                            state,
                            db,
                            sys,
                        )
                        .await
                        {
                            let cwd_override = state
                                .chains
                                .get(&chain_id)
                                .and_then(|c| c.cwd_override.clone());
                            process_chain_advance(
                                chain_id,
                                ready_queue,
                                &to_cancel,
                                0,
                                cwd_override,
                                state,
                                db,
                                sys,
                            )
                            .await;
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
                if state.crons.remove(&cid).is_some() {
                    remove_cron_from_db(db, cid);
                    info!(%cid, "scheduler: cron removed");
                    ResponsePayload::ack()
                } else {
                    ResponsePayload::err(error_code::NOT_FOUND, format!("cron {id} not found"))
                }
            } else {
                warn!(%id, "scheduler: kill target not found");
                ResponsePayload::err(error_code::NOT_FOUND, format!("{id} not found"))
            }
        }

        ResolvedCommand::Cancel { id } => {
            if let Some(jid) = parse_job_id(&id) {
                let status = state.jobs.get(&jid).map(|entry| entry.status.clone());
                match status {
                    Some(JobStatus::Pending) | Some(JobStatus::Running) => {
                        if matches!(status, Some(JobStatus::Running)) {
                            let _ = sys
                                .process_mgr
                                .send(ProcessMgrMsg::KillJob { job_id: jid })
                                .await;
                        }
                        info!(%jid, "scheduler: job cancelled");
                        if let Some((chain_id, ready_queue, to_cancel)) = set_job_terminal_state(
                            jid,
                            TerminalStateUpdate {
                                status: JobStatus::Cancelled(CancelReason::User),
                                exit_code: -1,
                                end_scope: None,
                                advance_chain: true,
                            },
                            state,
                            db,
                            sys,
                        )
                        .await
                        {
                            let cwd_override = state
                                .chains
                                .get(&chain_id)
                                .and_then(|c| c.cwd_override.clone());
                            process_chain_advance(
                                chain_id,
                                ready_queue,
                                &to_cancel,
                                0,
                                cwd_override,
                                state,
                                db,
                                sys,
                            )
                            .await;
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
                if let Some(entry) = state.crons.get_mut(&cid) {
                    if entry.status.is_terminal() {
                        return ResponsePayload::err(
                            error_code::INVALID_STATE,
                            format!("cron {cid} is already terminal"),
                        );
                    }
                    entry.status = CronStatus::Paused;
                    persist_cron_entry(db, entry);
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
                if let Some(entry) = state.crons.get_mut(&cid) {
                    if entry.status.is_terminal() {
                        return ResponsePayload::err(
                            error_code::INVALID_STATE,
                            format!("cron {cid} is already terminal"),
                        );
                    }
                    entry.status = CronStatus::Scheduled;
                    if let Some(next_trigger) = next_trigger_instant(&entry.schedule, 0) {
                        entry.next_trigger = next_trigger;
                    }
                    persist_cron_entry(db, entry);
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
            let mut list: Vec<JobInfo> = state.jobs.values().map(job_info_from_entry).collect();
            list.sort_by_key(|job| parse_job_id(&job.id).map(|id| id.0).unwrap_or(u32::MAX));
            ResponsePayload::Ok(OkPayload::JobList(list))
        }

        ResolvedCommand::Crons => {
            let mut list: Vec<CronInfo> = state
                .crons
                .values()
                .map(|c| CronInfo {
                    id: c.cron_id.to_string(),
                    schedule: c.schedule_text.clone(),
                    command: chain_to_text(&c.chain),
                    status: c.status,
                })
                .collect();
            list.sort_by_key(|cron| parse_cron_id(&cron.id).map(|id| id.0).unwrap_or(u32::MAX));
            ResponsePayload::Ok(OkPayload::CronList(list))
        }

        ResolvedCommand::Scopes => handle_list_scopes(sys).await,

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
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    let _ = sys
                        .scope_store
                        .send(ScopeStoreMsg::Fork { delta, reply: tx })
                        .await;
                    match rx.await {
                        Ok(Ok(hash)) => match get_scope_snapshot_by_hash(sys, hash).await {
                            Ok(updated) => ResponsePayload::Ok(OkPayload::ScopeCreated {
                                hash: hash.to_string(),
                                label: Some("env set".into()),
                                summary: format_scope_change_summary(hash, &snapshot, &updated),
                            }),
                            Err(message) => ResponsePayload::err(error_code::INTERNAL, message),
                        },
                        Ok(Err(e)) => ResponsePayload::err(error_code::INTERNAL, e.to_string()),
                        Err(_) => {
                            ResponsePayload::err(error_code::INTERNAL, "scope_store unreachable")
                        }
                    }
                }
                Err(message) => ResponsePayload::err(error_code::INVALID_SYNTAX, message),
            }
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
                    let mut lines = vec![
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
                            "  interactive bypass: {}",
                            if config.wrapper.denylist.interactive {
                                "enabled"
                            } else {
                                "disabled"
                            }
                        ),
                    ];
                    if !config.wrapper.denylist.commands.is_empty() {
                        lines.push(format!(
                            "  denylist: {}",
                            config.wrapper.denylist.commands.join(", ")
                        ));
                    }
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
            let (tx, rx) = tokio::sync::oneshot::channel();
            let _ = sys
                .scope_store
                .send(ScopeStoreMsg::Fork { delta, reply: tx })
                .await;
            match rx.await {
                Ok(Ok(hash)) => match get_scope_snapshot_by_hash(sys, hash).await {
                    Ok(updated) => ResponsePayload::Ok(OkPayload::ScopeCreated {
                        hash: hash.to_string(),
                        label: Some(format!("cd {}", resolved.display())),
                        summary: format_scope_change_summary(hash, &snapshot, &updated),
                    }),
                    Err(message) => ResponsePayload::err(error_code::INTERNAL, message),
                },
                Ok(Err(e)) => ResponsePayload::err(error_code::INTERNAL, e.to_string()),
                Err(_) => ResponsePayload::err(error_code::INTERNAL, "scope_store unreachable"),
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
            // Apply retry backoff: if the job has a chain with retry config, use it;
            // otherwise apply a short default delay.
            let delay = state
                .chains
                .values()
                .find(|c| c.leaf_jobs.values().any(|jid| *jid == job_id))
                .and_then(|c| c.retry_delay)
                .unwrap_or(std::time::Duration::from_millis(500));
            info!(%job_id, ?delay, "scheduler: retrying job with delay");
            tokio::time::sleep(delay).await;
            let wrapper_enabled = state.wrapper_enabled(config);
            spawn_chain(
                chain,
                start_scope,
                0,
                0,
                None,
                wrapper_enabled,
                0,
                None,
                state,
                db,
                sys,
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
        OkPayload::ScopeCreated { hash, summary, .. } => ScriptItemResult::Message {
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

fn persist_script_submission(
    script_id: ScriptId,
    mode: Mode,
    items: &[ScriptItemInfo],
    submit_error: Option<&ScriptSubmitError>,
    db: &storage::SharedConnection,
) {
    let db = Arc::clone(db);
    let run = storage::StoredScriptRun {
        id: script_id.to_string(),
        mode: match mode {
            Mode::Job => "job".into(),
            Mode::Cron => "cron".into(),
        },
        input: items
            .iter()
            .map(|item| item.source.as_str())
            .collect::<Vec<_>>()
            .join("\n"),
        status: if submit_error.is_some() {
            "partial_error".into()
        } else {
            "submitted".into()
        },
        item_count: items.len(),
        error_code: submit_error.map(|error| error.code.clone()),
        error_message: submit_error.map(|error| error.message.clone()),
    };
    let stored_items = items
        .iter()
        .map(|item| storage::StoredScriptItem {
            script_id: script_id.to_string(),
            item_index: item.index,
            source_text: item.source.clone(),
            kind: match &item.result {
                ScriptItemResult::Job { .. } => "job".into(),
                ScriptItemResult::Chain { .. } => "chain".into(),
                ScriptItemResult::Cron { .. } => "cron".into(),
                ScriptItemResult::Message { .. } => "message".into(),
            },
            target_id: match &item.result {
                ScriptItemResult::Job { job_id, .. } => Some(job_id.clone()),
                ScriptItemResult::Chain { chain_id, .. } => Some(chain_id.clone()),
                ScriptItemResult::Cron { cron_id } => Some(cron_id.clone()),
                ScriptItemResult::Message { .. } => None,
            },
            chain_id: match &item.result {
                ScriptItemResult::Chain { chain_id, .. } => Some(chain_id.clone()),
                _ => None,
            },
            job_ids: match &item.result {
                ScriptItemResult::Job { job_id, .. } => vec![job_id.clone()],
                ScriptItemResult::Chain { job_ids, .. } => job_ids.clone(),
                _ => Vec::new(),
            },
        })
        .collect::<Vec<_>>();
    tokio::spawn(async move {
        if let Err(error) = storage::with_connection(&db, move |conn| {
            storage::upsert_script_run(conn, &run, &stored_items)
        })
        .await
        {
            warn!(script = %script_id, "scheduler: failed to persist script submission: {error}");
        }
    });
}

/// Get the HEAD scope hash from the scope store.
async fn get_head_scope(sys: &ActorSystem) -> Result<ScopeHash, ResponsePayload> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let _ = sys
        .scope_store
        .send(ScopeStoreMsg::GetHead { reply: tx })
        .await;
    rx.await
        .map_err(|_| ResponsePayload::err(error_code::INTERNAL, "scope_store unreachable"))
}

/// Parse a string like `"J5"` into a `JobId`.
fn parse_job_id(s: &str) -> Option<JobId> {
    let s = s.trim();
    s.strip_prefix('J')
        .and_then(|n| n.parse::<u32>().ok())
        .map(JobId)
}

/// Parse a string like `"C3"` into a `CronId`.
fn parse_cron_id(s: &str) -> Option<CronId> {
    let s = s.trim();
    s.strip_prefix('C')
        .and_then(|n| n.parse::<u32>().ok())
        .map(CronId)
}

async fn remove_job_logs(job_id: JobId) {
    let _ = tokio::task::spawn_blocking(move || {
        for suffix in [".log", ".stderr"] {
            let path = crate::dirs::output_dir().join(format!("{job_id}{suffix}"));
            if let Err(error) = std::fs::remove_file(&path)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                warn!(%job_id, path = %path.display(), "scheduler: failed to remove output log: {error}");
            }
        }
    })
    .await;
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
            matches!(
                spec.category,
                CommandCategory::Job | CommandCategory::Scope | CommandCategory::System
            )
        })
}

fn is_cron_help_topic(topic: &str) -> bool {
    topic == "cron"
        || command_spec(topic).is_some_and(|spec| spec.category == CommandCategory::Cron)
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
            "- `cargo test || cargo clippy`\n",
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
        .filter(|spec| categories.contains(&spec.category))
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
        Ok((head, mut scopes)) => {
            let head_str = head.to_string();
            scopes.sort_by(|a, b| {
                let a_head = a.hash == head_str;
                let b_head = b.hash == head_str;
                b_head.cmp(&a_head).then(a.hash.cmp(&b.hash))
            });
            ResponsePayload::Ok(OkPayload::ScopeList(scopes))
        }
        Err(_) => ResponsePayload::err(error_code::INTERNAL, "scope_store reply dropped"),
    }
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
                        entry.cron_id, entry.schedule_text, entry.status
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
                entry.cron_id, entry.schedule_text, entry.status
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
        Ok(Some(data)) => {
            let truncated = data.len() >= tail_bytes;
            let text = String::from_utf8_lossy(&data).into_owned();
            ResponsePayload::Ok(OkPayload::Output {
                id,
                data: text,
                truncated,
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
    match tokio::task::spawn_blocking(move || {
        let path = crate::dirs::output_dir().join(format!("{job_id}.log"));
        std::fs::read(path)
    })
    .await
    {
        Ok(Ok(data)) => {
            let truncated = data.len() > tail_bytes;
            let trimmed = if truncated {
                &data[data.len() - tail_bytes..]
            } else {
                &data
            };
            let text = String::from_utf8_lossy(trimmed).into_owned();
            ResponsePayload::Ok(OkPayload::Output {
                id,
                data: text,
                truncated,
            })
        }
        Ok(Err(_)) => {
            ResponsePayload::err(error_code::NOT_FOUND, format!("no output found for {id}"))
        }
        Err(_) => ResponsePayload::err(error_code::INTERNAL, "blocking task panicked"),
    }
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
        })) => {
            let truncated = data.len() >= tail_bytes;
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

    // Try the dedicated stderr log (pipe-mode jobs).
    let stderr_data = tokio::task::spawn_blocking(move || {
        let path = crate::dirs::output_dir().join(format!("{job_id}.stderr"));
        std::fs::read(path)
    })
    .await;
    if let Ok(Ok(data)) = stderr_data {
        let truncated = data.len() > tail_bytes;
        let trimmed = if truncated {
            &data[data.len() - tail_bytes..]
        } else {
            &data
        };
        return ResponsePayload::Ok(OkPayload::Output {
            id,
            data: String::from_utf8_lossy(trimmed).into_owned(),
            truncated,
        });
    }

    // No dedicated stderr — return combined PTY log with notice.
    let id2 = id.clone();
    match tokio::task::spawn_blocking(move || {
        let path = crate::dirs::output_dir().join(format!("{job_id}.log"));
        std::fs::read(path)
    })
    .await
    {
        Ok(Ok(data)) => {
            let truncated = data.len() > tail_bytes;
            let trimmed = if truncated {
                &data[data.len() - tail_bytes..]
            } else {
                &data
            };
            let body = String::from_utf8_lossy(trimmed).into_owned();
            ResponsePayload::Ok(OkPayload::Output {
                id: id2,
                data: format!("[PTY: stdout and stderr are merged]\n{body}"),
                truncated,
            })
        }
        Ok(Err(_)) => {
            ResponsePayload::err(error_code::NOT_FOUND, format!("no output found for {id}"))
        }
        Err(_) => ResponsePayload::err(error_code::INTERNAL, "blocking task panicked"),
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use cue_core::pipeline::{PipeSegment, Pipeline};
    use std::path::Path;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    /// Helper: build a simple leaf from a command string.
    fn leaf(cmd: &str) -> ChainNode {
        ChainNode::Leaf(Pipeline {
            segments: vec![PipeSegment {
                command: cmd.split_whitespace().map(String::from).collect(),
                pipe_to_next: None,
            }],
        })
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

    /// Spawn a fake scope_store that always replies with a zero hash.
    fn spawn_fake_scope_store(mut rx: mpsc::Receiver<ScopeStoreMsg>) {
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                match msg {
                    ScopeStoreMsg::GetHead { reply } => {
                        let _ = reply.send(ScopeHash([0u8; 32]));
                    }
                    ScopeStoreMsg::GetHeadSnapshot { reply } => {
                        let _ = reply.send(Some(cue_core::scope::EnvSnapshot {
                            env: std::collections::BTreeMap::new(),
                            cwd: std::env::current_dir().expect("current dir"),
                        }));
                    }
                    ScopeStoreMsg::GetScope { hash, reply } => {
                        let _ = reply.send(Some(cue_core::scope::Scope {
                            hash,
                            parent: None,
                            delta: None,
                            snapshot: Some(cue_core::scope::EnvSnapshot {
                                env: std::collections::BTreeMap::new(),
                                cwd: std::env::current_dir().expect("current dir"),
                            }),
                        }));
                    }
                    ScopeStoreMsg::Shutdown => break,
                    _ => {}
                }
            }
        });
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

    async fn recv_gateway_msg(rx: &mut mpsc::Receiver<GatewayMsg>) -> GatewayMsg {
        tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .expect("gateway message timeout")
            .expect("gateway channel closed")
    }

    #[test]
    fn help_renderer_supports_mode_topics() {
        let job = render_help_text(Some("job"));
        assert!(job.contains("JOB mode"));
        assert!(job.contains(":tail J<n>"));

        let cron = render_help_text(Some("cron"));
        assert!(cron.contains("CRON mode"));
        assert!(cron.contains("every 5m cargo test"));
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
            chain,
            ScopeHash([0; 32]),
            1,
            1,
            None,
            false,
            0,
            None,
            &mut state,
            &conn,
            &sys,
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
            chain,
            ScopeHash([0; 32]),
            1,
            1,
            None,
            false,
            0,
            None,
            &mut state,
            &conn,
            &sys,
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
            chain,
            ScopeHash([0; 32]),
            1,
            1,
            None,
            false,
            0,
            None,
            &mut state,
            &conn,
            &sys,
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
            chain,
            ScopeHash([0; 32]),
            1,
            1,
            None,
            false,
            0,
            None,
            &mut state,
            &conn,
            &sys,
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
            chain,
            ScopeHash([0; 32]),
            1,
            1,
            None,
            false,
            0,
            None,
            &mut state,
            &conn,
            &sys,
        )
        .await;
        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(spawned.len(), 2);
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
            chain,
            ScopeHash([0; 32]),
            1,
            1,
            None,
            false,
            0,
            None,
            &mut state,
            &conn,
            &sys,
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
        rt.block_on(restore_jobs(&conn, &mut state));

        assert_eq!(state.next_job, 8);
        assert_eq!(state.jobs[&JobId(7)].pipeline_text, "cargo test");
        assert_eq!(state.jobs[&JobId(7)].status, JobStatus::Done);
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
                age_secs: 0,
            },
        )
        .unwrap();
        drop(guard);

        let mut state = SchedulerState::new();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(restore_crons(&conn, &mut state));

        assert_eq!(state.next_cron, 5);
        assert!(state.crons.contains_key(&CronId(4)));
        // schedule_text field replaced by schedule: CronSchedule
        assert_eq!(state.crons[&CronId(4)].status, CronStatus::Scheduled);
        assert_eq!(
            state.crons[&CronId(4)].cwd_override.as_deref(),
            Some(std::path::Path::new("/tmp/cue-cron-cwd"))
        );
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
                age_secs: 0,
            },
        )
        .unwrap();
        guard
            .execute(
                "UPDATE crons SET created_at = datetime('now', '-5 seconds') WHERE id = 'C1'",
                [],
            )
            .unwrap();
        drop(guard);

        let mut state = SchedulerState::new();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(restore_crons(&conn, &mut state));

        assert_eq!(state.crons.len(), 1);
        assert_eq!(state.crons[&CronId(1)].status, CronStatus::Expired);
        let guard = conn.lock().unwrap();
        let crons = storage::load_crons(&guard).unwrap();
        assert_eq!(crons.len(), 1);
        assert_eq!(crons[0].status, CronStatus::Expired);
    }

    #[tokio::test]
    async fn single_leaf_no_chain_tracking() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let mut state = SchedulerState::new();
        let chain = leaf("echo hello");

        let resp = spawn_chain(
            chain,
            ScopeHash([0; 32]),
            1,
            1,
            None,
            false,
            0,
            None,
            &mut state,
            &conn,
            &sys,
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
            chain,
            ScopeHash([0; 32]),
            1,
            1,
            None,
            false,
            0,
            None,
            &mut state,
            &conn,
            &sys,
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
            leaf("echo hello"),
            ScopeHash([0; 32]),
            1,
            1,
            None,
            false,
            0,
            None,
            &mut state,
            &conn,
            &sys,
        )
        .await;
        let job_id = match resp {
            ResponsePayload::Ok(OkPayload::JobCreated { job_id, .. }) => job_id,
            other => panic!("expected JobCreated, got {other:?}"),
        };
        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        let jid = spawned[0];

        assert!(
            handle_wait_command(job_id.clone(), 7, 42, &mut state, &sys)
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
            leaf("echo hello"),
            ScopeHash([0; 32]),
            1,
            1,
            None,
            false,
            0,
            None,
            &mut state,
            &conn,
            &sys,
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
    fn parse_schedule_every() {
        assert_eq!(
            parse_schedule("every 5m"),
            Some(CronSchedule::Interval(std::time::Duration::from_secs(300)))
        );
    }

    #[test]
    fn parse_schedule_in() {
        assert_eq!(
            parse_schedule("in 30s"),
            Some(CronSchedule::Delay(std::time::Duration::from_secs(30)))
        );
    }

    #[test]
    fn parse_schedule_hours() {
        assert_eq!(
            parse_schedule("every 2h"),
            Some(CronSchedule::Interval(std::time::Duration::from_secs(7200)))
        );
    }

    #[test]
    fn parse_schedule_at_on_weekdays() {
        assert_eq!(
            parse_schedule("at 9am on weekdays"),
            Some(CronSchedule::TimeOfDay {
                time_secs: 9 * 3600,
                days: Some(DayFilter {
                    days: vec![
                        Weekday::Mon,
                        Weekday::Tue,
                        Weekday::Wed,
                        Weekday::Thu,
                        Weekday::Fri,
                    ],
                }),
            })
        );
    }

    #[test]
    fn parse_schedule_crontab() {
        assert_eq!(
            parse_schedule("cron */5 * * * *"),
            Some(CronSchedule::Crontab("*/5 * * * *".into()))
        );
    }

    #[test]
    fn parse_schedule_invalid() {
        assert!(parse_schedule("every").is_none());
        assert!(parse_schedule("at").is_none());
        assert!(parse_schedule("cron * * * *").is_none());
        assert!(parse_schedule("every 30m 9am-5pm weekdays").is_none());
    }

    #[test]
    fn parse_job_id_valid() {
        assert_eq!(parse_job_id("J1"), Some(JobId(1)));
        assert_eq!(parse_job_id("J42"), Some(JobId(42)));
    }

    #[test]
    fn parse_job_id_invalid() {
        assert_eq!(parse_job_id("C1"), None);
        assert_eq!(parse_job_id("foo"), None);
    }

    #[test]
    fn parse_cron_id_valid() {
        assert_eq!(parse_cron_id("C1"), Some(CronId(1)));
        assert_eq!(parse_cron_id("C99"), Some(CronId(99)));
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

    // ── FIX 1 test: Race + Serial — cancelled leaf must not be re-spawned ──

    /// `(a -> b) ||? c` — when `c` succeeds, Race should cancel both `a`/`b`.
    /// When `a` also succeeds, `b` should NOT be spawned because it was cancelled.
    #[tokio::test]
    async fn race_serial_cancelled_leaf_not_respawned() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let mut state = SchedulerState::new();
        // (a -> b) ||? c
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
            chain,
            ScopeHash([0; 32]),
            1,
            1,
            None,
            false,
            0,
            None,
            &mut state,
            &conn,
            &sys,
        )
        .await;
        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        // Initially ready: a (idx 0) and c (idx 2).
        assert_eq!(spawned.len(), 2);
        let a_jid = spawned[0]; // leaf 0 = a
        let c_jid = spawned[1]; // leaf 2 = c

        // c succeeds first → Race fires, cancels a (running) and b (pending).
        handle_job_finished(c_jid, 0, &mut state, &conn, &sys).await;

        // a was killed via cancel; drain the KillJob.
        let mut kill_ids = Vec::new();
        tokio::task::yield_now().await;
        while let Ok(msg) = pm_rx.try_recv() {
            if let ProcessMgrMsg::KillJob { job_id } = msg {
                kill_ids.push(job_id);
            }
        }
        assert!(kill_ids.contains(&a_jid), "a should have been killed");

        // Now a finishes (process exits after kill signal).
        handle_job_finished(a_jid, 0, &mut state, &conn, &sys).await;

        // b should NOT be spawned — it was already cancelled by Race.
        let after = drain_spawn_jobs(&mut pm_rx).await;
        assert!(after.is_empty(), "b must not be spawned after cancellation");

        // Chain should be complete.
        assert!(state.chains.is_empty(), "chain should be cleaned up");
    }

    // ── FIX 3 test: Race waits for entire branch, not single leaf ──

    /// `(compile -> test) ||? lint`
    /// When `compile` succeeds but `test` hasn't run yet, Race should NOT fire.
    #[tokio::test]
    async fn race_does_not_fire_on_partial_branch_success() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let mut state = SchedulerState::new();
        // (compile -> test) ||? lint
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
            chain,
            ScopeHash([0; 32]),
            1,
            1,
            None,
            false,
            0,
            None,
            &mut state,
            &conn,
            &sys,
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

    // ── FIX 2 test: :cancel updates chain leaf_status and advances chain ──

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
            chain,
            ScopeHash([0; 32]),
            1,
            1,
            None,
            false,
            0,
            None,
            &mut state,
            &conn,
            &sys,
        )
        .await;
        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        assert_eq!(spawned.len(), 1);
        let a_jid = spawned[0];

        // Cancel a via :cancel.
        let resp = handle_command(
            ResolvedCommand::Cancel {
                id: format!("J{}", a_jid.0),
            },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
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

    // ── FIX 2 test: :kill does not get overwritten by later JobFinished ──

    #[tokio::test]
    async fn kill_status_not_overwritten_by_job_finished() {
        let (sys, _gw_rx, _sched_rx, mut pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = Config::default();
        let mut state = SchedulerState::new();
        let chain = leaf("long-running");

        let _ = spawn_chain(
            chain,
            ScopeHash([0; 32]),
            1,
            1,
            None,
            false,
            0,
            None,
            &mut state,
            &conn,
            &sys,
        )
        .await;
        let spawned = drain_spawn_jobs(&mut pm_rx).await;
        let jid = spawned[0];

        // Kill the job.
        let resp = handle_command(
            ResolvedCommand::Kill {
                id: format!("J{}", jid.0),
            },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        assert!(matches!(resp, ResponsePayload::Ok(OkPayload::Ack {})));
        assert_eq!(state.jobs[&jid].status, JobStatus::Killed);

        // Now the process exits (JobFinished arrives).
        handle_job_finished(jid, -9, &mut state, &conn, &sys).await;

        // Status should still be Killed, not overwritten to Failed.
        assert_eq!(
            state.jobs[&jid].status,
            JobStatus::Killed,
            "Killed status must not be overwritten by JobFinished"
        );
    }
}
