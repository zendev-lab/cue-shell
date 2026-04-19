//! Scheduler actor — command routing, ID assignment, chain execution, cron timer heap.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use chrono::{
    DateTime, Datelike, Duration as ChronoDuration, Local, LocalResult, NaiveTime, TimeZone,
    Timelike,
};
use rusqlite::Connection;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use cue_core::agent::{AgentRole, AgentStatus};
use cue_core::command::{ModeParams, ParamValue};
use cue_core::cron::{CronPreset, CronSchedule, CronStatus, DayFilter, Weekday};
use cue_core::ipc::{
    AgentInfo, ChainInfo, ChainJobInfo, CronInfo, EventPayload, JobInfo, JobOpenHint, OkPayload,
    ResponsePayload, error_code,
};
use cue_core::job::{CancelReason, JobStatus};
use cue_core::pipeline::{ChainNode, ParallelOp, SerialOp};
use cue_core::scope::EnvSnapshot;
use cue_core::{AgentId, ChainId, CronId, JobId, ScopeHash};

use crate::config::{AgentBackendConfig, Config};
use crate::parser::parse::Parser as CueParser;
use crate::parser::resolver::{ResolvedCommand, Resolver};
use crate::parser::token::Token;
use crate::parser::tokenizer::Tokenizer;
use crate::runtime_env::effective_snapshot;
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
    #[allow(dead_code)]
    chain_id: ChainId,
    #[allow(dead_code)]
    client_id: u64,
    #[allow(dead_code)]
    request_id: u32,
    node: ChainNode,
    /// Maps each leaf index (0-based, left-to-right DFS) to its `JobId`.
    leaf_jobs: HashMap<usize, JobId>,
    /// Maps each leaf index to its current status.
    leaf_status: HashMap<usize, LeafStatus>,
    scope_hash: ScopeHash,
    pipeline_text: String,
    /// Explicit working directory override for all jobs in this chain.
    cwd_override: Option<std::path::PathBuf>,
}

/// Flattened representation of a chain leaf for easy lookup.
struct FlatLeaf {
    /// Index in the DFS-order leaf list.
    index: usize,
    /// Command words for the first segment (used to spawn the job).
    command: Vec<String>,
    /// Human-readable pipeline text.
    pipeline_text: String,
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
    #[allow(dead_code)]
    chain_id: Option<ChainId>,
    chain_index: Option<usize>,
    chain_total: Option<usize>,
    /// Captured stderr text (empty for PTY-mode jobs where streams are merged).
    #[allow(dead_code)]
    stderr: String,
}

struct AgentEntry {
    agent_id: AgentId,
    backend: String,
    role: AgentRole,
    status: AgentStatus,
    control: Option<mpsc::Sender<AgentControl>>,
    session_id: Option<String>,
    model: Option<String>,
    scope_hash: Option<ScopeHash>,
    transcript: String,
    last_role: Option<String>,
}

enum AgentControl {
    Prompt(String),
    Abort,
    Shutdown,
}

#[derive(Debug, Clone)]
enum AgentLaunch {
    Prompt {
        initial_prompt: String,
        requested_session: Option<String>,
    },
    Restore {
        session_id: String,
    },
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
    next_agent: u32,
    next_cron: u32,
    next_chain: u32,

    /// Active chains keyed by `ChainId`.
    chains: HashMap<ChainId, ChainState>,
    /// Reverse lookup: `JobId` → `(ChainId, leaf_index)`.
    job_to_chain: HashMap<JobId, (ChainId, usize)>,
    /// All jobs the scheduler knows about.
    jobs: HashMap<JobId, JobEntry>,
    /// All agents the scheduler knows about.
    agents: HashMap<AgentId, AgentEntry>,
    /// Registered cron entries.
    crons: HashMap<CronId, CronEntry>,
    /// Deferred `:wait` responses keyed by job ID.
    job_waiters: HashMap<JobId, Vec<PendingWait>>,
    /// Deferred `:wait` responses keyed by agent ID.
    agent_waiters: HashMap<AgentId, Vec<PendingWait>>,
}

impl SchedulerState {
    fn new() -> Self {
        Self {
            next_job: 1,
            next_agent: 1,
            next_cron: 1,
            next_chain: 1,
            chains: HashMap::new(),
            job_to_chain: HashMap::new(),
            jobs: HashMap::new(),
            agents: HashMap::new(),
            crons: HashMap::new(),
            job_waiters: HashMap::new(),
            agent_waiters: HashMap::new(),
        }
    }

    fn alloc_job(&mut self) -> JobId {
        let id = JobId(self.next_job);
        self.next_job += 1;
        id
    }

    fn alloc_agent(&mut self) -> AgentId {
        let id = AgentId(self.next_agent);
        self.next_agent += 1;
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
}

// ── Spawn the actor ─────────────────────────────────────────────────────────

/// Spawn the Scheduler actor task.
pub fn spawn(mut rx: mpsc::Receiver<SchedulerMsg>, conn: Connection, sys: ActorSystem) {
    tokio::spawn(async move {
        let db = Arc::new(Mutex::new(conn));
        let config = match Config::load() {
            Ok(config) => config,
            Err(e) => {
                warn!("scheduler: failed to load config, using defaults: {e}");
                Config::default()
            }
        };
        let mut state = SchedulerState::new();
        restore_jobs(&db, &mut state);
        restore_crons(&db, &mut state);
        restore_agents(&db, &mut state, &config, &sys).await;
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

                        SchedulerMsg::AgentMessage {
                            agent_id,
                            role,
                            content,
                        } => {
                            if let Some(entry) = state.agents.get_mut(&agent_id) {
                                append_agent_transcript(entry, &role, &content);
                                persist_agent_entry(&db, entry);
                            }
                            let _ = sys
                                .gateway
                                .send(GatewayMsg::PushEvent {
                                    payload: EventPayload::AgentMessage {
                                        agent_id: agent_id.to_string(),
                                        role,
                                        content,
                                    },
                                    channel: "agents".into(),
                                })
                                .await;
                        }

                        SchedulerMsg::AgentStateChanged { agent_id, status } => {
                            if let Some(entry) = state.agents.get_mut(&agent_id) {
                                let old_state = entry.status.clone();
                                entry.status = status.clone();
                                if status.is_terminal() {
                                    entry.control = None;
                                }
                                persist_agent_entry(&db, entry);
                                let _ = sys
                                    .gateway
                                    .send(GatewayMsg::PushEvent {
                                        payload: EventPayload::AgentStateChanged {
                                            agent_id: agent_id.to_string(),
                                            old_state,
                                            new_state: status.clone(),
                                        },
                                        channel: "agents".into(),
                                    })
                                    .await;
                                if status.clone().is_terminal() {
                                    notify_agent_waiters(&mut state, &sys, agent_id).await;
                                }
                            }
                        }

                        SchedulerMsg::AgentSessionBound {
                            agent_id,
                            session_id,
                        } => {
                            if let Some(entry) = state.agents.get_mut(&agent_id) {
                                entry.session_id = Some(session_id);
                                persist_agent_entry(&db, entry);
                            }
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

fn restore_jobs(db: &Arc<Mutex<Connection>>, state: &mut SchedulerState) {
    let conn = match db.lock() {
        Ok(conn) => conn,
        Err(e) => {
            warn!("scheduler: failed to lock job history db: {e}");
            return;
        }
    };

    let restored = match storage::load_job_history(&conn) {
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
                stderr: job.stderr,
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

fn restore_crons(db: &Arc<Mutex<Connection>>, state: &mut SchedulerState) {
    let conn = match db.lock() {
        Ok(conn) => conn,
        Err(e) => {
            warn!("scheduler: failed to lock cron db: {e}");
            return;
        }
    };

    let restored = match storage::load_crons(&conn) {
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
            if let Err(e) = storage::upsert_cron(
                &conn,
                &storage::StoredCron {
                    id: cron.id.clone(),
                    schedule: cron.schedule.clone(),
                    command: cron.command.clone(),
                    status,
                    scope_hash: cron.scope_hash,
                    age_secs: cron.age_secs,
                },
            ) {
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
                cwd_override: None,
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

async fn restore_agents(
    db: &Arc<Mutex<Connection>>,
    state: &mut SchedulerState,
    config: &Config,
    sys: &ActorSystem,
) {
    let restored = {
        let conn = match db.lock() {
            Ok(conn) => conn,
            Err(e) => {
                warn!("scheduler: failed to lock agent history db: {e}");
                return;
            }
        };
        match storage::load_agent_history(&conn) {
            Ok(agents) => agents,
            Err(e) => {
                warn!("scheduler: failed to load agent history: {e}");
                return;
            }
        }
    };

    let mut max_agent = 0;
    let mut resumed = 0;
    for agent in restored {
        let Some(agent_id) = parse_agent_id(&agent.id) else {
            warn!(id = %agent.id, "scheduler: skipping invalid persisted agent id");
            continue;
        };
        max_agent = max_agent.max(agent_id.0);

        if agent.status.is_terminal() {
            state.agents.insert(
                agent_id,
                AgentEntry {
                    agent_id,
                    backend: agent.backend.clone(),
                    role: agent.role,
                    status: agent.status.clone(),
                    control: None,
                    session_id: agent.session_id.clone(),
                    model: agent.model.clone(),
                    scope_hash: agent.scope_hash,
                    transcript: agent.transcript.clone(),
                    last_role: agent.last_role.clone(),
                },
            );
            continue;
        }

        let Some(session_id) = agent.session_id.clone() else {
            warn!(id = %agent.id, "scheduler: cannot restore agent without persisted session id");
            state.agents.insert(
                agent_id,
                AgentEntry {
                    agent_id,
                    backend: agent.backend.clone(),
                    role: agent.role,
                    status: AgentStatus::Failed,
                    control: None,
                    session_id: None,
                    model: agent.model.clone(),
                    scope_hash: agent.scope_hash,
                    transcript: agent.transcript.clone(),
                    last_role: agent.last_role.clone(),
                },
            );
            if let Some(entry) = state.agents.get(&agent_id) {
                persist_agent_entry(db, entry);
            }
            continue;
        };
        let Some(scope_hash) = agent.scope_hash else {
            warn!(id = %agent.id, "scheduler: cannot restore agent without persisted scope");
            state.agents.insert(
                agent_id,
                AgentEntry {
                    agent_id,
                    backend: agent.backend.clone(),
                    role: agent.role,
                    status: AgentStatus::Failed,
                    control: None,
                    session_id: Some(session_id),
                    model: agent.model.clone(),
                    scope_hash: None,
                    transcript: agent.transcript.clone(),
                    last_role: agent.last_role.clone(),
                },
            );
            if let Some(entry) = state.agents.get(&agent_id) {
                persist_agent_entry(db, entry);
            }
            continue;
        };

        let snapshot = match get_scope_snapshot_by_hash(sys, scope_hash).await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                warn!(id = %agent.id, %error, "scheduler: cannot restore agent scope");
                state.agents.insert(
                    agent_id,
                    AgentEntry {
                        agent_id,
                        backend: agent.backend.clone(),
                        role: agent.role,
                        status: AgentStatus::Failed,
                        control: None,
                        session_id: Some(session_id),
                        model: agent.model.clone(),
                        scope_hash: Some(scope_hash),
                        transcript: agent.transcript.clone(),
                        last_role: agent.last_role.clone(),
                    },
                );
                if let Some(entry) = state.agents.get(&agent_id) {
                    persist_agent_entry(db, entry);
                }
                continue;
            }
        };
        let backend = match config.agent.backend(Some(&agent.backend)) {
            Ok((_, backend)) => backend,
            Err(error) => {
                warn!(id = %agent.id, backend = %agent.backend, "scheduler: cannot restore agent backend: {error}");
                state.agents.insert(
                    agent_id,
                    AgentEntry {
                        agent_id,
                        backend: agent.backend.clone(),
                        role: agent.role,
                        status: AgentStatus::Failed,
                        control: None,
                        session_id: Some(session_id),
                        model: agent.model.clone(),
                        scope_hash: Some(scope_hash),
                        transcript: agent.transcript.clone(),
                        last_role: agent.last_role.clone(),
                    },
                );
                if let Some(entry) = state.agents.get(&agent_id) {
                    persist_agent_entry(db, entry);
                }
                continue;
            }
        };

        match launch_agent(
            agent_id,
            AgentLaunch::Restore {
                session_id: session_id.clone(),
            },
            backend,
            agent.model.clone(),
            snapshot,
            sys.scheduler.clone(),
        ) {
            Ok(control) => {
                state.agents.insert(
                    agent_id,
                    AgentEntry {
                        agent_id,
                        backend: agent.backend.clone(),
                        role: agent.role,
                        status: AgentStatus::Running,
                        control: Some(control),
                        session_id: Some(session_id),
                        model: agent.model.clone(),
                        scope_hash: Some(scope_hash),
                        transcript: agent.transcript.clone(),
                        last_role: agent.last_role.clone(),
                    },
                );
                if let Some(entry) = state.agents.get(&agent_id) {
                    persist_agent_entry(db, entry);
                }
                resumed += 1;
            }
            Err(error) => {
                warn!(id = %agent.id, "scheduler: failed to relaunch persisted agent: {error:?}");
                state.agents.insert(
                    agent_id,
                    AgentEntry {
                        agent_id,
                        backend: agent.backend.clone(),
                        role: agent.role,
                        status: AgentStatus::Failed,
                        control: None,
                        session_id: Some(session_id),
                        model: agent.model.clone(),
                        scope_hash: Some(scope_hash),
                        transcript: agent.transcript.clone(),
                        last_role: agent.last_role.clone(),
                    },
                );
                if let Some(entry) = state.agents.get(&agent_id) {
                    persist_agent_entry(db, entry);
                }
            }
        }
    }

    if max_agent > 0 {
        state.next_agent = max_agent + 1;
        info!(
            restored = resumed,
            next_agent = state.next_agent,
            "scheduler: restored active agents"
        );
    }
}

fn persist_job_entry(db: &Arc<Mutex<Connection>>, entry: &JobEntry) {
    if !entry.status.is_terminal() {
        return;
    }

    let conn = match db.lock() {
        Ok(conn) => conn,
        Err(e) => {
            warn!(job = %entry.job_id, "scheduler: failed to lock job history db: {e}");
            return;
        }
    };

    if let Err(e) = storage::upsert_job_history(
        &conn,
        &storage::StoredJob {
            id: entry.job_id.to_string(),
            pipeline: entry.pipeline_text.clone(),
            status: entry.status.clone(),
            exit_code: entry.exit_code,
            start_scope: entry.start_scope,
            end_scope: entry.end_scope,
            chain_id: entry.chain_id.map(|id| id.to_string()),
            stderr: String::new(),
        },
    ) {
        warn!(job = %entry.job_id, "scheduler: failed to persist job history: {e}");
    }
}

fn persist_cron_entry(db: &Arc<Mutex<Connection>>, entry: &CronEntry) {
    persist_cron_record(
        db,
        &storage::StoredCron {
            id: entry.cron_id.to_string(),
            schedule: entry.schedule_text.clone(),
            command: chain_to_text(&entry.chain),
            status: entry.status,
            scope_hash: Some(entry.scope_hash),
            age_secs: 0,
        },
    );
}

fn persist_cron_record(db: &Arc<Mutex<Connection>>, cron: &storage::StoredCron) {
    let conn = match db.lock() {
        Ok(conn) => conn,
        Err(e) => {
            warn!(cron = %cron.id, "scheduler: failed to lock cron db: {e}");
            return;
        }
    };

    if let Err(e) = storage::upsert_cron(&conn, cron) {
        warn!(cron = %cron.id, "scheduler: failed to persist cron: {e}");
    }
}

fn persist_agent_entry(db: &Arc<Mutex<Connection>>, entry: &AgentEntry) {
    let conn = match db.lock() {
        Ok(conn) => conn,
        Err(e) => {
            warn!(agent = %entry.agent_id, "scheduler: failed to lock agent db: {e}");
            return;
        }
    };

    if let Err(e) = storage::upsert_agent_history(
        &conn,
        &storage::StoredAgent {
            id: entry.agent_id.to_string(),
            backend: entry.backend.clone(),
            role: entry.role,
            status: entry.status.clone(),
            session_id: entry.session_id.clone(),
            model: entry.model.clone(),
            scope_hash: entry.scope_hash,
            transcript: entry.transcript.clone(),
            last_role: entry.last_role.clone(),
        },
    ) {
        warn!(agent = %entry.agent_id, "scheduler: failed to persist agent history: {e}");
    }
}

fn append_agent_transcript(entry: &mut AgentEntry, role: &str, content: &str) {
    if content.is_empty() {
        return;
    }

    if role == "system"
        && let Some(session_id) = entry.session_id.as_deref()
        && content == format!("ACP session: {session_id}")
        && entry.transcript.contains(content)
    {
        entry.last_role = Some(role.to_string());
        return;
    }

    let same_role = entry.last_role.as_deref() == Some(role);
    if !entry.transcript.is_empty() && !same_role {
        entry.transcript.push_str("\n\n");
    }
    if !same_role && role != "assistant" {
        entry.transcript.push_str(&format!("[{role}] "));
    }
    entry.transcript.push_str(content);
    entry.last_role = Some(role.to_string());
}

fn mode_param_string(params: &ModeParams, key: &str) -> Option<String> {
    match params.get(key) {
        Some(ParamValue::Str(value)) => Some(value.clone()),
        _ => None,
    }
}

#[allow(clippy::result_large_err)]
fn parse_agent_role(params: &ModeParams, default: AgentRole) -> Result<AgentRole, ResponsePayload> {
    match mode_param_string(params, "role").as_deref() {
        None => Ok(default),
        Some("planner") => Ok(AgentRole::Planner),
        Some("executor") => Ok(AgentRole::Executor),
        Some(other) => Err(ResponsePayload::err(
            error_code::INVALID_SYNTAX,
            format!("invalid agent role `{other}`"),
        )),
    }
}

#[allow(clippy::result_large_err)]
fn resolve_backend(
    config: &Config,
    params: &ModeParams,
    allow_kind_param: bool,
) -> Result<(String, AgentBackendConfig), ResponsePayload> {
    let backend_name = mode_param_string(params, "agent").or_else(|| {
        if allow_kind_param {
            mode_param_string(params, "kind")
        } else {
            None
        }
    });
    config
        .agent
        .backend(backend_name.as_deref())
        .map_err(|error| ResponsePayload::err(error_code::NOT_FOUND, error.to_string()))
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

async fn send_agent_failure(
    scheduler: mpsc::Sender<SchedulerMsg>,
    agent_id: AgentId,
    message: String,
) {
    let _ = scheduler
        .send(SchedulerMsg::AgentMessage {
            agent_id,
            role: "system".into(),
            content: message,
        })
        .await;
    let _ = scheduler
        .send(SchedulerMsg::AgentStateChanged {
            agent_id,
            status: AgentStatus::Failed,
        })
        .await;
}

async fn write_agent_rpc_line(
    stdin: &mut tokio::process::ChildStdin,
    value: serde_json::Value,
) -> std::io::Result<()> {
    stdin.write_all(value.to_string().as_bytes()).await?;
    stdin.write_all(b"\n").await?;
    stdin.flush().await
}

async fn write_agent_request(
    stdin: &mut tokio::process::ChildStdin,
    request_id: u64,
    method: &str,
    params: serde_json::Value,
) -> std::io::Result<()> {
    write_agent_rpc_line(
        stdin,
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params,
        }),
    )
    .await
}

async fn write_agent_response(
    stdin: &mut tokio::process::ChildStdin,
    id: serde_json::Value,
    result: serde_json::Value,
) -> std::io::Result<()> {
    write_agent_rpc_line(
        stdin,
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result,
        }),
    )
    .await
}

async fn write_agent_error_response(
    stdin: &mut tokio::process::ChildStdin,
    id: serde_json::Value,
    code: i64,
    message: String,
) -> std::io::Result<()> {
    write_agent_rpc_line(
        stdin,
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {
                "code": code,
                "message": message,
            },
        }),
    )
    .await
}

async fn write_acp_prompt(
    stdin: &mut tokio::process::ChildStdin,
    request_id: u64,
    session_id: &str,
    prompt: String,
) -> std::io::Result<()> {
    write_agent_request(
        stdin,
        request_id,
        "session/prompt",
        serde_json::json!({
            "sessionId": session_id,
            "prompt": [{
                "type": "text",
                "text": prompt,
            }],
        }),
    )
    .await
}

fn extract_acp_text_block(value: &serde_json::Value) -> Option<String> {
    match value.get("type").and_then(|value| value.as_str()) {
        Some("text") => value
            .get("text")
            .and_then(|value| value.as_str())
            .filter(|text| !text.is_empty())
            .map(ToOwned::to_owned),
        Some("content") => value.get("content").and_then(extract_acp_text_block),
        _ => None,
    }
}

fn extract_acp_text_blocks(values: &[serde_json::Value]) -> Option<String> {
    let text = values
        .iter()
        .filter_map(extract_acp_text_block)
        .collect::<Vec<_>>()
        .join("");
    (!text.is_empty()).then_some(text)
}

async fn forward_acp_session_update(
    scheduler: &mpsc::Sender<SchedulerMsg>,
    agent_id: AgentId,
    update: &serde_json::Value,
) {
    let Some(kind) = update.get("sessionUpdate").and_then(|value| value.as_str()) else {
        return;
    };

    let forwarded = match kind {
        "agent_message_chunk" => update
            .get("content")
            .and_then(extract_acp_text_block)
            .map(|content| ("assistant".to_string(), content)),
        "user_message_chunk" => update
            .get("content")
            .and_then(extract_acp_text_block)
            .map(|content| ("user".to_string(), content)),
        "plan" => update
            .get("entries")
            .and_then(|value| value.as_array())
            .map(|entries| {
                let content = entries
                    .iter()
                    .filter_map(|entry| {
                        let text = entry.get("content")?.as_str()?;
                        let status = entry
                            .get("status")
                            .and_then(|value| value.as_str())
                            .unwrap_or("pending");
                        Some(format!("- [{status}] {text}"))
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                ("system".to_string(), format!("[plan]\n{content}"))
            })
            .filter(|(_, content)| !content.trim().is_empty()),
        "tool_call" => Some((
            "system".to_string(),
            format!(
                "[tool] {} ({})",
                update
                    .get("title")
                    .and_then(|value| value.as_str())
                    .unwrap_or("tool call"),
                update
                    .get("status")
                    .and_then(|value| value.as_str())
                    .unwrap_or("pending"),
            ),
        )),
        "tool_call_update" => {
            let status = update
                .get("status")
                .and_then(|value| value.as_str())
                .unwrap_or("updated");
            let body = update
                .get("content")
                .and_then(|value| value.as_array())
                .and_then(|items| extract_acp_text_blocks(items));
            Some((
                "system".to_string(),
                match body {
                    Some(text) => format!("[tool:{status}] {text}"),
                    None => format!("[tool:{status}]"),
                },
            ))
        }
        _ => None,
    };

    if let Some((role, content)) = forwarded {
        let _ = scheduler
            .send(SchedulerMsg::AgentMessage {
                agent_id,
                role,
                content,
            })
            .await;
    }
}

fn acp_response_error(value: &serde_json::Value) -> Option<String> {
    let error = value.get("error")?;
    error
        .get("message")
        .and_then(|value| value.as_str())
        .or_else(|| error.as_str())
        .map(ToOwned::to_owned)
}

enum AcpPhase {
    Initializing {
        request_id: u64,
        launch: AgentLaunch,
    },
    OpeningSession {
        request_id: u64,
        launch: AgentLaunch,
    },
    Prompting {
        request_id: u64,
        session_id: String,
        cancel_request_id: Option<u64>,
    },
    Idle {
        session_id: String,
    },
}

#[allow(clippy::result_large_err)]
fn launch_agent(
    agent_id: AgentId,
    launch: AgentLaunch,
    backend: AgentBackendConfig,
    model_override: Option<String>,
    snapshot: cue_core::scope::EnvSnapshot,
    scheduler: mpsc::Sender<SchedulerMsg>,
) -> Result<mpsc::Sender<AgentControl>, ResponsePayload> {
    if backend.command.trim().is_empty() {
        return Err(ResponsePayload::err(
            error_code::INVALID_STATE,
            "ACP agent backend command is empty; set [agent.backends.<name>].command in server.toml (or legacy config.toml)",
        ));
    }

    let snapshot = effective_snapshot(&snapshot);
    let mut command = Command::new(&backend.command);
    command.args(&backend.args);
    if let Some(model) = model_override.or(backend.model.clone()) {
        command.arg("--model").arg(model);
    }
    let session_cwd = snapshot.cwd.clone();
    command.current_dir(&session_cwd);
    command.env_clear();
    command.envs(snapshot.env);
    command.stdin(std::process::Stdio::piped());
    command.stdout(std::process::Stdio::piped());
    command.stderr(std::process::Stdio::piped());

    let mut child = command.spawn().map_err(|error| {
        ResponsePayload::err(
            error_code::INTERNAL,
            format!(
                "failed to start agent backend `{}`: {error}",
                backend.command
            ),
        )
    })?;
    let mut stdin = child.stdin.take().ok_or_else(|| {
        ResponsePayload::err(error_code::INTERNAL, "agent backend missing stdin pipe")
    })?;
    let stdout = child.stdout.take().ok_or_else(|| {
        ResponsePayload::err(error_code::INTERNAL, "agent backend missing stdout pipe")
    })?;
    let stderr = child.stderr.take().ok_or_else(|| {
        ResponsePayload::err(error_code::INTERNAL, "agent backend missing stderr pipe")
    })?;
    let (control_tx, mut control_rx) = mpsc::channel::<AgentControl>(16);

    tokio::spawn(async move {
        let stderr_task = tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            let mut collected = String::new();
            while let Ok(Some(line)) = lines.next_line().await {
                if !collected.is_empty() {
                    collected.push('\n');
                }
                collected.push_str(&line);
            }
            collected
        });

        let mut next_request_id = 1_u64;
        let init_request_id = next_request_id;
        next_request_id += 1;
        if write_agent_request(
            &mut stdin,
            init_request_id,
            "initialize",
            serde_json::json!({
                "protocolVersion": 1,
                "clientCapabilities": {},
                "clientInfo": {
                    "name": "cue-shell",
                    "title": "Cue Shell",
                    "version": cue_core::version(),
                },
            }),
        )
        .await
        .is_err()
        {
            let _ = child.kill().await;
            let _ = child.wait().await;
            send_agent_failure(
                scheduler.clone(),
                agent_id,
                "failed to initialize ACP agent backend".into(),
            )
            .await;
            return;
        }

        let mut lines = BufReader::new(stdout).lines();
        let mut current_status = AgentStatus::Running;
        let mut terminal_status_sent = false;
        let mut phase = AcpPhase::Initializing {
            request_id: init_request_id,
            launch,
        };

        loop {
            tokio::select! {
                control = control_rx.recv() => {
                    match control {
                        Some(AgentControl::Prompt(next_prompt)) => {
                            let session_id = match &phase {
                                AcpPhase::Idle { session_id } => session_id.clone(),
                                _ => continue,
                            };
                            let request_id = next_request_id;
                            next_request_id += 1;
                            if write_acp_prompt(&mut stdin, request_id, &session_id, next_prompt).await.is_err() {
                                let _ = child.kill().await;
                                terminal_status_sent = true;
                                send_agent_failure(
                                    scheduler.clone(),
                                    agent_id,
                                    "failed to send prompt to agent backend".into(),
                                )
                                .await;
                                break;
                            }
                            phase = AcpPhase::Prompting {
                                request_id,
                                session_id,
                                cancel_request_id: None,
                            };
                            if current_status != AgentStatus::Running {
                                current_status = AgentStatus::Running;
                                let _ = scheduler
                                    .send(SchedulerMsg::AgentStateChanged {
                                        agent_id,
                                        status: AgentStatus::Running,
                                    })
                                    .await;
                            }
                        }
                        Some(AgentControl::Abort) => {
                            let (prompt_request_id, session_id, cancel_request_id) = match &phase {
                                AcpPhase::Prompting {
                                    request_id,
                                    session_id,
                                    cancel_request_id,
                                } => (*request_id, session_id.clone(), *cancel_request_id),
                                _ => continue,
                            };
                            if cancel_request_id.is_some() {
                                continue;
                            }
                            let request_id = next_request_id;
                            next_request_id += 1;
                            if write_agent_request(
                                &mut stdin,
                                request_id,
                                "session/cancel",
                                serde_json::json!({ "sessionId": session_id }),
                            )
                                .await
                                .is_err()
                            {
                                let _ = child.kill().await;
                                terminal_status_sent = true;
                                send_agent_failure(
                                    scheduler.clone(),
                                    agent_id,
                                    "failed to cancel current ACP prompt turn".into(),
                                )
                                .await;
                                break;
                            }
                            phase = AcpPhase::Prompting {
                                request_id: prompt_request_id,
                                session_id,
                                cancel_request_id: Some(request_id),
                            };
                        }
                        Some(AgentControl::Shutdown) | None => {
                            let _ = child.kill().await;
                            let _ = child.wait().await;
                            let _ = scheduler
                                .send(SchedulerMsg::AgentMessage {
                                    agent_id,
                                    role: "system".into(),
                                    content: "aborted by user".into(),
                                })
                                .await;
                            let _ = scheduler
                                .send(SchedulerMsg::AgentStateChanged {
                                    agent_id,
                                    status: AgentStatus::Failed,
                                })
                                .await;
                            return;
                        }
                    }
                }
                line = lines.next_line() => {
                    let Ok(Some(line)) = line else {
                        break;
                    };
                    let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) else {
                        continue;
                    };

                    if let Some(method) = value.get("method").and_then(|value| value.as_str()) {
                        if method == "session/update" && value.get("id").is_none() {
                            if let Some(update) = value.pointer("/params/update") {
                                forward_acp_session_update(&scheduler, agent_id, update).await;
                            }
                            continue;
                        }

                        let Some(request_id) = value.get("id").cloned() else {
                            continue;
                        };
                        let params = value.get("params").cloned().unwrap_or(serde_json::Value::Null);
                        let result = if method == "session/request_permission" {
                            if let Some(option_id) = params
                                .pointer("/options/0/optionId")
                                .and_then(|value| value.as_str())
                            {
                                write_agent_response(
                                    &mut stdin,
                                    request_id,
                                    serde_json::json!({
                                        "outcome": "selected",
                                        "optionId": option_id,
                                    }),
                                )
                                .await
                            } else {
                                write_agent_response(
                                    &mut stdin,
                                    request_id,
                                    serde_json::json!({ "outcome": "cancelled" }),
                                )
                                .await
                            }
                        } else {
                            write_agent_error_response(
                                &mut stdin,
                                request_id,
                                -32601,
                                format!("ACP client method `{method}` is not supported by cue-shell yet"),
                            )
                            .await
                        };
                        if result.is_err() {
                            let _ = child.kill().await;
                            terminal_status_sent = true;
                            send_agent_failure(
                                scheduler.clone(),
                                agent_id,
                                "failed to reply to ACP agent request".into(),
                            )
                            .await;
                            break;
                        }
                        continue;
                    }

                    let Some(response_id) = value.get("id").and_then(|value| value.as_u64()) else {
                        continue;
                    };
                    if let Some(error) = acp_response_error(&value) {
                        let _ = child.kill().await;
                        terminal_status_sent = true;
                        send_agent_failure(scheduler.clone(), agent_id, error).await;
                        break;
                    }

                    match &phase {
                        AcpPhase::Initializing {
                            request_id,
                            launch,
                        } if response_id == *request_id => {
                            let launch = launch.clone();
                            let load_supported = value
                                .pointer("/result/agentCapabilities/loadSession")
                                .and_then(|value| value.as_bool())
                                .unwrap_or(false);
                            let request_id = next_request_id;
                            next_request_id += 1;
                            let request_result = match &launch {
                                AgentLaunch::Prompt {
                                    requested_session: Some(session_id),
                                    ..
                                }
                                | AgentLaunch::Restore { session_id } => {
                                    if !load_supported {
                                        let _ = child.kill().await;
                                        terminal_status_sent = true;
                                        send_agent_failure(
                                            scheduler.clone(),
                                            agent_id,
                                            format!("ACP agent backend does not support session/load for session `{session_id}`"),
                                        )
                                        .await;
                                        break;
                                    }
                                    write_agent_request(
                                        &mut stdin,
                                        request_id,
                                        "session/load",
                                        serde_json::json!({
                                            "sessionId": session_id,
                                            "cwd": session_cwd.to_string_lossy().into_owned(),
                                            "mcpServers": [],
                                        }),
                                    )
                                    .await
                                }
                                AgentLaunch::Prompt {
                                    requested_session: None,
                                    ..
                                } => {
                                    write_agent_request(
                                        &mut stdin,
                                        request_id,
                                        "session/new",
                                        serde_json::json!({
                                            "cwd": session_cwd.to_string_lossy().into_owned(),
                                            "mcpServers": [],
                                        }),
                                    )
                                    .await
                                }
                            };
                            if request_result.is_err() {
                                let _ = child.kill().await;
                                terminal_status_sent = true;
                                send_agent_failure(
                                    scheduler.clone(),
                                    agent_id,
                                    "failed to open ACP session".into(),
                                )
                                .await;
                                break;
                            }
                            phase = AcpPhase::OpeningSession {
                                request_id,
                                launch,
                            };
                        }
                        AcpPhase::OpeningSession {
                            request_id,
                            launch,
                        } if response_id == *request_id => {
                            let session_id = match launch {
                                AgentLaunch::Prompt {
                                    requested_session: Some(session_id),
                                    ..
                                }
                                | AgentLaunch::Restore { session_id } => session_id.clone(),
                                AgentLaunch::Prompt {
                                    requested_session: None,
                                    ..
                                } => value
                                    .pointer("/result/sessionId")
                                    .and_then(|value| value.as_str())
                                    .unwrap_or_default()
                                    .to_string(),
                            };
                            if session_id.is_empty() {
                                let _ = child.kill().await;
                                terminal_status_sent = true;
                                send_agent_failure(
                                    scheduler.clone(),
                                    agent_id,
                                    "ACP session open response did not include sessionId".into(),
                                )
                                .await;
                                break;
                            }
                            let _ = scheduler
                                .send(SchedulerMsg::AgentSessionBound {
                                    agent_id,
                                    session_id: session_id.clone(),
                                })
                                .await;
                            let _ = scheduler
                                .send(SchedulerMsg::AgentMessage {
                                    agent_id,
                                    role: "system".into(),
                                    content: format!("ACP session: {session_id}"),
                                })
                                .await;
                            match launch {
                                AgentLaunch::Prompt { initial_prompt, .. } => {
                                    let prompt_request_id = next_request_id;
                                    next_request_id += 1;
                                    if write_acp_prompt(
                                        &mut stdin,
                                        prompt_request_id,
                                        &session_id,
                                        initial_prompt.clone(),
                                    )
                                    .await
                                    .is_err()
                                    {
                                        let _ = child.kill().await;
                                        terminal_status_sent = true;
                                        send_agent_failure(
                                            scheduler.clone(),
                                            agent_id,
                                            "failed to send ACP prompt".into(),
                                        )
                                        .await;
                                        break;
                                    }
                                    phase = AcpPhase::Prompting {
                                        request_id: prompt_request_id,
                                        session_id,
                                        cancel_request_id: None,
                                    };
                                }
                                AgentLaunch::Restore { .. } => {
                                    current_status = AgentStatus::WaitingInput;
                                    let _ = scheduler
                                        .send(SchedulerMsg::AgentStateChanged {
                                            agent_id,
                                            status: AgentStatus::WaitingInput,
                                        })
                                        .await;
                                    phase = AcpPhase::Idle { session_id };
                                }
                            }
                        }
                        AcpPhase::Prompting {
                            request_id,
                            session_id,
                            cancel_request_id,
                        } if response_id == *request_id
                            || cancel_request_id.is_some_and(|cancel_id| response_id == cancel_id) =>
                        {
                            let session_id = session_id.clone();
                            if current_status != AgentStatus::WaitingInput {
                                current_status = AgentStatus::WaitingInput;
                                let _ = scheduler
                                    .send(SchedulerMsg::AgentStateChanged {
                                        agent_id,
                                        status: AgentStatus::WaitingInput,
                                    })
                                    .await;
                            }
                            phase = AcpPhase::Idle { session_id };
                        }
                        _ => {}
                    }
                }
            }
        }

        let exit_status = child.wait().await.ok();
        let stderr_output = stderr_task.await.unwrap_or_default();

        if !terminal_status_sent {
            if !stderr_output.trim().is_empty() {
                let _ = scheduler
                    .send(SchedulerMsg::AgentMessage {
                        agent_id,
                        role: "system".into(),
                        content: stderr_output.trim().to_string(),
                    })
                    .await;
            }
            let status = if exit_status.is_some_and(|status| status.success()) {
                AgentStatus::Done
            } else {
                AgentStatus::Failed
            };
            let _ = scheduler
                .send(SchedulerMsg::AgentStateChanged { agent_id, status })
                .await;
        }
    });

    Ok(control_tx)
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
            let command = pipeline
                .segments
                .first()
                .map(|s| s.command.clone())
                .unwrap_or_default();
            let pipeline_text = pipeline_to_text(pipeline);
            out.push(FlatLeaf {
                index: idx,
                command,
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
        CronSchedule::FreeForm(_) => None,
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

fn parse_time_of_day(input: &str) -> Option<u32> {
    let normalized = input.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "midnight" => return Some(0),
        "noon" => return Some(12 * 3600),
        _ => {}
    }

    let (core, meridiem) = if let Some(stripped) = normalized.strip_suffix("am") {
        (stripped, Some("am"))
    } else if let Some(stripped) = normalized.strip_suffix("pm") {
        (stripped, Some("pm"))
    } else {
        (normalized.as_str(), None)
    };

    let (mut hour, minute) = if let Some((hour, minute)) = core.split_once(':') {
        (hour.parse::<u32>().ok()?, minute.parse::<u32>().ok()?)
    } else {
        (core.parse::<u32>().ok()?, 0)
    };
    if minute >= 60 {
        return None;
    }

    match meridiem {
        Some("am") => {
            if hour == 12 {
                hour = 0;
            } else if hour > 11 {
                return None;
            }
        }
        Some("pm") => {
            if hour < 12 {
                hour += 12;
            } else if hour > 12 {
                return None;
            }
        }
        None if hour > 23 => return None,
        None => {}
        _ => return None,
    }

    Some(hour * 3600 + minute * 60)
}

fn parse_day_filter(input: &str) -> Option<DayFilter> {
    let normalized = input.trim().to_ascii_lowercase();
    let days = match normalized.as_str() {
        "daily" => vec![
            Weekday::Mon,
            Weekday::Tue,
            Weekday::Wed,
            Weekday::Thu,
            Weekday::Fri,
            Weekday::Sat,
            Weekday::Sun,
        ],
        "weekdays" => vec![
            Weekday::Mon,
            Weekday::Tue,
            Weekday::Wed,
            Weekday::Thu,
            Weekday::Fri,
        ],
        "weekends" => vec![Weekday::Sat, Weekday::Sun],
        _ => {
            let mut out = Vec::new();
            for part in normalized.split(',') {
                if let Some((start, end)) = part.split_once('-') {
                    let start = parse_weekday_name(start)?;
                    let end = parse_weekday_name(end)?;
                    out.extend(expand_weekday_range(start, end));
                } else {
                    out.push(parse_weekday_name(part)?);
                }
            }
            out
        }
    };
    Some(DayFilter { days })
}

fn parse_weekday_name(input: &str) -> Option<Weekday> {
    match input.trim().to_ascii_lowercase().as_str() {
        "mon" | "monday" => Some(Weekday::Mon),
        "tue" | "tues" | "tuesday" => Some(Weekday::Tue),
        "wed" | "wednesday" => Some(Weekday::Wed),
        "thu" | "thur" | "thurs" | "thursday" => Some(Weekday::Thu),
        "fri" | "friday" => Some(Weekday::Fri),
        "sat" | "saturday" => Some(Weekday::Sat),
        "sun" | "sunday" => Some(Weekday::Sun),
        _ => None,
    }
}

fn expand_weekday_range(start: Weekday, end: Weekday) -> Vec<Weekday> {
    let ordered = [
        Weekday::Mon,
        Weekday::Tue,
        Weekday::Wed,
        Weekday::Thu,
        Weekday::Fri,
        Weekday::Sat,
        Weekday::Sun,
    ];
    let start_idx = ordered.iter().position(|day| *day == start).unwrap_or(0);
    let end_idx = ordered
        .iter()
        .position(|day| *day == end)
        .unwrap_or(start_idx);
    if start_idx <= end_idx {
        ordered[start_idx..=end_idx].to_vec()
    } else {
        ordered[start_idx..]
            .iter()
            .chain(ordered[..=end_idx].iter())
            .copied()
            .collect()
    }
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
                return Err("`cd` inside `:run` expects exactly one path argument".into());
            }
            Ok(Some(ScopeTransform::Cd {
                path: words[1].clone(),
            }))
        }
        "env" if words.get(1).map(String::as_str) == Some("set") => {
            if words.len() < 3 {
                return Err(
                    "`env set` inside `:run` expects at least one KEY=VALUE assignment".into(),
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

fn classify_job_open_hint(command_line: &[String]) -> JobOpenHint {
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

fn agent_info_from_entry(entry: &AgentEntry) -> AgentInfo {
    AgentInfo {
        id: entry.agent_id.to_string(),
        status: entry.status.clone(),
        backend: entry.backend.clone(),
        role: match entry.role {
            AgentRole::Planner => "planner".into(),
            AgentRole::Executor => "executor".into(),
        },
        transcript: entry.transcript.clone(),
        last_role: entry.last_role.clone(),
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

async fn notify_agent_waiters(state: &mut SchedulerState, sys: &ActorSystem, agent_id: AgentId) {
    let Some(waiters) = state.agent_waiters.remove(&agent_id) else {
        return;
    };
    let Some(entry) = state.agents.get(&agent_id) else {
        return;
    };
    let payload = ResponsePayload::Ok(OkPayload::AgentInfo(agent_info_from_entry(entry)));
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
        let response = spawn_chain(chain, scope_hash, 0, 0, cwd_override, state, db, sys).await;
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

/// Spawn a chain (or a single job) from a `ChainNode`, returning the response payload.
#[allow(clippy::too_many_arguments)]
async fn spawn_chain(
    chain: ChainNode,
    scope_hash: ScopeHash,
    client_id: u64,
    request_id: u32,
    cwd_override: Option<std::path::PathBuf>,
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
        let open_hint = classify_job_open_hint(&leaf.command);

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
                stderr: String::new(),
            },
        );

        publish_job_created(sys, state, jid, &leaf.pipeline_text, scope_hash, open_hint).await;

        match scope_transform_from_command(&leaf.command) {
            Ok(Some(_)) => {
                info!(%jid, pipeline = %leaf.pipeline_text, "scheduler: applying single scope-transform job");
                match apply_scope_transform(sys, scope_hash, &leaf.command).await {
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
                        command_line: leaf.command.clone(),
                        scope_hash,
                        cwd_override: cwd_override.clone(),
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
        chain_id,
        client_id,
        request_id,
        node: chain,
        leaf_jobs: HashMap::new(),
        leaf_status,
        scope_hash,
        pipeline_text: chain_text,
        cwd_override: cwd_override.clone(),
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

    let leaves = {
        let Some(chain) = state.chains.get(&chain_id) else {
            return Vec::new();
        };
        flatten_leaves(&chain.node)
    };

    let mut queue: VecDeque<(usize, ScopeHash)> = newly_ready.into();
    let mut captured = Vec::new();

    while let Some((idx, start_scope)) = queue.pop_front() {
        let jid = state.alloc_job();
        let open_hint = classify_job_open_hint(&leaves[idx].command);
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
                stderr: String::new(),
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

        match scope_transform_from_command(&leaves[idx].command) {
            Ok(Some(_)) => {
                match apply_scope_transform(sys, start_scope, &leaves[idx].command).await {
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
                        command_line: leaves[idx].command.clone(),
                        scope_hash: start_scope,
                        cwd_override: cwd_override.clone(),
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

    if let Some(agent_id) = parse_agent_id(&id) {
        let Some(entry) = state.agents.get(&agent_id) else {
            return Some(ResponsePayload::err(
                error_code::NOT_FOUND,
                format!("agent {id} not found"),
            ));
        };
        if entry.status.is_terminal() {
            return Some(ResponsePayload::Ok(OkPayload::AgentInfo(
                agent_info_from_entry(entry),
            )));
        }
        state
            .agent_waiters
            .entry(agent_id)
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
            "`:wait` currently supports job and agent IDs only",
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
        ResolvedCommand::Run { chain, params } => {
            // Get current HEAD scope hash.
            let scope_hash = match get_head_scope(sys).await {
                Ok(h) => h,
                Err(resp) => return resp,
            };
            let cwd_override = params.cwd();
            spawn_chain(chain, scope_hash, 0, 0, cwd_override, state, db, sys).await
        }

        ResolvedCommand::Ask { text, params } => {
            let role = match parse_agent_role(&params, AgentRole::Planner) {
                Ok(role) => role,
                Err(response) => return response,
            };
            let model_override = mode_param_string(&params, "model");
            let session_override = mode_param_string(&params, "session");
            let (label, backend) = match resolve_backend(config, &params, true) {
                Ok(value) => value,
                Err(response) => return response,
            };
            let scope_hash = match get_head_scope(sys).await {
                Ok(hash) => hash,
                Err(response) => return response,
            };
            let mut snapshot = match get_head_snapshot(sys).await {
                Ok(snapshot) => snapshot,
                Err(response) => return response,
            };
            if let Some(cwd) = params.cwd() {
                snapshot.cwd = cwd;
            }
            let aid = state.alloc_agent();
            let control = match launch_agent(
                aid,
                AgentLaunch::Prompt {
                    initial_prompt: text.clone(),
                    requested_session: session_override.clone(),
                },
                backend.clone(),
                model_override.clone(),
                snapshot,
                sys.scheduler.clone(),
            ) {
                Ok(control) => control,
                Err(response) => return response,
            };
            state.agents.insert(
                aid,
                AgentEntry {
                    agent_id: aid,
                    backend: label.clone(),
                    role,
                    status: AgentStatus::Running,
                    control: Some(control),
                    session_id: session_override,
                    model: model_override,
                    scope_hash: Some(scope_hash),
                    transcript: String::new(),
                    last_role: None,
                },
            );
            if let Some(entry) = state.agents.get(&aid) {
                persist_agent_entry(db, entry);
            }
            info!(%aid, backend = %label, %text, "scheduler: planner agent spawned");
            ResponsePayload::Ok(OkPayload::AgentSpawned {
                agent_id: aid.to_string(),
            })
        }

        ResolvedCommand::Cron {
            schedule_text,
            chain,
            params,
        } => {
            let Some(schedule) = parse_schedule(&schedule_text) else {
                return ResponsePayload::err(
                    error_code::INVALID_SYNTAX,
                    format!("cannot parse schedule: {schedule_text}"),
                );
            };

            let scope_hash = match get_head_scope(sys).await {
                Ok(h) => h,
                Err(resp) => return resp,
            };

            let cron_id = state.alloc_cron();
            let Some(next_trigger) = next_trigger_instant(&schedule, 0) else {
                return ResponsePayload::err(
                    error_code::INVALID_SYNTAX,
                    format!("cannot compute next trigger for schedule: {schedule_text}"),
                );
            };
            let entry = CronEntry {
                cron_id,
                schedule_text: schedule_text.clone(),
                schedule,
                chain,
                scope_hash,
                status: CronStatus::Scheduled,
                next_trigger,
                cwd_override: params.cwd(),
            };
            persist_cron_entry(db, &entry);
            state.crons.insert(cron_id, entry);

            info!(%cron_id, %schedule_text, "scheduler: cron added");
            ResponsePayload::Ok(OkPayload::CronAdded {
                cron_id: cron_id.to_string(),
            })
        }

        ResolvedCommand::Spawn { text, params } => {
            let role = match parse_agent_role(&params, AgentRole::Executor) {
                Ok(role) => role,
                Err(response) => return response,
            };
            let model_override = mode_param_string(&params, "model");
            let session_override = mode_param_string(&params, "session");
            let (label, backend) = match resolve_backend(config, &params, true) {
                Ok(value) => value,
                Err(response) => return response,
            };
            let scope_hash = match get_head_scope(sys).await {
                Ok(hash) => hash,
                Err(response) => return response,
            };
            let mut snapshot = match get_head_snapshot(sys).await {
                Ok(snapshot) => snapshot,
                Err(response) => return response,
            };
            if let Some(cwd) = params.cwd() {
                snapshot.cwd = cwd;
            }
            let aid = state.alloc_agent();
            let control = match launch_agent(
                aid,
                AgentLaunch::Prompt {
                    initial_prompt: text.clone(),
                    requested_session: session_override.clone(),
                },
                backend.clone(),
                model_override.clone(),
                snapshot,
                sys.scheduler.clone(),
            ) {
                Ok(control) => control,
                Err(response) => return response,
            };
            state.agents.insert(
                aid,
                AgentEntry {
                    agent_id: aid,
                    backend: label.clone(),
                    role,
                    status: AgentStatus::Running,
                    control: Some(control),
                    session_id: session_override,
                    model: model_override,
                    scope_hash: Some(scope_hash),
                    transcript: String::new(),
                    last_role: None,
                },
            );
            if let Some(entry) = state.agents.get(&aid) {
                persist_agent_entry(db, entry);
            }
            info!(%aid, backend = %label, %text, "scheduler: executor agent spawned");
            ResponsePayload::Ok(OkPayload::AgentSpawned {
                agent_id: aid.to_string(),
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
            } else if let Some(agent_id) = parse_agent_id(&id) {
                match state.agents.get(&agent_id) {
                    Some(entry) if !entry.status.is_terminal() => {
                        ResponsePayload::Ok(OkPayload::FgAttached { id })
                    }
                    Some(_) => ResponsePayload::err(
                        error_code::INVALID_STATE,
                        format!("agent {agent_id} is already terminal"),
                    ),
                    None => {
                        ResponsePayload::err(error_code::NOT_FOUND, format!("agent {id} not found"))
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
            } else if let Some(aid) = parse_agent_id(&id) {
                let Some(entry) = state.agents.get(&aid) else {
                    return ResponsePayload::err(
                        error_code::NOT_FOUND,
                        format!("agent {id} not found"),
                    );
                };
                if entry.status.is_terminal() {
                    return ResponsePayload::err(
                        error_code::INVALID_STATE,
                        format!("agent {aid} is already terminal"),
                    );
                }
                let Some(control) = entry.control.clone() else {
                    return ResponsePayload::err(
                        error_code::INVALID_STATE,
                        format!("agent {aid} runtime is unavailable"),
                    );
                };
                if control.send(AgentControl::Shutdown).await.is_err() {
                    return ResponsePayload::err(error_code::INTERNAL, "agent runtime dropped");
                }
                ResponsePayload::ack()
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
            } else if let Some(aid) = parse_agent_id(&id) {
                let Some(entry) = state.agents.get(&aid) else {
                    return ResponsePayload::err(
                        error_code::NOT_FOUND,
                        format!("agent {id} not found"),
                    );
                };
                if entry.status != AgentStatus::Running {
                    return ResponsePayload::err(
                        error_code::INVALID_STATE,
                        format!("agent {aid} is not running"),
                    );
                }
                let Some(control) = entry.control.clone() else {
                    return ResponsePayload::err(
                        error_code::INVALID_STATE,
                        format!("agent {aid} runtime is unavailable"),
                    );
                };
                if control.send(AgentControl::Abort).await.is_err() {
                    return ResponsePayload::err(error_code::INTERNAL, "agent runtime dropped");
                }
                ResponsePayload::ack()
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

        ResolvedCommand::Agents => {
            let mut list: Vec<AgentInfo> =
                state.agents.values().map(agent_info_from_entry).collect();
            list.sort_by_key(|agent| parse_agent_id(&agent.id).map(|id| id.0).unwrap_or(u32::MAX));
            ResponsePayload::Ok(OkPayload::AgentList(list))
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

        ResolvedCommand::Confirm { text } => {
            ResponsePayload::Ok(OkPayload::ConfirmRequest { prompt: text })
        }

        ResolvedCommand::Escalate { text } => {
            let params = ModeParams::new();
            let role = AgentRole::Planner;
            let model_override = mode_param_string(&params, "model");
            let session_override = mode_param_string(&params, "session");
            let (label, backend) = match resolve_backend(config, &params, true) {
                Ok(value) => value,
                Err(response) => return response,
            };
            let scope_hash = match get_head_scope(sys).await {
                Ok(hash) => hash,
                Err(response) => return response,
            };
            let snapshot = match get_head_snapshot(sys).await {
                Ok(snapshot) => snapshot,
                Err(response) => return response,
            };
            let aid = state.alloc_agent();
            let control = match launch_agent(
                aid,
                AgentLaunch::Prompt {
                    initial_prompt: format!("executor escalation: {text}"),
                    requested_session: session_override.clone(),
                },
                backend.clone(),
                model_override.clone(),
                snapshot,
                sys.scheduler.clone(),
            ) {
                Ok(control) => control,
                Err(response) => return response,
            };
            state.agents.insert(
                aid,
                AgentEntry {
                    agent_id: aid,
                    backend: label.clone(),
                    role,
                    status: AgentStatus::Running,
                    control: Some(control),
                    session_id: session_override,
                    model: model_override,
                    scope_hash: Some(scope_hash),
                    transcript: String::new(),
                    last_role: None,
                },
            );
            if let Some(entry) = state.agents.get(&aid) {
                persist_agent_entry(db, entry);
            }
            info!(%aid, backend = %label, %text, "scheduler: planner escalation spawned");
            ResponsePayload::Ok(OkPayload::AgentSpawned {
                agent_id: aid.to_string(),
            })
        }

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
            } else if let Some(agent_id) = parse_agent_id(&id) {
                let Some(entry) = state.agents.get(&agent_id) else {
                    return ResponsePayload::err(
                        error_code::NOT_FOUND,
                        format!("agent {id} not found"),
                    );
                };
                if entry.status != AgentStatus::WaitingInput {
                    return ResponsePayload::err(
                        error_code::INVALID_STATE,
                        format!("agent {agent_id} is not waiting for input"),
                    );
                }
                let Some(control) = entry.control.clone() else {
                    return ResponsePayload::err(
                        error_code::INVALID_STATE,
                        format!("agent {agent_id} runtime is unavailable"),
                    );
                };
                if control.send(AgentControl::Prompt(data)).await.is_err() {
                    return ResponsePayload::err(error_code::INTERNAL, "agent runtime dropped");
                }
                ResponsePayload::ack()
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
            }
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
            spawn_chain(chain, start_scope, 0, 0, None, state, db, sys).await
        }

        ResolvedCommand::Probe { query } => {
            let parsed = match parse_probe_query(&query) {
                Ok(parsed) => parsed,
                Err(message) => return ResponsePayload::err(error_code::INVALID_SYNTAX, message),
            };
            handle_probe(parsed, state, sys).await
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

/// Parse a string like `"A2"` into an `AgentId`.
fn parse_agent_id(s: &str) -> Option<AgentId> {
    let s = s.trim();
    s.strip_prefix('A')
        .and_then(|n| n.parse::<u32>().ok())
        .map(AgentId)
}

/// Parse a string like `"C3"` into a `CronId`.
fn parse_cron_id(s: &str) -> Option<CronId> {
    let s = s.trim();
    s.strip_prefix('C')
        .and_then(|n| n.parse::<u32>().ok())
        .map(CronId)
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

enum ProbeCommand {
    Status { id: String },
    Output { id: String, tail_bytes: usize },
    Env { target: String, keys: Vec<String> },
}

fn parse_probe_query(input: &str) -> Result<ProbeCommand, String> {
    let words = tokenize_words(input)?;
    let Some((verb, rest)) = words.split_first() else {
        return Err("`:probe` requires a subcommand".into());
    };
    match verb.as_str() {
        "status" => {
            let id = rest
                .first()
                .cloned()
                .ok_or_else(|| "`:probe status` expects an ID".to_string())?;
            Ok(ProbeCommand::Status { id })
        }
        "out" | "err" => {
            let id = rest
                .first()
                .cloned()
                .ok_or_else(|| format!("`:probe {verb}` expects a job ID"))?;
            let mut tail_bytes = 4096usize;
            let mut idx = 1;
            while idx < rest.len() {
                match rest[idx].as_str() {
                    "--tail" => {
                        let value = rest
                            .get(idx + 1)
                            .ok_or_else(|| "missing value after `--tail`".to_string())?;
                        tail_bytes = value
                            .parse::<usize>()
                            .map_err(|_| format!("invalid `--tail` value `{value}`"))?
                            .min(4096);
                        idx += 2;
                    }
                    other => {
                        return Err(format!("unsupported `:probe {verb}` flag `{other}`"));
                    }
                }
            }
            Ok(ProbeCommand::Output { id, tail_bytes })
        }
        "env" => {
            let target = rest
                .first()
                .cloned()
                .ok_or_else(|| "`:probe env` expects `head` or a job ID".to_string())?;
            Ok(ProbeCommand::Env {
                target,
                keys: rest[1..].to_vec(),
            })
        }
        other => Err(format!(
            "unsupported `:probe` subcommand `{other}` (use status/out/err/env)"
        )),
    }
}

async fn handle_probe(
    command: ProbeCommand,
    state: &SchedulerState,
    sys: &ActorSystem,
) -> ResponsePayload {
    match command {
        ProbeCommand::Status { id } => {
            if let Some(job_id) = parse_job_id(&id) {
                let Some(entry) = state.jobs.get(&job_id) else {
                    return ResponsePayload::err(
                        error_code::NOT_FOUND,
                        format!("job {id} not found"),
                    );
                };
                ResponsePayload::Ok(OkPayload::JobInfo(job_info_from_entry(entry)))
            } else if let Some(agent_id) = parse_agent_id(&id) {
                let Some(entry) = state.agents.get(&agent_id) else {
                    return ResponsePayload::err(
                        error_code::NOT_FOUND,
                        format!("agent {id} not found"),
                    );
                };
                ResponsePayload::Ok(OkPayload::AgentInfo(agent_info_from_entry(entry)))
            } else if let Some(cron_id) = parse_cron_id(&id) {
                let Some(entry) = state.crons.get(&cron_id) else {
                    return ResponsePayload::err(
                        error_code::NOT_FOUND,
                        format!("cron {id} not found"),
                    );
                };
                ResponsePayload::Ok(OkPayload::EvalText {
                    text: format!(
                        "{} {} status={:?} next={:?}",
                        entry.cron_id, entry.schedule_text, entry.status, entry.next_trigger
                    ),
                })
            } else {
                ResponsePayload::err(error_code::NOT_FOUND, format!("{id} not found"))
            }
        }
        ProbeCommand::Output { id, tail_bytes } => {
            let Some(job_id) = parse_job_id(&id) else {
                return ResponsePayload::err(
                    error_code::NOT_FOUND,
                    format!("invalid job id: {id}"),
                );
            };
            read_job_output(sys, job_id, &id, tail_bytes).await
        }
        ProbeCommand::Env { target, keys } => {
            let snapshot = if target.eq_ignore_ascii_case("head") {
                match get_head_snapshot(sys).await {
                    Ok(snapshot) => snapshot,
                    Err(response) => return response,
                }
            } else if let Some(job_id) = parse_job_id(&target) {
                let Some(entry) = state.jobs.get(&job_id) else {
                    return ResponsePayload::err(
                        error_code::NOT_FOUND,
                        format!("job {target} not found"),
                    );
                };
                let Some(scope_hash) = entry.end_scope.or(entry.start_scope) else {
                    return ResponsePayload::err(
                        error_code::INVALID_SCOPE,
                        format!("job {job_id} has no recorded scope"),
                    );
                };
                match get_scope_snapshot_by_hash(sys, scope_hash).await {
                    Ok(snapshot) => snapshot,
                    Err(message) => return ResponsePayload::err(error_code::INTERNAL, message),
                }
            } else {
                return ResponsePayload::err(
                    error_code::NOT_SUPPORTED,
                    "`:probe env` currently supports `head` and job IDs only",
                );
            };
            ResponsePayload::Ok(OkPayload::EvalText {
                text: format_probe_env(&snapshot, &keys),
            })
        }
    }
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

fn format_probe_env(snapshot: &EnvSnapshot, keys: &[String]) -> String {
    if keys.is_empty() {
        return format_snapshot_env(snapshot);
    }
    let mut lines = vec![format!("cwd={}", snapshot.cwd.display())];
    for key in keys {
        let value = snapshot
            .env
            .get(key)
            .map(|value| value.escape_default().to_string())
            .unwrap_or_else(|| "<unset>".into());
        lines.push(format!("{key}={value}"));
    }
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
        None => general_help_text().into(),
        Some(topic) if is_job_help_topic(topic) => job_help_text().into(),
        Some(topic) if is_agent_help_topic(topic) => agent_help_text().into(),
        Some(topic) if is_cron_help_topic(topic) => cron_help_text().into(),
        Some(topic) => format!(
            "Unknown help topic `{topic}`.\n\nAvailable help topics: job, agent, cron.\nUse bare `?` to show detailed help for the current mode."
        ),
    }
}

fn is_job_help_topic(topic: &str) -> bool {
    matches!(
        topic,
        "job"
            | "jobs"
            | "run"
            | "out"
            | "tail"
            | "err"
            | "fg"
            | "wait"
            | "retry"
            | "scope"
            | "scopes"
            | "env"
            | "cd"
            | "log"
    )
}

fn is_agent_help_topic(topic: &str) -> bool {
    matches!(
        topic,
        "agent" | "agents" | "ask" | "spawn" | "send" | "cancel" | "confirm" | "escalate" | "probe"
    )
}

fn is_cron_help_topic(topic: &str) -> bool {
    matches!(topic, "cron" | "crons" | "pause" | "resume")
}

fn general_help_text() -> &'static str {
    concat!(
        "cue-shell help\n",
        "\n",
        "Modes:\n",
        "- JOB: run shell commands and inspect output / scopes.\n",
        "- AGENT: send prompts to ACP-backed agent sessions.\n",
        "- CRON: define scheduled commands.\n",
        "\n",
        "Quick tips:\n",
        "- Enter bare `?` to show detailed help for the current mode.\n",
        "- Use `:help job`, `:help agent`, or `:help cron` for mode-specific help.\n",
        "- Builtins start with `:` and are executed by `cued`.\n",
        "- Modes only change how bare input is interpreted.\n"
    )
}

fn job_help_text() -> &'static str {
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
        "- `:out J<n>` snapshot stdout\n",
        "- `:tail J<n> [bytes]` follow live stdout\n",
        "- `:err J<n>` inspect stderr / error output\n",
        "- `:fg J<n>` attach an interactive job in the foreground\n",
        "- `:wait J<n>` block until the job finishes\n",
        "- `:retry J<n>` rerun from the original start scope\n",
        "- `:probe status|out|err|env ...` inspect job state without changing it\n",
        "- `:env`, `:env set ...`, `:cd ...` inspect or update the persisted HEAD scope\n"
    )
}

fn agent_help_text() -> &'static str {
    concat!(
        "AGENT mode\n",
        "\n",
        "Bare input sends a prompt to the default ACP agent backend.\n",
        "Examples:\n",
        "- `explain this test failure`\n",
        "- `draft a fix plan for parser regressions`\n",
        "\n",
        "Useful builtins:\n",
        "- `:ask ...` send a one-shot prompt explicitly\n",
        "- `:spawn ...` create a long-lived agent session\n",
        "- `:send A<n> ...` continue an existing agent session\n",
        "- `:cancel A<n>` cancel the current in-flight turn\n",
        "- `:kill A<n>` terminate the whole session\n",
        "- `:agents` list known agent sessions\n",
        "- `:confirm ...` send a structured confirmation-style reply\n",
        "- `:escalate ...` create a planner-style follow-up session\n",
        "- `session=<id>` can resume a backend session when loadSession is available\n"
    )
}

fn cron_help_text() -> &'static str {
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
        "- `:cron <schedule> <command>` add a cron explicitly\n",
        "- `:crons` list configured cron entries\n",
        "- `:pause C<n>` temporarily disable a cron\n",
        "- `:resume C<n>` re-enable a paused cron\n",
        "- `:kill C<n>` remove a cron entry\n"
    )
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

/// Build a human-readable log of jobs and agents.
///
/// If `id` is given, only log for that specific job or agent is shown.
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
        if let Some(agent_id) = parse_agent_id(id) {
            return state
                .agents
                .get(&agent_id)
                .map(|entry| {
                    format!(
                        "{}: backend={} role={:?} status={:?}",
                        entry.agent_id, entry.backend, entry.role, entry.status
                    )
                })
                .unwrap_or_else(|| format!("{id}: agent not found"));
        }
        return format!("{id}: unrecognised ID (expected J<n> or A<n>)");
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

    let mut agents: Vec<&AgentEntry> = state.agents.values().collect();
    agents.sort_by_key(|a| a.agent_id.0);
    if agents.is_empty() {
        lines.push("agents: none".into());
    } else {
        lines.push("=== Agents ===".into());
        for entry in agents {
            lines.push(format!(
                "  {}: backend={} role={:?} [{:?}]",
                entry.agent_id, entry.backend, entry.role, entry.status
            ));
        }
    }

    lines.join("\n")
}

/// Format the active config as human-readable text.
fn format_config_text(config: &Config) -> String {
    let mut lines = vec![format!(
        "agent.default_backend = {}",
        config.agent.default_backend
    )];
    for (name, backend) in &config.agent.backends {
        lines.push(format!("[agent.backends.{name}]"));
        if !backend.command.is_empty() {
            lines.push(format!("  command = {}", backend.command));
        }
        if !backend.args.is_empty() {
            let args = backend
                .args
                .iter()
                .map(|a| format!("{a:?}"))
                .collect::<Vec<_>>()
                .join(", ");
            lines.push(format!("  args = [{args}]"));
        }
        if let Some(model) = &backend.model {
            lines.push(format!("  model = {model}"));
        }
    }
    lines.join("\n")
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
    if let Ok(Ok(data)) = stderr_data
        && !data.is_empty()
    {
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
    use crate::config::{AgentBackendConfig, AgentConfig};
    use cue_core::pipeline::{PipeSegment, Pipeline};
    use std::path::Path;
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

    fn fake_agent_config() -> Config {
        Config {
            agent: AgentConfig {
                default_backend: "fake".into(),
                backends: std::collections::BTreeMap::from([(
                    "fake".into(),
                    AgentBackendConfig {
                        command: "/bin/sh".into(),
                        args: vec![
                            "-c".into(),
                            r#"
blocked_prompt_id=
count=0
while IFS= read -r line; do
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":[[:space:]]*\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s\n' "{\"jsonrpc\":\"2.0\",\"id\":$id,\"result\":{\"protocolVersion\":1,\"agentCapabilities\":{\"loadSession\":true}}}"
      ;;
    *'"method":"session/new"'*)
      printf '%s\n' "{\"jsonrpc\":\"2.0\",\"id\":$id,\"result\":{\"sessionId\":\"sess_fake\"}}"
      ;;
    *'"method":"session/load"'*)
      session_id=$(printf '%s\n' "$line" | sed -n 's/.*"sessionId":"\([^"]*\)".*/\1/p')
      printf '%s\n' "{\"jsonrpc\":\"2.0\",\"id\":$id,\"result\":{\"sessionId\":\"$session_id\"}}"
      ;;
    *'"method":"session/cancel"'*)
      if [ -n "$blocked_prompt_id" ]; then
        printf '%s\n' "{\"jsonrpc\":\"2.0\",\"id\":$blocked_prompt_id,\"result\":{}}"
        blocked_prompt_id=
      fi
      printf '%s\n' "{\"jsonrpc\":\"2.0\",\"id\":$id,\"result\":{}}"
      ;;
    *'"method":"session/prompt"'*)
      count=$((count + 1))
      if [ "$count" -eq 1 ]; then
        printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"sess_fake","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"reply:first"}}}}'
        printf '%s\n' "{\"jsonrpc\":\"2.0\",\"id\":$id,\"result\":{}}"
      elif [ "$count" -eq 2 ]; then
        printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"sess_fake","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"reply:second"}}}}'
        printf '%s\n' "{\"jsonrpc\":\"2.0\",\"id\":$id,\"result\":{}}"
      else
        blocked_prompt_id=$id
      fi
      ;;
  esac
done
"#
                            .into(),
                        ],
                        model: None,
                    },
                )]),
            },
        }
    }

    fn fake_agent_config_without_load_session() -> Config {
        let mut config = fake_agent_config();
        let backend = config.agent.backends.get_mut("fake").expect("fake backend");
        backend.args[1] =
            backend.args[1].replace(r#"\"loadSession\":true"#, r#"\"loadSession\":false"#);
        config
    }

    async fn recv_scheduler_msg(rx: &mut mpsc::Receiver<SchedulerMsg>) -> SchedulerMsg {
        tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .expect("scheduler message timeout")
            .expect("scheduler channel closed")
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

        let agent = render_help_text(Some("agent"));
        assert!(agent.contains("AGENT mode"));
        assert!(agent.contains(":send A<n>"));

        let cron = render_help_text(Some("cron"));
        assert!(cron.contains("CRON mode"));
        assert!(cron.contains("every 5m cargo test"));
    }

    #[test]
    fn help_renderer_maps_command_aliases_to_modes() {
        assert!(render_help_text(Some("run")).contains("JOB mode"));
        assert!(render_help_text(Some("ask")).contains("AGENT mode"));
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
            schedule_text: "every 5m".into(),
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
            schedule_text: "every 1h".into(),
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
        restore_jobs(&conn, &mut state);

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
                age_secs: 0,
            },
        )
        .unwrap();
        drop(guard);

        let mut state = SchedulerState::new();
        restore_crons(&conn, &mut state);

        assert_eq!(state.next_cron, 5);
        assert!(state.crons.contains_key(&CronId(4)));
        assert_eq!(state.crons[&CronId(4)].schedule_text, "every 5m");
        assert_eq!(state.crons[&CronId(4)].status, CronStatus::Scheduled);
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
        restore_crons(&conn, &mut state);

        assert_eq!(state.crons.len(), 1);
        assert_eq!(state.crons[&CronId(1)].status, CronStatus::Expired);
        let guard = conn.lock().unwrap();
        let crons = storage::load_crons(&guard).unwrap();
        assert_eq!(crons.len(), 1);
        assert_eq!(crons[0].status, CronStatus::Expired);
    }

    #[tokio::test]
    async fn restore_agents_reloads_active_sessions_and_next_id() {
        let (sys, _gw_rx, mut sched_rx, _pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        {
            let guard = conn.lock().unwrap();
            storage::upsert_agent_history(
                &guard,
                &storage::StoredAgent {
                    id: "A4".into(),
                    backend: "fake".into(),
                    role: AgentRole::Executor,
                    status: AgentStatus::WaitingInput,
                    session_id: Some("sess_resume".into()),
                    model: None,
                    scope_hash: Some(ScopeHash([0; 32])),
                    transcript: "[assistant] already there".into(),
                    last_role: Some("assistant".into()),
                },
            )
            .unwrap();
        }

        let config = fake_agent_config();
        let mut state = SchedulerState::new();
        restore_agents(&conn, &mut state, &config, &sys).await;

        assert_eq!(state.next_agent, 5);
        let restored = state.agents.get(&AgentId(4)).expect("restored agent");
        assert_eq!(restored.session_id.as_deref(), Some("sess_resume"));
        assert_eq!(restored.status, AgentStatus::Running);
        assert!(restored.control.is_some());
        assert!(restored.transcript.contains("already there"));
        assert_eq!(restored.last_role.as_deref(), Some("assistant"));

        let mut saw_session = false;
        let mut saw_waiting = false;
        for _ in 0..4 {
            match recv_scheduler_msg(&mut sched_rx).await {
                SchedulerMsg::AgentMessage {
                    agent_id, content, ..
                } if agent_id == AgentId(4) && content.contains("ACP session: sess_resume") => {
                    saw_session = true;
                }
                SchedulerMsg::AgentStateChanged { agent_id, status }
                    if agent_id == AgentId(4) && status == AgentStatus::WaitingInput =>
                {
                    saw_waiting = true;
                    break;
                }
                _ => {}
            }
        }

        assert!(
            saw_session,
            "restored agent did not surface its ACP session"
        );
        assert!(saw_waiting, "restored agent did not settle to WaitingInput");
    }

    #[tokio::test]
    async fn restore_agents_keeps_terminal_history_visible() {
        let (sys, _gw_rx, _sched_rx, _pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        {
            let guard = conn.lock().unwrap();
            storage::upsert_agent_history(
                &guard,
                &storage::StoredAgent {
                    id: "A9".into(),
                    backend: "fake".into(),
                    role: AgentRole::Planner,
                    status: AgentStatus::Done,
                    session_id: Some("sess_done".into()),
                    model: None,
                    scope_hash: Some(ScopeHash([1; 32])),
                    transcript: "done".into(),
                    last_role: Some("assistant".into()),
                },
            )
            .unwrap();
        }

        let config = fake_agent_config();
        let mut state = SchedulerState::new();
        restore_agents(&conn, &mut state, &config, &sys).await;

        assert_eq!(state.next_agent, 10);
        let restored = state.agents.get(&AgentId(9)).expect("restored agent");
        assert_eq!(restored.status, AgentStatus::Done);
        assert!(restored.control.is_none());
        assert_eq!(restored.transcript, "done");
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

    #[tokio::test]
    async fn agent_follow_up_abort_and_kill() {
        let (sys, _gw_rx, mut sched_rx, _pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = fake_agent_config();
        let mut state = SchedulerState::new();

        let resp = handle_command(
            ResolvedCommand::Ask {
                text: "first".into(),
                params: cue_core::command::ModeParams::new(),
            },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        let agent_id = match resp {
            ResponsePayload::Ok(OkPayload::AgentSpawned { agent_id }) => agent_id,
            other => panic!("expected AgentSpawned, got {other:?}"),
        };
        let aid = parse_agent_id(&agent_id).expect("valid agent id");

        let mut saw_first_reply = false;
        let mut saw_waiting = false;
        for _ in 0..4 {
            match recv_scheduler_msg(&mut sched_rx).await {
                SchedulerMsg::AgentMessage {
                    agent_id, content, ..
                } => {
                    if agent_id == aid && content.contains("reply:first") {
                        saw_first_reply = true;
                    }
                }
                SchedulerMsg::AgentStateChanged { agent_id, status } => {
                    if agent_id == aid && status == AgentStatus::WaitingInput {
                        saw_waiting = true;
                        break;
                    }
                }
                _ => {}
            }
        }
        assert!(saw_first_reply, "agent did not stream the first reply");
        assert!(saw_waiting, "agent did not enter WaitingInput");
        state.agents.get_mut(&aid).unwrap().status = AgentStatus::WaitingInput;

        let send_resp = handle_command(
            ResolvedCommand::Send {
                id: agent_id.clone(),
                data: "second".into(),
            },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        assert!(matches!(send_resp, ResponsePayload::Ok(OkPayload::Ack {})));

        let mut saw_second_reply = false;
        let mut saw_second_waiting = false;
        for _ in 0..4 {
            match recv_scheduler_msg(&mut sched_rx).await {
                SchedulerMsg::AgentMessage {
                    agent_id, content, ..
                } => {
                    if agent_id == aid && content.contains("reply:second") {
                        saw_second_reply = true;
                    }
                }
                SchedulerMsg::AgentStateChanged { agent_id, status } => {
                    if agent_id == aid && status == AgentStatus::WaitingInput {
                        saw_second_waiting = true;
                        break;
                    }
                }
                _ => {}
            }
        }
        assert!(saw_second_reply, "agent did not stream the follow-up reply");
        assert!(saw_second_waiting, "agent did not return to WaitingInput");
        state.agents.get_mut(&aid).unwrap().status = AgentStatus::WaitingInput;

        let block_resp = handle_command(
            ResolvedCommand::Send {
                id: agent_id.clone(),
                data: "__block__".into(),
            },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        assert!(matches!(block_resp, ResponsePayload::Ok(OkPayload::Ack {})));
        state.agents.get_mut(&aid).unwrap().status = AgentStatus::Running;

        let cancel_resp = handle_command(
            ResolvedCommand::Cancel {
                id: agent_id.clone(),
            },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        assert!(matches!(
            cancel_resp,
            ResponsePayload::Ok(OkPayload::Ack {})
        ));

        let mut saw_abort_waiting = false;
        for _ in 0..4 {
            if let SchedulerMsg::AgentStateChanged { agent_id, status } =
                recv_scheduler_msg(&mut sched_rx).await
                && agent_id == aid
                && status == AgentStatus::WaitingInput
            {
                saw_abort_waiting = true;
                break;
            }
        }
        assert!(
            saw_abort_waiting,
            "agent did not return to WaitingInput after cancel"
        );
        state.agents.get_mut(&aid).unwrap().status = AgentStatus::WaitingInput;

        let kill_resp = handle_command(
            ResolvedCommand::Kill {
                id: agent_id.clone(),
            },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        assert!(matches!(kill_resp, ResponsePayload::Ok(OkPayload::Ack {})));

        let mut saw_kill_message = false;
        let mut saw_failed = false;
        for _ in 0..4 {
            match recv_scheduler_msg(&mut sched_rx).await {
                SchedulerMsg::AgentMessage {
                    agent_id, content, ..
                } => {
                    if agent_id == aid && content.contains("aborted by user") {
                        saw_kill_message = true;
                    }
                }
                SchedulerMsg::AgentStateChanged { agent_id, status } => {
                    if agent_id == aid && status == AgentStatus::Failed {
                        saw_failed = true;
                        break;
                    }
                }
                _ => {}
            }
        }
        assert!(saw_kill_message, "agent did not emit an abort message");
        assert!(saw_failed, "agent did not transition to Failed after kill");
    }

    #[tokio::test]
    async fn agent_session_override_loads_existing_session() {
        let (sys, _gw_rx, mut sched_rx, _pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = fake_agent_config();
        let mut state = SchedulerState::new();
        let mut params = cue_core::command::ModeParams::new();
        params.insert("session", ParamValue::Str("sess_resume".into()));

        let resp = handle_command(
            ResolvedCommand::Ask {
                text: "resume".into(),
                params,
            },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        let agent_id = match resp {
            ResponsePayload::Ok(OkPayload::AgentSpawned { agent_id }) => agent_id,
            other => panic!("expected AgentSpawned, got {other:?}"),
        };
        let aid = parse_agent_id(&agent_id).expect("valid agent id");

        let mut saw_session_message = false;
        let mut saw_reply = false;
        let mut saw_waiting = false;
        for _ in 0..5 {
            match recv_scheduler_msg(&mut sched_rx).await {
                SchedulerMsg::AgentMessage {
                    agent_id, content, ..
                } => {
                    if agent_id == aid && content.contains("ACP session: sess_resume") {
                        saw_session_message = true;
                    }
                    if agent_id == aid && content.contains("reply:first") {
                        saw_reply = true;
                    }
                }
                SchedulerMsg::AgentStateChanged { agent_id, status } => {
                    if agent_id == aid && status == AgentStatus::WaitingInput {
                        saw_waiting = true;
                        break;
                    }
                }
                _ => {}
            }
        }

        assert!(
            saw_session_message,
            "agent did not surface the loaded ACP session ID"
        );
        assert!(
            saw_reply,
            "agent did not continue the loaded session prompt"
        );
        assert!(saw_waiting, "agent did not settle back to WaitingInput");
    }

    #[tokio::test]
    async fn fg_accepts_agent_sessions() {
        let (sys, _gw_rx, _sched_rx, _pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = fake_agent_config();
        let mut state = SchedulerState::new();

        let resp = handle_command(
            ResolvedCommand::Ask {
                text: "first".into(),
                params: cue_core::command::ModeParams::new(),
            },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        let agent_id = match resp {
            ResponsePayload::Ok(OkPayload::AgentSpawned { agent_id }) => agent_id,
            other => panic!("expected AgentSpawned, got {other:?}"),
        };

        let fg_resp = handle_command(
            ResolvedCommand::Fg {
                id: agent_id.clone(),
            },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        assert!(matches!(
            fg_resp,
            ResponsePayload::Ok(OkPayload::FgAttached { id }) if id == agent_id
        ));
    }

    #[tokio::test]
    async fn agent_session_override_requires_load_session_capability() {
        let (sys, _gw_rx, mut sched_rx, _pm_rx, ss_rx, _eb_rx) = test_actor_system();
        spawn_fake_scope_store(ss_rx);

        let conn = test_db();
        let config = fake_agent_config_without_load_session();
        let mut state = SchedulerState::new();
        let mut params = cue_core::command::ModeParams::new();
        params.insert("session", ParamValue::Str("sess_resume".into()));

        let resp = handle_command(
            ResolvedCommand::Ask {
                text: "resume".into(),
                params,
            },
            0,
            &mut state,
            &conn,
            &config,
            &sys,
        )
        .await;
        let agent_id = match resp {
            ResponsePayload::Ok(OkPayload::AgentSpawned { agent_id }) => agent_id,
            other => panic!("expected AgentSpawned, got {other:?}"),
        };
        let aid = parse_agent_id(&agent_id).expect("valid agent id");

        let mut saw_failure_message = false;
        let mut saw_failed = false;
        for _ in 0..4 {
            match recv_scheduler_msg(&mut sched_rx).await {
                SchedulerMsg::AgentMessage {
                    agent_id, content, ..
                } => {
                    if agent_id == aid && content.contains("does not support session/load") {
                        saw_failure_message = true;
                    }
                }
                SchedulerMsg::AgentStateChanged { agent_id, status } => {
                    if agent_id == aid && status == AgentStatus::Failed {
                        saw_failed = true;
                        break;
                    }
                }
                _ => {}
            }
        }

        assert!(
            saw_failure_message,
            "agent did not explain the missing loadSession capability"
        );
        assert!(
            saw_failed,
            "agent did not fail after rejecting session/load"
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
