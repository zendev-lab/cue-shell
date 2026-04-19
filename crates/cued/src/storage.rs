//! SQLite persistence layer for cued.
//!
//! Uses WAL mode for concurrent reads.  The schema is migrated on open.

use std::path::Path;

use anyhow::{Context, Result};
use cue_core::ScopeHash;
use cue_core::agent::{AgentRole, AgentStatus};
use cue_core::cron::CronStatus;
use cue_core::job::{CancelReason, JobStatus};
use cue_core::scope::Scope;
use rusqlite::Connection;

// ── Schema migration ──

/// Current schema version (bump when adding migrations).
const SCHEMA_VERSION: u32 = 7;

const MIGRATION_V1: &str = r"
CREATE TABLE IF NOT EXISTS scopes (
    hash        BLOB PRIMARY KEY,   -- 32-byte blake3
    parent      BLOB,               -- nullable FK → scopes.hash
    delta_json  TEXT,                -- JSON-encoded EnvDelta (NULL for root)
    snap_json   TEXT                 -- JSON-encoded EnvSnapshot
);

CREATE TABLE IF NOT EXISTS scope_head (
    id   INTEGER PRIMARY KEY CHECK (id = 0),
    hash BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS crons (
    id          TEXT PRIMARY KEY,    -- e.g. 'C1'
    schedule    TEXT NOT NULL,
    command     TEXT NOT NULL,
    enabled     INTEGER NOT NULL DEFAULT 1,
    scope_hash  BLOB,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS jobs_history (
    id          TEXT PRIMARY KEY,    -- e.g. 'J1'
    pipeline    TEXT NOT NULL,
    status      TEXT NOT NULL,
    exit_code   INTEGER,
    scope_hash  BLOB,
    start_scope BLOB,
    end_scope   BLOB,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at TEXT
);

CREATE TABLE IF NOT EXISTS agents_history (
    id          TEXT PRIMARY KEY,    -- e.g. 'A1'
    kind        TEXT NOT NULL,
    role        TEXT NOT NULL,
    status      TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at TEXT
);

CREATE TABLE IF NOT EXISTS config_cache (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
";

const MIGRATION_V2: &str = r"
ALTER TABLE crons ADD COLUMN scope_hash BLOB;
";

const MIGRATION_V3: &str = r"
UPDATE jobs_history
SET start_scope = COALESCE(start_scope, scope_hash),
    end_scope = COALESCE(end_scope, scope_hash)
WHERE start_scope IS NULL OR end_scope IS NULL;
";

/// Open (or create) the database at `path`, apply WAL mode and run migrations.
pub fn open_db(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)
        .with_context(|| format!("failed to open database at {}", path.display()))?;

    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;

    migrate(&conn)?;
    Ok(conn)
}

fn migrate(conn: &Connection) -> Result<()> {
    let current: u32 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if current < 1 {
        conn.execute_batch(MIGRATION_V1)
            .context("failed to run schema migration v1")?;
        conn.pragma_update(None, "user_version", 1)?;
    }
    if current < 2 {
        if !column_exists(conn, "crons", "scope_hash")? {
            conn.execute_batch(MIGRATION_V2)
                .context("failed to run schema migration v2")?;
        }
        conn.pragma_update(None, "user_version", 2)?;
    }
    if current < 3 {
        if !column_exists(conn, "jobs_history", "start_scope")? {
            conn.execute_batch("ALTER TABLE jobs_history ADD COLUMN start_scope BLOB;")
                .context("failed to add jobs_history.start_scope")?;
        }
        if !column_exists(conn, "jobs_history", "end_scope")? {
            conn.execute_batch("ALTER TABLE jobs_history ADD COLUMN end_scope BLOB;")
                .context("failed to add jobs_history.end_scope")?;
        }
        conn.execute_batch(MIGRATION_V3)
            .context("failed to backfill jobs_history start/end scope")?;
        conn.pragma_update(None, "user_version", 3)?;
    }
    if current < 4 {
        if !column_exists(conn, "agents_history", "backend")? {
            conn.execute_batch("ALTER TABLE agents_history ADD COLUMN backend TEXT;")
                .context("failed to add agents_history.backend")?;
        }
        if !column_exists(conn, "agents_history", "session_id")? {
            conn.execute_batch("ALTER TABLE agents_history ADD COLUMN session_id TEXT;")
                .context("failed to add agents_history.session_id")?;
        }
        if !column_exists(conn, "agents_history", "model")? {
            conn.execute_batch("ALTER TABLE agents_history ADD COLUMN model TEXT;")
                .context("failed to add agents_history.model")?;
        }
        if !column_exists(conn, "agents_history", "scope_hash")? {
            conn.execute_batch("ALTER TABLE agents_history ADD COLUMN scope_hash BLOB;")
                .context("failed to add agents_history.scope_hash")?;
        }
        conn.execute_batch(
            "UPDATE agents_history
             SET backend = COALESCE(backend, kind)
             WHERE backend IS NULL OR backend = '';",
        )
        .context("failed to backfill agents_history.backend")?;
        conn.pragma_update(None, "user_version", 4)?;
    }
    if current < 5 {
        if !column_exists(conn, "agents_history", "transcript")? {
            conn.execute_batch(
                "ALTER TABLE agents_history ADD COLUMN transcript TEXT NOT NULL DEFAULT '';",
            )
            .context("failed to add agents_history.transcript")?;
        }
        if !column_exists(conn, "agents_history", "last_role")? {
            conn.execute_batch("ALTER TABLE agents_history ADD COLUMN last_role TEXT;")
                .context("failed to add agents_history.last_role")?;
        }
        conn.pragma_update(None, "user_version", 5)?;
    }
    if current < 6 {
        if !column_exists(conn, "crons", "status")? {
            conn.execute_batch("ALTER TABLE crons ADD COLUMN status TEXT;")
                .context("failed to add crons.status")?;
        }
        conn.execute_batch(
            "UPDATE crons
             SET status = CASE WHEN enabled != 0 THEN 'scheduled' ELSE 'paused' END
             WHERE status IS NULL OR status = '';",
        )
        .context("failed to backfill crons.status")?;
        conn.pragma_update(None, "user_version", 6)?;
    }
    if current < 7 {
        if !column_exists(conn, "jobs_history", "chain_id")? {
            conn.execute_batch("ALTER TABLE jobs_history ADD COLUMN chain_id TEXT;")
                .context("failed to add jobs_history.chain_id")?;
        }
        conn.pragma_update(None, "user_version", 7)?;
    }
    if current < 8 {
        if !column_exists(conn, "jobs_history", "stderr")? {
            conn.execute_batch(
                "ALTER TABLE jobs_history ADD COLUMN stderr TEXT NOT NULL DEFAULT '';",
            )
            .context("failed to add jobs_history.stderr")?;
        }
        conn.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    }
    Ok(())
}

// ── Scope CRUD ──

/// Insert a scope (cache + persistence).
pub fn insert_scope(conn: &Connection, scope: &Scope) -> Result<()> {
    let hash_bytes = scope.hash.0.as_slice();
    let parent_bytes = scope.parent.map(|p| p.0.to_vec());
    let delta_json = scope
        .delta
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    let snap_json = scope
        .snapshot
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;

    conn.execute(
        "INSERT OR IGNORE INTO scopes (hash, parent, delta_json, snap_json) VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![hash_bytes, parent_bytes, delta_json, snap_json],
    )?;
    Ok(())
}

/// Retrieve a scope by hash.
pub fn get_scope(conn: &Connection, hash: &ScopeHash) -> Result<Option<Scope>> {
    let mut stmt =
        conn.prepare("SELECT hash, parent, delta_json, snap_json FROM scopes WHERE hash = ?1")?;

    let result = stmt
        .query_row(rusqlite::params![hash.0.as_slice()], |row| {
            let hash_blob: Vec<u8> = row.get(0)?;
            let parent_blob: Option<Vec<u8>> = row.get(1)?;
            let delta_json: Option<String> = row.get(2)?;
            let snap_json: Option<String> = row.get(3)?;
            Ok((hash_blob, parent_blob, delta_json, snap_json))
        })
        .optional()?;

    let Some((hash_blob, parent_blob, delta_json, snap_json)) = result else {
        return Ok(None);
    };

    let hash = blob_to_scope_hash(&hash_blob)?;
    let parent = parent_blob.as_deref().map(blob_to_scope_hash).transpose()?;
    let delta = delta_json
        .map(|j| serde_json::from_str(&j))
        .transpose()
        .context("corrupt delta_json")?;
    let snapshot = snap_json
        .map(|j| serde_json::from_str(&j))
        .transpose()
        .context("corrupt snap_json")?;

    Ok(Some(Scope {
        hash,
        parent,
        delta,
        snapshot,
    }))
}

/// Get the current HEAD scope hash.
pub fn get_head(conn: &Connection) -> Result<Option<ScopeHash>> {
    let mut stmt = conn.prepare("SELECT hash FROM scope_head WHERE id = 0")?;
    let result = stmt
        .query_row([], |row| {
            let blob: Vec<u8> = row.get(0)?;
            Ok(blob)
        })
        .optional()?;

    match result {
        Some(blob) => Ok(Some(blob_to_scope_hash(&blob)?)),
        None => Ok(None),
    }
}

/// Set (or create) the HEAD pointer.
pub fn set_head(conn: &Connection, hash: &ScopeHash) -> Result<()> {
    conn.execute(
        "INSERT INTO scope_head (id, hash) VALUES (0, ?1)
         ON CONFLICT(id) DO UPDATE SET hash = excluded.hash",
        rusqlite::params![hash.0.as_slice()],
    )?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredJob {
    pub id: String,
    pub pipeline: String,
    pub status: JobStatus,
    pub exit_code: Option<i32>,
    pub start_scope: Option<ScopeHash>,
    pub end_scope: Option<ScopeHash>,
    pub chain_id: Option<String>,
    /// Captured stderr text.  Empty string for PTY-mode jobs (streams are merged).
    pub stderr: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredCron {
    pub id: String,
    pub schedule: String,
    pub command: String,
    pub status: CronStatus,
    pub scope_hash: Option<ScopeHash>,
    pub age_secs: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredAgent {
    pub id: String,
    pub backend: String,
    pub role: AgentRole,
    pub status: AgentStatus,
    pub session_id: Option<String>,
    pub model: Option<String>,
    pub scope_hash: Option<ScopeHash>,
    pub transcript: String,
    pub last_role: Option<String>,
}

pub fn upsert_job_history(conn: &Connection, job: &StoredJob) -> Result<()> {
    let status_json = serde_json::to_string(&job.status).context("serialize job status")?;
    let start_scope = job.start_scope.map(|hash| hash.0.to_vec());
    let end_scope = job.end_scope.map(|hash| hash.0.to_vec());
    let finished = if job.status.is_terminal() { 1 } else { 0 };

    conn.execute(
        "INSERT INTO jobs_history (
             id, pipeline, status, exit_code, scope_hash, start_scope, end_scope, chain_id, stderr, finished_at
         )
         VALUES (
             ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, CASE WHEN ?10 THEN datetime('now') ELSE NULL END
         )
         ON CONFLICT(id) DO UPDATE SET
              pipeline = excluded.pipeline,
              status = excluded.status,
              exit_code = excluded.exit_code,
              scope_hash = excluded.scope_hash,
              start_scope = excluded.start_scope,
              end_scope = excluded.end_scope,
              chain_id = excluded.chain_id,
              stderr = excluded.stderr,
              finished_at = CASE WHEN ?10 THEN datetime('now') ELSE jobs_history.finished_at END",
        rusqlite::params![
            job.id,
            job.pipeline,
            status_json,
            job.exit_code,
            start_scope.clone(),
            start_scope,
            end_scope,
            job.chain_id,
            job.stderr,
            finished,
        ],
    )?;
    Ok(())
}

pub fn load_job_history(conn: &Connection) -> Result<Vec<StoredJob>> {
    let mut stmt = conn.prepare(
        "SELECT id, pipeline, status, exit_code, start_scope, end_scope,
                COALESCE(chain_id, NULL) AS chain_id,
                COALESCE(stderr, '') AS stderr
         FROM jobs_history",
    )?;
    let rows = stmt.query_map([], |row| {
        let id: String = row.get(0)?;
        let pipeline: String = row.get(1)?;
        let status_text: String = row.get(2)?;
        let exit_code: Option<i32> = row.get(3)?;
        let start_scope_blob: Option<Vec<u8>> = row.get(4)?;
        let end_scope_blob: Option<Vec<u8>> = row.get(5)?;
        let chain_id: Option<String> = row.get(6)?;
        let stderr: String = row.get(7)?;
        Ok((
            id,
            pipeline,
            status_text,
            exit_code,
            start_scope_blob,
            end_scope_blob,
            chain_id,
            stderr,
        ))
    })?;

    let mut jobs = Vec::new();
    for row in rows {
        let (
            id,
            pipeline,
            status_text,
            exit_code,
            start_scope_blob,
            end_scope_blob,
            chain_id,
            stderr,
        ) = row?;
        jobs.push(StoredJob {
            id,
            pipeline,
            status: parse_job_status(&status_text)?,
            exit_code,
            start_scope: start_scope_blob
                .as_deref()
                .map(blob_to_scope_hash)
                .transpose()?,
            end_scope: end_scope_blob
                .as_deref()
                .map(blob_to_scope_hash)
                .transpose()?,
            chain_id,
            stderr,
        });
    }

    jobs.sort_by_key(|job| parse_numeric_suffix(&job.id).unwrap_or(u32::MAX));
    Ok(jobs)
}

pub fn upsert_agent_history(conn: &Connection, agent: &StoredAgent) -> Result<()> {
    let role_json = serde_json::to_string(&agent.role).context("serialize agent role")?;
    let status_json = serde_json::to_string(&agent.status).context("serialize agent status")?;
    let finished = if agent.status.is_terminal() { 1 } else { 0 };
    let scope_hash = agent.scope_hash.map(|hash| hash.0.to_vec());

    conn.execute(
        "INSERT INTO agents_history (
             id, kind, backend, role, status, session_id, model, scope_hash, transcript, last_role, finished_at
         )
         VALUES (
             ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
             CASE WHEN ?11 THEN datetime('now') ELSE NULL END
         )
         ON CONFLICT(id) DO UPDATE SET
             kind = excluded.kind,
             backend = excluded.backend,
             role = excluded.role,
             status = excluded.status,
             session_id = excluded.session_id,
             model = excluded.model,
             scope_hash = excluded.scope_hash,
             transcript = excluded.transcript,
             last_role = excluded.last_role,
             finished_at = CASE WHEN ?11 THEN datetime('now') ELSE agents_history.finished_at END",
        rusqlite::params![
            agent.id,
            agent.backend,
            agent.backend,
            role_json,
            status_json,
            agent.session_id.as_deref(),
            agent.model.as_deref(),
            scope_hash,
            agent.transcript.as_str(),
            agent.last_role.as_deref(),
            finished,
        ],
    )?;
    Ok(())
}

pub fn load_agent_history(conn: &Connection) -> Result<Vec<StoredAgent>> {
    let mut stmt = conn.prepare(
        "SELECT id,
                COALESCE(backend, kind) AS backend,
                role,
                status,
                session_id,
                model,
                scope_hash,
                COALESCE(transcript, '') AS transcript,
                last_role
         FROM agents_history",
    )?;
    let rows = stmt.query_map([], |row| {
        let id: String = row.get(0)?;
        let backend: String = row.get(1)?;
        let role_text: String = row.get(2)?;
        let status_text: String = row.get(3)?;
        let session_id: Option<String> = row.get(4)?;
        let model: Option<String> = row.get(5)?;
        let scope_hash_blob: Option<Vec<u8>> = row.get(6)?;
        let transcript: String = row.get(7)?;
        let last_role: Option<String> = row.get(8)?;
        Ok((
            id,
            backend,
            role_text,
            status_text,
            session_id,
            model,
            scope_hash_blob,
            transcript,
            last_role,
        ))
    })?;

    let mut agents = Vec::new();
    for row in rows {
        let (
            id,
            backend,
            role_text,
            status_text,
            session_id,
            model,
            scope_hash_blob,
            transcript,
            last_role,
        ) = row?;
        agents.push(StoredAgent {
            id,
            backend,
            role: parse_agent_role(&role_text)?,
            status: parse_agent_status(&status_text)?,
            session_id,
            model,
            scope_hash: scope_hash_blob
                .as_deref()
                .map(blob_to_scope_hash)
                .transpose()?,
            transcript,
            last_role,
        });
    }

    agents.sort_by_key(|agent| parse_numeric_suffix(&agent.id).unwrap_or(u32::MAX));
    Ok(agents)
}

pub fn upsert_cron(conn: &Connection, cron: &StoredCron) -> Result<()> {
    let scope_hash = cron.scope_hash.map(|hash| hash.0.to_vec());
    let status = serde_json::to_string(&cron.status).context("serialize cron status")?;
    let enabled = i64::from(cron.status.is_runnable());
    conn.execute(
        "INSERT INTO crons (id, schedule, command, enabled, scope_hash, status)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(id) DO UPDATE SET
              schedule = excluded.schedule,
              command = excluded.command,
              enabled = excluded.enabled,
              scope_hash = excluded.scope_hash,
              status = excluded.status",
        rusqlite::params![
            cron.id,
            cron.schedule,
            cron.command,
            enabled,
            scope_hash,
            status,
        ],
    )?;
    Ok(())
}

pub fn delete_cron(conn: &Connection, id: &str) -> Result<()> {
    conn.execute("DELETE FROM crons WHERE id = ?1", rusqlite::params![id])?;
    Ok(())
}

pub fn load_crons(conn: &Connection) -> Result<Vec<StoredCron>> {
    let mut stmt = conn.prepare(
        "SELECT id, schedule, command, enabled, scope_hash,
                COALESCE(status, CASE WHEN enabled != 0 THEN 'scheduled' ELSE 'paused' END) AS status,
                CAST((julianday('now') - julianday(created_at)) * 86400 AS INTEGER) AS age_secs
          FROM crons",
    )?;
    let rows = stmt.query_map([], |row| {
        let id: String = row.get(0)?;
        let schedule: String = row.get(1)?;
        let command: String = row.get(2)?;
        let enabled: i64 = row.get(3)?;
        let scope_blob: Option<Vec<u8>> = row.get(4)?;
        let status_text: String = row.get(5)?;
        let age_secs: i64 = row.get(6)?;
        Ok((
            id,
            schedule,
            command,
            enabled,
            scope_blob,
            status_text,
            age_secs,
        ))
    })?;

    let mut crons = Vec::new();
    for row in rows {
        let (id, schedule, command, enabled, scope_blob, status_text, age_secs) = row?;
        let status = match parse_cron_status(&status_text) {
            Ok(status) => status,
            Err(_) => {
                if enabled != 0 {
                    CronStatus::Scheduled
                } else {
                    CronStatus::Paused
                }
            }
        };
        crons.push(StoredCron {
            id,
            schedule,
            command,
            status,
            scope_hash: scope_blob.as_deref().map(blob_to_scope_hash).transpose()?,
            age_secs,
        });
    }

    crons.sort_by_key(|cron| parse_numeric_suffix(&cron.id).unwrap_or(u32::MAX));
    Ok(crons)
}

// ── Helpers ──

fn parse_job_status(text: &str) -> Result<JobStatus> {
    if let Ok(status) = serde_json::from_str(text) {
        return Ok(status);
    }

    let legacy = match text {
        "pending" => JobStatus::Pending,
        "running" => JobStatus::Running,
        "done" => JobStatus::Done,
        "failed" => JobStatus::Failed,
        "killed" => JobStatus::Killed,
        "cancelled" => JobStatus::Cancelled(CancelReason::User),
        _ => {
            return Err(anyhow::anyhow!("unknown job status encoding: {text}"));
        }
    };
    Ok(legacy)
}

fn parse_cron_status(text: &str) -> Result<CronStatus> {
    if let Ok(status) = serde_json::from_str(text) {
        return Ok(status);
    }
    match text.trim_matches('"') {
        "scheduled" | "enabled" => Ok(CronStatus::Scheduled),
        "paused" | "disabled" => Ok(CronStatus::Paused),
        "completed" | "done" => Ok(CronStatus::Completed),
        "expired" => Ok(CronStatus::Expired),
        other => anyhow::bail!("unknown cron status {other:?}"),
    }
}

fn parse_agent_status(text: &str) -> Result<AgentStatus> {
    if let Ok(status) = serde_json::from_str(text) {
        return Ok(status);
    }

    let legacy = match text {
        "running" => AgentStatus::Running,
        "waiting" | "waiting_input" => AgentStatus::WaitingInput,
        "done" => AgentStatus::Done,
        "failed" => AgentStatus::Failed,
        _ => {
            return Err(anyhow::anyhow!("unknown agent status encoding: {text}"));
        }
    };
    Ok(legacy)
}

fn parse_agent_role(text: &str) -> Result<AgentRole> {
    if let Ok(role) = serde_json::from_str(text) {
        return Ok(role);
    }

    let legacy = match text {
        "planner" => AgentRole::Planner,
        "executor" => AgentRole::Executor,
        _ => return Err(anyhow::anyhow!("unknown agent role encoding: {text}")),
    };
    Ok(legacy)
}

fn parse_numeric_suffix(id: &str) -> Option<u32> {
    let digits = id.trim_start_matches(|ch: char| ch.is_ascii_alphabetic());
    digits.parse().ok()
}

fn column_exists(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    let query = format!("PRAGMA table_info({table})");
    let mut stmt = conn.prepare(&query)?;
    let columns = stmt.query_map([], |row| row.get::<_, String>(1))?;
    for name in columns {
        if name? == column {
            return Ok(true);
        }
    }
    Ok(false)
}

fn blob_to_scope_hash(blob: &[u8]) -> Result<ScopeHash> {
    let arr: [u8; 32] = blob
        .try_into()
        .map_err(|_| anyhow::anyhow!("scope hash blob is not 32 bytes (got {})", blob.len()))?;
    Ok(ScopeHash(arr))
}

/// Extension trait on `rusqlite::Statement` to get optional results.
trait OptionalExt<T> {
    fn optional(self) -> rusqlite::Result<Option<T>>;
}

impl<T> OptionalExt<T> for rusqlite::Result<T> {
    fn optional(self) -> rusqlite::Result<Option<T>> {
        match self {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use cue_core::scope::EnvSnapshot;

    use super::*;

    fn in_memory_db() -> Connection {
        open_db(Path::new(":memory:")).expect("open in-memory db")
    }

    #[test]
    fn migration_is_idempotent() {
        let conn = in_memory_db();
        // Running migrate again should be a no-op.
        migrate(&conn).expect("second migration");
    }

    #[test]
    fn scope_roundtrip() {
        let conn = in_memory_db();
        let snap = EnvSnapshot {
            env: BTreeMap::from([("PATH".into(), "/usr/bin".into())]),
            cwd: PathBuf::from("/tmp"),
        };
        let scope = Scope::root(snap);
        insert_scope(&conn, &scope).unwrap();
        let loaded = get_scope(&conn, &scope.hash)
            .unwrap()
            .expect("scope exists");
        assert_eq!(loaded.hash, scope.hash);
        assert!(loaded.parent.is_none());
        assert!(loaded.snapshot.is_some());
    }

    #[test]
    fn head_roundtrip() {
        let conn = in_memory_db();
        assert!(get_head(&conn).unwrap().is_none());
        let hash = ScopeHash([42; 32]);
        set_head(&conn, &hash).unwrap();
        assert_eq!(get_head(&conn).unwrap(), Some(hash));

        // Update head.
        let hash2 = ScopeHash([99; 32]);
        set_head(&conn, &hash2).unwrap();
        assert_eq!(get_head(&conn).unwrap(), Some(hash2));
    }

    #[test]
    fn job_history_roundtrip() {
        let conn = in_memory_db();
        let start_scope = ScopeHash([7; 32]);
        let end_scope = ScopeHash([8; 32]);
        let job = StoredJob {
            id: "J12".into(),
            pipeline: "cargo test".into(),
            status: JobStatus::Cancelled(CancelReason::User),
            exit_code: Some(130),
            start_scope: Some(start_scope),
            end_scope: Some(end_scope),
            chain_id: None,
            stderr: String::new(),
        };

        upsert_job_history(&conn, &job).unwrap();
        let loaded = load_job_history(&conn).unwrap();

        assert_eq!(loaded, vec![job]);
    }

    #[test]
    fn cron_roundtrip() {
        let conn = in_memory_db();
        let cron = StoredCron {
            id: "C3".into(),
            schedule: "every 5m".into(),
            command: "cargo test".into(),
            status: CronStatus::Scheduled,
            scope_hash: Some(ScopeHash([9; 32])),
            age_secs: 0,
        };

        upsert_cron(&conn, &cron).unwrap();
        let loaded = load_crons(&conn).unwrap();

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, cron.id);
        assert_eq!(loaded[0].schedule, cron.schedule);
        assert_eq!(loaded[0].command, cron.command);
        assert_eq!(loaded[0].status, cron.status);
        assert_eq!(loaded[0].scope_hash, cron.scope_hash);
    }

    #[test]
    fn agent_history_roundtrip() {
        let conn = in_memory_db();
        let agent = StoredAgent {
            id: "A7".into(),
            backend: "copilot".into(),
            role: AgentRole::Executor,
            status: AgentStatus::WaitingInput,
            session_id: Some("sess_123".into()),
            model: Some("gpt-5.4".into()),
            scope_hash: Some(ScopeHash([11; 32])),
            transcript: "[system] ACP session: sess_123\n\nhello".into(),
            last_role: Some("assistant".into()),
        };

        upsert_agent_history(&conn, &agent).unwrap();
        let loaded = load_agent_history(&conn).unwrap();

        assert_eq!(loaded, vec![agent]);
    }

    #[test]
    fn job_stderr_persistence_roundtrip() {
        let conn = in_memory_db();
        let job = StoredJob {
            id: "J3".into(),
            pipeline: "echo oops 1>&2".into(),
            status: cue_core::job::JobStatus::Failed,
            exit_code: Some(1),
            start_scope: None,
            end_scope: None,
            chain_id: None,
            stderr: "error: something went wrong\n".into(),
        };

        upsert_job_history(&conn, &job).unwrap();
        let loaded = load_job_history(&conn).unwrap();

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, "J3");
        assert_eq!(loaded[0].stderr, "error: something went wrong\n");
    }

    #[test]
    fn job_stderr_defaults_to_empty_on_old_rows() {
        // Simulate an existing row that pre-dates the stderr column (DEFAULT '').
        let conn = in_memory_db();
        // Insert without specifying stderr (rely on DEFAULT).
        conn.execute(
            "INSERT INTO jobs_history (id, pipeline, status) VALUES ('J1', 'echo hi', '\"Done\"')",
            [],
        )
        .unwrap();

        let loaded = load_job_history(&conn).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].stderr, "");
    }
}
