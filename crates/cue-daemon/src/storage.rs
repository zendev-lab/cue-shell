//! SQLite persistence layer for cued.
//!
//! Uses WAL mode for concurrent reads.  The schema is migrated on open.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, anyhow};
use cue_core::ScopeHash;
use cue_core::cron::CronStatus;
use cue_core::job::{CancelReason, JobStatus};
use cue_core::scope::Scope;
use rusqlite::Connection;

pub type SharedConnection = Arc<Mutex<Connection>>;

pub fn shared_connection(conn: Connection) -> SharedConnection {
    Arc::new(Mutex::new(conn))
}

pub async fn with_connection<T, F>(db: &SharedConnection, f: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce(&Connection) -> Result<T> + Send + 'static,
{
    let db = Arc::clone(db);
    tokio::task::spawn_blocking(move || {
        let conn = db
            .lock()
            .map_err(|error| anyhow!("lock sqlite connection: {error}"))?;
        f(&conn)
    })
    .await
    .context("join sqlite task")?
}

// ── Schema migration ──

/// Current schema version (bump when adding migrations).
const SCHEMA_VERSION: u32 = 11;

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
    cwd_override TEXT,
    scope_enabled INTEGER NOT NULL DEFAULT 0,
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

const MIGRATION_V9: &str = r"
CREATE TABLE IF NOT EXISTS script_runs (
    id            TEXT PRIMARY KEY,
    mode          TEXT NOT NULL,
    input         TEXT NOT NULL,
    status        TEXT NOT NULL,
    item_count    INTEGER NOT NULL,
    error_code    TEXT,
    error_message TEXT,
    created_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS script_items (
    script_id     TEXT NOT NULL REFERENCES script_runs(id) ON DELETE CASCADE,
    item_index    INTEGER NOT NULL,
    source_text   TEXT NOT NULL,
    kind          TEXT NOT NULL,
    target_id     TEXT,
    chain_id      TEXT,
    job_ids_json  TEXT NOT NULL DEFAULT '[]',
    PRIMARY KEY (script_id, item_index)
);
";

const MIGRATION_V10: &str = r"
ALTER TABLE crons ADD COLUMN cwd_override TEXT;
";

const MIGRATION_V11: &str = r"
ALTER TABLE crons ADD COLUMN scope_enabled INTEGER NOT NULL DEFAULT 0;
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
        conn.pragma_update(None, "user_version", 4)?;
    }
    if current < 5 {
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
    if current < 9 {
        conn.execute_batch(MIGRATION_V9)
            .context("failed to run schema migration v9")?;
        conn.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    }
    if current < 10 {
        if !column_exists(conn, "crons", "cwd_override")? {
            conn.execute_batch(MIGRATION_V10)
                .context("failed to run schema migration v10")?;
        }
        conn.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    }
    if current < 11 {
        if !column_exists(conn, "crons", "scope_enabled")? {
            conn.execute_batch(MIGRATION_V11)
                .context("failed to run schema migration v11")?;
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
    pub cwd_override: Option<PathBuf>,
    pub scope_enabled: bool,
    pub age_secs: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredScriptRun {
    pub id: String,
    pub mode: String,
    pub input: String,
    pub status: String,
    pub item_count: usize,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredScriptItem {
    pub script_id: String,
    pub item_index: usize,
    pub source_text: String,
    pub kind: String,
    pub target_id: Option<String>,
    pub chain_id: Option<String>,
    pub job_ids: Vec<String>,
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

pub fn upsert_cron(conn: &Connection, cron: &StoredCron) -> Result<()> {
    let scope_hash = cron.scope_hash.map(|hash| hash.0.to_vec());
    let status = serde_json::to_string(&cron.status).context("serialize cron status")?;
    let enabled = i64::from(cron.status.is_runnable());
    let cwd_override = cron
        .cwd_override
        .as_ref()
        .map(|path| path.to_string_lossy().into_owned());
    let scope_enabled = i64::from(cron.scope_enabled);
    conn.execute(
        "INSERT INTO crons (id, schedule, command, enabled, scope_hash, status, cwd_override, scope_enabled)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT(id) DO UPDATE SET
              schedule = excluded.schedule,
              command = excluded.command,
              enabled = excluded.enabled,
              scope_hash = excluded.scope_hash,
              status = excluded.status,
              cwd_override = excluded.cwd_override,
              scope_enabled = excluded.scope_enabled",
        rusqlite::params![
            cron.id,
            cron.schedule,
            cron.command,
            enabled,
            scope_hash,
            status,
            cwd_override,
            scope_enabled,
        ],
    )?;
    Ok(())
}

pub fn upsert_script_run(
    conn: &Connection,
    script: &StoredScriptRun,
    items: &[StoredScriptItem],
) -> Result<()> {
    conn.execute(
        "INSERT INTO script_runs (id, mode, input, status, item_count, error_code, error_message)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
         ON CONFLICT(id) DO UPDATE SET
              mode = excluded.mode,
              input = excluded.input,
              status = excluded.status,
              item_count = excluded.item_count,
              error_code = excluded.error_code,
              error_message = excluded.error_message",
        rusqlite::params![
            script.id,
            script.mode,
            script.input,
            script.status,
            script.item_count as i64,
            script.error_code,
            script.error_message,
        ],
    )?;
    conn.execute(
        "DELETE FROM script_items WHERE script_id = ?1",
        rusqlite::params![script.id],
    )?;
    for item in items {
        conn.execute(
            "INSERT INTO script_items (
                 script_id, item_index, source_text, kind, target_id, chain_id, job_ids_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![
                item.script_id,
                item.item_index as i64,
                item.source_text,
                item.kind,
                item.target_id,
                item.chain_id,
                serde_json::to_string(&item.job_ids).context("serialize script item job ids")?,
            ],
        )?;
    }
    Ok(())
}

pub fn max_script_run_id(conn: &Connection) -> Result<Option<u32>> {
    let mut stmt = conn.prepare("SELECT id FROM script_runs")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut max_id = None;
    for row in rows {
        let id = row?;
        if let Some(n) = parse_numeric_suffix(&id) {
            max_id = Some(max_id.unwrap_or(0).max(n));
        }
    }
    Ok(max_id)
}

pub fn prune_job_history(conn: &Connection, keep: usize) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT id FROM jobs_history")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut ids = Vec::new();
    for row in rows {
        ids.push(row?);
    }
    ids.sort_by_key(|id| parse_numeric_suffix(id).unwrap_or(u32::MAX));
    let drop_count = ids.len().saturating_sub(keep);
    let removed = ids.into_iter().take(drop_count).collect::<Vec<_>>();
    for id in &removed {
        conn.execute(
            "DELETE FROM jobs_history WHERE id = ?1",
            rusqlite::params![id],
        )?;
    }
    Ok(removed)
}

pub fn prune_script_runs(conn: &Connection, keep: usize) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT id FROM script_runs")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut ids = Vec::new();
    for row in rows {
        ids.push(row?);
    }
    ids.sort_by_key(|id| parse_numeric_suffix(id).unwrap_or(u32::MAX));
    let drop_count = ids.len().saturating_sub(keep);
    let removed = ids.into_iter().take(drop_count).collect::<Vec<_>>();
    for id in &removed {
        conn.execute(
            "DELETE FROM script_runs WHERE id = ?1",
            rusqlite::params![id],
        )?;
    }
    Ok(removed)
}

pub fn delete_cron(conn: &Connection, id: &str) -> Result<()> {
    conn.execute("DELETE FROM crons WHERE id = ?1", rusqlite::params![id])?;
    Ok(())
}

pub fn load_crons(conn: &Connection) -> Result<Vec<StoredCron>> {
    let mut stmt = conn.prepare(
        "SELECT id, schedule, command, enabled, scope_hash,
                COALESCE(status, CASE WHEN enabled != 0 THEN 'scheduled' ELSE 'paused' END) AS status,
                cwd_override,
                COALESCE(scope_enabled, 0) AS scope_enabled,
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
        let cwd_override: Option<String> = row.get(6)?;
        let scope_enabled: i64 = row.get(7)?;
        let age_secs: i64 = row.get(8)?;
        Ok((
            id,
            schedule,
            command,
            enabled,
            scope_blob,
            status_text,
            cwd_override,
            scope_enabled,
            age_secs,
        ))
    })?;

    let mut crons = Vec::new();
    for row in rows {
        let (
            id,
            schedule,
            command,
            enabled,
            scope_blob,
            status_text,
            cwd_override,
            scope_enabled,
            age_secs,
        ) = row?;
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
            cwd_override: cwd_override.map(PathBuf::from),
            scope_enabled: scope_enabled != 0,
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
            cwd_override: Some(PathBuf::from("/tmp/cue-cron-cwd")),
            scope_enabled: true,
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
        assert_eq!(loaded[0].cwd_override, cron.cwd_override);
        assert_eq!(loaded[0].scope_enabled, cron.scope_enabled);
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
