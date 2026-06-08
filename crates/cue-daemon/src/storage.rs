//! SQLite persistence layer for cued.
//!
//! Uses WAL mode for concurrent reads.  The schema is migrated on open.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use cue_core::cron::CronStatus;
use cue_core::job::JobStatus;
use cue_core::scope::Scope;
use cue_core::{CronId, JobId, ScopeHash, ScriptId};
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
const SCHEMA_VERSION: u32 = 14;

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
    wrapper_enabled INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    created_at_ms INTEGER
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

const MIGRATION_V12: &str = r"
ALTER TABLE crons ADD COLUMN wrapper_enabled INTEGER NOT NULL DEFAULT 0;
";

const CRON_CREATED_AT_MS_EXPR: &str = "CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)";

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
    let mut current: u32 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if current > SCHEMA_VERSION {
        return Err(anyhow!(
            "database schema version {current} is newer than supported version {SCHEMA_VERSION}"
        ));
    }
    if current < 1 {
        conn.execute_batch(MIGRATION_V1)
            .context("failed to run schema migration v1")?;
        set_schema_version(conn, &mut current, 1)?;
    }
    if current < 2 {
        if !column_exists(conn, "crons", "scope_hash")? {
            conn.execute_batch(MIGRATION_V2)
                .context("failed to run schema migration v2")?;
        }
        set_schema_version(conn, &mut current, 2)?;
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
        set_schema_version(conn, &mut current, 3)?;
    }
    if current < 4 {
        set_schema_version(conn, &mut current, 4)?;
    }
    if current < 5 {
        set_schema_version(conn, &mut current, 5)?;
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
        set_schema_version(conn, &mut current, 6)?;
    }
    if current < 7 {
        if !column_exists(conn, "jobs_history", "chain_id")? {
            conn.execute_batch("ALTER TABLE jobs_history ADD COLUMN chain_id TEXT;")
                .context("failed to add jobs_history.chain_id")?;
        }
        set_schema_version(conn, &mut current, 7)?;
    }
    if current < 8 {
        if !column_exists(conn, "jobs_history", "stderr")? {
            conn.execute_batch(
                "ALTER TABLE jobs_history ADD COLUMN stderr TEXT NOT NULL DEFAULT '';",
            )
            .context("failed to add jobs_history.stderr")?;
        }
        set_schema_version(conn, &mut current, 8)?;
    }
    if current < 9 {
        conn.execute_batch(MIGRATION_V9)
            .context("failed to run schema migration v9")?;
        set_schema_version(conn, &mut current, 9)?;
    }
    if current < 10 {
        if !column_exists(conn, "crons", "cwd_override")? {
            conn.execute_batch(MIGRATION_V10)
                .context("failed to run schema migration v10")?;
        }
        set_schema_version(conn, &mut current, 10)?;
    }
    if current < 11 {
        if !column_exists(conn, "crons", "scope_enabled")? {
            conn.execute_batch(MIGRATION_V11)
                .context("failed to run schema migration v11")?;
        }
        set_schema_version(conn, &mut current, 11)?;
    }
    if current < 12 {
        if !column_exists(conn, "crons", "wrapper_enabled")? {
            conn.execute_batch(MIGRATION_V12)
                .context("failed to run schema migration v12")?;
        }
        set_schema_version(conn, &mut current, 12)?;
    }
    if current < 13 {
        if !column_exists(conn, "script_runs", "exit_code")? {
            conn.execute_batch("ALTER TABLE script_runs ADD COLUMN exit_code INTEGER;")
                .context("failed to add script_runs.exit_code")?;
        }
        if !column_exists(conn, "script_runs", "failed_item_index")? {
            conn.execute_batch("ALTER TABLE script_runs ADD COLUMN failed_item_index INTEGER;")
                .context("failed to add script_runs.failed_item_index")?;
        }
        if !column_exists(conn, "script_runs", "finished_at")? {
            conn.execute_batch("ALTER TABLE script_runs ADD COLUMN finished_at TEXT;")
                .context("failed to add script_runs.finished_at")?;
        }
        set_schema_version(conn, &mut current, 13)?;
    }
    if current < 14 {
        if !column_exists(conn, "crons", "created_at_ms")? {
            conn.execute_batch("ALTER TABLE crons ADD COLUMN created_at_ms INTEGER;")
                .context("failed to add crons.created_at_ms")?;
        }
        conn.execute_batch(&format!(
            "UPDATE crons
             SET created_at_ms = {CRON_CREATED_AT_MS_EXPR}
             WHERE created_at_ms IS NULL;"
        ))
        .context("failed to initialize crons.created_at_ms")?;
        set_schema_version(conn, &mut current, 14)?;
    }
    Ok(())
}

fn set_schema_version(conn: &Connection, current: &mut u32, version: u32) -> Result<()> {
    conn.pragma_update(None, "user_version", version)?;
    *current = version;
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

    Ok(Some(scope_from_row_parts(
        hash_blob,
        parent_blob,
        delta_json,
        snap_json,
    )?))
}

pub fn list_scopes(conn: &Connection) -> Result<Vec<Scope>> {
    let mut stmt = conn.prepare("SELECT hash, parent, delta_json, snap_json FROM scopes")?;
    let rows = stmt.query_map([], |row| {
        let hash_blob: Vec<u8> = row.get(0)?;
        let parent_blob: Option<Vec<u8>> = row.get(1)?;
        let delta_json: Option<String> = row.get(2)?;
        let snap_json: Option<String> = row.get(3)?;
        Ok((hash_blob, parent_blob, delta_json, snap_json))
    })?;

    let mut scopes = Vec::new();
    for row in rows {
        let (hash_blob, parent_blob, delta_json, snap_json) = row?;
        scopes.push(scope_from_row_parts(
            hash_blob,
            parent_blob,
            delta_json,
            snap_json,
        )?);
    }
    Ok(scopes)
}

fn scope_from_row_parts(
    hash_blob: Vec<u8>,
    parent_blob: Option<Vec<u8>>,
    delta_json: Option<String>,
    snap_json: Option<String>,
) -> Result<Scope> {
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

    Ok(Scope {
        hash,
        parent,
        delta,
        snapshot,
    })
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
    pub wrapper_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedCron {
    pub record: StoredCron,
    pub elapsed: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredScriptRun {
    pub id: String,
    pub mode: String,
    pub input: String,
    pub status: StoredScriptRunStatus,
    pub item_count: usize,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub exit_code: Option<i32>,
    pub failed_item_index: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoredScriptRunStatus {
    Submitted,
    PartialError,
    Done,
    Failed,
}

impl StoredScriptRunStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Submitted => "submitted",
            Self::PartialError => "partial_error",
            Self::Done => "done",
            Self::Failed => "failed",
        }
    }

    fn is_terminal(self) -> bool {
        matches!(self, Self::Done | Self::Failed)
    }
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
        let n = parse_job_history_id(&id)?;
        jobs.push((
            n,
            StoredJob {
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
            },
        ));
    }

    jobs.sort_by_key(|(n, _)| *n);
    Ok(jobs.into_iter().map(|(_, job)| job).collect())
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
    let wrapper_enabled = i64::from(cron.wrapper_enabled);
    conn.execute(
        &format!(
            "INSERT INTO crons (id, schedule, command, enabled, scope_hash, status, cwd_override, scope_enabled, wrapper_enabled, created_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, {CRON_CREATED_AT_MS_EXPR})
         ON CONFLICT(id) DO UPDATE SET
              schedule = excluded.schedule,
              command = excluded.command,
              enabled = excluded.enabled,
              scope_hash = excluded.scope_hash,
              status = excluded.status,
              cwd_override = excluded.cwd_override,
              scope_enabled = excluded.scope_enabled,
              wrapper_enabled = excluded.wrapper_enabled"
        ),
        rusqlite::params![
            cron.id,
            cron.schedule,
            cron.command,
            enabled,
            scope_hash,
            status,
            cwd_override,
            scope_enabled,
            wrapper_enabled,
        ],
    )?;
    Ok(())
}

pub fn upsert_script_run(
    conn: &Connection,
    script: &StoredScriptRun,
    items: &[StoredScriptItem],
) -> Result<()> {
    let tx = conn
        .unchecked_transaction()
        .context("begin script run upsert transaction")?;
    let status = script.status.as_str();
    let failed_item_index = script.failed_item_index.map(|index| index as i64);
    let finished = script.status.is_terminal();
    tx.execute(
        "INSERT INTO script_runs (
             id, mode, input, status, item_count, error_code, error_message,
             exit_code, failed_item_index, finished_at
         )
         VALUES (
             ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9,
             CASE WHEN ?10 THEN datetime('now') ELSE NULL END
         )
         ON CONFLICT(id) DO UPDATE SET
              mode = excluded.mode,
              input = excluded.input,
              status = excluded.status,
              item_count = excluded.item_count,
              error_code = excluded.error_code,
              error_message = excluded.error_message,
              exit_code = excluded.exit_code,
              failed_item_index = excluded.failed_item_index,
              finished_at = CASE WHEN ?10 THEN datetime('now') ELSE NULL END",
        rusqlite::params![
            script.id,
            script.mode,
            script.input,
            status,
            script.item_count as i64,
            script.error_code,
            script.error_message,
            script.exit_code,
            failed_item_index,
            finished,
        ],
    )
    .context("upsert script run")?;
    tx.execute(
        "DELETE FROM script_items WHERE script_id = ?1",
        rusqlite::params![script.id],
    )
    .context("delete existing script items")?;
    for item in items {
        tx.execute(
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
        )
        .context("insert script item")?;
    }
    tx.commit().context("commit script run upsert")
}

pub fn max_script_run_id(conn: &Connection) -> Result<Option<u32>> {
    let mut stmt = conn.prepare("SELECT id FROM script_runs")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut max_id = None;
    for row in rows {
        let id = row?;
        let n = parse_script_run_id(&id)?;
        max_id = Some(max_id.unwrap_or(0).max(n));
    }
    Ok(max_id)
}

pub fn prune_job_history(conn: &Connection, keep: usize) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT id, status FROM jobs_history")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut ids = Vec::new();
    for row in rows {
        let (id, status_text) = row?;
        let n = parse_job_history_id(&id)?;
        let status = parse_job_status(&status_text)?;
        ids.push((n, id, status.is_terminal()));
    }
    ids.sort_by_key(|(n, _, _)| *n);
    let terminal_count = ids
        .iter()
        .filter(|(_, _, is_terminal)| *is_terminal)
        .count();
    let drop_count = terminal_count.saturating_sub(keep);
    let removed = ids
        .into_iter()
        .filter(|(_, _, is_terminal)| *is_terminal)
        .take(drop_count)
        .map(|(_, id, _)| id)
        .collect::<Vec<_>>();
    for id in &removed {
        conn.execute(
            "DELETE FROM jobs_history WHERE id = ?1",
            rusqlite::params![id],
        )?;
    }
    Ok(removed)
}

pub fn prune_script_runs(conn: &Connection, keep: usize) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT id, status FROM script_runs")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut ids = Vec::new();
    for row in rows {
        let (id, status_text) = row?;
        let n = parse_script_run_id(&id)?;
        let status = parse_script_run_status(&status_text)?;
        ids.push((n, id, status.is_terminal()));
    }
    ids.sort_by_key(|(n, _, _)| *n);
    let terminal_count = ids
        .iter()
        .filter(|(_, _, is_terminal)| *is_terminal)
        .count();
    let drop_count = terminal_count.saturating_sub(keep);
    let removed = ids
        .into_iter()
        .filter(|(_, _, is_terminal)| *is_terminal)
        .take(drop_count)
        .map(|(_, id, _)| id)
        .collect::<Vec<_>>();
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

pub fn load_crons(conn: &Connection) -> Result<Vec<LoadedCron>> {
    let mut stmt = conn.prepare(&format!(
        "WITH now_ms(value) AS (SELECT {CRON_CREATED_AT_MS_EXPR})
         SELECT id, schedule, command, scope_hash,
                status,
                cwd_override,
                scope_enabled,
                wrapper_enabled,
                now_ms.value - created_at_ms AS age_millis
          FROM crons, now_ms"
    ))?;
    let rows = stmt.query_map([], |row| {
        let id: String = row.get(0)?;
        let schedule: String = row.get(1)?;
        let command: String = row.get(2)?;
        let scope_blob: Option<Vec<u8>> = row.get(3)?;
        let status_text: String = row.get(4)?;
        let cwd_override: Option<String> = row.get(5)?;
        let scope_enabled: i64 = row.get(6)?;
        let wrapper_enabled: i64 = row.get(7)?;
        let age_millis: i64 = row.get(8)?;
        Ok((
            id,
            schedule,
            command,
            scope_blob,
            status_text,
            cwd_override,
            scope_enabled,
            wrapper_enabled,
            age_millis,
        ))
    })?;

    let mut crons = Vec::new();
    for row in rows {
        let (
            id,
            schedule,
            command,
            scope_blob,
            status_text,
            cwd_override,
            scope_enabled,
            wrapper_enabled,
            age_millis,
        ) = row?;
        let n = parse_cron_id(&id)?;
        let status =
            parse_cron_status(&status_text).with_context(|| format!("parse cron {id} status"))?;
        crons.push((
            n,
            LoadedCron {
                record: StoredCron {
                    id,
                    schedule,
                    command,
                    status,
                    scope_hash: scope_blob.as_deref().map(blob_to_scope_hash).transpose()?,
                    cwd_override: cwd_override.map(PathBuf::from),
                    scope_enabled: scope_enabled != 0,
                    wrapper_enabled: wrapper_enabled != 0,
                },
                elapsed: duration_from_nonnegative_millis(age_millis)
                    .context("load cron elapsed age")?,
            },
        ));
    }

    crons.sort_by_key(|(n, _)| *n);
    Ok(crons.into_iter().map(|(_, cron)| cron).collect())
}

// ── Helpers ──

fn duration_from_nonnegative_millis(millis: i64) -> Result<Duration> {
    let millis = u64::try_from(millis).context("cron created_at is in the future")?;
    Ok(Duration::from_millis(millis))
}

fn parse_job_status(text: &str) -> Result<JobStatus> {
    serde_json::from_str(text).with_context(|| format!("unknown job status encoding: {text}"))
}

fn parse_script_run_status(text: &str) -> Result<StoredScriptRunStatus> {
    match text.trim_matches('"') {
        "submitted" => Ok(StoredScriptRunStatus::Submitted),
        "partial_error" => Ok(StoredScriptRunStatus::PartialError),
        "done" => Ok(StoredScriptRunStatus::Done),
        "failed" => Ok(StoredScriptRunStatus::Failed),
        other => anyhow::bail!("unknown script run status {other:?}"),
    }
}

fn parse_cron_status(text: &str) -> Result<CronStatus> {
    serde_json::from_str(text).with_context(|| format!("unknown cron status encoding: {text}"))
}

fn parse_cron_id(id: &str) -> Result<u32> {
    id.parse::<CronId>()
        .map(|id| id.0)
        .with_context(|| format!("invalid cron id {id}"))
}

fn parse_job_history_id(id: &str) -> Result<u32> {
    id.parse::<JobId>()
        .map(|id| id.0)
        .with_context(|| format!("invalid job history id {id}"))
}

fn parse_script_run_id(id: &str) -> Result<u32> {
    id.parse::<ScriptId>()
        .map(|id| id.0)
        .with_context(|| format!("invalid script run id {id}"))
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

    use cue_core::job::CancelReason;
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
    fn failed_later_migration_preserves_last_successful_version() {
        let conn = Connection::open_in_memory().expect("open in-memory db");
        conn.execute_batch(
            "
            CREATE VIEW crons AS
            SELECT
                'C1' AS id,
                'every 5m' AS schedule,
                'echo hi' AS command,
                1 AS enabled,
                NULL AS scope_hash,
                datetime('now') AS created_at;
            CREATE TABLE jobs_history (
                id          TEXT PRIMARY KEY,
                pipeline    TEXT NOT NULL,
                status      TEXT NOT NULL,
                exit_code   INTEGER,
                scope_hash  BLOB,
                start_scope BLOB,
                end_scope   BLOB,
                chain_id    TEXT,
                stderr      TEXT NOT NULL DEFAULT '',
                created_at  TEXT NOT NULL DEFAULT (datetime('now')),
                finished_at TEXT
            );
            PRAGMA user_version = 8;
            ",
        )
        .expect("seed broken v8 database");

        let error = migrate(&conn).expect_err("v10 must fail against a crons view");
        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("read user_version");

        assert!(
            error
                .to_string()
                .contains("failed to run schema migration v10")
        );
        assert_eq!(version, 9);
    }

    #[test]
    fn migration_rejects_newer_schema_version() {
        let conn = Connection::open_in_memory().expect("open in-memory db");
        conn.pragma_update(None, "user_version", SCHEMA_VERSION + 1)
            .expect("set future schema version");

        let error = migrate(&conn).expect_err("future schema must be rejected");

        assert!(error.to_string().contains("newer than supported version"));
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
    fn list_scopes_returns_persisted_scopes() {
        let conn = in_memory_db();
        let root = Scope::root(EnvSnapshot {
            env: BTreeMap::from([("PATH".into(), "/usr/bin".into())]),
            cwd: PathBuf::from("/tmp/root"),
        });
        let child = Scope::fork(
            root.hash,
            root.snapshot.as_ref().expect("root snapshot"),
            cue_core::scope::EnvDelta {
                set: BTreeMap::from([("FOO".into(), "bar".into())]),
                unset: vec![],
                cwd: Some(PathBuf::from("/tmp/child")),
            },
        );
        insert_scope(&conn, &root).unwrap();
        insert_scope(&conn, &child).unwrap();

        let scopes = list_scopes(&conn).unwrap();
        let hashes = scopes.iter().map(|scope| scope.hash).collect::<Vec<_>>();

        assert!(hashes.contains(&root.hash));
        assert!(hashes.contains(&child.hash));
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
    fn load_job_history_rejects_invalid_ids() {
        let conn = in_memory_db();
        conn.execute(
            "INSERT INTO jobs_history (id, pipeline, status)
             VALUES ('not-a-job', 'echo bad', '\"Done\"')",
            [],
        )
        .unwrap();

        let error = load_job_history(&conn).unwrap_err();

        assert!(error.to_string().contains("invalid job history id"));
    }

    #[test]
    fn prune_job_history_rejects_invalid_ids_without_deleting_valid_rows() {
        let conn = in_memory_db();
        conn.execute(
            "INSERT INTO jobs_history (id, pipeline, status)
             VALUES ('J1', 'echo ok', '\"Done\"')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO jobs_history (id, pipeline, status)
             VALUES ('bad-job', 'echo bad', '\"Done\"')",
            [],
        )
        .unwrap();

        let error = prune_job_history(&conn, 0).unwrap_err();

        assert!(error.to_string().contains("invalid job history id"));
        let valid_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM jobs_history WHERE id = 'J1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(valid_count, 1);
    }

    #[test]
    fn prune_job_history_keeps_active_jobs_when_over_limit() {
        let conn = in_memory_db();
        for job in [
            StoredJob {
                id: "J1".into(),
                pipeline: "sleep 60".into(),
                status: JobStatus::Running,
                exit_code: None,
                start_scope: None,
                end_scope: None,
                chain_id: None,
                stderr: String::new(),
            },
            StoredJob {
                id: "J2".into(),
                pipeline: "echo old".into(),
                status: JobStatus::Done,
                exit_code: Some(0),
                start_scope: None,
                end_scope: None,
                chain_id: None,
                stderr: String::new(),
            },
            StoredJob {
                id: "J3".into(),
                pipeline: "echo latest".into(),
                status: JobStatus::Done,
                exit_code: Some(0),
                start_scope: None,
                end_scope: None,
                chain_id: None,
                stderr: String::new(),
            },
        ] {
            upsert_job_history(&conn, &job).unwrap();
        }

        let removed = prune_job_history(&conn, 1).unwrap();

        assert_eq!(removed, vec!["J2"]);
        let remaining = load_job_history(&conn)
            .unwrap()
            .into_iter()
            .map(|job| (job.id, job.status))
            .collect::<Vec<_>>();
        assert_eq!(
            remaining,
            vec![
                ("J1".into(), JobStatus::Running),
                ("J3".into(), JobStatus::Done)
            ]
        );
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
            wrapper_enabled: true,
        };

        upsert_cron(&conn, &cron).unwrap();
        let loaded = load_crons(&conn).unwrap();

        assert_eq!(loaded.len(), 1);
        let loaded = &loaded[0].record;
        assert_eq!(loaded.id, cron.id);
        assert_eq!(loaded.schedule, cron.schedule);
        assert_eq!(loaded.command, cron.command);
        assert_eq!(loaded.status, cron.status);
        assert_eq!(loaded.scope_hash, cron.scope_hash);
        assert_eq!(loaded.cwd_override, cron.cwd_override);
        assert_eq!(loaded.scope_enabled, cron.scope_enabled);
        assert_eq!(loaded.wrapper_enabled, cron.wrapper_enabled);
    }

    #[test]
    fn failed_cron_status_roundtrips() {
        let conn = in_memory_db();
        let cron = StoredCron {
            id: "C4".into(),
            schedule: "in 1s".into(),
            command: "echo due".into(),
            status: CronStatus::Failed,
            scope_hash: Some(ScopeHash([4; 32])),
            cwd_override: None,
            scope_enabled: false,
            wrapper_enabled: false,
        };

        upsert_cron(&conn, &cron).unwrap();
        let loaded = load_crons(&conn).unwrap();

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].record.status, CronStatus::Failed);
    }

    #[test]
    fn cron_load_preserves_millisecond_age() {
        let conn = in_memory_db();
        let cron = StoredCron {
            id: "C7".into(),
            schedule: "in 1500ms".into(),
            command: "echo soon".into(),
            status: CronStatus::Scheduled,
            scope_hash: Some(ScopeHash([7; 32])),
            cwd_override: None,
            scope_enabled: false,
            wrapper_enabled: false,
        };
        upsert_cron(&conn, &cron).unwrap();
        conn.execute(
            &format!(
                "UPDATE crons
                 SET created_at_ms = {CRON_CREATED_AT_MS_EXPR} - 1500
                 WHERE id = 'C7'"
            ),
            [],
        )
        .unwrap();

        let loaded = load_crons(&conn).unwrap();

        assert_eq!(loaded.len(), 1);
        assert!(loaded[0].elapsed >= Duration::from_millis(1500));
        assert!(loaded[0].elapsed < Duration::from_secs(3));
    }

    #[test]
    fn cron_load_rejects_invalid_status_text() {
        let conn = in_memory_db();
        conn.execute(
            &format!(
                "INSERT INTO crons (id, schedule, command, enabled, scope_hash, status, created_at_ms)
                 VALUES ('C6', 'every 5m', 'echo invalid', 1, ?1, 'unknown', {CRON_CREATED_AT_MS_EXPR})"
            ),
            rusqlite::params![vec![6u8; 32]],
        )
        .unwrap();

        let error = load_crons(&conn).unwrap_err();

        assert!(error.to_string().contains("parse cron C6 status"));
    }

    #[test]
    fn load_crons_rejects_invalid_ids() {
        let conn = in_memory_db();
        conn.execute(
            &format!(
                "INSERT INTO crons (id, schedule, command, enabled, scope_hash, status, created_at_ms)
                 VALUES ('not-a-cron', 'every 5m', 'echo invalid', 1, ?1, '\"scheduled\"', {CRON_CREATED_AT_MS_EXPR})"
            ),
            rusqlite::params![vec![6u8; 32]],
        )
        .unwrap();

        let error = load_crons(&conn).unwrap_err();

        assert!(error.to_string().contains("invalid cron id"));
    }

    #[test]
    fn script_run_upsert_rolls_back_when_items_cannot_be_written() {
        let conn = in_memory_db();
        let original = StoredScriptRun {
            id: "R1".into(),
            mode: "job".into(),
            input: "echo old".into(),
            status: StoredScriptRunStatus::Submitted,
            item_count: 1,
            error_code: None,
            error_message: None,
            exit_code: None,
            failed_item_index: None,
        };
        let original_items = vec![StoredScriptItem {
            script_id: "R1".into(),
            item_index: 0,
            source_text: "echo old".into(),
            kind: "job".into(),
            target_id: Some("J1".into()),
            chain_id: None,
            job_ids: vec!["J1".into()],
        }];
        upsert_script_run(&conn, &original, &original_items).unwrap();

        conn.execute_batch("DROP TABLE script_items;").unwrap();
        let updated = StoredScriptRun {
            id: "R1".into(),
            mode: "job".into(),
            input: "echo new".into(),
            status: StoredScriptRunStatus::PartialError,
            item_count: 0,
            error_code: Some("INTERNAL".into()),
            error_message: Some("write failed".into()),
            exit_code: None,
            failed_item_index: None,
        };

        let error = upsert_script_run(&conn, &updated, &[]).unwrap_err();
        assert!(error.to_string().contains("delete existing script items"));

        let (input, status, item_count, error_code): (String, String, i64, Option<String>) = conn
            .query_row(
                "SELECT input, status, item_count, error_code FROM script_runs WHERE id = 'R1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(input, "echo old");
        assert_eq!(status, "submitted");
        assert_eq!(item_count, 1);
        assert_eq!(error_code, None);
    }

    #[test]
    fn script_run_terminal_state_persists_exit_and_failed_item() {
        let conn = in_memory_db();
        let script = StoredScriptRun {
            id: "R2".into(),
            mode: "job".into(),
            input: "false".into(),
            status: StoredScriptRunStatus::Failed,
            item_count: 1,
            error_code: None,
            error_message: None,
            exit_code: Some(7),
            failed_item_index: Some(0),
        };
        let items = vec![StoredScriptItem {
            script_id: "R2".into(),
            item_index: 0,
            source_text: "false".into(),
            kind: "job".into(),
            target_id: Some("J2".into()),
            chain_id: None,
            job_ids: vec!["J2".into()],
        }];

        upsert_script_run(&conn, &script, &items).unwrap();

        let (status, exit_code, failed_item_index, finished_at): (
            String,
            Option<i32>,
            Option<i64>,
            Option<String>,
        ) = conn
            .query_row(
                "SELECT status, exit_code, failed_item_index, finished_at
                 FROM script_runs WHERE id = 'R2'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(status, "failed");
        assert_eq!(exit_code, Some(7));
        assert_eq!(failed_item_index, Some(0));
        assert!(finished_at.is_some());
    }

    #[test]
    fn max_script_run_id_rejects_invalid_ids() {
        let conn = in_memory_db();
        conn.execute(
            "INSERT INTO script_runs (id, mode, input, status, item_count)
             VALUES ('not-a-script', 'job', 'echo bad', 'submitted', 1)",
            [],
        )
        .unwrap();

        let error = max_script_run_id(&conn).unwrap_err();

        assert!(error.to_string().contains("invalid script run id"));
    }

    #[test]
    fn prune_script_runs_rejects_invalid_ids_without_deleting_valid_rows() {
        let conn = in_memory_db();
        conn.execute(
            "INSERT INTO script_runs (id, mode, input, status, item_count)
             VALUES ('R1', 'job', 'echo ok', 'submitted', 1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO script_runs (id, mode, input, status, item_count)
             VALUES ('bad-script', 'job', 'echo bad', 'submitted', 1)",
            [],
        )
        .unwrap();

        let error = prune_script_runs(&conn, 0).unwrap_err();

        assert!(error.to_string().contains("invalid script run id"));
        let valid_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM script_runs WHERE id = 'R1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(valid_count, 1);
    }

    #[test]
    fn prune_script_runs_keeps_active_runs_when_over_limit() {
        let conn = in_memory_db();
        for script in [
            StoredScriptRun {
                id: "R1".into(),
                mode: "job".into(),
                input: "sleep 60".into(),
                status: StoredScriptRunStatus::Submitted,
                item_count: 1,
                error_code: None,
                error_message: None,
                exit_code: None,
                failed_item_index: None,
            },
            StoredScriptRun {
                id: "R2".into(),
                mode: "job".into(),
                input: "echo old".into(),
                status: StoredScriptRunStatus::Done,
                item_count: 1,
                error_code: None,
                error_message: None,
                exit_code: Some(0),
                failed_item_index: None,
            },
            StoredScriptRun {
                id: "R3".into(),
                mode: "job".into(),
                input: "echo latest".into(),
                status: StoredScriptRunStatus::Failed,
                item_count: 1,
                error_code: None,
                error_message: None,
                exit_code: Some(7),
                failed_item_index: Some(0),
            },
        ] {
            upsert_script_run(&conn, &script, &[]).unwrap();
        }

        let removed = prune_script_runs(&conn, 1).unwrap();

        assert_eq!(removed, vec!["R2"]);
        let remaining = conn
            .prepare("SELECT id, status FROM script_runs ORDER BY id")
            .unwrap()
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .map(|row| row.unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            remaining,
            vec![
                ("R1".into(), "submitted".into()),
                ("R3".into(), "failed".into())
            ]
        );
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
    fn job_stderr_defaults_to_empty_when_omitted() {
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
