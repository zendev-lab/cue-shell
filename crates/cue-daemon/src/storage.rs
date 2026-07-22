//! SQLite persistence layer for cued.
//!
//! Uses WAL mode for concurrent reads.  The schema is migrated on open.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use cue_core::cron::CronStatus;
use cue_core::job::JobStatus;
use cue_core::scope::{EnvDelta, EnvSnapshot, Scope};
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
const SCHEMA_VERSION: u32 = 18;

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

const MIGRATION_V16: &str = r"
CREATE TABLE IF NOT EXISTS sessions (
    id                  TEXT PRIMARY KEY,
    name                TEXT NOT NULL UNIQUE,
    scope_hash          BLOB REFERENCES scopes(hash),
    pty_default         INTEGER,
    wrapper_enabled     INTEGER,
    created_at_ms       INTEGER NOT NULL,
    updated_at_ms       INTEGER NOT NULL
);
";

const CRON_CREATED_AT_MS_EXPR: &str = "CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)";

/// Open (or create) the database at `path`, apply WAL mode and run migrations.
pub fn open_db(path: &Path) -> Result<Connection> {
    if path != Path::new(":memory:") {
        crate::dirs::ensure_private_file(path)
            .with_context(|| format!("secure database file {}", path.display()))?;
    }
    let conn = Connection::open(path)
        .with_context(|| format!("failed to open database at {}", path.display()))?;

    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;

    migrate(&conn)?;
    crate::dirs::secure_database_files(path)
        .with_context(|| format!("secure database files for {}", path.display()))?;
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
    if current < 15 {
        let report = purge_sensitive_scope_history(conn)
            .context("failed to purge persisted sensitive environment scopes")?;
        if report.removed_scopes > 0 {
            tracing::warn!(
                removed_scopes = report.removed_scopes,
                cleared_job_rows = report.cleared_job_rows,
                cleared_crons = report.cleared_crons,
                disabled_scope_crons = report.disabled_scope_crons,
                "storage: removed persisted scopes containing sensitive environment data"
            );
        }
        set_schema_version(conn, &mut current, 15)?;
    }
    if current < 16 {
        conn.execute_batch(MIGRATION_V16)
            .context("failed to run schema migration v16")?;
        set_schema_version(conn, &mut current, 16)?;
    }
    if current < 17 {
        if !column_exists(conn, "jobs_history", "session_id")? {
            conn.execute_batch(
                "ALTER TABLE jobs_history
                 ADD COLUMN session_id TEXT REFERENCES sessions(id);",
            )
            .context("failed to add jobs_history.session_id")?;
        }
        if !column_exists(conn, "crons", "session_id")? {
            conn.execute_batch(
                "ALTER TABLE crons
                 ADD COLUMN session_id TEXT REFERENCES sessions(id);",
            )
            .context("failed to add crons.session_id")?;
        }
        set_schema_version(conn, &mut current, 17)?;
    }
    if current < 18 {
        if !column_exists(conn, "sessions", "archived_at_ms")? {
            conn.execute_batch("ALTER TABLE sessions ADD COLUMN archived_at_ms INTEGER;")
                .context("failed to add sessions.archived_at_ms")?;
        }
        set_schema_version(conn, &mut current, 18)?;
    }
    Ok(())
}

fn set_schema_version(conn: &Connection, current: &mut u32, version: u32) -> Result<()> {
    conn.pragma_update(None, "user_version", version)?;
    *current = version;
    Ok(())
}

#[derive(Debug, Default, PartialEq, Eq)]
struct SensitiveScopePurgeReport {
    removed_scopes: usize,
    cleared_job_rows: usize,
    cleared_crons: usize,
    disabled_scope_crons: usize,
}

#[derive(Debug)]
struct PersistedScopeMetadata {
    hash: Vec<u8>,
    parent: Option<Vec<u8>>,
    contains_sensitive_environment: bool,
}

/// Remove legacy scopes that persisted credential-like environment values.
///
/// Purging includes every descendant because retaining a child whose parent is
/// absent would break the immutable scope chain. The migration deliberately
/// inspects environment names only; values are neither classified nor logged.
fn purge_sensitive_scope_history(conn: &Connection) -> Result<SensitiveScopePurgeReport> {
    let scopes = {
        let mut stmt = conn.prepare("SELECT hash, parent, delta_json, snap_json FROM scopes")?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, Option<Vec<u8>>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })?;
        let mut scopes = Vec::new();
        for row in rows {
            let (hash, parent, delta_json, snap_json) = row?;
            let contains_sensitive_environment = persisted_scope_has_sensitive_environment(
                delta_json.as_deref(),
                snap_json.as_deref(),
            );
            scopes.push(PersistedScopeMetadata {
                hash,
                parent,
                contains_sensitive_environment,
            });
        }
        scopes
    };

    let mut purged_hashes = scopes
        .iter()
        .filter(|scope| scope.contains_sensitive_environment)
        .map(|scope| scope.hash.clone())
        .collect::<HashSet<_>>();
    loop {
        let previous_len = purged_hashes.len();
        for scope in &scopes {
            if scope
                .parent
                .as_ref()
                .is_some_and(|parent| purged_hashes.contains(parent))
            {
                purged_hashes.insert(scope.hash.clone());
            }
        }
        if purged_hashes.len() == previous_len {
            break;
        }
    }

    if purged_hashes.is_empty() {
        return Ok(SensitiveScopePurgeReport::default());
    }

    let tx = conn
        .unchecked_transaction()
        .context("begin sensitive scope purge transaction")?;
    tx.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS sensitive_scope_purge (
             hash BLOB PRIMARY KEY
         ) WITHOUT ROWID;
         DELETE FROM sensitive_scope_purge;",
    )?;
    for hash in &purged_hashes {
        tx.execute(
            "INSERT OR IGNORE INTO sensitive_scope_purge (hash) VALUES (?1)",
            rusqlite::params![hash],
        )?;
    }

    let cleared_job_rows = tx.execute(
        "UPDATE jobs_history
         SET scope_hash = CASE
                 WHEN scope_hash IN (SELECT hash FROM sensitive_scope_purge) THEN NULL
                 ELSE scope_hash
             END,
             start_scope = CASE
                 WHEN start_scope IN (SELECT hash FROM sensitive_scope_purge) THEN NULL
                 ELSE start_scope
             END,
             end_scope = CASE
                 WHEN end_scope IN (SELECT hash FROM sensitive_scope_purge) THEN NULL
                 ELSE end_scope
             END
         WHERE scope_hash IN (SELECT hash FROM sensitive_scope_purge)
            OR start_scope IN (SELECT hash FROM sensitive_scope_purge)
            OR end_scope IN (SELECT hash FROM sensitive_scope_purge)",
        [],
    )?;
    let cleared_crons = tx.query_row(
        "SELECT COUNT(*) FROM crons
         WHERE scope_hash IN (SELECT hash FROM sensitive_scope_purge)",
        [],
        |row| row.get::<_, i64>(0),
    )?;
    let cleared_crons = usize::try_from(cleared_crons).context("invalid cleared cron count")?;
    let disabled_scope_crons = tx.query_row(
        "SELECT COUNT(*) FROM crons
         WHERE enabled != 0
           AND scope_hash IN (SELECT hash FROM sensitive_scope_purge)",
        [],
        |row| row.get::<_, i64>(0),
    )?;
    let disabled_scope_crons =
        usize::try_from(disabled_scope_crons).context("invalid disabled cron count")?;
    tx.execute(
        r#"UPDATE crons
            SET scope_hash = NULL,
                enabled = 0,
                status = '"paused"'
            WHERE scope_hash IN (SELECT hash FROM sensitive_scope_purge)"#,
        [],
    )?;
    tx.execute(
        "DELETE FROM scope_head
         WHERE hash IN (SELECT hash FROM sensitive_scope_purge)",
        [],
    )?;
    let removed_scopes = tx.execute(
        "DELETE FROM scopes
         WHERE hash IN (SELECT hash FROM sensitive_scope_purge)",
        [],
    )?;
    tx.execute_batch("DROP TABLE sensitive_scope_purge;")?;
    tx.commit().context("commit sensitive scope purge")?;

    Ok(SensitiveScopePurgeReport {
        removed_scopes,
        cleared_job_rows,
        cleared_crons,
        disabled_scope_crons,
    })
}

fn persisted_scope_has_sensitive_environment(
    delta_json: Option<&str>,
    snapshot_json: Option<&str>,
) -> bool {
    let delta_is_sensitive = delta_json.is_some_and(|json| {
        serde_json::from_str::<EnvDelta>(json)
            .map(|delta| delta.set.keys().any(|name| is_sensitive_env_name(name)))
            // Corrupt scope JSON cannot be proven safe and was unusable anyway.
            .unwrap_or(true)
    });
    let snapshot_is_sensitive = snapshot_json.is_some_and(|json| {
        serde_json::from_str::<EnvSnapshot>(json)
            .map(|snapshot| snapshot.env.keys().any(|name| is_sensitive_env_name(name)))
            .unwrap_or(true)
    });
    delta_is_sensitive || snapshot_is_sensitive
}

fn scope_contains_sensitive_environment(scope: &Scope) -> bool {
    scope
        .snapshot
        .as_ref()
        .is_some_and(|snapshot| snapshot.env.keys().any(|name| is_sensitive_env_name(name)))
        || scope
            .delta
            .as_ref()
            .is_some_and(|delta| delta.set.keys().any(|name| is_sensitive_env_name(name)))
}

fn is_sensitive_env_name(name: &str) -> bool {
    let words = name
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|word| !word.is_empty())
        .map(str::to_ascii_uppercase)
        .collect::<Vec<_>>();
    let compact = words.concat();
    let has_word = |candidate: &str| words.iter().any(|word| word == candidate);

    if [
        "TOKEN",
        "SECRET",
        "PASSWORD",
        "PASSWD",
        "PASS",
        "CREDENTIAL",
        "CREDENTIALS",
        "AUTH",
        "AUTHORIZATION",
        "OAUTH",
        "COOKIE",
        "DSN",
        "PASSPHRASE",
    ]
    .into_iter()
    .any(has_word)
    {
        return true;
    }

    if compact.ends_with("TOKEN")
        || compact.ends_with("SECRET")
        || compact.contains("PASSWORD")
        || compact.ends_with("CREDENTIAL")
        || compact.ends_with("CREDENTIALS")
        || compact.ends_with("COOKIE")
        || compact.contains("APIKEY")
        || compact.contains("ACCESSKEY")
        || compact.contains("PRIVATEKEY")
    {
        return true;
    }

    let names_database = [
        "DATABASE",
        "REDIS",
        "MONGO",
        "MONGODB",
        "POSTGRES",
        "POSTGRESQL",
    ]
    .into_iter()
    .any(|backend| compact.contains(backend));
    let names_connection_locator = words
        .iter()
        .any(|word| matches!(word.as_str(), "URL" | "URI" | "CONNECTIONSTRING"))
        || compact.contains("CONNECTIONSTRING");
    names_database && names_connection_locator
}

// ── Scope CRUD ──

/// Whether a scope was durable or intentionally retained in memory only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopePersistence {
    Persisted,
    VolatileSensitiveEnvironment,
    VolatileParent,
}

impl ScopePersistence {
    pub fn is_volatile(self) -> bool {
        !matches!(self, Self::Persisted)
    }

    pub fn reason(self) -> &'static str {
        match self {
            Self::Persisted => "persisted",
            Self::VolatileSensitiveEnvironment => "sensitive_environment",
            Self::VolatileParent => "unpersisted_parent",
        }
    }
}

/// Insert a scope (cache + persistence).
///
/// Scopes containing credential-like environment keys are deliberately not
/// persisted. Their descendants also stay volatile so a durable child never
/// points at a missing parent. The caller remains responsible for retaining
/// volatile scopes in its process-local cache.
pub fn insert_scope(conn: &Connection, scope: &Scope) -> Result<ScopePersistence> {
    if scope_contains_sensitive_environment(scope) {
        return Ok(ScopePersistence::VolatileSensitiveEnvironment);
    }
    if let Some(parent) = scope.parent {
        let parent_exists = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM scopes WHERE hash = ?1)",
            rusqlite::params![parent.0.as_slice()],
            |row| row.get::<_, bool>(0),
        )?;
        if !parent_exists {
            return Ok(ScopePersistence::VolatileParent);
        }
    }

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
    Ok(ScopePersistence::Persisted)
}

fn is_persisted_scope(conn: &Connection, hash: ScopeHash) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM scopes WHERE hash = ?1)",
        rusqlite::params![hash.0.as_slice()],
        |row| row.get(0),
    )
    .context("query persisted scope")
}

fn durable_scope_reference(conn: &Connection, scope: Option<ScopeHash>) -> Result<Option<Vec<u8>>> {
    match scope {
        Some(hash) if is_persisted_scope(conn, hash)? => Ok(Some(hash.0.to_vec())),
        Some(_) => Ok(None),
        None => Ok(None),
    }
}

/// Delete every persisted scope outside the scheduler-supplied reachable set.
///
/// The caller must already have expanded roots to their complete ancestor
/// closure. Durable job, cron, and named-session references are checked inside
/// the transaction; a missing root therefore fails the sweep instead of
/// creating dangling data.
pub fn sweep_scopes(conn: &Connection, reachable: &HashSet<ScopeHash>) -> Result<usize> {
    let tx = conn
        .unchecked_transaction()
        .context("begin scope garbage-collection transaction")?;
    tx.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS scope_gc_reachable (
             hash BLOB PRIMARY KEY
         ) WITHOUT ROWID;
         DELETE FROM scope_gc_reachable;",
    )?;
    for hash in reachable {
        tx.execute(
            "INSERT OR IGNORE INTO scope_gc_reachable (hash) VALUES (?1)",
            rusqlite::params![hash.0.as_slice()],
        )?;
    }

    let unmarked_reference = tx
        .query_row(
            "SELECT hex(scopes.hash)
             FROM scopes
             WHERE NOT EXISTS (
                       SELECT 1 FROM scope_gc_reachable
                       WHERE scope_gc_reachable.hash = scopes.hash
                   )
               AND (
                   scopes.hash IN (
                       SELECT scope_hash FROM jobs_history WHERE scope_hash IS NOT NULL
                   )
                   OR scopes.hash IN (
                       SELECT start_scope FROM jobs_history WHERE start_scope IS NOT NULL
                   )
                   OR scopes.hash IN (
                       SELECT end_scope FROM jobs_history WHERE end_scope IS NOT NULL
                   )
                   OR scopes.hash IN (
                       SELECT scope_hash FROM crons WHERE scope_hash IS NOT NULL
                   )
                   OR scopes.hash IN (
                       SELECT scope_hash FROM sessions WHERE scope_hash IS NOT NULL
                   )
               )
             LIMIT 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    if let Some(hash) = unmarked_reference {
        return Err(anyhow!(
            "refusing scope sweep: persisted job, cron, or session still references unmarked scope {hash}"
        ));
    }

    tx.execute(
        "DELETE FROM scope_head
         WHERE NOT EXISTS (
             SELECT 1 FROM scope_gc_reachable
             WHERE scope_gc_reachable.hash = scope_head.hash
         )",
        [],
    )?;
    let removed = tx.execute(
        "DELETE FROM scopes
         WHERE NOT EXISTS (
             SELECT 1 FROM scope_gc_reachable
             WHERE scope_gc_reachable.hash = scopes.hash
         )",
        [],
    )?;
    tx.execute_batch("DROP TABLE scope_gc_reachable;")?;
    tx.commit().context("commit scope garbage collection")?;
    Ok(removed)
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredSession {
    pub id: String,
    pub name: String,
    /// Durable scope cursor. Volatile scopes are represented as `None` on disk.
    pub scope_hash: Option<ScopeHash>,
    /// `None` inherits the daemon default.
    pub pty_default: Option<bool>,
    /// `None` inherits the daemon default.
    pub wrapper_enabled: Option<bool>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub archived_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredJob {
    pub id: String,
    pub session_id: Option<String>,
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
    pub session_id: Option<String>,
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

/// Insert or update a durable named-session record.
///
/// The returned boolean reports whether the requested scope hash was stored as
/// a durable reference. Missing and process-local volatile scopes are both
/// persisted as `NULL`, preserving the named identity without persisting its
/// environment.
pub fn upsert_session(conn: &Connection, session: &StoredSession) -> Result<bool> {
    let scope_hash = durable_scope_reference(conn, session.scope_hash)?;
    let stored_scope = scope_hash.is_some();
    let pty_default = session.pty_default.map(i64::from);
    let wrapper_enabled = session.wrapper_enabled.map(i64::from);

    conn.execute(
        "INSERT INTO sessions (
             id, name, scope_hash, pty_default, wrapper_enabled, created_at_ms, updated_at_ms,
             archived_at_ms
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT(id) DO UPDATE SET
              name = excluded.name,
              scope_hash = excluded.scope_hash,
              pty_default = excluded.pty_default,
              wrapper_enabled = excluded.wrapper_enabled,
              updated_at_ms = excluded.updated_at_ms,
              archived_at_ms = excluded.archived_at_ms",
        rusqlite::params![
            session.id,
            session.name,
            scope_hash,
            pty_default,
            wrapper_enabled,
            session.created_at_ms,
            session.updated_at_ms,
            session.archived_at_ms,
        ],
    )?;
    Ok(stored_scope)
}

/// Atomically update only the reversible lifecycle state of a durable session.
///
/// The scheduler applies the in-memory change only after this statement
/// succeeds, so a storage failure cannot make memory disagree with SQLite.
pub fn set_session_archived_at(
    conn: &Connection,
    id: &str,
    archived_at_ms: Option<i64>,
    updated_at_ms: i64,
) -> Result<()> {
    let changed = conn.execute(
        "UPDATE sessions
         SET archived_at_ms = ?2, updated_at_ms = ?3
         WHERE id = ?1",
        rusqlite::params![id, archived_at_ms, updated_at_ms],
    )?;
    if changed != 1 {
        return Err(anyhow!("named session {id} is missing from storage"));
    }
    Ok(())
}

pub fn load_sessions(conn: &Connection) -> Result<Vec<StoredSession>> {
    let mut stmt = conn.prepare(
        "SELECT id, name, scope_hash, pty_default, wrapper_enabled,
                created_at_ms, updated_at_ms, archived_at_ms
         FROM sessions
         ORDER BY created_at_ms, id",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<Vec<u8>>>(2)?,
            row.get::<_, Option<bool>>(3)?,
            row.get::<_, Option<bool>>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, i64>(6)?,
            row.get::<_, Option<i64>>(7)?,
        ))
    })?;

    let mut sessions = Vec::new();
    for row in rows {
        let (
            id,
            name,
            scope_hash_blob,
            pty_default,
            wrapper_enabled,
            created_at_ms,
            updated_at_ms,
            archived_at_ms,
        ) = row?;
        sessions.push(StoredSession {
            id,
            name,
            scope_hash: scope_hash_blob
                .as_deref()
                .map(blob_to_scope_hash)
                .transpose()?,
            pty_default,
            wrapper_enabled,
            created_at_ms,
            updated_at_ms,
            archived_at_ms,
        });
    }
    Ok(sessions)
}

pub fn upsert_job_history(conn: &Connection, job: &StoredJob) -> Result<()> {
    let status_json = serde_json::to_string(&job.status).context("serialize job status")?;
    let start_scope = durable_scope_reference(conn, job.start_scope)?;
    let end_scope = durable_scope_reference(conn, job.end_scope)?;
    let finished = if job.status.is_terminal() { 1 } else { 0 };

    conn.execute(
        "INSERT INTO jobs_history (
             id, session_id, pipeline, status, exit_code, scope_hash, start_scope, end_scope, chain_id, stderr, finished_at
         )
         VALUES (
             ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, CASE WHEN ?11 THEN datetime('now') ELSE NULL END
         )
         ON CONFLICT(id) DO UPDATE SET
              session_id = excluded.session_id,
              pipeline = excluded.pipeline,
              status = excluded.status,
              exit_code = excluded.exit_code,
              scope_hash = excluded.scope_hash,
              start_scope = excluded.start_scope,
              end_scope = excluded.end_scope,
              chain_id = excluded.chain_id,
              stderr = excluded.stderr,
              finished_at = CASE WHEN ?11 THEN datetime('now') ELSE jobs_history.finished_at END",
        rusqlite::params![
            job.id,
            job.session_id,
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
        "SELECT id, session_id, pipeline, status, exit_code, start_scope, end_scope,
                COALESCE(chain_id, NULL) AS chain_id,
                COALESCE(stderr, '') AS stderr
         FROM jobs_history",
    )?;
    let rows = stmt.query_map([], |row| {
        let id: String = row.get(0)?;
        let session_id: Option<String> = row.get(1)?;
        let pipeline: String = row.get(2)?;
        let status_text: String = row.get(3)?;
        let exit_code: Option<i32> = row.get(4)?;
        let start_scope_blob: Option<Vec<u8>> = row.get(5)?;
        let end_scope_blob: Option<Vec<u8>> = row.get(6)?;
        let chain_id: Option<String> = row.get(7)?;
        let stderr: String = row.get(8)?;
        Ok((
            id,
            session_id,
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
            session_id,
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
                session_id,
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
    let volatile_scope = cron
        .scope_hash
        .map(|hash| is_persisted_scope(conn, hash).map(|persisted| !persisted))
        .transpose()?
        .unwrap_or(false);
    let scope_hash = if volatile_scope {
        None
    } else {
        cron.scope_hash.map(|hash| hash.0.to_vec())
    };
    let persisted_status = if volatile_scope {
        CronStatus::Paused
    } else {
        cron.status
    };
    let status = serde_json::to_string(&persisted_status).context("serialize cron status")?;
    let enabled = i64::from(persisted_status.is_runnable());
    let cwd_override = cron
        .cwd_override
        .as_ref()
        .map(|path| path.to_string_lossy().into_owned());
    let scope_enabled = i64::from(cron.scope_enabled);
    let wrapper_enabled = i64::from(cron.wrapper_enabled);
    conn.execute(
        &format!(
            "INSERT INTO crons (id, session_id, schedule, command, enabled, scope_hash, status, cwd_override, scope_enabled, wrapper_enabled, created_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, {CRON_CREATED_AT_MS_EXPR})
         ON CONFLICT(id) DO UPDATE SET
              session_id = excluded.session_id,
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
            cron.session_id,
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
         SELECT id, session_id, schedule, command, scope_hash,
                status,
                cwd_override,
                scope_enabled,
                wrapper_enabled,
                now_ms.value - created_at_ms AS age_millis
          FROM crons, now_ms"
    ))?;
    let rows = stmt.query_map([], |row| {
        let id: String = row.get(0)?;
        let session_id: Option<String> = row.get(1)?;
        let schedule: String = row.get(2)?;
        let command: String = row.get(3)?;
        let scope_blob: Option<Vec<u8>> = row.get(4)?;
        let status_text: String = row.get(5)?;
        let cwd_override: Option<String> = row.get(6)?;
        let scope_enabled: i64 = row.get(7)?;
        let wrapper_enabled: i64 = row.get(8)?;
        let age_millis: i64 = row.get(9)?;
        Ok((
            id,
            session_id,
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
            session_id,
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
                    session_id,
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
    use std::os::unix::fs::PermissionsExt as _;
    use std::path::PathBuf;

    use cue_core::job::CancelReason;
    use cue_core::scope::{EnvDelta, EnvSnapshot};

    use super::*;

    fn in_memory_db() -> Connection {
        open_db(Path::new(":memory:")).expect("open in-memory db")
    }

    fn raw_insert_scope(conn: &Connection, scope: &Scope) {
        let delta_json = scope
            .delta
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .expect("serialize fixture delta");
        let snapshot_json = scope
            .snapshot
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .expect("serialize fixture snapshot");
        conn.execute(
            "INSERT INTO scopes (hash, parent, delta_json, snap_json)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![
                scope.hash.0.as_slice(),
                scope.parent.map(|parent| parent.0.to_vec()),
                delta_json,
                snapshot_json,
            ],
        )
        .expect("insert raw scope fixture");
    }

    fn insert_safe_scope(conn: &Connection, cwd: &str) -> ScopeHash {
        let scope = Scope::root(EnvSnapshot {
            env: BTreeMap::from([("PATH".into(), "/usr/bin".into())]),
            cwd: PathBuf::from(cwd),
        });
        assert_eq!(
            insert_scope(conn, &scope).expect("insert safe scope fixture"),
            ScopePersistence::Persisted
        );
        scope.hash
    }

    #[test]
    fn migration_is_idempotent() {
        let conn = in_memory_db();
        // Running migrate again should be a no-op.
        migrate(&conn).expect("second migration");
    }

    #[test]
    fn migration_from_v15_adds_named_sessions_table() {
        let conn = Connection::open_in_memory().expect("open legacy in-memory db");
        conn.execute_batch(MIGRATION_V1)
            .expect("seed legacy schema");
        conn.pragma_update(None, "user_version", 15)
            .expect("mark schema version 15");

        migrate(&conn).expect("migrate schema to v16");

        for column in [
            "id",
            "name",
            "scope_hash",
            "pty_default",
            "wrapper_enabled",
            "created_at_ms",
            "updated_at_ms",
            "archived_at_ms",
        ] {
            assert!(
                column_exists(&conn, "sessions", column).expect("inspect sessions schema"),
                "missing sessions.{column}"
            );
        }
        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("read migrated schema version");
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[test]
    fn migration_v17_and_v18_preserve_legacy_unowned_jobs_and_crons() {
        let conn = Connection::open_in_memory().expect("open legacy in-memory db");
        conn.execute_batch(MIGRATION_V1)
            .expect("seed legacy base schema");
        conn.execute_batch(
            "ALTER TABLE jobs_history ADD COLUMN chain_id TEXT;
             ALTER TABLE jobs_history ADD COLUMN stderr TEXT NOT NULL DEFAULT '';
             ALTER TABLE crons ADD COLUMN status TEXT;
             CREATE TABLE sessions (
                 id                  TEXT PRIMARY KEY,
                 name                TEXT NOT NULL UNIQUE,
                 scope_hash          BLOB REFERENCES scopes(hash),
                 pty_default         INTEGER,
                 wrapper_enabled     INTEGER,
                 created_at_ms       INTEGER NOT NULL,
                 updated_at_ms       INTEGER NOT NULL
             );",
        )
        .expect("complete schema version 16 fixture");
        let job_status = serde_json::to_string(&JobStatus::Done).expect("serialize job status");
        conn.execute(
            "INSERT INTO jobs_history (id, pipeline, status)
             VALUES ('J1', 'echo legacy job', ?1)",
            rusqlite::params![job_status],
        )
        .expect("insert legacy unowned job");
        let cron_status =
            serde_json::to_string(&CronStatus::Paused).expect("serialize cron status");
        conn.execute(
            &format!(
                "INSERT INTO crons (
                     id, schedule, command, enabled, status, created_at_ms
                 ) VALUES (
                     'C1', 'every 5m', 'echo legacy cron', 0, ?1, {CRON_CREATED_AT_MS_EXPR}
                 )"
            ),
            rusqlite::params![cron_status],
        )
        .expect("insert legacy unowned cron");
        conn.pragma_update(None, "user_version", 16)
            .expect("mark schema version 16");

        migrate(&conn).expect("migrate schema to current version");

        assert!(column_exists(&conn, "jobs_history", "session_id").expect("inspect jobs schema"));
        assert!(column_exists(&conn, "crons", "session_id").expect("inspect crons schema"));
        assert!(
            column_exists(&conn, "sessions", "archived_at_ms").expect("inspect sessions schema")
        );
        assert_eq!(
            load_job_history(&conn).expect("load legacy jobs")[0].session_id,
            None
        );
        assert_eq!(
            load_crons(&conn).expect("load legacy crons")[0]
                .record
                .session_id,
            None
        );
        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("read migrated schema version");
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[test]
    fn migration_v18_preserves_sessions_as_active() {
        let conn = Connection::open_in_memory().expect("open legacy in-memory db");
        conn.execute_batch(MIGRATION_V1)
            .expect("seed legacy base schema");
        conn.execute_batch(MIGRATION_V16)
            .expect("seed legacy sessions schema");
        conn.execute(
            "INSERT INTO sessions (
                 id, name, scope_hash, pty_default, wrapper_enabled, created_at_ms, updated_at_ms
             ) VALUES ('SS-legacy', 'legacy', NULL, NULL, NULL, 10, 20)",
            [],
        )
        .expect("insert legacy session");
        conn.pragma_update(None, "user_version", 17)
            .expect("mark schema version 17");

        migrate(&conn).expect("migrate schema to v18");

        assert_eq!(
            load_sessions(&conn).expect("load migrated session")[0].archived_at_ms,
            None
        );
        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("read migrated schema version");
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[test]
    fn file_database_and_sidecars_use_private_permissions() {
        let root = std::env::temp_dir().join(format!(
            "cue-storage-permissions-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("create temp directory");
        let path = root.join("cued.db");
        rusqlite::Connection::open(&path)
            .expect("create database")
            .close()
            .expect("close database");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
            .expect("set wide database mode");

        let conn = open_db(&path).expect("open private database");

        for file in [
            path.clone(),
            crate::dirs::database_sidecar_path(&path, "-wal"),
            crate::dirs::database_sidecar_path(&path, "-shm"),
        ] {
            if file.exists() {
                assert_eq!(
                    std::fs::metadata(&file)
                        .expect("stat database file")
                        .permissions()
                        .mode()
                        & 0o777,
                    0o600,
                    "{}",
                    file.display()
                );
            }
        }
        drop(conn);
        std::fs::remove_dir_all(root).expect("remove temp directory");
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
    fn sensitive_environment_names_are_classified_without_reading_values() {
        for name in [
            "TOKEN",
            "GITHUB_TOKEN",
            "OPENAI_API_KEY",
            "AWS_ACCESS_KEY_ID",
            "PRIVATE_KEY",
            "CLIENT_SECRET",
            "PGPASSWORD",
            "DB_PASS",
            "SHARED_CREDENTIALS_FILE",
            "HTTP_AUTH",
            "AUTHORIZATION",
            "SESSION_COOKIE",
            "SENTRY_DSN",
            "DATABASE_URL",
            "REDIS_URI",
            "MONGODB_CONNECTION_STRING",
            "POSTGRESQL_URL_READ_ONLY",
        ] {
            assert!(is_sensitive_env_name(name), "expected sensitive name");
        }

        for name in [
            "PATH",
            "HOME",
            "PWD",
            "OLDPWD",
            "AUTHOR",
            "AUTHORS",
            "TOKENIZERS_PARALLELISM",
            "DATABASE_HOST",
            "REDIS_PORT",
            "POSTGRES_USER",
        ] {
            assert!(!is_sensitive_env_name(name), "expected non-sensitive name");
        }
    }

    #[test]
    fn sensitive_scope_and_its_descendant_remain_volatile() {
        let conn = in_memory_db();
        let sensitive = Scope::root(EnvSnapshot {
            env: BTreeMap::from([
                ("PATH".into(), "/usr/bin".into()),
                ("OPENAI_API_KEY".into(), "fixture-do-not-persist".into()),
            ]),
            cwd: PathBuf::from("/tmp/root"),
        });
        let sensitive_result = insert_scope(&conn, &sensitive).expect("insert sensitive scope");
        assert_eq!(
            sensitive_result,
            ScopePersistence::VolatileSensitiveEnvironment
        );
        assert!(
            get_scope(&conn, &sensitive.hash)
                .expect("query sensitive scope")
                .is_none()
        );

        let descendant = Scope::fork(
            sensitive.hash,
            sensitive.snapshot.as_ref().expect("sensitive snapshot"),
            EnvDelta {
                set: BTreeMap::new(),
                unset: vec!["OPENAI_API_KEY".into()],
                cwd: Some(PathBuf::from("/tmp/clean-child")),
            },
        );
        assert_eq!(
            descendant
                .snapshot
                .as_ref()
                .expect("descendant snapshot")
                .compute_hash(),
            descendant.hash
        );
        assert!(
            !scope_contains_sensitive_environment(&descendant),
            "unset names do not contain persisted values"
        );
        assert_eq!(
            insert_scope(&conn, &descendant).expect("insert descendant"),
            ScopePersistence::VolatileParent
        );
        assert!(
            get_scope(&conn, &descendant.hash)
                .expect("query descendant")
                .is_none()
        );

        upsert_job_history(
            &conn,
            &StoredJob {
                id: "J1".into(),
                session_id: None,
                pipeline: "volatile scopes".into(),
                status: JobStatus::Done,
                exit_code: Some(0),
                start_scope: Some(sensitive.hash),
                end_scope: Some(descendant.hash),
                chain_id: None,
                stderr: String::new(),
            },
        )
        .expect("persist job without volatile scope references");
        let job = load_job_history(&conn)
            .expect("load job")
            .pop()
            .expect("job exists");
        assert_eq!(job.start_scope, None);
        assert_eq!(job.end_scope, None);

        for cron in [
            StoredCron {
                id: "C1".into(),
                session_id: None,
                schedule: "every 5m".into(),
                command: "echo scoped".into(),
                status: CronStatus::Scheduled,
                scope_hash: Some(sensitive.hash),
                cwd_override: None,
                scope_enabled: true,
                wrapper_enabled: false,
            },
            StoredCron {
                id: "C2".into(),
                session_id: None,
                schedule: "every 5m".into(),
                command: "echo unscoped".into(),
                status: CronStatus::Scheduled,
                scope_hash: Some(descendant.hash),
                cwd_override: None,
                scope_enabled: false,
                wrapper_enabled: false,
            },
        ] {
            upsert_cron(&conn, &cron).expect("persist cron without volatile scope reference");
        }
        let crons = load_crons(&conn).expect("load crons");
        assert_eq!(crons[0].record.scope_hash, None);
        assert_eq!(crons[0].record.status, CronStatus::Paused);
        assert_eq!(crons[1].record.scope_hash, None);
        assert_eq!(crons[1].record.status, CronStatus::Paused);
    }

    #[test]
    fn migration_purges_sensitive_scopes_and_descendants_and_repairs_references() {
        let conn = in_memory_db();
        let safe = Scope::root(EnvSnapshot {
            env: BTreeMap::from([("PATH".into(), "/usr/bin".into())]),
            cwd: PathBuf::from("/tmp/safe"),
        });
        let sensitive = Scope::root(EnvSnapshot {
            env: BTreeMap::from([
                ("PATH".into(), "/usr/bin".into()),
                ("DATABASE_URL".into(), "fixture-do-not-log".into()),
            ]),
            cwd: PathBuf::from("/tmp/sensitive"),
        });
        let descendant = Scope::fork(
            sensitive.hash,
            sensitive.snapshot.as_ref().expect("sensitive snapshot"),
            EnvDelta {
                set: BTreeMap::new(),
                unset: vec!["DATABASE_URL".into()],
                cwd: Some(PathBuf::from("/tmp/descendant")),
            },
        );
        assert!(!scope_contains_sensitive_environment(&descendant));
        raw_insert_scope(&conn, &safe);
        raw_insert_scope(&conn, &sensitive);
        raw_insert_scope(&conn, &descendant);
        conn.execute(
            "INSERT INTO scope_head (id, hash) VALUES (0, ?1)",
            rusqlite::params![sensitive.hash.0.as_slice()],
        )
        .expect("insert legacy scope head");

        upsert_job_history(
            &conn,
            &StoredJob {
                id: "J1".into(),
                session_id: None,
                pipeline: "first".into(),
                status: JobStatus::Done,
                exit_code: Some(0),
                start_scope: Some(sensitive.hash),
                end_scope: Some(safe.hash),
                chain_id: None,
                stderr: String::new(),
            },
        )
        .expect("insert first job");
        upsert_job_history(
            &conn,
            &StoredJob {
                id: "J2".into(),
                session_id: None,
                pipeline: "second".into(),
                status: JobStatus::Done,
                exit_code: Some(0),
                start_scope: Some(safe.hash),
                end_scope: Some(descendant.hash),
                chain_id: None,
                stderr: String::new(),
            },
        )
        .expect("insert second job");
        for cron in [
            StoredCron {
                id: "C1".into(),
                session_id: None,
                schedule: "every 5m".into(),
                command: "echo scoped".into(),
                status: CronStatus::Scheduled,
                scope_hash: Some(sensitive.hash),
                cwd_override: None,
                scope_enabled: true,
                wrapper_enabled: false,
            },
            StoredCron {
                id: "C2".into(),
                session_id: None,
                schedule: "every 5m".into(),
                command: "echo unscoped".into(),
                status: CronStatus::Scheduled,
                scope_hash: Some(descendant.hash),
                cwd_override: None,
                scope_enabled: false,
                wrapper_enabled: false,
            },
            StoredCron {
                id: "C3".into(),
                session_id: None,
                schedule: "every 5m".into(),
                command: "echo safe".into(),
                status: CronStatus::Scheduled,
                scope_hash: Some(safe.hash),
                cwd_override: None,
                scope_enabled: true,
                wrapper_enabled: false,
            },
        ] {
            upsert_cron(&conn, &cron).expect("insert cron fixture");
        }

        conn.pragma_update(None, "user_version", 14)
            .expect("mark legacy schema");
        migrate(&conn).expect("migrate sensitive scope history");

        assert!(get_scope(&conn, &safe.hash).expect("load safe").is_some());
        assert!(
            get_scope(&conn, &sensitive.hash)
                .expect("load sensitive")
                .is_none()
        );
        assert!(
            get_scope(&conn, &descendant.hash)
                .expect("load descendant")
                .is_none()
        );
        let jobs = load_job_history(&conn).expect("load repaired jobs");
        assert_eq!(jobs[0].start_scope, None);
        assert_eq!(jobs[0].end_scope, Some(safe.hash));
        assert_eq!(jobs[1].start_scope, Some(safe.hash));
        assert_eq!(jobs[1].end_scope, None);

        let crons = load_crons(&conn).expect("load repaired crons");
        assert_eq!(crons[0].record.scope_hash, None);
        assert_eq!(crons[0].record.status, CronStatus::Paused);
        assert!(crons[0].record.scope_enabled);
        assert_eq!(crons[1].record.scope_hash, None);
        assert_eq!(crons[1].record.status, CronStatus::Paused);
        assert!(!crons[1].record.scope_enabled);
        assert_eq!(crons[2].record.scope_hash, Some(safe.hash));
        assert_eq!(crons[2].record.status, CronStatus::Scheduled);

        let scope_head_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM scope_head", [], |row| row.get(0))
            .expect("count scope heads");
        assert_eq!(scope_head_count, 0);
        let persisted_fixture_values: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM scopes
                 WHERE instr(COALESCE(delta_json, ''), 'fixture-do-not-log') != 0
                    OR instr(COALESCE(snap_json, ''), 'fixture-do-not-log') != 0",
                [],
                |row| row.get(0),
            )
            .expect("check fixture value removal");
        assert_eq!(persisted_fixture_values, 0);
        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("read migrated schema version");
        assert_eq!(version, SCHEMA_VERSION);
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
        assert_eq!(loaded.snapshot, scope.snapshot);
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
    fn sweep_scopes_deletes_only_unmarked_scopes() {
        let conn = in_memory_db();
        let root = Scope::root(EnvSnapshot {
            env: BTreeMap::from([("PATH".into(), "/usr/bin".into())]),
            cwd: PathBuf::from("/tmp/root"),
        });
        let child = Scope::fork(
            root.hash,
            root.snapshot.as_ref().expect("root snapshot"),
            EnvDelta {
                set: BTreeMap::from([("MODE".into(), "debug".into())]),
                unset: vec![],
                cwd: None,
            },
        );
        let orphan = Scope::root(EnvSnapshot {
            env: BTreeMap::from([("PATH".into(), "/bin".into())]),
            cwd: PathBuf::from("/tmp/orphan"),
        });
        for scope in [&root, &child, &orphan] {
            insert_scope(&conn, scope).expect("insert scope fixture");
        }

        let removed =
            sweep_scopes(&conn, &HashSet::from([root.hash, child.hash])).expect("sweep scopes");

        assert_eq!(removed, 1);
        assert!(get_scope(&conn, &root.hash).expect("load root").is_some());
        assert!(get_scope(&conn, &child.hash).expect("load child").is_some());
        assert!(
            get_scope(&conn, &orphan.hash)
                .expect("load orphan")
                .is_none()
        );
    }

    #[test]
    fn sweep_scopes_refuses_to_delete_durable_references() {
        let conn = in_memory_db();
        let root = Scope::root(EnvSnapshot {
            env: BTreeMap::from([("PATH".into(), "/usr/bin".into())]),
            cwd: PathBuf::from("/tmp/referenced"),
        });
        insert_scope(&conn, &root).expect("insert referenced scope");
        upsert_job_history(
            &conn,
            &StoredJob {
                id: "J1".into(),
                session_id: None,
                pipeline: "echo retained".into(),
                status: JobStatus::Done,
                exit_code: Some(0),
                start_scope: Some(root.hash),
                end_scope: Some(root.hash),
                chain_id: None,
                stderr: String::new(),
            },
        )
        .expect("persist referenced job");

        let error = sweep_scopes(&conn, &HashSet::new())
            .expect_err("unmarked durable reference must fail closed");

        assert!(
            error
                .to_string()
                .contains("still references unmarked scope")
        );
        assert!(get_scope(&conn, &root.hash).expect("load root").is_some());
    }

    #[test]
    fn sweep_scopes_refuses_to_delete_named_session_scope() {
        let conn = in_memory_db();
        let scope_hash = insert_safe_scope(&conn, "/tmp/named-session");
        let session = StoredSession {
            id: "session-1".into(),
            name: "shared".into(),
            scope_hash: Some(scope_hash),
            pty_default: None,
            wrapper_enabled: None,
            created_at_ms: 100,
            updated_at_ms: 100,
            archived_at_ms: None,
        };
        assert!(upsert_session(&conn, &session).expect("persist named session"));

        let error = sweep_scopes(&conn, &HashSet::new())
            .expect_err("unmarked named-session scope must fail closed");

        assert!(
            error
                .to_string()
                .contains("session still references unmarked scope")
        );
        assert!(
            get_scope(&conn, &scope_hash)
                .expect("load named-session scope")
                .is_some()
        );
    }

    #[test]
    fn named_session_roundtrips_durable_scope_and_defaults() {
        let conn = in_memory_db();
        let scope_hash = insert_safe_scope(&conn, "/tmp/shared");
        let session = StoredSession {
            id: "session-2".into(),
            name: "pairing".into(),
            scope_hash: Some(scope_hash),
            pty_default: Some(true),
            wrapper_enabled: Some(false),
            created_at_ms: 200,
            updated_at_ms: 250,
            archived_at_ms: Some(260),
        };

        assert!(upsert_session(&conn, &session).expect("persist durable session scope"));

        assert_eq!(
            load_sessions(&conn).expect("load named sessions"),
            vec![session]
        );
    }

    #[test]
    fn named_session_archive_state_updates_without_replacing_identity_or_scope() {
        let conn = in_memory_db();
        let scope_hash = insert_safe_scope(&conn, "/tmp/archive-state");
        let session = StoredSession {
            id: "session-archive".into(),
            name: "archive-state".into(),
            scope_hash: Some(scope_hash),
            pty_default: Some(true),
            wrapper_enabled: Some(false),
            created_at_ms: 10,
            updated_at_ms: 20,
            archived_at_ms: None,
        };
        assert!(upsert_session(&conn, &session).expect("persist session"));

        set_session_archived_at(&conn, &session.id, Some(30), 30).expect("archive session");
        let archived = load_sessions(&conn).expect("load archived session");
        assert_eq!(archived[0].archived_at_ms, Some(30));
        assert_eq!(archived[0].scope_hash, Some(scope_hash));
        assert_eq!(archived[0].name, session.name);

        set_session_archived_at(&conn, &session.id, None, 40).expect("restore session");
        let restored = load_sessions(&conn).expect("load restored session");
        assert_eq!(restored[0].archived_at_ms, None);
        assert_eq!(restored[0].updated_at_ms, 40);
    }

    #[test]
    fn named_session_without_scope_reports_not_durable() {
        let conn = in_memory_db();
        let session = StoredSession {
            id: "session-unbound".into(),
            name: "needs-refresh".into(),
            scope_hash: None,
            pty_default: None,
            wrapper_enabled: None,
            created_at_ms: 275,
            updated_at_ms: 275,
            archived_at_ms: None,
        };

        assert!(!upsert_session(&conn, &session).expect("persist unbound session"));
        assert_eq!(
            load_sessions(&conn).expect("load unbound session"),
            vec![session]
        );
    }

    #[test]
    fn named_session_keeps_identity_but_not_volatile_scope() {
        let conn = in_memory_db();
        let volatile_scope = Scope::root(EnvSnapshot {
            env: BTreeMap::from([
                ("PATH".into(), "/usr/bin".into()),
                ("API_TOKEN".into(), "fixture-do-not-persist".into()),
            ]),
            cwd: PathBuf::from("/tmp/volatile-session"),
        });
        assert_eq!(
            insert_scope(&conn, &volatile_scope).expect("insert volatile session scope"),
            ScopePersistence::VolatileSensitiveEnvironment
        );
        let requested = StoredSession {
            id: "session-3".into(),
            name: "agent-pair".into(),
            scope_hash: Some(volatile_scope.hash),
            pty_default: Some(false),
            wrapper_enabled: None,
            created_at_ms: 300,
            updated_at_ms: 300,
            archived_at_ms: None,
        };

        assert!(
            !upsert_session(&conn, &requested).expect("persist volatile session metadata"),
            "volatile scope must not be reported as durable"
        );

        let mut expected = requested;
        expected.scope_hash = None;
        assert_eq!(
            load_sessions(&conn).expect("load volatile session metadata"),
            vec![expected]
        );
        let persisted_fixture_values: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM scopes
                 WHERE instr(COALESCE(delta_json, ''), 'fixture-do-not-persist') != 0
                    OR instr(COALESCE(snap_json, ''), 'fixture-do-not-persist') != 0",
                [],
                |row| row.get(0),
            )
            .expect("check volatile value was not persisted");
        assert_eq!(persisted_fixture_values, 0);
    }

    #[test]
    fn volatile_scope_references_stay_null_across_database_connections() {
        let root = std::env::temp_dir().join(format!(
            "cue-volatile-reference-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("create temp directory");
        let path = root.join("cued.db");
        let scope_conn = open_db(&path).expect("open scope connection");
        let scheduler_conn = open_db(&path).expect("open scheduler connection");
        let sensitive = Scope::root(EnvSnapshot {
            env: BTreeMap::from([("API_TOKEN".into(), "memory-only".into())]),
            cwd: PathBuf::from("/tmp/sensitive"),
        });
        assert_eq!(
            insert_scope(&scope_conn, &sensitive).expect("insert volatile scope"),
            ScopePersistence::VolatileSensitiveEnvironment
        );

        upsert_job_history(
            &scheduler_conn,
            &StoredJob {
                id: "J1".into(),
                session_id: None,
                pipeline: "echo volatile".into(),
                status: JobStatus::Done,
                exit_code: Some(0),
                start_scope: Some(sensitive.hash),
                end_scope: Some(sensitive.hash),
                chain_id: None,
                stderr: String::new(),
            },
        )
        .expect("persist job without volatile scope reference");

        let jobs = load_job_history(&scheduler_conn).expect("load job history");
        assert_eq!(jobs[0].start_scope, None);
        assert_eq!(jobs[0].end_scope, None);
        drop(scope_conn);
        drop(scheduler_conn);
        std::fs::remove_dir_all(root).expect("remove temp directory");
    }

    #[test]
    fn named_session_owner_roundtrips_for_jobs_and_crons() {
        let conn = in_memory_db();
        let scope_hash = insert_safe_scope(&conn, "/tmp/owned-work");
        let session = StoredSession {
            id: "session-owned".into(),
            name: "owned-work".into(),
            scope_hash: Some(scope_hash),
            pty_default: None,
            wrapper_enabled: None,
            created_at_ms: 400,
            updated_at_ms: 400,
            archived_at_ms: None,
        };
        assert!(upsert_session(&conn, &session).expect("persist owning session"));
        let job = StoredJob {
            id: "J40".into(),
            session_id: Some(session.id.clone()),
            pipeline: "echo owned job".into(),
            status: JobStatus::Done,
            exit_code: Some(0),
            start_scope: Some(scope_hash),
            end_scope: Some(scope_hash),
            chain_id: None,
            stderr: String::new(),
        };
        let cron = StoredCron {
            id: "C40".into(),
            session_id: Some(session.id.clone()),
            schedule: "every 5m".into(),
            command: "echo owned cron".into(),
            status: CronStatus::Scheduled,
            scope_hash: Some(scope_hash),
            cwd_override: None,
            scope_enabled: false,
            wrapper_enabled: false,
        };

        upsert_job_history(&conn, &job).expect("persist owned job");
        upsert_cron(&conn, &cron).expect("persist owned cron");

        assert_eq!(load_job_history(&conn).expect("load owned job"), vec![job]);
        assert_eq!(load_crons(&conn).expect("load owned cron")[0].record, cron);
    }

    #[test]
    fn job_history_roundtrip() {
        let conn = in_memory_db();
        let start_scope = insert_safe_scope(&conn, "/tmp/job-start");
        let end_scope = insert_safe_scope(&conn, "/tmp/job-end");
        let job = StoredJob {
            id: "J12".into(),
            session_id: None,
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
                session_id: None,
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
                session_id: None,
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
                session_id: None,
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
        let scope_hash = insert_safe_scope(&conn, "/tmp/cron");
        let cron = StoredCron {
            id: "C3".into(),
            session_id: None,
            schedule: "every 5m".into(),
            command: "cargo test".into(),
            status: CronStatus::Scheduled,
            scope_hash: Some(scope_hash),
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
        let scope_hash = insert_safe_scope(&conn, "/tmp/failed-cron");
        let cron = StoredCron {
            id: "C4".into(),
            session_id: None,
            schedule: "in 1s".into(),
            command: "echo due".into(),
            status: CronStatus::Failed,
            scope_hash: Some(scope_hash),
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
        let scope_hash = insert_safe_scope(&conn, "/tmp/elapsed-cron");
        let cron = StoredCron {
            id: "C7".into(),
            session_id: None,
            schedule: "in 1500ms".into(),
            command: "echo soon".into(),
            status: CronStatus::Scheduled,
            scope_hash: Some(scope_hash),
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
            session_id: None,
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
