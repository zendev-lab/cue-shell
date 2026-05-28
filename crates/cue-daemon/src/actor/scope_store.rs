//! ScopeStore actor — environment scope management.
//!
//! Maintains an in-memory cache backed by SQLite.  The "HEAD" pointer
//! tracks the current active scope (analogous to git HEAD).

use anyhow::Result;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

use rusqlite::Connection;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

use cue_core::ScopeHash;
use cue_core::scope::{EnvSnapshot, Scope};

use super::{ActorSystem, EventBusMsg, ScopeStoreMsg};
use crate::storage;

use cue_core::ipc::EventPayload;

/// Spawn the ScopeStore actor task.
///
/// Initialises a root scope from the current process environment.
pub fn spawn(mut rx: mpsc::Receiver<ScopeStoreMsg>, conn: Connection, sys: ActorSystem) {
    tokio::spawn(async move {
        let db = storage::shared_connection(conn);
        let (mut cache, mut current_head, restored) = match load_initial_scope(&db).await {
            Ok(initial) => initial,
            Err(e) => {
                error!("scope_store: failed to load initial scope: {e}");
                return;
            }
        };

        if restored {
            info!(%current_head, "scope_store: restored persisted head scope");
        } else {
            info!(%current_head, "scope_store: started with root scope");
        }

        while let Some(msg) = rx.recv().await {
            match msg {
                ScopeStoreMsg::GetHead { reply } => {
                    let _ = reply.send(current_head);
                }

                ScopeStoreMsg::GetScope { hash, reply } => {
                    // Check cache first, then SQLite.
                    let scope = if let Some(scope) = cache.get(&hash) {
                        Ok(Some(scope.clone()))
                    } else {
                        match storage::with_connection(&db, move |conn| {
                            storage::get_scope(conn, &hash)
                        })
                        .await
                        {
                            Ok(Some(scope)) => {
                                cache.insert(scope.hash, scope.clone());
                                Ok(Some(scope))
                            }
                            Ok(None) => Ok(None),
                            Err(error) => {
                                error!("scope_store: db error: {error}");
                                Err(error)
                            }
                        }
                    };
                    let _ = reply.send(scope);
                }

                ScopeStoreMsg::GetHeadSnapshot { reply } => {
                    let snap = cache.get(&current_head).and_then(|s| s.snapshot.clone());
                    let _ = reply.send(snap);
                }

                ScopeStoreMsg::CreateRoot { snapshot, reply } => {
                    let scope = Scope::root(snapshot);
                    let hash = scope.hash;
                    let scope_for_db = scope.clone();
                    if let Err(e) = storage::with_connection(&db, move |conn| {
                        storage::insert_scope(conn, &scope_for_db)
                            .and_then(|_| storage::set_head(conn, &hash))
                    })
                    .await
                    {
                        error!("scope_store: persist root failed: {e}");
                        let _ = reply.send(Err(anyhow::anyhow!("persist root scope {hash}: {e}")));
                        continue;
                    }
                    cache.insert(hash, scope);

                    let old_hash = current_head;
                    current_head = hash;

                    let _ = sys
                        .event_bus
                        .send(EventBusMsg::Publish {
                            payload: EventPayload::HeadChanged {
                                old_hash: old_hash.to_string(),
                                new_hash: current_head.to_string(),
                            },
                            channel: "scopes".into(),
                        })
                        .await;

                    let _ = reply.send(Ok(hash));
                }

                ScopeStoreMsg::Fork { delta, reply } => {
                    let parent_scope = cache.get(&current_head).cloned();
                    let Some(parent) = parent_scope else {
                        let _ = reply.send(Err(anyhow::anyhow!(
                            "HEAD scope {} not in cache",
                            current_head
                        )));
                        continue;
                    };
                    let Some(ref parent_snap) = parent.snapshot else {
                        let _ = reply.send(Err(anyhow::anyhow!(
                            "HEAD scope {} has no snapshot",
                            current_head
                        )));
                        continue;
                    };

                    let child = Scope::fork(current_head, parent_snap, delta);
                    let child_hash = child.hash;
                    let child_for_db = child.clone();
                    if let Err(e) = storage::with_connection(&db, move |conn| {
                        storage::insert_scope(conn, &child_for_db)
                            .and_then(|_| storage::set_head(conn, &child_hash))
                    })
                    .await
                    {
                        error!("scope_store: persist fork failed: {e}");
                        let _ = reply.send(Err(anyhow::anyhow!(
                            "persist forked scope {child_hash}: {e}"
                        )));
                        continue;
                    }
                    cache.insert(child_hash, child);

                    let old_hash = current_head;
                    current_head = child_hash;

                    let _ = sys
                        .event_bus
                        .send(EventBusMsg::Publish {
                            payload: EventPayload::HeadChanged {
                                old_hash: old_hash.to_string(),
                                new_hash: current_head.to_string(),
                            },
                            channel: "scopes".into(),
                        })
                        .await;

                    let _ = reply.send(Ok(child_hash));
                }

                ScopeStoreMsg::Derive { base, delta, reply } => {
                    let parent_scope = if let Some(scope) = cache.get(&base) {
                        Some(scope.clone())
                    } else {
                        match storage::with_connection(&db, move |conn| {
                            storage::get_scope(conn, &base)
                        })
                        .await
                        {
                            Ok(Some(scope)) => {
                                cache.insert(scope.hash, scope.clone());
                                Some(scope)
                            }
                            Ok(None) => None,
                            Err(e) => {
                                error!("scope_store: db error: {e}");
                                None
                            }
                        }
                    };
                    let Some(parent) = parent_scope else {
                        let _ = reply.send(Err(anyhow::anyhow!("scope {} not found", base)));
                        continue;
                    };
                    let Some(ref parent_snap) = parent.snapshot else {
                        let _ = reply.send(Err(anyhow::anyhow!(
                            "scope {} has no snapshot",
                            parent.hash
                        )));
                        continue;
                    };

                    let child = Scope::fork(parent.hash, parent_snap, delta);
                    let child_hash = child.hash;
                    let child_for_db = child.clone();
                    if let Err(e) = storage::with_connection(&db, move |conn| {
                        storage::insert_scope(conn, &child_for_db)
                    })
                    .await
                    {
                        error!("scope_store: persist derived scope failed: {e}");
                        let _ = reply.send(Err(anyhow::anyhow!(
                            "persist derived scope {child_hash}: {e}"
                        )));
                        continue;
                    }
                    cache.insert(child_hash, child);
                    let _ = reply.send(Ok(child_hash));
                }

                ScopeStoreMsg::Shutdown => {
                    debug!("scope_store: shutting down");
                    break;
                }

                ScopeStoreMsg::ListScopes { reply } => {
                    let mut scopes: Vec<cue_core::ipc::ScopeInfo> = cache
                        .values()
                        .map(|scope| {
                            let snapshot = scope.snapshot.as_ref();
                            cue_core::ipc::ScopeInfo {
                                hash: scope.hash.to_string(),
                                parent: scope.parent.map(|p| p.to_string()),
                                cwd: snapshot
                                    .map(|s| s.cwd.display().to_string())
                                    .unwrap_or_default(),
                                env_count: snapshot.map(|s| s.env.len()).unwrap_or(0),
                            }
                        })
                        .collect();
                    scopes.sort_by(|a, b| a.hash.cmp(&b.hash));
                    let _ = reply.send((current_head, scopes));
                }
            }
        }

        debug!("scope_store: stopped");
    });
}

async fn load_initial_scope(
    db: &storage::SharedConnection,
) -> Result<(HashMap<ScopeHash, Scope>, ScopeHash, bool)> {
    if let Some(head) = storage::with_connection(db, storage::get_head).await? {
        if let Some(scope) =
            storage::with_connection(db, move |conn| storage::get_scope(conn, &head)).await?
        {
            if scope.snapshot.is_none() {
                anyhow::bail!("persisted head scope {head} has no snapshot");
            }
            let mut cache = HashMap::new();
            cache.insert(scope.hash, scope);
            return Ok((cache, head, true));
        }
        anyhow::bail!("persisted head scope {head} is missing");
    }

    let (cache, head, restored) = create_and_persist_root_scope(db).await?;
    Ok((cache, head, restored))
}

async fn create_and_persist_root_scope(
    db: &storage::SharedConnection,
) -> Result<(HashMap<ScopeHash, Scope>, ScopeHash, bool)> {
    let mut cache = HashMap::new();

    let env: BTreeMap<String, String> = std::env::vars().collect();
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/"));
    let snapshot = EnvSnapshot { env, cwd };
    let root = Scope::root(snapshot);
    let head = root.hash;

    let root_for_db = root.clone();
    storage::with_connection(db, move |conn| {
        storage::insert_scope(conn, &root_for_db)?;
        storage::set_head(conn, &head)?;
        Ok(())
    })
    .await?;
    cache.insert(root.hash, root);

    Ok((cache, head, false))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::{ACTOR_CHANNEL_CAP, GatewayMsg, ProcessMgrMsg, SchedulerMsg};
    use cue_core::scope::EnvSnapshot;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::sync::oneshot;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn in_memory_db() -> Connection {
        storage::open_db(Path::new(":memory:")).expect("open in-memory db")
    }

    fn make_temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "cue-scope-store-test-{}-{}",
            std::process::id(),
            TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    #[test]
    fn load_initial_scope_restores_persisted_head() {
        let conn = in_memory_db();
        let snapshot = EnvSnapshot {
            env: BTreeMap::from([("PATH".into(), "/usr/bin".into())]),
            cwd: PathBuf::from("/tmp/persisted"),
        };
        let scope = Scope::root(snapshot);
        storage::insert_scope(&conn, &scope).unwrap();
        storage::set_head(&conn, &scope.hash).unwrap();

        let db = storage::shared_connection(conn);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (cache, head, restored) = rt.block_on(load_initial_scope(&db)).unwrap();

        assert!(restored);
        assert_eq!(head, scope.hash);
        let restored_scope = cache.get(&head).expect("restored scope in cache");
        assert_eq!(restored_scope.hash, scope.hash);
        assert_eq!(restored_scope.snapshot, scope.snapshot);
    }

    #[test]
    fn load_initial_scope_rejects_missing_persisted_head() {
        let conn = in_memory_db();
        let missing = ScopeHash([7; 32]);
        storage::set_head(&conn, &missing).expect("set missing head");

        let db = storage::shared_connection(conn);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let error = rt
            .block_on(load_initial_scope(&db))
            .expect_err("missing persisted head should fail");

        assert!(error.to_string().contains("persisted head scope"));
        assert!(error.to_string().contains("is missing"));
    }

    #[test]
    fn load_initial_scope_rejects_head_without_snapshot() {
        let conn = in_memory_db();
        let scope = Scope {
            hash: ScopeHash([8; 32]),
            parent: None,
            delta: None,
            snapshot: None,
        };
        storage::insert_scope(&conn, &scope).expect("insert invalid scope");
        storage::set_head(&conn, &scope.hash).expect("set head");

        let db = storage::shared_connection(conn);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let error = rt
            .block_on(load_initial_scope(&db))
            .expect_err("snapshotless persisted head should fail");

        assert!(error.to_string().contains("has no snapshot"));
    }

    #[tokio::test]
    async fn fork_reports_persist_error_without_advancing_head() {
        let dir = make_temp_dir();
        let db_path = dir.join("scope.db");
        let conn = storage::open_db(&db_path).expect("open scope db");

        let (gateway_tx, _gateway_rx) = mpsc::channel::<GatewayMsg>(ACTOR_CHANNEL_CAP);
        let (scheduler_tx, _scheduler_rx) = mpsc::channel::<SchedulerMsg>(ACTOR_CHANNEL_CAP);
        let (process_tx, _process_rx) = mpsc::channel::<ProcessMgrMsg>(ACTOR_CHANNEL_CAP);
        let (scope_tx, scope_rx) = mpsc::channel::<ScopeStoreMsg>(ACTOR_CHANNEL_CAP);
        let (event_tx, _event_rx) = mpsc::channel::<EventBusMsg>(ACTOR_CHANNEL_CAP);
        let sys = ActorSystem {
            gateway: gateway_tx,
            scheduler: scheduler_tx,
            process_mgr: process_tx,
            scope_store: scope_tx.clone(),
            event_bus: event_tx,
            config: crate::config::Config::default(),
        };
        spawn(scope_rx, conn, sys);

        let (head_tx, head_rx) = oneshot::channel();
        scope_tx
            .send(ScopeStoreMsg::GetHead { reply: head_tx })
            .await
            .expect("request head");
        let original_head = tokio::time::timeout(std::time::Duration::from_secs(1), head_rx)
            .await
            .expect("head reply")
            .expect("head sender");

        let external = Connection::open(&db_path).expect("open external db");
        external
            .execute_batch("DROP TABLE scopes;")
            .expect("drop scopes table");
        drop(external);

        let (fork_tx, fork_rx) = oneshot::channel();
        scope_tx
            .send(ScopeStoreMsg::Fork {
                delta: cue_core::scope::EnvDelta {
                    set: BTreeMap::from([("FOO".to_string(), "bar".to_string())]),
                    unset: vec![],
                    cwd: None,
                },
                reply: fork_tx,
            })
            .await
            .expect("request fork");
        let error = tokio::time::timeout(std::time::Duration::from_secs(1), fork_rx)
            .await
            .expect("fork reply")
            .expect("fork sender")
            .expect_err("fork should report persistence failure");
        assert!(error.to_string().contains("persist forked scope"));

        let (head_tx, head_rx) = oneshot::channel();
        scope_tx
            .send(ScopeStoreMsg::GetHead { reply: head_tx })
            .await
            .expect("request head after failed fork");
        let current_head = tokio::time::timeout(std::time::Duration::from_secs(1), head_rx)
            .await
            .expect("head reply after failed fork")
            .expect("head sender after failed fork");
        assert_eq!(current_head, original_head);

        let _ = scope_tx.send(ScopeStoreMsg::Shutdown).await;
        std::fs::remove_dir_all(dir).expect("remove temp dir");
    }

    #[tokio::test]
    async fn get_scope_reports_storage_errors() {
        let dir = make_temp_dir();
        let db_path = dir.join("scope.db");
        let conn = storage::open_db(&db_path).expect("open scope db");

        let (gateway_tx, _gateway_rx) = mpsc::channel::<GatewayMsg>(ACTOR_CHANNEL_CAP);
        let (scheduler_tx, _scheduler_rx) = mpsc::channel::<SchedulerMsg>(ACTOR_CHANNEL_CAP);
        let (process_tx, _process_rx) = mpsc::channel::<ProcessMgrMsg>(ACTOR_CHANNEL_CAP);
        let (scope_tx, scope_rx) = mpsc::channel::<ScopeStoreMsg>(ACTOR_CHANNEL_CAP);
        let (event_tx, _event_rx) = mpsc::channel::<EventBusMsg>(ACTOR_CHANNEL_CAP);
        let sys = ActorSystem {
            gateway: gateway_tx,
            scheduler: scheduler_tx,
            process_mgr: process_tx,
            scope_store: scope_tx.clone(),
            event_bus: event_tx,
            config: crate::config::Config::default(),
        };
        spawn(scope_rx, conn, sys);

        let (head_tx, head_rx) = oneshot::channel();
        scope_tx
            .send(ScopeStoreMsg::GetHead { reply: head_tx })
            .await
            .expect("request head");
        tokio::time::timeout(std::time::Duration::from_secs(1), head_rx)
            .await
            .expect("head reply")
            .expect("head sender");

        let external = Connection::open(&db_path).expect("open external db");
        external
            .execute_batch("DROP TABLE scopes;")
            .expect("drop scopes table");
        drop(external);

        let (scope_reply_tx, scope_reply_rx) = oneshot::channel();
        scope_tx
            .send(ScopeStoreMsg::GetScope {
                hash: ScopeHash([42; 32]),
                reply: scope_reply_tx,
            })
            .await
            .expect("request uncached scope");
        let error = tokio::time::timeout(std::time::Duration::from_secs(1), scope_reply_rx)
            .await
            .expect("scope reply")
            .expect("scope sender")
            .expect_err("storage failure should be reported");
        assert!(error.to_string().contains("no such table"));

        let _ = scope_tx.send(ScopeStoreMsg::Shutdown).await;
        std::fs::remove_dir_all(dir).expect("remove temp dir");
    }
}
