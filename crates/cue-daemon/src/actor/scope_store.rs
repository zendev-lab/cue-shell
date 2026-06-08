//! ScopeStore actor — environment scope management.
//!
//! Maintains an in-memory cache backed by SQLite.  The "HEAD" pointer
//! tracks the current active scope (analogous to git HEAD).

use anyhow::{Context, Result};
use std::collections::{BTreeMap, HashMap};

use rusqlite::Connection;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

use cue_core::scope::{EnvSnapshot, Scope};
use cue_core::{EventChannel, ScopeHash};

use super::{ActorSystem, ScopeStoreMsg, publish_event as publish_actor_event};
use crate::storage;

use cue_core::ipc::EventPayload;

/// Spawn the ScopeStore actor task.
///
/// Initialises a root scope from the current process environment.
pub(super) async fn spawn(
    mut rx: mpsc::Receiver<ScopeStoreMsg>,
    conn: Connection,
    sys: ActorSystem,
) -> Result<()> {
    let db = storage::shared_connection(conn);
    let (mut cache, mut current_head, restored) = load_initial_scope(&db).await?;

    tokio::spawn(async move {
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
                    let scope = load_scope(&mut cache, &db, hash).await;
                    if let Err(error) = &scope {
                        error!("scope_store: db error: {error}");
                    }
                    let _ = reply.send(scope);
                }

                ScopeStoreMsg::GetHeadSnapshot { reply } => {
                    let snapshot = match load_scope(&mut cache, &db, current_head).await {
                        Ok(Some(scope)) => scope.snapshot.ok_or_else(|| {
                            anyhow::anyhow!("HEAD scope {current_head} has no snapshot")
                        }),
                        Ok(None) => Err(anyhow::anyhow!("HEAD scope {current_head} not found")),
                        Err(error) => {
                            Err(anyhow::anyhow!("load HEAD scope {current_head}: {error}"))
                        }
                    };
                    if let Err(error) = &snapshot {
                        error!("scope_store: get head snapshot failed: {error}");
                    }
                    let _ = reply.send(snapshot);
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

                    publish_actor_event(
                        "scope_store",
                        &sys.event_bus,
                        EventChannel::Scopes,
                        EventPayload::HeadChanged {
                            old_hash: old_hash.to_string(),
                            new_hash: current_head.to_string(),
                        },
                    )
                    .await;

                    let _ = reply.send(Ok(child_hash));
                }

                ScopeStoreMsg::Derive { base, delta, reply } => {
                    let parent_scope = match load_scope(&mut cache, &db, base).await {
                        Ok(scope) => scope,
                        Err(error) => {
                            error!("scope_store: db error: {error}");
                            let _ =
                                reply.send(Err(anyhow::anyhow!("load base scope {base}: {error}")));
                            continue;
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
                    let scopes = match storage::with_connection(&db, storage::list_scopes).await {
                        Ok(scopes) => scopes,
                        Err(error) => {
                            error!("scope_store: list scopes failed: {error}");
                            let _ =
                                reply.send(Err(anyhow::anyhow!("list persisted scopes: {error}")));
                            continue;
                        }
                    };
                    let mut scope_infos = Vec::with_capacity(scopes.len());
                    let mut list_error = None;
                    for scope in scopes {
                        let info = match scope_info(&scope) {
                            Ok(info) => info,
                            Err(error) => {
                                list_error = Some(error);
                                break;
                            }
                        };
                        cache.insert(scope.hash, scope.clone());
                        scope_infos.push(info);
                    }
                    if let Some(error) = list_error {
                        error!("scope_store: list scopes failed: {error}");
                        let _ = reply.send(Err(anyhow::anyhow!("list persisted scopes: {error}")));
                        continue;
                    }
                    let mut scopes = scope_infos;
                    scopes.sort_by(|a, b| a.hash.cmp(&b.hash));
                    let _ = reply.send(Ok((current_head, scopes)));
                }
            }
        }

        debug!("scope_store: stopped");
    });
    Ok(())
}

async fn load_scope(
    cache: &mut HashMap<ScopeHash, Scope>,
    db: &storage::SharedConnection,
    hash: ScopeHash,
) -> Result<Option<Scope>> {
    if let Some(scope) = cache.get(&hash) {
        return Ok(Some(scope.clone()));
    }

    let scope = storage::with_connection(db, move |conn| storage::get_scope(conn, &hash)).await?;
    if let Some(scope) = &scope {
        cache.insert(scope.hash, scope.clone());
    }
    Ok(scope)
}

fn scope_info(scope: &Scope) -> Result<cue_core::ipc::ScopeInfo> {
    let snapshot = scope
        .snapshot
        .as_ref()
        .with_context(|| format!("scope {} has no snapshot", scope.hash))?;
    Ok(cue_core::ipc::ScopeInfo {
        hash: scope.hash.to_string(),
        parent: scope.parent.map(|p| p.to_string()),
        cwd: snapshot.cwd.display().to_string(),
        env_count: snapshot.env.len(),
    })
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
    let cwd = std::env::current_dir().context("read current working directory for root scope")?;
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
    use crate::actor::{ACTOR_CHANNEL_CAP, EventBusMsg, GatewayMsg, ProcessMgrMsg, SchedulerMsg};
    use cue_core::scope::EnvSnapshot;
    use std::path::{Path, PathBuf};
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
        spawn(scope_rx, conn, sys).await.expect("spawn scope store");

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
        spawn(scope_rx, conn, sys).await.expect("spawn scope store");

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

    #[tokio::test]
    async fn derive_reports_base_scope_storage_errors() {
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
        spawn(scope_rx, conn, sys).await.expect("spawn scope store");

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

        let (derive_tx, derive_rx) = oneshot::channel();
        scope_tx
            .send(ScopeStoreMsg::Derive {
                base: ScopeHash([99; 32]),
                delta: cue_core::scope::EnvDelta {
                    set: BTreeMap::from([("FOO".to_string(), "bar".to_string())]),
                    unset: vec![],
                    cwd: None,
                },
                reply: derive_tx,
            })
            .await
            .expect("request derive");
        let error = tokio::time::timeout(std::time::Duration::from_secs(1), derive_rx)
            .await
            .expect("derive reply")
            .expect("derive sender")
            .expect_err("storage failure should be reported");
        assert!(error.to_string().contains("load base scope"));
        assert!(error.to_string().contains("no such table"));

        let _ = scope_tx.send(ScopeStoreMsg::Shutdown).await;
        std::fs::remove_dir_all(dir).expect("remove temp dir");
    }

    #[tokio::test]
    async fn list_scopes_reads_persisted_scopes_not_only_cache() {
        let dir = make_temp_dir();
        let db_path = dir.join("scope.db");
        let conn = storage::open_db(&db_path).expect("open scope db");
        let root = Scope::root(EnvSnapshot {
            env: BTreeMap::from([("PATH".into(), "/usr/bin".into())]),
            cwd: PathBuf::from("/tmp/root"),
        });
        let child = Scope::fork(
            root.hash,
            root.snapshot.as_ref().expect("root snapshot"),
            cue_core::scope::EnvDelta {
                set: BTreeMap::new(),
                unset: vec![],
                cwd: Some(PathBuf::from("/tmp/child")),
            },
        );
        storage::insert_scope(&conn, &root).expect("insert root scope");
        storage::insert_scope(&conn, &child).expect("insert child scope");
        storage::set_head(&conn, &child.hash).expect("set head");

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
        spawn(scope_rx, conn, sys).await.expect("spawn scope store");

        let (reply_tx, reply_rx) = oneshot::channel();
        scope_tx
            .send(ScopeStoreMsg::ListScopes { reply: reply_tx })
            .await
            .expect("request list scopes");
        let (_head, scopes) = tokio::time::timeout(std::time::Duration::from_secs(1), reply_rx)
            .await
            .expect("list scopes reply")
            .expect("list scopes sender")
            .expect("list scopes result");
        let hashes = scopes
            .iter()
            .map(|scope| scope.hash.as_str())
            .collect::<Vec<_>>();
        let root_hash = root.hash.to_string();
        let child_hash = child.hash.to_string();

        assert!(hashes.contains(&root_hash.as_str()));
        assert!(hashes.contains(&child_hash.as_str()));

        let _ = scope_tx.send(ScopeStoreMsg::Shutdown).await;
        std::fs::remove_dir_all(dir).expect("remove temp dir");
    }

    #[tokio::test]
    async fn list_scopes_rejects_scope_without_snapshot() {
        let dir = make_temp_dir();
        let db_path = dir.join("scope.db");
        let conn = storage::open_db(&db_path).expect("open scope db");
        let root = Scope::root(EnvSnapshot {
            env: BTreeMap::from([(String::from("PATH"), String::from("/usr/bin"))]),
            cwd: PathBuf::from("/tmp/root"),
        });
        let missing_snapshot = Scope {
            hash: ScopeHash([9; 32]),
            parent: Some(root.hash),
            delta: None,
            snapshot: None,
        };
        storage::insert_scope(&conn, &root).expect("insert root scope");
        storage::insert_scope(&conn, &missing_snapshot).expect("insert broken scope");
        storage::set_head(&conn, &root.hash).expect("set head");

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
        spawn(scope_rx, conn, sys).await.expect("spawn scope store");

        let (reply_tx, reply_rx) = oneshot::channel();
        scope_tx
            .send(ScopeStoreMsg::ListScopes { reply: reply_tx })
            .await
            .expect("request list scopes");
        let error = tokio::time::timeout(std::time::Duration::from_secs(1), reply_rx)
            .await
            .expect("list scopes reply")
            .expect("list scopes sender")
            .expect_err("snapshotless scope should be reported");
        assert!(error.to_string().contains("has no snapshot"));

        let _ = scope_tx.send(ScopeStoreMsg::Shutdown).await;
        std::fs::remove_dir_all(dir).expect("remove temp dir");
    }

    #[tokio::test]
    async fn list_scopes_reports_storage_errors() {
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
        spawn(scope_rx, conn, sys).await.expect("spawn scope store");

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

        let (reply_tx, reply_rx) = oneshot::channel();
        scope_tx
            .send(ScopeStoreMsg::ListScopes { reply: reply_tx })
            .await
            .expect("request list scopes");
        let error = tokio::time::timeout(std::time::Duration::from_secs(1), reply_rx)
            .await
            .expect("list scopes reply")
            .expect("list scopes sender")
            .expect_err("storage failure should be reported");
        assert!(error.to_string().contains("list persisted scopes"));

        let _ = scope_tx.send(ScopeStoreMsg::Shutdown).await;
        std::fs::remove_dir_all(dir).expect("remove temp dir");
    }
}
