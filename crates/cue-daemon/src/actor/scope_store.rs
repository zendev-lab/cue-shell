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
                let (cache, head, _) = create_and_persist_root_scope(&db).await;
                (cache, head, false)
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
                    let scope = if let Some(s) = cache.get(&hash) {
                        Some(s.clone())
                    } else {
                        match storage::with_connection(&db, move |conn| {
                            storage::get_scope(conn, &hash)
                        })
                        .await
                        {
                            Ok(Some(s)) => {
                                cache.insert(s.hash, s.clone());
                                Some(s)
                            }
                            Ok(None) => None,
                            Err(e) => {
                                error!("scope_store: db error: {e}");
                                None
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
                    })
                    .await
                    {
                        error!("scope_store: persist root failed: {e}");
                    }
                    cache.insert(hash, scope);

                    let old_hash = current_head;
                    current_head = hash;
                    if let Err(error) = storage::with_connection(&db, move |conn| {
                        storage::set_head(conn, &current_head)
                    })
                    .await
                    {
                        error!("scope_store: persist head failed: {error}");
                    }

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

                    let _ = reply.send(hash);
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
                    })
                    .await
                    {
                        error!("scope_store: persist fork failed: {e}");
                    }
                    cache.insert(child_hash, child);

                    let old_hash = current_head;
                    current_head = child_hash;
                    if let Err(error) = storage::with_connection(&db, move |conn| {
                        storage::set_head(conn, &current_head)
                    })
                    .await
                    {
                        error!("scope_store: persist head failed: {error}");
                    }

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
            let mut cache = HashMap::new();
            cache.insert(scope.hash, scope);
            return Ok((cache, head, true));
        }
        error!(%head, "scope_store: persisted head is missing; recreating root");
    }

    let (cache, head, restored) = create_and_persist_root_scope(db).await;
    Ok((cache, head, restored))
}

async fn create_and_persist_root_scope(
    db: &storage::SharedConnection,
) -> (HashMap<ScopeHash, Scope>, ScopeHash, bool) {
    let mut cache = HashMap::new();

    let env: BTreeMap<String, String> = std::env::vars().collect();
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/"));
    let snapshot = EnvSnapshot { env, cwd };
    let root = Scope::root(snapshot);
    let head = root.hash;

    let root_for_db = root.clone();
    if let Err(e) = storage::with_connection(db, move |conn| {
        storage::insert_scope(conn, &root_for_db)?;
        storage::set_head(conn, &head)?;
        Ok(())
    })
    .await
    {
        error!("scope_store: failed to set initial head: {e}");
    }
    cache.insert(root.hash, root);

    (cache, head, false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cue_core::scope::EnvSnapshot;
    use std::path::Path;

    fn in_memory_db() -> Connection {
        storage::open_db(Path::new(":memory:")).expect("open in-memory db")
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
}
