use anyhow::{Context, Result};
use cue_client::{ClientConnector, CuedClient};

/// Attach an already-connected frontend before it is split for TUI use.
pub(crate) async fn attach_named_session(
    mut client: CuedClient,
    selector: &str,
    refresh_if_needed: bool,
) -> Result<CuedClient> {
    let attach = client
        .attach_session_with_refresh_if_needed(selector, refresh_if_needed)
        .await;
    let session = if refresh_if_needed {
        attach.with_context(|| {
            format!("attach named session `{selector}` with explicit restart recovery")
        })?
    } else {
        attach.with_context(|| {
            format!(
                "attach named session `{selector}`; if it reports needs_refresh after a daemon restart, restart cue-tui with `--session-refresh` or `CUE_SESSION_REFRESH=1` to explicitly replace its scope from this process environment"
            )
        })?
    };

    if !session.current {
        anyhow::bail!(
            "daemon did not make named session `{selector}` current after a successful attach"
        );
    }

    Ok(client)
}

/// Decorate a transport connector so every new connection re-attaches to the
/// selected named session before the connection manager exposes it to the UI.
pub(crate) fn connector_with_named_session(
    connector: ClientConnector,
    selector: Option<String>,
    refresh_if_needed: bool,
) -> ClientConnector {
    let Some(selector) = selector else {
        return connector;
    };

    ClientConnector::new(move || {
        let connector = connector.clone();
        let selector = selector.clone();
        async move {
            let client = connector.connect().await?;
            attach_named_session(client, &selector, refresh_if_needed).await
        }
    })
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, duplex};
    use tokio::sync::mpsc;

    use cue_core::ipc::{
        Message, OkPayload, RequestPayload, ResponsePayload, SessionInfo, SessionScopeState,
        encode_message, error_code,
    };

    use super::*;

    async fn read_message(stream: &mut DuplexStream) -> Message {
        let mut prefix = [0u8; 4];
        stream
            .read_exact(&mut prefix)
            .await
            .expect("read message length");
        let mut body = vec![0u8; u32::from_be_bytes(prefix) as usize];
        stream
            .read_exact(&mut body)
            .await
            .expect("read message body");
        serde_json::from_slice(&body).expect("decode message")
    }

    async fn write_message(stream: &mut DuplexStream, message: &Message) {
        stream
            .write_all(&encode_message(message).expect("encode message"))
            .await
            .expect("write message");
    }

    fn attached_session() -> SessionInfo {
        SessionInfo {
            id: "SS-00000000-0000-4000-8000-000000000001".into(),
            name: "shared".into(),
            scope_state: SessionScopeState::ReadyDurable,
            scope_hash: Some("scope-1".into()),
            connected_clients: 1,
            restart_safe: true,
            current: true,
            created_at_ms: 1,
            updated_at_ms: 2,
            archived_at_ms: None,
        }
    }

    fn test_connector() -> (ClientConnector, mpsc::UnboundedReceiver<DuplexStream>) {
        let (daemon_tx, daemon_rx) = mpsc::unbounded_channel();
        let connector = ClientConnector::new(move || {
            let daemon_tx = daemon_tx.clone();
            async move {
                let (client, daemon) = duplex(4096);
                daemon_tx.send(daemon).expect("send daemon stream");
                Ok(CuedClient::from_stream(client))
            }
        });
        (connector, daemon_rx)
    }

    #[tokio::test]
    async fn decorated_connector_attaches_every_connection_before_returning() {
        let (connector, mut daemon_rx) = test_connector();
        let connector = connector_with_named_session(connector, Some("shared".into()), false);

        for _ in 0..2 {
            let connect = tokio::spawn({
                let connector = connector.clone();
                async move { connector.connect().await }
            });
            let mut daemon = daemon_rx.recv().await.expect("daemon stream");
            let request_id = match read_message(&mut daemon).await {
                Message::Request {
                    id,
                    payload: RequestPayload::AttachSession { selector, refresh },
                    ..
                } => {
                    assert_eq!(selector, "shared");
                    assert!(!refresh);
                    id
                }
                message => panic!("unexpected attach message: {message:?}"),
            };
            write_message(
                &mut daemon,
                &Message::Response {
                    id: request_id,
                    payload: ResponsePayload::Ok(OkPayload::SessionInfo(Box::new(
                        attached_session(),
                    ))),
                },
            )
            .await;

            connect
                .await
                .expect("join connector")
                .expect("attach connection");
        }
    }

    #[tokio::test]
    async fn explicit_recovery_refreshes_only_after_needs_refresh_is_confirmed() {
        let (connector, mut daemon_rx) = test_connector();
        let connector = connector_with_named_session(connector, Some("shared".into()), true);

        let connect = tokio::spawn(async move { connector.connect().await });
        let mut daemon = daemon_rx.recv().await.expect("daemon stream");

        let initial_attach_id = match read_message(&mut daemon).await {
            Message::Request {
                id,
                payload: RequestPayload::AttachSession { selector, refresh },
                ..
            } => {
                assert_eq!(selector, "shared");
                assert!(!refresh, "restart recovery must begin with a safe probe");
                id
            }
            message => panic!("unexpected initial attach message: {message:?}"),
        };
        write_message(
            &mut daemon,
            &Message::Response {
                id: initial_attach_id,
                payload: ResponsePayload::err(
                    error_code::INVALID_STATE,
                    "volatile session scope was lost during daemon restart",
                ),
            },
        )
        .await;

        let info_id = match read_message(&mut daemon).await {
            Message::Request {
                id,
                payload: RequestPayload::SessionInfo { selector },
                ..
            } => {
                assert_eq!(selector.as_deref(), Some("shared"));
                id
            }
            message => panic!("expected session info probe, got {message:?}"),
        };
        let mut needs_refresh = attached_session();
        needs_refresh.scope_state = SessionScopeState::NeedsRefresh;
        needs_refresh.scope_hash = None;
        needs_refresh.connected_clients = 0;
        needs_refresh.restart_safe = false;
        needs_refresh.current = false;
        write_message(
            &mut daemon,
            &Message::Response {
                id: info_id,
                payload: ResponsePayload::Ok(OkPayload::SessionInfo(Box::new(needs_refresh))),
            },
        )
        .await;

        let refresh_id = match read_message(&mut daemon).await {
            Message::Request {
                id,
                payload: RequestPayload::AttachSession { selector, refresh },
                ..
            } => {
                assert_eq!(selector, "shared");
                assert!(refresh, "confirmed recovery must explicitly replace scope");
                id
            }
            message => panic!("expected explicit refresh attach, got {message:?}"),
        };
        write_message(
            &mut daemon,
            &Message::Response {
                id: refresh_id,
                payload: ResponsePayload::Ok(OkPayload::SessionInfo(Box::new(attached_session()))),
            },
        )
        .await;

        connect
            .await
            .expect("join connector")
            .expect("recover session connection");
    }

    #[tokio::test]
    async fn decorated_connector_surfaces_attach_failure_with_selector() {
        let (connector, mut daemon_rx) = test_connector();
        let connector = connector_with_named_session(connector, Some("missing".into()), false);

        let connect = tokio::spawn(async move { connector.connect().await });
        let mut daemon = daemon_rx.recv().await.expect("daemon stream");
        let request_id = match read_message(&mut daemon).await {
            Message::Request {
                id,
                payload: RequestPayload::AttachSession { selector, .. },
                ..
            } => {
                assert_eq!(selector, "missing");
                id
            }
            message => panic!("unexpected attach message: {message:?}"),
        };
        write_message(
            &mut daemon,
            &Message::Response {
                id: request_id,
                payload: ResponsePayload::err(error_code::NOT_FOUND, "named session not found"),
            },
        )
        .await;

        let error = match connect.await.expect("join connector") {
            Ok(_) => panic!("missing session attach should fail"),
            Err(error) => error,
        };
        let message = format!("{error:#}");
        assert!(
            message.contains("attach named session `missing`"),
            "{message}"
        );
        assert!(message.contains("named session not found"), "{message}");
        assert!(message.contains("--session-refresh"), "{message}");
    }
}
