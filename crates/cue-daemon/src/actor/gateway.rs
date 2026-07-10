//! Gateway actor — Unix socket listener, per-client handlers, message framing.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::{Context, Result, bail};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, watch};
use tracing::{debug, error, info, warn};

use cue_core::EventChannel;
use cue_core::command_spec::{command_names, command_spec, mode_param_specs_for_command};
use cue_core::ipc::{
    CompletionItem, CompletionKind, EventPayload, HighlightKind, HighlightSpan,
    IPC_PROTOCOL_VERSION, MAX_MESSAGE_SIZE, Message, OkPayload, RequestPayload, ResponsePayload,
    current_protocol_capabilities, encode_message, error_code,
};
use cue_core::scope::EnvSnapshot;

use crate::parser::{ResolvedCommand, Token, Tokenizer, parse_command, parse_file_script_command};

use super::operation_ledger::{BeginOutcome, OperationLedger, OperationWaiter};
use super::{ActorSystem, CLIENT_EVENT_CAP, EventBusMsg, GatewayMsg, SchedulerMsg};

/// Next client id counter (global, atomic).
static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

// ── Message framing ──

/// Read one length-prefixed JSON message from the stream.
pub(crate) async fn read_message<R>(stream: &mut R) -> Result<Message>
where
    R: AsyncRead + Unpin,
{
    let len = stream.read_u32().await.context("read length prefix")?;
    if len as usize > MAX_MESSAGE_SIZE {
        bail!("message too large: {len} bytes");
    }
    let mut buf = vec![0u8; len as usize];
    stream.read_exact(&mut buf).await.context("read body")?;
    let msg: Message = serde_json::from_slice(&buf).context("deserialize message")?;
    Ok(msg)
}

/// Write one length-prefixed JSON message to the stream.
pub(crate) async fn write_message<W>(stream: &mut W, msg: &Message) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let encoded = encode_message(msg)?;
    stream.write_all(&encoded).await.context("write message")?;
    stream.flush().await.context("flush")?;
    Ok(())
}

async fn write_client_message<W>(stream: &mut W, msg: &Message) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    tokio::time::timeout(CLIENT_WRITE_TIMEOUT, write_message(stream, msg))
        .await
        .context("client write timed out")?
}

const CLIENT_RESPONSE_CAP: usize = 64;
const MAX_INFLIGHT_REQUESTS_PER_CLIENT: usize = 1_024;
const CLIENT_WRITE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Clone)]
struct ClientQueues {
    responses: mpsc::Sender<(u32, ResponsePayload)>,
    events: mpsc::Sender<EventPayload>,
    disconnect: watch::Sender<bool>,
}

/// Shared registry for each client's bounded outbound queues.
type ClientMap = Arc<Mutex<HashMap<u64, ClientQueues>>>;
type SharedOperationLedger = Arc<Mutex<OperationLedger>>;

fn client_registry(clients: &ClientMap) -> MutexGuard<'_, HashMap<u64, ClientQueues>> {
    clients
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn operation_ledger(operations: &SharedOperationLedger) -> MutexGuard<'_, OperationLedger> {
    operations
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn reserve_request_id(inflight: &Arc<Mutex<HashSet<u32>>>, request_id: u32) -> bool {
    let mut inflight = inflight
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if inflight.len() >= MAX_INFLIGHT_REQUESTS_PER_CLIENT {
        return false;
    }
    inflight.insert(request_id)
}

fn release_request_id(inflight: &Arc<Mutex<HashSet<u32>>>, request_id: u32) {
    inflight
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&request_id);
}

/// Spawn the Gateway actor.
///
/// This creates a Unix socket listener and spawns a task that accepts connections.
/// Per-client handler tasks are spawned for each connection.
pub(super) async fn spawn(
    mut rx: mpsc::Receiver<GatewayMsg>,
    socket_path: PathBuf,
    sys: ActorSystem,
) -> Result<()> {
    // Startup owns stale-socket cleanup while holding the socket-specific
    // instance lock. The gateway must never unlink a path that may belong to a
    // live listener.
    let listener = bind_private_listener(&socket_path)?;

    info!(path = %socket_path.display(), "gateway: listening");

    // Shared state: client_id → bounded outbound queues and eviction signal.
    let clients: ClientMap = Arc::new(Mutex::new(HashMap::new()));
    // One daemon-lifetime ledger spans every transport connection.
    let operations: SharedOperationLedger = Arc::new(Mutex::new(OperationLedger::default()));

    let clients_for_dispatch = Arc::clone(&clients);
    let operations_for_dispatch = Arc::clone(&operations);

    // Accept loop — runs in its own task.
    let sys_accept = sys.clone();
    let operations_for_accept = Arc::clone(&operations);
    let accept_handle = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _addr)) => {
                    let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
                    info!(%client_id, "gateway: client connected");
                    let sys_clone = sys_accept.clone();
                    let clients_clone = Arc::clone(&clients_for_dispatch);
                    let operations_clone = Arc::clone(&operations_for_accept);
                    tokio::spawn(handle_client(
                        client_id,
                        stream,
                        sys_clone,
                        clients_clone,
                        operations_clone,
                    ));
                }
                Err(e) => {
                    error!("gateway: accept error: {e}");
                }
            }
        }
    });

    // Dispatch loop — routes responses/events back to clients.
    tokio::spawn(async move {
        let mut accept_handle = Some(accept_handle);
        while let Some(msg) = rx.recv().await {
            match msg {
                GatewayMsg::SendResponse {
                    client_id,
                    request_id,
                    payload,
                } => {
                    let routed_request = OperationWaiter {
                        client_id,
                        request_id,
                    };
                    let completion = operation_ledger(&operations_for_dispatch)
                        .complete(routed_request, payload.clone());
                    if let Some(completion) = completion {
                        for waiter in completion.waiters {
                            queue_response_for_client(
                                &clients,
                                waiter.client_id,
                                waiter.request_id,
                                completion.response.clone(),
                            );
                        }
                    } else {
                        queue_response_for_client(&clients, client_id, request_id, payload);
                    }
                }

                GatewayMsg::SendEvent { client_id, payload } => {
                    queue_event_for_client(&clients, client_id, payload);
                }

                GatewayMsg::Shutdown => {
                    info!("gateway: shutdown signal received");
                    if let Some(handle) = accept_handle.take() {
                        handle.abort();
                        let _ = handle.await;
                    }
                    break;
                }
            }
        }

        if let Some(handle) = accept_handle.take() {
            handle.abort();
            let _ = handle.await;
        }

        debug!("gateway: dispatch loop stopped");
    });

    Ok(())
}

fn bind_private_listener(socket_path: &Path) -> Result<UnixListener> {
    let listener = UnixListener::bind(socket_path)
        .with_context(|| format!("bind socket {}", socket_path.display()))?;
    if let Err(error) = crate::dirs::secure_private_file(socket_path) {
        drop(listener);
        let _ = std::fs::remove_file(socket_path);
        return Err(error).with_context(|| format!("secure socket {}", socket_path.display()));
    }
    Ok(listener)
}

/// Handle one client connection.
async fn handle_client(
    client_id: u64,
    stream: UnixStream,
    sys: ActorSystem,
    clients: ClientMap,
    operations: SharedOperationLedger,
) {
    // Per-client response channel.
    let (resp_tx, mut resp_rx) = mpsc::channel::<(u32, ResponsePayload)>(CLIENT_RESPONSE_CAP);
    // Per-client event channel.
    let (evt_tx, mut evt_rx) = mpsc::channel::<EventPayload>(CLIENT_EVENT_CAP);
    let (disconnect_tx, mut disconnect_rx) = watch::channel(false);
    let inflight_request_ids = Arc::new(Mutex::new(HashSet::new()));

    // Register.
    client_registry(&clients).insert(
        client_id,
        ClientQueues {
            responses: resp_tx,
            events: evt_tx.clone(),
            disconnect: disconnect_tx.clone(),
        },
    );
    let mut session_namespace = None;

    // Framing reads are not cancellation-safe. A dedicated reader owns each
    // full length-prefix/body read so outbound traffic can never drop a
    // partially consumed inbound frame.
    let (mut reader, mut writer) = stream.into_split();
    let (incoming_tx, mut incoming_rx) = mpsc::channel(CLIENT_RESPONSE_CAP);
    let reader_handle = tokio::spawn(async move {
        loop {
            let message = read_message(&mut reader)
                .await
                .map_err(|error| error.to_string());
            let terminal = message.is_err();
            if incoming_tx.send(message).await.is_err() || terminal {
                break;
            }
        }
    });

    loop {
        tokio::select! {
            // Receive complete frames from the non-cancellable reader loop.
            msg_result = incoming_rx.recv() => {
                let Some(msg_result) = msg_result else {
                    break;
                };
                match msg_result {
                    Ok(Message::Request {
                        id,
                        operation_id,
                        payload,
                    }) => {
                        if !reserve_request_id(&inflight_request_ids, id) {
                            warn!(
                                %client_id,
                                request_id = id,
                                "gateway: disconnecting client after duplicate or excessive in-flight request ids"
                            );
                            break;
                        }
                        let waiter = OperationWaiter {
                            client_id,
                            request_id: id,
                        };
                        let outcome = idempotency_outcome(
                            &operations,
                            session_namespace,
                            operation_id.as_deref(),
                            &payload,
                            waiter,
                        );
                        let should_route = match outcome {
                            Ok(BeginOutcome::Route) => true,
                            Ok(BeginOutcome::Wait) => false,
                            Ok(BeginOutcome::Respond(payload)) => {
                                if sys.gateway.send(GatewayMsg::SendResponse {
                                    client_id,
                                    request_id: id,
                                    payload,
                                }).await.is_err() {
                                    break;
                                }
                                false
                            }
                            Err(error) => {
                                if sys.gateway.send(GatewayMsg::SendResponse {
                                    client_id,
                                    request_id: id,
                                    payload: ResponsePayload::err(
                                        error_code::INTERNAL,
                                        error.to_string(),
                                    ),
                                }).await.is_err() {
                                    break;
                                }
                                false
                            }
                        };
                        if should_route {
                            match route_request(
                                client_id,
                                id,
                                payload,
                                &sys,
                                &evt_tx,
                                &disconnect_tx,
                            ).await {
                                Ok(Some(established_namespace)) => {
                                    session_namespace = Some(established_namespace);
                                }
                                Ok(None) => {}
                                Err(e) => {
                                    warn!(%client_id, "gateway: route error: {e}");
                                    if sys.gateway.send(GatewayMsg::SendResponse {
                                        client_id,
                                        request_id: id,
                                        payload: ResponsePayload::err(
                                            error_code::INTERNAL,
                                            e.to_string(),
                                        ),
                                    }).await.is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    Ok(_) => {
                        // Clients should only send Request messages.
                        warn!(%client_id, "gateway: unexpected non-request message");
                    }
                    Err(e) => {
                        debug!(%client_id, "gateway: read error (disconnect?): {e}");
                        break;
                    }
                }
            }

            // Deliver response back to client.
            Some((request_id, payload)) = resp_rx.recv() => {
                let msg = Message::Response { id: request_id, payload };
                if write_client_message(&mut writer, &msg).await.is_err() {
                    break;
                }
                // Keep the fence until bytes are written, not merely queued.
                release_request_id(&inflight_request_ids, request_id);
            }

            // Deliver pushed event to client.
            Some(event) = evt_rx.recv() => {
                let msg = Message::Event { payload: event };
                if write_client_message(&mut writer, &msg).await.is_err() {
                    break;
                }
            }

            changed = disconnect_rx.changed() => {
                if changed.is_ok() && *disconnect_rx.borrow() {
                    warn!(%client_id, "gateway: disconnecting evicted client");
                }
                break;
            }
        }
    }

    // Cleanup.
    reader_handle.abort();
    let _ = reader_handle.await;
    info!(%client_id, "gateway: client disconnected");
    client_registry(&clients).remove(&client_id);
    operation_ledger(&operations).remove_waiters_for_client(client_id);
    if sys
        .event_bus
        .send(EventBusMsg::UnsubscribeAll { client_id })
        .await
        .is_err()
    {
        debug!(%client_id, "gateway: event bus unavailable during client cleanup");
    }
    if sys
        .process_mgr
        .send(super::ProcessMgrMsg::DetachFg {
            client_id,
            reason: "client disconnected".into(),
        })
        .await
        .is_err()
    {
        debug!(%client_id, "gateway: process manager unavailable during client cleanup");
    }
    if sys
        .scheduler
        .send(SchedulerMsg::Disconnect { client_id })
        .await
        .is_err()
    {
        debug!(%client_id, "gateway: scheduler unavailable during client cleanup");
    }
}

fn idempotency_outcome(
    operations: &SharedOperationLedger,
    session_namespace: Option<[u8; 32]>,
    operation_id: Option<&str>,
    payload: &RequestPayload,
    waiter: OperationWaiter,
) -> Result<BeginOutcome> {
    let Some(operation_id) = operation_id else {
        return Ok(BeginOutcome::Route);
    };
    if !is_side_effecting_request(payload) {
        return Ok(BeginOutcome::Respond(ResponsePayload::err(
            error_code::INVALID_REQUEST,
            "operation_id is supported only for daemon-global side-effecting requests",
        )));
    }
    let Some(session_namespace) = session_namespace else {
        return Ok(BeginOutcome::Respond(ResponsePayload::err(
            error_code::INVALID_REQUEST,
            "operation_id requires a successful session handshake",
        )));
    };
    let fingerprint = OperationLedger::fingerprint(payload).context("fingerprint IPC request")?;
    Ok(operation_ledger(operations).begin(session_namespace, operation_id, fingerprint, waiter))
}

fn is_side_effecting_request(payload: &RequestPayload) -> bool {
    matches!(
        payload,
        RequestPayload::Eval { .. }
            | RequestPayload::RunScript { .. }
            | RequestPayload::KillJob { .. }
            | RequestPayload::CancelExecution { .. }
            | RequestPayload::RemoveCron { .. }
            | RequestPayload::Shutdown {}
    )
}

/// Route an incoming request to the appropriate actor.
async fn route_request(
    client_id: u64,
    request_id: u32,
    payload: RequestPayload,
    sys: &ActorSystem,
    evt_tx: &mpsc::Sender<EventPayload>,
    disconnect_tx: &watch::Sender<bool>,
) -> Result<Option<[u8; 32]>> {
    match payload {
        RequestPayload::Handshake {
            session_id,
            cwd,
            env,
            refresh,
        } => {
            let namespace_session_id = session_id.clone();
            let snapshot = EnvSnapshot {
                env,
                cwd: PathBuf::from(cwd),
            };
            let (reply, result) = tokio::sync::oneshot::channel();
            sys.scheduler
                .send(SchedulerMsg::Connect {
                    client_id,
                    session_id,
                    snapshot,
                    refresh,
                    reply,
                })
                .await
                .context("send session handshake to scheduler")?;
            match result.await {
                Ok(Ok(binding)) => {
                    sys.gateway
                        .send(GatewayMsg::SendResponse {
                            client_id,
                            request_id,
                            payload: ResponsePayload::ack(),
                        })
                        .await
                        .context("send handshake ack")?;
                    return Ok(Some(OperationLedger::session_incarnation_namespace(
                        &namespace_session_id,
                        binding.incarnation,
                    )));
                }
                Ok(Err(error)) => {
                    sys.gateway
                        .send(GatewayMsg::SendResponse {
                            client_id,
                            request_id,
                            payload: ResponsePayload::err(error_code::INTERNAL, error.to_string()),
                        })
                        .await
                        .context("send handshake error")?;
                }
                Err(_) => {
                    sys.gateway
                        .send(GatewayMsg::SendResponse {
                            client_id,
                            request_id,
                            payload: ResponsePayload::err(
                                error_code::INTERNAL,
                                "scheduler session reply dropped",
                            ),
                        })
                        .await
                        .context("send handshake dropped error")?;
                }
            }
        }

        RequestPayload::Eval { input, mode } => {
            let input = sys.config.aliases.apply(&input);
            match parse_command(&input, mode) {
                Ok(command) => {
                    if matches!(
                        command,
                        ResolvedCommand::Script {
                            source: cue_core::ipc::ScriptSource::Inline,
                            ..
                        }
                    ) {
                        sys.gateway
                            .send(GatewayMsg::SendResponse {
                                client_id,
                                request_id,
                                payload: inline_script_disabled_response(),
                            })
                            .await
                            .context("send inline script rejection")?;
                        return Ok(None);
                    }
                    send_scheduler_eval(sys, client_id, request_id, command, "send to scheduler")
                        .await?;
                }
                Err(e) => {
                    sys.gateway
                        .send(GatewayMsg::SendResponse {
                            client_id,
                            request_id,
                            payload: ResponsePayload::err(
                                error_code::INVALID_SYNTAX,
                                syntax_error_message(&input, &e.to_string()),
                            ),
                        })
                        .await
                        .context("send error response")?;
                }
            }
        }

        RequestPayload::RunScript { path, input } => match parse_file_script_command(&input) {
            Ok(mut command) => {
                if let ResolvedCommand::Script { source, .. } = &mut command {
                    *source = cue_core::ipc::ScriptSource::File { path };
                }
                send_scheduler_eval(
                    sys,
                    client_id,
                    request_id,
                    command,
                    "send script to scheduler",
                )
                .await?;
            }
            Err(e) => {
                sys.gateway
                    .send(GatewayMsg::SendResponse {
                        client_id,
                        request_id,
                        payload: ResponsePayload::err(
                            error_code::INVALID_SYNTAX,
                            syntax_error_message(&input, &e.to_string()),
                        ),
                    })
                    .await
                    .context("send error response")?;
            }
        },

        RequestPayload::ListJobs { limit } => {
            send_scheduler_eval(
                sys,
                client_id,
                request_id,
                ResolvedCommand::ListJobs { limit },
                "send list jobs to scheduler",
            )
            .await?;
        }

        RequestPayload::ListCrons { limit } => {
            send_scheduler_eval(
                sys,
                client_id,
                request_id,
                ResolvedCommand::ListCrons { limit },
                "send list crons to scheduler",
            )
            .await?;
        }

        RequestPayload::ListScopes { limit } => {
            send_scheduler_eval(
                sys,
                client_id,
                request_id,
                ResolvedCommand::ListScopes { limit },
                "send list scopes to scheduler",
            )
            .await?;
        }

        RequestPayload::ScriptInfo { id } => {
            sys.scheduler
                .send(SchedulerMsg::ScriptInfo {
                    client_id,
                    request_id,
                    id,
                })
                .await
                .context("send script info query to scheduler")?;
        }

        RequestPayload::ShowLog {
            id,
            limit,
            tail_bytes,
        } => {
            send_scheduler_eval(
                sys,
                client_id,
                request_id,
                ResolvedCommand::ShowLog {
                    id,
                    limit,
                    tail_bytes,
                },
                "send show log to scheduler",
            )
            .await?;
        }

        RequestPayload::JobOutput {
            id,
            stdout_bytes,
            stderr_bytes,
        } => {
            send_scheduler_eval(
                sys,
                client_id,
                request_id,
                ResolvedCommand::JobOutput {
                    id,
                    stdout_bytes,
                    stderr_bytes,
                },
                "send job output to scheduler",
            )
            .await?;
        }

        RequestPayload::KillJob { id } => {
            send_scheduler_eval(
                sys,
                client_id,
                request_id,
                ResolvedCommand::KillJob { id },
                "send kill job to scheduler",
            )
            .await?;
        }

        RequestPayload::CancelExecution { id } => {
            send_scheduler_eval(
                sys,
                client_id,
                request_id,
                ResolvedCommand::CancelExecution { id },
                "send cancel execution to scheduler",
            )
            .await?;
        }

        RequestPayload::RemoveCron { id } => {
            send_scheduler_eval(
                sys,
                client_id,
                request_id,
                ResolvedCommand::RemoveCron { id },
                "send remove cron to scheduler",
            )
            .await?;
        }

        RequestPayload::ShowEnv { tail_bytes } => {
            send_scheduler_eval(
                sys,
                client_id,
                request_id,
                ResolvedCommand::ShowEnv { tail_bytes },
                "send show env to scheduler",
            )
            .await?;
        }

        RequestPayload::ShowConfig { tail_bytes } => {
            send_scheduler_eval(
                sys,
                client_id,
                request_id,
                ResolvedCommand::ShowConfig { tail_bytes },
                "send show config to scheduler",
            )
            .await?;
        }

        RequestPayload::Subscribe { channels } => {
            let channels = match EventChannel::parse_list(&channels) {
                Ok(channels) => channels,
                Err(error) => {
                    sys.gateway
                        .send(GatewayMsg::SendResponse {
                            client_id,
                            request_id,
                            payload: invalid_event_channel_response(error.input()),
                        })
                        .await
                        .context("send invalid subscribe response")?;
                    return Ok(None);
                }
            };
            for channel in channels {
                sys.event_bus
                    .send(EventBusMsg::Subscribe {
                        client_id,
                        channel,
                        sender: evt_tx.clone(),
                        disconnect: disconnect_tx.clone(),
                    })
                    .await?;
            }
            sys.gateway
                .send(GatewayMsg::SendResponse {
                    client_id,
                    request_id,
                    payload: ResponsePayload::ack(),
                })
                .await?;
        }

        RequestPayload::Unsubscribe { channels } => {
            let channels = match EventChannel::parse_list(&channels) {
                Ok(channels) => channels,
                Err(error) => {
                    sys.gateway
                        .send(GatewayMsg::SendResponse {
                            client_id,
                            request_id,
                            payload: invalid_event_channel_response(error.input()),
                        })
                        .await
                        .context("send invalid unsubscribe response")?;
                    return Ok(None);
                }
            };
            for channel in channels {
                sys.event_bus
                    .send(EventBusMsg::Unsubscribe { client_id, channel })
                    .await?;
            }
            sys.gateway
                .send(GatewayMsg::SendResponse {
                    client_id,
                    request_id,
                    payload: ResponsePayload::ack(),
                })
                .await?;
        }

        RequestPayload::FgAttach { id } => {
            send_scheduler_eval(
                sys,
                client_id,
                request_id,
                ResolvedCommand::Fg { id },
                "send fg attach to scheduler",
            )
            .await?;
        }

        RequestPayload::FgDetach {} => {
            sys.process_mgr
                .send(super::ProcessMgrMsg::DetachFg {
                    client_id,
                    reason: "detached".into(),
                })
                .await
                .context("send fg detach to process_mgr")?;
            sys.gateway
                .send(GatewayMsg::SendResponse {
                    client_id,
                    request_id,
                    payload: ResponsePayload::ack(),
                })
                .await?;
        }

        RequestPayload::FgInput { data } => {
            let (tx, rx) = tokio::sync::oneshot::channel();
            sys.process_mgr
                .send(super::ProcessMgrMsg::FgInput {
                    client_id,
                    data,
                    reply: tx,
                })
                .await
                .context("send fg input to process_mgr")?;
            let payload = match rx.await {
                Ok(Ok(())) => ResponsePayload::ack(),
                Ok(Err(message)) => ResponsePayload::err(error_code::INVALID_STATE, message),
                Err(_) => ResponsePayload::err(error_code::INTERNAL, "process_mgr unreachable"),
            };
            sys.gateway
                .send(GatewayMsg::SendResponse {
                    client_id,
                    request_id,
                    payload,
                })
                .await?;
        }

        RequestPayload::FgResize { cols, rows } => {
            let (tx, rx) = tokio::sync::oneshot::channel();
            sys.process_mgr
                .send(super::ProcessMgrMsg::FgResize {
                    client_id,
                    cols,
                    rows,
                    reply: tx,
                })
                .await
                .context("send fg resize to process_mgr")?;
            let payload = match rx.await {
                Ok(Ok(())) => ResponsePayload::ack(),
                Ok(Err(message)) => ResponsePayload::err(error_code::INVALID_STATE, message),
                Err(_) => ResponsePayload::err(error_code::INTERNAL, "process_mgr unreachable"),
            };
            sys.gateway
                .send(GatewayMsg::SendResponse {
                    client_id,
                    request_id,
                    payload,
                })
                .await?;
        }

        RequestPayload::Complete { input, cursor } => {
            sys.gateway
                .send(GatewayMsg::SendResponse {
                    client_id,
                    request_id,
                    payload: ResponsePayload::Ok(OkPayload::CompletionList {
                        items: complete_input(&input, cursor),
                    }),
                })
                .await?;
        }

        RequestPayload::Highlight { input } => {
            sys.gateway
                .send(GatewayMsg::SendResponse {
                    client_id,
                    request_id,
                    payload: ResponsePayload::Ok(OkPayload::HighlightResult {
                        spans: highlight_input(&input),
                    }),
                })
                .await?;
        }

        RequestPayload::Ping {} => {
            sys.gateway
                .send(GatewayMsg::SendResponse {
                    client_id,
                    request_id,
                    payload: ResponsePayload::Ok(OkPayload::Pong {
                        version: crate::version().to_string(),
                        instance_id: crate::daemon_instance_id().to_string(),
                        protocol_version: IPC_PROTOCOL_VERSION,
                        capabilities: current_protocol_capabilities(),
                    }),
                })
                .await?;
        }

        RequestPayload::Shutdown {} => {
            info!("gateway: shutdown request from client {client_id}");
            sys.gateway
                .send(GatewayMsg::SendResponse {
                    client_id,
                    request_id,
                    payload: ResponsePayload::ack(),
                })
                .await?;
            // Signal the main process so async_main performs a full coordinated shutdown.
            unsafe {
                libc::kill(std::process::id() as i32, libc::SIGTERM);
            }
        }
    }

    Ok(None)
}

async fn send_scheduler_eval(
    sys: &ActorSystem,
    client_id: u64,
    request_id: u32,
    command: ResolvedCommand,
    context: &'static str,
) -> Result<()> {
    sys.scheduler
        .send(SchedulerMsg::Eval {
            client_id,
            request_id,
            command: Box::new(command),
        })
        .await
        .context(context)
}

fn queue_response_for_client(
    clients: &ClientMap,
    client_id: u64,
    request_id: u32,
    payload: ResponsePayload,
) {
    let client = client_registry(clients).get(&client_id).cloned();

    if let Some(client) = client {
        match client.responses.try_send((request_id, payload)) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!(%client_id, "gateway: evicting lagging client with full response queue");
                evict_client(clients, client_id);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                warn!(%client_id, "gateway: evicting client with closed response queue");
                evict_client(clients, client_id);
            }
        }
    } else {
        warn!(%client_id, "gateway: no such client for response");
    }
}

fn queue_event_for_client(clients: &ClientMap, client_id: u64, payload: EventPayload) {
    let client = client_registry(clients).get(&client_id).cloned();

    if let Some(client) = client {
        match client.events.try_send(payload) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!(%client_id, "gateway: evicting lagging client with full direct-event queue");
                evict_client(clients, client_id);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                warn!(%client_id, "gateway: evicting client with closed direct-event queue");
                evict_client(clients, client_id);
            }
        }
    } else {
        warn!(%client_id, "gateway: no such client for direct event");
    }
}

fn evict_client(clients: &ClientMap, client_id: u64) {
    let client = client_registry(clients).remove(&client_id);
    if let Some(client) = client {
        let _ = client.disconnect.send(true);
    }
}

fn inline_script_disabled_response() -> ResponsePayload {
    ResponsePayload::err(
        error_code::NOT_SUPPORTED,
        "interactive multiline script submissions have been removed; write the items to a .cue file and run `cue run path/to/file.cue`",
    )
}

fn invalid_event_channel_response(channel: &str) -> ResponsePayload {
    ResponsePayload::err(
        error_code::INVALID_REQUEST,
        format!(
            "invalid event channel `{channel}`; expected {}",
            EventChannel::EXPECTED
        ),
    )
}

fn syntax_error_message(input: &str, base: &str) -> String {
    let hints = bash_syntax_hints(input);
    if hints.is_empty() {
        base.to_string()
    } else {
        format!(
            "{base}\n\nPossible bash syntax issue:\n- {}",
            hints.join("\n- ")
        )
    }
}

fn bash_syntax_hints(input: &str) -> Vec<&'static str> {
    let mut hints = Vec::new();
    if input.contains(';') {
        hints.push("cue-shell does not use ';' command separators; use a script submission or cue-shell chain operators such as '->' or '~>'");
    }
    if input.contains("$(") || input.contains('`') {
        hints.push(
            "command substitution is shell syntax; use an explicit helper command/script instead",
        );
    }
    if input.contains("2>") || input.contains("1>") || input.contains(" >") || input.contains("<") {
        hints.push("redirection is shell syntax; use cue-shell pipes '|>'/'|&>' or write/read files explicitly");
    }
    if input.contains(" | ")
        && !input.contains("|>")
        && !input.contains("|&>")
        && !input.contains("|!>")
    {
        hints.push("bare '|' is shell syntax; use cue-shell '|>' for stdout pipes or '|&>' for stdout+stderr pipes");
    }
    hints
}

fn complete_input(input: &str, cursor: usize) -> Vec<CompletionItem> {
    let prefix = prefix_before_cursor(input, cursor).trim_start();

    if let Some((command, param_prefix)) = mode_param_key_prefix(prefix) {
        return mode_param_specs_for_command(command)
            .filter(|param| param.name.starts_with(param_prefix))
            .map(|param| CompletionItem {
                label: param.name.into(),
                insert_text: format!("{}={}", param.name, param.value_hint),
                kind: CompletionKind::Param,
                detail: Some(param.detail.into()),
            })
            .collect();
    }

    if let Some(command_prefix) = prefix.strip_prefix(':') {
        let word = command_prefix
            .rsplit_once(char::is_whitespace)
            .map(|(_, word)| word)
            .unwrap_or(command_prefix);
        return command_names()
            .filter_map(command_spec)
            .filter(|spec| spec.name.starts_with(word))
            .map(|spec| CompletionItem {
                label: format!(":{}", spec.name),
                insert_text: format!(":{}", spec.name),
                kind: CompletionKind::Command,
                detail: Some(spec.detail.into()),
            })
            .collect();
    }

    Vec::new()
}

fn prefix_before_cursor(input: &str, cursor: usize) -> &str {
    let mut cursor = cursor.min(input.len());
    while !input.is_char_boundary(cursor) {
        cursor -= 1;
    }
    &input[..cursor]
}

fn mode_param_key_prefix(prefix: &str) -> Option<(&str, &str)> {
    let open = prefix.rfind('(')?;
    let command = prefix[..open].strip_prefix(':')?;
    let command = command.split_whitespace().next().unwrap_or(command);
    if !command_spec(command)?.accepts_mode_params() {
        return None;
    }
    let params = &prefix[open + 1..];
    if params.contains(')') {
        return None;
    }
    let current = params
        .rsplit_once([',', ' ', '\t'])
        .map(|(_, current)| current)
        .unwrap_or(params);
    if current.contains('=') {
        return None;
    }
    Some((command, current))
}

fn highlight_input(input: &str) -> Vec<HighlightSpan> {
    match Tokenizer::tokenize(input) {
        Ok(tokens) => tokens
            .into_iter()
            .filter_map(|spanned| {
                let kind = match spanned.token {
                    Token::Command(_) => HighlightKind::CommandName,
                    Token::ModeParenOpen
                    | Token::ModeParenClose
                    | Token::ParamEq
                    | Token::ParamValue(_)
                    | Token::Comma => HighlightKind::ModeParam,
                    Token::SerialThen
                    | Token::SerialAlways
                    | Token::ParallelAll
                    | Token::ParallelRace
                    | Token::JobAnd
                    | Token::JobOr
                    | Token::PipeStdout
                    | Token::PipeAll
                    | Token::PipeStderr => HighlightKind::Operator,
                    Token::IdRef(_, _) => HighlightKind::IdRef,
                    Token::Word(_) => HighlightKind::Word,
                    Token::Colon => HighlightKind::CommandPrefix,
                    Token::GroupOpen | Token::GroupClose => HighlightKind::Word,
                    Token::Whitespace(_) | Token::Newline | Token::Eof => return None,
                };
                Some(HighlightSpan {
                    start: spanned.span.start,
                    end: spanned.span.end,
                    kind,
                })
            })
            .collect(),
        Err(error) => vec![HighlightSpan {
            start: error.pos,
            end: error.pos.saturating_add(1).min(input.len()),
            kind: HighlightKind::Error,
        }],
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt as _;

    use super::*;

    #[tokio::test]
    async fn custom_socket_is_private_after_bind() {
        let socket = PathBuf::from(format!(
            "/tmp/cue-gateway-permissions-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));

        let listener = bind_private_listener(&socket).expect("bind private listener");

        assert_eq!(
            std::fs::metadata(&socket)
                .expect("stat socket")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        drop(listener);
        std::fs::remove_file(socket).expect("remove socket");
    }

    #[tokio::test]
    async fn existing_live_socket_is_rejected_without_unlinking_it() {
        let socket = PathBuf::from(format!(
            "/tmp/cue-gateway-live-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));
        let listener = bind_private_listener(&socket).expect("bind first listener");

        let error = bind_private_listener(&socket).expect_err("second bind must fail");
        assert!(
            error.to_string().contains("bind socket"),
            "unexpected error: {error:#}"
        );
        assert!(socket.exists(), "live socket path must remain in place");
        let _client = UnixStream::connect(&socket)
            .await
            .expect("first listener remains reachable");

        drop(listener);
        std::fs::remove_file(socket).expect("remove socket");
    }

    #[tokio::test]
    async fn message_framing_roundtrip() {
        // Create a connected pair.
        let (mut client, mut server) = UnixStream::pair().unwrap();

        let msg = Message::Request {
            id: 42,
            operation_id: None,
            payload: RequestPayload::Ping {},
        };

        write_message(&mut client, &msg).await.unwrap();
        let decoded = read_message(&mut server).await.unwrap();

        if let Message::Request {
            id,
            payload: RequestPayload::Ping {},
            ..
        } = decoded
        {
            assert_eq!(id, 42);
        } else {
            panic!("wrong message variant");
        }
    }

    #[tokio::test]
    async fn partial_request_frame_survives_concurrent_outbound_event() {
        let (mut client, server) = UnixStream::pair().expect("socket pair");
        let clients: ClientMap = Arc::new(Mutex::new(HashMap::new()));
        let operations: SharedOperationLedger = Arc::new(Mutex::new(OperationLedger::default()));
        let (event_bus_tx, _event_bus_rx) = mpsc::channel(1);
        let (gateway_tx, mut gateway_rx) = mpsc::channel(2);
        let sys = test_actor_system(event_bus_tx, gateway_tx);
        let handler = tokio::spawn(handle_client(
            77,
            server,
            sys,
            Arc::clone(&clients),
            operations,
        ));
        while !client_registry(&clients).contains_key(&77) {
            tokio::task::yield_now().await;
        }

        let request = encode_message(&Message::Request {
            id: 9,
            operation_id: None,
            payload: RequestPayload::Ping {},
        })
        .expect("encode request");
        let split_at = 8.min(request.len() - 1);
        client
            .write_all(&request[..split_at])
            .await
            .expect("write partial frame");
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;

        queue_event_for_client(
            &clients,
            77,
            EventPayload::ShuttingDown {
                reason: "concurrent event".into(),
            },
        );
        assert!(matches!(
            read_message(&mut client).await.expect("read event"),
            Message::Event {
                payload: EventPayload::ShuttingDown { .. }
            }
        ));

        client
            .write_all(&request[split_at..])
            .await
            .expect("finish request frame");
        let GatewayMsg::SendResponse {
            client_id,
            request_id,
            payload,
        } = gateway_rx.recv().await.expect("ping response")
        else {
            panic!("expected ping response");
        };
        queue_response_for_client(&clients, client_id, request_id, payload);
        assert!(matches!(
            read_message(&mut client).await.expect("read ping response"),
            Message::Response { id: 9, .. }
        ));

        drop(client);
        tokio::time::timeout(std::time::Duration::from_secs(1), handler)
            .await
            .expect("handler exits")
            .expect("handler task");
    }

    #[tokio::test]
    async fn response_roundtrip() {
        let (mut a, mut b) = UnixStream::pair().unwrap();
        let msg = Message::Response {
            id: 1,
            payload: ResponsePayload::Ok(OkPayload::Pong {
                version: "0.1.0".into(),
                instance_id: "00000000-0000-4000-8000-000000000000".into(),
                protocol_version: IPC_PROTOCOL_VERSION,
                capabilities: current_protocol_capabilities(),
            }),
        };
        write_message(&mut a, &msg).await.unwrap();
        let decoded = read_message(&mut b).await.unwrap();
        assert!(matches!(
            decoded,
            Message::Response {
                id: 1,
                payload: ResponsePayload::Ok(OkPayload::Pong { version, .. }),
            } if version == "0.1.0"
        ));
    }

    struct TestClientQueues {
        queues: ClientQueues,
        responses: mpsc::Receiver<(u32, ResponsePayload)>,
        events: mpsc::Receiver<EventPayload>,
        disconnect: watch::Receiver<bool>,
    }

    fn test_client_queues(capacity: usize) -> TestClientQueues {
        let (response_tx, responses) = mpsc::channel(capacity);
        let (event_tx, events) = mpsc::channel(capacity);
        let (disconnect_tx, disconnect) = watch::channel(false);
        TestClientQueues {
            queues: ClientQueues {
                responses: response_tx,
                events: event_tx,
                disconnect: disconnect_tx,
            },
            responses,
            events,
            disconnect,
        }
    }

    #[test]
    fn request_id_fence_rejects_reuse_until_response_is_written() {
        let inflight = Arc::new(Mutex::new(HashSet::new()));

        assert!(reserve_request_id(&inflight, 7));
        assert!(!reserve_request_id(&inflight, 7));
        release_request_id(&inflight, 7);
        assert!(reserve_request_id(&inflight, 7));
    }

    #[test]
    fn response_dispatch_evicts_lagging_client_without_blocking_healthy_client() {
        let clients: ClientMap = Arc::new(Mutex::new(HashMap::new()));
        let mut slow = test_client_queues(1);
        let mut healthy = test_client_queues(1);
        slow.queues
            .responses
            .try_send((1, ResponsePayload::ack()))
            .unwrap();
        client_registry(&clients).insert(7, slow.queues.clone());
        client_registry(&clients).insert(8, healthy.queues.clone());

        queue_response_for_client(&clients, 7, 2, ResponsePayload::ack());
        queue_response_for_client(&clients, 8, 3, ResponsePayload::ack());

        assert!(*slow.disconnect.borrow_and_update());
        assert!(!client_registry(&clients).contains_key(&7));
        assert_eq!(healthy.responses.try_recv().unwrap().0, 3);
        assert!(client_registry(&clients).contains_key(&8));
    }

    #[test]
    fn direct_event_dispatch_evicts_lagging_client_without_blocking_healthy_client() {
        let clients: ClientMap = Arc::new(Mutex::new(HashMap::new()));
        let mut slow = test_client_queues(1);
        let mut healthy = test_client_queues(1);
        slow.queues
            .events
            .try_send(EventPayload::ShuttingDown {
                reason: "first".into(),
            })
            .unwrap();
        client_registry(&clients).insert(7, slow.queues.clone());
        client_registry(&clients).insert(8, healthy.queues.clone());

        queue_event_for_client(
            &clients,
            7,
            EventPayload::ShuttingDown {
                reason: "second".into(),
            },
        );
        queue_event_for_client(
            &clients,
            8,
            EventPayload::ShuttingDown {
                reason: "healthy".into(),
            },
        );

        assert!(*slow.disconnect.borrow_and_update());
        assert!(!client_registry(&clients).contains_key(&7));
        assert!(matches!(
            healthy.events.try_recv().unwrap(),
            EventPayload::ShuttingDown { reason } if reason == "healthy"
        ));
        assert!(client_registry(&clients).contains_key(&8));
    }

    fn test_actor_system(
        event_bus: mpsc::Sender<EventBusMsg>,
        gateway: mpsc::Sender<GatewayMsg>,
    ) -> ActorSystem {
        let (scheduler, _scheduler_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (process_mgr, _process_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scope_store, _scope_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        ActorSystem {
            gateway,
            scheduler,
            process_mgr,
            scope_store,
            event_bus,
            config: crate::config::Config::default(),
            resources: std::sync::Arc::new(crate::resource::ProviderRegistry::empty()),
        }
    }

    #[tokio::test]
    async fn subscribe_request_registers_only_requested_channels() {
        let (event_bus_tx, mut event_bus_rx) = mpsc::channel(2);
        let (gateway_tx, mut gateway_rx) = mpsc::channel(1);
        let sys = test_actor_system(event_bus_tx, gateway_tx);
        let (evt_tx, mut evt_rx) = mpsc::channel(1);
        let (disconnect_tx, _disconnect_rx) = watch::channel(false);

        route_request(
            7,
            42,
            RequestPayload::subscribe(&[EventChannel::System]),
            &sys,
            &evt_tx,
            &disconnect_tx,
        )
        .await
        .unwrap();

        match event_bus_rx.recv().await.unwrap() {
            EventBusMsg::Subscribe {
                client_id,
                channel,
                sender,
                disconnect: _,
            } => {
                assert_eq!(client_id, 7);
                assert_eq!(channel, EventChannel::System);
                sender
                    .try_send(EventPayload::ShuttingDown {
                        reason: "test".into(),
                    })
                    .unwrap();
                assert!(matches!(
                    evt_rx.try_recv().unwrap(),
                    EventPayload::ShuttingDown { .. }
                ));
            }
            _ => panic!("expected explicit system subscription"),
        }

        match gateway_rx.recv().await.unwrap() {
            GatewayMsg::SendResponse {
                client_id,
                request_id,
                payload,
            } => {
                assert_eq!(client_id, 7);
                assert_eq!(request_id, 42);
                assert!(matches!(payload, ResponsePayload::Ok(OkPayload::Ack {})));
            }
            _ => panic!("expected subscribe ack"),
        }
        assert!(event_bus_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn subscribe_rejects_unknown_event_channels() {
        let (event_bus_tx, mut event_bus_rx) = mpsc::channel(1);
        let (gateway_tx, mut gateway_rx) = mpsc::channel(1);
        let sys = test_actor_system(event_bus_tx, gateway_tx);
        let (evt_tx, _evt_rx) = mpsc::channel(1);
        let (disconnect_tx, _disconnect_rx) = watch::channel(false);

        route_request(
            7,
            42,
            RequestPayload::Subscribe {
                channels: vec!["output:C1".into()],
            },
            &sys,
            &evt_tx,
            &disconnect_tx,
        )
        .await
        .unwrap();

        assert!(event_bus_rx.try_recv().is_err());
        match gateway_rx.recv().await.unwrap() {
            GatewayMsg::SendResponse {
                client_id,
                request_id,
                payload: ResponsePayload::Err { code, message },
            } => {
                assert_eq!(client_id, 7);
                assert_eq!(request_id, 42);
                assert_eq!(code, error_code::INVALID_REQUEST);
                assert!(message.contains("invalid event channel `output:C1`"));
            }
            _ => panic!("expected invalid subscription response"),
        }
    }

    #[test]
    fn completion_uses_shared_command_specs() {
        let items = complete_input(":ta", 3);
        assert!(items.iter().any(|item| item.label == ":tail"));
    }

    #[test]
    fn completion_clamps_cursor_to_utf8_boundary() {
        let input = ":r💖un";
        let cursor_inside_heart = ":r".len() + 1;

        assert_eq!(prefix_before_cursor(input, cursor_inside_heart), ":r");
        let items = complete_input(input, cursor_inside_heart);

        assert!(items.iter().any(|item| item.label == ":run"));
    }

    #[test]
    fn completion_uses_shared_mode_param_specs() {
        let items = complete_input(":run(p", 6);
        assert!(items.iter().any(|item| item.label == "pty"));
        assert!(!items.iter().any(|item| item.label == "retry"));

        let cron_items = complete_input(":cron(p", 7);
        assert!(!cron_items.iter().any(|item| item.label == "pty"));
    }

    #[test]
    fn inline_multiline_script_rejection_points_to_cue_run() {
        let command = parse_command("cargo test\n:run cargo clippy", cue_core::Mode::Job).unwrap();
        assert!(matches!(
            command,
            ResolvedCommand::Script {
                source: cue_core::ipc::ScriptSource::Inline,
                ..
            }
        ));
        let response = inline_script_disabled_response();
        match response {
            ResponsePayload::Err { code, message } => {
                assert_eq!(code, error_code::NOT_SUPPORTED);
                assert!(message.contains("cue run path/to/file.cue"));
            }
            _ => panic!("expected error response"),
        }
    }

    #[tokio::test]
    async fn run_script_requests_are_resolved_with_job_mode() {
        let (event_bus_tx, _event_bus_rx) = mpsc::channel(1);
        let (gateway_tx, mut gateway_rx) = mpsc::channel(1);
        let (scheduler_tx, mut scheduler_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (process_mgr, _process_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let (scope_store, _scope_rx) = mpsc::channel(super::super::ACTOR_CHANNEL_CAP);
        let sys = ActorSystem {
            gateway: gateway_tx,
            scheduler: scheduler_tx,
            process_mgr,
            scope_store,
            event_bus: event_bus_tx,
            config: crate::config::Config::default(),
            resources: std::sync::Arc::new(crate::resource::ProviderRegistry::empty()),
        };
        let (evt_tx, _evt_rx) = mpsc::channel(1);
        let (disconnect_tx, _disconnect_rx) = watch::channel(false);

        route_request(
            7,
            42,
            RequestPayload::RunScript {
                path: "build.cue".into(),
                input: "every 5m echo hi".into(),
            },
            &sys,
            &evt_tx,
            &disconnect_tx,
        )
        .await
        .unwrap();

        assert!(gateway_rx.try_recv().is_err());
        match scheduler_rx.recv().await.unwrap() {
            SchedulerMsg::Eval {
                client_id,
                request_id,
                command,
            } => {
                assert_eq!(client_id, 7);
                assert_eq!(request_id, 42);
                match *command {
                    ResolvedCommand::Script { source, items, .. } => {
                        assert_eq!(
                            source,
                            cue_core::ipc::ScriptSource::File {
                                path: "build.cue".into(),
                            }
                        );
                        assert_eq!(items.len(), 1);
                        assert!(matches!(
                            *items.into_iter().next().unwrap().command,
                            ResolvedCommand::Run { .. }
                        ));
                    }
                    other => panic!("expected file script command, got {other:?}"),
                }
            }
            _ => panic!("expected scheduler eval"),
        }
    }

    #[test]
    fn syntax_error_message_adds_bash_hints() {
        let message = syntax_error_message("echo hi | wc -c > out.txt", "parse failed");
        assert!(message.contains("Possible bash syntax issue"));
        assert!(message.contains("bare '|' is shell syntax"));
        assert!(message.contains("redirection is shell syntax"));
    }

    #[test]
    fn highlight_tokenizes_command_and_operator_spans() {
        let spans = highlight_input(":run cargo test -> :jobs");
        assert!(
            spans
                .iter()
                .any(|span| span.kind == HighlightKind::CommandName)
        );
        assert!(
            spans
                .iter()
                .any(|span| span.kind == HighlightKind::Operator)
        );
    }
}
