//! Gateway actor — Unix socket listener, per-client handlers, message framing.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, bail};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use cue_core::EventChannel;
use cue_core::command_spec::{command_names, command_spec, mode_param_specs_for_command};
use cue_core::ipc::{
    CompletionItem, CompletionKind, EventPayload, HighlightKind, HighlightSpan, MAX_MESSAGE_SIZE,
    Message, OkPayload, RequestPayload, ResponsePayload, encode_message, error_code,
};

use crate::parser::{ResolvedCommand, Token, Tokenizer, parse_command, parse_file_script_command};

use super::{ActorSystem, CLIENT_EVENT_CAP, EventBusMsg, GatewayMsg, SchedulerMsg};

/// Next client id counter (global, atomic).
static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

// ── Message framing ──

/// Read one length-prefixed JSON message from the stream.
pub(crate) async fn read_message(stream: &mut UnixStream) -> Result<Message> {
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
pub(crate) async fn write_message(stream: &mut UnixStream, msg: &Message) -> Result<()> {
    let encoded = encode_message(msg)?;
    stream.write_all(&encoded).await.context("write message")?;
    stream.flush().await.context("flush")?;
    Ok(())
}

/// Type alias for the shared client map to avoid clippy::type_complexity.
type ClientMap = Arc<tokio::sync::Mutex<HashMap<u64, mpsc::Sender<(u32, ResponsePayload)>>>>;
type ClientEventMap = Arc<tokio::sync::Mutex<HashMap<u64, mpsc::Sender<EventPayload>>>>;

/// Spawn the Gateway actor.
///
/// This creates a Unix socket listener and spawns a task that accepts connections.
/// Per-client handler tasks are spawned for each connection.
pub(super) async fn spawn(
    mut rx: mpsc::Receiver<GatewayMsg>,
    socket_path: PathBuf,
    sys: ActorSystem,
) -> Result<()> {
    // Remove stale socket file.
    if socket_path.exists() {
        std::fs::remove_file(&socket_path)
            .with_context(|| format!("remove stale socket {}", socket_path.display()))?;
    }

    let listener = UnixListener::bind(&socket_path)
        .with_context(|| format!("bind socket {}", socket_path.display()))?;

    info!(path = %socket_path.display(), "gateway: listening");

    // Shared state: client_id → response sender.
    let clients: ClientMap = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
    let event_clients: ClientEventMap = Arc::new(tokio::sync::Mutex::new(HashMap::new()));

    let clients_for_dispatch = Arc::clone(&clients);
    let event_clients_for_dispatch = Arc::clone(&event_clients);

    // Accept loop — runs in its own task.
    let sys_accept = sys.clone();
    let accept_handle = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _addr)) => {
                    let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
                    info!(%client_id, "gateway: client connected");
                    let sys_clone = sys_accept.clone();
                    let clients_clone = Arc::clone(&clients_for_dispatch);
                    let event_clients_clone = Arc::clone(&event_clients_for_dispatch);
                    tokio::spawn(handle_client(
                        client_id,
                        stream,
                        sys_clone,
                        clients_clone,
                        event_clients_clone,
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
                    queue_response_for_client(&clients, client_id, request_id, payload).await;
                }

                GatewayMsg::SendEvent { client_id, payload } => {
                    let sender = {
                        let guard = event_clients.lock().await;
                        guard.get(&client_id).cloned()
                    };
                    if let Some(sender) = sender {
                        if sender.send(payload).await.is_err() {
                            warn!(%client_id, "gateway: direct event channel closed");
                        }
                    } else {
                        warn!(%client_id, "gateway: no such client for direct event");
                    }
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

/// Handle one client connection.
async fn handle_client(
    client_id: u64,
    mut stream: UnixStream,
    sys: ActorSystem,
    clients: ClientMap,
    event_clients: ClientEventMap,
) {
    // Per-client response channel.
    let (resp_tx, mut resp_rx) = mpsc::channel::<(u32, ResponsePayload)>(64);
    // Per-client event channel.
    let (evt_tx, mut evt_rx) = mpsc::channel::<EventPayload>(CLIENT_EVENT_CAP);

    // Register.
    {
        let mut guard = clients.lock().await;
        guard.insert(client_id, resp_tx);
    }
    {
        let mut guard = event_clients.lock().await;
        guard.insert(client_id, evt_tx.clone());
    }

    // Split stream.
    // Use a single-task select loop for simplicity.
    loop {
        tokio::select! {
            // Read a message from the client.
            msg_result = read_message(&mut stream) => {
                match msg_result {
                    Ok(Message::Request { id, payload }) => {
                        if let Err(e) = route_request(
                            client_id, id, payload, &sys, &evt_tx,
                        ).await {
                            warn!(%client_id, "gateway: route error: {e}");
                            let err_resp = Message::Response {
                                id,
                                payload: ResponsePayload::err(error_code::INTERNAL, e.to_string()),
                            };
                            if write_message(&mut stream, &err_resp).await.is_err() {
                                break;
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
                if write_message(&mut stream, &msg).await.is_err() {
                    break;
                }
            }

            // Deliver pushed event to client.
            Some(event) = evt_rx.recv() => {
                let msg = Message::Event { payload: event };
                if write_message(&mut stream, &msg).await.is_err() {
                    break;
                }
            }
        }
    }

    // Cleanup.
    info!(%client_id, "gateway: client disconnected");
    {
        let mut guard = clients.lock().await;
        guard.remove(&client_id);
    }
    {
        let mut guard = event_clients.lock().await;
        guard.remove(&client_id);
    }
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
}

/// Route an incoming request to the appropriate actor.
async fn route_request(
    client_id: u64,
    request_id: u32,
    payload: RequestPayload,
    sys: &ActorSystem,
    evt_tx: &mpsc::Sender<EventPayload>,
) -> Result<()> {
    match payload {
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
                        return Ok(());
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
                    return Ok(());
                }
            };
            for channel in channels {
                sys.event_bus
                    .send(EventBusMsg::Subscribe {
                        client_id,
                        channel,
                        sender: evt_tx.clone(),
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
                    return Ok(());
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

    Ok(())
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

async fn queue_response_for_client(
    clients: &ClientMap,
    client_id: u64,
    request_id: u32,
    payload: ResponsePayload,
) {
    let sender = {
        let guard = clients.lock().await;
        guard.get(&client_id).cloned()
    };

    if let Some(sender) = sender {
        if sender.send((request_id, payload)).await.is_err() {
            warn!(%client_id, "gateway: response channel closed");
        }
    } else {
        warn!(%client_id, "gateway: no such client for response");
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
    use super::*;

    #[tokio::test]
    async fn message_framing_roundtrip() {
        // Create a connected pair.
        let (mut client, mut server) = UnixStream::pair().unwrap();

        let msg = Message::Request {
            id: 42,
            payload: RequestPayload::Ping {},
        };

        write_message(&mut client, &msg).await.unwrap();
        let decoded = read_message(&mut server).await.unwrap();

        if let Message::Request {
            id,
            payload: RequestPayload::Ping {},
        } = decoded
        {
            assert_eq!(id, 42);
        } else {
            panic!("wrong message variant");
        }
    }

    #[tokio::test]
    async fn response_roundtrip() {
        let (mut a, mut b) = UnixStream::pair().unwrap();
        let msg = Message::Response {
            id: 1,
            payload: ResponsePayload::Ok(OkPayload::Pong {
                version: "0.1.0".into(),
            }),
        };
        write_message(&mut a, &msg).await.unwrap();
        let decoded = read_message(&mut b).await.unwrap();
        assert!(matches!(
            decoded,
            Message::Response {
                id: 1,
                payload: ResponsePayload::Ok(OkPayload::Pong { version }),
            } if version == "0.1.0"
        ));
    }

    #[tokio::test]
    async fn response_dispatch_waits_for_client_capacity() {
        let clients: ClientMap =
            std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));
        let (tx, mut rx) = mpsc::channel(1);
        tx.send((1, ResponsePayload::ack())).await.unwrap();
        clients.lock().await.insert(7, tx);

        let dispatch = queue_response_for_client(&clients, 7, 2, ResponsePayload::ack());
        tokio::pin!(dispatch);

        tokio::select! {
            () = &mut dispatch => panic!("response dispatch should wait for client capacity"),
            () = tokio::task::yield_now() => {}
        }

        let first = rx.recv().await.unwrap();
        assert_eq!(first.0, 1);
        dispatch.await;

        let second = rx.recv().await.unwrap();
        assert_eq!(second.0, 2);
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
        }
    }

    #[tokio::test]
    async fn subscribe_request_registers_only_requested_channels() {
        let (event_bus_tx, mut event_bus_rx) = mpsc::channel(2);
        let (gateway_tx, mut gateway_rx) = mpsc::channel(1);
        let sys = test_actor_system(event_bus_tx, gateway_tx);
        let (evt_tx, mut evt_rx) = mpsc::channel(1);

        route_request(
            7,
            42,
            RequestPayload::subscribe(&[EventChannel::System]),
            &sys,
            &evt_tx,
        )
        .await
        .unwrap();

        match event_bus_rx.recv().await.unwrap() {
            EventBusMsg::Subscribe {
                client_id,
                channel,
                sender,
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

        route_request(
            7,
            42,
            RequestPayload::Subscribe {
                channels: vec!["output:C1".into()],
            },
            &sys,
            &evt_tx,
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
        };
        let (evt_tx, _evt_rx) = mpsc::channel(1);

        route_request(
            7,
            42,
            RequestPayload::RunScript {
                path: "build.cue".into(),
                input: "every 5m echo hi".into(),
            },
            &sys,
            &evt_tx,
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
