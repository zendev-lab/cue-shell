use std::ffi::OsString;
use std::io::Write;
use std::path::PathBuf;

use crate::{CuedClient, ResolvedTransport, connect_ssh_transport, load_transport_config};
use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use cue_core::ipc::{EventPayload, Message, OkPayload, ResponsePayload, Stream};

use crate::daemon_lifecycle::{
    check_local_daemon_version, ensure_daemon_running, version_from_ping,
    warn_on_remote_version_mismatch,
};

pub fn run(path: PathBuf, session_refresh_flag: bool) -> Result<i32> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build tokio runtime")?;

    rt.block_on(async_run(path, session_refresh_flag))
}

async fn async_run(path: PathBuf, session_refresh_flag: bool) -> Result<i32> {
    let input = std::fs::read_to_string(&path)
        .with_context(|| format!("read .cue script `{}`", path.display()))?;
    let display_path = path.display().to_string();
    let selector = cue_session_selector(std::env::var_os("CUE_SESSION"))?;
    let refresh_if_needed =
        session_refresh_flag || cue_session_refresh(std::env::var_os("CUE_SESSION_REFRESH"))?;
    if refresh_if_needed && selector.is_none() {
        bail!("session refresh requires CUE_SESSION to select a named session");
    }
    let mut client = connect_for_script().await?;
    run_in_session_with_client(
        &mut client,
        &display_path,
        &input,
        selector,
        refresh_if_needed,
    )
    .await
}

async fn run_in_session_with_client(
    client: &mut CuedClient,
    path: &str,
    input: &str,
    selector: Option<String>,
    refresh_if_needed: bool,
) -> Result<i32> {
    if let Some(selector) = selector {
        let attach = client
            .attach_session_with_refresh_if_needed(&selector, refresh_if_needed)
            .await;
        if refresh_if_needed {
            attach.with_context(|| {
                format!("attach cue script to session `{selector}` with explicit restart recovery")
            })?;
        } else {
            attach.with_context(|| {
                format!(
                    "attach cue script to session `{selector}`; if it reports needs_refresh after a daemon restart, rerun with `--session-refresh` or `CUE_SESSION_REFRESH=1` to explicitly replace its scope from this process environment"
                )
            })?;
        }
    }
    run_with_client(client, path, input).await
}

fn cue_session_selector(value: Option<OsString>) -> Result<Option<String>> {
    let Some(value) = value.filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    value
        .into_string()
        .map(Some)
        .map_err(|_| anyhow::anyhow!("CUE_SESSION must be valid UTF-8"))
}

fn cue_session_refresh(value: Option<OsString>) -> Result<bool> {
    let Some(value) = value else {
        return Ok(false);
    };
    let value = value
        .into_string()
        .map_err(|_| anyhow::anyhow!("CUE_SESSION_REFRESH must be valid UTF-8"))?;
    match value.as_str() {
        "1" | "true" => Ok(true),
        "0" | "false" => Ok(false),
        _ => bail!("CUE_SESSION_REFRESH must be one of: 1, true, 0, false"),
    }
}

async fn connect_for_script() -> Result<CuedClient> {
    let transport = load_transport_config()?
        .resolve_transport(std::env::var_os("CUE_SOCKET").map(PathBuf::from))?;
    match transport {
        ResolvedTransport::Unix { socket_path, .. } => {
            let client = ensure_daemon_running(&socket_path).await.ok_or_else(|| {
                anyhow::anyhow!("cued is not available at {}", socket_path.display())
            })?;
            check_local_daemon_version(Some(client), &socket_path)
                .await
                .ok_or_else(|| {
                    anyhow::anyhow!("cued is not available at {}", socket_path.display())
                })
        }
        ssh_transport @ ResolvedTransport::Ssh { .. } => {
            let (client, daemon_version) = connect_ssh_transport(&ssh_transport).await?;
            warn_on_remote_version_mismatch(version_from_ping(daemon_version));
            Ok(client)
        }
    }
}

async fn run_with_client(client: &mut CuedClient, path: &str, input: &str) -> Result<i32> {
    let request_id = client.run_script(path, input).await?;
    let mut script_id: Option<String> = None;
    let mut pending_finished: Vec<(String, i32)> = Vec::new();

    loop {
        match client.recv().await? {
            Message::Response { id, payload } if id == request_id => match payload {
                ResponsePayload::Ok(OkPayload::ScriptCreated {
                    script_id: created,
                    items: _,
                    submit_error,
                    ..
                }) => {
                    script_id = Some(created.clone());
                    if let Some((_, exit_code)) = pending_finished
                        .iter()
                        .find(|(finished, _)| finished == &created)
                    {
                        return Ok(*exit_code);
                    }
                    if let Some(error) = submit_error {
                        bail!(
                            "script {created} submission failed at item {} [{}]: {}",
                            error.index,
                            error.code,
                            error.message
                        );
                    }
                }
                ResponsePayload::Err { code, message } => {
                    bail!("cue run failed [{code}]: {message}");
                }
                other => bail!("unexpected cue run response: {other:?}"),
            },
            Message::Response { .. } => {}
            Message::Request { .. } => {
                bail!("unexpected request message from cued");
            }
            Message::Event { payload } => match payload {
                EventPayload::OutputChunk { stream, data, .. } => {
                    write_stream(stream, data.as_bytes())?;
                }
                EventPayload::OutputChunkBinary { stream, base64, .. } => {
                    let bytes = decode_binary_output_chunk(&base64)?;
                    write_stream(stream, &bytes)?;
                }
                EventPayload::ScriptFinished {
                    script_id: finished,
                    exit_code,
                    ..
                } => {
                    if script_id.as_deref() == Some(finished.as_str()) {
                        return Ok(exit_code);
                    }
                    pending_finished.push((finished, exit_code));
                }
                _ => {}
            },
        }
    }
}

fn write_stream(stream: Stream, bytes: &[u8]) -> Result<()> {
    match stream {
        Stream::Stdout => std::io::stdout().write_all(bytes)?,
        Stream::Stderr => std::io::stderr().write_all(bytes)?,
    }
    Ok(())
}

fn decode_binary_output_chunk(base64: &str) -> Result<Vec<u8>> {
    BASE64_STANDARD
        .decode(base64.as_bytes())
        .context("decode binary output chunk")
}

#[cfg(test)]
mod tests {
    use super::*;
    use cue_core::ipc::{
        MAX_MESSAGE_SIZE, RequestPayload, ScriptRunStatus, ScriptSource, SessionInfo,
        SessionScopeState, encode_message,
    };
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

    async fn read_test_message<R>(stream: &mut R) -> Message
    where
        R: AsyncRead + Unpin,
    {
        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .await
            .expect("read message length");
        let len = u32::from_be_bytes(len_buf) as usize;
        assert!(len <= MAX_MESSAGE_SIZE, "test message too large: {len}");
        let mut body = vec![0u8; len];
        stream
            .read_exact(&mut body)
            .await
            .expect("read message body");
        serde_json::from_slice(&body).expect("decode message")
    }

    async fn write_test_message<W>(stream: &mut W, message: Message)
    where
        W: AsyncWrite + Unpin,
    {
        let encoded = encode_message(&message).expect("encode message");
        stream
            .write_all(&encoded)
            .await
            .expect("write test message");
    }

    #[test]
    fn binary_output_chunks_decode_to_original_bytes() {
        let encoded = BASE64_STANDARD.encode([0, 159, 146, 150, b'\n']);

        let decoded = decode_binary_output_chunk(&encoded).expect("decode binary chunk");

        assert_eq!(decoded, vec![0, 159, 146, 150, b'\n']);
    }

    #[test]
    fn cue_session_selector_ignores_missing_and_empty_values() {
        assert_eq!(cue_session_selector(None).unwrap(), None);
        assert_eq!(cue_session_selector(Some(OsString::new())).unwrap(), None);
    }

    #[test]
    fn cue_session_selector_accepts_name_or_id() {
        assert_eq!(
            cue_session_selector(Some(OsString::from("shared-bench"))).unwrap(),
            Some("shared-bench".into())
        );
        assert_eq!(
            cue_session_selector(Some(OsString::from("S42"))).unwrap(),
            Some("S42".into())
        );
    }

    #[test]
    fn cue_session_refresh_requires_an_explicit_boolean() {
        assert!(!cue_session_refresh(None).unwrap());
        assert!(!cue_session_refresh(Some(OsString::from("0"))).unwrap());
        assert!(!cue_session_refresh(Some(OsString::from("false"))).unwrap());
        assert!(cue_session_refresh(Some(OsString::from("1"))).unwrap());
        assert!(cue_session_refresh(Some(OsString::from("true"))).unwrap());

        let error = cue_session_refresh(Some(OsString::new()))
            .expect_err("an empty opt-in must not silently enable refresh");
        assert!(
            format!("{error:#}").contains("CUE_SESSION_REFRESH must be one of"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn configured_session_attaches_before_script_submission() {
        let (client_stream, mut server_stream) = tokio::io::duplex(4096);
        let mut client = CuedClient::from_stream(client_stream);
        let runner = tokio::spawn(async move {
            run_in_session_with_client(
                &mut client,
                "shared.cue",
                ":help\n",
                Some("shared-bench".into()),
                false,
            )
            .await
        });

        let attach_id = match read_test_message(&mut server_stream).await {
            Message::Request {
                id,
                payload: RequestPayload::AttachSession { selector, refresh },
                ..
            } => {
                assert_eq!(selector, "shared-bench");
                assert!(!refresh);
                id
            }
            other => panic!("expected AttachSession before RunScript, got {other:?}"),
        };
        write_test_message(
            &mut server_stream,
            Message::Response {
                id: attach_id,
                payload: ResponsePayload::Ok(OkPayload::SessionInfo(Box::new(SessionInfo {
                    id: "S42".into(),
                    name: "shared-bench".into(),
                    scope_state: SessionScopeState::ReadyDurable,
                    scope_hash: Some("abc123".into()),
                    connected_clients: 2,
                    restart_safe: true,
                    current: true,
                    created_at_ms: 10,
                    updated_at_ms: 20,
                    archived_at_ms: None,
                }))),
            },
        )
        .await;

        let run_id = match read_test_message(&mut server_stream).await {
            Message::Request {
                id,
                payload: RequestPayload::RunScript { path, input },
                ..
            } => {
                assert_eq!(path, "shared.cue");
                assert_eq!(input, ":help\n");
                id
            }
            other => panic!("expected RunScript after attach confirmation, got {other:?}"),
        };
        write_test_message(
            &mut server_stream,
            Message::Response {
                id: run_id,
                payload: ResponsePayload::Ok(OkPayload::ScriptCreated {
                    script_id: "R1".into(),
                    source: ScriptSource::File {
                        path: "shared.cue".into(),
                    },
                    items: vec![],
                    submit_error: None,
                }),
            },
        )
        .await;
        write_test_message(
            &mut server_stream,
            Message::Event {
                payload: EventPayload::ScriptFinished {
                    script_id: "R1".into(),
                    status: ScriptRunStatus::Done,
                    exit_code: 0,
                    failed_item_index: None,
                },
            },
        )
        .await;

        assert_eq!(runner.await.unwrap().unwrap(), 0);
    }

    #[tokio::test]
    async fn session_attach_failure_explains_explicit_restart_recovery() {
        let (client_stream, mut server_stream) = tokio::io::duplex(4096);
        let mut client = CuedClient::from_stream(client_stream);
        let runner = tokio::spawn(async move {
            run_in_session_with_client(
                &mut client,
                "shared.cue",
                ":help\n",
                Some("shared-bench".into()),
                false,
            )
            .await
        });

        let attach_id = match read_test_message(&mut server_stream).await {
            Message::Request {
                id,
                payload: RequestPayload::AttachSession { selector, refresh },
                ..
            } => {
                assert_eq!(selector, "shared-bench");
                assert!(!refresh);
                id
            }
            other => panic!("expected AttachSession, got {other:?}"),
        };
        write_test_message(
            &mut server_stream,
            Message::Response {
                id: attach_id,
                payload: ResponsePayload::Err {
                    code: "INVALID_STATE".into(),
                    message: "named session needs_refresh after daemon restart".into(),
                },
            },
        )
        .await;

        let error = runner
            .await
            .expect("join script runner")
            .expect_err("non-refreshing run must fail closed");
        let message = format!("{error:#}");
        assert!(message.contains("--session-refresh"), "{message}");
        assert!(message.contains("CUE_SESSION_REFRESH=1"), "{message}");
        assert!(message.contains("needs_refresh"), "{message}");
    }

    #[tokio::test]
    async fn run_with_client_uses_direct_script_finished_without_jobs_subscription() {
        let (client_stream, mut server_stream) = tokio::io::duplex(4096);
        let mut client = CuedClient::from_stream(client_stream);
        let runner =
            tokio::spawn(async move { run_with_client(&mut client, "fast.cue", ":help\n").await });

        match read_test_message(&mut server_stream).await {
            Message::Request {
                id,
                payload: RequestPayload::RunScript { path, input },
                ..
            } => {
                assert_eq!(id, 1);
                assert_eq!(path, "fast.cue");
                assert_eq!(input, ":help\n");
            }
            other => panic!("expected first request to be RunScript, got {other:?}"),
        }

        write_test_message(
            &mut server_stream,
            Message::Event {
                payload: EventPayload::ScriptFinished {
                    script_id: "R1".into(),
                    status: ScriptRunStatus::Done,
                    exit_code: 0,
                    failed_item_index: None,
                },
            },
        )
        .await;
        write_test_message(
            &mut server_stream,
            Message::Response {
                id: 1,
                payload: ResponsePayload::Ok(OkPayload::ScriptCreated {
                    script_id: "R1".into(),
                    source: ScriptSource::File {
                        path: "fast.cue".into(),
                    },
                    items: vec![],
                    submit_error: None,
                }),
            },
        )
        .await;

        let exit_code = runner
            .await
            .expect("runner task")
            .expect("run_with_client succeeds");
        assert_eq!(exit_code, 0);
    }
}
