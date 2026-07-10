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

pub fn run(path: PathBuf) -> Result<i32> {
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

    rt.block_on(async_run(path))
}

async fn async_run(path: PathBuf) -> Result<i32> {
    let input = std::fs::read_to_string(&path)
        .with_context(|| format!("read .cue script `{}`", path.display()))?;
    let display_path = path.display().to_string();
    let mut client = connect_for_script().await?;
    run_with_client(&mut client, &display_path, &input).await
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
        MAX_MESSAGE_SIZE, RequestPayload, ScriptRunStatus, ScriptSource, encode_message,
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
