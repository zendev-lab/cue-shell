use std::io::Write;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use cue_client::CuedClient;
use cue_core::ipc::{EventPayload, Message, OkPayload, ResponsePayload, ScriptRunStatus, Stream};

use crate::config::Config;
use crate::frontend_connection::connect_required_frontend_transport;

pub fn run(path: PathBuf) -> Result<i32> {
    crate::tracing_config::init_stderr_tracing("warn")?;

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
    let client_config = Config::load()?;
    let transport =
        client_config.resolve_transport(std::env::var_os("CUE_SOCKET").map(PathBuf::from))?;
    connect_required_frontend_transport(&transport).await
}

async fn run_with_client(client: &mut CuedClient, path: &str, input: &str) -> Result<i32> {
    let request_id = client.run_script(path, input).await?;
    let mut script_id: Option<String> = None;
    let mut pending_finished: Vec<(String, ScriptFinishEvent)> = Vec::new();

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
                    if let Some(error) = submit_error {
                        bail!(
                            "script {created} submission failed at item {} [{}]: {}",
                            error.index,
                            error.code,
                            error.message
                        );
                    }
                    if let Some((_, finish)) = pending_finished
                        .iter()
                        .find(|(finished, _)| finished == &created)
                    {
                        return script_finished_exit_code(&created, *finish);
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
                    status,
                    exit_code,
                    failed_item_index,
                    ..
                } => {
                    let finish = ScriptFinishEvent {
                        status,
                        exit_code,
                        failed_item_index,
                    };
                    if script_id.as_deref() == Some(finished.as_str()) {
                        return script_finished_exit_code(&finished, finish);
                    }
                    pending_finished.push((finished, finish));
                }
                _ => {}
            },
        }
    }
}

#[derive(Clone, Copy)]
struct ScriptFinishEvent {
    status: ScriptRunStatus,
    exit_code: i32,
    failed_item_index: Option<usize>,
}

fn script_finished_exit_code(script_id: &str, finish: ScriptFinishEvent) -> Result<i32> {
    match (finish.status, finish.exit_code) {
        (ScriptRunStatus::Done, 0) => Ok(0),
        (ScriptRunStatus::Done, code) => {
            bail!("script {script_id} reported done with non-zero exit code {code}")
        }
        (ScriptRunStatus::Failed, 0) => {
            let item = finish
                .failed_item_index
                .map(|index| format!(" at item {index}"))
                .unwrap_or_default();
            bail!("script {script_id} reported failed{item} with zero exit code")
        }
        (ScriptRunStatus::Failed, code) => Ok(code),
    }
}

fn write_stream(stream: Stream, bytes: &[u8]) -> Result<()> {
    match stream {
        Stream::Stdout => write_stream_to(&mut std::io::stdout(), bytes)?,
        Stream::Stderr => write_stream_to(&mut std::io::stderr(), bytes)?,
    }
    Ok(())
}

fn write_stream_to<W>(writer: &mut W, bytes: &[u8]) -> Result<()>
where
    W: Write,
{
    writer.write_all(bytes)?;
    writer.flush()?;
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
        MAX_MESSAGE_SIZE, RequestPayload, ScriptSource, ScriptSubmitError, encode_message,
    };
    use cue_core::job::EXIT_CODE_UNAVAILABLE;
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

    #[derive(Default)]
    struct RecordingWriter {
        bytes: Vec<u8>,
        flush_count: usize,
    }

    impl std::io::Write for RecordingWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.bytes.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.flush_count += 1;
            Ok(())
        }
    }

    #[test]
    fn output_chunks_flush_after_write() {
        let mut writer = RecordingWriter::default();

        write_stream_to(&mut writer, b"partial output").expect("write output chunk");

        assert_eq!(writer.bytes, b"partial output");
        assert_eq!(writer.flush_count, 1);
    }

    #[test]
    fn script_finished_exit_code_rejects_inconsistent_status() {
        let error = script_finished_exit_code(
            "R9",
            ScriptFinishEvent {
                status: ScriptRunStatus::Failed,
                exit_code: 0,
                failed_item_index: Some(2),
            },
        )
        .expect_err("failed status with zero exit code should be invalid");

        assert!(format!("{error:#}").contains("script R9 reported failed at item 2"));
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

    #[tokio::test]
    async fn run_with_client_rejects_inconsistent_finished_status() {
        let (client_stream, mut server_stream) = tokio::io::duplex(4096);
        let mut client = CuedClient::from_stream(client_stream);
        let runner = tokio::spawn(async move {
            run_with_client(&mut client, "inconsistent.cue", ":help\n").await
        });

        match read_test_message(&mut server_stream).await {
            Message::Request {
                id,
                payload: RequestPayload::RunScript { path, input },
            } => {
                assert_eq!(id, 1);
                assert_eq!(path, "inconsistent.cue");
                assert_eq!(input, ":help\n");
            }
            other => panic!("expected first request to be RunScript, got {other:?}"),
        }

        write_test_message(
            &mut server_stream,
            Message::Response {
                id: 1,
                payload: ResponsePayload::Ok(OkPayload::ScriptCreated {
                    script_id: "R1".into(),
                    source: ScriptSource::File {
                        path: "inconsistent.cue".into(),
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
                    exit_code: 7,
                    failed_item_index: None,
                },
            },
        )
        .await;

        let error = runner
            .await
            .expect("runner task")
            .expect_err("inconsistent status should be reported");
        assert!(format!("{error:#}").contains("script R1 reported done with non-zero exit code 7"));
    }

    #[tokio::test]
    async fn run_with_client_reports_submit_error_when_finished_arrives_first() {
        let (client_stream, mut server_stream) = tokio::io::duplex(4096);
        let mut client = CuedClient::from_stream(client_stream);
        let runner =
            tokio::spawn(async move { run_with_client(&mut client, "bad.cue", ":bad\n").await });

        match read_test_message(&mut server_stream).await {
            Message::Request {
                id,
                payload: RequestPayload::RunScript { path, input },
            } => {
                assert_eq!(id, 1);
                assert_eq!(path, "bad.cue");
                assert_eq!(input, ":bad\n");
            }
            other => panic!("expected first request to be RunScript, got {other:?}"),
        }

        write_test_message(
            &mut server_stream,
            Message::Event {
                payload: EventPayload::ScriptFinished {
                    script_id: "R1".into(),
                    status: ScriptRunStatus::Failed,
                    exit_code: EXIT_CODE_UNAVAILABLE,
                    failed_item_index: Some(0),
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
                        path: "bad.cue".into(),
                    },
                    items: vec![],
                    submit_error: Some(ScriptSubmitError {
                        index: 0,
                        source: ":bad".into(),
                        code: "PARSE_ERROR".into(),
                        message: "unknown command".into(),
                    }),
                }),
            },
        )
        .await;

        let error = runner
            .await
            .expect("runner task")
            .expect_err("submit error should be reported");
        assert!(
            format!("{error:#}")
                .contains("script R1 submission failed at item 0 [PARSE_ERROR]: unknown command")
        );
    }
}
