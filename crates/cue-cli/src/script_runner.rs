use std::io::Write;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use cue_client::{CuedClient, ResolvedTransport, connect_ssh_transport};
use cue_core::Mode;
use cue_core::ipc::{EventPayload, Message, OkPayload, RequestPayload, ResponsePayload, Stream};

use crate::config::Config;
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
    let client_config = Config::load()?;
    let transport =
        client_config.resolve_transport(std::env::var_os("CUE_SOCKET").map(PathBuf::from))?;
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
    let subscribe_id = client
        .send(RequestPayload::Subscribe {
            channels: vec!["jobs".into()],
        })
        .await?;
    let request_id = client.run_script(path, input, Mode::Job).await?;
    let mut script_id: Option<String> = None;
    let mut pending_finished: Vec<(String, i32)> = Vec::new();

    loop {
        match client.recv().await? {
            Message::Response { id, payload } if id == subscribe_id => match payload {
                ResponsePayload::Ok(OkPayload::Ack {}) => {}
                ResponsePayload::Err { code, message } => {
                    bail!("subscribe failed [{code}]: {message}");
                }
                other => bail!("unexpected subscribe response: {other:?}"),
            },
            Message::Response { id, payload } if id == request_id => match payload {
                ResponsePayload::Ok(OkPayload::ScriptCreated {
                    script_id: created,
                    items,
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
                    subscribe_script_item_outputs(client, &items).await?;
                    if let Some(error) = submit_error {
                        bail!(
                            "script {created} submission failed at item {} [{}]: {}",
                            error.index,
                            error.code,
                            error.message
                        );
                    }
                }
                ResponsePayload::Ok(OkPayload::ScriptFinished { exit_code, .. }) => {
                    return Ok(exit_code);
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
                EventPayload::JobCreated { job_id, .. } => {
                    subscribe_output(client, &job_id).await?;
                }
                EventPayload::OutputChunk { stream, data, .. } => {
                    write_stream(stream, data.as_bytes())?;
                }
                EventPayload::OutputChunkBinary { stream, base64, .. } => {
                    write_stream(stream, base64.as_bytes())?;
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

async fn subscribe_script_item_outputs(
    client: &mut CuedClient,
    items: &[cue_core::ipc::ScriptItemInfo],
) -> Result<()> {
    for item in items {
        match &item.result {
            cue_core::ipc::ScriptItemResult::Job { job_id, .. } => {
                subscribe_output(client, job_id).await?;
            }
            cue_core::ipc::ScriptItemResult::Chain { job_ids, .. } => {
                for job_id in job_ids {
                    subscribe_output(client, job_id).await?;
                }
            }
            cue_core::ipc::ScriptItemResult::Cron { .. }
            | cue_core::ipc::ScriptItemResult::Message { .. } => {}
        }
    }
    Ok(())
}

async fn subscribe_output(client: &mut CuedClient, job_id: &str) -> Result<()> {
    let _ = client
        .send(RequestPayload::Subscribe {
            channels: vec![format!("output:{job_id}")],
        })
        .await?;
    Ok(())
}

fn write_stream(stream: Stream, bytes: &[u8]) -> Result<()> {
    match stream {
        Stream::Stdout => std::io::stdout().write_all(bytes)?,
        Stream::Stderr => std::io::stderr().write_all(bytes)?,
    }
    Ok(())
}
