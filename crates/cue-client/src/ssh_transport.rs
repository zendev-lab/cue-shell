use std::io;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use tokio::process::{Child, ChildStdin, ChildStdout, Command as TokioCommand};
use tokio::task::JoinHandle;

use crate::{ClientConnector, CuedClient, ResolvedTransport};

/// Build a reusable connector for an already-resolved transport profile.
pub fn transport_connector(transport: &ResolvedTransport) -> ClientConnector {
    match transport {
        ResolvedTransport::Unix { socket_path, .. } => ClientConnector::unix(socket_path.clone()),
        ssh_transport @ ResolvedTransport::Ssh { .. } => {
            let ssh_transport = ssh_transport.clone();
            ClientConnector::new(move || {
                let ssh_transport = ssh_transport.clone();
                async move {
                    connect_ssh_transport(&ssh_transport)
                        .await
                        .map(|(client, _version)| client)
                }
            })
        }
    }
}

/// Connect to a remote daemon through `ssh <destination> <gateway_command>`.
///
/// The returned version is the remote daemon's optional `Pong.version` field.
pub async fn connect_ssh_transport(
    transport: &ResolvedTransport,
) -> Result<(CuedClient, Option<String>)> {
    let ResolvedTransport::Ssh {
        profile_name,
        destination,
        gateway_command,
        start_command,
    } = transport
    else {
        bail!("expected an ssh transport profile");
    };

    let SpawnedSshTransport { stream, stderr } =
        spawn_ssh_transport(destination, gateway_command).await?;
    let mut client = CuedClient::from_stream(stream);
    match client.ping_for_version().await {
        Ok(version) => Ok((client, version)),
        Err(error) => Err(ssh_connect_error(
            profile_name,
            destination,
            gateway_command,
            start_command,
            &stderr,
            error,
        )
        .await),
    }
}

pub fn ssh_invocation(destination: &str, remote_command: &str) -> String {
    format!(
        "ssh {} {}",
        shell_quote(destination),
        shell_quote(remote_command)
    )
}

#[derive(Clone, Default)]
struct SharedStderr(Arc<Mutex<Vec<u8>>>);

impl SharedStderr {
    fn push(&self, chunk: &[u8]) {
        self.0
            .lock()
            .expect("ssh stderr buffer lock poisoned")
            .extend_from_slice(chunk);
    }

    fn push_read_error(&self, error: &io::Error) {
        let mut buffer = self.0.lock().expect("ssh stderr buffer lock poisoned");
        if !buffer.is_empty() {
            buffer.push(b'\n');
        }
        buffer.extend_from_slice(format!("failed to read ssh stderr: {error}").as_bytes());
    }

    fn snapshot(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().expect("ssh stderr buffer lock poisoned"))
            .trim()
            .to_string()
    }
}

struct SpawnedSshTransport {
    stream: SshChildStream,
    stderr: SharedStderr,
}

struct SshChildStream {
    _child: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
    _stderr_task: JoinHandle<()>,
}

impl AsyncRead for SshChildStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stdout).poll_read(cx, buf)
    }
}

impl AsyncWrite for SshChildStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stdin).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stdin).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stdin).poll_shutdown(cx)
    }
}

async fn spawn_ssh_transport(
    destination: &str,
    gateway_command: &str,
) -> Result<SpawnedSshTransport> {
    let stderr = SharedStderr::default();
    let mut command = TokioCommand::new("ssh");
    command
        .arg(destination)
        .arg(gateway_command)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let mut child = command
        .spawn()
        .with_context(|| format!("spawn `{}`", ssh_invocation(destination, gateway_command)))?;
    let stdin = child.stdin.take().context("capture ssh stdin")?;
    let stdout = child.stdout.take().context("capture ssh stdout")?;
    let stderr_reader = child.stderr.take().context("capture ssh stderr")?;
    let stderr_capture = stderr.clone();
    let stderr_task = tokio::spawn(async move {
        capture_ssh_stderr(stderr_reader, stderr_capture).await;
    });

    Ok(SpawnedSshTransport {
        stream: SshChildStream {
            _child: child,
            stdin,
            stdout,
            _stderr_task: stderr_task,
        },
        stderr,
    })
}

async fn capture_ssh_stderr<R>(stderr: R, buffer: SharedStderr)
where
    R: AsyncRead + Unpin,
{
    let mut stderr = stderr;
    let mut chunk = [0u8; 1024];
    loop {
        match stderr.read(&mut chunk).await {
            Ok(0) => break,
            Ok(read) => buffer.push(&chunk[..read]),
            Err(error) => {
                tracing::warn!(%error, "failed to read ssh stderr");
                buffer.push_read_error(&error);
                break;
            }
        }
    }
}

async fn ssh_connect_error(
    profile_name: &str,
    destination: &str,
    gateway_command: &str,
    start_command: &str,
    stderr: &SharedStderr,
    error: anyhow::Error,
) -> anyhow::Error {
    let detail = ssh_error_detail(stderr, format!("{error:#}")).await;

    if remote_daemon_missing(&detail) {
        ssh_handshake_error(
            profile_name,
            destination,
            gateway_command,
            start_command,
            &detail,
            &detail,
        )
    } else {
        anyhow::anyhow!(
            "client profile `{profile_name}` failed to connect via `{}`: {detail}",
            ssh_invocation(destination, gateway_command),
        )
    }
}

async fn ssh_error_detail(stderr: &SharedStderr, fallback: String) -> String {
    let detail = stderr.snapshot();
    if !detail.is_empty() {
        return detail;
    }

    tokio::time::sleep(Duration::from_millis(50)).await;
    let detail = stderr.snapshot();
    if detail.is_empty() { fallback } else { detail }
}

fn ssh_handshake_error(
    profile_name: &str,
    destination: &str,
    gateway_command: &str,
    start_command: &str,
    stderr: impl AsRef<str>,
    detail: impl AsRef<str>,
) -> anyhow::Error {
    let stderr = stderr.as_ref();
    let detail = detail.as_ref();
    let remote_start = ssh_invocation(destination, start_command);
    let remote_detail = if stderr.is_empty() {
        detail.to_string()
    } else {
        stderr.to_string()
    };
    anyhow::anyhow!(
        "client profile `{profile_name}` reached `{}` but the remote cue daemon is unavailable. Start it explicitly with `{remote_start}` (or log in and run `{start_command}`) and retry. Remote error: {remote_detail}",
        ssh_invocation(destination, gateway_command),
    )
}

fn remote_daemon_missing(detail: &str) -> bool {
    let detail = detail.to_ascii_lowercase();
    detail.contains("daemon socket not found")
        || detail.contains("daemon socket refused the connection")
        || detail.contains("failed to connect to daemon socket")
}

fn shell_quote(input: &str) -> String {
    if !input.chars().any(|ch| {
        matches!(
            ch,
            ' ' | '\t' | '\n' | '\'' | '"' | '\\' | '$' | '`' | '!' | ';' | '&' | '|' | '<' | '>'
        )
    }) {
        return input.to_string();
    }

    format!("'{}'", input.replace('\'', r"'\''"))
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::{Context as TaskContext, Poll};

    use tokio::io::ReadBuf;

    use super::*;

    struct FailingStderrReader;

    impl AsyncRead for FailingStderrReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Err(io::Error::other("stderr pipe failed")))
        }
    }

    #[test]
    fn ssh_invocation_quotes_remote_command() {
        assert_eq!(
            ssh_invocation(
                "user@example.com",
                "cued start --socket ~/.cache/cue shell.sock"
            ),
            "ssh user@example.com 'cued start --socket ~/.cache/cue shell.sock'"
        );
    }

    #[tokio::test]
    async fn remote_daemon_error_includes_explicit_start_command() {
        let error = ssh_connect_error(
            "remote",
            "devbox",
            "cued gateway --stdio",
            "cued start --socket ~/.cache/cue-shell/remote.sock",
            &SharedStderr::default(),
            anyhow::anyhow!(
                "connect to /run/user/1000/cue-shell/cued.sock: daemon socket not found; is cued running?"
            ),
        )
        .await;

        let message = format!("{error:#}");
        assert!(message.contains("Start it explicitly with"));
        assert!(
            message.contains("ssh devbox 'cued start --socket ~/.cache/cue-shell/remote.sock'")
        );
        assert!(message.contains("daemon socket not found"));
    }

    #[tokio::test]
    async fn stderr_capture_records_read_errors() {
        let stderr = SharedStderr::default();
        stderr.push(b"partial remote stderr");

        capture_ssh_stderr(FailingStderrReader, stderr.clone()).await;

        let detail = stderr.snapshot();
        assert!(detail.contains("partial remote stderr"));
        assert!(detail.contains("failed to read ssh stderr: stderr pipe failed"));
    }
}
