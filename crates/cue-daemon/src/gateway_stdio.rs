//! Stateless stdio bridge for the local cued Unix socket.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tokio::io::{self, AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::task::JoinHandle;
use tracing::warn;

/// Connect to the local gateway socket and relay raw IPC bytes over stdio.
pub(crate) async fn run(socket_path: PathBuf) -> Result<()> {
    let socket = connect_socket(&socket_path).await?;
    relay(tokio::io::stdin(), tokio::io::stdout(), socket).await
}

async fn connect_socket(socket_path: &Path) -> Result<UnixStream> {
    UnixStream::connect(socket_path).await.map_err(|err| {
        let hint = match err.kind() {
            ErrorKind::NotFound => "daemon socket not found; is cued running?",
            ErrorKind::ConnectionRefused => {
                "daemon socket refused the connection; is cued running?"
            }
            _ => "failed to connect to daemon socket",
        };
        anyhow::anyhow!("connect to {}: {hint} ({err})", socket_path.display())
    })
}

pub(crate) async fn relay<R, W, S>(stdin: R, stdout: W, socket: S) -> Result<()>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut socket_reader, mut socket_writer) = tokio::io::split(socket);

    let mut stdin_task = tokio::spawn(async move {
        let mut stdin = stdin;
        io::copy(&mut stdin, &mut socket_writer)
            .await
            .context("relay stdin to socket")?;
        socket_writer
            .shutdown()
            .await
            .context("shutdown socket write")?;
        Ok::<(), anyhow::Error>(())
    });

    let mut stdout_task = tokio::spawn(async move {
        let mut stdout = stdout;
        io::copy(&mut socket_reader, &mut stdout)
            .await
            .context("relay socket to stdout")?;
        stdout.flush().await.context("flush stdout")?;
        Ok::<(), anyhow::Error>(())
    });

    tokio::select! {
        result = &mut stdin_task => {
            let result = result.context("join stdin relay task")?;
            abort_peer_task(stdout_task, "stdout").await;
            normalize_disconnect(result)
        }
        result = &mut stdout_task => {
            let result = result.context("join stdout relay task")?;
            abort_peer_task(stdin_task, "stdin").await;
            normalize_disconnect(result)
        }
    }
}

async fn abort_peer_task(task: JoinHandle<Result<()>>, direction: &'static str) {
    task.abort();
    match task.await {
        Err(error) if error.is_cancelled() => {}
        Err(error) => {
            warn!(direction, err = %error, "gateway stdio peer relay task failed");
        }
        Ok(Ok(())) => {}
        Ok(Err(error)) if is_disconnect_error(&error) => {}
        Ok(Err(error)) => {
            warn!(direction, err = %error, "gateway stdio peer relay returned an error");
        }
    }
}

fn normalize_disconnect(result: Result<()>) -> Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(err) if is_disconnect_error(&err) => Ok(()),
        Err(err) => Err(err),
    }
}

fn is_disconnect_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io_err| {
                matches!(
                    io_err.kind(),
                    ErrorKind::BrokenPipe
                        | ErrorKind::ConnectionReset
                        | ErrorKind::UnexpectedEof
                        | ErrorKind::NotConnected
                )
            })
    })
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use cue_core::ipc::{Message, OkPayload, RequestPayload, ResponsePayload, encode_message};
    use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, duplex};
    use tokio::time::{Duration, timeout};

    use super::*;

    struct FailingWriter;

    impl AsyncWrite for FailingWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Err(std::io::Error::other("stdout sink failed")))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn relays_ipc_bytes_in_both_directions() {
        let (mut client_input, relay_input) = duplex(4096);
        let (relay_output, mut client_output) = duplex(4096);
        let (relay_socket, mut daemon_socket) = duplex(4096);

        let bridge = tokio::spawn(relay(relay_input, relay_output, relay_socket));

        let request = encode_message(&Message::Request {
            id: 7,
            payload: RequestPayload::Ping {},
        })
        .unwrap();
        client_input.write_all(&request).await.unwrap();

        let mut forwarded_request = vec![0; request.len()];
        daemon_socket
            .read_exact(&mut forwarded_request)
            .await
            .unwrap();
        assert_eq!(forwarded_request, request);

        let response = encode_message(&Message::Response {
            id: 7,
            payload: ResponsePayload::Ok(OkPayload::Pong {
                version: "0.1.0".into(),
            }),
        })
        .unwrap();
        daemon_socket.write_all(&response).await.unwrap();
        daemon_socket.shutdown().await.unwrap();

        let mut forwarded_response = vec![0; response.len()];
        client_output
            .read_exact(&mut forwarded_response)
            .await
            .unwrap();
        assert_eq!(forwarded_response, response);

        timeout(Duration::from_secs(1), bridge)
            .await
            .expect("bridge timed out")
            .expect("bridge task panicked")
            .expect("bridge failed");
    }

    #[tokio::test]
    async fn exits_when_stdin_reaches_eof() {
        let (client_input, relay_input) = duplex(64);
        let (relay_output, _client_output) = duplex(64);
        let (relay_socket, _daemon_socket) = duplex(64);

        drop(client_input);

        timeout(
            Duration::from_secs(1),
            relay(relay_input, relay_output, relay_socket),
        )
        .await
        .expect("bridge timed out")
        .expect("bridge failed");
    }

    #[tokio::test]
    async fn propagates_stdout_write_errors() {
        let (_client_input, relay_input) = duplex(64);
        let (relay_socket, mut daemon_socket) = duplex(64);

        let bridge = tokio::spawn(relay(relay_input, FailingWriter, relay_socket));

        daemon_socket.write_all(b"reply bytes").await.unwrap();

        let error = timeout(Duration::from_secs(1), bridge)
            .await
            .expect("bridge timed out")
            .expect("bridge task panicked")
            .expect_err("bridge should fail when stdout write fails");
        assert!(
            error.to_string().contains("relay socket to stdout"),
            "{error:#}"
        );
        assert!(format!("{error:#}").contains("stdout sink failed"));
    }
}
