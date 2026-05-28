use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::mpsc;

use cue_core::ipc::Message;

use crate::{ClientReader, CuedClient, WriterHandle, spawn_writer_task};

type ConnectFuture = Pin<Box<dyn Future<Output = Result<CuedClient>> + Send + 'static>>;

/// Commands that can be sent to a controllable connection manager to influence
/// its reconnect behaviour at runtime.
pub enum ReconnectCmd {
    /// Drop the current transport and immediately attempt to reconnect using
    /// the supplied connector.  If the first attempt fails the manager falls
    /// back to its normal periodic retry loop.
    SwitchTarget(ClientConnector),
    /// Shut the connection manager down cleanly.
    Shutdown,
}

/// Cloneable connector used by the shared reconnect loop.
#[derive(Clone)]
pub struct ClientConnector {
    connect: Arc<dyn Fn() -> ConnectFuture + Send + Sync>,
}

impl ClientConnector {
    pub fn new<F, Fut>(connect: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<CuedClient>> + Send + 'static,
    {
        Self {
            connect: Arc::new(move || Box::pin(connect())),
        }
    }

    pub fn unix(socket_path: PathBuf) -> Self {
        Self::new(move || {
            let socket_path = socket_path.clone();
            async move { CuedClient::connect(&socket_path).await }
        })
    }

    pub async fn connect(&self) -> Result<CuedClient> {
        (self.connect)().await
    }
}

/// Default reconnect interval after the daemon disconnects.
pub const DEFAULT_RECONNECT_DELAY: Duration = Duration::from_secs(3);

/// Events produced by the shared connection manager.
pub enum ConnectionEvent {
    Incoming(Message),
    Disconnected,
    ReconnectFailed { message: String },
    Reconnected { writer: WriterHandle },
}

/// Spawn the socket connection manager and return a receiver of shared client
/// events.
pub fn spawn_socket_manager(
    initial_reader: Option<ClientReader>,
    socket_path: PathBuf,
) -> mpsc::UnboundedReceiver<ConnectionEvent> {
    spawn_connection_manager(initial_reader, ClientConnector::unix(socket_path))
}

/// Spawn the socket connection manager with a custom reconnect interval.
pub fn spawn_socket_manager_with_delay(
    initial_reader: Option<ClientReader>,
    socket_path: PathBuf,
    reconnect_delay: Duration,
) -> mpsc::UnboundedReceiver<ConnectionEvent> {
    spawn_connection_manager_with_delay(
        initial_reader,
        ClientConnector::unix(socket_path),
        reconnect_delay,
    )
}

/// Spawn the shared connection manager with a custom connector.
pub fn spawn_connection_manager(
    initial_reader: Option<ClientReader>,
    connector: ClientConnector,
) -> mpsc::UnboundedReceiver<ConnectionEvent> {
    spawn_connection_manager_with_delay(initial_reader, connector, DEFAULT_RECONNECT_DELAY)
}

/// Spawn the shared connection manager with a custom connector and reconnect interval.
pub fn spawn_connection_manager_with_delay(
    initial_reader: Option<ClientReader>,
    connector: ClientConnector,
    reconnect_delay: Duration,
) -> mpsc::UnboundedReceiver<ConnectionEvent> {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(run_connection_manager_with_delay(
        initial_reader,
        connector,
        reconnect_delay,
        tx,
    ));
    rx
}

/// Spawn the connection manager with a control channel.
///
/// Returns `(event_rx, cmd_tx)`:
/// - `event_rx` delivers [`ConnectionEvent`]s to the caller.
/// - `cmd_tx` lets the caller send [`ReconnectCmd`]s (e.g. `SwitchTarget`).
pub fn spawn_connection_manager_controllable(
    initial_reader: Option<ClientReader>,
    connector: ClientConnector,
) -> (
    mpsc::UnboundedReceiver<ConnectionEvent>,
    mpsc::Sender<ReconnectCmd>,
) {
    spawn_connection_manager_controllable_with_delay(
        initial_reader,
        connector,
        DEFAULT_RECONNECT_DELAY,
    )
}

/// Spawn the connection manager with a control channel and a custom reconnect
/// interval.
pub fn spawn_connection_manager_controllable_with_delay(
    initial_reader: Option<ClientReader>,
    connector: ClientConnector,
    reconnect_delay: Duration,
) -> (
    mpsc::UnboundedReceiver<ConnectionEvent>,
    mpsc::Sender<ReconnectCmd>,
) {
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let (cmd_tx, cmd_rx) = mpsc::channel(8);
    tokio::spawn(run_controllable_connection_manager(
        initial_reader,
        connector,
        reconnect_delay,
        event_tx,
        cmd_rx,
    ));
    (event_rx, cmd_tx)
}

/// Long-lived task that reads from the daemon socket and auto-reconnects.
pub async fn run_socket_manager(
    initial_reader: Option<ClientReader>,
    socket_path: PathBuf,
    tx: mpsc::UnboundedSender<ConnectionEvent>,
) {
    run_connection_manager(initial_reader, ClientConnector::unix(socket_path), tx).await;
}

/// Long-lived task that reads from the daemon socket and auto-reconnects using
/// `reconnect_delay`.
pub async fn run_socket_manager_with_delay(
    initial_reader: Option<ClientReader>,
    socket_path: PathBuf,
    reconnect_delay: Duration,
    tx: mpsc::UnboundedSender<ConnectionEvent>,
) {
    run_connection_manager_with_delay(
        initial_reader,
        ClientConnector::unix(socket_path),
        reconnect_delay,
        tx,
    )
    .await;
}

/// Long-lived task that reads from the daemon connection and auto-reconnects.
pub async fn run_connection_manager(
    initial_reader: Option<ClientReader>,
    connector: ClientConnector,
    tx: mpsc::UnboundedSender<ConnectionEvent>,
) {
    run_connection_manager_with_delay(initial_reader, connector, DEFAULT_RECONNECT_DELAY, tx).await;
}

/// Long-lived task that reads from the daemon connection and auto-reconnects using
/// `reconnect_delay`.
pub async fn run_connection_manager_with_delay(
    initial_reader: Option<ClientReader>,
    connector: ClientConnector,
    reconnect_delay: Duration,
    tx: mpsc::UnboundedSender<ConnectionEvent>,
) {
    let mut reader_opt = initial_reader;

    loop {
        if let Some(mut reader) = reader_opt.take() {
            if read_until_disconnect(&mut reader, &tx).await.is_err() {
                return;
            }
            if tx.send(ConnectionEvent::Disconnected).is_err() {
                return;
            }
        }

        let mut failure_reported = false;
        loop {
            tokio::time::sleep(reconnect_delay).await;

            match connector.connect().await {
                Ok(client) => {
                    let (reader, writer) = client.into_split();
                    let writer = spawn_writer_task(writer);
                    if tx.send(ConnectionEvent::Reconnected { writer }).is_err() {
                        return;
                    }
                    reader_opt = Some(reader);
                    break;
                }
                Err(error) => {
                    if !failure_reported {
                        if send_reconnect_failed(&tx, error).is_err() {
                            return;
                        }
                        failure_reported = true;
                    }
                }
            }
        }
    }
}

/// Long-lived controllable task.  Reads from the active connection, forwards
/// messages and handles [`ReconnectCmd`]s concurrently.
async fn run_controllable_connection_manager(
    initial_reader: Option<ClientReader>,
    mut connector: ClientConnector,
    reconnect_delay: Duration,
    tx: mpsc::UnboundedSender<ConnectionEvent>,
    mut cmd_rx: mpsc::Receiver<ReconnectCmd>,
) {
    let mut reader_opt = initial_reader;

    'outer: loop {
        // ── Reading phase ──────────────────────────────────────────────────
        if let Some(mut reader) = reader_opt.take() {
            loop {
                tokio::select! {
                    result = reader.recv() => {
                        match result {
                            Ok(msg) => {
                                if tx.send(ConnectionEvent::Incoming(msg)).is_err() {
                                    return;
                                }
                            }
                            Err(_) => {
                                // Natural disconnect — fall through to reconnect.
                                if tx.send(ConnectionEvent::Disconnected).is_err() {
                                    return;
                                }
                                break;
                            }
                        }
                    }
                    cmd = cmd_rx.recv() => {
                        match cmd {
                            Some(ReconnectCmd::SwitchTarget(new_connector)) => {
                                // Drop current connection and switch connector.
                                drop(reader);
                                connector = new_connector;
                                if tx.send(ConnectionEvent::Disconnected).is_err() {
                                    return;
                                }
                                // Attempt immediate connection before falling back
                                // to the periodic retry loop.
                                match connector.connect().await {
                                    Ok(client) => {
                                        let (new_reader, writer) = client.into_split();
                                        let writer = spawn_writer_task(writer);
                                        if tx.send(ConnectionEvent::Reconnected { writer }).is_err() {
                                            return;
                                        }
                                        reader_opt = Some(new_reader);
                                    }
                                    Err(error) => {
                                        if send_reconnect_failed(&tx, error).is_err() {
                                            return;
                                        }
                                    }
                                }
                                continue 'outer;
                            }
                            Some(ReconnectCmd::Shutdown) | None => return,
                        }
                    }
                }
            }
        }

        // ── Reconnect phase ────────────────────────────────────────────────
        let mut failure_reported = false;
        loop {
            tokio::select! {
                _ = tokio::time::sleep(reconnect_delay) => {
                    match connector.connect().await {
                        Ok(client) => {
                            let (new_reader, writer) = client.into_split();
                            let writer = spawn_writer_task(writer);
                            if tx.send(ConnectionEvent::Reconnected { writer }).is_err() {
                                return;
                            }
                            reader_opt = Some(new_reader);
                            continue 'outer;
                        }
                        Err(error) => {
                            if !failure_reported {
                                if send_reconnect_failed(&tx, error).is_err() {
                                    return;
                                }
                                failure_reported = true;
                            }
                        }
                    }
                }
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(ReconnectCmd::SwitchTarget(new_connector)) => {
                            connector = new_connector;
                            // Try the new target immediately; on failure keep retrying.
                            match connector.connect().await {
                                Ok(client) => {
                                    let (new_reader, writer) = client.into_split();
                                    let writer = spawn_writer_task(writer);
                                    if tx.send(ConnectionEvent::Reconnected { writer }).is_err() {
                                        return;
                                    }
                                    reader_opt = Some(new_reader);
                                    continue 'outer;
                                }
                                Err(error) => {
                                    if send_reconnect_failed(&tx, error).is_err() {
                                        return;
                                    }
                                    failure_reported = true;
                                }
                            }
                        }
                        Some(ReconnectCmd::Shutdown) | None => return,
                    }
                }
            }
        }
    }
}

fn send_reconnect_failed(
    tx: &mpsc::UnboundedSender<ConnectionEvent>,
    error: anyhow::Error,
) -> Result<(), ()> {
    tx.send(ConnectionEvent::ReconnectFailed {
        message: format!("{error:#}"),
    })
    .map_err(|_| ())
}

async fn read_until_disconnect(
    reader: &mut ClientReader,
    tx: &mpsc::UnboundedSender<ConnectionEvent>,
) -> Result<(), ()> {
    loop {
        match reader.recv().await {
            Ok(msg) => {
                if tx.send(ConnectionEvent::Incoming(msg)).is_err() {
                    return Err(());
                }
            }
            Err(_) => return Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::io::{AsyncReadExt, duplex};
    use tokio::time::{Duration, timeout};

    use cue_core::ipc::RequestPayload;

    use super::*;

    #[tokio::test]
    async fn custom_connector_reconnects_and_provides_writer() {
        let (initial_client_stream, initial_daemon_stream) = duplex(256);
        let initial_client = CuedClient::from_stream(initial_client_stream);
        let (initial_reader, _initial_writer) = initial_client.into_split();

        let (daemon_tx, mut daemon_rx) = mpsc::unbounded_channel();
        let attempts = Arc::new(AtomicUsize::new(0));
        let connector = ClientConnector::new({
            let daemon_tx = daemon_tx.clone();
            let attempts = attempts.clone();
            move || {
                let daemon_tx = daemon_tx.clone();
                let attempts = attempts.clone();
                async move {
                    attempts.fetch_add(1, Ordering::Relaxed);
                    let (client_stream, daemon_stream) = duplex(256);
                    daemon_tx.send(daemon_stream).expect("send daemon stream");
                    Ok(CuedClient::from_stream(client_stream))
                }
            }
        });

        let (tx, mut rx) = mpsc::unbounded_channel();
        tokio::spawn(run_connection_manager_with_delay(
            Some(initial_reader),
            connector,
            Duration::from_millis(10),
            tx,
        ));

        drop(initial_daemon_stream);

        let disconnected = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("disconnect timeout")
            .expect("disconnect event");
        assert!(matches!(disconnected, ConnectionEvent::Disconnected));

        let reconnected = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("reconnect timeout")
            .expect("reconnect event");

        let daemon_stream = daemon_rx.recv().await.expect("daemon stream");
        let mut daemon_stream = daemon_stream;
        match reconnected {
            ConnectionEvent::Reconnected { writer } => {
                writer
                    .try_send(RequestPayload::Ping {})
                    .expect("queue ping request");
            }
            _ => panic!("expected reconnect event"),
        }

        let mut len_prefix = [0u8; 4];
        timeout(
            Duration::from_secs(1),
            daemon_stream.read_exact(&mut len_prefix),
        )
        .await
        .expect("writer timeout")
        .expect("read request");
        assert!(u32::from_be_bytes(len_prefix) > 0);
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn switch_target_cmd_reconnects_to_new_connector() {
        let (initial_client_stream, initial_daemon_stream) = duplex(256);
        let initial_client = CuedClient::from_stream(initial_client_stream);
        let (initial_reader, _initial_writer) = initial_client.into_split();

        // Connector for the new target.
        let (new_daemon_tx, mut new_daemon_rx) = mpsc::unbounded_channel();
        let new_connector = ClientConnector::new({
            let new_daemon_tx = new_daemon_tx.clone();
            move || {
                let new_daemon_tx = new_daemon_tx.clone();
                async move {
                    let (client_stream, daemon_stream) = duplex(256);
                    new_daemon_tx
                        .send(daemon_stream)
                        .expect("send daemon stream");
                    Ok(CuedClient::from_stream(client_stream))
                }
            }
        });

        // Stub initial connector — must not be called after SwitchTarget.
        let initial_connector = ClientConnector::new(move || async move {
            let (client_stream, _daemon) = duplex(256);
            Ok(CuedClient::from_stream(client_stream))
        });

        let (mut event_rx, cmd_tx) = spawn_connection_manager_controllable_with_delay(
            Some(initial_reader),
            initial_connector,
            Duration::from_millis(10),
        );

        // Trigger a target switch while connected.
        cmd_tx
            .send(ReconnectCmd::SwitchTarget(new_connector))
            .await
            .expect("send SwitchTarget");

        // Expect Disconnected then Reconnected.
        let ev1 = timeout(Duration::from_secs(2), event_rx.recv())
            .await
            .expect("timeout waiting for Disconnected")
            .expect("channel closed");
        assert!(
            matches!(ev1, ConnectionEvent::Disconnected),
            "expected Disconnected, got other event"
        );

        let ev2 = timeout(Duration::from_secs(2), event_rx.recv())
            .await
            .expect("timeout waiting for Reconnected")
            .expect("channel closed");
        assert!(
            matches!(ev2, ConnectionEvent::Reconnected { .. }),
            "expected Reconnected, got other event"
        );

        // The new daemon stream should have been accepted.
        let _new_daemon_stream = new_daemon_rx
            .recv()
            .await
            .expect("new daemon stream not received");

        // Drop the initial daemon stream (already detached after SwitchTarget).
        drop(initial_daemon_stream);
    }

    #[tokio::test]
    async fn switch_target_reports_failed_attempt_and_keeps_retrying() {
        let (initial_client_stream, initial_daemon_stream) = duplex(256);
        let initial_client = CuedClient::from_stream(initial_client_stream);
        let (initial_reader, _initial_writer) = initial_client.into_split();

        let (new_daemon_tx, mut new_daemon_rx) = mpsc::unbounded_channel();
        let attempts = Arc::new(AtomicUsize::new(0));
        let new_connector = ClientConnector::new({
            let attempts = attempts.clone();
            let new_daemon_tx = new_daemon_tx.clone();
            move || {
                let attempts = attempts.clone();
                let new_daemon_tx = new_daemon_tx.clone();
                async move {
                    if attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                        anyhow::bail!("dial failed");
                    }
                    let (client_stream, daemon_stream) = duplex(256);
                    new_daemon_tx
                        .send(daemon_stream)
                        .expect("send daemon stream");
                    Ok(CuedClient::from_stream(client_stream))
                }
            }
        });

        let initial_connector = ClientConnector::new(move || async move {
            let (client_stream, _daemon) = duplex(256);
            Ok(CuedClient::from_stream(client_stream))
        });

        let (mut event_rx, cmd_tx) = spawn_connection_manager_controllable_with_delay(
            Some(initial_reader),
            initial_connector,
            Duration::from_millis(10),
        );

        cmd_tx
            .send(ReconnectCmd::SwitchTarget(new_connector))
            .await
            .expect("send SwitchTarget");

        let disconnected = timeout(Duration::from_secs(2), event_rx.recv())
            .await
            .expect("timeout waiting for Disconnected")
            .expect("channel closed");
        assert!(matches!(disconnected, ConnectionEvent::Disconnected));

        let failed = timeout(Duration::from_secs(2), event_rx.recv())
            .await
            .expect("timeout waiting for ReconnectFailed")
            .expect("channel closed");
        match failed {
            ConnectionEvent::ReconnectFailed { message } => {
                assert!(message.contains("dial failed"), "{message}");
            }
            _ => panic!("expected ReconnectFailed"),
        }

        let reconnected = timeout(Duration::from_secs(2), event_rx.recv())
            .await
            .expect("timeout waiting for Reconnected")
            .expect("channel closed");
        assert!(matches!(reconnected, ConnectionEvent::Reconnected { .. }));
        let _new_daemon_stream = new_daemon_rx
            .recv()
            .await
            .expect("new daemon stream not received");
        assert_eq!(attempts.load(Ordering::Relaxed), 2);

        drop(initial_daemon_stream);
    }
}
