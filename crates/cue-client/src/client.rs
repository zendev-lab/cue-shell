use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::{Context, Result, bail};
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;

use cue_core::Mode;
use cue_core::ipc::{
    EventPayload, MAX_MESSAGE_SIZE, Message, OkPayload, RequestPayload, ResponsePayload,
    encode_message,
};

/// Client handle for a single connection to the cued daemon.
pub struct CuedClient {
    stream: BoxedClientStream,
    next_id: u32,
}

impl CuedClient {
    /// Build a client from any bidirectional byte stream that speaks the cue IPC.
    pub fn from_stream<S>(stream: S) -> Self
    where
        S: ClientStream + 'static,
    {
        Self {
            stream: Box::new(stream),
            next_id: 1,
        }
    }

    /// Connect to the daemon at `socket_path`.
    pub async fn connect(socket_path: &Path) -> Result<Self> {
        let stream = UnixStream::connect(socket_path)
            .await
            .with_context(|| format!("connect to {}", socket_path.display()))?;
        Ok(Self::from_stream(stream))
    }

    /// Send a request and return the assigned request ID.
    pub async fn send(&mut self, payload: RequestPayload) -> Result<u32> {
        send_request(&mut self.stream, &mut self.next_id, payload).await
    }

    /// Read the next message from the daemon.
    pub async fn recv(&mut self) -> Result<Message> {
        read_message(&mut self.stream).await
    }

    /// Convenience: send an Eval request.
    pub async fn eval(&mut self, input: &str, mode: Mode) -> Result<u32> {
        self.send(RequestPayload::Eval {
            input: input.to_string(),
            mode,
        })
        .await
    }

    /// Convenience: subscribe to event channels.
    pub async fn subscribe(&mut self, channels: &[&str]) -> Result<()> {
        self.send(RequestPayload::Subscribe {
            channels: channels.iter().map(|s| (*s).to_string()).collect(),
        })
        .await?;
        Ok(())
    }

    /// Convenience: send a Ping request.
    pub async fn ping(&mut self) -> Result<u32> {
        self.send(RequestPayload::Ping {}).await
    }

    /// Validate that the daemon speaks the expected IPC protocol.
    pub async fn ping_roundtrip(&mut self) -> Result<()> {
        let ping_id = self.ping().await?;
        match self.recv().await? {
            Message::Response {
                id,
                payload: ResponsePayload::Ok(OkPayload::Pong {}),
            } if id == ping_id => Ok(()),
            message => bail!("unexpected message while validating daemon transport: {message:?}"),
        }
    }

    /// Split the client into read/write halves for concurrent use.
    ///
    /// Returns `(reader, writer_stream)` where the reader can call `recv()`
    /// and the writer keeps the `next_id` counter.
    pub fn into_split(self) -> (ClientReader, ClientWriter) {
        let (read_half, write_half) = io::split(self.stream);
        (
            ClientReader { stream: read_half },
            ClientWriter {
                stream: write_half,
                next_id: self.next_id,
            },
        )
    }
}

/// Read half of a split client connection.
pub struct ClientReader {
    stream: io::ReadHalf<BoxedClientStream>,
}

impl ClientReader {
    /// Read the next message from the daemon.
    pub async fn recv(&mut self) -> Result<Message> {
        read_message(&mut self.stream).await
    }
}

/// Write half of a split client connection.
pub struct ClientWriter {
    stream: io::WriteHalf<BoxedClientStream>,
    next_id: u32,
}

impl ClientWriter {
    /// Send a request and return the assigned request ID.
    pub async fn send(&mut self, payload: RequestPayload) -> Result<u32> {
        send_request(&mut self.stream, &mut self.next_id, payload).await
    }
}

/// A cloneable handle for sending requests to the daemon.
///
/// Internally holds an [`mpsc::Sender`] that feeds a dedicated writer task
/// which owns the actual [`ClientWriter`].
#[derive(Clone)]
pub struct WriterHandle {
    tx: mpsc::Sender<OutboundRequest>,
    next_id: Arc<AtomicU32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriterSendError {
    Full,
    Closed,
}

impl std::fmt::Display for WriterSendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full => f.write_str("writer queue is full"),
            Self::Closed => f.write_str("writer task is closed"),
        }
    }
}

impl std::error::Error for WriterSendError {}

impl WriterHandle {
    /// Enqueue a request payload to be sent to the daemon (non-blocking).
    ///
    /// Returns `Ok(())` if the message was enqueued, or `Err` if the
    /// writer task has exited or the channel is full.
    pub fn try_send(&self, payload: RequestPayload) -> Result<u32, WriterSendError> {
        let request = self.next_request(payload);
        let id = request.id;
        self.tx.try_send(request).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => WriterSendError::Full,
            mpsc::error::TrySendError::Closed(_) => WriterSendError::Closed,
        })?;
        Ok(id)
    }

    /// Enqueue a request asynchronously, waiting for buffer space if needed.
    pub fn send(&self, payload: RequestPayload) -> u32 {
        let request = self.next_request(payload);
        let id = request.id;
        let tx = self.tx.clone();
        tokio::spawn(async move {
            let _ = tx.send(request).await;
        });
        id
    }

    /// Enqueue a request asynchronously, returning an error if the writer task
    /// has already exited.
    pub async fn send_async(&self, payload: RequestPayload) -> Result<u32, WriterSendError> {
        let request = self.next_request(payload);
        let id = request.id;
        self.enqueue_request(request).await?;
        Ok(id)
    }

    fn next_request(&self, payload: RequestPayload) -> OutboundRequest {
        let id = next_atomic_request_id(&self.next_id);
        OutboundRequest { id, payload }
    }

    async fn enqueue_request(&self, request: OutboundRequest) -> Result<(), WriterSendError> {
        self.tx
            .send(request)
            .await
            .map_err(|_| WriterSendError::Closed)
    }
}

/// Spawn a dedicated writer task that owns the [`ClientWriter`] and receives
/// messages from a bounded channel. Returns a [`WriterHandle`] for sending
/// requests.
///
/// The task exits when all [`WriterHandle`] clones are dropped.
pub fn spawn_writer_task(mut writer: ClientWriter) -> WriterHandle {
    let next_id = Arc::new(AtomicU32::new(writer.next_id));
    let (tx, mut rx) = mpsc::channel::<OutboundRequest>(64);
    tokio::spawn(async move {
        while let Some(request) = rx.recv().await {
            if let Err(error) = writer.send_with_id(request.id, request.payload).await {
                tracing::error!(%error, "writer task send error");
                break;
            }
        }
        tracing::debug!("writer task exiting");
    });
    WriterHandle { tx, next_id }
}

type PendingResponses = Arc<StdMutex<HashMap<u32, oneshot::Sender<Result<ResponsePayload>>>>>;

/// High-level shared client that routes responses by request ID so multiple
/// callers can safely share one IPC connection.
pub struct MultiplexedClient {
    writer: WriterHandle,
    pending: PendingResponses,
    events: Mutex<mpsc::UnboundedReceiver<EventPayload>>,
    reader_task: JoinHandle<()>,
}

impl MultiplexedClient {
    /// Build a concurrent request/response client from a split connection.
    pub fn new(reader: ClientReader, writer: WriterHandle) -> Self {
        let pending = Arc::new(StdMutex::new(HashMap::new()));
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let reader_task =
            tokio::spawn(run_multiplex_reader(reader, Arc::clone(&pending), event_tx));
        Self {
            writer,
            pending,
            events: Mutex::new(event_rx),
            reader_task,
        }
    }

    /// Send a request and wait for the matching response payload.
    pub async fn call(&self, payload: RequestPayload) -> Result<ResponsePayload> {
        let request = self.writer.next_request(payload);
        let request_id = request.id;
        let (tx, rx) = oneshot::channel();

        {
            let mut pending = self.pending.lock().expect("lock pending response map");
            pending.insert(request_id, tx);
        }

        if let Err(error) = self.writer.enqueue_request(request).await {
            let mut pending = self.pending.lock().expect("lock pending response map");
            pending.remove(&request_id);
            return Err(anyhow::Error::new(error)).context(format!("send request {request_id}"));
        }

        match rx.await {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!(
                "response waiter for request {request_id} closed"
            )),
        }
    }

    /// Convenience: send an Eval request and wait for its response.
    pub async fn eval(&self, input: &str, mode: Mode) -> Result<ResponsePayload> {
        self.call(RequestPayload::Eval {
            input: input.to_string(),
            mode,
        })
        .await
    }

    /// List jobs with optional server-side limit and pagination metadata.
    pub async fn list_jobs(&self, limit: Option<usize>) -> Result<ResponsePayload> {
        self.call(RequestPayload::ListJobs { limit }).await
    }

    /// List crons with optional server-side limit and pagination metadata.
    pub async fn list_crons(&self, limit: Option<usize>) -> Result<ResponsePayload> {
        self.call(RequestPayload::ListCrons { limit }).await
    }

    /// List scopes with optional server-side limit and pagination metadata.
    pub async fn list_scopes(&self, limit: Option<usize>) -> Result<ResponsePayload> {
        self.call(RequestPayload::ListScopes { limit }).await
    }

    /// Show log/history with optional target id, line limit, and byte tail.
    pub async fn show_log(
        &self,
        id: Option<String>,
        limit: Option<usize>,
        tail_bytes: Option<usize>,
    ) -> Result<ResponsePayload> {
        self.call(RequestPayload::ShowLog {
            id,
            limit,
            tail_bytes,
        })
        .await
    }

    /// Get stdout and stderr for one job with independent byte tails.
    pub async fn job_output(
        &self,
        id: impl Into<String>,
        stdout_bytes: Option<usize>,
        stderr_bytes: Option<usize>,
    ) -> Result<ResponsePayload> {
        self.call(RequestPayload::JobOutput {
            id: id.into(),
            stdout_bytes,
            stderr_bytes,
        })
        .await
    }

    /// Kill a job ID only; cron IDs are rejected by the daemon.
    pub async fn kill_job(&self, id: impl Into<String>) -> Result<ResponsePayload> {
        self.call(RequestPayload::KillJob { id: id.into() }).await
    }

    /// Remove a cron ID only; job IDs are rejected by the daemon.
    pub async fn remove_cron(&self, id: impl Into<String>) -> Result<ResponsePayload> {
        self.call(RequestPayload::RemoveCron { id: id.into() })
            .await
    }

    /// Show HEAD environment with an optional byte tail.
    pub async fn show_env(&self, tail_bytes: Option<usize>) -> Result<ResponsePayload> {
        self.call(RequestPayload::ShowEnv { tail_bytes }).await
    }

    /// Show cue-shell config with an optional byte tail.
    pub async fn show_config(&self, tail_bytes: Option<usize>) -> Result<ResponsePayload> {
        self.call(RequestPayload::ShowConfig { tail_bytes }).await
    }

    /// Receive the next pushed event from the daemon.
    pub async fn next_event(&self) -> Option<EventPayload> {
        self.events.lock().await.recv().await
    }
}

impl Drop for MultiplexedClient {
    fn drop(&mut self) {
        self.reader_task.abort();
        fail_pending_responses(&self.pending, "multiplexed client dropped");
    }
}

const APP_DIR: &str = "cue-shell";

#[doc(hidden)]
pub trait ClientStream: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T> ClientStream for T where T: AsyncRead + AsyncWrite + Send + Unpin {}

type BoxedClientStream = Box<dyn ClientStream>;

/// Resolve the default socket path: `$XDG_RUNTIME_DIR/cue-shell/cued.sock`.
pub fn default_socket_path() -> PathBuf {
    let runtime_dir = if let Ok(dir) = std::env::var("XDG_RUNTIME_DIR") {
        PathBuf::from(dir).join(APP_DIR)
    } else {
        std::env::temp_dir().join(APP_DIR)
    };
    runtime_dir.join("cued.sock")
}

async fn read_message<R>(stream: &mut R) -> Result<Message>
where
    R: AsyncRead + Unpin,
{
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .context("read length prefix")?;
    let len = u32::from_be_bytes(len_buf) as usize;

    if len > MAX_MESSAGE_SIZE {
        bail!("message too large: {len} bytes (max {MAX_MESSAGE_SIZE})");
    }

    let mut body = vec![0u8; len];
    stream
        .read_exact(&mut body)
        .await
        .context("read message body")?;

    serde_json::from_slice(&body).context("deserialize message")
}

async fn send_request<W>(stream: &mut W, next_id: &mut u32, payload: RequestPayload) -> Result<u32>
where
    W: AsyncWrite + Unpin,
{
    let id = *next_id;
    *next_id = next_request_id(*next_id);

    send_request_with_id(stream, id, payload).await?;
    Ok(id)
}

async fn send_request_with_id<W>(stream: &mut W, id: u32, payload: RequestPayload) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let msg = Message::Request { id, payload };
    let buf = encode_message(&msg).context("encode request")?;
    stream.write_all(&buf).await.context("write to socket")?;
    Ok(())
}

struct OutboundRequest {
    id: u32,
    payload: RequestPayload,
}

async fn run_multiplex_reader(
    mut reader: ClientReader,
    pending: PendingResponses,
    event_tx: mpsc::UnboundedSender<EventPayload>,
) {
    let disconnect_reason = loop {
        match reader.recv().await {
            Ok(Message::Response { id, payload }) => {
                let waiter = {
                    let mut pending = pending.lock().expect("lock pending response map");
                    pending.remove(&id)
                };
                if let Some(waiter) = waiter {
                    let _ = waiter.send(Ok(payload));
                } else {
                    tracing::debug!(request_id = id, "dropping response for unknown request");
                }
            }
            Ok(Message::Event { payload }) => {
                let _ = event_tx.send(payload);
            }
            Ok(Message::Request { id, .. }) => {
                tracing::warn!(
                    request_id = id,
                    "client received unexpected request message"
                );
            }
            Err(error) => {
                break format!("cued connection closed: {error}");
            }
        }
    };

    fail_pending_responses(&pending, disconnect_reason);
}

fn fail_pending_responses(pending: &PendingResponses, message: impl Into<String>) {
    let message = message.into();
    let waiters = {
        let mut pending = pending.lock().expect("lock pending response map");
        pending
            .drain()
            .map(|(_, waiter)| waiter)
            .collect::<Vec<_>>()
    };

    for waiter in waiters {
        let _ = waiter.send(Err(anyhow::anyhow!(message.clone())));
    }
}

impl ClientWriter {
    async fn send_with_id(&mut self, id: u32, payload: RequestPayload) -> Result<u32> {
        send_request_with_id(&mut self.stream, id, payload).await?;
        self.next_id = self.next_id.max(next_request_id(id));
        Ok(id)
    }
}

fn next_request_id(current: u32) -> u32 {
    match current {
        u32::MAX => 1,
        _ => current + 1,
    }
}

fn next_atomic_request_id(next_id: &AtomicU32) -> u32 {
    loop {
        let current = next_id.load(Ordering::Relaxed);
        let next = next_request_id(current);
        if next_id
            .compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return current;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use cue_core::ipc::{OkPayload, encode_message};
    use tokio::io::duplex;
    use tokio::time::{Duration, timeout};

    use super::*;

    async fn write_message<S>(stream: &mut S, msg: &Message)
    where
        S: AsyncWrite + Unpin,
    {
        let encoded = encode_message(msg).unwrap();
        stream.write_all(&encoded).await.unwrap();
    }

    #[test]
    fn request_ids_wrap_without_using_zero() {
        assert_eq!(next_request_id(1), 2);
        assert_eq!(next_request_id(u32::MAX - 1), u32::MAX);
        assert_eq!(next_request_id(u32::MAX), 1);
    }

    #[test]
    fn atomic_request_ids_follow_same_wrap_policy() {
        let next_id = AtomicU32::new(u32::MAX);
        assert_eq!(next_atomic_request_id(&next_id), u32::MAX);
        assert_eq!(next_id.load(Ordering::Relaxed), 1);
        assert_eq!(next_atomic_request_id(&next_id), 1);
        assert_eq!(next_id.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn writer_handle_send_async_reports_closed_writer() {
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let writer = WriterHandle {
            tx,
            next_id: Arc::new(AtomicU32::new(1)),
        };

        let error = writer
            .send_async(RequestPayload::Ping {})
            .await
            .unwrap_err();
        assert_eq!(error, WriterSendError::Closed);
    }

    #[tokio::test]
    async fn multiplexed_client_matches_concurrent_eval_responses_by_request_id() {
        let (client_stream, mut server_stream) = duplex(4096);
        let client = CuedClient::from_stream(client_stream);
        let (reader, writer) = client.into_split();
        let client = Arc::new(MultiplexedClient::new(reader, spawn_writer_task(writer)));

        let mut tasks = Vec::new();
        for index in 0..3usize {
            let client = Arc::clone(&client);
            tasks.push(tokio::spawn(async move {
                let response = client
                    .eval(&format!("job-{index}"), Mode::Job)
                    .await
                    .unwrap();
                match response {
                    ResponsePayload::Ok(OkPayload::JobCreated { job_id, .. }) => {
                        assert_eq!(job_id, format!("J{index}"));
                    }
                    other => panic!("unexpected response: {other:?}"),
                }
            }));
        }

        let mut request_inputs = Vec::new();
        for _ in 0..3 {
            let message = read_message(&mut server_stream).await.unwrap();
            match message {
                Message::Request {
                    id,
                    payload: RequestPayload::Eval { input, mode },
                } => {
                    assert_eq!(mode, Mode::Job);
                    request_inputs.push((id, input));
                }
                other => panic!("unexpected request: {other:?}"),
            }
        }

        let unique_request_ids = request_inputs
            .iter()
            .map(|(id, _)| *id)
            .collect::<HashSet<_>>();
        assert_eq!(unique_request_ids.len(), 3);

        for (request_id, input) in request_inputs.iter().rev() {
            let job_suffix = input
                .strip_prefix("job-")
                .expect("test eval input should have job- prefix");
            write_message(
                &mut server_stream,
                &Message::Response {
                    id: *request_id,
                    payload: ResponsePayload::Ok(OkPayload::JobCreated {
                        job_id: format!("J{job_suffix}"),
                        start_scope: None,
                        open_hint: cue_core::ipc::JobOpenHint::Stream,
                        chain_id: None,
                        chain_index: None,
                        chain_total: None,
                        warnings: Vec::new(),
                    }),
                },
            )
            .await;
        }

        for task in tasks {
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn multiplexed_client_reports_disconnect_to_pending_callers() {
        let (client_stream, mut server_stream) = duplex(4096);
        let client = CuedClient::from_stream(client_stream);
        let (reader, writer) = client.into_split();
        let client = Arc::new(MultiplexedClient::new(reader, spawn_writer_task(writer)));

        let first = {
            let client = Arc::clone(&client);
            tokio::spawn(async move { client.call(RequestPayload::Ping {}).await })
        };
        let second = {
            let client = Arc::clone(&client);
            tokio::spawn(async move { client.call(RequestPayload::Ping {}).await })
        };

        for _ in 0..2 {
            let message = read_message(&mut server_stream).await.unwrap();
            match message {
                Message::Request {
                    payload: RequestPayload::Ping {},
                    ..
                } => {}
                other => panic!("unexpected request: {other:?}"),
            }
        }
        drop(server_stream);

        let first_error = timeout(Duration::from_secs(1), first)
            .await
            .expect("first caller timed out")
            .unwrap()
            .unwrap_err();
        assert!(first_error.to_string().contains("cued connection closed"));

        let second_error = timeout(Duration::from_secs(1), second)
            .await
            .expect("second caller timed out")
            .unwrap()
            .unwrap_err();
        assert!(second_error.to_string().contains("cued connection closed"));
    }

    #[tokio::test]
    async fn multiplexed_client_delivers_events_without_consuming_responses() {
        let (client_stream, mut server_stream) = duplex(4096);
        let client = CuedClient::from_stream(client_stream);
        let (reader, writer) = client.into_split();
        let client = Arc::new(MultiplexedClient::new(reader, spawn_writer_task(writer)));

        let response_task = tokio::spawn({
            let client = Arc::clone(&client);
            async move { client.call(RequestPayload::Ping {}).await }
        });

        let request_id = match read_message(&mut server_stream).await.unwrap() {
            Message::Request {
                id,
                payload: RequestPayload::Ping {},
            } => id,
            other => panic!("unexpected request: {other:?}"),
        };

        write_message(
            &mut server_stream,
            &Message::Event {
                payload: EventPayload::DaemonReady {},
            },
        )
        .await;
        write_message(
            &mut server_stream,
            &Message::Response {
                id: request_id,
                payload: ResponsePayload::Ok(OkPayload::Pong {}),
            },
        )
        .await;

        match response_task.await.unwrap().unwrap() {
            ResponsePayload::Ok(OkPayload::Pong {}) => {}
            other => panic!("unexpected response: {other:?}"),
        }

        match client.next_event().await {
            Some(EventPayload::DaemonReady {}) => {}
            other => panic!("unexpected event: {other:?}"),
        }
    }
}
