use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;

use cue_core::ipc::{
    EventPayload, ForegroundAttachmentInfo, IPC_CAPABILITY_FOREGROUND_OBSERVERS,
    IPC_CAPABILITY_NAMED_SESSIONS, IPC_CAPABILITY_SESSION_ARCHIVE,
    IPC_CAPABILITY_SESSION_HANDSHAKE_REQUIRED, IPC_PROTOCOL_VERSION, MAX_MESSAGE_SIZE, Message,
    OkPayload, RequestPayload, ResponsePayload, SessionInfo, SessionScopeState, encode_message,
};
use cue_core::{EventChannel, Mode};

/// Client handle for a single connection to the cued daemon.
pub struct CuedClient {
    stream: BoxedClientStream,
    next_id: u32,
    daemon_capabilities: Option<BTreeSet<String>>,
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
            daemon_capabilities: None,
        }
    }

    /// Build a client for a custom transport whose daemon capabilities were
    /// already obtained from an authenticated Pong during transport bootstrap.
    ///
    /// Normal socket and SSH callers should use [`Self::connect`] or call
    /// [`Self::ping_for_version`] instead of supplying capabilities directly.
    #[doc(hidden)]
    pub fn from_stream_with_capabilities<S, I, C>(stream: S, capabilities: I) -> Self
    where
        S: ClientStream + 'static,
        I: IntoIterator<Item = C>,
        C: Into<String>,
    {
        let mut client = Self::from_stream(stream);
        client.daemon_capabilities = Some(capabilities.into_iter().map(Into::into).collect());
        client
    }

    /// Connect to the daemon at `socket_path`.
    pub async fn connect(socket_path: &Path) -> Result<Self> {
        let stream = UnixStream::connect(socket_path)
            .await
            .with_context(|| format!("connect to {}", socket_path.display()))?;
        let mut client = Self::from_stream(stream);
        client.handshake().await?;
        let snapshot = client
            .ping_for_protocol_snapshot()
            .await
            .context("query daemon IPC capabilities")?;
        client
            .validate_protocol_snapshot(&snapshot)
            .context("validate daemon IPC capabilities")?;
        Ok(client)
    }

    /// Send and acknowledge the initial session handshake for a newly connected transport.
    pub async fn handshake(&mut self) -> Result<()> {
        let session_id = process_session_id();
        let cwd = std::env::current_dir()
            .context("read current working directory for cue session handshake")?
            .display()
            .to_string();
        let env = std::env::vars().collect::<BTreeMap<_, _>>();
        let request_id = self
            .send(RequestPayload::Handshake {
                session_id,
                cwd,
                env,
                refresh: false,
            })
            .await?;
        match self.recv().await? {
            Message::Response {
                id,
                payload: ResponsePayload::Ok(OkPayload::Ack {}),
            } if id == request_id => Ok(()),
            message => bail!("unexpected message while handshaking with daemon: {message:?}"),
        }
    }

    /// Send a request and return the assigned request ID.
    pub async fn send(&mut self, payload: RequestPayload) -> Result<u32> {
        self.require_request_capability(&payload)?;
        send_request(&mut self.stream, &mut self.next_id, payload).await
    }

    /// Return whether the daemon advertised `capability` in its latest Pong.
    ///
    /// A false result also covers transports created with [`Self::from_stream`]
    /// that have not completed [`Self::ping_for_version`] yet.
    pub fn supports_capability(&self, capability: &str) -> bool {
        self.daemon_capabilities
            .as_ref()
            .is_some_and(|capabilities| capabilities.contains(capability))
    }

    fn require_request_capability(&self, payload: &RequestPayload) -> Result<()> {
        if let Some(capability) = required_request_capability(payload)
            && self
                .daemon_capabilities
                .as_ref()
                .is_some_and(|capabilities| !capabilities.contains(capability))
        {
            bail!(unsupported_capability_message(capability));
        }
        Ok(())
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

    /// Convenience: send a file-script request.
    pub async fn run_script(&mut self, path: &str, input: &str) -> Result<u32> {
        self.send(RequestPayload::RunScript {
            path: path.to_string(),
            input: input.to_string(),
        })
        .await
    }

    /// Acquire the controller lease for a foreground-capable job.
    pub async fn fg_attach(&mut self, id: impl Into<String>) -> Result<u32> {
        self.send(RequestPayload::FgAttach { id: id.into() }).await
    }

    /// Observe a foreground-capable job without taking its controller lease.
    pub async fn fg_watch(&mut self, id: impl Into<String>) -> Result<u32> {
        self.send(RequestPayload::FgWatch { id: id.into() }).await
    }

    /// Claim the free controller lease for the currently watched job.
    pub async fn fg_claim_control(&mut self) -> Result<u32> {
        self.send(RequestPayload::FgClaimControl {}).await
    }

    /// Release the controller lease while remaining attached as an observer.
    pub async fn fg_release_control(&mut self) -> Result<u32> {
        self.send(RequestPayload::FgReleaseControl {}).await
    }

    /// Detach this connection from its current foreground job.
    pub async fn fg_detach(&mut self) -> Result<u32> {
        self.send(RequestPayload::FgDetach {}).await
    }

    /// Send terminal input while this connection owns the controller lease.
    pub async fn fg_input(&mut self, data: impl Into<Vec<u8>>) -> Result<u32> {
        self.send(RequestPayload::FgInput { data: data.into() })
            .await
    }

    /// Resize the PTY while this connection owns the controller lease.
    pub async fn fg_resize(&mut self, cols: u16, rows: u16) -> Result<u32> {
        self.send(RequestPayload::FgResize { cols, rows }).await
    }

    /// Acquire a foreground controller lease and return its atomic snapshot.
    pub async fn fg_attach_roundtrip(
        &mut self,
        id: impl Into<String>,
    ) -> Result<ForegroundAttachmentInfo> {
        let request_id = self.fg_attach(id).await?;
        self.wait_for_foreground_attachment(request_id, "attach foreground")
            .await
    }

    /// Watch a foreground job and return its atomic snapshot.
    pub async fn fg_watch_roundtrip(
        &mut self,
        id: impl Into<String>,
    ) -> Result<ForegroundAttachmentInfo> {
        let request_id = self.fg_watch(id).await?;
        self.wait_for_foreground_attachment(request_id, "watch foreground")
            .await
    }

    /// Create a durable named session from this connection's current scope.
    ///
    /// The daemon also attaches this client to the newly created session.
    pub async fn create_session(&mut self, name: impl Into<String>) -> Result<u32> {
        self.send(RequestPayload::CreateSession { name: name.into() })
            .await
    }

    /// List durable named sessions known to the connected daemon.
    pub async fn list_sessions(&mut self) -> Result<u32> {
        self.send(RequestPayload::ListSessions {}).await
    }

    /// List archived durable named sessions known to the connected daemon.
    pub async fn list_archived_sessions(&mut self) -> Result<u32> {
        self.send(RequestPayload::ListArchivedSessions {}).await
    }

    /// List both active and archived durable named sessions.
    pub async fn list_all_sessions(&mut self) -> Result<u32> {
        self.send(RequestPayload::ListAllSessions {}).await
    }

    /// Archive an idle named session without deleting its durable state.
    pub async fn archive_session(&mut self, selector: impl Into<String>) -> Result<u32> {
        self.send(RequestPayload::ArchiveSession {
            selector: selector.into(),
        })
        .await
    }

    /// Restore a previously archived named session.
    pub async fn restore_session(&mut self, selector: impl Into<String>) -> Result<u32> {
        self.send(RequestPayload::RestoreSession {
            selector: selector.into(),
        })
        .await
    }

    /// Attach this connection to an existing durable named session.
    pub async fn attach_session(
        &mut self,
        selector: impl Into<String>,
        refresh: bool,
    ) -> Result<u32> {
        self.send(RequestPayload::AttachSession {
            selector: selector.into(),
            refresh,
        })
        .await
    }

    /// Inspect this connection's current named session or an explicit selector.
    pub async fn session_info(&mut self, selector: Option<String>) -> Result<u32> {
        self.send(RequestPayload::SessionInfo { selector }).await
    }

    /// Create a named session and wait for its authoritative metadata.
    ///
    /// Call this before splitting the connection for concurrent frontend use.
    pub async fn create_session_roundtrip(
        &mut self,
        name: impl Into<String>,
    ) -> Result<SessionInfo> {
        let request_id = self.create_session(name).await?;
        self.wait_for_session_info(request_id, "create session")
            .await
    }

    /// List named sessions and wait for the authoritative snapshot.
    ///
    /// Call this before splitting the connection for concurrent frontend use.
    pub async fn list_sessions_roundtrip(&mut self) -> Result<Vec<SessionInfo>> {
        let request_id = self.list_sessions().await?;
        self.wait_for_session_list(request_id, "listing sessions")
            .await
    }

    /// List archived named sessions and wait for the authoritative snapshot.
    pub async fn list_archived_sessions_roundtrip(&mut self) -> Result<Vec<SessionInfo>> {
        let request_id = self.list_archived_sessions().await?;
        self.wait_for_session_list(request_id, "listing archived sessions")
            .await
    }

    /// List active and archived named sessions and wait for the authoritative snapshot.
    pub async fn list_all_sessions_roundtrip(&mut self) -> Result<Vec<SessionInfo>> {
        let request_id = self.list_all_sessions().await?;
        self.wait_for_session_list(request_id, "listing all sessions")
            .await
    }

    /// Archive an idle named session and wait for its authoritative metadata.
    pub async fn archive_session_roundtrip(
        &mut self,
        selector: impl Into<String>,
    ) -> Result<SessionInfo> {
        let request_id = self.archive_session(selector).await?;
        self.wait_for_session_info(request_id, "archiving session")
            .await
    }

    /// Restore an archived named session and wait for its authoritative metadata.
    pub async fn restore_session_roundtrip(
        &mut self,
        selector: impl Into<String>,
    ) -> Result<SessionInfo> {
        let request_id = self.restore_session(selector).await?;
        self.wait_for_session_info(request_id, "restoring session")
            .await
    }

    /// Attach this connection to a named session and wait for confirmation.
    ///
    /// This is the preferred frontend entry point: finish the attach before
    /// splitting the connection or submitting session-owned work.
    pub async fn attach_session_roundtrip(
        &mut self,
        selector: impl Into<String>,
        refresh: bool,
    ) -> Result<SessionInfo> {
        let request_id = self.attach_session(selector, refresh).await?;
        self.wait_for_session_info(request_id, "attach session")
            .await
    }

    /// Attach without replacing the named session's scope, with an optional
    /// recovery fallback for a volatile scope lost during daemon restart.
    ///
    /// Even when `refresh_if_needed` is true, the first attach is always
    /// non-refreshing. The current process scope is only submitted after the
    /// daemon confirms through [`Self::session_info_roundtrip`] that the named
    /// session is specifically in [`SessionScopeState::NeedsRefresh`]. This
    /// prevents ordinary reconnects from unexpectedly overwriting a ready
    /// shared session's environment.
    pub async fn attach_session_with_refresh_if_needed(
        &mut self,
        selector: impl Into<String>,
        refresh_if_needed: bool,
    ) -> Result<SessionInfo> {
        let selector = selector.into();
        let initial_error = match self.attach_session_roundtrip(selector.clone(), false).await {
            Ok(session) => return Ok(session),
            Err(error) => error,
        };

        if !refresh_if_needed {
            return Err(initial_error);
        }

        let scope_state = match self.session_info_roundtrip(Some(selector.clone())).await {
            Ok(session) => session.scope_state,
            Err(_) => return Err(initial_error),
        };
        if scope_state != SessionScopeState::NeedsRefresh {
            return Err(initial_error);
        }

        self.attach_session_roundtrip(selector, true)
            .await
            .context("explicitly refresh named session scope after daemon restart")
    }

    /// Inspect a named session and wait for its authoritative metadata.
    ///
    /// `None` selects this connection's current session.
    pub async fn session_info_roundtrip(
        &mut self,
        selector: Option<String>,
    ) -> Result<SessionInfo> {
        let request_id = self.session_info(selector).await?;
        self.wait_for_session_info(request_id, "inspect session")
            .await
    }

    async fn wait_for_session_info(
        &mut self,
        request_id: u32,
        operation: &str,
    ) -> Result<SessionInfo> {
        match self.wait_for_response(request_id).await? {
            ResponsePayload::Ok(OkPayload::SessionInfo(session)) => Ok(*session),
            ResponsePayload::Err { code, message } => {
                bail!("daemon error while {operation} [{code}]: {message}")
            }
            payload => bail!("unexpected response while {operation}: {payload:?}"),
        }
    }

    async fn wait_for_session_list(
        &mut self,
        request_id: u32,
        operation: &str,
    ) -> Result<Vec<SessionInfo>> {
        match self.wait_for_response(request_id).await? {
            ResponsePayload::Ok(OkPayload::SessionList(sessions)) => Ok(sessions),
            ResponsePayload::Err { code, message } => {
                bail!("daemon error while {operation} [{code}]: {message}")
            }
            payload => bail!("unexpected response while {operation}: {payload:?}"),
        }
    }

    async fn wait_for_foreground_attachment(
        &mut self,
        request_id: u32,
        operation: &str,
    ) -> Result<ForegroundAttachmentInfo> {
        match self.wait_for_response(request_id).await? {
            ResponsePayload::Ok(OkPayload::FgAttached(attachment)) => Ok(*attachment),
            ResponsePayload::Err { code, message } => {
                bail!("daemon error while {operation} [{code}]: {message}")
            }
            payload => bail!("unexpected response while {operation}: {payload:?}"),
        }
    }

    async fn wait_for_response(&mut self, request_id: u32) -> Result<ResponsePayload> {
        loop {
            match self.recv().await? {
                Message::Response { id, payload } if id == request_id => return Ok(payload),
                Message::Event { .. } => {}
                Message::Response { id, .. } => {
                    bail!(
                        "received response for request {id} while waiting for request {request_id}"
                    )
                }
                Message::Request { .. } => bail!("daemon sent an unexpected request message"),
            }
        }
    }

    /// Convenience: send a Subscribe request and return its request ID.
    pub async fn subscribe(&mut self, channels: &[EventChannel]) -> Result<u32> {
        self.send(RequestPayload::subscribe(channels)).await
    }

    /// Convenience: send an Unsubscribe request and return its request ID.
    pub async fn unsubscribe(&mut self, channels: &[EventChannel]) -> Result<u32> {
        self.send(RequestPayload::unsubscribe(channels)).await
    }

    /// Convenience: send a Ping request.
    pub async fn ping(&mut self) -> Result<u32> {
        self.send(RequestPayload::Ping {}).await
    }

    /// Validate that the daemon speaks the expected IPC protocol.
    pub async fn ping_roundtrip(&mut self) -> Result<()> {
        self.ping_for_version().await.map(|_| ())
    }

    /// Send a `Ping` and return the daemon-reported version.
    pub async fn ping_for_version(&mut self) -> Result<String> {
        let snapshot = self.ping_for_protocol_snapshot().await?;
        self.validate_protocol_snapshot(&snapshot)?;
        Ok(snapshot.version)
    }

    fn validate_protocol_snapshot(&self, snapshot: &DaemonProtocolSnapshot) -> Result<()> {
        if !snapshot.ready {
            bail!("daemon startup is not ready; retry after restart handoff completes");
        }
        if snapshot.protocol_version < IPC_PROTOCOL_VERSION {
            bail!(
                "daemon IPC protocol version {} is older than required {IPC_PROTOCOL_VERSION}; upgrade/restart cued",
                snapshot.protocol_version
            );
        }
        if !self.supports_capability(IPC_CAPABILITY_SESSION_HANDSHAKE_REQUIRED) {
            bail!(
                "daemon IPC protocol is missing required capability {IPC_CAPABILITY_SESSION_HANDSHAKE_REQUIRED}; upgrade/restart cued"
            );
        }
        Ok(())
    }

    async fn ping_for_protocol_snapshot(&mut self) -> Result<DaemonProtocolSnapshot> {
        let ping_id = self.ping().await?;
        match self.recv().await? {
            Message::Response {
                id,
                payload:
                    ResponsePayload::Ok(OkPayload::Pong {
                        version,
                        ready,
                        protocol_version,
                        capabilities,
                        ..
                    }),
            } if id == ping_id => {
                self.daemon_capabilities = Some(capabilities.into_iter().collect());
                Ok(DaemonProtocolSnapshot {
                    version,
                    ready,
                    protocol_version,
                })
            }
            message => bail!("unexpected message while validating daemon transport: {message:?}"),
        }
    }

    /// Split the client into a reader and cloneable writer handle for
    /// concurrent use by frontends.
    pub fn into_reader_and_writer_handle(self) -> (ClientReader, WriterHandle) {
        let (reader, writer) = self.into_split();
        (reader, spawn_writer_task(writer))
    }

    /// Split the client into raw read/write halves for internal connection
    /// managers.
    ///
    /// Returns `(reader, writer_stream)` where the reader can call `recv()`
    /// and the writer keeps the `next_id` counter.
    pub(crate) fn into_split(self) -> (ClientReader, ClientWriter) {
        let (read_half, write_half) = io::split(self.stream);
        (
            ClientReader { stream: read_half },
            ClientWriter {
                stream: write_half,
                next_id: self.next_id,
                daemon_capabilities: Arc::new(self.daemon_capabilities),
            },
        )
    }
}

struct DaemonProtocolSnapshot {
    version: String,
    ready: bool,
    protocol_version: u32,
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
pub(crate) struct ClientWriter {
    stream: io::WriteHalf<BoxedClientStream>,
    next_id: u32,
    daemon_capabilities: Arc<Option<BTreeSet<String>>>,
}

/// A cloneable handle for sending requests to the daemon.
///
/// Internally holds an [`mpsc::Sender`] that feeds a dedicated writer task
/// which owns the actual split writer stream.
#[derive(Clone)]
pub struct WriterHandle {
    tx: mpsc::Sender<OutboundRequest>,
    next_id: Arc<AtomicU32>,
    daemon_capabilities: Arc<Option<BTreeSet<String>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriterSendError {
    Full,
    Closed,
    UnsupportedCapability { capability: &'static str },
}

impl std::fmt::Display for WriterSendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full => f.write_str("writer queue is full"),
            Self::Closed => f.write_str("writer task is closed"),
            Self::UnsupportedCapability { capability } => {
                f.write_str(&unsupported_capability_message(capability))
            }
        }
    }
}

impl std::error::Error for WriterSendError {}

impl WriterHandle {
    /// Enqueue a request payload to be sent to the daemon (non-blocking).
    ///
    /// Returns `Ok(id)` if the message was enqueued, or `Err` if the
    /// writer task has exited or the channel is full.
    pub fn try_send(&self, payload: RequestPayload) -> Result<u32, WriterSendError> {
        self.require_request_capability(&payload)?;
        let request = self.next_request(payload);
        let id = request.id;
        self.tx.try_send(request).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => WriterSendError::Full,
            mpsc::error::TrySendError::Closed(_) => WriterSendError::Closed,
        })?;
        Ok(id)
    }

    /// Enqueue a request from synchronous UI code.
    pub fn send(&self, payload: RequestPayload) -> Result<u32, WriterSendError> {
        self.try_send(payload)
    }

    /// Enqueue a request asynchronously, returning an error if the writer task
    /// has already exited.
    pub async fn send_async(&self, payload: RequestPayload) -> Result<u32, WriterSendError> {
        self.require_request_capability(&payload)?;
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

    /// Return whether the daemon advertised `capability` before this
    /// connection was split for concurrent frontend use.
    pub fn supports_capability(&self, capability: &str) -> bool {
        self.daemon_capabilities
            .as_ref()
            .as_ref()
            .is_some_and(|capabilities| capabilities.contains(capability))
    }

    fn require_request_capability(&self, payload: &RequestPayload) -> Result<(), WriterSendError> {
        if let Some(capability) = required_request_capability(payload)
            && self
                .daemon_capabilities
                .as_ref()
                .as_ref()
                .is_some_and(|capabilities| !capabilities.contains(capability))
        {
            return Err(WriterSendError::UnsupportedCapability { capability });
        }
        Ok(())
    }
}

/// Spawn a dedicated writer task that owns the split writer stream and receives
/// messages from a bounded channel. Returns a [`WriterHandle`] for sending
/// requests.
///
/// The task exits when all [`WriterHandle`] clones are dropped.
pub(crate) fn spawn_writer_task(mut writer: ClientWriter) -> WriterHandle {
    let next_id = Arc::new(AtomicU32::new(writer.next_id));
    let daemon_capabilities = Arc::clone(&writer.daemon_capabilities);
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
    WriterHandle {
        tx,
        next_id,
        daemon_capabilities,
    }
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

    /// Return whether the daemon advertised `capability` for this connection.
    pub fn supports_capability(&self, capability: &str) -> bool {
        self.writer.supports_capability(capability)
    }

    /// Send a request and wait for the matching response payload.
    pub async fn call(&self, payload: RequestPayload) -> Result<ResponsePayload> {
        self.writer
            .require_request_capability(&payload)
            .map_err(anyhow::Error::new)?;
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

    /// Convenience: send a file-script request and wait for its response.
    pub async fn run_script(
        &self,
        path: impl Into<String>,
        input: impl Into<String>,
    ) -> Result<ResponsePayload> {
        self.call(RequestPayload::RunScript {
            path: path.into(),
            input: input.into(),
        })
        .await
    }

    /// Acquire the controller lease for a foreground-capable job.
    pub async fn fg_attach(&self, id: impl Into<String>) -> Result<ResponsePayload> {
        self.call(RequestPayload::FgAttach { id: id.into() }).await
    }

    /// Observe a foreground-capable job without taking its controller lease.
    pub async fn fg_watch(&self, id: impl Into<String>) -> Result<ResponsePayload> {
        self.call(RequestPayload::FgWatch { id: id.into() }).await
    }

    /// Claim the free controller lease for the currently watched job.
    pub async fn fg_claim_control(&self) -> Result<ResponsePayload> {
        self.call(RequestPayload::FgClaimControl {}).await
    }

    /// Release the controller lease while remaining attached as an observer.
    pub async fn fg_release_control(&self) -> Result<ResponsePayload> {
        self.call(RequestPayload::FgReleaseControl {}).await
    }

    /// Detach this connection from its current foreground job.
    pub async fn fg_detach(&self) -> Result<ResponsePayload> {
        self.call(RequestPayload::FgDetach {}).await
    }

    /// Send terminal input while this connection owns the controller lease.
    pub async fn fg_input(&self, data: impl Into<Vec<u8>>) -> Result<ResponsePayload> {
        self.call(RequestPayload::FgInput { data: data.into() })
            .await
    }

    /// Resize the PTY while this connection owns the controller lease.
    pub async fn fg_resize(&self, cols: u16, rows: u16) -> Result<ResponsePayload> {
        self.call(RequestPayload::FgResize { cols, rows }).await
    }

    /// List archived durable named sessions.
    pub async fn list_archived_sessions(&self) -> Result<ResponsePayload> {
        self.call(RequestPayload::ListArchivedSessions {}).await
    }

    /// List active and archived durable named sessions.
    pub async fn list_all_sessions(&self) -> Result<ResponsePayload> {
        self.call(RequestPayload::ListAllSessions {}).await
    }

    /// Archive an idle named session without deleting it.
    pub async fn archive_session(&self, selector: impl Into<String>) -> Result<ResponsePayload> {
        self.call(RequestPayload::ArchiveSession {
            selector: selector.into(),
        })
        .await
    }

    /// Restore a previously archived named session.
    pub async fn restore_session(&self, selector: impl Into<String>) -> Result<ResponsePayload> {
        self.call(RequestPayload::RestoreSession {
            selector: selector.into(),
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

    /// Idempotently cancel a job, chain, or script and wait for its running
    /// child processes to stop.
    pub async fn cancel_execution(&self, id: impl Into<String>) -> Result<ResponsePayload> {
        self.call(RequestPayload::CancelExecution { id: id.into() })
            .await
    }

    /// Remove a cron ID only; job IDs are rejected by the daemon.
    pub async fn remove_cron(&self, id: impl Into<String>) -> Result<ResponsePayload> {
        self.call(RequestPayload::RemoveCron { id: id.into() })
            .await
    }

    /// Show the current session environment with an optional byte tail.
    pub async fn show_env(&self, tail_bytes: Option<usize>) -> Result<ResponsePayload> {
        self.call(RequestPayload::ShowEnv { tail_bytes }).await
    }

    /// Show cue-shell config with an optional byte tail.
    pub async fn show_config(&self, tail_bytes: Option<usize>) -> Result<ResponsePayload> {
        self.call(RequestPayload::ShowConfig { tail_bytes }).await
    }

    /// Subscribe to pushed event channels and wait for the daemon response.
    pub async fn subscribe(&self, channels: &[EventChannel]) -> Result<ResponsePayload> {
        self.call(RequestPayload::subscribe(channels)).await
    }

    /// Unsubscribe from pushed event channels and wait for the daemon response.
    pub async fn unsubscribe(&self, channels: &[EventChannel]) -> Result<ResponsePayload> {
        self.call(RequestPayload::unsubscribe(channels)).await
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

fn required_request_capability(payload: &RequestPayload) -> Option<&'static str> {
    match payload {
        RequestPayload::FgWatch { .. }
        | RequestPayload::FgClaimControl {}
        | RequestPayload::FgReleaseControl {} => Some(IPC_CAPABILITY_FOREGROUND_OBSERVERS),
        RequestPayload::CreateSession { .. }
        | RequestPayload::ListSessions {}
        | RequestPayload::AttachSession { .. }
        | RequestPayload::SessionInfo { .. } => Some(IPC_CAPABILITY_NAMED_SESSIONS),
        RequestPayload::ListArchivedSessions {}
        | RequestPayload::ListAllSessions {}
        | RequestPayload::ArchiveSession { .. }
        | RequestPayload::RestoreSession { .. } => Some(IPC_CAPABILITY_SESSION_ARCHIVE),
        RequestPayload::Eval { input, .. } if eval_command_name(input) == Some("watch") => {
            Some(IPC_CAPABILITY_FOREGROUND_OBSERVERS)
        }
        _ => None,
    }
}

fn eval_command_name(input: &str) -> Option<&str> {
    let command = input.trim_start().strip_prefix(':')?;
    let end = command
        .find(|character: char| character.is_whitespace() || character == '(')
        .unwrap_or(command.len());
    Some(&command[..end])
}

fn unsupported_capability_message(capability: &'static str) -> String {
    if capability == IPC_CAPABILITY_FOREGROUND_OBSERVERS {
        format!(
            "daemon does not advertise IPC capability `{capability}`; upgrade/restart cued before using :watch or shared foreground control"
        )
    } else if capability == IPC_CAPABILITY_NAMED_SESSIONS {
        format!(
            "daemon does not advertise IPC capability `{capability}`; upgrade/restart cued before using named sessions"
        )
    } else if capability == IPC_CAPABILITY_SESSION_ARCHIVE {
        format!(
            "daemon does not advertise IPC capability `{capability}`; upgrade/restart cued before archiving, restoring, or listing archived sessions"
        )
    } else {
        format!("daemon does not advertise IPC capability `{capability}`; upgrade/restart cued")
    }
}

const APP_DIR: &str = "cue-shell";
static PROCESS_SESSION_ID: OnceLock<String> = OnceLock::new();

#[doc(hidden)]
pub trait ClientStream: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T> ClientStream for T where T: AsyncRead + AsyncWrite + Send + Unpin {}

type BoxedClientStream = Box<dyn ClientStream>;

/// Resolve the default socket path: `$XDG_RUNTIME_DIR/cue-shell/cued.sock`.
pub fn default_socket_path() -> PathBuf {
    default_socket_path_from_env(std::env::var_os("XDG_RUNTIME_DIR"), std::env::temp_dir())
}

fn process_session_id() -> String {
    PROCESS_SESSION_ID.get_or_init(generate_session_id).clone()
}

fn generate_session_id() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let stack = 0u8;
    format!("cue-{}-{}-{:p}", std::process::id(), now, &stack)
}

fn default_socket_path_from_env(xdg_runtime_dir: Option<OsString>, temp_dir: PathBuf) -> PathBuf {
    let runtime_dir = if let Some(dir) = non_empty_env(xdg_runtime_dir) {
        PathBuf::from(dir).join(APP_DIR)
    } else {
        temp_dir.join(APP_DIR)
    };
    runtime_dir.join("cued.sock")
}

fn non_empty_env(value: Option<OsString>) -> Option<OsString> {
    value.filter(|value| !value.is_empty())
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
    let msg = Message::Request {
        id,
        operation_id: None,
        payload,
    };
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

    use cue_core::ipc::{
        ForegroundAttachmentInfo, ForegroundRole, OkPayload, SessionScopeState, encode_message,
    };
    use cue_core::{EventChannel, JobId};
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

    fn enable_shared_foreground(client: &mut CuedClient) {
        client
            .daemon_capabilities
            .get_or_insert_default()
            .insert(IPC_CAPABILITY_FOREGROUND_OBSERVERS.into());
    }

    fn enable_named_sessions(client: &mut CuedClient) {
        client
            .daemon_capabilities
            .get_or_insert_default()
            .insert(IPC_CAPABILITY_NAMED_SESSIONS.into());
    }

    fn enable_session_archive(client: &mut CuedClient) {
        client
            .daemon_capabilities
            .get_or_insert_default()
            .insert(IPC_CAPABILITY_SESSION_ARCHIVE.into());
    }

    fn session_fixture(archived_at_ms: Option<i64>) -> SessionInfo {
        SessionInfo {
            id: "S42".into(),
            name: "shared-bench".into(),
            scope_state: SessionScopeState::ReadyDurable,
            scope_hash: Some("abc123".into()),
            connected_clients: 0,
            restart_safe: true,
            current: false,
            created_at_ms: 10,
            updated_at_ms: 20,
            archived_at_ms,
        }
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

    #[test]
    fn default_socket_path_uses_runtime_dir_when_present() {
        assert_eq!(
            default_socket_path_from_env(Some(OsString::from("/runtime")), PathBuf::from("/tmp")),
            PathBuf::from("/runtime").join(APP_DIR).join("cued.sock")
        );
    }

    #[test]
    fn default_socket_path_uses_temp_dir_when_runtime_dir_is_missing_or_empty() {
        assert_eq!(
            default_socket_path_from_env(None, PathBuf::from("/tmp")),
            PathBuf::from("/tmp").join(APP_DIR).join("cued.sock")
        );
        assert_eq!(
            default_socket_path_from_env(Some(OsString::new()), PathBuf::from("/tmp")),
            PathBuf::from("/tmp").join(APP_DIR).join("cued.sock")
        );
    }

    #[tokio::test]
    async fn cued_client_subscribe_uses_typed_channels_and_returns_request_id() {
        let (client_stream, mut server_stream) = duplex(4096);
        let mut client = CuedClient::from_stream(client_stream);

        let request_id = client
            .subscribe(&[EventChannel::Jobs, EventChannel::Output(JobId(7))])
            .await
            .expect("send subscribe request");

        assert_eq!(request_id, 1);
        match read_message(&mut server_stream).await.unwrap() {
            Message::Request {
                id,
                payload: RequestPayload::Subscribe { channels },
                ..
            } => {
                assert_eq!(id, 1);
                assert_eq!(channels, vec!["jobs", "output:J7"]);
            }
            other => panic!("unexpected request: {other:?}"),
        }
    }

    #[tokio::test]
    async fn cued_client_unsubscribe_uses_typed_channels_and_returns_request_id() {
        let (client_stream, mut server_stream) = duplex(4096);
        let mut client = CuedClient::from_stream(client_stream);

        let request_id = client
            .unsubscribe(&[EventChannel::System])
            .await
            .expect("send unsubscribe request");

        assert_eq!(request_id, 1);
        match read_message(&mut server_stream).await.unwrap() {
            Message::Request {
                id,
                payload: RequestPayload::Unsubscribe { channels },
                ..
            } => {
                assert_eq!(id, 1);
                assert_eq!(channels, vec!["system"]);
            }
            other => panic!("unexpected request: {other:?}"),
        }
    }

    #[tokio::test]
    async fn cued_client_foreground_helpers_emit_typed_requests() {
        let (client_stream, mut server_stream) = duplex(4096);
        let mut client = CuedClient::from_stream(client_stream);
        enable_shared_foreground(&mut client);

        assert_eq!(client.fg_attach("J1").await.unwrap(), 1);
        assert_eq!(client.fg_watch("J2").await.unwrap(), 2);
        assert_eq!(client.fg_claim_control().await.unwrap(), 3);
        assert_eq!(client.fg_release_control().await.unwrap(), 4);
        assert_eq!(client.fg_input(b"whoami\r".to_vec()).await.unwrap(), 5);
        assert_eq!(client.fg_resize(120, 40).await.unwrap(), 6);
        assert_eq!(client.fg_detach().await.unwrap(), 7);

        let expected = [
            RequestPayload::FgAttach { id: "J1".into() },
            RequestPayload::FgWatch { id: "J2".into() },
            RequestPayload::FgClaimControl {},
            RequestPayload::FgReleaseControl {},
            RequestPayload::FgInput {
                data: b"whoami\r".to_vec(),
            },
            RequestPayload::FgResize {
                cols: 120,
                rows: 40,
            },
            RequestPayload::FgDetach {},
        ];

        for (index, expected_payload) in expected.into_iter().enumerate() {
            match read_message(&mut server_stream).await.unwrap() {
                Message::Request { id, payload, .. } => {
                    assert_eq!(id, index as u32 + 1);
                    assert_eq!(
                        serde_json::to_value(payload).unwrap(),
                        serde_json::to_value(expected_payload).unwrap()
                    );
                }
                other => panic!("unexpected request: {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn shared_foreground_requests_are_gated_before_writing_to_an_old_daemon() {
        let (client_stream, mut server_stream) = duplex(4096);
        let mut client =
            CuedClient::from_stream_with_capabilities(client_stream, std::iter::empty::<&str>());

        assert_eq!(client.fg_attach("J1").await.unwrap(), 1);
        assert!(matches!(
            read_message(&mut server_stream).await.unwrap(),
            Message::Request {
                payload: RequestPayload::FgAttach { .. },
                ..
            }
        ));

        for error in [
            client.fg_watch("J1").await.unwrap_err(),
            client.fg_claim_control().await.unwrap_err(),
            client.fg_release_control().await.unwrap_err(),
            client.eval(":watch J1", Mode::Job).await.unwrap_err(),
            client
                .eval(":watch(read_only=true) J1", Mode::Job)
                .await
                .unwrap_err(),
        ] {
            let message = error.to_string();
            assert!(message.contains(IPC_CAPABILITY_FOREGROUND_OBSERVERS));
            assert!(message.contains("upgrade/restart cued"));
        }

        assert!(
            timeout(Duration::from_millis(20), read_message(&mut server_stream))
                .await
                .is_err(),
            "capability-gated requests must not reach the daemon"
        );
    }

    #[tokio::test]
    async fn pong_capabilities_survive_the_reader_writer_split() {
        let (client_stream, mut server_stream) = duplex(4096);
        let client_task = tokio::spawn(async move {
            let mut client = CuedClient::from_stream(client_stream);
            assert!(!client.supports_capability(IPC_CAPABILITY_FOREGROUND_OBSERVERS));
            assert_eq!(client.ping_for_version().await.unwrap(), "0.1.0");
            assert!(client.supports_capability(IPC_CAPABILITY_FOREGROUND_OBSERVERS));
            let (_reader, writer) = client.into_reader_and_writer_handle();
            assert!(writer.supports_capability(IPC_CAPABILITY_FOREGROUND_OBSERVERS));
            writer.try_send(RequestPayload::FgReleaseControl {})
        });

        let ping_id = match read_message(&mut server_stream).await.unwrap() {
            Message::Request {
                id,
                payload: RequestPayload::Ping {},
                ..
            } => id,
            other => panic!("unexpected request: {other:?}"),
        };
        write_message(
            &mut server_stream,
            &Message::Response {
                id: ping_id,
                payload: ResponsePayload::Ok(OkPayload::Pong {
                    version: "0.1.0".into(),
                    instance_id: String::new(),
                    generation_id: String::new(),
                    ready: true,
                    protocol_version: IPC_PROTOCOL_VERSION,
                    capabilities: vec![
                        IPC_CAPABILITY_SESSION_HANDSHAKE_REQUIRED.into(),
                        IPC_CAPABILITY_FOREGROUND_OBSERVERS.into(),
                    ],
                }),
            },
        )
        .await;

        let release_id = client_task.await.unwrap().unwrap();
        assert!(matches!(
            read_message(&mut server_stream).await.unwrap(),
            Message::Request {
                id,
                payload: RequestPayload::FgReleaseControl {},
                ..
            } if id == release_id
        ));
    }

    #[tokio::test]
    async fn fg_watch_roundtrip_returns_atomic_attachment_snapshot() {
        let (client_stream, mut server_stream) = duplex(4096);
        let mut client = CuedClient::from_stream(client_stream);
        enable_shared_foreground(&mut client);
        let watching = tokio::spawn(async move { client.fg_watch_roundtrip("J9").await });

        let request_id = match read_message(&mut server_stream).await.unwrap() {
            Message::Request {
                id,
                payload: RequestPayload::FgWatch { id: job_id },
                ..
            } => {
                assert_eq!(job_id, "J9");
                id
            }
            other => panic!("unexpected request: {other:?}"),
        };
        let attachment = ForegroundAttachmentInfo {
            id: "J9".into(),
            attachment_id: 9,
            role: ForegroundRole::Observer,
            control_available: false,
            snapshot: b"existing output\r\n".to_vec(),
            snapshot_truncated: true,
        };
        write_message(
            &mut server_stream,
            &Message::Response {
                id: request_id,
                payload: ResponsePayload::Ok(OkPayload::FgAttached(Box::new(attachment.clone()))),
            },
        )
        .await;

        assert_eq!(watching.await.unwrap().unwrap(), attachment);
    }

    #[tokio::test]
    async fn cued_client_named_session_helpers_emit_typed_requests() {
        let (client_stream, mut server_stream) = duplex(4096);
        let mut client = CuedClient::from_stream(client_stream);
        enable_named_sessions(&mut client);

        assert_eq!(client.create_session("bench").await.unwrap(), 1);
        assert_eq!(client.list_sessions().await.unwrap(), 2);
        assert_eq!(client.attach_session("S42", false).await.unwrap(), 3);
        assert_eq!(client.session_info(Some("bench".into())).await.unwrap(), 4);

        let expected = [
            RequestPayload::CreateSession {
                name: "bench".into(),
            },
            RequestPayload::ListSessions {},
            RequestPayload::AttachSession {
                selector: "S42".into(),
                refresh: false,
            },
            RequestPayload::SessionInfo {
                selector: Some("bench".into()),
            },
        ];

        for (index, expected_payload) in expected.into_iter().enumerate() {
            match read_message(&mut server_stream).await.unwrap() {
                Message::Request { id, payload, .. } => {
                    assert_eq!(id, index as u32 + 1);
                    assert_eq!(
                        serde_json::to_value(payload).unwrap(),
                        serde_json::to_value(expected_payload).unwrap()
                    );
                }
                other => panic!("unexpected request: {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn named_session_requests_are_gated_before_writing_to_an_old_daemon() {
        let (client_stream, mut server_stream) = duplex(4096);
        let mut client =
            CuedClient::from_stream_with_capabilities(client_stream, std::iter::empty::<&str>());

        for error in [
            client.create_session("bench").await.unwrap_err(),
            client.list_sessions().await.unwrap_err(),
            client.attach_session("bench", false).await.unwrap_err(),
            client.session_info(Some("bench".into())).await.unwrap_err(),
        ] {
            let message = error.to_string();
            assert!(message.contains(IPC_CAPABILITY_NAMED_SESSIONS));
            assert!(message.contains("upgrade/restart cued"));
        }
        assert!(
            timeout(Duration::from_millis(20), read_message(&mut server_stream))
                .await
                .is_err(),
            "named-session requests must not reach an old daemon"
        );
    }

    #[tokio::test]
    async fn session_archive_roundtrips_emit_typed_requests_and_decode_metadata() {
        let (client_stream, mut server_stream) = duplex(8192);
        let mut client = CuedClient::from_stream(client_stream);
        enable_session_archive(&mut client);
        let client_task = tokio::spawn(async move {
            let archived = client.list_archived_sessions_roundtrip().await.unwrap();
            let all = client.list_all_sessions_roundtrip().await.unwrap();
            let archived_info = client
                .archive_session_roundtrip("shared-bench")
                .await
                .unwrap();
            let restored_info = client.restore_session_roundtrip("S42").await.unwrap();
            (archived, all, archived_info, restored_info)
        });

        let archived = session_fixture(Some(30));
        let restored = session_fixture(None);
        let expected = [
            RequestPayload::ListArchivedSessions {},
            RequestPayload::ListAllSessions {},
            RequestPayload::ArchiveSession {
                selector: "shared-bench".into(),
            },
            RequestPayload::RestoreSession {
                selector: "S42".into(),
            },
        ];
        for (index, expected_payload) in expected.into_iter().enumerate() {
            let request_id = match read_message(&mut server_stream).await.unwrap() {
                Message::Request { id, payload, .. } => {
                    assert_eq!(
                        serde_json::to_value(payload).unwrap(),
                        serde_json::to_value(expected_payload).unwrap()
                    );
                    id
                }
                other => panic!("unexpected request: {other:?}"),
            };
            let payload = match index {
                0 | 1 => ResponsePayload::Ok(OkPayload::SessionList(vec![archived.clone()])),
                2 => ResponsePayload::Ok(OkPayload::SessionInfo(Box::new(archived.clone()))),
                3 => ResponsePayload::Ok(OkPayload::SessionInfo(Box::new(restored.clone()))),
                _ => unreachable!(),
            };
            write_message(
                &mut server_stream,
                &Message::Response {
                    id: request_id,
                    payload,
                },
            )
            .await;
        }

        let (archived_list, all_list, archived_info, restored_info) = client_task.await.unwrap();
        assert_eq!(archived_list, vec![archived.clone()]);
        assert_eq!(all_list, vec![archived.clone()]);
        assert_eq!(archived_info, archived);
        assert_eq!(restored_info, restored);
    }

    #[tokio::test]
    async fn session_archive_requests_are_gated_before_writing_to_an_old_daemon() {
        let (client_stream, mut server_stream) = duplex(4096);
        let mut client =
            CuedClient::from_stream_with_capabilities(client_stream, std::iter::empty::<&str>());

        for error in [
            client.list_archived_sessions().await.unwrap_err(),
            client.list_all_sessions().await.unwrap_err(),
            client.archive_session("shared-bench").await.unwrap_err(),
            client.restore_session("shared-bench").await.unwrap_err(),
        ] {
            let message = error.to_string();
            assert!(message.contains(IPC_CAPABILITY_SESSION_ARCHIVE));
            assert!(message.contains("upgrade/restart cued"));
        }
        assert!(
            timeout(Duration::from_millis(20), read_message(&mut server_stream))
                .await
                .is_err(),
            "archive requests must not reach an old daemon"
        );
    }

    #[tokio::test]
    async fn split_and_multiplexed_archive_requests_do_not_write_to_an_old_daemon() {
        let (writer_client_stream, mut writer_server_stream) = duplex(4096);
        let writer_client = CuedClient::from_stream_with_capabilities(
            writer_client_stream,
            std::iter::empty::<&str>(),
        );
        let (_reader, writer) = writer_client.into_reader_and_writer_handle();
        for payload in [
            RequestPayload::ListArchivedSessions {},
            RequestPayload::ListAllSessions {},
            RequestPayload::ArchiveSession {
                selector: "shared-bench".into(),
            },
            RequestPayload::RestoreSession {
                selector: "shared-bench".into(),
            },
        ] {
            assert!(matches!(
                writer.try_send(payload),
                Err(WriterSendError::UnsupportedCapability {
                    capability: IPC_CAPABILITY_SESSION_ARCHIVE
                })
            ));
        }
        assert!(
            timeout(
                Duration::from_millis(20),
                read_message(&mut writer_server_stream)
            )
            .await
            .is_err(),
            "split writer must gate archive requests before enqueueing"
        );

        let (multiplexed_client_stream, mut multiplexed_server_stream) = duplex(4096);
        let multiplexed_client = CuedClient::from_stream_with_capabilities(
            multiplexed_client_stream,
            std::iter::empty::<&str>(),
        );
        let (reader, writer) = multiplexed_client.into_reader_and_writer_handle();
        let client = MultiplexedClient::new(reader, writer);
        for error in [
            client.list_archived_sessions().await.unwrap_err(),
            client.list_all_sessions().await.unwrap_err(),
            client.archive_session("shared-bench").await.unwrap_err(),
            client.restore_session("shared-bench").await.unwrap_err(),
        ] {
            assert!(error.to_string().contains(IPC_CAPABILITY_SESSION_ARCHIVE));
        }
        assert!(
            timeout(
                Duration::from_millis(20),
                read_message(&mut multiplexed_server_stream)
            )
            .await
            .is_err(),
            "multiplexed client must gate archive requests before enqueueing"
        );
    }

    #[tokio::test]
    async fn attach_session_roundtrip_returns_authoritative_session_info() {
        let (client_stream, mut server_stream) = duplex(4096);
        let mut client = CuedClient::from_stream(client_stream);
        enable_named_sessions(&mut client);
        let attached =
            tokio::spawn(
                async move { client.attach_session_roundtrip("shared-bench", false).await },
            );

        let request_id = match read_message(&mut server_stream).await.unwrap() {
            Message::Request {
                id,
                payload: RequestPayload::AttachSession { selector, refresh },
                ..
            } => {
                assert_eq!(selector, "shared-bench");
                assert!(!refresh);
                id
            }
            other => panic!("unexpected request: {other:?}"),
        };

        let session = SessionInfo {
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
        };
        write_message(
            &mut server_stream,
            &Message::Response {
                id: request_id,
                payload: ResponsePayload::Ok(OkPayload::SessionInfo(Box::new(session.clone()))),
            },
        )
        .await;

        assert_eq!(attached.await.unwrap().unwrap(), session);
    }

    #[tokio::test]
    async fn attach_session_roundtrip_preserves_typed_daemon_error() {
        let (client_stream, mut server_stream) = duplex(4096);
        let mut client = CuedClient::from_stream(client_stream);
        enable_named_sessions(&mut client);
        let attached =
            tokio::spawn(async move { client.attach_session_roundtrip("missing", false).await });

        let request_id = match read_message(&mut server_stream).await.unwrap() {
            Message::Request {
                id,
                payload: RequestPayload::AttachSession { .. },
                ..
            } => id,
            other => panic!("unexpected request: {other:?}"),
        };
        write_message(
            &mut server_stream,
            &Message::Response {
                id: request_id,
                payload: ResponsePayload::Err {
                    code: "NOT_FOUND".into(),
                    message: "unknown session".into(),
                },
            },
        )
        .await;

        let error = attached.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("[NOT_FOUND]: unknown session"));
    }

    #[tokio::test]
    async fn refresh_if_needed_does_not_replace_a_ready_session_scope() {
        let (client_stream, mut server_stream) = duplex(4096);
        let mut client = CuedClient::from_stream(client_stream);
        enable_named_sessions(&mut client);
        let attached = tokio::spawn(async move {
            client
                .attach_session_with_refresh_if_needed("shared-bench", true)
                .await
        });

        let request_id = match read_message(&mut server_stream).await.unwrap() {
            Message::Request {
                id,
                payload: RequestPayload::AttachSession { selector, refresh },
                ..
            } => {
                assert_eq!(selector, "shared-bench");
                assert!(
                    !refresh,
                    "ready sessions must be probed without replacement"
                );
                id
            }
            other => panic!("unexpected request: {other:?}"),
        };
        let session = SessionInfo {
            id: "S42".into(),
            name: "shared-bench".into(),
            scope_state: SessionScopeState::ReadyVolatile,
            scope_hash: Some("volatile-scope".into()),
            connected_clients: 2,
            restart_safe: false,
            current: true,
            created_at_ms: 10,
            updated_at_ms: 20,
            archived_at_ms: None,
        };
        write_message(
            &mut server_stream,
            &Message::Response {
                id: request_id,
                payload: ResponsePayload::Ok(OkPayload::SessionInfo(Box::new(session.clone()))),
            },
        )
        .await;

        assert_eq!(attached.await.unwrap().unwrap(), session);
    }

    #[tokio::test]
    async fn refresh_if_needed_replaces_only_a_confirmed_needs_refresh_scope() {
        let (client_stream, mut server_stream) = duplex(4096);
        let mut client = CuedClient::from_stream(client_stream);
        enable_named_sessions(&mut client);
        let attached = tokio::spawn(async move {
            client
                .attach_session_with_refresh_if_needed("shared-bench", true)
                .await
        });

        let initial_attach_id = match read_message(&mut server_stream).await.unwrap() {
            Message::Request {
                id,
                payload: RequestPayload::AttachSession { selector, refresh },
                ..
            } => {
                assert_eq!(selector, "shared-bench");
                assert!(!refresh);
                id
            }
            other => panic!("unexpected request: {other:?}"),
        };
        write_message(
            &mut server_stream,
            &Message::Response {
                id: initial_attach_id,
                payload: ResponsePayload::Err {
                    code: "INVALID_STATE".into(),
                    message: "volatile scope was lost during daemon restart".into(),
                },
            },
        )
        .await;

        let info_id = match read_message(&mut server_stream).await.unwrap() {
            Message::Request {
                id,
                payload: RequestPayload::SessionInfo { selector },
                ..
            } => {
                assert_eq!(selector.as_deref(), Some("shared-bench"));
                id
            }
            other => panic!("expected session-state probe, got {other:?}"),
        };
        write_message(
            &mut server_stream,
            &Message::Response {
                id: info_id,
                payload: ResponsePayload::Ok(OkPayload::SessionInfo(Box::new(SessionInfo {
                    id: "S42".into(),
                    name: "shared-bench".into(),
                    scope_state: SessionScopeState::NeedsRefresh,
                    scope_hash: None,
                    connected_clients: 0,
                    restart_safe: false,
                    current: false,
                    created_at_ms: 10,
                    updated_at_ms: 20,
                    archived_at_ms: None,
                }))),
            },
        )
        .await;

        let refresh_id = match read_message(&mut server_stream).await.unwrap() {
            Message::Request {
                id,
                payload: RequestPayload::AttachSession { selector, refresh },
                ..
            } => {
                assert_eq!(selector, "shared-bench");
                assert!(refresh);
                id
            }
            other => panic!("expected explicit refresh attach, got {other:?}"),
        };
        let refreshed = SessionInfo {
            id: "S42".into(),
            name: "shared-bench".into(),
            scope_state: SessionScopeState::ReadyVolatile,
            scope_hash: Some("new-volatile-scope".into()),
            connected_clients: 1,
            restart_safe: false,
            current: true,
            created_at_ms: 10,
            updated_at_ms: 30,
            archived_at_ms: None,
        };
        write_message(
            &mut server_stream,
            &Message::Response {
                id: refresh_id,
                payload: ResponsePayload::Ok(OkPayload::SessionInfo(Box::new(refreshed.clone()))),
            },
        )
        .await;

        assert_eq!(attached.await.unwrap().unwrap(), refreshed);
    }

    #[tokio::test]
    async fn writer_handle_send_async_reports_closed_writer() {
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let writer = WriterHandle {
            tx,
            next_id: Arc::new(AtomicU32::new(1)),
            daemon_capabilities: Arc::new(None),
        };

        let error = writer
            .send_async(RequestPayload::Ping {})
            .await
            .unwrap_err();
        assert_eq!(error, WriterSendError::Closed);
    }

    #[test]
    fn writer_handle_send_reports_closed_writer() {
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let writer = WriterHandle {
            tx,
            next_id: Arc::new(AtomicU32::new(1)),
            daemon_capabilities: Arc::new(None),
        };

        let error = writer.send(RequestPayload::Ping {}).unwrap_err();
        assert_eq!(error, WriterSendError::Closed);
    }

    #[tokio::test]
    async fn reader_writer_handle_split_sends_requests_without_exposing_raw_writer() {
        let (client_stream, mut server_stream) = duplex(4096);
        let client = CuedClient::from_stream(client_stream);
        let (_reader, writer) = client.into_reader_and_writer_handle();

        let request_id = writer
            .send_async(RequestPayload::Ping {})
            .await
            .expect("send ping through writer handle");

        assert_eq!(request_id, 1);
        match read_message(&mut server_stream).await.unwrap() {
            Message::Request {
                id,
                payload: RequestPayload::Ping {},
                ..
            } => assert_eq!(id, request_id),
            other => panic!("unexpected request: {other:?}"),
        }
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
                    ..
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
    async fn multiplexed_foreground_watch_routes_atomic_attachment_response() {
        let (client_stream, mut server_stream) = duplex(4096);
        let mut client = CuedClient::from_stream(client_stream);
        enable_shared_foreground(&mut client);
        let (reader, writer) = client.into_split();
        let client = Arc::new(MultiplexedClient::new(reader, spawn_writer_task(writer)));

        let watching = tokio::spawn({
            let client = Arc::clone(&client);
            async move { client.fg_watch("J12").await }
        });
        let request_id = match read_message(&mut server_stream).await.unwrap() {
            Message::Request {
                id,
                payload: RequestPayload::FgWatch { id: job_id },
                ..
            } => {
                assert_eq!(job_id, "J12");
                id
            }
            other => panic!("unexpected request: {other:?}"),
        };
        write_message(
            &mut server_stream,
            &Message::Response {
                id: request_id,
                payload: ResponsePayload::Ok(OkPayload::FgAttached(Box::new(
                    ForegroundAttachmentInfo {
                        id: "J12".into(),
                        attachment_id: 12,
                        role: ForegroundRole::Observer,
                        control_available: false,
                        snapshot: b"shared output".to_vec(),
                        snapshot_truncated: false,
                    },
                ))),
            },
        )
        .await;

        match watching.await.unwrap().unwrap() {
            ResponsePayload::Ok(OkPayload::FgAttached(attachment)) => {
                assert_eq!(attachment.id, "J12");
                assert_eq!(attachment.role, ForegroundRole::Observer);
                assert_eq!(attachment.snapshot, b"shared output");
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }

    #[tokio::test]
    async fn multiplexed_client_gates_shared_foreground_before_allocating_or_writing() {
        let (client_stream, mut server_stream) = duplex(4096);
        let client =
            CuedClient::from_stream_with_capabilities(client_stream, std::iter::empty::<&str>());
        let (reader, writer) = client.into_split();
        let client = MultiplexedClient::new(reader, spawn_writer_task(writer));

        for error in [
            client.fg_watch("J4").await.unwrap_err(),
            client.eval(":watch J4", Mode::Job).await.unwrap_err(),
        ] {
            let message = error.to_string();
            assert!(message.contains(IPC_CAPABILITY_FOREGROUND_OBSERVERS));
            assert!(message.contains("upgrade/restart cued"));
        }
        let named_error = client
            .call(RequestPayload::ListSessions {})
            .await
            .unwrap_err()
            .to_string();
        assert!(named_error.contains(IPC_CAPABILITY_NAMED_SESSIONS));
        assert!(named_error.contains("upgrade/restart cued"));
        assert!(
            timeout(Duration::from_millis(20), read_message(&mut server_stream))
                .await
                .is_err(),
            "multiplexed capability failures must not write a request"
        );
    }

    #[tokio::test]
    async fn multiplexed_client_subscribe_waits_for_daemon_response() {
        let (client_stream, mut server_stream) = duplex(4096);
        let client = CuedClient::from_stream(client_stream);
        let (reader, writer) = client.into_split();
        let client = Arc::new(MultiplexedClient::new(reader, spawn_writer_task(writer)));

        let response_task = tokio::spawn({
            let client = Arc::clone(&client);
            async move {
                client
                    .subscribe(&[EventChannel::Crons, EventChannel::System])
                    .await
            }
        });

        let request_id = match read_message(&mut server_stream).await.unwrap() {
            Message::Request {
                id,
                payload: RequestPayload::Subscribe { channels },
                ..
            } => {
                assert_eq!(channels, vec!["crons", "system"]);
                id
            }
            other => panic!("unexpected request: {other:?}"),
        };
        write_message(
            &mut server_stream,
            &Message::Response {
                id: request_id,
                payload: ResponsePayload::Ok(OkPayload::Ack {}),
            },
        )
        .await;

        match response_task.await.unwrap().unwrap() {
            ResponsePayload::Ok(OkPayload::Ack {}) => {}
            other => panic!("unexpected response: {other:?}"),
        }
    }

    #[tokio::test]
    async fn multiplexed_client_unsubscribe_waits_for_daemon_response() {
        let (client_stream, mut server_stream) = duplex(4096);
        let client = CuedClient::from_stream(client_stream);
        let (reader, writer) = client.into_split();
        let client = Arc::new(MultiplexedClient::new(reader, spawn_writer_task(writer)));

        let response_task = tokio::spawn({
            let client = Arc::clone(&client);
            async move { client.unsubscribe(&[EventChannel::Output(JobId(3))]).await }
        });

        let request_id = match read_message(&mut server_stream).await.unwrap() {
            Message::Request {
                id,
                payload: RequestPayload::Unsubscribe { channels },
                ..
            } => {
                assert_eq!(channels, vec!["output:J3"]);
                id
            }
            other => panic!("unexpected request: {other:?}"),
        };
        write_message(
            &mut server_stream,
            &Message::Response {
                id: request_id,
                payload: ResponsePayload::Ok(OkPayload::Ack {}),
            },
        )
        .await;

        match response_task.await.unwrap().unwrap() {
            ResponsePayload::Ok(OkPayload::Ack {}) => {}
            other => panic!("unexpected response: {other:?}"),
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
                ..
            } => id,
            other => panic!("unexpected request: {other:?}"),
        };

        write_message(
            &mut server_stream,
            &Message::Event {
                payload: EventPayload::ShuttingDown {
                    reason: "test".into(),
                },
            },
        )
        .await;
        write_message(
            &mut server_stream,
            &Message::Response {
                id: request_id,
                payload: ResponsePayload::Ok(OkPayload::Pong {
                    version: "0.1.0".into(),
                    instance_id: "00000000-0000-4000-8000-000000000000".into(),
                    generation_id: "generation-1".into(),
                    ready: true,
                    protocol_version: IPC_PROTOCOL_VERSION,
                    capabilities: vec![IPC_CAPABILITY_SESSION_HANDSHAKE_REQUIRED.into()],
                }),
            },
        )
        .await;

        match response_task.await.unwrap().unwrap() {
            ResponsePayload::Ok(OkPayload::Pong { version, .. }) if version == "0.1.0" => {}
            other => panic!("unexpected response: {other:?}"),
        }

        match client.next_event().await {
            Some(EventPayload::ShuttingDown { reason }) => {
                assert_eq!(reason, "test");
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn ping_roundtrip_rejects_starting_pong() {
        let (client_stream, mut server_stream) = duplex(4096);
        let mut client = CuedClient::from_stream(client_stream);
        let ping = tokio::spawn(async move { client.ping_for_version().await });
        let request_id = match read_message(&mut server_stream).await.unwrap() {
            Message::Request {
                id,
                payload: RequestPayload::Ping {},
                ..
            } => id,
            other => panic!("unexpected request: {other:?}"),
        };
        write_message(
            &mut server_stream,
            &Message::Response {
                id: request_id,
                payload: ResponsePayload::Ok(OkPayload::Pong {
                    version: "0.1.0".into(),
                    instance_id: "00000000-0000-4000-8000-000000000000".into(),
                    generation_id: "generation-1".into(),
                    ready: false,
                    protocol_version: IPC_PROTOCOL_VERSION,
                    capabilities: vec![IPC_CAPABILITY_SESSION_HANDSHAKE_REQUIRED.into()],
                }),
            },
        )
        .await;

        let error = ping
            .await
            .unwrap()
            .expect_err("Starting Pong must not be ready");
        assert!(error.to_string().contains("daemon startup is not ready"));
    }
}
