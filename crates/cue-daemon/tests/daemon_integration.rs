//! End-to-end integration tests for the `cued` daemon.
//!
//! Each test spawns a real `cued start --fg --socket <unique>` process, connects
//! over the Unix domain socket, exercises the IPC protocol, then shuts down.
//!
//! Environment isolation: every test sets `XDG_RUNTIME_DIR`, `XDG_DATA_HOME`,
//! `XDG_STATE_HOME`, and `XDG_CONFIG_HOME` to a per-test temp directory so the
//! daemon uses its own PID file, database, and socket — never colliding with a
//! real running `cued` instance.

use std::collections::BTreeMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Stdio;
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;
use std::{fs, os::unix::fs::PermissionsExt};

use tokio::io::{
    AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf, duplex,
};
use tokio::net::UnixStream;
use tokio::process::{Child, Command};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::timeout;

use cue_core::ipc::{
    self, EventPayload, ForegroundRole, JobInfo, Message, OkPayload, RequestPayload,
    ResponsePayload, ScriptItemResult, ScriptRunStatus, SessionInfo, SessionScopeState,
};
use cue_core::job::JobStatus;
use cue_core::mode::Mode;

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Per-test timeout to prevent hangs.
const TEST_TIMEOUT: Duration = Duration::from_secs(15);

/// These integration tests spawn real daemons and child processes. Run them one
/// at a time so the default Rust test harness does not create cross-test
/// process/resource interference.
static DAEMON_TEST_PERMIT: Semaphore = Semaphore::const_new(1);

async fn run_daemon_test(test: impl Future<Output = ()>) {
    let _permit = DAEMON_TEST_PERMIT
        .acquire()
        .await
        .expect("daemon integration test permit is never closed");
    timeout(TEST_TIMEOUT, test).await.expect("test timed out");
}

/// A self-contained test environment with unique dirs and socket.
struct TestEnv {
    /// Root temp directory (cleaned up on drop).
    root: PathBuf,
    /// Path to the Unix domain socket.
    socket: PathBuf,
}

impl TestEnv {
    /// Create a fresh, isolated temp directory tree for one test.
    fn new(label: &str) -> Self {
        let pid = std::process::id();
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = PathBuf::from(format!("/tmp/cue-itest-{label}-{pid}-{ts}"));
        std::fs::create_dir_all(&root).expect("create test root");
        let socket = root.join("cued.sock");
        Self { root, socket }
    }

    /// Spawn `cued start --fg --socket <path>` with isolated XDG env vars.
    fn spawn_daemon(&self) -> Child {
        self.spawn_daemon_with_env(std::iter::empty::<(&str, String)>())
    }

    fn spawn_daemon_with_env<I, K, V>(&self, extra_env: I) -> Child
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<std::ffi::OsStr>,
        V: AsRef<std::ffi::OsStr>,
    {
        let mut command = Command::new(env!("CARGO_BIN_EXE_cued"));
        command
            .args(["start", "--fg", "--socket"])
            .arg(&self.socket)
            .env("XDG_RUNTIME_DIR", &self.root)
            .env("XDG_DATA_HOME", self.root.join("data"))
            .env("XDG_STATE_HOME", self.root.join("state"))
            .env("XDG_CONFIG_HOME", self.root.join("config"))
            .env("HOME", &self.root)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        for (key, value) in extra_env {
            command.env(key, value);
        }
        command.spawn().expect("failed to spawn cued")
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// Wait (with retries) until the socket file appears and is connectable.
async fn wait_for_socket(socket: &Path, child: &mut Child) -> UnixStream {
    let mut stream = wait_for_raw_socket(socket, child).await;
    let session_id = default_test_session_id(socket);
    let cwd = default_test_session_cwd(socket);
    handshake(&mut stream, &session_id, &cwd).await;
    stream
}

async fn wait_for_socket_with_session(
    socket: &Path,
    child: &mut Child,
    session_id: &str,
    cwd: &Path,
) -> UnixStream {
    let mut stream = wait_for_raw_socket(socket, child).await;
    handshake(&mut stream, session_id, cwd).await;
    stream
}

async fn wait_for_raw_socket(socket: &Path, child: &mut Child) -> UnixStream {
    for _ in 0..80 {
        if socket.exists()
            && let Ok(stream) = UnixStream::connect(socket).await
        {
            return stream;
        }
        if let Some(status) = child.try_wait().expect("poll cued startup") {
            let stderr = read_child_stderr(child).await;
            panic!(
                "daemon exited before creating socket {} with status {status}; stderr:\n{stderr}",
                socket.display()
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let stderr = read_child_stderr(child).await;
    panic!(
        "daemon did not create socket within 8 s: {}; stderr:\n{stderr}",
        socket.display(),
    );
}

fn default_test_session_id(socket: &Path) -> String {
    format!("itest:{}", socket.display())
}

fn default_test_session_cwd(socket: &Path) -> PathBuf {
    socket
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| std::env::current_dir().expect("current dir"))
}

async fn read_child_stderr(child: &mut Child) -> String {
    let Some(mut stderr) = child.stderr.take() else {
        return String::new();
    };
    let mut buf = String::new();
    match timeout(Duration::from_millis(200), stderr.read_to_string(&mut buf)).await {
        Ok(Ok(_)) => buf,
        Ok(Err(error)) => format!("<failed to read stderr: {error}>"),
        Err(_) if buf.is_empty() => "<stderr still open>".into(),
        Err(_) => buf,
    }
}

struct SplitStream<R, W> {
    reader: R,
    writer: W,
}

impl<R, W> SplitStream<R, W> {
    fn new(reader: R, writer: W) -> Self {
        Self { reader, writer }
    }
}

impl<R, W> AsyncRead for SplitStream<R, W>
where
    R: AsyncRead + Unpin,
    W: Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.reader).poll_read(cx, buf)
    }
}

impl<R, W> AsyncWrite for SplitStream<R, W>
where
    R: Unpin,
    W: AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        Pin::new(&mut this.writer).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.writer).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.writer).poll_shutdown(cx)
    }
}

async fn connect_bridge(
    socket: &Path,
) -> (
    SplitStream<DuplexStream, DuplexStream>,
    JoinHandle<anyhow::Result<()>>,
) {
    connect_bridge_with_session(
        socket,
        &default_test_session_id(socket),
        &default_test_session_cwd(socket),
    )
    .await
}

async fn connect_bridge_with_session(
    socket: &Path,
    session_id: &str,
    cwd: &Path,
) -> (
    SplitStream<DuplexStream, DuplexStream>,
    JoinHandle<anyhow::Result<()>>,
) {
    let (client_writer, relay_input) = duplex(16 * 1024);
    let (relay_output, client_reader) = duplex(16 * 1024);
    let socket_stream = UnixStream::connect(socket)
        .await
        .expect("connect bridge socket");
    let relay = tokio::spawn(cue_daemon::relay_gateway_stdio(
        relay_input,
        relay_output,
        socket_stream,
    ));
    let mut stream = SplitStream::new(client_reader, client_writer);
    handshake(&mut stream, session_id, cwd).await;
    (stream, relay)
}

fn write_executable_script(path: &Path, body: &str) {
    fs::write(path, body).expect("write test script");
    let mut permissions = fs::metadata(path).expect("stat test script").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("chmod test script");
}

fn write_daemon_config(env: &TestEnv, text: &str) {
    let config_dir = env.root.join("config/cue-shell");
    fs::create_dir_all(&config_dir).expect("create config dir");
    fs::write(config_dir.join("daemon.toml"), text).expect("write daemon.toml");
}

fn unix_mode(path: &Path) -> u32 {
    fs::metadata(path)
        .unwrap_or_else(|error| panic!("stat {}: {error}", path.display()))
        .permissions()
        .mode()
        & 0o777
}

fn job_id_from_created(resp: ResponsePayload) -> String {
    match resp {
        ResponsePayload::Ok(OkPayload::JobCreated { job_id, .. }) => job_id,
        other => panic!("expected JobCreated, got {other:?}"),
    }
}

fn job_created_from_response(resp: ResponsePayload) -> (String, Option<String>) {
    match resp {
        ResponsePayload::Ok(OkPayload::JobCreated {
            job_id,
            start_scope,
            ..
        }) => (job_id, start_scope),
        other => panic!("expected JobCreated, got {other:?}"),
    }
}

/// Write a length-prefixed JSON message to the stream.
async fn send<S>(stream: &mut S, msg: &Message)
where
    S: AsyncWrite + Unpin,
{
    let encoded = ipc::encode_message(msg).expect("encode");
    stream.write_all(&encoded).await.expect("write");
    stream.flush().await.expect("flush");
}

/// Read one length-prefixed JSON message from the stream.
async fn recv<S>(stream: &mut S) -> Message
where
    S: AsyncRead + Unpin,
{
    let len = stream.read_u32().await.expect("read length");
    let mut buf = vec![0u8; len as usize];
    stream.read_exact(&mut buf).await.expect("read body");
    serde_json::from_slice(&buf).expect("deserialize")
}

/// Best-effort roundtrip for readiness polling across a socket handoff. A
/// predecessor may close between connect and read; that is a retry, not a test
/// failure.
async fn try_roundtrip<S>(
    stream: &mut S,
    id: u32,
    payload: RequestPayload,
) -> Option<ResponsePayload>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let encoded = ipc::encode_message(&request(id, payload)).ok()?;
    stream.write_all(&encoded).await.ok()?;
    stream.flush().await.ok()?;
    let len = stream.read_u32().await.ok()?;
    let mut buf = vec![0u8; len as usize];
    stream.read_exact(&mut buf).await.ok()?;
    match serde_json::from_slice(&buf).ok()? {
        Message::Response {
            id: response_id,
            payload,
        } if response_id == id => Some(payload),
        _ => None,
    }
}

/// Build a `Request` envelope.
fn request(id: u32, payload: RequestPayload) -> Message {
    Message::Request {
        id,
        operation_id: None,
        payload,
    }
}

async fn handshake<S>(stream: &mut S, session_id: &str, cwd: &Path)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let resp = roundtrip(
        stream,
        0,
        RequestPayload::Handshake {
            session_id: session_id.to_string(),
            cwd: cwd.display().to_string(),
            env: BTreeMap::new(),
            refresh: false,
        },
    )
    .await;
    assert!(matches!(resp, ResponsePayload::Ok(OkPayload::Ack {})));
}

async fn handshake_with_env<S>(
    stream: &mut S,
    session_id: &str,
    cwd: &Path,
    env: BTreeMap<String, String>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let resp = roundtrip(
        stream,
        0,
        RequestPayload::Handshake {
            session_id: session_id.to_string(),
            cwd: cwd.display().to_string(),
            env,
            refresh: false,
        },
    )
    .await;
    assert!(matches!(resp, ResponsePayload::Ok(OkPayload::Ack {})));
}

async fn create_named_session<S>(stream: &mut S, request_id: u32, name: &str) -> SessionInfo
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match roundtrip(
        stream,
        request_id,
        RequestPayload::CreateSession {
            name: name.to_string(),
        },
    )
    .await
    {
        ResponsePayload::Ok(OkPayload::SessionInfo(info)) => *info,
        other => panic!("expected named SessionInfo after create, got {other:?}"),
    }
}

async fn attach_named_session<S>(
    stream: &mut S,
    request_id: u32,
    selector: &str,
    refresh: bool,
) -> ResponsePayload
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    roundtrip(
        stream,
        request_id,
        RequestPayload::AttachSession {
            selector: selector.to_string(),
            refresh,
        },
    )
    .await
}

fn assert_missing_session_error(response: ResponsePayload) {
    match response {
        ResponsePayload::Err { code, message } => {
            assert_eq!(code, "INVALID_REQUEST");
            assert_eq!(message, "client session handshake required");
        }
        other => panic!("expected missing-session error before handshake, got {other:?}"),
    }
}

/// Send a request and return the matching response payload.
async fn roundtrip<S>(stream: &mut S, id: u32, payload: RequestPayload) -> ResponsePayload
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    send(stream, &request(id, payload)).await;
    // Drain until we get a Response with the matching id (skip Events).
    loop {
        let msg = recv(stream).await;
        if let Message::Response {
            id: rid, payload, ..
        } = msg
            && rid == id
        {
            return payload;
        }
    }
}

async fn roundtrip_with_operation<S>(
    stream: &mut S,
    id: u32,
    operation_id: &str,
    payload: RequestPayload,
) -> ResponsePayload
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    send(
        stream,
        &Message::Request {
            id,
            operation_id: Some(operation_id.into()),
            payload,
        },
    )
    .await;
    loop {
        let msg = recv(stream).await;
        if let Message::Response {
            id: response_id,
            payload,
        } = msg
            && response_id == id
        {
            return payload;
        }
    }
}

/// Send a request and return the matching response plus any messages observed before it.
async fn roundtrip_with_messages<S>(
    stream: &mut S,
    id: u32,
    payload: RequestPayload,
) -> (ResponsePayload, Vec<Message>)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    send(stream, &request(id, payload)).await;
    let mut observed = Vec::new();
    loop {
        let msg = recv(stream).await;
        match msg {
            Message::Response {
                id: rid, payload, ..
            } if rid == id => return (payload, observed),
            other => observed.push(other),
        }
    }
}

async fn job_info_from_jobs<S>(stream: &mut S, request_id: u32, job_id: &str) -> Option<JobInfo>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let resp = roundtrip(
        stream,
        request_id,
        RequestPayload::Eval {
            input: ":jobs".into(),
            mode: Mode::Job,
        },
    )
    .await;
    match resp {
        ResponsePayload::Ok(OkPayload::JobList(list)) => {
            list.into_iter().find(|job| job.id == job_id)
        }
        other => panic!("expected JobList, got {other:?}"),
    }
}

async fn wait_for_job_status<S>(
    stream: &mut S,
    mut request_id: u32,
    job_id: &str,
    predicate: impl Fn(&JobInfo) -> bool,
) -> JobInfo
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(job) = job_info_from_jobs(stream, request_id, job_id).await
            && predicate(&job)
        {
            return job;
        }
        request_id += 1;

        assert!(
            tokio::time::Instant::now() < deadline,
            "job {job_id} did not reach expected state in time"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Poll `:jobs` until `job_id` reaches a terminal state.
async fn wait_for_job_terminal<S>(stream: &mut S, request_id: u32, job_id: &str) -> JobStatus
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    wait_for_job_status(stream, request_id, job_id, |job| job.status.is_terminal())
        .await
        .status
}

async fn run_pwd_and_read<S>(stream: &mut S, next_request_id: &mut u32) -> PathBuf
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let resp = roundtrip(
        stream,
        *next_request_id,
        RequestPayload::Eval {
            input: "/bin/pwd".into(),
            mode: Mode::Job,
        },
    )
    .await;
    *next_request_id += 1;
    let job_id = job_id_from_created(resp);
    let status = wait_for_job_terminal(stream, *next_request_id, &job_id).await;
    *next_request_id += 1;
    assert_eq!(status, JobStatus::Done);
    let out = roundtrip(
        stream,
        *next_request_id,
        RequestPayload::Eval {
            input: format!(":out {job_id}"),
            mode: Mode::Job,
        },
    )
    .await;
    *next_request_id += 1;
    match out {
        ResponsePayload::Ok(OkPayload::Output { data, .. }) => {
            std::fs::canonicalize(data.trim()).expect("canonicalize pwd output")
        }
        other => panic!("expected Output, got {other:?}"),
    }
}

async fn scope_cwd_from_list<S>(stream: &mut S, request_id: u32, scope_hash: &str) -> String
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let resp = roundtrip(
        stream,
        request_id,
        RequestPayload::Eval {
            input: ":scopes".into(),
            mode: Mode::Job,
        },
    )
    .await;
    match resp {
        ResponsePayload::Ok(OkPayload::ScopeList(scopes)) => {
            scopes
                .into_iter()
                .find(|scope| scope.hash == scope_hash)
                .unwrap_or_else(|| panic!("scope {scope_hash} missing from :scopes"))
                .cwd
        }
        other => panic!("expected ScopeList, got {other:?}"),
    }
}

async fn wait_for_done_job_matching<S>(
    stream: &mut S,
    mut request_id: u32,
    exclude: &std::collections::HashSet<String>,
    predicate: impl Fn(&JobInfo) -> bool,
) -> JobInfo
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let resp = roundtrip(
            stream,
            request_id,
            RequestPayload::Eval {
                input: ":jobs".into(),
                mode: Mode::Job,
            },
        )
        .await;
        match resp {
            ResponsePayload::Ok(OkPayload::JobList(list)) => {
                if let Some(job) = list.into_iter().find(|job| {
                    job.status == JobStatus::Done && !exclude.contains(&job.id) && predicate(job)
                }) {
                    return job;
                }
            }
            other => panic!("expected JobList, got {other:?}"),
        }
        request_id += 1;

        assert!(
            tokio::time::Instant::now() < deadline,
            "matching done job did not appear in time"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Subscribe to a set of channels.
async fn subscribe<S, I, T>(stream: &mut S, id: u32, channels: I)
where
    S: AsyncRead + AsyncWrite + Unpin,
    I: IntoIterator<Item = T>,
    T: AsRef<str>,
{
    let resp = roundtrip(
        stream,
        id,
        RequestPayload::Subscribe {
            channels: channels
                .into_iter()
                .map(|channel| channel.as_ref().to_string())
                .collect(),
        },
    )
    .await;
    assert!(
        matches!(resp, ResponsePayload::Ok(OkPayload::Ack {})),
        "subscribe failed: {resp:?}"
    );
}

/// Collect messages until `predicate` returns `true` (with a timeout).
async fn collect_until<S, F>(stream: &mut S, dur: Duration, mut predicate: F) -> Vec<Message>
where
    S: AsyncRead + Unpin,
    F: FnMut(&Message) -> bool,
{
    let mut collected = Vec::new();
    let deadline = tokio::time::Instant::now() + dur;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match timeout(remaining, recv(stream)).await {
            Ok(msg) => {
                let done = predicate(&msg);
                collected.push(msg);
                if done {
                    break;
                }
            }
            Err(_) => break, // timeout
        }
    }
    collected
}

fn append_foreground_output(message: &Message, job_id: &str, output: &mut Vec<u8>) {
    if let Message::Event {
        payload: EventPayload::FgOutput { id, data, .. },
    } = message
        && id == job_id
    {
        output.extend_from_slice(data);
    }
}

async fn wait_for_foreground_output<S>(
    stream: &mut S,
    job_id: &str,
    expected: &[u8],
    mut observed: Vec<Message>,
) -> Vec<Message>
where
    S: AsyncRead + Unpin,
{
    let mut output = Vec::new();
    for message in &observed {
        append_foreground_output(message, job_id, &mut output);
    }
    if !output
        .windows(expected.len())
        .any(|window| window == expected)
    {
        observed.extend(
            collect_until(stream, Duration::from_secs(3), |message| {
                append_foreground_output(message, job_id, &mut output);
                output
                    .windows(expected.len())
                    .any(|window| window == expected)
            })
            .await,
        );
    }
    assert!(
        output
            .windows(expected.len())
            .any(|window| window == expected),
        "foreground output for {job_id} did not contain {:?}; messages: {observed:?}",
        String::from_utf8_lossy(expected),
    );
    observed
}

/// Send `:shutdown` and wait for the child to exit.
async fn shutdown_daemon<S>(stream: &mut S, child: &mut Child)
where
    S: AsyncWrite + Unpin,
{
    // Best-effort IPC shutdown (stops the gateway dispatch loop).
    let _ = send(stream, &request(9999, RequestPayload::Shutdown {})).await;
    // The daemon's main loop waits for a Unix signal to exit. Send SIGTERM.
    if let Some(pid) = child.id() {
        unsafe {
            libc::kill(pid as i32, libc::SIGTERM);
        }
    }
    let _ = timeout(Duration::from_secs(5), child.wait()).await;
    // If still alive, force kill.
    let _ = child.kill().await;
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_daemon_lifecycle() {
    run_daemon_test(async {
        let env = TestEnv::new("lifecycle");
        let mut child = env.spawn_daemon();

        // Connect and ping.
        let mut stream = wait_for_socket(&env.socket, &mut child).await;
        let resp = roundtrip(&mut stream, 1, RequestPayload::Ping {}).await;
        assert!(
            matches!(resp, ResponsePayload::Ok(OkPayload::Pong { .. })),
            "expected Pong, got {resp:?}"
        );

        // Shutdown via IPC.
        let resp = roundtrip(&mut stream, 2, RequestPayload::Shutdown {}).await;
        assert!(
            matches!(resp, ResponsePayload::Ok(OkPayload::Ack {})),
            "expected Ack for shutdown, got {resp:?}"
        );

        // The IPC Shutdown stops the gateway dispatch loop but the daemon's
        // main loop waits for a Unix signal. Send SIGTERM to the child.
        let pid = child.id().expect("child pid");
        unsafe {
            libc::kill(pid as i32, libc::SIGTERM);
        }

        // Daemon should exit.
        let status = timeout(Duration::from_secs(5), child.wait())
            .await
            .expect("daemon did not exit in time")
            .expect("wait failed");
        // Might exit 0 or via signal — both are acceptable.
        let _ = status;
    })
    .await;
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_restart_recovers_live_daemon_after_socket_is_unlinked() {
    run_daemon_test(async {
        let env = TestEnv::new("restart-unlinked-socket");
        let mut old_daemon = env.spawn_daemon();
        let stream = wait_for_socket(&env.socket, &mut old_daemon).await;
        let old_pid = old_daemon.id().expect("old daemon pid");
        drop(stream);

        fs::remove_file(&env.socket).expect("unlink live daemon socket");
        assert!(
            old_daemon.try_wait().expect("poll old daemon").is_none(),
            "old daemon should remain alive after its socket is unlinked"
        );

        let output = Command::new(env!("CARGO_BIN_EXE_cued"))
            .args(["restart", "--socket"])
            .arg(&env.socket)
            .env("XDG_RUNTIME_DIR", &env.root)
            .env("XDG_DATA_HOME", env.root.join("data"))
            .env("XDG_STATE_HOME", env.root.join("state"))
            .env("XDG_CONFIG_HOME", env.root.join("config"))
            .env("HOME", &env.root)
            .output()
            .await
            .expect("run cued restart");
        assert!(
            output.status.success(),
            "restart failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        timeout(Duration::from_secs(5), old_daemon.wait())
            .await
            .expect("old daemon did not exit after verified restart")
            .expect("wait for old daemon");

        let pid_path = env.root.join("cued.sock.cued.pid");
        let (new_pid, mut stream) = timeout(Duration::from_secs(5), async {
            loop {
                let pid = fs::read_to_string(&pid_path)
                    .ok()
                    .and_then(|value| value.trim().parse::<u32>().ok());
                if let Some(pid) = pid
                    && pid != old_pid
                    && let Ok(stream) = UnixStream::connect(&env.socket).await
                {
                    return (pid, stream);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("restarted daemon did not become reachable");

        handshake(
            &mut stream,
            &default_test_session_id(&env.socket),
            &default_test_session_cwd(&env.socket),
        )
        .await;
        let response = roundtrip(&mut stream, 1, RequestPayload::Ping {}).await;
        assert!(matches!(
            response,
            ResponsePayload::Ok(OkPayload::Pong { .. })
        ));

        let _ = roundtrip(&mut stream, 2, RequestPayload::Shutdown {}).await;
        unsafe {
            libc::kill(new_pid as i32, libc::SIGTERM);
        }
        for _ in 0..50 {
            if unsafe { libc::kill(new_pid as i32, 0) } != 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("restarted daemon pid {new_pid} did not exit");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_restart_drains_active_job_rejects_late_admission_and_fences_successor() {
    run_daemon_test(async {
        let env = TestEnv::new("graceful-restart");
        let marker = env.root.join("restart-marker");
        let script = env.root.join("finish-once.sh");
        write_executable_script(
            &script,
            &format!("#!/bin/sh\nsleep 1\nprintf x >> '{}'\n", marker.display()),
        );

        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;
        let predecessor = match roundtrip(&mut stream, 1, RequestPayload::Ping {}).await {
            ResponsePayload::Ok(OkPayload::Pong {
                instance_id,
                protocol_version,
                ..
            }) => (instance_id, protocol_version),
            other => panic!("expected predecessor Pong, got {other:?}"),
        };

        let job_id = job_id_from_created(
            roundtrip(
                &mut stream,
                2,
                RequestPayload::Eval {
                    input: script.display().to_string(),
                    mode: Mode::Job,
                },
            )
            .await,
        );
        wait_for_job_status(&mut stream, 3, &job_id, |job| {
            job.status == JobStatus::Running
        })
        .await;

        let (restart_id, target_generation) =
            match roundtrip(&mut stream, 20, RequestPayload::Restart {}).await {
                ResponsePayload::Ok(OkPayload::RestartAccepted {
                    restart_id,
                    daemon_instance_id,
                    target_generation,
                }) => {
                    assert_eq!(daemon_instance_id, predecessor.0);
                    (restart_id, target_generation)
                }
                other => panic!("expected RestartAccepted, got {other:?}"),
            };
        match roundtrip(&mut stream, 21, RequestPayload::Restart {}).await {
            ResponsePayload::Ok(OkPayload::RestartAccepted {
                restart_id: repeated,
                target_generation: repeated_generation,
                ..
            }) => {
                assert_eq!(repeated, restart_id);
                assert_eq!(repeated_generation, target_generation);
            }
            other => panic!("expected idempotent RestartAccepted, got {other:?}"),
        }

        match roundtrip(
            &mut stream,
            22,
            RequestPayload::Eval {
                input: "echo must-not-start".into(),
                mode: Mode::Job,
            },
        )
        .await
        {
            ResponsePayload::Err { code, .. } => assert_eq!(code, "DAEMON_DRAINING"),
            other => panic!("late execution must be rejected, got {other:?}"),
        }

        timeout(Duration::from_secs(8), child.wait())
            .await
            .expect("predecessor did not exit after active job completed")
            .expect("wait for predecessor");
        assert_eq!(fs::read_to_string(&marker).unwrap(), "x");

        let mut successor = timeout(Duration::from_secs(10), async {
            loop {
                if let Ok(mut candidate) = UnixStream::connect(&env.socket).await {
                    let ping = timeout(
                        Duration::from_secs(1),
                        try_roundtrip(&mut candidate, 30, RequestPayload::Ping {}),
                    )
                    .await;
                    if let Ok(Some(ResponsePayload::Ok(OkPayload::Pong {
                        instance_id,
                        generation_id,
                        protocol_version,
                        ready,
                        ..
                    }))) = ping
                        && instance_id != predecessor.0
                        && generation_id == target_generation
                        && protocol_version == predecessor.1
                        && ready
                    {
                        break candidate;
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("successor did not pass fenced readiness");
        handshake(&mut successor, "restart-successor", &env.root).await;
        let restored = job_info_from_jobs(&mut successor, 31, &job_id)
            .await
            .expect("completed job should remain in successor history");
        assert_eq!(restored.status, JobStatus::Done);
        assert_eq!(fs::read_to_string(&marker).unwrap(), "x");

        let response = roundtrip(&mut successor, 32, RequestPayload::Shutdown {}).await;
        assert!(matches!(response, ResponsePayload::Ok(OkPayload::Ack {})));
        let mut stopped = false;
        for _ in 0..50 {
            if UnixStream::connect(&env.socket).await.is_err() {
                stopped = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(stopped, "successor did not release its socket after stop");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_concurrent_foreground_starts_have_one_socket_owner() {
    run_daemon_test(async {
        let env = TestEnv::new("single-instance-race");
        let mut first = env.spawn_daemon();
        let mut second = env.spawn_daemon();
        let mut stream = None;
        let mut first_status = None;
        let mut second_status = None;

        for _ in 0..80 {
            if stream.is_none()
                && env.socket.exists()
                && let Ok(connected) = UnixStream::connect(&env.socket).await
            {
                stream = Some(connected);
            }
            if first_status.is_none() {
                first_status = first.try_wait().expect("poll first daemon");
            }
            if second_status.is_none() {
                second_status = second.try_wait().expect("poll second daemon");
            }
            if stream.is_some() && (first_status.is_some() ^ second_status.is_some()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let mut stream = stream.expect("one daemon should own a reachable socket");
        assert!(
            first_status.is_some() ^ second_status.is_some(),
            "exactly one concurrent foreground start must exit"
        );
        handshake(
            &mut stream,
            &default_test_session_id(&env.socket),
            &env.root,
        )
        .await;
        let response = roundtrip(&mut stream, 1, RequestPayload::Ping {}).await;
        assert!(matches!(
            response,
            ResponsePayload::Ok(OkPayload::Pong { .. })
        ));

        if let Some(status) = first_status {
            assert!(
                !status.success(),
                "losing daemon must report startup failure"
            );
            assert!(second_status.is_none(), "winner must still be running");
            shutdown_daemon(&mut stream, &mut second).await;
        } else {
            let status = second_status.expect("second daemon is the loser");
            assert!(
                !status.success(),
                "losing daemon must report startup failure"
            );
            shutdown_daemon(&mut stream, &mut first).await;
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_unhandshaken_client_can_recover_by_handshaking_on_same_connection() {
    run_daemon_test(async {
        let env = TestEnv::new("handshake-recover");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_raw_socket(&env.socket, &mut child).await;

        let response = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input: "?".into(),
                mode: Mode::Job,
            },
        )
        .await;
        assert_missing_session_error(response);

        let response = roundtrip(
            &mut stream,
            2,
            RequestPayload::RunScript {
                path: "recover.cue".into(),
                input: "echo script".into(),
            },
        )
        .await;
        assert_missing_session_error(response);

        handshake(&mut stream, "recover-session", &env.root).await;
        let response = roundtrip(&mut stream, 3, RequestPayload::Ping {}).await;
        assert!(
            matches!(response, ResponsePayload::Ok(OkPayload::Pong { .. })),
            "expected Pong after late handshake, got {response:?}"
        );

        let _ = roundtrip(&mut stream, 4, RequestPayload::Shutdown {}).await;
        let _ = child.wait().await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_foreground_sigint_exits_promptly() {
    run_daemon_test(async {
        let env = TestEnv::new("sigint-exit");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        let pid = child.id().expect("child pid");
        unsafe {
            libc::kill(pid as i32, libc::SIGINT);
        }

        let status = timeout(Duration::from_secs(5), child.wait())
            .await
            .expect("daemon did not exit after SIGINT")
            .expect("wait failed");
        let _ = status;

        let _ = stream.shutdown().await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_simple_job_execution() {
    run_daemon_test(async {
        let env = TestEnv::new("simplejob");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        // Subscribe to job events.
        subscribe(&mut stream, 1, vec!["jobs"]).await;

        // Send `echo hello` (bare input → :run in Job mode).
        let resp = roundtrip(
            &mut stream,
            2,
            RequestPayload::Eval {
                input: "echo hello".into(),
                mode: Mode::Job,
            },
        )
        .await;

        // Should get JobCreated or ChainCreated.
        match &resp {
            ResponsePayload::Ok(OkPayload::JobCreated {
                job_id,
                start_scope,
                ..
            }) => {
                assert!(job_id.starts_with('J'), "unexpected job id: {job_id}");
                assert!(
                    start_scope.is_some(),
                    "missing start_scope in JobCreated response"
                );
            }
            ResponsePayload::Ok(OkPayload::ChainCreated { job_ids, .. }) => {
                assert!(!job_ids.is_empty());
            }
            other => panic!("expected job/chain created, got {other:?}"),
        }

        // Wait for the job to reach a terminal state via events.
        let msgs = collect_until(&mut stream, Duration::from_secs(10), |msg| {
            matches!(
                msg,
                Message::Event {
                    payload: EventPayload::JobStateChanged {
                        new_state: JobStatus::Done | JobStatus::Failed,
                        ..
                    },
                }
            )
        })
        .await;

        // Verify we saw at least one state transition to Done.
        let reached_done = msgs.iter().any(|m| {
            matches!(
                m,
                Message::Event {
                    payload: EventPayload::JobStateChanged {
                        new_state: JobStatus::Done,
                        ..
                    },
                }
            )
        });
        assert!(reached_done, "job never reached Done; events: {msgs:?}");

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_run_script_item_events_exclude_other_clients_jobs() {
    run_daemon_test(async {
        let env = TestEnv::new("script-item-authority");
        let mut child = env.spawn_daemon();
        let mut script_client =
            wait_for_socket_with_session(&env.socket, &mut child, "script-client", &env.root).await;
        let mut outsider_client =
            wait_for_socket_with_session(&env.socket, &mut child, "outsider-client", &env.root)
                .await;
        subscribe(&mut script_client, 1, vec!["jobs"]).await;

        let response = roundtrip(
            &mut script_client,
            2,
            RequestPayload::RunScript {
                path: "two-items.cue".into(),
                input: "sleep 1\necho script-second".into(),
            },
        )
        .await;
        let (script_id, first_job_id) = match response {
            ResponsePayload::Ok(OkPayload::ScriptCreated {
                script_id, items, ..
            }) => {
                assert_eq!(items.len(), 1);
                let first_job_id = match &items[0].result {
                    ScriptItemResult::Job { job_id, .. } => job_id.clone(),
                    other => panic!("expected first script item job, got {other:?}"),
                };
                (script_id, first_job_id)
            }
            other => panic!("expected ScriptCreated, got {other:?}"),
        };

        let outsider_job_id = job_id_from_created(
            roundtrip(
                &mut outsider_client,
                1,
                RequestPayload::Eval {
                    input: "echo outsider".into(),
                    mode: Mode::Job,
                },
            )
            .await,
        );

        let messages = collect_until(&mut script_client, Duration::from_secs(10), |message| {
            matches!(
                message,
                Message::Event {
                    payload: EventPayload::ScriptFinished { script_id: finished, .. },
                } if finished == &script_id
            )
        })
        .await;

        assert!(
            messages.iter().any(|message| matches!(
                message,
                Message::Event {
                    payload: EventPayload::JobCreated { job_id, .. },
                } if job_id == &outsider_job_id
            )),
            "script observer should still see the outsider's global JobCreated event"
        );
        let second_item_job_id = messages
            .iter()
            .find_map(|message| match message {
                Message::Event {
                    payload:
                        EventPayload::ScriptItemCreated {
                            script_id: event_script_id,
                            item,
                        },
                } if event_script_id == &script_id && item.index == 1 => match &item.result {
                    ScriptItemResult::Job { job_id, .. } => Some(job_id.clone()),
                    _ => None,
                },
                _ => None,
            })
            .expect("missing authoritative event for the second script item");

        assert_ne!(first_job_id, outsider_job_id);
        assert_ne!(second_item_job_id, outsider_job_id);
        assert!(messages.iter().all(|message| !matches!(
            message,
            Message::Event {
                payload: EventPayload::ScriptItemCreated { item, .. },
            } if matches!(
                &item.result,
                ScriptItemResult::Job { job_id, .. } if job_id == &outsider_job_id
            )
        )));

        shutdown_daemon(&mut script_client, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_daemon_creates_and_migrates_private_runtime_files() {
    run_daemon_test(async {
        let env = TestEnv::new("private-files");
        let runtime_dir = env.root.join("cue-shell");
        let sandbox_dir = runtime_dir.join("sandbox");
        let data_dir = env.root.join("data/cue-shell");
        let output_dir = data_dir.join("output");
        let state_dir = env.root.join("state/cue-shell");
        let config_dir = env.root.join("config/cue-shell");
        let directories = [
            runtime_dir.clone(),
            sandbox_dir,
            data_dir.clone(),
            output_dir.clone(),
            state_dir.clone(),
            config_dir.clone(),
        ];
        for dir in &directories {
            fs::create_dir_all(dir).expect("create wide app directory");
            fs::set_permissions(dir, fs::Permissions::from_mode(0o755))
                .expect("set wide app directory mode");
        }
        let migrated_files = [
            data_dir.join("input-history.json"),
            state_dir.join("cued.log"),
            config_dir.join("daemon.toml"),
            output_dir.join("J999.log"),
            output_dir.join("J999.stderr"),
        ];
        for file in &migrated_files {
            let contents = if file.ends_with("input-history.json") {
                "[]"
            } else {
                ""
            };
            fs::write(file, contents).expect("create wide app file");
            fs::set_permissions(file, fs::Permissions::from_mode(0o644))
                .expect("set wide app file mode");
        }

        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        for dir in &directories {
            assert_eq!(unix_mode(dir), 0o700, "{}", dir.display());
        }
        for file in [
            env.socket.clone(),
            env.root.join("cued.sock.cued.pid"),
            env.root.join("cued.sock.cued.lock"),
            data_dir.join("cued.db"),
        ]
        .iter()
        .chain(migrated_files.iter())
        {
            assert_eq!(unix_mode(file), 0o600, "{}", file.display());
        }
        assert!(
            !runtime_dir.join("cued.pid").exists(),
            "custom socket must not write the default runtime PID marker"
        );
        for sidecar in [data_dir.join("cued.db-wal"), data_dir.join("cued.db-shm")] {
            if sidecar.exists() {
                assert_eq!(unix_mode(&sidecar), 0o600, "{}", sidecar.display());
            }
        }

        let response = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input: ":run(pty=false) echo private-output".into(),
                mode: Mode::Job,
            },
        )
        .await;
        let job_id = job_id_from_created(response);
        assert_eq!(
            wait_for_job_terminal(&mut stream, 2, &job_id).await,
            JobStatus::Done
        );
        for suffix in [".log", ".stderr"] {
            let path = output_dir.join(format!("{job_id}{suffix}"));
            assert_eq!(unix_mode(&path), 0o600, "{}", path.display());
        }

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_resource_cli_provider_admission_env_and_inspection() {
    run_daemon_test(async {
        let env = TestEnv::new("resource-cli-provider");
        let provider_script = env.root.join("license-provider.sh");
        let state_file = env.root.join("license-held");
        write_executable_script(
            &provider_script,
            r#"#!/bin/sh
set -eu
command="$1"
state="$2"
case "$command" in
  probe)
    if [ -f "$state" ]; then free=0; else free=1; fi
    printf '{"units":[{"id":"pool","attrs":{"free":{"kind":"count","value":%s}}}]}\n' "$free"
    ;;
  reserve)
    cat >/dev/null
    if [ -f "$state" ]; then
      printf '%s\n' '{"ok":false,"reason":"license busy"}'
    else
      printf '%s\n' held > "$state"
      printf '%s\n' '{"ok":true,"grant_id":"lease-1","env":{"LICENSE_TOKEN":"leased"},"info":{"license":{"kind":"count","value":1}}}'
    fi
    ;;
  release)
    cat >/dev/null
    rm -f "$state"
    printf '%s\n' '{}'
    ;;
  *)
    echo "unknown command: $command" >&2
    exit 64
    ;;
esac
"#,
        );
        write_daemon_config(
            &env,
            &format!(
                r#"
[resources.cli.license]
keys = ["license"]
probe = ["{}", "probe", "{}"]
reserve = ["{}", "reserve", "{}"]
release = ["{}", "release", "{}"]
timeout_ms = 5000
"#,
                provider_script.display(),
                state_file.display(),
                provider_script.display(),
                state_file.display(),
                provider_script.display(),
                state_file.display(),
            ),
        );

        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        let providers = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input: ":providers".into(),
                mode: Mode::Job,
            },
        )
        .await;
        assert!(matches!(
            providers,
            ResponsePayload::Ok(OkPayload::EvalText { ref text }) if text.contains("- license: license")
        ));

        let first = roundtrip(
            &mut stream,
            2,
            RequestPayload::Eval {
                input: r#":run(pty=false, need.license=1) /bin/sh -c 'printf "%s\n" "$LICENSE_TOKEN"; sleep 2'"#.into(),
                mode: Mode::Job,
            },
        )
        .await;
        let first_job = job_id_from_created(first);

        let second = roundtrip(
            &mut stream,
            3,
            RequestPayload::Eval {
                input: r#":run(pty=false, need.license=1) /bin/sh -c 'printf "%s\n" "$LICENSE_TOKEN"'"#.into(),
                mode: Mode::Job,
            },
        )
        .await;
        let second_job = job_id_from_created(second);

        let pending = wait_for_job_status(&mut stream, 4, &second_job, |job| {
            job.status == JobStatus::Pending
        })
        .await;
        assert_eq!(
            pending.pending_reason.as_deref(),
            Some("license: license busy")
        );

        let resources = roundtrip(
            &mut stream,
            30,
            RequestPayload::Eval {
                input: ":resources".into(),
                mode: Mode::Job,
            },
        )
        .await;
        assert!(matches!(
            resources,
            ResponsePayload::Ok(OkPayload::EvalText { ref text })
                if text.contains("provider license") && text.contains("unit pool: free=0")
        ));

        assert_eq!(
            wait_for_job_terminal(&mut stream, 40, &first_job).await,
            JobStatus::Done
        );
        assert_eq!(
            wait_for_job_terminal(&mut stream, 60, &second_job).await,
            JobStatus::Done
        );

        for (request_id, job_id) in [(80, &first_job), (81, &second_job)] {
            let out = roundtrip(
                &mut stream,
                request_id,
                RequestPayload::Eval {
                    input: format!(":out {job_id}"),
                    mode: Mode::Job,
                },
            )
            .await;
            assert!(matches!(
                out,
                ResponsePayload::Ok(OkPayload::Output { ref data, .. }) if data.contains("leased")
            ));
        }

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_run_cwd_is_start_scope_and_pty_is_launch_option() {
    run_daemon_test(async {
        let env = TestEnv::new("run-cwd-launch");
        let repo = env.root.join("repo");
        std::fs::create_dir_all(&repo).expect("create repo dir");

        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        let created = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input: format!(":run(cwd={}, pty=false) /bin/pwd", repo.display()),
                mode: Mode::Job,
            },
        )
        .await;
        let (job_id, start_scope) = job_created_from_response(created);
        let start_scope = start_scope.expect("run should return a start scope");
        assert_eq!(
            wait_for_job_terminal(&mut stream, 2, &job_id).await,
            JobStatus::Done
        );

        let out = roundtrip(
            &mut stream,
            20,
            RequestPayload::Eval {
                input: format!(":out {job_id}"),
                mode: Mode::Job,
            },
        )
        .await;
        assert!(matches!(
            out,
            ResponsePayload::Ok(OkPayload::Output { ref data, .. }) if data.trim() == repo.display().to_string()
        ));
        assert_eq!(
            scope_cwd_from_list(&mut stream, 21, &start_scope).await,
            repo.display().to_string()
        );

        let env_resp = roundtrip(
            &mut stream,
            22,
            RequestPayload::Eval {
                input: ":env".into(),
                mode: Mode::Job,
            },
        )
        .await;
        assert!(matches!(
            env_resp,
            ResponsePayload::Ok(OkPayload::EvalText { ref text }) if !text.contains(&format!("cwd={}", repo.display()))
        ));

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_run_need_params_are_admitted_from_start_scope() {
    run_daemon_test(async {
        let env = TestEnv::new("run-need-scope");
        let provider_script = env.root.join("gpu-provider.sh");
        let request_log = env.root.join("reserve.jsonl");
        let bin_dir = env.root.join("bin");
        std::fs::create_dir_all(&bin_dir).expect("create bin dir");
        write_executable_script(
            &bin_dir.join("uv"),
            r#"#!/bin/sh
if [ "$1" != "run" ] || [ "$2" != "python" ]; then
  exit 64
fi
printf '%s\n' "$GPU_TOKEN"
"#,
        );
        std::fs::write(env.root.join("train.py"), "# fake training script\n")
            .expect("write train.py");
        write_executable_script(
            &provider_script,
            r#"#!/bin/sh
command="$1"
log="$2"
case "$command" in
  probe)
    printf '{"units":[{"id":"gpu0","attrs":{}}]}'
    ;;
  reserve)
    cat >> "$log"
    printf '\n' >> "$log"
    printf '{"ok":true,"grant_id":"gpu-grant","env":{"GPU_TOKEN":"reserved-from-scope"},"info":{}}'
    ;;
  release)
    cat >/dev/null
    ;;
  *)
    exit 64
    ;;
esac
"#,
        );
        write_daemon_config(
            &env,
            &format!(
                r#"
[resources.cli.gpu]
keys = ["gpu", "gpu_mem"]
probe = ["{}", "probe", "{}"]
reserve = ["{}", "reserve", "{}"]
release = ["{}", "release", "{}"]
timeout_ms = 5000
"#,
                provider_script.display(),
                request_log.display(),
                provider_script.display(),
                request_log.display(),
                provider_script.display(),
                request_log.display(),
            ),
        );

        let test_path = format!(
            "{}:{}",
            bin_dir.display(),
            std::env::var("PATH").unwrap_or_default()
        );
        let mut child = env.spawn_daemon_with_env([("PATH", test_path)]);
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        let created = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input: ":run(need.gpu=1, need.gpu_mem=24GiB) uv run python train.py".into(),
                mode: Mode::Job,
            },
        )
        .await;
        let (job_id, start_scope) = job_created_from_response(created);
        assert!(start_scope.is_some(), "resource run should return start scope");
        assert_eq!(
            wait_for_job_terminal(&mut stream, 2, &job_id).await,
            JobStatus::Done
        );

        let out = roundtrip(
            &mut stream,
            20,
            RequestPayload::Eval {
                input: format!(":out {job_id}"),
                mode: Mode::Job,
            },
        )
        .await;
        assert!(matches!(
            out,
            ResponsePayload::Ok(OkPayload::Output { ref data, .. }) if data.contains("reserved-from-scope")
        ));

        let request_log = std::fs::read_to_string(&request_log).expect("read reserve log");
        assert!(request_log.contains(r#""gpu":{"kind":"count","value":1}"#));
        assert!(request_log.contains(r#""gpu_mem":{"kind":"bytes","value":25769803776}"#));

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_run_overlay_tmpfs_mode_param_reaches_launch() {
    run_daemon_test(async {
        let env = TestEnv::new("run-overlay-tmpfs");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        let created = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input: ":run(sandbox=overlay, sandbox.upper=tmpfs) /bin/sh -c 'printf ok'".into(),
                mode: Mode::Job,
            },
        )
        .await;
        let (job_id, start_scope) = job_created_from_response(created);
        assert!(
            start_scope.is_some(),
            "sandbox run should return start scope"
        );
        let status = wait_for_job_terminal(&mut stream, 2, &job_id).await;

        match status {
            JobStatus::Done => {
                let out = roundtrip(
                    &mut stream,
                    20,
                    RequestPayload::Eval {
                        input: format!(":out {job_id}"),
                        mode: Mode::Job,
                    },
                )
                .await;
                assert!(matches!(
                    out,
                    ResponsePayload::Ok(OkPayload::Output { ref data, .. }) if data.contains("ok")
                ));
            }
            JobStatus::Failed => {
                let err = roundtrip(
                    &mut stream,
                    21,
                    RequestPayload::Eval {
                        input: format!(":err {job_id}"),
                        mode: Mode::Job,
                    },
                )
                .await;
                assert!(matches!(
                    err,
                    ResponsePayload::Ok(OkPayload::Output { ref data, .. })
                        if data.contains("overlay sandbox")
                            || data.contains("tmpfs")
                            || data.contains("only supported on Linux")
                            || data.contains("Operation not permitted")
                            || data.contains("permission denied")
                ));
            }
            other => panic!("sandbox job should be terminal, got {other:?}"),
        }

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_restart_restores_jobs_without_global_scope_head() {
    run_daemon_test(async {
        let env = TestEnv::new("persist");
        let persisted_cwd = env.root.join("persisted-cwd");
        std::fs::create_dir_all(&persisted_cwd).expect("create persisted cwd");

        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        let first = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input: "echo persisted".into(),
                mode: Mode::Job,
            },
        )
        .await;
        let first_job = match first {
            ResponsePayload::Ok(OkPayload::JobCreated { job_id, .. }) => job_id,
            other => panic!("expected JobCreated, got {other:?}"),
        };
        let status = wait_for_job_terminal(&mut stream, 2, &first_job).await;
        assert_eq!(status, JobStatus::Done);

        let cd_resp = roundtrip(
            &mut stream,
            20,
            RequestPayload::Eval {
                input: format!(":cd {}", persisted_cwd.display()),
                mode: Mode::Job,
            },
        )
        .await;
        match cd_resp {
            ResponsePayload::Ok(OkPayload::ScopeCreated { summary, .. }) => {
                assert!(summary.contains("cwd:"));
                assert!(summary.contains(&persisted_cwd.display().to_string()));
            }
            other => panic!("expected ScopeCreated, got {other:?}"),
        }

        shutdown_daemon(&mut stream, &mut child).await;

        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        let jobs_resp = roundtrip(
            &mut stream,
            30,
            RequestPayload::Eval {
                input: ":jobs".into(),
                mode: Mode::Job,
            },
        )
        .await;
        let restored_job = match jobs_resp {
            ResponsePayload::Ok(OkPayload::JobList(list)) => list
                .into_iter()
                .find(|job| job.id == first_job)
                .expect("restored job missing"),
            other => panic!("expected JobList, got {other:?}"),
        };
        assert_eq!(restored_job.pipeline, "echo persisted");
        assert_eq!(restored_job.status, JobStatus::Done);
        assert_eq!(restored_job.exit_code, Some(0));
        assert!(restored_job.start_scope.is_some());
        assert!(restored_job.end_scope.is_some());

        let second = roundtrip(
            &mut stream,
            31,
            RequestPayload::Eval {
                input: "pwd".into(),
                mode: Mode::Job,
            },
        )
        .await;
        let second_job = match second {
            ResponsePayload::Ok(OkPayload::JobCreated { job_id, .. }) => job_id,
            other => panic!("expected JobCreated after restart, got {other:?}"),
        };
        assert_eq!(second_job, "J2");

        let status = wait_for_job_terminal(&mut stream, 32, &second_job).await;
        assert_eq!(status, JobStatus::Done);

        let out_resp = roundtrip(
            &mut stream,
            40,
            RequestPayload::Eval {
                input: format!(":out {second_job}"),
                mode: Mode::Job,
            },
        )
        .await;
        match out_resp {
            ResponsePayload::Ok(OkPayload::Output { data, .. }) => {
                let actual = std::fs::canonicalize(data.trim()).expect("canonicalize restart cwd");
                let expected = std::fs::canonicalize(default_test_session_cwd(&env.socket))
                    .expect("canonicalize expected session cwd");
                assert_eq!(actual, expected);
                assert_ne!(actual, std::fs::canonicalize(&persisted_cwd).unwrap());
            }
            other => panic!("expected Output, got {other:?}"),
        }

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_sessions_keep_cwd_isolated_and_reconnect_by_id() {
    run_daemon_test(async {
        let env = TestEnv::new("session-cwd");
        let alpha = env.root.join("alpha");
        let beta = env.root.join("beta");
        std::fs::create_dir_all(&alpha).expect("create alpha cwd");
        std::fs::create_dir_all(&beta).expect("create beta cwd");

        let mut child = env.spawn_daemon();
        let mut session_a = wait_for_socket_with_session(
            &env.socket,
            &mut child,
            "session-a",
            &default_test_session_cwd(&env.socket),
        )
        .await;
        let mut session_b = wait_for_socket_with_session(
            &env.socket,
            &mut child,
            "session-b",
            &default_test_session_cwd(&env.socket),
        )
        .await;

        let cd_a = roundtrip(
            &mut session_a,
            1,
            RequestPayload::Eval {
                input: format!(":cd {}", alpha.display()),
                mode: Mode::Job,
            },
        )
        .await;
        assert!(matches!(
            cd_a,
            ResponsePayload::Ok(OkPayload::ScopeCreated { .. })
        ));

        let mut a_request = 2;
        let mut b_request = 1;
        assert_eq!(
            run_pwd_and_read(&mut session_a, &mut a_request).await,
            std::fs::canonicalize(&alpha).expect("canonicalize alpha")
        );
        assert_eq!(
            run_pwd_and_read(&mut session_b, &mut b_request).await,
            std::fs::canonicalize(default_test_session_cwd(&env.socket))
                .expect("canonicalize default cwd")
        );

        let cd_b = roundtrip(
            &mut session_b,
            b_request,
            RequestPayload::Eval {
                input: format!(":cd {}", beta.display()),
                mode: Mode::Job,
            },
        )
        .await;
        b_request += 1;
        assert!(matches!(
            cd_b,
            ResponsePayload::Ok(OkPayload::ScopeCreated { .. })
        ));
        assert_eq!(
            run_pwd_and_read(&mut session_b, &mut b_request).await,
            std::fs::canonicalize(&beta).expect("canonicalize beta")
        );
        assert_eq!(
            run_pwd_and_read(&mut session_a, &mut a_request).await,
            std::fs::canonicalize(&alpha).expect("canonicalize alpha")
        );

        drop(session_a);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut session_a = wait_for_socket_with_session(
            &env.socket,
            &mut child,
            "session-a",
            &default_test_session_cwd(&env.socket),
        )
        .await;
        let mut reconnect_request = 1;
        assert_eq!(
            run_pwd_and_read(&mut session_a, &mut reconnect_request).await,
            std::fs::canonicalize(&alpha).expect("canonicalize alpha")
        );

        shutdown_daemon(&mut session_b, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_named_session_is_shared_and_restored_with_owned_jobs() {
    run_daemon_test(async {
        let env = TestEnv::new("named-session-shared");
        let shared_cwd = env.root.join("shared-cwd");
        std::fs::create_dir_all(&shared_cwd).expect("create shared cwd");

        let mut child = env.spawn_daemon();
        let default_cwd = default_test_session_cwd(&env.socket);
        let mut human =
            wait_for_socket_with_session(&env.socket, &mut child, "human-client", &default_cwd)
                .await;
        let mut agent =
            wait_for_socket_with_session(&env.socket, &mut child, "agent-client", &default_cwd)
                .await;

        let created = roundtrip(
            &mut human,
            1,
            RequestPayload::CreateSession {
                name: "shared-dev".into(),
            },
        )
        .await;
        let session_id = match created {
            ResponsePayload::Ok(OkPayload::SessionInfo(info)) => {
                assert_eq!(info.name, "shared-dev");
                assert!(info.current);
                assert!(info.restart_safe);
                info.id
            }
            other => panic!("expected SessionInfo after create, got {other:?}"),
        };

        let cd = roundtrip(
            &mut human,
            2,
            RequestPayload::Eval {
                input: format!(":cd {}", shared_cwd.display()),
                mode: Mode::Job,
            },
        )
        .await;
        assert!(matches!(
            cd,
            ResponsePayload::Ok(OkPayload::ScopeCreated { .. })
        ));

        let attached = roundtrip(
            &mut agent,
            1,
            RequestPayload::AttachSession {
                selector: "shared-dev".into(),
                refresh: false,
            },
        )
        .await;
        assert!(matches!(
            attached,
            ResponsePayload::Ok(OkPayload::SessionInfo(ref info))
                if info.id == session_id && info.connected_clients == 2 && info.current
        ));

        let mut agent_request = 2;
        assert_eq!(
            run_pwd_and_read(&mut agent, &mut agent_request).await,
            std::fs::canonicalize(&shared_cwd).expect("canonicalize shared cwd")
        );

        let created_job = roundtrip(
            &mut human,
            3,
            RequestPayload::Eval {
                input: "echo shared-job".into(),
                mode: Mode::Job,
            },
        )
        .await;
        let job_id = job_id_from_created(created_job);
        assert_eq!(
            wait_for_job_terminal(&mut agent, agent_request, &job_id).await,
            JobStatus::Done
        );
        agent_request += 1;
        let jobs = roundtrip(
            &mut agent,
            agent_request,
            RequestPayload::ListJobs { limit: None },
        )
        .await;
        match jobs {
            ResponsePayload::Ok(OkPayload::JobListPage { jobs, .. }) => {
                let owned = jobs
                    .iter()
                    .find(|job| job.id == job_id)
                    .expect("shared job visible to attached agent");
                assert_eq!(owned.session_id.as_deref(), Some(session_id.as_str()));
            }
            other => panic!("expected owned JobListPage, got {other:?}"),
        }

        shutdown_daemon(&mut human, &mut child).await;

        let mut child = env.spawn_daemon();
        let mut restored = wait_for_socket_with_session(
            &env.socket,
            &mut child,
            "restored-client",
            &default_cwd,
        )
        .await;
        let sessions = roundtrip(&mut restored, 1, RequestPayload::ListSessions {}).await;
        assert!(matches!(
            sessions,
            ResponsePayload::Ok(OkPayload::SessionList(ref sessions))
                if sessions.iter().any(|session| session.id == session_id && session.restart_safe)
        ));
        let attach = roundtrip(
            &mut restored,
            2,
            RequestPayload::AttachSession {
                selector: session_id.clone(),
                refresh: false,
            },
        )
        .await;
        assert!(matches!(
            attach,
            ResponsePayload::Ok(OkPayload::SessionInfo(ref info)) if info.id == session_id
        ));
        let mut restored_request = 3;
        assert_eq!(
            run_pwd_and_read(&mut restored, &mut restored_request).await,
            std::fs::canonicalize(&shared_cwd).expect("canonicalize restored cwd")
        );
        let jobs = roundtrip(
            &mut restored,
            restored_request,
            RequestPayload::ListJobs { limit: None },
        )
        .await;
        assert!(matches!(
            jobs,
            ResponsePayload::Ok(OkPayload::JobListPage { ref jobs, .. })
                if jobs.iter().any(|job| job.id == job_id && job.session_id.as_deref() == Some(session_id.as_str()))
        ));

        shutdown_daemon(&mut restored, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_named_session_archive_restore_roundtrips_over_ipc_and_restart() {
    run_daemon_test(async {
        let env = TestEnv::new("named-session-archive");
        let cwd = default_test_session_cwd(&env.socket);
        let mut child = env.spawn_daemon();
        let mut owner =
            wait_for_socket_with_session(&env.socket, &mut child, "archive-owner", &cwd).await;

        let pong = roundtrip(&mut owner, 1, RequestPayload::Ping {}).await;
        assert!(matches!(
            pong,
            ResponsePayload::Ok(OkPayload::Pong { ref capabilities, .. })
                if capabilities.iter().any(|capability| capability == ipc::IPC_CAPABILITY_SESSION_ARCHIVE)
        ));
        let session = create_named_session(&mut owner, 2, "archive-daily").await;
        assert_eq!(session.archived_at_ms, None);
        let job_id = job_id_from_created(
            roundtrip(
                &mut owner,
                3,
                RequestPayload::Eval {
                    input: "echo archive-history".into(),
                    mode: Mode::Job,
                },
            )
            .await,
        );
        assert_eq!(
            wait_for_job_terminal(&mut owner, 4, &job_id).await,
            JobStatus::Done
        );
        drop(owner);

        let mut cleaner =
            wait_for_socket_with_session(&env.socket, &mut child, "archive-cleaner", &cwd).await;
        let mut disconnected = false;
        for request_id in 1..=40 {
            match roundtrip(
                &mut cleaner,
                request_id,
                RequestPayload::SessionInfo {
                    selector: Some(session.id.clone()),
                },
            )
            .await
            {
                ResponsePayload::Ok(OkPayload::SessionInfo(info))
                    if info.connected_clients == 0 =>
                {
                    disconnected = true;
                    break;
                }
                ResponsePayload::Ok(OkPayload::SessionInfo(_)) => {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                other => panic!("expected session info while waiting for disconnect: {other:?}"),
            }
        }
        assert!(disconnected, "session owner did not disconnect");

        let archived = roundtrip_with_operation(
            &mut cleaner,
            100,
            "archive-daily-1",
            RequestPayload::ArchiveSession {
                selector: "archive-daily".into(),
            },
        )
        .await;
        let archived_at_ms = match archived {
            ResponsePayload::Ok(OkPayload::SessionInfo(info)) => {
                info.archived_at_ms.expect("archive timestamp")
            }
            other => panic!("expected archived SessionInfo, got {other:?}"),
        };
        let replayed = roundtrip_with_operation(
            &mut cleaner,
            101,
            "archive-daily-1",
            RequestPayload::ArchiveSession {
                selector: "archive-daily".into(),
            },
        )
        .await;
        assert!(matches!(
            replayed,
            ResponsePayload::Ok(OkPayload::SessionInfo(info))
                if info.archived_at_ms == Some(archived_at_ms)
        ));

        assert!(matches!(
            roundtrip(&mut cleaner, 102, RequestPayload::ListSessions {}).await,
            ResponsePayload::Ok(OkPayload::SessionList(ref sessions))
                if sessions.iter().all(|candidate| candidate.id != session.id)
        ));
        assert!(matches!(
            roundtrip(
                &mut cleaner,
                103,
                RequestPayload::ListArchivedSessions {},
            )
            .await,
            ResponsePayload::Ok(OkPayload::SessionList(ref sessions))
                if sessions.len() == 1
                    && sessions[0].id == session.id
                    && sessions[0].archived_at_ms == Some(archived_at_ms)
        ));
        assert!(matches!(
            roundtrip(&mut cleaner, 104, RequestPayload::ListAllSessions {}).await,
            ResponsePayload::Ok(OkPayload::SessionList(ref sessions))
                if sessions.len() == 1 && sessions[0].id == session.id
        ));
        assert!(matches!(
            attach_named_session(&mut cleaner, 105, &session.id, false).await,
            ResponsePayload::Err { ref code, .. } if code == ipc::error_code::INVALID_STATE
        ));
        assert!(matches!(
            roundtrip(
                &mut cleaner,
                106,
                RequestPayload::CreateSession {
                    name: "archive-daily".into(),
                },
            )
            .await,
            ResponsePayload::Err { ref code, .. } if code == ipc::error_code::ALREADY_EXISTS
        ));

        shutdown_daemon(&mut cleaner, &mut child).await;

        let mut child = env.spawn_daemon();
        let mut restored =
            wait_for_socket_with_session(&env.socket, &mut child, "archive-restorer", &cwd).await;
        assert!(matches!(
            roundtrip(&mut restored, 1, RequestPayload::ListSessions {}).await,
            ResponsePayload::Ok(OkPayload::SessionList(ref sessions))
                if sessions.iter().all(|candidate| candidate.id != session.id)
        ));
        assert!(matches!(
            roundtrip(
                &mut restored,
                2,
                RequestPayload::ListArchivedSessions {},
            )
            .await,
            ResponsePayload::Ok(OkPayload::SessionList(ref sessions))
                if sessions.len() == 1
                    && sessions[0].id == session.id
                    && sessions[0].archived_at_ms == Some(archived_at_ms)
        ));

        let restored_info = roundtrip_with_operation(
            &mut restored,
            3,
            "restore-daily-1",
            RequestPayload::RestoreSession {
                selector: session.id.clone(),
            },
        )
        .await;
        assert!(matches!(
            restored_info,
            ResponsePayload::Ok(OkPayload::SessionInfo(info)) if info.archived_at_ms.is_none()
        ));
        let restored_replay = roundtrip_with_operation(
            &mut restored,
            4,
            "restore-daily-1",
            RequestPayload::RestoreSession {
                selector: session.id.clone(),
            },
        )
        .await;
        assert!(matches!(
            restored_replay,
            ResponsePayload::Ok(OkPayload::SessionInfo(info)) if info.archived_at_ms.is_none()
        ));
        assert!(matches!(
            attach_named_session(&mut restored, 5, &session.id, false).await,
            ResponsePayload::Ok(OkPayload::SessionInfo(ref info))
                if info.id == session.id && info.current && info.archived_at_ms.is_none()
        ));
        assert!(matches!(
            roundtrip(&mut restored, 6, RequestPayload::ListJobs { limit: None }).await,
            ResponsePayload::Ok(OkPayload::JobListPage { ref jobs, .. })
                if jobs.iter().any(|job| job.id == job_id && job.session_id.as_deref() == Some(session.id.as_str()))
        ));

        shutdown_daemon(&mut restored, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_named_sessions_isolate_events_output_and_id_controls() {
    run_daemon_test(async {
        let env = TestEnv::new("named-session-boundaries");
        let alpha_script = env.root.join("alpha-output.sh");
        let beta_script = env.root.join("beta-output.sh");
        let alpha_long_script = env.root.join("alpha-long.sh");
        let output_gate = env.root.join("release-output");
        write_executable_script(
            &alpha_script,
            &format!(
                "#!/bin/sh\nwhile [ ! -f '{}' ]; do sleep 0.05; done\nprintf 'alpha-session-output\\n'\n",
                output_gate.display()
            ),
        );
        write_executable_script(
            &beta_script,
            &format!(
                "#!/bin/sh\nwhile [ ! -f '{}' ]; do sleep 0.05; done\nprintf 'beta-session-output\\n'\n",
                output_gate.display()
            ),
        );
        write_executable_script(&alpha_long_script, "#!/bin/sh\nsleep 30\n");

        let mut child = env.spawn_daemon();
        let cwd = default_test_session_cwd(&env.socket);

        let mut alpha_control =
            wait_for_socket_with_session(&env.socket, &mut child, "alpha-control", &cwd).await;
        let alpha = create_named_session(&mut alpha_control, 1, "alpha").await;
        let mut beta_control =
            wait_for_socket_with_session(&env.socket, &mut child, "beta-control", &cwd).await;
        let beta = create_named_session(&mut beta_control, 1, "beta").await;

        let mut alpha_jobs =
            wait_for_socket_with_session(&env.socket, &mut child, "alpha-jobs", &cwd).await;
        assert!(matches!(
            attach_named_session(&mut alpha_jobs, 1, &alpha.id, false).await,
            ResponsePayload::Ok(OkPayload::SessionInfo(ref info)) if info.id == alpha.id
        ));
        subscribe(&mut alpha_jobs, 2, ["jobs"]).await;

        let mut beta_jobs =
            wait_for_socket_with_session(&env.socket, &mut child, "beta-jobs", &cwd).await;
        assert!(matches!(
            attach_named_session(&mut beta_jobs, 1, &beta.id, false).await,
            ResponsePayload::Ok(OkPayload::SessionInfo(ref info)) if info.id == beta.id
        ));
        subscribe(&mut beta_jobs, 2, ["jobs"]).await;

        // An unattached client preserves the legacy global event view.
        let mut legacy_jobs =
            wait_for_socket_with_session(&env.socket, &mut child, "legacy-jobs", &cwd).await;
        subscribe(&mut legacy_jobs, 1, ["jobs"]).await;

        let alpha_job = job_id_from_created(
            roundtrip(
                &mut alpha_control,
                2,
                RequestPayload::Eval {
                    input: alpha_script.display().to_string(),
                    mode: Mode::Job,
                },
            )
            .await,
        );
        let beta_job = job_id_from_created(
            roundtrip(
                &mut beta_control,
                2,
                RequestPayload::Eval {
                    input: beta_script.display().to_string(),
                    mode: Mode::Job,
                },
            )
            .await,
        );

        let mut alpha_output =
            wait_for_socket_with_session(&env.socket, &mut child, "alpha-output", &cwd).await;
        assert!(matches!(
            attach_named_session(&mut alpha_output, 1, &alpha.id, false).await,
            ResponsePayload::Ok(OkPayload::SessionInfo(ref info)) if info.id == alpha.id
        ));
        subscribe(&mut alpha_output, 2, [format!("output:{alpha_job}")]).await;

        let mut beta_output =
            wait_for_socket_with_session(&env.socket, &mut child, "beta-output", &cwd).await;
        assert!(matches!(
            attach_named_session(&mut beta_output, 1, &beta.id, false).await,
            ResponsePayload::Ok(OkPayload::SessionInfo(ref info)) if info.id == beta.id
        ));
        // Subscribing to a foreign ID is harmless: no foreign chunks may leak.
        subscribe(
            &mut beta_output,
            2,
            [format!("output:{beta_job}"), format!("output:{alpha_job}")],
        )
        .await;

        let mut legacy_output =
            wait_for_socket_with_session(&env.socket, &mut child, "legacy-output", &cwd).await;
        subscribe(
            &mut legacy_output,
            1,
            [format!("output:{alpha_job}"), format!("output:{beta_job}")],
        )
        .await;
        fs::write(&output_gate, "release\n").expect("release output scripts after subscriptions");

        let mut alpha_job_messages =
            collect_until(&mut alpha_jobs, Duration::from_secs(5), |message| {
                matches!(
                    message,
                    Message::Event {
                        payload: EventPayload::JobStateChanged {
                            job_id,
                            new_state,
                            ..
                        },
                    } if job_id == &alpha_job && new_state.is_terminal()
                )
            })
            .await;
        alpha_job_messages
            .extend(collect_until(&mut alpha_jobs, Duration::from_millis(300), |_| false).await);
        let alpha_event_ids = alpha_job_messages
            .iter()
            .filter_map(|message| match message {
                Message::Event {
                    payload:
                        EventPayload::JobCreated { job_id, .. }
                        | EventPayload::JobStateChanged { job_id, .. },
                } => Some(job_id.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(alpha_event_ids.contains(&alpha_job.as_str()));
        assert!(
            alpha_event_ids.iter().all(|job_id| *job_id == alpha_job),
            "alpha received foreign job events: {alpha_job_messages:?}"
        );

        let mut beta_job_messages =
            collect_until(&mut beta_jobs, Duration::from_secs(5), |message| {
                matches!(
                    message,
                    Message::Event {
                        payload: EventPayload::JobStateChanged {
                            job_id,
                            new_state,
                            ..
                        },
                    } if job_id == &beta_job && new_state.is_terminal()
                )
            })
            .await;
        beta_job_messages
            .extend(collect_until(&mut beta_jobs, Duration::from_millis(300), |_| false).await);
        let beta_event_ids = beta_job_messages
            .iter()
            .filter_map(|message| match message {
                Message::Event {
                    payload:
                        EventPayload::JobCreated { job_id, .. }
                        | EventPayload::JobStateChanged { job_id, .. },
                } => Some(job_id.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(beta_event_ids.contains(&beta_job.as_str()));
        assert!(
            beta_event_ids.iter().all(|job_id| *job_id == beta_job),
            "beta received foreign job events: {beta_job_messages:?}"
        );

        let mut legacy_alpha_done = false;
        let mut legacy_beta_done = false;
        let legacy_job_messages =
            collect_until(&mut legacy_jobs, Duration::from_secs(5), |message| {
                if let Message::Event {
                    payload:
                        EventPayload::JobStateChanged {
                            job_id, new_state, ..
                        },
                } = message
                    && new_state.is_terminal()
                {
                    legacy_alpha_done |= job_id == &alpha_job;
                    legacy_beta_done |= job_id == &beta_job;
                }
                legacy_alpha_done && legacy_beta_done
            })
            .await;
        for expected in [&alpha_job, &beta_job] {
            assert!(
                legacy_job_messages.iter().any(|message| matches!(
                    message,
                    Message::Event {
                        payload:
                            EventPayload::JobCreated { job_id, .. }
                            | EventPayload::JobStateChanged { job_id, .. },
                    } if job_id == expected
                )),
                "legacy subscriber missed {expected}: {legacy_job_messages:?}"
            );
        }

        let alpha_output_messages =
            collect_until(&mut alpha_output, Duration::from_secs(5), |message| {
                matches!(
                    message,
                    Message::Event {
                        payload: EventPayload::OutputEof { id },
                    } if id == &alpha_job
                )
            })
            .await;
        assert!(alpha_output_messages.iter().any(|message| matches!(
            message,
            Message::Event {
                payload: EventPayload::OutputChunk { id, data, .. },
            } if id == &alpha_job && data.contains("alpha-session-output")
        )));
        assert!(alpha_output_messages.iter().all(|message| !matches!(
            message,
            Message::Event {
                payload:
                    EventPayload::OutputChunk { id, .. }
                    | EventPayload::OutputChunkBinary { id, .. }
                    | EventPayload::OutputEof { id },
            } if id == &beta_job
        )));

        let beta_output_messages =
            collect_until(&mut beta_output, Duration::from_secs(5), |message| {
                matches!(
                    message,
                    Message::Event {
                        payload: EventPayload::OutputEof { id },
                    } if id == &beta_job
                )
            })
            .await;
        assert!(beta_output_messages.iter().any(|message| matches!(
            message,
            Message::Event {
                payload: EventPayload::OutputChunk { id, data, .. },
            } if id == &beta_job && data.contains("beta-session-output")
        )));
        assert!(
            beta_output_messages.iter().all(|message| !matches!(
                message,
                Message::Event {
                    payload:
                        EventPayload::OutputChunk { id, .. }
                        | EventPayload::OutputChunkBinary { id, .. }
                        | EventPayload::OutputEof { id },
                } if id == &alpha_job
            )),
            "beta received alpha output despite only owning beta: {beta_output_messages:?}"
        );

        let mut legacy_alpha_eof = false;
        let mut legacy_beta_eof = false;
        let legacy_output_messages =
            collect_until(&mut legacy_output, Duration::from_secs(5), |message| {
                if let Message::Event {
                    payload: EventPayload::OutputEof { id },
                } = message
                {
                    legacy_alpha_eof |= id == &alpha_job;
                    legacy_beta_eof |= id == &beta_job;
                }
                legacy_alpha_eof && legacy_beta_eof
            })
            .await;
        for (expected_id, expected_output) in [
            (&alpha_job, "alpha-session-output"),
            (&beta_job, "beta-session-output"),
        ] {
            assert!(
                legacy_output_messages.iter().any(|message| matches!(
                    message,
                    Message::Event {
                        payload: EventPayload::OutputChunk { id, data, .. },
                    } if id == expected_id && data.contains(expected_output)
                )),
                "legacy subscriber missed output for {expected_id}: {legacy_output_messages:?}"
            );
        }

        match roundtrip(
            &mut alpha_control,
            3,
            RequestPayload::JobOutput {
                id: alpha_job.clone(),
                stdout_bytes: None,
                stderr_bytes: None,
            },
        )
        .await
        {
            ResponsePayload::Ok(OkPayload::JobOutput { stdout, .. }) => {
                assert!(stdout.data.contains("alpha-session-output"));
            }
            other => panic!("alpha must read its own output, got {other:?}"),
        }
        match roundtrip(
            &mut beta_control,
            3,
            RequestPayload::JobOutput {
                id: alpha_job.clone(),
                stdout_bytes: None,
                stderr_bytes: None,
            },
        )
        .await
        {
            ResponsePayload::Err { code, .. } => assert_eq!(code, ipc::error_code::NOT_FOUND),
            other => panic!("foreign output lookup must be hidden, got {other:?}"),
        }

        let alpha_long_job = job_id_from_created(
            roundtrip(
                &mut alpha_control,
                4,
                RequestPayload::Eval {
                    input: alpha_long_script.display().to_string(),
                    mode: Mode::Job,
                },
            )
            .await,
        );
        wait_for_job_status(&mut alpha_control, 5, &alpha_long_job, |job| {
            job.status == JobStatus::Running
        })
        .await;
        match roundtrip(
            &mut beta_control,
            4,
            RequestPayload::KillJob {
                id: alpha_long_job.clone(),
            },
        )
        .await
        {
            ResponsePayload::Err { code, .. } => assert_eq!(code, ipc::error_code::NOT_FOUND),
            other => panic!("foreign kill must be hidden, got {other:?}"),
        }
        assert!(matches!(
            roundtrip(
                &mut alpha_control,
                100,
                RequestPayload::KillJob { id: alpha_long_job },
            )
            .await,
            ResponsePayload::Ok(OkPayload::Ack {})
        ));

        shutdown_daemon(&mut alpha_control, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_named_session_switch_fences_events_and_foreground_owner() {
    run_daemon_test(async {
        let env = TestEnv::new("named-session-switch-fence");
        let output_gate = env.root.join("release-alpha-direct-output");
        let alpha_script = env.root.join("alpha-direct-output.sh");
        write_executable_script(
            &alpha_script,
            &format!(
                "#!/bin/sh\nwhile [ ! -f '{}' ]; do sleep 0.05; done\nprintf 'alpha-direct-after-switch\\n'\n",
                output_gate.display()
            ),
        );

        let mut child = env.spawn_daemon();
        let cwd = default_test_session_cwd(&env.socket);
        let mut alpha_control =
            wait_for_socket_with_session(&env.socket, &mut child, "switch-alpha-control", &cwd)
                .await;
        let alpha = create_named_session(&mut alpha_control, 1, "switch-alpha").await;
        let mut beta_control =
            wait_for_socket_with_session(&env.socket, &mut child, "switch-beta-control", &cwd)
                .await;
        let beta = create_named_session(&mut beta_control, 1, "switch-beta").await;

        let mut switcher =
            wait_for_socket_with_session(&env.socket, &mut child, "switching-client", &cwd).await;
        assert!(matches!(
            attach_named_session(&mut switcher, 1, &alpha.id, false).await,
            ResponsePayload::Ok(OkPayload::SessionInfo(ref info)) if info.id == alpha.id
        ));
        subscribe(&mut switcher, 2, ["jobs"]).await;

        // File-script jobs stream output directly back to their submitting
        // connection. Keep that A-owned output blocked until after the same
        // connection has successfully attached to B.
        let (script_id, alpha_script_job) = match roundtrip(
            &mut switcher,
            3,
            RequestPayload::RunScript {
                path: "alpha-direct-output.cue".into(),
                input: alpha_script.display().to_string(),
            },
        )
        .await
        {
            ResponsePayload::Ok(OkPayload::ScriptCreated {
                script_id, items, ..
            }) => {
                assert_eq!(items.len(), 1);
                let job_id = match &items[0].result {
                    ScriptItemResult::Job { job_id, .. } => job_id.clone(),
                    other => panic!("expected gated script job, got {other:?}"),
                };
                (script_id, job_id)
            }
            other => panic!("expected ScriptCreated, got {other:?}"),
        };
        subscribe(
            &mut switcher,
            4,
            [format!("output:{alpha_script_job}")],
        )
        .await;
        wait_for_job_status(&mut alpha_control, 10, &alpha_script_job, |job| {
            job.status == JobStatus::Running
        })
        .await;

        let alpha_fg_job = job_id_from_created(
            roundtrip(
                &mut switcher,
                5,
                RequestPayload::Eval {
                    input: "cat".into(),
                    mode: Mode::Job,
                },
            )
            .await,
        );
        wait_for_job_status(&mut alpha_control, 100, &alpha_fg_job, |job| {
            job.status == JobStatus::Running
        })
        .await;

        let attach_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        let mut fg_attach_request = 10;
        loop {
            let response = roundtrip(
                &mut switcher,
                fg_attach_request,
                RequestPayload::FgAttach {
                    id: alpha_fg_job.clone(),
                },
            )
            .await;
            fg_attach_request += 1;
            match response {
                ResponsePayload::Ok(OkPayload::FgAttached(_)) => break,
                ResponsePayload::Err { message, .. } if message.contains("is not running") => {
                    assert!(
                        tokio::time::Instant::now() < attach_deadline,
                        "A foreground job never became attachable"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                other => panic!("unexpected A foreground attach response: {other:?}"),
            }
        }
        assert!(matches!(
            roundtrip(
                &mut switcher,
                50,
                RequestPayload::FgResize { cols: 100, rows: 40 },
            )
            .await,
            ResponsePayload::Ok(OkPayload::Ack {})
        ));

        // Submit one last A job immediately before the switch. Its already
        // queued events may appear before the attach response, but never after
        // that response has established the B boundary.
        let queued_alpha_job = job_id_from_created(
            roundtrip(
                &mut alpha_control,
                1000,
                RequestPayload::Eval {
                    input: "echo alpha-before-switch".into(),
                    mode: Mode::Job,
                },
            )
            .await,
        );
        let (attach_beta, _messages_before_attach_ack) = roundtrip_with_messages(
            &mut switcher,
            51,
            RequestPayload::AttachSession {
                selector: beta.id.clone(),
                refresh: false,
            },
        )
        .await;
        assert!(matches!(
            attach_beta,
            ResponsePayload::Ok(OkPayload::SessionInfo(ref info))
                if info.id == beta.id && info.current
        ));

        let alpha_job_ids = [&alpha_script_job, &alpha_fg_job, &queued_alpha_job];
        let is_alpha_event = |message: &Message| {
            let Message::Event { payload } = message else {
                return false;
            };
            match payload {
                EventPayload::JobCreated { job_id, .. }
                | EventPayload::JobStateChanged { job_id, .. }
                | EventPayload::JobRemoved { job_id } => alpha_job_ids
                    .iter()
                    .any(|expected| expected.as_str() == job_id),
                EventPayload::OutputChunk { id, .. }
                | EventPayload::OutputChunkBinary { id, .. }
                | EventPayload::OutputEof { id } => id == &alpha_script_job,
                EventPayload::ScriptItemCreated {
                    script_id: observed,
                    ..
                }
                | EventPayload::ScriptFinished {
                    script_id: observed,
                    ..
                } => observed == &script_id,
                EventPayload::FgOutput { .. } => true,
                EventPayload::FgExited { id, .. } => id == &alpha_fg_job,
                _ => false,
            }
        };

        let (input_after_switch, mut messages_after_attach_ack) = roundtrip_with_messages(
            &mut switcher,
            52,
            RequestPayload::FgInput {
                data: b"must-not-reach-alpha\n".to_vec(),
            },
        )
        .await;
        match input_after_switch {
            ResponsePayload::Err { code, message } => {
                assert_eq!(code, ipc::error_code::INVALID_STATE);
                assert!(message.contains("no foreground session attached"));
            }
            other => panic!("B must not retain A foreground input ownership: {other:?}"),
        }

        let (resize_after_switch, observed) = roundtrip_with_messages(
            &mut switcher,
            53,
            RequestPayload::FgResize { cols: 80, rows: 24 },
        )
        .await;
        messages_after_attach_ack.extend(observed);
        match resize_after_switch {
            ResponsePayload::Err { code, message } => {
                assert_eq!(code, ipc::error_code::INVALID_STATE);
                assert!(message.contains("no foreground session attached"));
            }
            other => panic!("B must not retain A foreground resize ownership: {other:?}"),
        }

        fs::write(&output_gate, "release\n")
            .expect("release A direct output only after B attach acknowledgement");
        messages_after_attach_ack.extend(
            collect_until(&mut switcher, Duration::from_secs(2), |_| false).await,
        );
        assert_eq!(
            wait_for_job_terminal(&mut alpha_control, 2000, &alpha_script_job).await,
            JobStatus::Done
        );
        messages_after_attach_ack.extend(
            collect_until(&mut switcher, Duration::from_millis(300), |_| false).await,
        );
        assert!(
            messages_after_attach_ack
                .iter()
                .all(|message| !is_alpha_event(message)),
            "A resource event crossed the B attach ACK fence: {messages_after_attach_ack:?}"
        );

        assert!(matches!(
            roundtrip(
                &mut alpha_control,
                3000,
                RequestPayload::KillJob { id: alpha_fg_job },
            )
            .await,
            ResponsePayload::Ok(OkPayload::Ack {})
        ));
        shutdown_daemon(&mut beta_control, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_sensitive_named_session_requires_refresh_after_restart() {
    run_daemon_test(async {
        let env = TestEnv::new("named-session-sensitive-restart");
        let cwd = default_test_session_cwd(&env.socket);
        let mut child = env.spawn_daemon();
        let mut owner = wait_for_raw_socket(&env.socket, &mut child).await;
        handshake_with_env(
            &mut owner,
            "sensitive-owner",
            &cwd,
            BTreeMap::from([
                ("PATH".into(), "/usr/bin:/bin".into()),
                (
                    "TEST_API_TOKEN".into(),
                    "fixture-sensitive-value-do-not-persist".into(),
                ),
            ]),
        )
        .await;
        let created = create_named_session(&mut owner, 1, "volatile-session").await;
        assert_eq!(created.scope_state, SessionScopeState::ReadyVolatile);
        assert!(!created.restart_safe);

        shutdown_daemon(&mut owner, &mut child).await;

        let mut child = env.spawn_daemon();
        let mut restored =
            wait_for_socket_with_session(&env.socket, &mut child, "fresh-owner", &cwd).await;
        match roundtrip(&mut restored, 1, RequestPayload::ListSessions {}).await {
            ResponsePayload::Ok(OkPayload::SessionList(sessions)) => {
                let session = sessions
                    .iter()
                    .find(|session| session.id == created.id)
                    .expect("volatile named session identity survives restart");
                assert_eq!(session.scope_state, SessionScopeState::NeedsRefresh);
                assert!(!session.restart_safe);
                assert!(!session.current);
            }
            other => panic!("expected restored SessionList, got {other:?}"),
        }

        match attach_named_session(&mut restored, 2, &created.id, false).await {
            ResponsePayload::Err { code, .. } => assert_eq!(code, ipc::error_code::INVALID_STATE),
            other => panic!("attach without refresh must fail closed, got {other:?}"),
        }
        match attach_named_session(&mut restored, 3, &created.id, true).await {
            ResponsePayload::Ok(OkPayload::SessionInfo(info)) => {
                assert_eq!(info.id, created.id);
                assert_eq!(info.scope_state, SessionScopeState::ReadyDurable);
                assert!(info.restart_safe);
                assert!(info.current);
            }
            other => panic!("explicit refresh must restore session, got {other:?}"),
        }

        shutdown_daemon(&mut restored, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_background_job_survives_client_disconnect() {
    run_daemon_test(async {
        let env = TestEnv::new("background-disconnect");
        let mut child = env.spawn_daemon();
        let cwd = default_test_session_cwd(&env.socket);
        let mut owner =
            wait_for_socket_with_session(&env.socket, &mut child, "bg-owner", &cwd).await;

        let resp = roundtrip(
            &mut owner,
            1,
            RequestPayload::Eval {
                input: "/bin/sleep 1".into(),
                mode: Mode::Job,
            },
        )
        .await;
        let job_id = job_id_from_created(resp);
        let owner_view = wait_for_job_status(&mut owner, 2, &job_id, |job| {
            matches!(job.status, JobStatus::Running | JobStatus::Done)
        })
        .await;
        assert!(owner_view.start_scope.is_some());

        drop(owner);
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut observer =
            wait_for_socket_with_session(&env.socket, &mut child, "bg-observer", &cwd).await;
        let observer_view = wait_for_job_status(&mut observer, 1, &job_id, |job| {
            matches!(job.status, JobStatus::Running | JobStatus::Done)
        })
        .await;
        assert_eq!(observer_view.id, job_id);
        assert!(observer_view.start_scope.is_some());

        let status = wait_for_job_terminal(&mut observer, 100, &job_id).await;
        assert_eq!(status, JobStatus::Done);

        shutdown_daemon(&mut observer, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_restart_jobs_merge_ambient_path_into_restored_scope() {
    run_daemon_test(async {
        let env = TestEnv::new("ambient-path");
        let live_bin = env.root.join("live-bin");
        std::fs::create_dir_all(&live_bin).expect("create live bin");
        let tool_path = live_bin.join("ambient-only");
        std::fs::write(&tool_path, "#!/bin/sh\necho ambient-ok\n").expect("write ambient tool");
        let mut perms = std::fs::metadata(&tool_path)
            .expect("stat ambient tool")
            .permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&tool_path, perms).expect("chmod ambient tool");

        let stale_path = "/usr/bin:/bin".to_string();
        let live_path = format!("{}:{stale_path}", live_bin.display());

        let mut child = env.spawn_daemon_with_env([("PATH", stale_path.clone())]);
        let mut stream = wait_for_socket(&env.socket, &mut child).await;
        shutdown_daemon(&mut stream, &mut child).await;

        let mut child = env.spawn_daemon_with_env([("PATH", live_path)]);
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        let resp = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input: "/usr/bin/which ambient-only".into(),
                mode: Mode::Job,
            },
        )
        .await;
        let job_id = match resp {
            ResponsePayload::Ok(OkPayload::JobCreated { job_id, .. }) => job_id,
            other => panic!("expected JobCreated, got {other:?}"),
        };

        let status = wait_for_job_terminal(&mut stream, 2, &job_id).await;
        assert_eq!(status, JobStatus::Done);

        let out_resp = roundtrip(
            &mut stream,
            3,
            RequestPayload::Eval {
                input: format!(":out {job_id}"),
                mode: Mode::Job,
            },
        )
        .await;
        match out_resp {
            ResponsePayload::Ok(OkPayload::Output { data, .. }) => {
                assert_eq!(data.trim(), tool_path.display().to_string());
            }
            other => panic!("expected Output, got {other:?}"),
        }

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_env_set_prints_deduped_scope_side_effects() {
    run_daemon_test(async {
        let env = TestEnv::new("env-set-effects");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        let resp = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input: ":env set FOO=first FOO=second BAR=three".into(),
                mode: Mode::Job,
            },
        )
        .await;
        match resp {
            ResponsePayload::Ok(OkPayload::ScopeCreated { summary, .. }) => {
                assert!(summary.contains("env: BAR: <unset> -> three"));
                assert!(summary.contains("env: FOO: <unset> -> second"));
                assert!(!summary.contains("first"));
            }
            other => panic!("expected ScopeCreated, got {other:?}"),
        }

        let env_resp = roundtrip(
            &mut stream,
            2,
            RequestPayload::Eval {
                input: ":env".into(),
                mode: Mode::Job,
            },
        )
        .await;
        match env_resp {
            ResponsePayload::Ok(OkPayload::EvalText { text }) => {
                assert!(text.contains("FOO=second"));
                assert!(text.contains("BAR=three"));
            }
            other => panic!("expected EvalText, got {other:?}"),
        }

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cd_rejects_missing_directory() {
    run_daemon_test(async {
        let env = TestEnv::new("badcd");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        let missing = env.root.join("definitely-missing");
        let resp = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input: format!(":cd {}", missing.display()),
                mode: Mode::Job,
            },
        )
        .await;

        match resp {
            ResponsePayload::Err { code, message } => {
                assert_eq!(code, ipc::error_code::NOT_FOUND);
                assert!(message.contains("cannot cd"));
            }
            other => panic!("expected invalid cd error, got {other:?}"),
        }

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_spawn_failure_does_not_reuse_stale_output_log() {
    run_daemon_test(async {
        let env = TestEnv::new("stale-log");
        let stale_output = env.root.join("data/cue-shell/output");
        std::fs::create_dir_all(&stale_output).expect("create stale output dir");
        std::fs::write(stale_output.join("J1.log"), "stale output\n").expect("write stale log");

        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        let resp = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input: "missing-command-for-stale-log-test".into(),
                mode: Mode::Job,
            },
        )
        .await;
        let job_id = match resp {
            ResponsePayload::Ok(OkPayload::JobCreated { job_id, .. }) => job_id,
            other => panic!("expected JobCreated, got {other:?}"),
        };
        assert_eq!(job_id, "J1");

        let status = wait_for_job_terminal(&mut stream, 2, &job_id).await;
        assert_eq!(status, JobStatus::Failed);

        let out_resp = roundtrip(
            &mut stream,
            3,
            RequestPayload::Eval {
                input: format!(":out {job_id}"),
                mode: Mode::Job,
            },
        )
        .await;
        match out_resp {
            ResponsePayload::Err { code, message } => {
                assert_eq!(code, ipc::error_code::NOT_FOUND);
                assert!(message.contains("no output found"));
            }
            other => panic!("expected no output, got {other:?}"),
        }

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_chain_execution() {
    run_daemon_test(async {
        let env = TestEnv::new("chain");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        // Subscribe to job events.
        subscribe(&mut stream, 1, vec!["jobs"]).await;

        // Submit a serial chain: echo first -> echo second
        let resp = roundtrip(
            &mut stream,
            2,
            RequestPayload::Eval {
                input: "echo first -> echo second".into(),
                mode: Mode::Job,
            },
        )
        .await;

        // For a serial chain `a -> b`, the scheduler returns ChainCreated with
        // only the initially-ready jobs (just the first leaf). The second leaf
        // is spawned when the first completes. Accept either ChainCreated or
        // JobCreated.
        match &resp {
            ResponsePayload::Ok(OkPayload::ChainCreated { job_ids, .. }) => {
                assert!(
                    !job_ids.is_empty(),
                    "chain created with no initially-ready jobs"
                );
            }
            ResponsePayload::Ok(OkPayload::JobCreated { .. }) => {
                // Single-leaf optimisation — still valid.
            }
            other => panic!("expected chain/job created, got {other:?}"),
        }

        // Wait for both jobs to complete (2 terminal state events).
        let mut done_count = 0;

        let msgs = collect_until(&mut stream, Duration::from_secs(10), |msg| {
            if matches!(
                msg,
                Message::Event {
                    payload: EventPayload::JobStateChanged {
                        new_state: JobStatus::Done | JobStatus::Failed,
                        ..
                    },
                }
            ) {
                done_count += 1;
            }
            done_count >= 2
        })
        .await;

        assert!(
            done_count >= 2,
            "expected 2 terminal states, got {done_count}; events: {msgs:?}"
        );

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_job_logical_operators_stay_single_job() {
    run_daemon_test(async {
        let env = TestEnv::new("job-logical");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        let resp = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input: "false && printf no || printf yes".into(),
                mode: Mode::Job,
            },
        )
        .await;
        let job_id = match resp {
            ResponsePayload::Ok(OkPayload::JobCreated { job_id, .. }) => job_id,
            other => panic!("expected single JobCreated, got {other:?}"),
        };

        let status = wait_for_job_terminal(&mut stream, 2, &job_id).await;
        assert_eq!(status, JobStatus::Done);

        let out_resp = roundtrip(
            &mut stream,
            3,
            RequestPayload::Eval {
                input: format!(":out {job_id}"),
                mode: Mode::Job,
            },
        )
        .await;
        match out_resp {
            ResponsePayload::Ok(OkPayload::Output { data, .. }) => {
                assert_eq!(data.trim(), "yes");
                assert!(!data.contains("no"));
            }
            other => panic!("expected Output, got {other:?}"),
        }

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_chain_parallel_operator_uses_triple_pipe() {
    run_daemon_test(async {
        let env = TestEnv::new("triple-pipe");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        let resp = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input: "printf a ||| printf b".into(),
                mode: Mode::Job,
            },
        )
        .await;
        match resp {
            ResponsePayload::Ok(OkPayload::ChainCreated { job_ids, .. }) => {
                assert_eq!(job_ids.len(), 2);
            }
            other => panic!("expected ChainCreated for |||, got {other:?}"),
        }

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_job_local_cd_does_not_update_session_scope_by_default() {
    run_daemon_test(async {
        let env = TestEnv::new("job-local-cd");
        let job_cwd = env.root.join("job-cwd");
        std::fs::create_dir_all(&job_cwd).expect("create job cwd");

        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        let resp = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input: format!("cd {} && pwd", job_cwd.display()),
                mode: Mode::Job,
            },
        )
        .await;
        let job_id = match resp {
            ResponsePayload::Ok(OkPayload::JobCreated { job_id, .. }) => job_id,
            other => panic!("expected JobCreated, got {other:?}"),
        };
        assert_eq!(
            wait_for_job_terminal(&mut stream, 2, &job_id).await,
            JobStatus::Done
        );

        let out_resp = roundtrip(
            &mut stream,
            3,
            RequestPayload::Eval {
                input: format!(":out {job_id}"),
                mode: Mode::Job,
            },
        )
        .await;
        match out_resp {
            ResponsePayload::Ok(OkPayload::Output { data, .. }) => {
                let actual = std::fs::canonicalize(data.trim()).expect("canonicalize job pwd");
                let expected = std::fs::canonicalize(&job_cwd).expect("canonicalize expected cwd");
                assert_eq!(actual, expected);
            }
            other => panic!("expected Output, got {other:?}"),
        }

        let pwd_resp = roundtrip(
            &mut stream,
            4,
            RequestPayload::Eval {
                input: "pwd".into(),
                mode: Mode::Job,
            },
        )
        .await;
        let pwd_job = match pwd_resp {
            ResponsePayload::Ok(OkPayload::JobCreated { job_id, .. }) => job_id,
            other => panic!("expected JobCreated, got {other:?}"),
        };
        assert_eq!(
            wait_for_job_terminal(&mut stream, 5, &pwd_job).await,
            JobStatus::Done
        );

        let pwd_out = roundtrip(
            &mut stream,
            6,
            RequestPayload::Eval {
                input: format!(":out {pwd_job}"),
                mode: Mode::Job,
            },
        )
        .await;
        match pwd_out {
            ResponsePayload::Ok(OkPayload::Output { data, .. }) => {
                let actual = std::fs::canonicalize(data.trim()).expect("canonicalize global pwd");
                let expected = std::fs::canonicalize(default_test_session_cwd(&env.socket))
                    .expect("canonicalize initial session cwd");
                assert_eq!(actual, expected);
            }
            other => panic!("expected Output, got {other:?}"),
        }

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_job_kill() {
    run_daemon_test(async {
        let env = TestEnv::new("kill");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        // Subscribe to events.
        subscribe(&mut stream, 1, vec!["jobs"]).await;

        // Start a long-running job.
        let resp = roundtrip(
            &mut stream,
            2,
            RequestPayload::Eval {
                input: "sleep 60".into(),
                mode: Mode::Job,
            },
        )
        .await;

        let job_id = match &resp {
            ResponsePayload::Ok(OkPayload::JobCreated { job_id, .. }) => job_id.clone(),
            ResponsePayload::Ok(OkPayload::ChainCreated { job_ids, .. }) => {
                job_ids.first().unwrap().clone()
            }
            other => panic!("expected job created, got {other:?}"),
        };

        // Wait for the job to reach Running state.
        let _ = collect_until(&mut stream, Duration::from_secs(5), |msg| {
            matches!(
                msg,
                Message::Event {
                    payload: EventPayload::JobStateChanged {
                        new_state: JobStatus::Running,
                        ..
                    },
                }
            )
        })
        .await;

        // Kill the job.
        let kill_resp = roundtrip(
            &mut stream,
            3,
            RequestPayload::Eval {
                input: format!(":kill {job_id}"),
                mode: Mode::Job,
            },
        )
        .await;
        assert!(
            matches!(kill_resp, ResponsePayload::Ok(OkPayload::Ack {})),
            "expected Ack for kill, got {kill_resp:?}"
        );

        let status = wait_for_job_terminal(&mut stream, 4, &job_id).await;
        assert!(
            matches!(
                status,
                JobStatus::Killed | JobStatus::Failed | JobStatus::Done | JobStatus::Cancelled(_)
            ),
            "expected terminal state after kill, got {status:?}"
        );

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cancel_execution_stops_job_chain_and_script_idempotently() {
    run_daemon_test(async {
        let env = TestEnv::new("cancel-execution");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;
        subscribe(&mut stream, 1, vec!["jobs"]).await;

        let job_id = job_id_from_created(
            roundtrip(
                &mut stream,
                2,
                RequestPayload::Eval {
                    input: ":run(pty=false) sleep 30".into(),
                    mode: Mode::Job,
                },
            )
            .await,
        );
        let cancelled = roundtrip(
            &mut stream,
            3,
            RequestPayload::CancelExecution { id: job_id.clone() },
        )
        .await;
        assert!(matches!(cancelled, ResponsePayload::Ok(OkPayload::Ack {})));
        assert!(matches!(
            wait_for_job_terminal(&mut stream, 4, &job_id).await,
            JobStatus::Cancelled(_)
        ));
        assert!(matches!(
            roundtrip(
                &mut stream,
                5,
                RequestPayload::CancelExecution { id: job_id },
            )
            .await,
            ResponsePayload::Ok(OkPayload::Ack {})
        ));

        let chain_marker = env.root.join("chain-second-ran");
        let chain_response = roundtrip(
            &mut stream,
            6,
            RequestPayload::Eval {
                input: format!(
                    ":run(pty=false) sleep 30 -> touch {}",
                    chain_marker.display()
                ),
                mode: Mode::Job,
            },
        )
        .await;
        let chain_id = match chain_response {
            ResponsePayload::Ok(OkPayload::ChainCreated { chain_id, .. }) => chain_id,
            other => panic!("expected ChainCreated, got {other:?}"),
        };
        assert!(matches!(
            roundtrip(
                &mut stream,
                7,
                RequestPayload::CancelExecution {
                    id: chain_id.clone(),
                },
            )
            .await,
            ResponsePayload::Ok(OkPayload::Ack {})
        ));
        assert!(
            !chain_marker.exists(),
            "cancelled chain advanced to its second leaf"
        );
        assert!(matches!(
            roundtrip(
                &mut stream,
                8,
                RequestPayload::CancelExecution { id: chain_id },
            )
            .await,
            ResponsePayload::Ok(OkPayload::Ack {})
        ));

        let script_marker = env.root.join("script-second-ran");
        let script_response = roundtrip(
            &mut stream,
            9,
            RequestPayload::RunScript {
                path: "cancel.cue".into(),
                input: format!("sleep 30\ntouch {}", script_marker.display()),
            },
        )
        .await;
        let script_id = match script_response {
            ResponsePayload::Ok(OkPayload::ScriptCreated { script_id, .. }) => script_id,
            other => panic!("expected ScriptCreated, got {other:?}"),
        };
        let (script_cancelled, observed) = roundtrip_with_messages(
            &mut stream,
            10,
            RequestPayload::CancelExecution {
                id: script_id.clone(),
            },
        )
        .await;
        assert!(matches!(
            script_cancelled,
            ResponsePayload::Ok(OkPayload::Ack {})
        ));
        let saw_finished = observed.iter().any(|message| {
            matches!(
                message,
                Message::Event {
                    payload: EventPayload::ScriptFinished {
                        script_id: finished,
                        status: ScriptRunStatus::Failed,
                        ..
                    },
                } if finished == &script_id
            )
        });
        if !saw_finished {
            let trailing = collect_until(&mut stream, Duration::from_secs(2), |message| {
                matches!(
                    message,
                    Message::Event {
                        payload: EventPayload::ScriptFinished {
                            script_id: finished,
                            status: ScriptRunStatus::Failed,
                            ..
                        },
                    } if finished == &script_id
                )
            })
            .await;
            assert!(trailing.iter().any(|message| matches!(
                message,
                Message::Event {
                    payload: EventPayload::ScriptFinished {
                        script_id: finished,
                        status: ScriptRunStatus::Failed,
                        ..
                    },
                } if finished == &script_id
            )));
        }
        assert!(
            !script_marker.exists(),
            "cancelled script advanced to its second item"
        );
        assert!(matches!(
            roundtrip(
                &mut stream,
                11,
                RequestPayload::CancelExecution { id: script_id },
            )
            .await,
            ResponsePayload::Ok(OkPayload::Ack {})
        ));

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_pipe_mode_job_kill() {
    run_daemon_test(async {
        let env = TestEnv::new("pipe-kill");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        subscribe(&mut stream, 1, vec!["jobs"]).await;

        let resp = roundtrip(
            &mut stream,
            2,
            RequestPayload::Eval {
                input: ":run(pty=false) sleep 60".into(),
                mode: Mode::Job,
            },
        )
        .await;

        let job_id = match &resp {
            ResponsePayload::Ok(OkPayload::JobCreated { job_id, .. }) => job_id.clone(),
            other => panic!("expected job created, got {other:?}"),
        };

        let running_events = collect_until(&mut stream, Duration::from_secs(5), |msg| {
            matches!(
                msg,
                Message::Event {
                    payload: EventPayload::JobStateChanged {
                        new_state: JobStatus::Running,
                        ..
                    },
                }
            )
        })
        .await;
        assert!(
            running_events.iter().any(|msg| matches!(
                msg,
                Message::Event {
                    payload: EventPayload::JobStateChanged {
                        new_state: JobStatus::Running,
                        ..
                    },
                }
            )),
            "job did not reach running state"
        );

        let kill_resp = roundtrip(
            &mut stream,
            3,
            RequestPayload::Eval {
                input: format!(":kill {job_id}"),
                mode: Mode::Job,
            },
        )
        .await;
        assert!(matches!(kill_resp, ResponsePayload::Ok(OkPayload::Ack {})));

        let status = wait_for_job_terminal(&mut stream, 4, &job_id).await;
        assert_eq!(status, JobStatus::Killed);

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_single_pipe_mode_stdin_is_closed_by_default() {
    run_daemon_test(async {
        let env = TestEnv::new("pipe-stdin-null");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        let resp = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input: ":run(pty=false) cat".into(),
                mode: Mode::Job,
            },
        )
        .await;

        let job_id = match &resp {
            ResponsePayload::Ok(OkPayload::JobCreated { job_id, .. }) => job_id.clone(),
            other => panic!("expected job created, got {other:?}"),
        };

        let status = wait_for_job_terminal(&mut stream, 2, &job_id).await;
        assert_eq!(status, JobStatus::Done);

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_pipe_mode_pipeline_stdin_is_closed_by_default() {
    run_daemon_test(async {
        let env = TestEnv::new("pipe-chain-stdin-null");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        let resp = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input: ":run(pty=false) cat |> wc -c".into(),
                mode: Mode::Job,
            },
        )
        .await;

        let job_id = match &resp {
            ResponsePayload::Ok(OkPayload::JobCreated { job_id, .. }) => job_id.clone(),
            other => panic!("expected job created, got {other:?}"),
        };

        let status = wait_for_job_terminal(&mut stream, 2, &job_id).await;
        assert_eq!(status, JobStatus::Done);

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_fg_attach_input_and_detach() {
    run_daemon_test(async {
        let env = TestEnv::new("fg");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        let job_resp = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input: "cat".into(),
                mode: Mode::Job,
            },
        )
        .await;

        let job_id = match job_resp {
            ResponsePayload::Ok(OkPayload::JobCreated { job_id, .. }) => job_id,
            other => panic!("expected JobCreated, got {other:?}"),
        };

        let attach_resp = roundtrip(
            &mut stream,
            2,
            RequestPayload::Eval {
                input: format!(":fg {job_id}"),
                mode: Mode::Job,
            },
        )
        .await;
        assert!(
            matches!(
                attach_resp,
                ResponsePayload::Ok(OkPayload::FgAttached(_))
            ),
            "expected FgAttached, got {attach_resp:?}"
        );

        let input = b"hello fg\n".to_vec();
        let expected_fragment = b"hello fg".to_vec();
        let input_resp = roundtrip(
            &mut stream,
            3,
            RequestPayload::FgInput {
                data: input.clone(),
            },
        )
        .await;
        assert!(
            matches!(input_resp, ResponsePayload::Ok(OkPayload::Ack {})),
            "expected Ack for fg input, got {input_resp:?}"
        );

        let msgs = collect_until(&mut stream, Duration::from_secs(5), |msg| {
            matches!(
                msg,
                Message::Event {
                    payload: EventPayload::FgOutput { data, .. },
                } if data.windows(expected_fragment.len()).any(|window| window == expected_fragment.as_slice())
            )
        })
        .await;
        assert!(
            msgs.iter().any(|msg| matches!(
                msg,
                Message::Event {
                    payload: EventPayload::FgOutput { data, .. },
                } if data.windows(expected_fragment.len()).any(|window| window == expected_fragment.as_slice())
            )),
            "expected FgOutput containing tty echo, got {msgs:?}"
        );

        let (detach_resp, mut msgs) =
            roundtrip_with_messages(&mut stream, 4, RequestPayload::FgDetach {}).await;
        assert!(
            matches!(detach_resp, ResponsePayload::Ok(OkPayload::Ack {})),
            "expected Ack for fg detach, got {detach_resp:?}"
        );

        if !msgs.iter().any(|msg| {
            matches!(
                msg,
                Message::Event {
                    payload: EventPayload::FgExited { id, reason, .. },
                } if id == &job_id && reason == "detached"
            )
        }) {
            msgs.extend(
                collect_until(&mut stream, Duration::from_secs(5), |msg| {
                    matches!(
                        msg,
                        Message::Event {
                            payload: EventPayload::FgExited { id, reason, .. },
                        } if id == &job_id && reason == "detached"
                    )
                })
                .await,
            );
        }
        assert!(
            msgs.iter().any(|msg| matches!(
                msg,
                Message::Event {
                    payload: EventPayload::FgExited { id, reason, .. },
                } if id == &job_id && reason == "detached"
            )),
            "expected detached fg exit event, got {msgs:?}"
        );

        let (reattach_resp, before_reattach) = roundtrip_with_messages(
            &mut stream,
            5,
            RequestPayload::FgAttach { id: job_id.clone() },
        )
        .await;
        let reattached = match reattach_resp {
            ResponsePayload::Ok(OkPayload::FgAttached(info)) => *info,
            other => panic!("expected foreground reattach, got {other:?}"),
        };
        assert_ne!(reattached.attachment_id, 0);
        assert!(
            reattached
                .snapshot
                .windows(expected_fragment.len())
                .any(|window| window == expected_fragment.as_slice()),
            "reattach snapshot missed retained tty history: {reattached:?}"
        );
        assert!(
            before_reattach.iter().all(|message| !matches!(
                message,
                Message::Event {
                    payload: EventPayload::FgOutput { id, .. },
                } if id == &job_id
            )),
            "legacy snapshot event crossed the attach response fence: {before_reattach:?}"
        );
        let legacy_snapshot = collect_until(&mut stream, Duration::from_secs(5), |message| {
            matches!(
                message,
                Message::Event {
                    payload: EventPayload::FgOutput {
                        id,
                        attachment_id: 0,
                        data,
                    },
                } if id == &job_id
                    && data.windows(expected_fragment.len())
                        .any(|window| window == expected_fragment.as_slice())
            )
        })
        .await;
        assert!(
            legacy_snapshot.iter().any(|message| matches!(
                message,
                Message::Event {
                    payload: EventPayload::FgOutput {
                        id,
                        attachment_id: 0,
                        data,
                    },
                } if id == &job_id
                    && data.windows(expected_fragment.len())
                        .any(|window| window == expected_fragment.as_slice())
            )),
            "old clients need the retained snapshot as a legacy epoch-0 event: {legacy_snapshot:?}"
        );

        let jobs_resp = roundtrip(
            &mut stream,
            6,
            RequestPayload::Eval {
                input: ":jobs".into(),
                mode: Mode::Job,
            },
        )
        .await;
        assert!(
            matches!(jobs_resp, ResponsePayload::Ok(OkPayload::JobList(_))),
            "expected JobList after fg detach, got {jobs_resp:?}"
        );

        let _ = roundtrip(
            &mut stream,
            7,
            RequestPayload::Eval {
                input: format!(":kill {job_id}"),
                mode: Mode::Job,
            },
        )
        .await;

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_named_session_shares_foreground_output_and_hands_off_control() {
    run_daemon_test(async {
        let env = TestEnv::new("named-session-shared-fg");
        let mut child = env.spawn_daemon();
        let cwd = default_test_session_cwd(&env.socket);

        let mut human =
            wait_for_socket_with_session(&env.socket, &mut child, "shared-fg-human", &cwd).await;
        let shared = create_named_session(&mut human, 1, "shared-foreground").await;

        let mut agent =
            wait_for_socket_with_session(&env.socket, &mut child, "shared-fg-agent", &cwd).await;
        assert!(matches!(
            attach_named_session(&mut agent, 1, &shared.id, false).await,
            ResponsePayload::Ok(OkPayload::SessionInfo(ref info)) if info.id == shared.id
        ));

        let mut foreign =
            wait_for_socket_with_session(&env.socket, &mut child, "foreign-fg-client", &cwd).await;
        let foreign_session = create_named_session(&mut foreign, 1, "foreign-foreground").await;
        assert_ne!(foreign_session.id, shared.id);

        let first_job = job_id_from_created(
            roundtrip(
                &mut human,
                2,
                RequestPayload::Eval {
                    input: "cat".into(),
                    mode: Mode::Job,
                },
            )
            .await,
        );
        let second_job = job_id_from_created(
            roundtrip(
                &mut human,
                3,
                RequestPayload::Eval {
                    input: "cat".into(),
                    mode: Mode::Job,
                },
            )
            .await,
        );
        wait_for_job_status(&mut human, 10, &first_job, |job| {
            job.status == JobStatus::Running
        })
        .await;
        wait_for_job_status(&mut human, 20, &second_job, |job| {
            job.status == JobStatus::Running
        })
        .await;

        match roundtrip(
            &mut foreign,
            2,
            RequestPayload::FgWatch {
                id: first_job.clone(),
            },
        )
        .await
        {
            ResponsePayload::Err { code, .. } => assert_eq!(code, ipc::error_code::NOT_FOUND),
            other => panic!("foreign named session must not watch {first_job}, got {other:?}"),
        }

        let (controller_response, before_controller_response) = roundtrip_with_messages(
            &mut human,
            30,
            RequestPayload::FgAttach {
                id: first_job.clone(),
            },
        )
        .await;
        let controller_info = match controller_response {
            ResponsePayload::Ok(OkPayload::FgAttached(info)) => *info,
            other => panic!("expected controller FgAttached response, got {other:?}"),
        };
        assert_eq!(controller_info.id, first_job);
        assert_ne!(controller_info.attachment_id, 0);
        assert_eq!(controller_info.role, ForegroundRole::Controller);
        assert!(!controller_info.control_available);
        assert!(
            before_controller_response.iter().all(|message| !matches!(
                message,
                Message::Event {
                    payload: EventPayload::FgOutput { id, .. },
                } if id == &first_job
            )),
            "foreground output crossed the controller attach response fence: {before_controller_response:?}"
        );

        // Seed the ring before the observer attaches. The observer must receive
        // this as response snapshot data, never as an event before FgAttached.
        let snapshot_input = b"snapshot-before-observer\n";
        let snapshot_marker = b"snapshot-before-observer";
        let (snapshot_input, snapshot_events) = roundtrip_with_messages(
            &mut human,
            31,
            RequestPayload::FgInput {
                data: snapshot_input.to_vec(),
            },
        )
        .await;
        assert!(matches!(
            snapshot_input,
            ResponsePayload::Ok(OkPayload::Ack {})
        ));
        let _ = wait_for_foreground_output(
            &mut human,
            &first_job,
            snapshot_marker,
            snapshot_events,
        )
        .await;

        let (observer_response, before_observer_response) = roundtrip_with_messages(
            &mut agent,
            2,
            RequestPayload::FgWatch {
                id: first_job.clone(),
            },
        )
        .await;
        let observer_info = match observer_response {
            ResponsePayload::Ok(OkPayload::FgAttached(info)) => *info,
            other => panic!("expected observer FgAttached response, got {other:?}"),
        };
        assert_eq!(observer_info.id, first_job);
        assert_ne!(observer_info.attachment_id, 0);
        assert_ne!(observer_info.attachment_id, controller_info.attachment_id);
        assert_eq!(observer_info.role, ForegroundRole::Observer);
        assert!(!observer_info.control_available);
        assert!(
            observer_info
                .snapshot
                .windows(snapshot_marker.len())
                .any(|window| window == snapshot_marker),
            "observer snapshot missed seeded PTY output: {observer_info:?}"
        );
        assert!(
            before_observer_response.iter().all(|message| !matches!(
                message,
                Message::Event {
                    payload: EventPayload::FgOutput { id, .. },
                } if id == &first_job
            )),
            "FgOutput arrived before observer FgAttached: {before_observer_response:?}"
        );

        match roundtrip(
            &mut human,
            32,
            RequestPayload::FgWatch {
                id: second_job.clone(),
            },
        )
        .await
        {
            ResponsePayload::Err { code, message } => {
                assert_eq!(code, ipc::error_code::INVALID_STATE);
                assert!(message.contains("already foreground-attached"));
                assert!(message.contains(&first_job));
            }
            other => panic!("one client must not attach a second job, got {other:?}"),
        }

        for (request_id, request, operation) in [
            (
                3,
                RequestPayload::FgInput {
                    data: b"observer-input-must-fail\n".to_vec(),
                },
                "FgInput",
            ),
            (
                4,
                RequestPayload::FgResize { cols: 100, rows: 40 },
                "FgResize",
            ),
            (
                5,
                RequestPayload::Eval {
                    input: format!(":send {first_job} observer-send-must-fail"),
                    mode: Mode::Job,
                },
                ":send",
            ),
        ] {
            match roundtrip(&mut agent, request_id, request).await {
                ResponsePayload::Err { code, .. } => {
                    assert_eq!(code, ipc::error_code::INVALID_STATE, "{operation}")
                }
                other => panic!("observer {operation} must be rejected, got {other:?}"),
            }
        }

        // The first output produced after FgAttached must be a live, job-scoped
        // event delivered to both controller and observer.
        let shared_input_bytes = b"shared-live-before-handoff\n";
        let shared_marker = b"shared-live-before-handoff";
        let (shared_input, human_live_events) = roundtrip_with_messages(
            &mut human,
            33,
            RequestPayload::FgInput {
                data: shared_input_bytes.to_vec(),
            },
        )
        .await;
        assert!(matches!(
            shared_input,
            ResponsePayload::Ok(OkPayload::Ack {})
        ));
        let _ = wait_for_foreground_output(
            &mut human,
            &first_job,
            shared_marker,
            human_live_events,
        )
        .await;
        let _ = wait_for_foreground_output(
            &mut agent,
            &first_job,
            shared_marker,
            Vec::new(),
        )
        .await;

        match roundtrip(
            &mut human,
            34,
            RequestPayload::FgReleaseControl {},
        )
        .await
        {
            ResponsePayload::Ok(OkPayload::FgRoleChanged {
                id,
                attachment_id,
                role,
                control_available,
            }) => {
                assert_eq!(id, first_job);
                assert_eq!(attachment_id, controller_info.attachment_id);
                assert_eq!(role, ForegroundRole::Observer);
                assert!(control_available);
            }
            other => panic!("expected controller release role update, got {other:?}"),
        }

        match roundtrip(&mut agent, 6, RequestPayload::FgClaimControl {}).await {
            ResponsePayload::Ok(OkPayload::FgRoleChanged {
                id,
                attachment_id,
                role,
                control_available,
            }) => {
                assert_eq!(id, first_job);
                assert_eq!(attachment_id, observer_info.attachment_id);
                assert_eq!(role, ForegroundRole::Controller);
                assert!(!control_available);
            }
            other => panic!("expected observer control claim, got {other:?}"),
        }

        match roundtrip(
            &mut human,
            35,
            RequestPayload::FgInput {
                data: b"old-controller-must-fail\n".to_vec(),
            },
        )
        .await
        {
            ResponsePayload::Err { code, .. } => {
                assert_eq!(code, ipc::error_code::INVALID_STATE)
            }
            other => panic!("released controller must lose input authority, got {other:?}"),
        }

        let handoff_input_bytes = b"shared-live-after-handoff\n";
        let handoff_marker = b"shared-live-after-handoff";
        let (handoff_input, agent_live_events) = roundtrip_with_messages(
            &mut agent,
            7,
            RequestPayload::FgInput {
                data: handoff_input_bytes.to_vec(),
            },
        )
        .await;
        assert!(matches!(
            handoff_input,
            ResponsePayload::Ok(OkPayload::Ack {})
        ));
        let _ = wait_for_foreground_output(
            &mut agent,
            &first_job,
            handoff_marker,
            agent_live_events,
        )
        .await;
        let _ = wait_for_foreground_output(
            &mut human,
            &first_job,
            handoff_marker,
            Vec::new(),
        )
        .await;

        assert!(matches!(
            roundtrip(&mut human, 36, RequestPayload::FgDetach {}).await,
            ResponsePayload::Ok(OkPayload::Ack {})
        ));
        let (reattach_response, _) = roundtrip_with_messages(
            &mut human,
            37,
            RequestPayload::FgWatch {
                id: first_job.clone(),
            },
        )
        .await;
        let reattached_info = match reattach_response {
            ResponsePayload::Ok(OkPayload::FgAttached(info)) => *info,
            other => panic!("expected observer reattach response, got {other:?}"),
        };
        assert_eq!(reattached_info.id, first_job);
        assert_eq!(reattached_info.role, ForegroundRole::Observer);
        assert!(!reattached_info.control_available);
        assert!(
            reattached_info.attachment_id > controller_info.attachment_id,
            "reattach must allocate a fresh epoch: first={controller_info:?}, reattached={reattached_info:?}"
        );

        shutdown_daemon(&mut human, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_jobs_run_in_tty() {
    run_daemon_test(async {
        let env = TestEnv::new("tty");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        let resp = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input: r#"/bin/sh -c "if [ -t 0 ]; then printf tty; else printf notty; fi""#.into(),
                mode: Mode::Job,
            },
        )
        .await;

        let job_id = match resp {
            ResponsePayload::Ok(OkPayload::JobCreated { job_id, .. }) => job_id,
            other => panic!("expected JobCreated, got {other:?}"),
        };

        let status = wait_for_job_terminal(&mut stream, 2, &job_id).await;
        assert_eq!(status, JobStatus::Done);

        let out_resp = roundtrip(
            &mut stream,
            3,
            RequestPayload::Eval {
                input: format!(":out {job_id}"),
                mode: Mode::Job,
            },
        )
        .await;
        match out_resp {
            ResponsePayload::Ok(OkPayload::Output { data, .. }) => {
                assert!(
                    data.contains("tty"),
                    "expected PTY-backed job output, got {data:?}"
                );
                assert!(
                    !data.contains("notty"),
                    "job should not see a pipe-backed stdin/stdout, got {data:?}"
                );
            }
            other => panic!("expected Output, got {other:?}"),
        }

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_job_command_expands_tilde_and_env_vars() {
    run_daemon_test(async {
        let env = TestEnv::new("expand");
        let bin_dir = env.root.join("bin");
        fs::create_dir_all(&bin_dir).expect("create test bin dir");

        let script_path = bin_dir.join("show-home.sh");
        fs::write(&script_path, "#!/bin/sh\nprintf '%s|%s' \"$1\" \"$2\"\n")
            .expect("write test script");
        let mut permissions = fs::metadata(&script_path)
            .expect("stat test script")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&script_path, permissions).expect("chmod test script");

        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        let resp = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input: "~/bin/show-home.sh ~ $HOME".into(),
                mode: Mode::Job,
            },
        )
        .await;

        let job_id = match resp {
            ResponsePayload::Ok(OkPayload::JobCreated { job_id, .. }) => job_id,
            other => panic!("expected JobCreated, got {other:?}"),
        };

        let status = wait_for_job_terminal(&mut stream, 2, &job_id).await;
        assert_eq!(status, JobStatus::Done);

        let out_resp = roundtrip(
            &mut stream,
            3,
            RequestPayload::Eval {
                input: format!(":out {job_id}"),
                mode: Mode::Job,
            },
        )
        .await;

        let expected_home = env.root.display().to_string();
        match out_resp {
            ResponsePayload::Ok(OkPayload::Output { data, .. }) => {
                assert!(
                    data.contains(&format!("{expected_home}|{expected_home}")),
                    "expected expanded tilde/env output, got {data:?}"
                );
            }
            other => panic!("expected Output, got {other:?}"),
        }

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cron_add_and_list() {
    run_daemon_test(async {
        let env = TestEnv::new("cron");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        let resp = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input: ":cron every 1h echo hello".into(),
                mode: Mode::Job,
            },
        )
        .await;

        let cron_id = match &resp {
            ResponsePayload::Ok(OkPayload::CronAdded { cron_id }) => cron_id.clone(),
            other => panic!("expected CronAdded, got {other:?}"),
        };
        assert!(cron_id.starts_with('C'), "unexpected cron id: {cron_id}");

        let list_resp = roundtrip(
            &mut stream,
            2,
            RequestPayload::Eval {
                input: ":crons".into(),
                mode: Mode::Job,
            },
        )
        .await;

        match &list_resp {
            ResponsePayload::Ok(OkPayload::CronList(list)) => {
                assert!(!list.is_empty(), "cron list should not be empty");
                let found = list.iter().any(|c| c.id == cron_id);
                assert!(found, "cron {cron_id} not in list: {list:?}");
                let entry = list.iter().find(|c| c.id == cron_id).unwrap();
                assert_eq!(
                    entry.status,
                    cue_core::cron::CronStatus::Scheduled,
                    "cron should be scheduled"
                );
                assert_eq!(entry.schedule, "every 1h");
                assert_eq!(entry.command, "echo hello");
            }
            other => panic!("expected CronList, got {other:?}"),
        }

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cron_cwd_mode_param_is_restored_scope_state() {
    run_daemon_test(async {
        let env = TestEnv::new("cron-cwd-scope");
        let repo = env.root.join("repo");
        std::fs::create_dir_all(&repo).expect("create repo dir");

        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        let resp = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input: format!(":cron(cwd={}) every 1s /bin/pwd", repo.display()),
                mode: Mode::Job,
            },
        )
        .await;
        assert!(matches!(resp, ResponsePayload::Ok(OkPayload::CronAdded { .. })));

        let initial = wait_for_done_job_matching(
            &mut stream,
            10,
            &std::collections::HashSet::new(),
            |job| job.pipeline == "/bin/pwd",
        )
        .await;
        let out = roundtrip(
            &mut stream,
            30,
            RequestPayload::Eval {
                input: format!(":out {}", initial.id),
                mode: Mode::Job,
            },
        )
        .await;
        assert!(matches!(
            out,
            ResponsePayload::Ok(OkPayload::Output { ref data, .. }) if data.trim() == repo.display().to_string()
        ));

        let before_restart = match roundtrip(
            &mut stream,
            31,
            RequestPayload::Eval {
                input: ":jobs".into(),
                mode: Mode::Job,
            },
        )
        .await
        {
            ResponsePayload::Ok(OkPayload::JobList(list)) => {
                list.into_iter().map(|job| job.id).collect::<std::collections::HashSet<_>>()
            }
            other => panic!("expected JobList, got {other:?}"),
        };

        shutdown_daemon(&mut stream, &mut child).await;

        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;
        let restored = wait_for_done_job_matching(&mut stream, 40, &before_restart, |job| {
            job.pipeline == "/bin/pwd"
        })
        .await;
        let out = roundtrip(
            &mut stream,
            60,
            RequestPayload::Eval {
                input: format!(":out {}", restored.id),
                mode: Mode::Job,
            },
        )
        .await;
        assert!(matches!(
            out,
            ResponsePayload::Ok(OkPayload::Output { ref data, .. }) if data.trim() == repo.display().to_string()
        ));

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cron_mode_bare_input_adds_cron() {
    run_daemon_test(async {
        let env = TestEnv::new("cron-mode");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        let resp = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input: "every 15m echo hello".into(),
                mode: Mode::Cron,
            },
        )
        .await;

        let cron_id = match &resp {
            ResponsePayload::Ok(OkPayload::CronAdded { cron_id }) => cron_id.clone(),
            other => panic!("expected CronAdded, got {other:?}"),
        };
        assert!(cron_id.starts_with('C'), "unexpected cron id: {cron_id}");

        let list_resp = roundtrip(
            &mut stream,
            2,
            RequestPayload::Eval {
                input: ":crons".into(),
                mode: Mode::Job,
            },
        )
        .await;

        match &list_resp {
            ResponsePayload::Ok(OkPayload::CronList(list)) => {
                let entry = list.iter().find(|cron| cron.id == cron_id).unwrap();
                assert_eq!(entry.schedule, "every 15m");
                assert_eq!(entry.command, "echo hello");
                assert_eq!(
                    entry.status,
                    cue_core::cron::CronStatus::Scheduled,
                    "cron should be scheduled"
                );
            }
            other => panic!("expected CronList, got {other:?}"),
        }

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_bare_question_returns_current_mode_help() {
    run_daemon_test(async {
        let env = TestEnv::new("mode-help");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        for (request_id, mode, expected) in
            [(1, Mode::Job, "JOB mode"), (2, Mode::Cron, "CRON mode")]
        {
            let resp = roundtrip(
                &mut stream,
                request_id,
                RequestPayload::Eval {
                    input: "?".into(),
                    mode,
                },
            )
            .await;

            match resp {
                ResponsePayload::Ok(OkPayload::EvalText { text }) => {
                    assert!(
                        text.contains(expected),
                        "expected `{expected}` in help text, got {text:?}"
                    );
                }
                other => panic!("expected EvalText help response, got {other:?}"),
            }
        }

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_gateway_stdio_bridge_shares_state_and_keeps_output_subscriptions_per_client() {
    run_daemon_test(async {
        let env = TestEnv::new("bridge-shared-state");
        let script_path = env.root.join("delayed-output.sh");
        write_executable_script(
            &script_path,
            "#!/bin/sh\nsleep 1\nprintf 'bridge-output\\n'\n",
        );

        let mut child = env.spawn_daemon();
        let mut local = wait_for_socket(&env.socket, &mut child).await;
        let (mut remote, remote_relay) = connect_bridge(&env.socket).await;

        subscribe(&mut local, 1, vec!["jobs"]).await;
        subscribe(&mut remote, 1, vec!["jobs"]).await;

        let create_resp = roundtrip(
            &mut remote,
            2,
            RequestPayload::Eval {
                input: script_path.display().to_string(),
                mode: Mode::Job,
            },
        )
        .await;

        let job_id = match create_resp {
            ResponsePayload::Ok(OkPayload::JobCreated { job_id, .. }) => job_id,
            other => panic!("expected JobCreated from bridged client, got {other:?}"),
        };

        subscribe(&mut local, 2, vec![format!("output:{job_id}")]).await;

        let local_msgs = collect_until(&mut local, Duration::from_secs(5), |msg| {
            matches!(
                msg,
                Message::Event {
                    payload: EventPayload::JobStateChanged { job_id: id, new_state, .. },
                } if id == &job_id && new_state.is_terminal()
            )
        })
        .await;
        assert!(
            local_msgs.iter().any(|msg| matches!(
                msg,
                Message::Event {
                    payload: EventPayload::JobCreated { job_id: id, .. },
                } if id == &job_id
            ) || matches!(
                msg,
                Message::Event {
                    payload: EventPayload::JobStateChanged { job_id: id, .. },
                } if id == &job_id
            )),
            "local client should observe bridged job events, got {local_msgs:?}"
        );
        assert!(
            local_msgs.iter().any(|msg| matches!(
                msg,
                Message::Event {
                    payload: EventPayload::OutputChunk { id, data, .. },
                } if id == &job_id && data.contains("bridge-output")
            )),
            "local client should receive subscribed output chunks, got {local_msgs:?}"
        );

        let remote_msgs = collect_until(&mut remote, Duration::from_secs(5), |msg| {
            matches!(
                msg,
                Message::Event {
                    payload: EventPayload::JobStateChanged { job_id: id, new_state, .. },
                } if id == &job_id && new_state.is_terminal()
            )
        })
        .await;
        assert!(
            remote_msgs.iter().any(|msg| matches!(
                msg,
                Message::Event {
                    payload: EventPayload::JobStateChanged { job_id: id, .. },
                } if id == &job_id
            )),
            "bridged client should receive shared job events, got {remote_msgs:?}"
        );
        assert!(
            remote_msgs.iter().all(|msg| !matches!(
                msg,
                Message::Event {
                    payload: EventPayload::OutputChunk { id, .. },
                } if id == &job_id
            )),
            "bridged client should not receive output without subscribing, got {remote_msgs:?}"
        );

        drop(remote);
        timeout(Duration::from_secs(2), remote_relay)
            .await
            .expect("bridged relay timed out")
            .expect("bridged relay panicked")
            .expect("bridged relay failed");

        shutdown_daemon(&mut local, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_gateway_stdio_bridge_releases_fg_owner_after_disconnect() {
    run_daemon_test(async {
        let env = TestEnv::new("bridge-fg-release");
        let mut child = env.spawn_daemon();
        let mut local = wait_for_socket(&env.socket, &mut child).await;
        let (mut remote, remote_relay) = connect_bridge(&env.socket).await;

        let job_resp = roundtrip(
            &mut local,
            1,
            RequestPayload::Eval {
                input: "cat".into(),
                mode: Mode::Job,
            },
        )
        .await;

        let job_id = match job_resp {
            ResponsePayload::Ok(OkPayload::JobCreated { job_id, .. }) => job_id,
            other => panic!("expected JobCreated, got {other:?}"),
        };

        let attach_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        let mut remote_request_id = 2;
        loop {
            let attach_resp = roundtrip(
                &mut remote,
                remote_request_id,
                RequestPayload::FgAttach { id: job_id.clone() },
            )
            .await;
            remote_request_id += 1;

            match attach_resp {
                ResponsePayload::Ok(OkPayload::FgAttached(_)) => break,
                ResponsePayload::Err { message, .. } if message.contains("is not running") => {
                    assert!(
                        tokio::time::Instant::now() < attach_deadline,
                        "job {job_id} never became attachable for bridged client"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                other => panic!("unexpected bridged attach response: {other:?}"),
            }
        }

        let local_attach_resp = roundtrip(
            &mut local,
            2,
            RequestPayload::FgAttach { id: job_id.clone() },
        )
        .await;
        match local_attach_resp {
            ResponsePayload::Err { code, message } => {
                assert_eq!(code, ipc::error_code::INVALID_STATE);
                assert!(
                    message.contains("foreground control is already held"),
                    "expected controller-busy foreground rejection, got {message:?}"
                );
            }
            other => {
                panic!("expected fg attach rejection while bridged client owns fg, got {other:?}")
            }
        }

        drop(remote);
        timeout(Duration::from_secs(2), remote_relay)
            .await
            .expect("bridged relay timed out")
            .expect("bridged relay panicked")
            .expect("bridged relay failed");

        let retry_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        let mut local_request_id = 3;
        loop {
            let attach_resp = roundtrip(
                &mut local,
                local_request_id,
                RequestPayload::FgAttach { id: job_id.clone() },
            )
            .await;
            local_request_id += 1;

            match attach_resp {
                ResponsePayload::Ok(OkPayload::FgAttached(_)) => break,
                ResponsePayload::Err { message, .. }
                    if message.contains("foreground control is already held") =>
                {
                    assert!(
                        tokio::time::Instant::now() < retry_deadline,
                        "fg ownership was not released after bridged disconnect"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                other => {
                    panic!("unexpected fg attach response after bridged disconnect: {other:?}")
                }
            }
        }

        let detach_resp =
            roundtrip(&mut local, local_request_id, RequestPayload::FgDetach {}).await;
        assert!(
            matches!(detach_resp, ResponsePayload::Ok(OkPayload::Ack {})),
            "expected Ack after reattached detach, got {detach_resp:?}"
        );

        shutdown_daemon(&mut local, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_err_command_returns_output_for_pty_job() {
    // Single-process jobs still run in PTY mode (stdout and stderr are merged).
    // `:err J<n>` should return the combined output prefixed with the PTY notice.
    run_daemon_test(async {
        let env = TestEnv::new("err-pty");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        // Run a simple job that writes to stdout.
        let resp = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input: "echo hello-from-err-test".into(),
                mode: Mode::Job,
            },
        )
        .await;
        let job_id = match resp {
            ResponsePayload::Ok(OkPayload::JobCreated { job_id, .. }) => job_id,
            other => panic!("expected JobCreated, got {other:?}"),
        };

        let status = wait_for_job_terminal(&mut stream, 2, &job_id).await;
        assert_eq!(status, JobStatus::Done);

        // :err should return output with the PTY notice.
        let err_resp = roundtrip(
            &mut stream,
            10,
            RequestPayload::Eval {
                input: format!(":err {job_id}"),
                mode: Mode::Job,
            },
        )
        .await;
        match err_resp {
            ResponsePayload::Ok(OkPayload::Output { id, data, .. }) => {
                assert_eq!(id, job_id);
                assert!(
                    data.contains("[PTY:"),
                    "expected PTY notice in :err output, got: {data:?}"
                );
                assert!(
                    data.contains("hello-from-err-test"),
                    "expected job output in :err response, got: {data:?}"
                );
            }
            other => panic!("expected Output for :err, got {other:?}"),
        }

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_native_stdout_pipeline_preserves_arguments_and_real_stderr() {
    run_daemon_test(async {
        let env = TestEnv::new("pipe-stdout");
        let producer = env.root.join("producer.sh");
        let consumer = env.root.join("consumer.sh");
        write_executable_script(
            &producer,
            "#!/bin/sh\nprintf 'out:%s\\n' \"$1\"\nprintf 'err:%s\\n' \"$1\" >&2\n",
        );
        write_executable_script(
            &consumer,
            "#!/bin/sh\nwhile IFS= read -r line; do printf 'pipe:%s\\n' \"$line\"; done\n",
        );

        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;
        let input = format!(
            "{} 'hello world;semi' |> {}",
            producer.display(),
            consumer.display()
        );
        let resp = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input,
                mode: Mode::Job,
            },
        )
        .await;
        let job_id = match resp {
            ResponsePayload::Ok(OkPayload::JobCreated { job_id, .. }) => job_id,
            other => panic!("expected JobCreated, got {other:?}"),
        };

        let status = wait_for_job_terminal(&mut stream, 2, &job_id).await;
        assert_eq!(status, JobStatus::Done);

        let out_resp = roundtrip(
            &mut stream,
            3,
            RequestPayload::Eval {
                input: format!(":out {job_id}"),
                mode: Mode::Job,
            },
        )
        .await;
        match out_resp {
            ResponsePayload::Ok(OkPayload::Output { data, .. }) => {
                assert!(
                    data.contains("pipe:out:hello world;semi"),
                    "expected piped stdout with literal arg, got {data:?}"
                );
                assert!(
                    !data.contains("[PTY:"),
                    "native pipeline stdout should not fall back to PTY output, got {data:?}"
                );
            }
            other => panic!("expected Output, got {other:?}"),
        }

        let err_resp = roundtrip(
            &mut stream,
            4,
            RequestPayload::Eval {
                input: format!(":err {job_id}"),
                mode: Mode::Job,
            },
        )
        .await;
        match err_resp {
            ResponsePayload::Ok(OkPayload::Output { data, .. }) => {
                assert!(
                    data.contains("err:hello world;semi"),
                    "expected real stderr output, got {data:?}"
                );
                assert!(
                    !data.contains("[PTY:"),
                    "native pipeline stderr should not include PTY notice, got {data:?}"
                );
            }
            other => panic!("expected stderr Output, got {other:?}"),
        }

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_native_stderr_only_pipeline_keeps_stdout_outside_pipe() {
    run_daemon_test(async {
        let env = TestEnv::new("pipe-stderr-only");
        let producer = env.root.join("producer.sh");
        let consumer = env.root.join("consumer.sh");
        write_executable_script(
            &producer,
            "#!/bin/sh\nprintf 'out:%s\\n' \"$1\"\nprintf 'err:%s\\n' \"$1\" >&2\n",
        );
        write_executable_script(
            &consumer,
            "#!/bin/sh\nwhile IFS= read -r line; do printf 'pipe:%s\\n' \"$line\"; done\n",
        );

        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;
        let input = format!(
            "{} 'semi;colon' |!> {}",
            producer.display(),
            consumer.display()
        );
        let resp = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input,
                mode: Mode::Job,
            },
        )
        .await;
        let job_id = match resp {
            ResponsePayload::Ok(OkPayload::JobCreated { job_id, .. }) => job_id,
            other => panic!("expected JobCreated, got {other:?}"),
        };

        let status = wait_for_job_terminal(&mut stream, 2, &job_id).await;
        assert_eq!(status, JobStatus::Done);

        let out_resp = roundtrip(
            &mut stream,
            3,
            RequestPayload::Eval {
                input: format!(":out {job_id}"),
                mode: Mode::Job,
            },
        )
        .await;
        match out_resp {
            ResponsePayload::Ok(OkPayload::Output { data, .. }) => {
                assert!(
                    data.contains("out:semi;colon"),
                    "expected producer stdout outside the pipe, got {data:?}"
                );
                assert!(
                    data.contains("pipe:err:semi;colon"),
                    "expected only stderr to reach the consumer, got {data:?}"
                );
            }
            other => panic!("expected Output, got {other:?}"),
        }

        let err_resp = roundtrip(
            &mut stream,
            4,
            RequestPayload::Eval {
                input: format!(":err {job_id}"),
                mode: Mode::Job,
            },
        )
        .await;
        match err_resp {
            ResponsePayload::Ok(OkPayload::Output { data, .. }) => {
                assert!(
                    data.is_empty(),
                    "stderr-only pipeline should not leak stderr after piping, got {data:?}"
                );
            }
            other => panic!("expected stderr Output, got {other:?}"),
        }

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_wrapper_allowlist_wraps_single_pipeline_and_logical_jobs() {
    run_daemon_test(async {
        let env = TestEnv::new("wrapper-allowlist");
        let wrapper_log = env.root.join("wrapper.log");
        let wrapper = env.root.join("rtk-test");
        write_executable_script(
            &wrapper,
            &format!(
                "#!/bin/sh\nprintf '%s\\n' \"$1\" >> {}\nexec \"$@\"\n",
                wrapper_log.display()
            ),
        );
        write_daemon_config(
            &env,
            &format!(
                r#"
[wrapper]
enabled = true
binary = "{}"

[wrapper.allowlist]
commands = ["printf", "sed", "true"]
"#,
                wrapper.display()
            ),
        );

        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        let single = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input: ":run(pty=false) printf single".into(),
                mode: Mode::Job,
            },
        )
        .await;
        let single_job = job_id_from_created(single);
        assert_eq!(
            wait_for_job_terminal(&mut stream, 2, &single_job).await,
            JobStatus::Done
        );

        let pipeline = roundtrip(
            &mut stream,
            3,
            RequestPayload::Eval {
                input: ":run(pty=false) printf pipe |> sed s/pipe/wrapped/".into(),
                mode: Mode::Job,
            },
        )
        .await;
        let pipeline_job = job_id_from_created(pipeline);
        assert_eq!(
            wait_for_job_terminal(&mut stream, 4, &pipeline_job).await,
            JobStatus::Done
        );

        let logical = roundtrip(
            &mut stream,
            5,
            RequestPayload::Eval {
                input: ":run(pty=false) true && printf logical".into(),
                mode: Mode::Job,
            },
        )
        .await;
        let logical_job = job_id_from_created(logical);
        assert_eq!(
            wait_for_job_terminal(&mut stream, 6, &logical_job).await,
            JobStatus::Done
        );

        let log = fs::read_to_string(&wrapper_log).expect("read wrapper log");
        assert!(
            log.contains("printf\n"),
            "expected printf wrap, got {log:?}"
        );
        assert!(log.contains("sed\n"), "expected sed wrap, got {log:?}");
        assert!(log.contains("true\n"), "expected true wrap, got {log:?}");

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_wrapper_non_allowlisted_and_param_disabled_do_not_wrap() {
    run_daemon_test(async {
        let env = TestEnv::new("wrapper-skip");
        let wrapper_log = env.root.join("wrapper.log");
        let wrapper = env.root.join("rtk-test");
        write_executable_script(
            &wrapper,
            &format!(
                "#!/bin/sh\nprintf '%s\\n' \"$1\" >> {}\nexec \"$@\"\n",
                wrapper_log.display()
            ),
        );
        write_daemon_config(
            &env,
            &format!(
                r#"
[wrapper]
enabled = true
binary = "{}"

[wrapper.allowlist]
commands = ["printf"]
"#,
                wrapper.display()
            ),
        );

        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        let non_allowlisted = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input: ":run(pty=false) echo plain".into(),
                mode: Mode::Job,
            },
        )
        .await;
        let non_allowlisted_job = job_id_from_created(non_allowlisted);
        assert_eq!(
            wait_for_job_terminal(&mut stream, 2, &non_allowlisted_job).await,
            JobStatus::Done
        );

        let disabled = roundtrip(
            &mut stream,
            3,
            RequestPayload::Eval {
                input: ":run(pty=false, wrapper=false) printf disabled".into(),
                mode: Mode::Job,
            },
        )
        .await;
        let disabled_job = job_id_from_created(disabled);
        assert_eq!(
            wait_for_job_terminal(&mut stream, 4, &disabled_job).await,
            JobStatus::Done
        );

        let log = fs::read_to_string(&wrapper_log).unwrap_or_default();
        assert!(log.is_empty(), "wrapper should not have run, got {log:?}");

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_scopes_returns_scope_list() {
    run_daemon_test(async {
        let env = TestEnv::new("scopes-list");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        let resp = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input: ":scopes".into(),
                mode: Mode::Job,
            },
        )
        .await;

        match resp {
            ResponsePayload::Ok(OkPayload::ScopeList(scopes)) => {
                assert!(
                    !scopes.is_empty(),
                    "expected at least one scope, got empty list"
                );
                for scope in &scopes {
                    assert!(!scope.hash.is_empty(), "scope hash should not be empty");
                }
            }
            other => panic!("expected ScopeList response, got {other:?}"),
        }

        let resp2 = roundtrip(
            &mut stream,
            2,
            RequestPayload::Eval {
                input: ":scope list".into(),
                mode: Mode::Job,
            },
        )
        .await;
        assert!(
            matches!(resp2, ResponsePayload::Ok(OkPayload::ScopeList(_))),
            "`:scope list` should also return ScopeList, got {resp2:?}"
        );

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_config_show_returns_daemon_info() {
    run_daemon_test(async {
        let env = TestEnv::new("config-show");
        let mut child = env.spawn_daemon();
        let mut stream = wait_for_socket(&env.socket, &mut child).await;

        let resp = roundtrip(
            &mut stream,
            1,
            RequestPayload::Eval {
                input: ":config".into(),
                mode: Mode::Job,
            },
        )
        .await;

        match resp {
            ResponsePayload::Ok(OkPayload::EvalText { text }) => {
                assert!(
                    text.contains("retention.max_job_history"),
                    "expected daemon config output, got: {text:?}"
                );
            }
            other => panic!("expected EvalText config response, got {other:?}"),
        }

        let resp2 = roundtrip(
            &mut stream,
            2,
            RequestPayload::Eval {
                input: ":config show".into(),
                mode: Mode::Job,
            },
        )
        .await;
        assert!(
            matches!(resp2, ResponsePayload::Ok(OkPayload::EvalText { .. })),
            "`:config show` should return EvalText, got {resp2:?}"
        );

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await;
}
