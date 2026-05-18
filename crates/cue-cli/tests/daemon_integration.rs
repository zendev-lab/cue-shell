//! End-to-end integration tests for the `cued` daemon.
//!
//! Each test spawns a real `cued start --fg --socket <unique>` process, connects
//! over the Unix domain socket, exercises the IPC protocol, then shuts down.
//!
//! Environment isolation: every test sets `XDG_RUNTIME_DIR`, `XDG_DATA_HOME`,
//! `XDG_STATE_HOME`, and `XDG_CONFIG_HOME` to a per-test temp directory so the
//! daemon uses its own PID file, database, and socket — never colliding with a
//! real running `cued` instance.

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
use tokio::task::JoinHandle;
use tokio::time::timeout;

use cue_core::ipc::{self, EventPayload, Message, OkPayload, RequestPayload, ResponsePayload};
use cue_core::job::JobStatus;
use cue_core::mode::Mode;

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Per-test timeout to prevent hangs.
const TEST_TIMEOUT: Duration = Duration::from_secs(15);

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
    let (client_writer, relay_input) = duplex(16 * 1024);
    let (relay_output, client_reader) = duplex(16 * 1024);
    let socket = UnixStream::connect(socket)
        .await
        .expect("connect bridge socket");
    let relay = tokio::spawn(cue_daemon::gateway_stdio::relay(
        relay_input,
        relay_output,
        socket,
    ));
    (SplitStream::new(client_reader, client_writer), relay)
}

fn write_executable_script(path: &Path, body: &str) {
    fs::write(path, body).expect("write test script");
    let mut permissions = fs::metadata(path).expect("stat test script").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("chmod test script");
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

/// Build a `Request` envelope.
fn request(id: u32, payload: RequestPayload) -> Message {
    Message::Request { id, payload }
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

/// Poll `:jobs` until `job_id` reaches a terminal state.
async fn wait_for_job_terminal<S>(stream: &mut S, mut request_id: u32, job_id: &str) -> JobStatus
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
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
        request_id += 1;

        if let ResponsePayload::Ok(OkPayload::JobList(list)) = resp
            && let Some(job) = list.into_iter().find(|job| job.id == job_id)
            && job.status.is_terminal()
        {
            return job.status;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "job {job_id} did not become terminal in time"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
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
    timeout(TEST_TIMEOUT, async {
        let env = TestEnv::new("lifecycle");
        let mut child = env.spawn_daemon();

        // Connect and ping.
        let mut stream = wait_for_socket(&env.socket, &mut child).await;
        let resp = roundtrip(&mut stream, 1, RequestPayload::Ping {}).await;
        assert!(
            matches!(resp, ResponsePayload::Ok(OkPayload::Pong {})),
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
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_foreground_sigint_exits_promptly() {
    timeout(TEST_TIMEOUT, async {
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
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_simple_job_execution() {
    timeout(TEST_TIMEOUT, async {
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
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_restart_restores_jobs_and_scope_head() {
    timeout(TEST_TIMEOUT, async {
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
                let actual = std::fs::canonicalize(data.trim()).expect("canonicalize restored cwd");
                let expected =
                    std::fs::canonicalize(&persisted_cwd).expect("canonicalize expected cwd");
                assert_eq!(actual, expected);
            }
            other => panic!("expected Output, got {other:?}"),
        }

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_restart_jobs_merge_ambient_path_into_restored_scope() {
    timeout(TEST_TIMEOUT, async {
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
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_env_set_prints_deduped_scope_side_effects() {
    timeout(TEST_TIMEOUT, async {
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
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cd_rejects_missing_directory() {
    timeout(TEST_TIMEOUT, async {
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
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_spawn_failure_does_not_reuse_stale_output_log() {
    timeout(TEST_TIMEOUT, async {
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
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_chain_execution() {
    timeout(TEST_TIMEOUT, async {
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
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_job_logical_operators_stay_single_job() {
    timeout(TEST_TIMEOUT, async {
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
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_chain_parallel_operator_uses_triple_pipe() {
    timeout(TEST_TIMEOUT, async {
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
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_job_local_cd_does_not_update_global_scope_by_default() {
    timeout(TEST_TIMEOUT, async {
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
                let expected = std::fs::canonicalize(std::env::current_dir().expect("cwd"))
                    .expect("canonicalize initial cwd");
                assert_eq!(actual, expected);
            }
            other => panic!("expected Output, got {other:?}"),
        }

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_job_kill() {
    timeout(TEST_TIMEOUT, async {
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
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_fg_attach_input_and_detach() {
    timeout(TEST_TIMEOUT, async {
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
                ResponsePayload::Ok(OkPayload::FgAttached { .. })
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
                    payload: EventPayload::FgOutput { data },
                } if data.windows(expected_fragment.len()).any(|window| window == expected_fragment.as_slice())
            )
        })
        .await;
        assert!(
            msgs.iter().any(|msg| matches!(
                msg,
                Message::Event {
                    payload: EventPayload::FgOutput { data },
                } if data.windows(expected_fragment.len()).any(|window| window == expected_fragment.as_slice())
            )),
            "expected FgOutput containing tty echo, got {msgs:?}"
        );

        let detach_resp = roundtrip(&mut stream, 4, RequestPayload::FgDetach {}).await;
        assert!(
            matches!(detach_resp, ResponsePayload::Ok(OkPayload::Ack {})),
            "expected Ack for fg detach, got {detach_resp:?}"
        );

        let msgs = collect_until(&mut stream, Duration::from_secs(5), |msg| {
            matches!(
                msg,
                Message::Event {
                    payload: EventPayload::FgExited { id, reason },
                } if id == &job_id && reason == "detached"
            )
        })
        .await;
        assert!(
            msgs.iter().any(|msg| matches!(
                msg,
                Message::Event {
                    payload: EventPayload::FgExited { id, reason },
                } if id == &job_id && reason == "detached"
            )),
            "expected detached fg exit event, got {msgs:?}"
        );

        let jobs_resp = roundtrip(
            &mut stream,
            5,
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
            6,
            RequestPayload::Eval {
                input: format!(":kill {job_id}"),
                mode: Mode::Job,
            },
        )
        .await;

        shutdown_daemon(&mut stream, &mut child).await;
    })
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_jobs_run_in_tty() {
    timeout(TEST_TIMEOUT, async {
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
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_job_command_expands_tilde_and_env_vars() {
    timeout(TEST_TIMEOUT, async {
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
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cron_add_and_list() {
    timeout(TEST_TIMEOUT, async {
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
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cron_mode_bare_input_adds_cron() {
    timeout(TEST_TIMEOUT, async {
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
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_bare_question_returns_current_mode_help() {
    timeout(TEST_TIMEOUT, async {
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
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_gateway_stdio_bridge_shares_state_and_keeps_output_subscriptions_per_client() {
    timeout(TEST_TIMEOUT, async {
        let env = TestEnv::new("bridge-shared-state");
        let script_path = env.root.join("delayed-output.sh");
        write_executable_script(
            &script_path,
            "#!/bin/sh\nsleep 1\nprintf 'bridge-output\\n'\n",
        );

        let mut child = env.spawn_daemon();
        let mut local = wait_for_socket(&env.socket, &mut child).await;
        let (mut remote, remote_relay) = connect_bridge(&env.socket).await;

        let create_resp = roundtrip(
            &mut remote,
            1,
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

        subscribe(&mut local, 1, vec![format!("output:{job_id}")]).await;

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
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_gateway_stdio_bridge_releases_fg_owner_after_disconnect() {
    timeout(TEST_TIMEOUT, async {
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
                ResponsePayload::Ok(OkPayload::FgAttached { .. }) => break,
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
            ResponsePayload::Err { message, .. } => {
                assert!(
                    message.contains("already foreground-attached"),
                    "expected single-owner fg rejection, got {message:?}"
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
                ResponsePayload::Ok(OkPayload::FgAttached { .. }) => break,
                ResponsePayload::Err { message, .. }
                    if message.contains("already foreground-attached") =>
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
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_err_command_returns_output_for_pty_job() {
    // Single-process jobs still run in PTY mode (stdout and stderr are merged).
    // `:err J<n>` should return the combined output prefixed with the PTY notice.
    timeout(TEST_TIMEOUT, async {
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
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_native_stdout_pipeline_preserves_arguments_and_real_stderr() {
    timeout(TEST_TIMEOUT, async {
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
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_native_stderr_only_pipeline_keeps_stdout_outside_pipe() {
    timeout(TEST_TIMEOUT, async {
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
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_scopes_returns_scope_list() {
    timeout(TEST_TIMEOUT, async {
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
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_config_show_returns_weft_info() {
    timeout(TEST_TIMEOUT, async {
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
                    text.contains("weft.socket_path"),
                    "expected 'weft.socket_path' in config output, got: {text:?}"
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
    .await
    .expect("test timed out");
}
