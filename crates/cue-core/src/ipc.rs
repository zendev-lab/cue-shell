//! IPC protocol types for cued ↔ client communication.
//!
//! Transport: Unix domain socket with length-prefixed JSON framing.
//! See `docs/design/ipc-protocol.md` for the full specification.

use std::collections::BTreeMap;
use std::ops::Range;

use serde::{Deserialize, Serialize};

use crate::cron::CronStatus;
use crate::event_channel::EventChannel;
use crate::job::JobStatus;
use crate::mode::Mode;

/// IPC protocol version required by sessionized clients.
pub const IPC_PROTOCOL_VERSION: u32 = 2;
/// Capability advertised by daemons that reject session-dependent requests before `Handshake`.
pub const IPC_CAPABILITY_SESSION_HANDSHAKE_REQUIRED: &str = "session-handshake-required";
/// Script item ownership is reported by authoritative `ScriptItemCreated` events.
pub const IPC_CAPABILITY_SCRIPT_ITEM_CREATED: &str = "script-item-created";
/// Typed, quiescent cancellation for jobs, chains, and script runs.
pub const IPC_CAPABILITY_CANCEL_EXECUTION: &str = "cancel-execution";
/// Cross-connection replay and conflict detection for side-effecting requests.
pub const IPC_CAPABILITY_OPERATION_IDEMPOTENCY: &str = "operation-idempotency";
/// Typed daemon-lifetime snapshots for reconnect-safe file-script recovery.
pub const IPC_CAPABILITY_SCRIPT_INFO_RECOVERY: &str = "script-info-recovery";
/// Drain-first daemon restart with a fenced single successor.
pub const IPC_CAPABILITY_GRACEFUL_RESTART: &str = "graceful-restart";
/// Durable named process sessions that multiple human and agent clients can attach to.
pub const IPC_CAPABILITY_NAMED_SESSIONS: &str = "named-sessions";
/// Multiple foreground observers with an explicit single-controller lease.
pub const IPC_CAPABILITY_FOREGROUND_OBSERVERS: &str = "foreground-observers";
/// Safe, reversible archive/restore lifecycle for durable named sessions.
pub const IPC_CAPABILITY_SESSION_ARCHIVE: &str = "session-archive";
const IPC_CAPABILITIES: &[&str] = &[
    IPC_CAPABILITY_SESSION_HANDSHAKE_REQUIRED,
    IPC_CAPABILITY_SCRIPT_ITEM_CREATED,
    IPC_CAPABILITY_CANCEL_EXECUTION,
    IPC_CAPABILITY_OPERATION_IDEMPOTENCY,
    IPC_CAPABILITY_SCRIPT_INFO_RECOVERY,
    IPC_CAPABILITY_GRACEFUL_RESTART,
    IPC_CAPABILITY_NAMED_SESSIONS,
    IPC_CAPABILITY_FOREGROUND_OBSERVERS,
    IPC_CAPABILITY_SESSION_ARCHIVE,
];

pub fn current_protocol_capabilities() -> Vec<String> {
    IPC_CAPABILITIES
        .iter()
        .map(|capability| (*capability).to_string())
        .collect()
}

// ── Message Envelope ──

/// Top-level message, length-prefixed JSON over Unix socket.
///
/// The envelope schema is fixed. Unknown envelope fields are rejected instead
/// of being silently ignored.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Message {
    Request {
        id: u32,
        /// Stable logical operation key used to deduplicate side effects across
        /// transport reconnects. It is optional for backward compatibility.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        operation_id: Option<String>,
        payload: RequestPayload,
    },
    Response {
        id: u32,
        payload: ResponsePayload,
    },
    Event {
        payload: EventPayload,
    },
}

// ── Requests (Client → cued) ──

/// All user commands go through `Eval`. Structured requests are only for
/// protocol-level operations not typed by the user.
/// Daemon input boundary. Unknown request fields are rejected so typed clients
/// cannot accidentally depend on parameters the daemon silently ignores.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum RequestPayload {
    // User commands (raw string, parsed by cued)
    Eval {
        input: String,
        mode: Mode,
    },
    RunScript {
        path: String,
        input: String,
    },

    // Connection management
    Handshake {
        session_id: String,
        cwd: String,
        env: BTreeMap<String, String>,
        /// Explicitly replace an existing session cursor with this handshake snapshot.
        /// Defaults to false so ordinary reconnects keep the existing session scope.
        #[serde(default)]
        refresh: bool,
    },
    /// Create a durable named session from the calling client's current scope
    /// and attach that client to it.
    CreateSession {
        name: String,
    },
    /// List active durable named sessions known to this daemon.
    /// Archived sessions are omitted; use `ListArchivedSessions` or
    /// `ListAllSessions` when cleanup state must be inspected explicitly.
    ListSessions {},
    /// List only archived durable named sessions.
    ListArchivedSessions {},
    /// List active and archived durable named sessions.
    ListAllSessions {},
    /// Hide an idle durable named session from the default list without
    /// deleting its identity, scope cursor, or terminal history.
    ArchiveSession {
        selector: String,
    },
    /// Make a previously archived durable named session attachable again.
    RestoreSession {
        selector: String,
    },
    /// Attach the calling client to an existing durable named session.
    ///
    /// `refresh` is required when a sensitive, process-local scope could not
    /// survive a daemon restart. It deliberately replaces the named session's
    /// missing cursor with the calling client's current scope.
    AttachSession {
        selector: String,
        #[serde(default)]
        refresh: bool,
    },
    /// Inspect the current named session or an explicitly selected one.
    SessionInfo {
        selector: Option<String>,
    },
    Subscribe {
        channels: Vec<String>,
    },
    Unsubscribe {
        channels: Vec<String>,
    },

    // :fg proxy
    FgAttach {
        id: String,
    },
    /// Observe a PTY job without acquiring its input/controller lease.
    FgWatch {
        id: String,
    },
    /// Acquire the free controller lease for the currently observed PTY job.
    FgClaimControl {},
    /// Release the controller lease while remaining an observer.
    FgReleaseControl {},
    FgDetach {},
    FgInput {
        #[serde(with = "serde_bytes_base64")]
        data: Vec<u8>,
    },
    FgResize {
        cols: u16,
        rows: u16,
    },
    // Editor services
    Complete {
        input: String,
        cursor: usize,
    },
    Highlight {
        input: String,
    },

    // Typed query/control APIs for non-interactive clients.
    ListJobs {
        limit: Option<usize>,
    },
    ListCrons {
        limit: Option<usize>,
    },
    ListScopes {
        limit: Option<usize>,
    },
    /// Recover the authoritative state of a daemon-lifetime file-script run.
    ScriptInfo {
        id: String,
    },
    ShowLog {
        id: Option<String>,
        limit: Option<usize>,
        tail_bytes: Option<usize>,
    },
    JobOutput {
        id: String,
        stdout_bytes: Option<usize>,
        stderr_bytes: Option<usize>,
    },
    KillJob {
        id: String,
    },
    /// Idempotently cancel a foreground execution by job (`J<n>`), chain
    /// (`CH<n>`), or script-run (`R<n>`) id. The acknowledgement is sent only
    /// after any currently running child processes have stopped.
    CancelExecution {
        id: String,
    },
    RemoveCron {
        id: String,
    },
    ShowEnv {
        tail_bytes: Option<usize>,
    },
    ShowConfig {
        tail_bytes: Option<usize>,
    },

    // System
    Ping {},
    /// Stop new execution admission, let already accepted work finish, then
    /// hand ownership to one successor daemon.
    Restart {},
    Shutdown {},
}

impl RequestPayload {
    pub fn subscribe(channels: &[EventChannel]) -> Self {
        Self::Subscribe {
            channels: event_channel_names(channels),
        }
    }

    pub fn unsubscribe(channels: &[EventChannel]) -> Self {
        Self::Unsubscribe {
            channels: event_channel_names(channels),
        }
    }
}

fn event_channel_names(channels: &[EventChannel]) -> Vec<String> {
    channels.iter().map(ToString::to_string).collect()
}

// ── Responses (cued → Client) ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResponsePayload {
    Ok(OkPayload),
    Err { code: String, message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OkPayload {
    Ack {},
    ScriptCreated {
        script_id: String,
        source: ScriptSource,
        items: Vec<ScriptItemInfo>,
        submit_error: Option<ScriptSubmitError>,
    },
    JobCreated {
        job_id: String,
        start_scope: Option<String>,
        open_hint: JobOpenHint,
        chain_id: Option<String>,
        chain_index: Option<usize>,
        chain_total: Option<usize>,
        #[serde(default)]
        warnings: Vec<String>,
    },
    ChainCreated {
        chain_id: String,
        job_ids: Vec<String>,
        chain: ChainInfo,
        #[serde(default)]
        warnings: Vec<String>,
    },
    CronAdded {
        cron_id: String,
    },
    ScopeCreated {
        hash: String,
        summary: String,
    },

    JobInfo(JobInfo),
    JobList(Vec<JobInfo>),
    JobListPage {
        jobs: Vec<JobInfo>,
        page: PageInfo,
    },
    CronList(Vec<CronInfo>),
    CronListPage {
        crons: Vec<CronInfo>,
        page: PageInfo,
    },
    ScopeInfo(ScopeInfo),
    ScopeList(Vec<ScopeInfo>),
    ScopeListPage {
        scopes: Vec<ScopeInfo>,
        page: PageInfo,
    },
    SessionInfo(Box<SessionInfo>),
    SessionList(Vec<SessionInfo>),
    ScriptInfo(ScriptInfo),
    Output {
        id: String,
        data: String,
        truncated: bool,
        #[serde(default)]
        encoding: OutputEncoding,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        base64: Option<String>,
    },
    JobOutput {
        id: String,
        stdout: StreamText,
        stderr: StreamText,
        stderr_pty_merged: bool,
    },

    EvalText {
        text: String,
    },
    TextOutput {
        text: String,
        truncated: bool,
        #[serde(default)]
        encoding: OutputEncoding,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        base64: Option<String>,
    },

    CompletionList {
        items: Vec<CompletionItem>,
    },
    HighlightResult {
        spans: Vec<HighlightSpan>,
    },

    FgAttached(Box<ForegroundAttachmentInfo>),
    FgRoleChanged {
        id: String,
        /// Identifies the exact foreground attachment this transition belongs to.
        #[serde(default)]
        attachment_id: u64,
        role: ForegroundRole,
        control_available: bool,
    },
    Pong {
        /// Daemon `cued` build version reported by the running daemon.
        version: String,
        /// Stable UUID for this daemon process. Changes after every restart.
        /// Empty when talking to a daemon that predates instance IDs.
        #[serde(default)]
        instance_id: String,
        /// Restart generation token. A planned successor must match the target
        /// generation preallocated in the restart intent.
        #[serde(default)]
        generation_id: String,
        /// True only after startup restoration, exact restart fencing, and
        /// scheduler execution activation have all completed. Missing means
        /// true for compatibility with daemons predating startup fencing.
        #[serde(default = "default_pong_ready")]
        ready: bool,
        /// IPC protocol version implemented by the daemon.
        protocol_version: u32,
        /// Feature flags implemented by the daemon for explicit client gating.
        capabilities: Vec<String>,
    },
    RestartAccepted {
        /// Stable across repeated restart requests handled by this generation.
        restart_id: String,
        /// The daemon generation that accepted and owns the drain.
        daemon_instance_id: String,
        /// Exact generation token the successor must advertise in Pong.
        target_generation: String,
    },
}

fn default_pong_ready() -> bool {
    true
}

// ── Events (cued → Client, pushed) ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventPayload {
    // Jobs channel
    JobStateChanged {
        job_id: String,
        old_state: JobStatus,
        new_state: JobStatus,
        end_scope: Option<String>,
        chain_id: Option<String>,
        chain_index: Option<usize>,
    },
    JobCreated {
        job_id: String,
        pipeline: String,
        start_scope: Option<String>,
        open_hint: JobOpenHint,
        chain_id: Option<String>,
        chain_index: Option<usize>,
        chain_total: Option<usize>,
    },
    ChainProgress {
        chain: ChainInfo,
    },
    /// A file-script item created after the initial `ScriptCreated` response.
    ///
    /// The daemon is the authority for the item-to-job/chain association.
    /// Clients must not infer script membership from globally ordered job IDs
    /// or unrelated `JobCreated` events.
    ScriptItemCreated {
        script_id: String,
        item: ScriptItemInfo,
    },
    ScriptFinished {
        script_id: String,
        status: ScriptRunStatus,
        /// Numeric process exit code, or `job::EXIT_CODE_UNAVAILABLE` when no
        /// process-provided status exists.
        exit_code: i32,
        failed_item_index: Option<usize>,
    },
    JobRemoved {
        job_id: String,
    },

    // Crons channel
    CronTriggered {
        cron_id: String,
        job_id: String,
    },
    CronRemoved {
        cron_id: String,
    },

    // Output channel (output:<id>)
    OutputChunk {
        id: String,
        stream: Stream,
        data: String,
    },
    OutputChunkBinary {
        id: String,
        stream: Stream,
        base64: String,
    },
    OutputEof {
        id: String,
    },

    // :fg (sent only to fg-attached client)
    FgOutput {
        /// Empty only when decoded from daemons predating job-scoped foreground streams.
        #[serde(default)]
        id: String,
        /// Zero only when decoded from daemons predating attachment epochs.
        #[serde(default)]
        attachment_id: u64,
        #[serde(with = "serde_bytes_base64")]
        data: Vec<u8>,
    },
    FgControlChanged {
        id: String,
        /// Zero only when decoded from daemons predating attachment epochs.
        #[serde(default)]
        attachment_id: u64,
        control_available: bool,
    },
    FgExited {
        id: String,
        /// Zero only when decoded from daemons predating attachment epochs.
        #[serde(default)]
        attachment_id: u64,
        reason: String,
    },

    // System channel
    ShuttingDown {
        reason: String,
    },
}

/// Output stream type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Stream {
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobOpenHint {
    Stream,
    Fg,
}

/// A client's effective role in a shared foreground attachment.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ForegroundRole {
    /// Owns the exclusive input and resize lease.
    #[default]
    Controller,
    /// Receives output and exit events but cannot write or resize.
    Observer,
}

/// Atomic foreground registration result: a byte snapshot followed by live events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForegroundAttachmentInfo {
    pub id: String,
    /// Monotonic, non-zero identifier for this exact job/client attachment.
    #[serde(default)]
    pub attachment_id: u64,
    /// Defaults to the historical exclusive attachment role when decoding an
    /// old `{ "FgAttached": { "id": ... } }` response.
    #[serde(default)]
    pub role: ForegroundRole,
    #[serde(default)]
    pub control_available: bool,
    #[serde(default, with = "serde_bytes_base64")]
    pub snapshot: Vec<u8>,
    #[serde(default)]
    pub snapshot_truncated: bool,
}

// ── Info structs (shared by Response and queries) ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageInfo {
    pub total: usize,
    pub shown: usize,
    pub limit: Option<usize>,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamText {
    /// Backward-compatible display text. For binary output this is an explicit
    /// lossy UTF-8 view; `base64` is the authoritative byte representation.
    pub data: String,
    pub truncated: bool,
    #[serde(default)]
    pub encoding: OutputEncoding,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base64: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutputEncoding {
    #[default]
    Utf8,
    Base64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobInfo {
    pub id: String,
    /// Durable named-session owner. Legacy and anonymous-session jobs have no owner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub status: JobStatus,
    pub pipeline: String,
    pub exit_code: Option<i32>,
    pub start_scope: Option<String>,
    pub end_scope: Option<String>,
    pub open_hint: JobOpenHint,
    pub chain_id: Option<String>,
    pub chain_index: Option<usize>,
    pub chain_total: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronInfo {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub schedule: String,
    pub command: String,
    pub status: CronStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScopeInfo {
    pub hash: String,
    pub parent: Option<String>,
    pub cwd: String,
    pub env_count: usize,
}

/// Whether a named session cursor can survive a daemon restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionScopeState {
    /// The scope is available now and has a durable SQLite record.
    ReadyDurable,
    /// The scope is available to this daemon process but intentionally stays
    /// in memory because it contains credential-like environment names.
    ReadyVolatile,
    /// The durable identity survived a restart, but its volatile scope did
    /// not. An explicit refreshed attach is required before execution.
    NeedsRefresh,
}

/// Public metadata for a durable named process session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionInfo {
    pub id: String,
    pub name: String,
    pub scope_state: SessionScopeState,
    pub scope_hash: Option<String>,
    pub connected_clients: usize,
    pub restart_safe: bool,
    pub current: bool,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    /// Present when the session is hidden from the default active-session list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archived_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainInfo {
    pub id: String,
    pub pipeline: String,
    pub total_jobs: usize,
    pub jobs: Vec<ChainJobInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainJobInfo {
    pub index: usize,
    pub pipeline: String,
    pub status: JobStatus,
    pub job_id: Option<String>,
    pub start_scope: Option<String>,
    pub end_scope: Option<String>,
    pub open_hint: Option<JobOpenHint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScriptItemInfo {
    pub index: usize,
    pub source: String,
    pub result: ScriptItemResult,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScriptSource {
    #[default]
    Inline,
    File {
        path: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScriptRunStatus {
    Done,
    Failed,
}

/// Recoverable lifecycle state for a daemon-lifetime script snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScriptInfoStatus {
    Running,
    Done,
    Failed,
}

/// Authoritative snapshot used to reconcile script events missed during a
/// transport disconnect. It is intentionally daemon-lifetime, not crash-safe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScriptInfo {
    pub script_id: String,
    pub status: ScriptInfoStatus,
    pub items: Vec<ScriptItemInfo>,
    pub exit_code: Option<i32>,
    pub failed_item_index: Option<usize>,
    pub submit_error: Option<ScriptSubmitError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScriptItemResult {
    Job {
        job_id: String,
        start_scope: Option<String>,
        open_hint: JobOpenHint,
    },
    Chain {
        chain_id: String,
        job_ids: Vec<String>,
        chain: ChainInfo,
    },
    Cron {
        cron_id: String,
    },
    Message {
        text: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScriptSubmitError {
    pub index: usize,
    pub source: String,
    pub code: String,
    pub message: String,
}

// ── Editor services ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionItem {
    pub label: String,
    pub insert_text: String,
    pub kind: CompletionKind,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompletionKind {
    Command,
    Param,
    Id,
    Path,
    Operator,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HighlightSpan {
    pub start: usize,
    pub end: usize,
    pub kind: HighlightKind,
}

impl HighlightSpan {
    pub fn range(&self) -> Range<usize> {
        self.start..self.end
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HighlightKind {
    CommandPrefix,
    CommandName,
    ModeParam,
    Operator,
    IdRef,
    Word,
    String,
    Number,
    Error,
}

// ── Error codes ──

/// Standard IPC error codes.
pub mod error_code {
    pub const NOT_FOUND: &str = "NOT_FOUND";
    pub const INVALID_REQUEST: &str = "INVALID_REQUEST";
    pub const INVALID_STATE: &str = "INVALID_STATE";
    pub const INVALID_SCOPE: &str = "INVALID_SCOPE";
    pub const INVALID_SYNTAX: &str = "INVALID_SYNTAX";
    pub const ALREADY_EXISTS: &str = "ALREADY_EXISTS";
    pub const NOT_SUPPORTED: &str = "NOT_SUPPORTED";
    pub const PERMISSION_DENIED: &str = "PERMISSION_DENIED";
    pub const BLOCKED: &str = "BLOCKED";
    pub const WARNED: &str = "WARNED";
    pub const INTERNAL: &str = "INTERNAL";
    pub const DAEMON_DRAINING: &str = "DAEMON_DRAINING";
}

impl ResponsePayload {
    /// Convenience: create an Ok(Ack) response.
    pub fn ack() -> Self {
        Self::Ok(OkPayload::Ack {})
    }

    /// Convenience: create an error response.
    pub fn err(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Err {
            code: code.into(),
            message: message.into(),
        }
    }
}

// ── Framing helpers ──

/// Maximum message body size (16 MiB).
pub const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Encode a message to length-prefixed JSON bytes.
pub fn encode_message(msg: &Message) -> Result<Vec<u8>, serde_json::Error> {
    let json = serde_json::to_vec(msg)?;
    let len = json.len() as u32;
    let mut buf = Vec::with_capacity(4 + json.len());
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&json);
    Ok(buf)
}

/// Serde helper for Vec<u8> ↔ base64 string (for binary data in JSON).
mod serde_bytes_base64 {
    use base64::Engine as _;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(data: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error> {
        base64::engine::general_purpose::STANDARD
            .encode(data)
            .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let text = String::deserialize(deserializer)?;
        base64::engine::general_purpose::STANDARD
            .decode(text)
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_eval_request() {
        let msg = Message::Request {
            id: 1,
            operation_id: Some("tool-call-1:eval".into()),
            payload: RequestPayload::Eval {
                input: ":run cargo test".into(),
                mode: Mode::Job,
            },
        };
        let encoded = encode_message(&msg).unwrap();
        // First 4 bytes = length
        let len = u32::from_be_bytes(encoded[..4].try_into().unwrap()) as usize;
        assert_eq!(len, encoded.len() - 4);
        // Deserialize body
        let decoded: Message = serde_json::from_slice(&encoded[4..]).unwrap();
        if let Message::Request {
            id,
            operation_id,
            payload: RequestPayload::Eval { input, mode },
        } = decoded
        {
            assert_eq!(id, 1);
            assert_eq!(operation_id.as_deref(), Some("tool-call-1:eval"));
            assert_eq!(input, ":run cargo test");
            assert_eq!(mode, Mode::Job);
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn request_message_rejects_unknown_envelope_fields() {
        let json = r#"{"type":"request","id":1,"payload":{"Ping":{}},"trace_id":"abc"}"#;

        let error = serde_json::from_str::<Message>(json)
            .expect_err("unknown top-level message fields must not be ignored");

        assert!(
            error.to_string().contains("unknown field `trace_id`"),
            "wrong error: {error}"
        );
    }

    #[test]
    fn subscription_request_constructors_use_event_channel_wire_names() {
        let subscribe = RequestPayload::subscribe(&[
            EventChannel::Jobs,
            EventChannel::Crons,
            EventChannel::Output(crate::JobId(7)),
        ]);
        match subscribe {
            RequestPayload::Subscribe { channels } => {
                assert_eq!(channels, vec!["jobs", "crons", "output:J7"]);
            }
            _ => panic!("wrong variant"),
        }

        let unsubscribe =
            RequestPayload::unsubscribe(&[EventChannel::Scopes, EventChannel::System]);
        match unsubscribe {
            RequestPayload::Unsubscribe { channels } => {
                assert_eq!(channels, vec!["scopes", "system"]);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_error_response() {
        let msg = Message::Response {
            id: 1,
            payload: ResponsePayload::err("INVALID_SYNTAX", "unexpected token"),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let decoded: Message = serde_json::from_str(&json).unwrap();
        if let Message::Response {
            payload: ResponsePayload::Err { code, message },
            ..
        } = decoded
        {
            assert_eq!(code, "INVALID_SYNTAX");
            assert_eq!(message, "unexpected token");
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn response_payload_helpers() {
        assert!(matches!(
            ResponsePayload::ack(),
            ResponsePayload::Ok(OkPayload::Ack {})
        ));
    }

    #[test]
    fn typed_query_payloads_roundtrip() {
        let msg = Message::Request {
            id: 7,
            operation_id: None,
            payload: RequestPayload::ShowLog {
                id: Some("J1".into()),
                limit: Some(20),
                tail_bytes: Some(4096),
            },
        };
        let json = serde_json::to_string(&msg).unwrap();
        let decoded: Message = serde_json::from_str(&json).unwrap();
        match decoded {
            Message::Request {
                payload:
                    RequestPayload::ShowLog {
                        id,
                        limit,
                        tail_bytes,
                    },
                ..
            } => {
                assert_eq!(id.as_deref(), Some("J1"));
                assert_eq!(limit, Some(20));
                assert_eq!(tail_bytes, Some(4096));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn named_session_requests_and_info_roundtrip() {
        let requests = [
            RequestPayload::CreateSession {
                name: "shared-dev".into(),
            },
            RequestPayload::ListSessions {},
            RequestPayload::ListArchivedSessions {},
            RequestPayload::ListAllSessions {},
            RequestPayload::ArchiveSession {
                selector: "shared-dev".into(),
            },
            RequestPayload::RestoreSession {
                selector: "SS-1".into(),
            },
            RequestPayload::AttachSession {
                selector: "shared-dev".into(),
                refresh: true,
            },
            RequestPayload::SessionInfo { selector: None },
        ];
        for payload in requests {
            let json = serde_json::to_string(&payload).expect("serialize session request");
            serde_json::from_str::<RequestPayload>(&json).expect("deserialize session request");
        }

        let info = SessionInfo {
            id: "SS-1".into(),
            name: "shared-dev".into(),
            scope_state: SessionScopeState::ReadyVolatile,
            scope_hash: Some("abc".into()),
            connected_clients: 2,
            restart_safe: false,
            current: true,
            created_at_ms: 1,
            updated_at_ms: 2,
            archived_at_ms: Some(3),
        };
        let payload = OkPayload::SessionInfo(Box::new(info.clone()));
        let decoded: OkPayload =
            serde_json::from_str(&serde_json::to_string(&payload).expect("serialize session info"))
                .expect("deserialize session info");
        assert!(matches!(decoded, OkPayload::SessionInfo(actual) if actual.as_ref() == &info));
        assert!(
            current_protocol_capabilities()
                .iter()
                .any(|capability| capability == IPC_CAPABILITY_NAMED_SESSIONS)
        );
        assert!(
            current_protocol_capabilities()
                .iter()
                .any(|capability| capability == IPC_CAPABILITY_SESSION_ARCHIVE)
        );

        let legacy_json = r#"{"id":"SS-1","name":"shared-dev","scope_state":"ready_durable","scope_hash":null,"connected_clients":0,"restart_safe":true,"current":false,"created_at_ms":1,"updated_at_ms":2}"#;
        let legacy: SessionInfo = serde_json::from_str(legacy_json).expect("legacy session info");
        assert_eq!(legacy.archived_at_ms, None);
    }

    #[test]
    fn rich_output_payloads_roundtrip() {
        let payload = ResponsePayload::Ok(OkPayload::JobOutput {
            id: "J1".into(),
            stdout: StreamText {
                data: "out".into(),
                truncated: false,
                encoding: OutputEncoding::Utf8,
                base64: None,
            },
            stderr: StreamText {
                data: "err".into(),
                truncated: true,
                encoding: OutputEncoding::Utf8,
                base64: None,
            },
            stderr_pty_merged: false,
        });
        let json = serde_json::to_string(&payload).unwrap();
        let decoded: ResponsePayload = serde_json::from_str(&json).unwrap();
        match decoded {
            ResponsePayload::Ok(OkPayload::JobOutput { stderr, .. }) => {
                assert_eq!(stderr.data, "err");
                assert!(stderr.truncated);
                assert_eq!(stderr.encoding, OutputEncoding::Utf8);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn legacy_output_without_encoding_defaults_to_utf8() {
        let decoded: ResponsePayload = serde_json::from_str(
            r#"{"Ok":{"JobOutput":{"id":"J1","stdout":{"data":"out","truncated":false},"stderr":{"data":"","truncated":false},"stderr_pty_merged":false}}}"#,
        )
        .unwrap();

        match decoded {
            ResponsePayload::Ok(OkPayload::JobOutput { stdout, stderr, .. }) => {
                assert_eq!(stdout.encoding, OutputEncoding::Utf8);
                assert_eq!(stderr.encoding, OutputEncoding::Utf8);
                assert!(stdout.base64.is_none());
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn binary_stream_text_roundtrips_with_authoritative_base64() {
        let payload = ResponsePayload::Ok(OkPayload::JobOutput {
            id: "J1".into(),
            stdout: StreamText {
                data: "�bin".into(),
                truncated: false,
                encoding: OutputEncoding::Base64,
                base64: Some("/2Jpbg==".into()),
            },
            stderr: StreamText {
                data: String::new(),
                truncated: false,
                encoding: OutputEncoding::Utf8,
                base64: None,
            },
            stderr_pty_merged: false,
        });

        let decoded: ResponsePayload =
            serde_json::from_str(&serde_json::to_string(&payload).unwrap()).unwrap();
        match decoded {
            ResponsePayload::Ok(OkPayload::JobOutput { stdout, .. }) => {
                assert_eq!(stdout.encoding, OutputEncoding::Base64);
                assert_eq!(stdout.base64.as_deref(), Some("/2Jpbg=="));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn run_script_request_roundtrips() {
        let msg = Message::Request {
            id: 9,
            operation_id: None,
            payload: RequestPayload::RunScript {
                path: "scripts/build.cue".into(),
                input: ":run cargo build".into(),
            },
        };
        let json = serde_json::to_string(&msg).unwrap();
        let decoded: Message = serde_json::from_str(&json).unwrap();
        match decoded {
            Message::Request {
                id,
                payload: RequestPayload::RunScript { path, input },
                ..
            } => {
                assert_eq!(id, 9);
                assert_eq!(path, "scripts/build.cue");
                assert_eq!(input, ":run cargo build");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn run_script_request_rejects_unknown_fields() {
        let json = r#"{"type":"request","id":9,"payload":{"RunScript":{"path":"scripts/build.cue","input":":run cargo build","mode":"job"}}}"#;

        let error = serde_json::from_str::<Message>(json)
            .expect_err("unknown request fields must not be ignored");

        assert!(
            error.to_string().contains("unknown field `mode`"),
            "wrong error: {error}"
        );
    }

    #[test]
    fn complete_request_roundtrips_without_mode() {
        let msg = Message::Request {
            id: 3,
            operation_id: None,
            payload: RequestPayload::Complete {
                input: ":ru".into(),
                cursor: 3,
            },
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(!json.contains("mode"));

        let decoded: Message = serde_json::from_str(&json).unwrap();
        match decoded {
            Message::Request {
                id,
                payload: RequestPayload::Complete { input, cursor },
                ..
            } => {
                assert_eq!(id, 3);
                assert_eq!(input, ":ru");
                assert_eq!(cursor, 3);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn script_created_item_and_finished_payloads_roundtrip() {
        let created = ResponsePayload::Ok(OkPayload::ScriptCreated {
            script_id: "R7".into(),
            source: ScriptSource::File {
                path: "scripts/build.cue".into(),
            },
            items: vec![],
            submit_error: None,
        });
        let json = serde_json::to_string(&created).unwrap();
        let decoded: ResponsePayload = serde_json::from_str(&json).unwrap();
        match decoded {
            ResponsePayload::Ok(OkPayload::ScriptCreated { source, .. }) => {
                assert_eq!(
                    source,
                    ScriptSource::File {
                        path: "scripts/build.cue".into()
                    }
                );
            }
            _ => panic!("wrong variant"),
        }

        let item_created = Message::Event {
            payload: EventPayload::ScriptItemCreated {
                script_id: "R7".into(),
                item: ScriptItemInfo {
                    index: 1,
                    source: "echo second".into(),
                    result: ScriptItemResult::Job {
                        job_id: "J9".into(),
                        start_scope: Some("S@abc12345".into()),
                        open_hint: JobOpenHint::Stream,
                    },
                },
            },
        };
        let json = serde_json::to_string(&item_created).unwrap();
        let decoded: Message = serde_json::from_str(&json).unwrap();
        match decoded {
            Message::Event {
                payload: EventPayload::ScriptItemCreated { script_id, item },
            } => {
                assert_eq!(script_id, "R7");
                assert_eq!(item.index, 1);
                assert!(matches!(
                    item.result,
                    ScriptItemResult::Job { ref job_id, .. } if job_id == "J9"
                ));
            }
            _ => panic!("wrong variant"),
        }

        let finished = Message::Event {
            payload: EventPayload::ScriptFinished {
                script_id: "R7".into(),
                status: ScriptRunStatus::Failed,
                exit_code: 2,
                failed_item_index: Some(1),
            },
        };
        let json = serde_json::to_string(&finished).unwrap();
        let decoded: Message = serde_json::from_str(&json).unwrap();
        match decoded {
            Message::Event {
                payload:
                    EventPayload::ScriptFinished {
                        script_id,
                        status,
                        exit_code,
                        failed_item_index,
                    },
            } => {
                assert_eq!(script_id, "R7");
                assert_eq!(status, ScriptRunStatus::Failed);
                assert_eq!(exit_code, 2);
                assert_eq!(failed_item_index, Some(1));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn scope_created_payload_has_no_label_field() {
        let payload = ResponsePayload::Ok(OkPayload::ScopeCreated {
            hash: "S@abc12345".into(),
            summary: "S@abc12345\ncwd: /old -> /tmp".into(),
        });
        let json = serde_json::to_string(&payload).unwrap();
        assert!(!json.contains("label"));

        let decoded: ResponsePayload = serde_json::from_str(&json).unwrap();
        match decoded {
            ResponsePayload::Ok(OkPayload::ScopeCreated { hash, summary }) => {
                assert_eq!(hash, "S@abc12345");
                assert!(summary.contains("cwd: /old -> /tmp"));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn binary_payloads_serialize_as_base64_strings() {
        let msg = Message::Event {
            payload: EventPayload::FgOutput {
                id: "J7".into(),
                attachment_id: 11,
                data: vec![0, 1, 2, 0xfe, 0xff],
            },
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"AAEC/v8=\""));
    }

    #[test]
    fn foreground_attachment_decodes_legacy_exclusive_response() {
        let json = r#"{"Ok":{"FgAttached":{"id":"J7"}}}"#;
        let decoded: ResponsePayload = serde_json::from_str(json).unwrap();

        match decoded {
            ResponsePayload::Ok(OkPayload::FgAttached(info)) => {
                assert_eq!(info.id, "J7");
                assert_eq!(info.attachment_id, 0);
                assert_eq!(info.role, ForegroundRole::Controller);
                assert!(!info.control_available);
                assert!(info.snapshot.is_empty());
                assert!(!info.snapshot_truncated);
            }
            _ => panic!("wrong foreground response"),
        }
    }

    #[test]
    fn foreground_attachment_snapshot_serializes_as_base64() {
        let payload =
            ResponsePayload::Ok(OkPayload::FgAttached(Box::new(ForegroundAttachmentInfo {
                id: "J7".into(),
                attachment_id: 23,
                role: ForegroundRole::Observer,
                control_available: true,
                snapshot: vec![0, 1, 2, 0xfe, 0xff],
                snapshot_truncated: true,
            })));

        let json = serde_json::to_string(&payload).unwrap();
        assert!(json.contains("\"AAEC/v8=\""));
        let decoded: ResponsePayload = serde_json::from_str(&json).unwrap();
        assert!(matches!(
            decoded,
            ResponsePayload::Ok(OkPayload::FgAttached(info))
                if info.attachment_id == 23
                    && info.role == ForegroundRole::Observer
                    && info.control_available
                    && info.snapshot == vec![0, 1, 2, 0xfe, 0xff]
                    && info.snapshot_truncated
        ));
    }

    #[test]
    fn foreground_output_decodes_legacy_event_without_job_id() {
        let json = r#"{"type":"event","payload":{"FgOutput":{"data":"QUJD"}}}"#;
        let decoded: Message = serde_json::from_str(json).unwrap();

        assert!(matches!(
            decoded,
            Message::Event {
                payload: EventPayload::FgOutput {
                    id,
                    attachment_id,
                    data,
                }
            } if id.is_empty() && attachment_id == 0 && data == b"ABC"
        ));
    }

    #[test]
    fn shared_foreground_requests_and_control_event_roundtrip() {
        for payload in [
            RequestPayload::FgWatch { id: "J7".into() },
            RequestPayload::FgClaimControl {},
            RequestPayload::FgReleaseControl {},
        ] {
            let json = serde_json::to_string(&payload).unwrap();
            serde_json::from_str::<RequestPayload>(&json).unwrap();
        }

        let message = Message::Event {
            payload: EventPayload::FgControlChanged {
                id: "J7".into(),
                attachment_id: 29,
                control_available: true,
            },
        };
        let json = serde_json::to_string(&message).unwrap();
        assert!(matches!(
            serde_json::from_str::<Message>(&json).unwrap(),
            Message::Event {
                payload: EventPayload::FgControlChanged {
                    id,
                    attachment_id: 29,
                    control_available: true,
                },
            } if id == "J7"
        ));

        let role_response = ResponsePayload::Ok(OkPayload::FgRoleChanged {
            id: "J7".into(),
            attachment_id: 29,
            role: ForegroundRole::Controller,
            control_available: false,
        });
        let json = serde_json::to_string(&role_response).unwrap();
        assert!(matches!(
            serde_json::from_str::<ResponsePayload>(&json).unwrap(),
            ResponsePayload::Ok(OkPayload::FgRoleChanged {
                id,
                attachment_id: 29,
                role: ForegroundRole::Controller,
                control_available: false,
            }) if id == "J7"
        ));
        assert!(
            current_protocol_capabilities()
                .iter()
                .any(|capability| capability == IPC_CAPABILITY_FOREGROUND_OBSERVERS)
        );
    }

    #[test]
    fn foreground_epoch_defaults_for_legacy_role_and_lifecycle_payloads() {
        let role_json =
            r#"{"Ok":{"FgRoleChanged":{"id":"J7","role":"observer","control_available":true}}}"#;
        assert!(matches!(
            serde_json::from_str::<ResponsePayload>(role_json).unwrap(),
            ResponsePayload::Ok(OkPayload::FgRoleChanged {
                id,
                attachment_id: 0,
                role: ForegroundRole::Observer,
                control_available: true,
            }) if id == "J7"
        ));

        let control_json = r#"{"type":"event","payload":{"FgControlChanged":{"id":"J7","control_available":true}}}"#;
        assert!(matches!(
            serde_json::from_str::<Message>(control_json).unwrap(),
            Message::Event {
                payload: EventPayload::FgControlChanged {
                    id,
                    attachment_id: 0,
                    control_available: true,
                },
            } if id == "J7"
        ));

        let exit_json =
            r#"{"type":"event","payload":{"FgExited":{"id":"J7","reason":"detached"}}}"#;
        assert!(matches!(
            serde_json::from_str::<Message>(exit_json).unwrap(),
            Message::Event {
                payload: EventPayload::FgExited {
                    id,
                    attachment_id: 0,
                    reason,
                },
            } if id == "J7" && reason == "detached"
        ));
    }

    #[test]
    fn binary_payloads_reject_array_encoding() {
        let json = r#"{"type":"event","payload":{"FgOutput":{"data":[65,66,67]}}}"#;
        let error = serde_json::from_str::<Message>(json)
            .expect_err("binary payloads must use base64 string encoding");

        assert!(
            error.to_string().contains("invalid type"),
            "wrong error: {error}"
        );
    }

    #[test]
    fn script_created_requires_source() {
        let json = r#"{"Ok":{"ScriptCreated":{"script_id":"R1","items":[],"submit_error":null}}}"#;
        let error = serde_json::from_str::<ResponsePayload>(json)
            .expect_err("ScriptCreated must carry an explicit source");

        assert!(
            error.to_string().contains("missing field `source`"),
            "wrong error: {error}"
        );
    }

    #[test]
    fn pong_requires_version_field() {
        let json = r#"{"Ok":{"Pong":{}}}"#;
        let error = serde_json::from_str::<ResponsePayload>(json)
            .expect_err("Pong must carry a daemon version");

        assert!(
            error.to_string().contains("missing field `version`"),
            "wrong error: {error}"
        );
    }

    #[test]
    fn pong_requires_protocol_version_field() {
        let json = r#"{"Ok":{"Pong":{"version":"0.1.0","capabilities":[]}}}"#;
        let error = serde_json::from_str::<ResponsePayload>(json)
            .expect_err("Pong must carry a protocol version");

        assert!(
            error
                .to_string()
                .contains("missing field `protocol_version`"),
            "wrong error: {error}"
        );
    }

    #[test]
    fn pong_requires_capabilities_field() {
        let json = r#"{"Ok":{"Pong":{"version":"0.1.0","instance_id":"00000000-0000-4000-8000-000000000000","protocol_version":2}}}"#;
        let error = serde_json::from_str::<ResponsePayload>(json)
            .expect_err("Pong must carry protocol capabilities");

        assert!(
            error.to_string().contains("missing field `capabilities`"),
            "wrong error: {error}"
        );
    }

    #[test]
    fn pong_decodes_legacy_payload_without_instance_id() {
        let json = r#"{"type":"response","id":7,"payload":{"Ok":{"Pong":{"version":"0.1.0","protocol_version":2,"capabilities":["session-handshake-required"]}}}}"#;
        let decoded: Message = serde_json::from_str(json).unwrap();

        match decoded {
            Message::Response {
                id: 7,
                payload:
                    ResponsePayload::Ok(OkPayload::Pong {
                        version,
                        instance_id,
                        generation_id,
                        ready,
                        protocol_version,
                        capabilities,
                    }),
            } => {
                assert_eq!(version, "0.1.0");
                assert_eq!(instance_id, "");
                assert_eq!(generation_id, "");
                assert!(ready);
                assert_eq!(protocol_version, IPC_PROTOCOL_VERSION);
                assert_eq!(
                    capabilities,
                    vec![IPC_CAPABILITY_SESSION_HANDSHAKE_REQUIRED.to_string()]
                );
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn pong_decodes_versioned_payload() {
        let json = r#"{"Ok":{"Pong":{"version":"0.1.0","instance_id":"00000000-0000-4000-8000-000000000000","generation_id":"generation-1","protocol_version":2,"capabilities":["session-handshake-required","script-item-created","cancel-execution","operation-idempotency","script-info-recovery","graceful-restart","named-sessions","foreground-observers","session-archive"]}}}"#;
        let decoded: ResponsePayload = serde_json::from_str(json).unwrap();
        match decoded {
            ResponsePayload::Ok(OkPayload::Pong {
                version,
                instance_id,
                generation_id,
                ready,
                protocol_version,
                capabilities,
            }) => {
                assert_eq!(version, "0.1.0");
                assert_eq!(instance_id, "00000000-0000-4000-8000-000000000000");
                assert_eq!(generation_id, "generation-1");
                assert!(ready);
                assert_eq!(protocol_version, IPC_PROTOCOL_VERSION);
                assert_eq!(capabilities, current_protocol_capabilities());
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn pong_serializes_reported_version() {
        let payload = ResponsePayload::Ok(OkPayload::Pong {
            version: "0.1.0".into(),
            instance_id: "00000000-0000-4000-8000-000000000000".into(),
            generation_id: "generation-1".into(),
            ready: true,
            protocol_version: IPC_PROTOCOL_VERSION,
            capabilities: current_protocol_capabilities(),
        });
        let json = serde_json::to_string(&payload).unwrap();
        assert_eq!(
            json,
            r#"{"Ok":{"Pong":{"version":"0.1.0","instance_id":"00000000-0000-4000-8000-000000000000","generation_id":"generation-1","ready":true,"protocol_version":2,"capabilities":["session-handshake-required","script-item-created","cancel-execution","operation-idempotency","script-info-recovery","graceful-restart","named-sessions","foreground-observers","session-archive"]}}}"#
        );
    }

    #[test]
    fn cancel_execution_roundtrips_as_typed_request() {
        let message = Message::Request {
            id: 42,
            operation_id: None,
            payload: RequestPayload::CancelExecution { id: "R7".into() },
        };
        let json = serde_json::to_string(&message).unwrap();
        assert_eq!(
            json,
            r#"{"type":"request","id":42,"payload":{"CancelExecution":{"id":"R7"}}}"#
        );
        assert!(matches!(
            serde_json::from_str::<Message>(&json).unwrap(),
            Message::Request {
                id: 42,
                payload: RequestPayload::CancelExecution { id },
                ..
            } if id == "R7"
        ));
    }
}
