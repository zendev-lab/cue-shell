//! IPC protocol types for cued ↔ client communication.
//!
//! Transport: Unix domain socket with length-prefixed JSON framing.
//! See `docs/design/ipc-protocol.md` for the full specification.

use std::ops::Range;

use serde::{Deserialize, Serialize};

use crate::cron::CronStatus;
use crate::event_channel::EventChannel;
use crate::job::JobStatus;
use crate::mode::Mode;

// ── Message Envelope ──

/// Top-level message, length-prefixed JSON over Unix socket.
///
/// The envelope schema is fixed. Unknown envelope fields are rejected instead
/// of being silently ignored.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Message {
    Request { id: u32, payload: RequestPayload },
    Response { id: u32, payload: ResponsePayload },
    Event { payload: EventPayload },
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
    Output {
        id: String,
        data: String,
        truncated: bool,
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
    },

    CompletionList {
        items: Vec<CompletionItem>,
    },
    HighlightResult {
        spans: Vec<HighlightSpan>,
    },

    FgAttached {
        id: String,
    },
    Pong {
        /// Daemon `cued` build version reported by the running daemon.
        version: String,
    },
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

    // Scopes channel
    HeadChanged {
        old_hash: String,
        new_hash: String,
    },

    // :fg (sent only to fg-attached client)
    FgOutput {
        #[serde(with = "serde_bytes_base64")]
        data: Vec<u8>,
    },
    FgExited {
        id: String,
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
    pub data: String,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobInfo {
    pub id: String,
    pub status: JobStatus,
    pub pipeline: String,
    pub exit_code: Option<i32>,
    pub start_scope: Option<String>,
    pub end_scope: Option<String>,
    pub open_hint: JobOpenHint,
    pub chain_id: Option<String>,
    pub chain_index: Option<usize>,
    pub chain_total: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronInfo {
    pub id: String,
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
            payload: RequestPayload::Eval { input, mode },
        } = decoded
        {
            assert_eq!(id, 1);
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
    fn rich_output_payloads_roundtrip() {
        let payload = ResponsePayload::Ok(OkPayload::JobOutput {
            id: "J1".into(),
            stdout: StreamText {
                data: "out".into(),
                truncated: false,
            },
            stderr: StreamText {
                data: "err".into(),
                truncated: true,
            },
            stderr_pty_merged: false,
        });
        let json = serde_json::to_string(&payload).unwrap();
        let decoded: ResponsePayload = serde_json::from_str(&json).unwrap();
        match decoded {
            ResponsePayload::Ok(OkPayload::JobOutput { stderr, .. }) => {
                assert_eq!(stderr.data, "err");
                assert!(stderr.truncated);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn run_script_request_roundtrips() {
        let msg = Message::Request {
            id: 9,
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
            } => {
                assert_eq!(id, 3);
                assert_eq!(input, ":ru");
                assert_eq!(cursor, 3);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn script_created_and_finished_payloads_roundtrip() {
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
    fn scope_events_roundtrip_as_head_changes() {
        let msg = Message::Event {
            payload: EventPayload::HeadChanged {
                old_hash: "S@old00000".into(),
                new_hash: "S@new00000".into(),
            },
        };
        let json = serde_json::to_string(&msg).unwrap();
        let decoded: Message = serde_json::from_str(&json).unwrap();
        match decoded {
            Message::Event {
                payload: EventPayload::HeadChanged { old_hash, new_hash },
            } => {
                assert_eq!(old_hash, "S@old00000");
                assert_eq!(new_hash, "S@new00000");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn binary_payloads_serialize_as_base64_strings() {
        let msg = Message::Event {
            payload: EventPayload::FgOutput {
                data: vec![0, 1, 2, 0xfe, 0xff],
            },
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"AAEC/v8=\""));
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
    fn pong_decodes_versioned_payload() {
        let json = r#"{"Ok":{"Pong":{"version":"0.1.0"}}}"#;
        let decoded: ResponsePayload = serde_json::from_str(json).unwrap();
        match decoded {
            ResponsePayload::Ok(OkPayload::Pong { version }) => {
                assert_eq!(version, "0.1.0");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn pong_serializes_reported_version() {
        let payload = ResponsePayload::Ok(OkPayload::Pong {
            version: "0.1.0".into(),
        });
        let json = serde_json::to_string(&payload).unwrap();
        assert_eq!(json, r#"{"Ok":{"Pong":{"version":"0.1.0"}}}"#);
    }
}
