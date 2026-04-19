//! IPC protocol types for cued ↔ client communication.
//!
//! Transport: Unix domain socket with length-prefixed JSON framing.
//! See `docs/design/ipc-protocol.md` for the full specification.

use std::ops::Range;

use serde::{Deserialize, Serialize};

use crate::agent::AgentStatus;
use crate::cron::CronStatus;
use crate::job::JobStatus;
use crate::mode::Mode;

// ── Message Envelope ──

/// Top-level message, length-prefixed JSON over Unix socket.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    Request { id: u32, payload: RequestPayload },
    Response { id: u32, payload: ResponsePayload },
    Event { payload: EventPayload },
}

// ── Requests (Client → cued) ──

/// All user commands go through `Eval`. Structured requests are only for
/// protocol-level operations not typed by the user.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RequestPayload {
    // User commands (raw string, parsed by cued)
    Eval {
        input: String,
        mode: Mode,
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
    AgentPrompt {
        id: String,
        prompt: String,
    },
    AgentCancel {
        id: String,
    },

    // Editor services
    Complete {
        input: String,
        cursor: usize,
        mode: Mode,
    },
    Highlight {
        input: String,
    },

    // System
    Ping {},
    Shutdown {},
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
    JobCreated {
        job_id: String,
        start_scope: Option<String>,
        open_hint: JobOpenHint,
        chain_id: Option<String>,
        chain_index: Option<usize>,
        chain_total: Option<usize>,
    },
    ChainCreated {
        chain_id: String,
        job_ids: Vec<String>,
        chain: ChainInfo,
    },
    AgentSpawned {
        agent_id: String,
    },
    CronAdded {
        cron_id: String,
    },
    ScopeCreated {
        hash: String,
        label: Option<String>,
        summary: String,
    },

    JobInfo(JobInfo),
    JobList(Vec<JobInfo>),
    AgentInfo(AgentInfo),
    AgentList(Vec<AgentInfo>),
    CronList(Vec<CronInfo>),
    ScopeInfo(ScopeInfo),
    ScopeList(Vec<ScopeInfo>),
    Output {
        id: String,
        data: String,
        truncated: bool,
    },

    EvalText {
        text: String,
    },
    ConfirmRequest {
        prompt: String,
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
    Pong {},
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
    ChainStarted {
        chain: ChainInfo,
    },
    ChainProgress {
        chain: ChainInfo,
    },
    ChainFinished {
        chain_id: String,
        success: bool,
    },
    JobRemoved {
        job_id: String,
    },

    // Agents channel
    AgentStateChanged {
        agent_id: String,
        old_state: AgentStatus,
        new_state: AgentStatus,
    },
    AgentMessage {
        agent_id: String,
        role: String,
        content: String,
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
    ScopeCreated {
        hash: String,
        label: Option<String>,
    },
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
    DaemonReady {},
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
pub struct AgentInfo {
    pub id: String,
    pub status: AgentStatus,
    pub backend: String,
    pub role: String,
    #[serde(default)]
    pub transcript: String,
    #[serde(default)]
    pub last_role: Option<String>,
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
    pub const INVALID_STATE: &str = "INVALID_STATE";
    pub const INVALID_SCOPE: &str = "INVALID_SCOPE";
    pub const INVALID_SYNTAX: &str = "INVALID_SYNTAX";
    pub const ALREADY_EXISTS: &str = "ALREADY_EXISTS";
    pub const NOT_SUPPORTED: &str = "NOT_SUPPORTED";
    pub const PERMISSION_DENIED: &str = "PERMISSION_DENIED";
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
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(data: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::Serialize;
        // For JSON, encode as array of numbers (more debuggable than base64 for small payloads)
        data.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        Vec::<u8>::deserialize(deserializer)
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
}
