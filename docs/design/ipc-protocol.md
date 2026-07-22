# IPC Protocol Design — cued ↔ Client

## 1. Transport

- **Unix domain socket**: `$XDG_RUNTIME_DIR/cue-shell/cued.sock`
- Runtime fallback: `$TMPDIR/cue-shell/cued.sock` (or the OS temp directory)
- Single bidirectional connection per client
- `cued gateway --stdio` relays the exact same byte stream over stdin/stdout for SSH-style remote clients
- Phase-1 remote support uses the system OpenSSH client and a client profile with
  an explicit `gateway_command` (typically `cued gateway --stdio`) plus an
  explicit `start_command` for the manual remote daemon start step

## 2. Framing

**Length-prefixed JSON**: 4-byte big-endian u32 length header + UTF-8 JSON body.

```
[4 bytes: body length (u32 BE)] [body: UTF-8 JSON]
```

- Max message size: 16 MiB (configurable)
- JSON body is always a valid `Message` envelope

## 3. Message Envelope

All messages share a unified envelope structure:

```rust
#[serde(tag = "type")]
enum Message {
    Request  { id: u32, payload: RequestPayload },
    Response { id: u32, payload: ResponsePayload },
    Event    { payload: EventPayload },
}
```

- `id` present only on Request/Response, used for correlation
- Client assigns monotonically increasing `id` per connection
- Events have no `id` — they are server-pushed

### JSON examples

```json
// Request: Eval (core job command)
{"type": "request", "id": 1, "payload": {"Eval": {"input": ":run(pty=false) cargo test", "mode": "Job"}}}

// Response (success — Eval resolved to a serial chain)
{"type": "response", "id": 1, "payload": {"Ok": {"ChainCreated": {"chain_id": "CH1", "job_ids": ["J1"], "chain": {"id": "CH1", "pipeline": "cargo test -> cargo clippy", "total_jobs": 2, "jobs": [{"index": 0, "pipeline": "cargo test", "status": "Running", "job_id": "J1", "start_scope": "S@32b17bec", "end_scope": null, "open_hint": "Stream"}, {"index": 1, "pipeline": "cargo clippy", "status": "Pending", "job_id": null, "start_scope": null, "end_scope": null, "open_hint": null}]}}}}}

// Response (error — session-dependent request sent before Handshake)
{"type": "response", "id": 1, "payload": {"Err": {"code": "INVALID_REQUEST", "message": "client session handshake required"}}}

// Response (error)
{"type": "response", "id": 1, "payload": {"Err": {"code": "INVALID_SYNTAX", "message": "cue chain operator `|?|` must be surrounded by whitespace"}}}

// Request: RunScript (file-script body loaded by cue-cli)
{"type": "request", "id": 4, "payload": {"RunScript": {"path": "scripts/build.cue", "input": "cargo test\ncargo fmt -> cargo clippy"}}}

// Response (success — file script submission created)
{"type": "response", "id": 4, "payload": {"Ok": {"ScriptCreated": {"script_id": "R7", "source": {"kind": "file", "path": "scripts/build.cue"}, "items": [{"index": 0, "source": "cargo test", "result": {"kind": "job", "job_id": "J9", "start_scope": "S@32b17bec", "open_hint": "Stream"}}], "submit_error": null}}}}

// Event (an item created after the initial response; authoritative script-to-job association)
{"type": "event", "payload": {"ScriptItemCreated": {"script_id": "R7", "item": {"index": 1, "source": "cargo fmt -> cargo clippy", "result": {"kind": "chain", "chain_id": "CH5", "job_ids": ["J10"], "chain": {"id": "CH5", "pipeline": "cargo fmt -> cargo clippy", "total_jobs": 2, "jobs": [{"index": 0, "pipeline": "cargo fmt", "status": "Running", "job_id": "J10", "start_scope": "S@32b17bec", "end_scope": null, "open_hint": "Stream"}, {"index": 1, "pipeline": "cargo clippy", "status": "Pending", "job_id": null, "start_scope": null, "end_scope": null, "open_hint": null}]}}}}}}

// Event (script terminal aggregate status; sent directly to the RunScript requester and published on jobs for other observers)
{"type": "event", "payload": {"ScriptFinished": {"script_id": "R7", "status": "done", "exit_code": 0, "failed_item_index": null}}}

// Event (output for jobs spawned by RunScript is sent directly to the requesting client and published on output:J<n> for other observers)
{"type": "event", "payload": {"OutputChunk": {"id": "J9", "stream": "Stdout", "data": "test output\n"}}}

// Request: Subscribe (protocol command)
{"type": "request", "id": 2, "payload": {"Subscribe": {"channels": ["jobs", "crons", "output:J1"]}}}

// Event
{"type": "event", "payload": {"ChainProgress": {"chain": {"id": "CH1", "pipeline": "cargo test -> cargo clippy", "total_jobs": 2, "jobs": [{"index": 0, "pipeline": "cargo test", "status": "Done", "job_id": "J1", "start_scope": "S@32b17bec", "end_scope": "S@32b17bec", "open_hint": "Stream"}, {"index": 1, "pipeline": "cargo clippy", "status": "Running", "job_id": "J2", "start_scope": "S@32b17bec", "end_scope": null, "open_hint": "Stream"}]}}}}

// Request: Complete (editor service)
{"type": "request", "id": 3, "payload": {"Complete": {"input": ":ru", "cursor": 3}}}
{"type": "response", "id": 3, "payload": {"Ok": {"CompletionList": {"items": [{"label": ":run", "insert_text": ":run", "kind": "Command", "detail": "Run a command as a job"}]}}}}
```

## 4. Communication Model

**Request-Response + Event Stream**, multiplexed on a single connection.

Flow:

1. Client connects to Unix socket or `cued gateway --stdio`
2. Client sends `Handshake { session_id, cwd, env }`
3. cued responds with `Ok`; session-dependent requests before this point return `INVALID_REQUEST: client session handshake required`
4. Client sends `Subscribe` requests to register interest channels
5. Bidirectional: client sends Requests, cued sends Responses (matched by `id`) + Events (no `id`)

Client must be prepared to receive interleaved Response and Event messages.

## 5. Event Subscription (Channel Model)

```rust
struct SubscribeRequest {
    channels: Vec<String>,
}
```

Channel types:

- `"jobs"` — all job state changes (created, state transitions, removed)
- `"crons"` — all cron state changes
- `"output:<id>"` — stdout/stderr chunks for a specific job (e.g., `"output:J1"`)
- `"scopes"` — scope creation/list updates; session cursor moves are returned to the requesting client as `ScopeCreated` responses, not broadcast as global state
- `"system"` — cued status, shutdown notices

Channel names are a closed protocol set. `Subscribe` / `Unsubscribe` requests
with an unknown channel or an `output:<id>` channel whose id is not a Job ID are
rejected with `INVALID_REQUEST`.

Operations:

- `Subscribe { channels }` — add channels (additive, no duplicates)
- `Unsubscribe { channels }` — remove channels

Every newly connected transport must first send `Handshake`. The client chooses
a stable `session_id` for its frontend process and reports its current `cwd` and
`env`. The daemon creates a new logical session from that snapshot or reconnects
to the existing session cursor/defaults for the same id until its disconnected
TTL expires.

The daemon does not add implicit subscriptions; clients must subscribe before
relying on pushed events from a channel.
TUI default subscription on connect: `["jobs", "crons", "system"]`
`:out J1` triggers additional: `Subscribe { channels: ["output:J1"] }`
`RunScript` is the exception to subscription-only delivery: output from jobs
spawned by that request, each later `ScriptItemCreated` association, and terminal
`ScriptFinished` status are also delivered directly to the requesting client.
These events are published to the corresponding channels for other observers.
Clients must use `ScriptCreated.items` plus matching `ScriptItemCreated` events
as the authority for script membership; global `JobCreated` order is unrelated
and must never be used to infer that a job belongs to a script.

## 6. Request Types (Client → cued)

### Design: Eval-centric

Interactive user commands go through `Eval`. File scripts use the structured
`RunScript` request so the client can attach source-path metadata while cued
still owns parsing (Tokenizer → Parser → Resolver). Other structured requests
are protocol-level operations that don't correspond to user-typed commands.

```rust
enum RequestPayload {
    // === User commands (raw string, parsed by cued) ===
    Eval { input: String, mode: Mode },
    // input: raw user input, e.g. ":run(pty=false) cargo test -> cargo build"
    //        or bare input "cargo test" (cued applies mode default)
    // mode: current TUI mode (JOB/CRON) for bare input resolution

    RunScript { path: String, input: String },
    // path: user-facing .cue file path, used as script source metadata
    // input: file contents already loaded by cue-cli
    // bare items are resolved by cued with JOB-mode semantics

    // === Protocol commands (structured, not user-typed) ===

    // Connection / subscription
    Handshake {
        session_id: String,
        cwd: String,
        env: BTreeMap<String, String>,
        refresh: bool,
    },
    Subscribe { channels: Vec<String> },
    Unsubscribe { channels: Vec<String> },

    // Foreground PTY attachment. FgAttach is the legacy-compatible exclusive
    // controller entry point; the other three operations require the
    // `foreground-observers` capability.
    FgAttach { id: String },  // attach as controller, J1
    FgWatch { id: String },   // attach as a read-only observer
    FgClaimControl {},        // claim only when the controller lease is free
    FgReleaseControl {},      // remain attached, but release the lease
    FgDetach {},
    FgInput { data: Vec<u8> },           // controller only
    FgResize { cols: u16, rows: u16 },   // controller only

    // Durable shared process sessions (`named-sessions` capability)
    CreateSession { name: String },
    ListSessions {},          // active only
    AttachSession {
        selector: String,  // session name or opaque SS-... id
        refresh: bool,     // explicit recovery for a lost volatile scope only
    },
    SessionInfo { selector: Option<String> },  // None = current session

    // Reversible lifecycle (`session-archive` capability)
    ListArchivedSessions {},
    ListAllSessions {},
    ArchiveSession { selector: String },
    RestoreSession { selector: String },

    // Editor services (completion & highlighting)
    Complete { input: String, cursor: usize },
    Highlight { input: String },

    // Typed query/control APIs for non-interactive clients. These mirror common
    // Eval commands but support server-side limits, pagination metadata, and
    // typed job/cron control without overloading IDs.
    ListJobs { limit: Option<usize> },
    ListCrons { limit: Option<usize> },
    ListScopes { limit: Option<usize> },
    ShowLog { id: Option<String>, limit: Option<usize>, tail_bytes: Option<usize> },
    JobOutput { id: String, stdout_bytes: Option<usize>, stderr_bytes: Option<usize> },
    KillJob { id: String },
    // Idempotent foreground cancellation. Ack is emitted only after active
    // child processes for J<n>, CH<n>, or R<n> have stopped.
    CancelExecution { id: String },
    RemoveCron { id: String },
    ShowEnv { tail_bytes: Option<usize> },
    ShowConfig { tail_bytes: Option<usize> },

    // System
    Ping {},
    Shutdown {},
}
```

## 7. Response Types (cued → Client)

```rust
enum ResponsePayload {
    Ok(OkPayload),
    Err { code: String, message: String },
}

enum OkPayload {
    Ack {},  // generic success (Subscribe, Kill, FgDetach, etc.)
    ScriptCreated {
        script_id: String,
        source: ScriptSource,  // Inline or File { path }
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
    },  // scope snapshot used when the job starts; open_hint tells the TUI whether running jobs should open as :out or :fg
    ChainCreated { chain_id: String, job_ids: Vec<String>, chain: ChainInfo },
    CronAdded { cron_id: String },
    ScopeCreated { hash: String, summary: String },

    JobInfo(JobInfo),
    JobList(Vec<JobInfo>),
    JobListPage { jobs: Vec<JobInfo>, page: PageInfo },
    CronList(Vec<CronInfo>),   // includes persisted cron status/history for reconnect snapshots
    CronListPage { crons: Vec<CronInfo>, page: PageInfo },
    ScopeInfo(ScopeInfo),
    ScopeList(Vec<ScopeInfo>),
    ScopeListPage { scopes: Vec<ScopeInfo>, page: PageInfo },
    Output {
        id: String,
        data: String,             // compatible UTF-8 view (lossy only for binary output)
        truncated: bool,
        encoding: OutputEncoding, // "utf8" or "base64"
        base64: Option<String>,   // authoritative bytes when encoding == Base64
    },
    JobOutput { id: String, stdout: StreamText, stderr: StreamText, stderr_pty_merged: bool },

    // Eval can return any of the above depending on the parsed command.
    // Additionally, some commands produce text output:
    EvalText { text: String },  // for :help, :env list, etc.
    TextOutput {
        text: String,
        truncated: bool,
        encoding: OutputEncoding,
        base64: Option<String>,
    },
    // Editor services
    CompletionList { items: Vec<CompletionItem> },
    HighlightResult { spans: Vec<HighlightSpan> },

    SessionInfo(SessionInfo),
    SessionList(Vec<SessionInfo>),

    FgAttached(ForegroundAttachmentInfo),
    FgRoleChanged {
        id: String,
        attachment_id: u64,
        role: ForegroundRole,
        control_available: bool,
    },
    Pong {
        version: String,          // reports cued's build version
        instance_id: String,      // changes for every daemon process
        generation_id: String,    // restart-fence generation
        ready: bool,              // false while restart handoff is incomplete
        protocol_version: u32,    // current sessionized IPC protocol version
        capabilities: Vec<String>,
        // includes session-handshake-required, named-sessions,
        // foreground-observers, and other independently gated features
    }
}

struct PageInfo {
    total: usize,
    shown: usize,
    limit: Option<usize>,
    truncated: bool,
}

struct StreamText {
    data: String,             // compatible UTF-8 view
    truncated: bool,
    encoding: OutputEncoding, // defaults to Utf8 when reading legacy payloads
    base64: Option<String>,   // exact bytes for Base64 payloads
}

enum ForegroundRole {
    Controller,
    Observer,
}

struct ForegroundAttachmentInfo {
    id: String,
    attachment_id: u64,       // opaque attachment generation; 0 is legacy
    role: ForegroundRole,
    control_available: bool,
    snapshot: Vec<u8>,        // base64 in JSON; precedes live FgOutput events
    snapshot_truncated: bool,
}

struct SessionInfo {
    id: String,
    name: String,
    scope_state: SessionScopeState,  // ReadyDurable, ReadyVolatile, NeedsRefresh
    scope_hash: Option<String>,
    connected_clients: usize,
    restart_safe: bool,
    current: bool,
    created_at_ms: i64,
    updated_at_ms: i64,
    archived_at_ms: Option<i64>,  // None = active; defaults to None for legacy peers
}

// Completion item (for Complete request)
struct CompletionItem {
    label: String,
    insert_text: String,
    kind: CompletionKind,  // Command, Param, Id, Path, Operator
    detail: Option<String>,
}

// Highlight span (for Highlight request)
struct HighlightSpan {
    range: (usize, usize),  // byte offset (start, end)
    kind: HighlightKind,    // CommandPrefix, CommandName, Operator, IdRef, Error, ...
}
```

`Ping`/`Pong` is also the feature gate for typed clients. Clients retain the
advertised capabilities on the connection and propagate them when splitting a
reader from a cloneable writer. Current cue-shell clients require
`protocol_version >= 2` plus `session-handshake-required`; newer typed requests
are gated independently by their feature capability.

`FgWatch`, `FgClaimControl`, `FgReleaseControl`, and the `:watch` Eval command
require `foreground-observers`. A client must reject these locally with an
upgrade/restart error when the capability is absent; sending an unknown request
to an older daemon can terminate the transport. `FgAttach` remains available as
the legacy-compatible exclusive attach operation. After its response, a current
daemon may also emit the retained snapshot once as an epoch-0 `FgOutput` for
pre-shared-mode clients; a current non-zero attachment must ignore that event.

`CreateSession`, `ListSessions`, `AttachSession`, and `SessionInfo` similarly
require `named-sessions`. Standard connections Ping before splitting into
reader/writer halves, so the same capability decision survives initial attach
and automatic reconnect. A custom unprobed byte stream must complete Ping (or
supply its already authenticated capability set) before it can make the same
mixed-version guarantee.

`ListArchivedSessions`, `ListAllSessions`, `ArchiveSession`, and
`RestoreSession` require `session-archive`. Clients must gate all four before
writing them, including after splitting or multiplexing a connection. This
keeps an older daemon from closing the transport on an unknown enum variant.

### Named-session archive lifecycle

`ListSessions` is the normal active view; archived sessions appear only in
`ListArchivedSessions` or `ListAllSessions`. Archiving is reversible metadata,
not deletion: it preserves the session identity, scope cursor, and retained
job/terminal history. An archived session cannot be attached or own new work
until `RestoreSession` clears `archived_at_ms`.

The daemon refuses archive while the session has connected clients,
non-terminal jobs, pending script or chain work, or an owned cron. Clients must
resolve those blockers explicitly; the protocol has no force-archive and no
hard-delete operation. Repeating archive or restore is idempotent and returns
the current `SessionInfo`.

## 8. Event Types (cued → Client, pushed)

```rust
enum EventPayload {
    // Job events (channel: "jobs")
    JobStateChanged {
        job_id: String,
        old_state: JobState,
        new_state: JobState,
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
    ChainProgress { chain: ChainInfo },
    ScriptItemCreated {
        script_id: String,
        item: ScriptItemInfo,
    },
    ScriptFinished {
        script_id: String,
        status: ScriptRunStatus,
        exit_code: i32,  // `EXIT_CODE_UNAVAILABLE` (-1) when no process exit status exists
        failed_item_index: Option<usize>,
    },
    JobRemoved { job_id: String },

    // Cron events (channel: "crons")
    CronTriggered { cron_id: String, job_id: String },  // cron fired, spawned job
    CronRemoved { cron_id: String },

    // Output events (channel: "output:<id>")
    OutputChunk { id: String, stream: Stream, data: String },
    // stream: "stdout" | "stderr"
    // data is UTF-8 text. Non-UTF-8 bytes use the distinct event below and
    // must remain bytes/base64 until a display layer explicitly renders them.
    OutputChunkBinary { id: String, stream: Stream, base64: String },
    OutputEof { id: String },  // process closed its output

    // Scope cursor changes are per-session responses (`ScopeCreated`), not global events.

    // Foreground events (no channel — sent to every observer of this attachment)
    FgOutput {
        id: String,
        attachment_id: u64,
        data: Vec<u8>,
    },
    FgControlChanged {
        id: String,
        attachment_id: u64,
        control_available: bool,
    },
    FgExited {
        id: String,
        attachment_id: u64,
        reason: String,
    },

    // System events (channel: "system")
    ShuttingDown { reason: String },
}

struct JobInfo {
    id: String,
    status: JobStatus,
    pipeline: String,
    exit_code: Option<i32>,
    start_scope: Option<String>,
    end_scope: Option<String>,
    open_hint: JobOpenHint,
    chain_id: Option<String>,
    chain_index: Option<usize>,
    chain_total: Option<usize>,
}

struct ChainInfo {
    id: String,
    pipeline: String,
    total_jobs: usize,
    jobs: Vec<ChainJobInfo>,
}

struct ScriptItemInfo {
    index: usize,
    source: String,
    result: ScriptItemResult,
}

enum ScriptSource {
    Inline,
    File { path: String },
}

enum ScriptRunStatus {
    Done,
    Failed,
}

enum ScriptItemResult {
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
    Cron { cron_id: String },
    Message { text: String },
}

struct ScriptSubmitError {
    index: usize,
    source: String,
    code: String,
    message: String,
}

struct ChainJobInfo {
    index: usize,
    pipeline: String,
    status: JobStatus,
    job_id: Option<String>,
    start_scope: Option<String>,
    end_scope: Option<String>,
    open_hint: Option<JobOpenHint>,
}

enum JobOpenHint {
    Stream,
    Fg,
}
```

Job scope fields are intentionally split:

- `start_scope` is the scope snapshot used when the job was created.
- `end_scope` is optional and becomes meaningful on terminal updates / snapshots.
- For no-side-effect jobs, `end_scope` may equal `start_scope`.
- `open_hint` is a server-computed display hint for running jobs: `Stream` means the preferred open action is `:out`; `Fg` means the preferred open action is `:fg`.
- Clients should merge repeated `JobStateChanged` events by `job_id` and treat a later non-`None` `end_scope` as authoritative.
- `Cancelled(reason)` remains structured on the wire. Compatibility clients may
  display the status as `Cancelled`, but must retain `User`, `ChainAborted`, or
  `Timeout`; Spark exposes it as `cancelReason` for the event's `new_state`.
- Buffered output always reports truncation. When `encoding` is `base64`, the
  `base64` field is authoritative and `data`/`text` is only an explicit lossy
  compatibility view.
- `chain_id` / `chain_index` / `chain_total` let clients correlate per-job events with a serial/parallel chain without waiting for a `:jobs` refresh.
- `ChainCreated` and `ChainProgress` carry the authoritative leaf-by-leaf chain snapshot, including pending leaves that do not have job IDs yet and serial scope handoffs via `start_scope` / `end_scope`.

## 9. Shared Foreground PTY Mode

Each connection may observe at most one PTY job at a time. A job may have many
observers but exactly one controller lease:

1. `FgAttach { id: "J1" }` atomically registers the connection as controller,
   or returns an error when another controller owns the lease.
2. `FgWatch { id: "J1" }` atomically registers a read-only observer without
   taking or stealing the controller lease.
3. cued returns `FgAttached(ForegroundAttachmentInfo)`. `snapshot` is the byte
   history captured at the registration cut; matching live `FgOutput` events
   are held until that response is written, so clients append snapshot then
   live bytes without a gap or duplicate interval.
4. Every subsequent foreground response/event carries both job `id` and opaque
   `attachment_id`. Clients must mutate a foreground view only when both match
   the active attachment. Legacy payloads decode with `attachment_id = 0`, and
   a legacy attachment accepts only legacy events whose attachment ID is also
   zero.
5. Only the controller may send `FgInput` or `FgResize`. An observer may call
   `FgClaimControl` only while `control_available` is true. The current
   controller calls `FgReleaseControl` to remain attached as an observer; there
   is no implicit steal or force-takeover path.
6. A successful claim/release returns `FgRoleChanged`; all observers receive
   `FgControlChanged` when lease availability changes. A TUI uses Ctrl+] for
   claim/release and shows failures in the foreground footer.
7. `FgDetach {}` (Ctrl+Z), connection close, or named-session switch removes
   the observer and releases its controller lease. `FgExited` closes only the
   matching attachment generation.

Other Request/Response and Event messages continue normally on the same
connection while foreground mode is active.

Foreground attach is intentionally connection-local. An `Eval` resolving to
`:fg` or `:watch` must not carry `operation_id`, because a replayed attachment
response cannot recreate registration on a different transport. `RunScript`
rejects any item resolving to either command with `NOT_SUPPORTED`; clients
reattach the named session and issue a fresh interactive foreground request.

## 10. Error Codes

Standard error codes returned in `Err { code, message }`:

| Code                | Meaning                                                      |
| ------------------- | ------------------------------------------------------------ |
| `NOT_FOUND`         | Job/Cron/Scope not found                                     |
| `INVALID_STATE`     | Operation not valid in current state (e.g., :fg on Done job) |
| `INVALID_SCOPE`     | Referenced scope hash not found                              |
| `INVALID_SYNTAX`    | Malformed pipeline/chain/cron expression                     |
| `ALREADY_EXISTS`    | Duplicate operation (e.g., already fg-attached)              |
| `NOT_SUPPORTED`     | Operation not supported                                      |
| `PERMISSION_DENIED` | Operation rejected by policy                                 |
| `INTERNAL`          | Unexpected cued error                                        |

## 11. Connection Lifecycle

```
Client                              cued
  |                                   |
  |--- connect (Unix socket) -------->|
  |--- Handshake {session...} ------->|
  |<-- Response {Ok: Ack} ------------|
  |--- Ping {} ---------------------->|
  |<-- Pong {capabilities...} --------|
  |                                   |
  |--- Subscribe {channels} --------->|
  |<-- Response {Ok: Ack} ------------|
  |                                   |
  |--- RunJob {pipeline} ------------>|
  |<-- Response {Ok: JobCreated} -----|
  |<-- Event {JobStateChanged} -------|  (async)
  |<-- Event {OutputChunk} -----------|  (if subscribed)
  |                                   |
  |--- FgAttach {id: "J1"} ---------->|
  |<-- FgAttached {snapshot, role...} -|
  |<-- FgOutput {id, attachment_id...}-|  (streaming)
  |--- FgInput {data} --------------->|  (keystrokes)
  |--- FgDetach {} ------------------->|
  |<-- Response {Ok: Ack} ------------|
  |                                   |
  |--- close connection -------------->|
```

## Design Notes

- All string IDs (J1, A2, C3) are used consistently across Request/Response/Event
- ModeParams is a `HashMap<String, Value>` matching the `()` syntax
- cued must buffer recent output per job for `GetOutput` (tail query) — configurable ring buffer
- Multiple clients can connect simultaneously; each has independent subscriptions
- Future: WebSocket bridge for remote access (same JSON protocol, different transport)
