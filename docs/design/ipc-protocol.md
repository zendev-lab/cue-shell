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

// Response (error)
{"type": "response", "id": 1, "payload": {"Err": {"code": "INVALID_SYNTAX", "message": "cue chain operator `|?|` must be surrounded by whitespace"}}}

// Request: RunScript (file-script body loaded by cue-cli)
{"type": "request", "id": 4, "payload": {"RunScript": {"path": "scripts/build.cue", "input": "cargo test\ncargo fmt -> cargo clippy"}}}

// Response (success — file script submission created)
{"type": "response", "id": 4, "payload": {"Ok": {"ScriptCreated": {"script_id": "R7", "source": {"kind": "file", "path": "scripts/build.cue"}, "items": [{"index": 0, "source": "cargo test", "result": {"kind": "job", "job_id": "J9", "start_scope": "S@32b17bec", "open_hint": "Stream"}}, {"index": 1, "source": "cargo fmt -> cargo clippy", "result": {"kind": "chain", "chain_id": "CH5", "job_ids": ["J10", "J11"], "chain": {"id": "CH5", "pipeline": "cargo fmt -> cargo clippy", "total_jobs": 2, "jobs": [{"index": 0, "pipeline": "cargo fmt", "status": "Running", "job_id": "J10", "start_scope": "S@32b17bec", "end_scope": null, "open_hint": "Stream"}, {"index": 1, "pipeline": "cargo clippy", "status": "Pending", "job_id": "J11", "start_scope": null, "end_scope": null, "open_hint": null}]}}}], "submit_error": null}}}}

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

1. Client connects to Unix socket
2. Client sends `Subscribe` request to register interest channels
3. cued responds with `Ok`
4. Bidirectional: client sends Requests, cued sends Responses (matched by `id`) + Events (no `id`)

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
- `"scopes"` — scope creation, HEAD changes
- `"system"` — cued status, shutdown notices

Channel names are a closed protocol set. `Subscribe` / `Unsubscribe` requests
with an unknown channel or an `output:<id>` channel whose id is not a Job ID are
rejected with `INVALID_REQUEST`.

Operations:

- `Subscribe { channels }` — add channels (additive, no duplicates)
- `Unsubscribe { channels }` — remove channels

The daemon does not add implicit subscriptions; clients must subscribe before
relying on pushed events from a channel.
TUI default subscription on connect: `["jobs", "crons", "system"]`
`:out J1` triggers additional: `Subscribe { channels: ["output:J1"] }`
`RunScript` is the exception to subscription-only delivery: output from jobs
spawned by that request and terminal `ScriptFinished` status are also delivered
directly to the requesting client. Output is published to other `output:J<n>`
subscribers and terminal status is published to other `jobs` subscribers, so
`cue run` does not race daemon-side execution against event-channel
subscriptions or receive duplicate direct-delivery events.

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
    Subscribe { channels: Vec<String> },
    Unsubscribe { channels: Vec<String> },

    // :fg mode (job pty attach)
    FgAttach { id: String },  // J1
    FgDetach {},
    FgInput { data: Vec<u8> },  // raw bytes from TUI keyboard
    FgResize { cols: u16, rows: u16 },  // terminal resize

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
    Output { id: String, data: String, truncated: bool },
    JobOutput { id: String, stdout: StreamText, stderr: StreamText, stderr_pty_merged: bool },

    // Eval can return any of the above depending on the parsed command.
    // Additionally, some commands produce text output:
    EvalText { text: String },  // for :help, :env list, etc.
    TextOutput { text: String, truncated: bool },
    // Editor services
    CompletionList { items: Vec<CompletionItem> },
    HighlightResult { spans: Vec<HighlightSpan> },

    FgAttached { id: String },  // J<n> = live PTY attach
    Pong { version: String },  // reports cued's build version
}

struct PageInfo {
    total: usize,
    shown: usize,
    limit: Option<usize>,
    truncated: bool,
}

struct StreamText {
    data: String,
    truncated: bool,
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
    // data: UTF-8 text; for non-UTF-8 bytes, base64-encoded with a sibling field:
    // OutputChunkBinary { id: String, stream: Stream, base64: String }
    OutputEof { id: String },  // process closed its output

    // Scope events (channel: "scopes")
    HeadChanged { old_hash: String, new_hash: String },

    // :fg events (no channel — only sent to fg-attached client)
    FgOutput { data: Vec<u8> },  // raw pty output
    FgExited { id: String, reason: String },  // process exited while in :fg

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
- `chain_id` / `chain_index` / `chain_total` let clients correlate per-job events with a serial/parallel chain without waiting for a `:jobs` refresh.
- `ChainCreated` and `ChainProgress` carry the authoritative leaf-by-leaf chain snapshot, including pending leaves that do not have job IDs yet and serial scope handoffs via `start_scope` / `end_scope`.

## 9. :fg Full-Duplex Proxy Mode

When client sends `FgAttach { id: "J1" }`:

1. cued responds `FgAttached { id: "J1" }`
2. Connection enters **fg proxy mode** for this job:
   - Client → cued: `FgInput { data }` messages (raw keystrokes)
   - cued → client: `FgOutput { data }` events (raw pty output)
   - Client → cued: `FgResize { cols, rows }` on terminal resize
3. Client sends `FgDetach {}` to exit (triggered by Ctrl+Z)
4. cued responds `Ack {}`, connection returns to normal mode

During fg mode, other Request/Response and Event messages continue normally on the same connection.

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
  |<-- Response {Ok: FgAttached} -----|
  |<-- FgOutput {data} ---------------|  (streaming)
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
