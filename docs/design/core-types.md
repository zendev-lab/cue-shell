# Cue Shell — core type contracts

This page records the domain model and invariants. It intentionally does not
mirror Rust field definitions; the source of truth for concrete structs,
serialization, and command metadata is the code:

- Core IDs, jobs, crons, chains, scopes, resources, and IPC payloads:
  `crates/cue-core/src/`
- Scope schema: `crates/cue-core/src/scope.rs`
- Job launch options: `crates/cue-core/src/job.rs`
- Command and mode-param metadata: `crates/cue-core/src/command_spec.rs`
- Daemon scheduling/runtime state: `crates/cue-daemon/src/actor/`
- Persistence schema/roundtrips: `crates/cue-daemon/src/storage.rs`

## IDs

User-facing IDs are compact display handles:

- Jobs: `J<n>`
- Cron entries: `C<n>`
- Chains: `CH<n>`
- Script runs: `R<n>`
- Scopes: `S@<short-content-hash>`

Use the concrete newtypes in `cue-core` when changing parsing, IPC, or storage.

## Scopes

A scope is an immutable, content-addressed snapshot. It carries the environment
and working directory only. Logical client sessions own mutable scope cursors;
`cwd=...` on `:run(...)` and `:cron(...)` derives child start scopes without
moving the caller's session cursor.

Rules:

- Scope identity is content-addressed; equivalent snapshots share a hash.
- Scope deltas point at a parent and inherit unspecified fields.
- `:env set` and `:cd` advance only the current session cursor.
- `cwd=...` launcher mode params derive a start scope for that job/cron only.
- Resource needs, PTY choices, and sandbox settings are launch options, not
  environment variables and not scope identity.

Implementation references:

- `crates/cue-core/src/scope.rs`
- `crates/cue-daemon/src/actor/scope_store.rs`
- `crates/cue-daemon/src/runtime_env.rs`

## Jobs

A job is a durable execution record created from one fixed start scope. Terminal
jobs keep their observed exit status and, when scope mutation is enabled for a
chain leaf, may also record an end scope.

Lifecycle summary:

```text
Pending -> Running -> Done | Failed | Killed | Cancelled(reason)
```

Rules:

- Jobs never move when a session cursor changes.
- Output belongs to jobs; scripts/chains only aggregate job outcomes.
- A missing process exit status is represented explicitly in code; do not infer
  success from missing status.

Implementation references:

- `crates/cue-core/src/job.rs`
- `crates/cue-daemon/src/actor/scheduler.rs`
- `crates/cue-daemon/src/actor/process_mgr.rs`

## Crons

Cron entries are recurring factories for jobs. They capture the scope and mode
settings needed to launch future jobs predictably.

Rules:

- `:cron(cwd=...)` stores cwd in the cron scope.
- Restored legacy cron records may still carry a daemon-side cwd override; new
  records should prefer captured start scopes.
- Cron removal and job cancellation are separate lifecycle operations.

Implementation references:

- `crates/cue-core/src/cron.rs`
- `crates/cue-daemon/src/actor/scheduler.rs`
- `crates/cue-daemon/src/actor/cron_schedule.rs`

## Pipelines and chains

Cue-shell has two composition layers:

- **Pipeline**: process-level composition inside one job (`|>`, `|&>`, `|!>`).
- **Chain**: job-level composition across jobs (`->`, `~>`, `|||`, `|?|`).

Use the parser and pipeline modules as the source of truth for exact AST and
operator semantics:

- `crates/cue-core/src/pipeline.rs`
- `crates/cue-daemon/src/parser/`
- `docs/design/commands-and-modes.md`

## Mode params and command metadata

Mode params are execution metadata in the command prefix, not argv. The canonical
support matrix is `crates/cue-core/src/command_spec.rs`; parser behavior and
snapshot tests live under `crates/cue-daemon/src/parser/parse.rs`.

Current design intent:

- `cwd` is scope-affecting where supported; it derives a start scope without
  moving the current session cursor.
- Explicit PTY, resource needs, and sandbox settings are launch options for the
  job request; they do not change scope hashes.
- `need.<resource>` is provider-owned and currently accepted only for `:run`.
- Sandbox mode params are currently accepted only for `:run`.
- Unsupported mode params fail during parsing or command resolution instead of
  being ignored.

For user-facing command syntax, see `docs/design/commands-and-modes.md`.
