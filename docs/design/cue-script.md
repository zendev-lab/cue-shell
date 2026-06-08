# `.cue` Script Mode v1

This document defines the v1 file-script contract for cue-shell.

The short version: **a script is a `.cue` file executed with `cue run <file.cue>`**.
Interactive JOB input is not a script entry point.

## Motivation

cue-shell already has durable jobs, chains, scopes, and schedules. A script mode
should make those primitives reproducible from a file without adding a second
shell language or overloading interactive multiline input.

v1 therefore keeps the surface intentionally small:

- the file suffix is always `.cue`
- the only CLI entry is `cue run <file.cue>`
- top-level file items reuse the existing cue-shell command/chain syntax
- execution is synchronous and fail-fast
- each run is scope-isolated from the default HEAD

## CLI surface

```text
cue run path/to/file.cue
```

Rules:

- `run` is an explicit cue CLI subcommand.
- Exactly one file path is accepted in v1.
- The file path must end in `.cue`.
- `cue foo` continues to mean extension dispatch, as it does today.
- `cue ./foo.cue` path-form dispatch is not supported in v1.
- Executing a `.cue` file directly via a shebang is not supported in v1.

## File syntax

A `.cue` script is a list of top-level cue-shell items. Each item is parsed with
the same command and chain grammar used by JOB-mode input and `:run`.

Bare top-level items default to JOB-mode `:run`, so ordinary command lines do
not need an explicit `:run` prefix. Keep explicit `:` commands for cue-shell
builtins and for `:run(...)` when mode parameters are needed.

Example:

```cue
# comments are ignored
cargo fmt --check
cargo test -> cargo clippy
:run(pty=false) cat Cargo.toml |> wc -l
:cron in 5m cargo test
```

See also the repository examples:

- [`../../examples/hello.cue`](../../examples/hello.cue)
- [`../../examples/chain.cue`](../../examples/chain.cue)
- [`../../examples/fail.cue`](../../examples/fail.cue)

v1 file-specific lexical rules:

- A leading shebang line is ignored when it is the first line and starts with
  `#!`.
- `#` starts a line comment when it appears at the beginning of a line or after
  horizontal whitespace in token-boundary context.
- `#` inside quoted words is part of the word, not a comment.
- Bare non-`:` items are resolved as `:run <item>` in JOB mode while preserving
  the original source text for `R<n>` script metadata.
- Newline separates top-level items only when the current chain is syntactically
  complete.
- Newline inside an unfinished chain is formatting whitespace.

No new AST family is introduced for v1. File scripts should reuse the existing
`Ast::Script { items }` shape, with a file-source parser entry that applies the
minimal shebang/comment preprocessing rules above.

## Execution model

`cue run <file.cue>` executes top-level items in file order.

For each top-level item:

1. submit the item to the daemon
2. wait until the item reaches a terminal state
3. attach that item's stdout/stderr to the calling terminal while it runs
4. continue only if the item succeeded

Items may still contain cue-shell chain operators such as `->`, `~>`, `|||`,
`|?|`, and pipelines such as `|>` inside the item. The sequential script model
applies between top-level file items; each item keeps the normal chain semantics
inside itself.

## Exit semantics

v1 is fail-fast by default, in the spirit of `bash` with `set -e`:

- If an item exits non-zero or otherwise fails, later top-level items are not
  submitted.
- The `cue run` process exits with the failing item's exit code.
- If every item succeeds, `cue run` exits with the last item's exit code
  (normally `0`).
- Empty/comment-only scripts are invalid and should fail before submission.

There is no `:set continue_on_error`, `--continue-on-error`, or `--no-wait` in
v1.

## Output semantics

`cue run` is synchronous. It waits for script completion and attaches output in
submission order:

- stdout chunks for the current item are written to the caller's stdout
- stderr chunks for the current item are written to the caller's stderr
- output for jobs spawned by a `RunScript` request is delivered directly to the
  requesting client, while still being published on normal `output:J<n>`
  channels for other observers
- terminal `ScriptFinished` status is delivered directly to the requesting
  client, while still being published on the `jobs` channel for other observers
- the daemon remains the canonical owner of job output logs
- TUI/script history may show the `R<n> -> item -> C/J` mapping and final status

## Scope semantics

Each script run forks a fresh isolated scope from the current default HEAD before
submitting item 0.

Within the script:

- item 0 starts from the forked scope
- each later top-level item inherits the previous top-level item's `end_scope`
- `cd ...`, `env set ...`, `:run cd ...`, and `:run env set ...` affect later items in the same script
- the default HEAD is not advanced or mutated by script execution

After the run, the isolated scope lineage may remain in normal scope storage and
retention, but it is not promoted to HEAD by default.

## Run identity and persistence

File scripts reuse cue-shell's existing script-run identity:

- each run gets an `R<n>` id
- the daemon may respond with `ScriptCreated`
- `script_runs` / `script_items` storage remains the metadata store
- `max_script_runs` continues to cap retained script metadata

The meaning of `R<n>` for v1 should be: a file-script run created by
`cue run <file.cue>`.

## Interactive multiline input

Interactive JOB multiline input is **not** a script mode entry point.

The previous inline multiline batch behavior is removed from the user-facing
contract. Users who want multiple top-level items should put them in a `.cue`
file and run:

```text
cue run path/to/file.cue
```

Implementation may keep the underlying `Ast::Script`, `ScriptId`,
`ScriptCreated`, and `script_runs` machinery because the file-script path reuses
that abstraction.

## TUI representation

The TUI script card should show file-script runs with enough context to diagnose
failures:

- `R<n>` id
- source file path
- per-item source text and created job/chain ids
- terminal status
- aggregate exit code
- failed item index and source text when fail-fast stops the run

## v1 non-goals

v1 explicitly does not include:

- control flow (`if`, `for`, functions, conditionals)
- script argument passing (`$1`, `$@`, `--` forwarding)
- import/include/module system
- remote file execution as a daemon-side file path
- `:set` directives
- shebang execution
- `cue ./foo.cue` path-form dispatch
- replacing or narrowing extension dispatch (`cue foo` stays extension-first)
- inline multiline batch handling outside explicit script execution

## Implementation checklist

The agreed rollout order is:

1. add this design document and cross-links
2. add a file-source parser entry with comments/shebang handling
3. enforce fork-from-HEAD isolated scope for script submissions
4. extend IPC with file source and final aggregated status
5. add `cue run <file.cue>`
6. remove the inline multiline batch entry
7. update the TUI script card
8. add `.cue` examples and final README references
