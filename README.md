# cue-shell

Durable process substrate with a TUI for managing long-lived jobs, scopes, and schedules.

> ⚠️ **Prototype stage** — core JOB / CRON flows are implemented, including
> real `:fg` PTY attach. Agent runtime concerns live in weft.

## Overview

cue-shell (`cue`) is a terminal-native runtime for durable async processes. It is **not** a traditional shell — it's a structured environment where jobs, scopes, chains, and crons are first-class primitives.

### Key Features

- **Three-layer architecture**: Process substrate (`cued` daemon) → Core model → Frontends (TUI/MCP/API)
- **Primary interaction modes**: JOB ⚡ · CRON ⏰ — switch with `Shift+Tab`
- **`:` prefix commands**: Vim-style builtin access (`:run`, `:kill`, `:jobs`, `:cron`, ...)
- **Multiline script submission**: multiline JOB input submits one `R<n>` script that fans out into async chains/jobs
- **Foreground PTY attach**: `:fg J<n>` proxies a real terminal session with input, paste, and resize support
- **Display tabs with clean semantics**: `:out J<n>` snapshots stdout, `:tail J<n>` follows live stdout, `:err J<n>` opens stderr
- **Scope persistence**: Environment snapshots with delta storage and lifecycle management
- **Chain syntax**: `->` serial · `~>` ignore-failure · `||` parallel · `||?` any-success
- **Daemon durability**: persisted HEAD scope, job history, cron definitions, auto-reconnect TUI

## Architecture

```
┌─────────────────────────────────────────┐
│  L3 Frontend: TUI / MCP / REST API      │
├─────────────────────────────────────────┤
│  L2 Core model (cue-core)                │
│  Job · Scope · Chain · Cron             │
├─────────────────────────────────────────┤
│  L1 Process substrate (cued daemon)     │
│  Unix socket · SQLite · Process mgmt    │
└─────────────────────────────────────────┘
```

## Workspace Structure

```
crates/
├── cue-core/   — Core types and logic: Job, Scope, Chain, Cron
├── cued/       — Background daemon: Unix socket server, job orchestration
├── cue-tui/    — TUI frontend: mode switching, command input, job display
└── cue-cli/    — CLI entry point: command parsing, mode dispatch
```

## Development

```bash
# Prerequisites: Rust 1.90+, just

# Build
cargo build

# Start daemon in foreground
cued -f

# Start TUI (auto-connect / auto-reconnect)
cargo run -p cue-cli -- tui

# Restart the daemon directly
cargo run -p cued -- restart

# Restart from inside the TUI
:restart

# Run checks
just check

# Run tests
just test

# Full CI locally
just ci

# Install pre-commit hooks
just pre-commit-install
```

## Design Documents

See [`docs/design/README.md`](docs/design/README.md) for the design index:

- **Design overview** — Three-layer architecture, crates, primitives, IPC summary
- **conceptual-model.md** — Jobs/scopes indexing, sequential composition, atomic tool surface
- **commands-and-modes.md** — Command reference, mode system, `:cron` syntax

## Client + server config

cue-shell now prefers a split config layout in the platform config dir:

- `client.toml` — client-side transport/profile selection used by `cue`
- `server.toml` — daemon-side runtime defaults used by `cued`

During migration, cue-shell still falls back to the legacy combined
`config.toml`. If you keep using that file for now, put client transport under
`[transport]`.

### Multiline script mode

In JOB mode, multiline input is treated as one script submission. Each top-level
line becomes one chain submission, and cue returns a stable `R -> C -> J`
mapping:

```text
cat _typos.toml |> rg files
|| cat Cargo.toml |> rg author
```

- the submission gets a script id such as `R12`
- items are dispatched asynchronously, but cue waits for item `N`'s creation ack
  before submitting item `N+1`
- canonical output still belongs to jobs (`:out J<n>`, `:tail J<n>`, `:err J<n>`)
- the TUI shows one script card summarizing the `R -> C -> J` mapping

### Client transport config

`cue` defaults to a local Unix socket profile, so local users do not need any
config for the current flow. To make the split explicit:

```toml
[transport]
default_profile = "local"

[transport.profiles.local]
transport = "unix"
# socket = "/custom/path/to/cued.sock"

[transport.profiles.remote-dev]
transport = "ssh"
destination = "user@example.com"
gateway_command = "cued gateway --stdio"
start_command = "cued start"
```

Phase 1 uses the system OpenSSH client and runs the configured gateway command
over SSH, so the client speaks the same IPC through `cued gateway --stdio`.
Remote daemon startup still stays explicit: `cue` will **not** run
`start_command` for you.

### Server retention config

`server.toml` can now cap persisted job/script history:

```toml
[retention]
max_job_history = 200
max_script_runs = 100
```

Typical remote flow:

```bash
# Step 1: start the remote daemon explicitly
ssh user@example.com "cued start"

# Step 2: connect with the SSH transport profile
cue
```

If the remote daemon is not running (or its socket is missing), `cue` fails
with a message that includes the profile's explicit `start_command`.

## Project Status

| Component | Status |
|-----------|--------|
| Design docs | ✅ Active |
| Cargo workspace | ✅ Scaffolded |
| CI/CD | ✅ Configured |
| cue-core | ✅ Core types / IPC / parser in place |
| cued daemon | 🚧 Functional prototype |
| cue-tui | 🚧 Functional prototype |
| cue-cli | 🚧 Functional prototype |

## License

Licensed under the [MIT License](LICENSE).
