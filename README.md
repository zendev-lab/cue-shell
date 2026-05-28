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
- **`.cue` file scripts**: `cue run <file.cue>` submits one `R<n>` script with fail-fast execution
- **Foreground PTY attach**: `:fg J<n>` proxies a real terminal session with input, paste, and resize support
- **Display tabs with clean semantics**: `:out J<n>` snapshots stdout, `:tail J<n>` follows live stdout, `:err J<n>` opens stderr
- **Scope persistence**: Environment snapshots with delta storage and lifecycle management
- **Chain syntax**: `->` serial · `~>` ignore-failure · `|||` parallel · `|?|` any-success; `&&` / `||` stay inside one job
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
├── cue-client/ — Client connection stack shared by frontends
├── cue-daemon/ — Background daemon implementation library used by the `cued` CLI
├── cue-tui/    — Optional TUI extension mounted as the `cue tui` subcommand
├── cue-cli/    — CLI entry crate; builds `cue` and `cued` via `extensions`/`tui`/`daemon`
```

## Installation

```bash
# Install both `cue` and `cued` from PyPI
uv tool install cue-shell
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
cargo run -p cue-cli --bin cued -- restart

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
- **cue-script.md** — `.cue` file script contract for `cue run <file.cue>`

## Client + server config

cue-shell now prefers a split config layout in the platform config dir:

- `client.toml` — client-side transport/profile selection used by `cue`
- `server.toml` — daemon-side runtime defaults used by `cued`

During migration, cue-shell still falls back to the legacy combined
`config.toml`. If you keep using that file for now, put client transport under
`[transport]`.

### `.cue` file script mode

Scripts are `.cue` files executed through the explicit CLI entry:

```text
cue run examples/hello.cue
```

A script run gets an id such as `R12`. Top-level items run in file order with
fail-fast semantics: if one item exits non-zero, later items are not submitted
and `cue run` exits with that code. Output still belongs canonically to jobs,
while `cue run` subscribes to live output and mirrors stdout/stderr to the
calling terminal.

Interactive JOB multiline input is not a script entry point; put multi-item
workflows in a `.cue` file. See [`docs/design/cue-script.md`](docs/design/cue-script.md)
and [`examples/`](examples/).

### Client transport and extension config

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

`cue` can also dispatch external CLI extensions from `client.toml`. This is
enabled by the default `cue-cli` `extensions` Cargo feature:

```toml
[extensions.commands.foo]
command = "cue-foo"
description = "Foo extension"
```

`command` is the executable path/name; extension arguments come from the `cue foo ...`
invocation. Then `cue foo arg` runs `cue-foo arg`. Built-in subcommands such as
`tui`, `help`, and `version` take precedence. Optional PATH lookup for unknown
`cue-<name>` binaries can be enabled explicitly:

```toml
[extensions]
path_lookup = true
```

### Server runtime config

`server.toml` can cap persisted job/script history:

```toml
[retention]
max_job_history = 200
max_script_runs = 100
```

It can also enable a runtime wrapper such as `rtk`. Wrapping is allowlist-only:
commands not listed under `[wrapper.allowlist]` are never wrapped, and an empty
allowlist wraps nothing.
Legacy `[wrapper.denylist]` config is rejected with a migration error because
the old "wrap everything except..." behavior cannot be safely converted.

```toml
[wrapper]
enabled = true
binary = "rtk"

[wrapper.allowlist]
commands = ["cargo", "git", "pnpm", "node"]
```

`:wrap on/off/status` overrides only the session-level enablement. Per-command
mode params such as `:run(wrapper=false) cargo test` and cron params such as
`:cron(wrapper=true) every 5m cargo test` override enablement for that
invocation, but still must match the allowlist.

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
| cue-daemon | 🚧 Functional prototype |
| cue-tui | 🚧 Functional prototype |
| cue-cli | 🚧 Functional prototype |

## License

Licensed under the [MIT License](LICENSE).
