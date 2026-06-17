# cue-shell

Durable process substrate with a TUI for managing long-lived jobs, scopes, and schedules.

> ⚠️ **Pre-1.0** — core JOB / CRON flows, `.cue` scripts, real `:fg` PTY
> attach, client target resolution, and the official command set are implemented.
> Public contracts may still change before 1.0. Agent runtime concerns live above cue-shell.

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
- **Daemon durability**: job history, cron definitions, and per-session cursor reconnects

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
├── cue-client/ — Client connection stack and `cue-client` CLI for target/run commands
├── cue-daemon/ — Background daemon library plus `cue-daemon` / `cued` CLIs
├── cue-tui/    — Interactive TUI frontend plus `cue-tui` CLI
├── cue-cli/    — `cue` aggregator entrypoint for explicit namespaces and extensions
```

## Installation

```bash
# Install the official command set from PyPI:
# `cue`, `cue-client`, `cue-tui`, `cue-daemon`, and `cued`.
uv tool install cue-shell
```

## Development

```bash
# Prerequisites: Rust 1.95+, just

# Build
cargo build

# Start daemon in foreground
cued -f

# Show the top-level aggregator help
cargo run -p cue-cli --

# Start TUI (auto-connect / auto-reconnect)
cargo run -p cue-tui --bin cue-tui
# or through the aggregator
cargo run -p cue-cli -- tui

# Restart the daemon directly
cargo run -p cue-daemon --bin cue-daemon -- restart
# `cued` remains as a daemon alias
cargo run -p cue-daemon --bin cued -- restart

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

## Client + daemon config

cue-shell uses split config files in the platform config dir:

- `client.toml` — client-side transport/profile selection used by `cue-client`, `cue-tui`, and `cue client ...`
- `daemon.toml` — daemon-side runtime defaults used by `cue-daemon` / `cued`

### `.cue` file script mode

Scripts are `.cue` files executed through the client CLI, with a top-level shortcut retained by the aggregator:

```text
cue-client run examples/hello.cue
cue client run examples/hello.cue
cue run examples/hello.cue
```

A script run gets an id such as `R12`. Top-level items run in file order with
fail-fast semantics: if one item exits non-zero, later items are not submitted
and `cue run` exits with that code. Bare non-`:` items default to `:run`, while
explicit `:` commands remain available for builtins and `:run(...)` mode params.
Output still belongs canonically to jobs, while the daemon delivers script job
output and terminal script status directly to the `cue run` process so
stdout/stderr and the final exit code do not depend on event-channel
subscriptions.

Interactive JOB multiline input is not a script entry point; put multi-item
workflows in a `.cue` file. See [`docs/design/cue-script.md`](docs/design/cue-script.md)
and [`examples/`](examples/).

### Client transport and extension config

`cue-client` and `cue-tui` default to a local Unix socket profile, so local users do not need any config for the current flow. The top-level `cue` command is an explicit aggregator: bare `cue` prints help, `cue client ...` forwards to `cue-client ...`, `cue tui ...` forwards to `cue-tui`, and `cue daemon ...` forwards to `cue-daemon`. Target/profile commands are intentionally namespaced under the client surface; use `cue-client target ...` or `cue client target ...` rather than `cue target ...`.

To make the split explicit:

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

The `local` profile name is reserved for Unix socket transport; use another
profile name for SSH targets.

Phase 1 uses the system OpenSSH client and runs the configured gateway command
over SSH, so the client speaks the same IPC through `cued gateway --stdio`.
Remote daemon startup still stays explicit: `cue` will **not** run
`start_command` for you. `cue-client` owns client-side transport parsing and
resolution, so CLI/TUI frontends share the same `client.toml` behavior.

When `auto_detect_ssh` is enabled, cue-shell adds implicit SSH profiles from
`~/.ssh/config`. Additional cluster inventory can be configured generically
without hardcoding scheduler-specific names in cue-shell:

```toml
[transport.discovery]
# Values are host lists separated by comma, semicolon, or whitespace.
env_hosts = ["CLUSTER_HOSTS"]
# Values are endpoint lists; cue-shell extracts hosts from host:port or URI values.
env_endpoints = ["CLUSTER_ENDPOINTS"]
# Values point to files containing one host per line or host plus extra columns.
env_hostfiles = ["CLUSTER_HOSTFILE"]
# Values use bracket range syntax such as gpu-[01-03,08].
env_bracket_ranges = ["CLUSTER_NODELIST"]
```

Site-specific schedulers should be wired through this config or through external
`cue-*` extensions rather than hardcoded in the cue-shell core. Other frontends,
including Pi cue integrations, should use the cue-client resolver if they need to honor `client.toml`:

```text
cue-client target resolve --json
cue client target resolve --json
cue-client target list --json
```

Direct daemon/socket integrations only see the server side and will not apply client profile selection by themselves.

`cue` can also dispatch external CLI extensions from `client.toml` after checking aggregator namespaces and direct shortcuts:

```toml
[extensions.commands.foo]
program = "cue-foo"
description = "Foo extension"
```

`program` is the executable path/name; extension arguments come from the `cue foo ...`
invocation. Then `cue foo arg` runs `cue-foo arg`. Aggregator namespaces and shortcuts such as `client`, `tui`, `daemon`, `run`, `target`, `help`, and `version` take precedence, and extension names must be kebab-case without colliding with built-in or first-party subcommands. Optional PATH lookup for unknown `cue-<name>` binaries can be enabled explicitly:

```toml
[extensions]
path_lookup = true
```

### Daemon runtime config

`daemon.toml` can block unsafe commands or command arguments and attach remediation hints.
Block rules run before advisory warnings, so a command can both warn generally
and fail fast for specific arguments.

```toml
[block.commands]
sh = "Avoid shell wrappers. Use cue-shell direct-exec, cwd=..., or cue operators."

[block.commands.git]
"--no-verify" = "Run the commit normally; if hooks fail, inspect and fix the hook/check."

[block.commands.npm]
"--force" = "Use the lockfile and normal install path."

[warn.commands]
cd = "Prefer cwd=... over cd in command strings."
```

Matching is literal, not glob or regex based:

- `[block.commands] sh = "..."` blocks a command whose `argv[0]` basename is exactly `sh`. It matches `sh` and `/bin/sh`; it does not match `zsh`, `/bin/zsh`, or `shellcheck`.
- Each `[block.commands.<name>]` entry maps one blocked argument pattern to its remediation hint. The command name is also matched by exact `argv[0]` basename.
- Argument patterns are checked against each argv token independently, not against the joined command line. `"--no-verify"` matches an argument token `--no-verify`; it also matches `--no-verify=...` via the `--flag=value` convention.
- `[warn.commands]` maps an exact command basename to an advisory hint.

`daemon.toml` can cap persisted job/script history:

```toml
[retention]
max_job_history = 200
max_script_runs = 100
```

It can also enable a runtime wrapper such as `rtk`. Wrapping is allowlist-only:
commands not listed under `[wrapper.allowlist]` are never wrapped, and an empty
allowlist wraps nothing.

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

If the remote daemon is not running (or its socket is missing), `cue` starts the
TUI offline and keeps retrying the configured gateway. `cue run <file>` still
fails immediately because file-script execution needs a live daemon.

## Project Status

| Component | Status |
|-----------|--------|
| Design docs | ✅ Active |
| Cargo workspace | ✅ Multi-crate workspace |
| CI/CD | ✅ Tests, package smokes, PyPI/GitHub release path |
| cue-core | ✅ Core types / IPC / parser in place |
| cue-client | ✅ Transport profiles / target JSON / script runner |
| cue-daemon | ✅ Durable jobs / crons / scopes / PTY attach |
| cue-tui | ✅ Interactive job+cron frontend / reconnect view |
| cue-cli | ✅ Aggregator / extension dispatch / PyPI command wrappers |

## License

Licensed under the [MIT License](LICENSE).
