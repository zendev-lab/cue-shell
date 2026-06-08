# Extension Registry and Runtime Wrapper Research

> Status: research note; implementation TODOs identified.
> Date: 2026-05-20

## Question

Two related but distinct extension points are under consideration:

1. A generic `cue` CLI extension registry, scoped to external subcommand registration (`cue foo` → `cue-foo`).
2. Runtime command wrappers, initially for `rtk`, with allowlist-only targeting and possible future multiple-wrapper composition.

This note summarizes the current repository state, implementation complexity, design options, and recommended next steps.

## Current repo state

### CLI / TUI extension shape

`cue-tui` is an optional Cargo dependency of `cue-cli` behind the `tui` feature. When that feature is enabled, `cue` and `cue tui` enter the TUI in-process. When `cue` is built without `tui` but with `extensions`, `cue tui` dispatches to the first-party external `cue-tui` companion binary next to `cue`.

A near-term external CLI extension registry now exists:

- manifest loader for explicitly configured CLI extensions via `[extensions.commands.<name>]` in `client.toml`;
- optional external binary discovery via `[extensions] path_lookup = true`, dispatching `cue foo` → `cue-foo`;
- no dynamic plugin ABI;
- no wasm/plugin runtime.

Existing future-looking docs are limited to `docs/design/daemon-architecture.md`, which mentions:

- WASM plugins like Zellij;
- extensions get their own Actor with a constrained message interface;
- resource limits via WASM fuel metering;
- extension manifest in config;
- multi-threaded Tokio was chosen partly to keep this option open.

That is architectural direction, not an implementation plan.

### Runtime wrapper state

The daemon has a single generic wrapper config with allowlist-only targeting:

```toml
[wrapper]
enabled = true
binary = "rtk"

[wrapper.allowlist]
commands = ["cargo", "git", "pnpm", "node"]
```

A command is wrapped only if it is explicitly listed. Empty/missing allowlist wraps nothing.

Relevant code:

- `crates/cue-daemon/src/config.rs`: parses `WrapperConfig` with `enabled`, `binary`, and `allowlist.commands`.
- `crates/cue-daemon/src/actor/scheduler.rs`: owns session-level `:wrap on/off/status`, applies per-invocation `wrapper` params, and persists cron wrapper enablement.
- `crates/cue-daemon/src/actor/process_mgr.rs`: applies wrapper prefix per spawned process segment.
- `crates/cue-core/src/command.rs`: has `ModeParams::wrapper_enabled()`.
- `crates/cue-core/src/pipeline.rs`: shares foreground-command detection for wrapper bypass/open hints.

Implemented behavior:

1. Wrapper applies to single-segment pipe/PTY jobs and native multi-segment pipelines.
2. Logical jobs (`&&` / `||`) receive/use wrapper state in streaming execution.
3. `:run(wrapper=true/false)` overrides config/session enablement while still respecting allowlist/idempotence.
4. Foreground-like commands are not wrapped.
5. Cron entries persist resolved wrapper enablement with `wrapper_enabled` in schema v12.

## Generic CLI extension registry: how hard?

It depends heavily on which level of extension is meant.

### Level 0 — static feature subcommands

Still supported for `cue tui` when `cue-cli` is built with the `tui` feature.

- Complexity: already present.
- Good for first-party compiled modules.
- Bad for third-party / local extension discovery.

### Level 1 — external subcommand dispatch (`cue-foo`)

`cue` discovers and execs external binaries by name, similar to `git-foo`, `cargo-foo`, `kubectl-foo`, etc.

Example:

```text
cue foo arg1 arg2
  -> find executable `cue-foo` on PATH
  -> exec `cue-foo arg1 arg2`
```

Optional config/registry can add metadata:

```toml
[extensions.commands.foo]
program = "cue-foo"
description = "Foo extension"
```

`program` is an executable path/name; arguments are forwarded from the `cue foo ...` invocation.

Complexity: low to medium.

Implementation sketch:

1. Keep known builtins (`tui`, `help`, `version`) first.
2. On unknown non-reserved subcommand, search the configured extension program
   table, then optionally PATH for `cue-{subcommand}` when `path_lookup = true`.
3. Treat first-party external subcommands such as `tui` as reserved extension names that use companion binary lookup only; user PATH discovery remains limited to non-reserved extensions when `path_lookup = true`.
4. Use `std::process::Command` for the current implementation; Unix `exec`-style replacement can be added later if needed.
5. Forward args and environment unchanged.
6. Add `cue extensions list` later if desired.

Pros:

- Small, familiar UX.
- No plugin ABI.
- No daemon trust boundary problem.
- Extensions are just programs; cue-shell remains process substrate.

Cons:

- No in-process UI composition.
- No direct access to internal Rust types.
- Extension author must speak CLI/IPC if it needs daemon state.

This is the recommended first implementation if the goal is a "generic extension registry" for CLI commands.

### Level 2 — manifest-backed CLI registry

Same as external subcommand dispatch, but with first-class manifest/config metadata.

Possible config:

```toml
[extensions]
enabled = ["foo", "bar"]

[extensions.commands.foo]
program = "cue-foo"
description = "Foo extension"
# aliases = ["f"] # TODO
```

Complexity: medium.

Additional decisions:

- Config file: `client.toml` is more appropriate for CLI extension discovery than `daemon.toml`.
- Precedence: built-in and first-party subcommands win; there is no current
  shadowing knob.
- Names: use kebab-case and reject names colliding with built-in or first-party
  subcommands by default.
- Trust: config-controlled extensions should be explicit, but PATH discovery can be opt-in/opt-out.

Pros over Level 1:

- Discoverable help/listing.
- Stable place for aliases and descriptions.
- Can gate untrusted PATH scanning.

Cons:

- More config/schema surface.
- Needs doc/test work to avoid drift.

### Level 3 — daemon extension actors / WASM plugins

This is what the daemon architecture doc hints at: extension code runs inside/alongside `cued` with constrained Actor messages.

Complexity: high.

Open questions:

- ABI and SDK shape.
- Sandboxing: WASM fuel, WASI permissions, file/network access.
- Persistence/state ownership.
- Upgrade/reload semantics.
- Event subscription permissions.
- API stability across cue-shell versions.

This should not be the first step. It is a different product surface than CLI subcommand extension.

### Level 4 — TUI component/plugin extensions

Let third-party code add panes/widgets/keybindings inside `cue-tui`.

Complexity: high.

This intersects with ratatui rendering, event loop ownership, focus/keybinding conflicts, and safety. It should be treated separately from CLI extension registry.

## Recommendation for CLI registry

Decision: use external subcommand registration as the near-term extension mechanism.

Recommended phased plan:

1. Level 1 dispatch: built-in/first-party names first, then configured program,
   then optional PATH lookup for `cue-{name}`.
2. Add `cue extensions list` once a manifest table exists.
3. Keep daemon extension actors and TUI plugins as future research, not current implementation.

This is not very hard if scoped to external subcommands. It becomes hard only if "extension" means in-process daemon/TUI plugins.

Implemented near-term behavior:

- built-in and first-party subcommands win by default;
- configured `[extensions.commands.<name>]` entries for non-reserved names are
  checked before PATH;
- PATH lookup is gated by `[extensions] path_lookup = true`;
- `cue tui` can fall through to the first-party external `cue-tui` companion binary next to `cue` when the in-process `tui` feature is not compiled in;
- unknown extensions receive forwarded args and environment.

TODO:

- [ ] Add `cue extensions list`.
- [ ] Add alias support if needed.
- [ ] Consider Unix `exec` replacement instead of spawn-and-exit.

## Runtime wrappers: design goals

Wrappers are not general CLI extensions. They transform job execution by changing the process argv. For example:

```text
cargo test
  -> rtk cargo test
```

Important constraints from cue-shell design:

- cue-shell should remain mechanism, not policy.
- Native pipelines should preserve per-segment observability; no `sh -c` fallback.
- Wrapping should be explicit and predictable; broad interception is risky.
- Interactive commands should not be accidentally wrapped into unusable PTY behavior.
- Wrappers must be idempotent.

## Current single-wrapper behavior

Before implementing multiple wrappers, keep the existing single-wrapper semantics minimal:

1. Use allowlist-only targeting.

   Target config:

   ```toml
   [wrapper]
   enabled = true
   binary = "rtk"

   [wrapper.allowlist]
   commands = ["cargo", "git", "pnpm", "node"]
   ```

   Behavior: only commands in `allowlist.commands` may be wrapped. If the allowlist is absent or empty, no commands are wrapped.

2. Use mode param override.

   Effective wrapper enablement should be:

   ```rust
   params.wrapper_enabled().unwrap_or_else(|| state.wrapper_enabled(config))
   ```

   for `:run` and `:cron` registration.

3. Persist cron wrapper behavior.

   If `:cron(wrapper=false)` is meaningful, the cron entry must remember it. Options:

   - persist `wrapper_enabled: Option<bool>` on cron;
   - persist only the resolved boolean at creation time;
   - do not support wrapper mode params on cron yet and remove/update docs.

   Implemented: persist the resolved boolean for cron runs. A cron created with wrapper on keeps using wrapper on until edited.

4. Apply wrapper to every spawned process segment.

   - Single PTY/pipe job: wrapper hook.
   - Native multi-segment pipeline: wraps each segment independently if allowed.
   - Logical job (`&&`/`||`): passes wrapper config/state into `run_job_plan_streaming`, then reuses the same native pipeline spawn path.

5. Make interactive bypass effective.

   The wrapper decision needs command classification. Currently similar logic lives in scheduler as open-hint classification. This should be shared or moved to `cue-core` to avoid drift.

   Implemented helper:

   ```rust
   command_prefers_foreground(command_line: &[String]) -> bool
   ```

   Wrapper checks pass the real foreground preference to `should_wrap`.

## Multiple wrappers: design space

Multiple wrappers are future work. Record the design space and TODOs now, but do not implement multi-wrapper semantics in the current single-wrapper fix.

TODO:

- [ ] Decide whether multiple wrappers are selected by first-match, chain, or per-command policy.
- [ ] Define wrapper priority ordering and whether earlier priority means outer or inner wrapper.
- [ ] Define idempotence rules so a command is not wrapped repeatedly.
- [ ] Decide whether replace/short-circuit belong in wrapper semantics or separate guardrail/blocking semantics.
- [ ] Add tests before enabling more than one wrapper.

Multiple wrappers can mean several different semantics. Do not implement until these are explicit.

### Wrapper model

Possible future config:

```toml
[wrappers.rtk]
enabled = true
binary = "rtk"
mode = "prefix"
priority = 10
commands = ["cargo", "git", "pnpm"]
interactive = "skip"

[wrappers.tracer]
enabled = true
binary = "cue-trace"
mode = "prefix"
priority = 20
commands = ["cargo"]

[wrapper_policy]
default = "first_match" # or "chain"
```

A wrapper decision can be represented as:

```text
Decision::Skip(reason)
Decision::Wrap { binary, args, policy }
Decision::ShortCircuit { response/error }
Decision::Replace { program, args }
```

### Composition semantics

#### 1. First match / short-circuit

Only the first matching wrapper applies.

```text
cargo test
  -> rtk cargo test
```

Pros:

- Simple and predictable.
- Avoids nested wrapper weirdness.
- Good default for safety wrappers.

Cons:

- Cannot combine tracing + policy + sandbox wrappers.

Recommended default.

#### 2. Chain / nested wrappers

All matching wrappers apply in priority order.

```text
cargo test
  -> trace rtk cargo test
```

or reverse depending on priority/inside-out semantics.

Key decision: priority order must define whether earlier wrappers are outer or inner.

Pros:

- Composable.
- Useful for telemetry + runtime policy.

Cons:

- Harder to reason about argv.
- Idempotence gets tricky.
- Wrapper A may not expect wrapper B's argv shape.

If supported, require explicit `composition = "chain"` per command/profile.

#### 3. Replace

A wrapper can replace the command rather than prefix it.

```text
python script.py
  -> uv run python script.py
```

Pros:

- Useful for runtime managers.

Cons:

- Different semantics from wrapper prefix.
- Can obscure the original executable.

Should be modeled separately from prefix wrappers.

#### 4. Short-circuit / deny

A wrapper may block execution before spawn, similar to guardrails.

```text
git commit --no-verify
  -> blocked by policy
```

This overlaps with `[block.commands]`. To avoid policy sprawl, use existing block/guardrail for blocking unless a future wrapper has a strong reason to short-circuit.

#### 5. Probe / no-op

A wrapper can inspect and decide not to wrap, returning a reason for logs/status.

Useful for diagnostics (`:wrap explain cargo test`).

## Wrapper matching dimensions

Whitelist matching should probably grow beyond command basename eventually:

- command basename: `cargo`, `git`;
- argv prefix: `git commit`, `cargo test`;
- cwd/project markers: only in repos with `Cargo.toml`;
- mode: job vs cron;
- pty: pty vs pipe;
- pipeline position: first/middle/last;
- interactive classification;
- environment/profile.

For now, basename allowlist is enough.

## Wrapper ordering with pipeline/logical semantics

Recommended invariant:

> Wrapper decisions are per spawned process segment, after word expansion and before `tokio::process::Command::new`, without changing cue-shell's pipeline topology.

That preserves native pipe observability:

```text
cat file |> rg pattern
```

can become:

```text
rtk cat file |> rtk rg pattern
```

if both commands are allowlisted, but it must not become:

```text
sh -c 'cat file | rg pattern'
```

## Open questions

1. Resolved: existing `[wrapper] enabled = true` without allowlist should wrap no commands. Explicit allowlist is required.
2. Resolved: `:wrap on/off` should override only enablement, not allowlist matching.
3. Resolved: `:run(wrapper=true)` may force wrapper enablement even if config `enabled=false`, but still only for allowlisted commands and with idempotence checks.
4. Should cron store resolved wrapper boolean or re-evaluate daemon config every trigger? Recommendation: store resolved boolean for predictability.
5. Should wrapper decisions be visible in job metadata/logs? A future `:log J<n>` line such as `argv: rtk cargo test` would help debugging.
6. Resolved: CLI extensions and runtime wrappers should not share a registry. CLI extension registry maps `cue subcommand` to programs; runtime wrappers transform job argv. Keep them separate.

## Implemented slice

1. Added tests for wrapper behavior:
   - `:run(wrapper=false)` overrides config/session.
   - `:run(wrapper=true)` enables wrapper when command is allowlisted.
   - non-allowlisted commands are not wrapped.
   - foreground-like commands are not wrapped.
   - native pipeline wraps each allowlisted segment.
   - logical `&&` / `||` wraps spawned child commands.
2. Use `allowlist.commands` in `WrapperConfig`.
3. Shared foreground/open-hint classification in `cue-core`.
4. Threaded `wrapper_enabled` through native pipeline and logical job paths.
5. Persisted cron wrapper behavior.
6. Updated README with allowlist-only wrapper config example.
7. Kept multi-wrapper as a documented TODO, not part of this implementation slice.

## Bottom line

- A practical generic CLI extension registry uses external subcommands plus optional manifest metadata in `client.toml`; the near-term implementation supports configured programs and opt-in `cue-*` PATH lookup.
- A daemon/TUI plugin system is much harder and should remain future work.
- Runtime wrappers are a separate mechanism from CLI extensions. The immediate safe path is a single wrapper with explicit allowlist-only targeting, consistent per-segment application, effective mode overrides, and working interactive bypass.
- Multiple wrappers are TODO/future work. If implemented later, they should likely default to `first_match`; nested/chained wrappers should be opt-in because composition semantics and idempotence are subtle.
