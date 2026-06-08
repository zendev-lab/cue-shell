# Conceptual model: jobs, scopes, and composition

Cue-shell can be read as **three interacting notions** (often called “categories”
here only as vocabulary) plus **one composition pattern** for creating jobs.
This is not extra abstraction for its own sake — it makes the invariants in
[`core-types.md`](core-types.md) and [`daemon-architecture.md`](daemon-architecture.md)
easier to reason about and keeps the **tool surface** aligned with **atomic**
operations.

---

## 1. Jobs as a state category (J)

### Objects

Stable job identities with an associated lifecycle state. See [`core-types.md`](core-types.md) (§ Job / `JobStatus`).

### Morphisms

**State transitions** — the daemon’s unidirectional job state machine
(`transition: Job × Event → Job` in `crates/cue-daemon/src/actor/scheduler.rs`):

```
                 ┌─────────┐
   :cancel ──→   │Cancelled│
                 └─────────┘
                      ↑
┌───────┐  sched  ┌───────┐  done   ┌──────┐
│Pending│ ────→   │Running│ ────→   │ Done │
└───────┘         └───────┘         └──────┘
                      │             ┌──────┐
                      ├───────────→ │Failed│
                      │             └──────┘
                      │             ┌──────┐
                      └───────────→ │Killed│
                                    └──────┘
```

### Properties

- **At most one transition per pair of states** (“thin” category — really a
  poset of allowed moves).
- **Terminal states**: Done, Failed, Killed, Cancelled — no further transitions
  except identity.
- **Distinct control paths**: `kill` and `:cancel` are different transitions
  with different outcomes; they are not interchangeable.
- **Monotone in time**: observable state only moves forward along the machine.

---

## 2. Scopes as content-addressed snapshots (S)

### Objects

**Content-addressed environment snapshots** (`ScopeHash`): immutable
`EnvSnapshot { env, cwd }`. Identity is purely by content (e.g. blake3) — no
names or versions as sources of truth.

### Morphisms

An **environment delta** (`EnvDelta`) is the step from one snapshot to another:

```rust
struct EnvDelta {
    set: BTreeMap<String, String>,  // new or modified variables
    unset: Vec<String>,             // removed variables
    cwd: Option<PathBuf>,           // changed cwd (None = inherit)
}
```

### Composition

Deltas compose by overlay (right wins on keys, unset accumulates, last cwd wins):

```
δ₂ ∘ δ₁  =  { set: δ₁.set ∪ δ₂.set (δ₂ wins),
              unset: δ₁.unset ∪ δ₂.unset,
              cwd: δ₂.cwd ?? δ₁.cwd }
```

### Properties

- **Monoid action**: deltas form a monoid under that composition; acting on a
  snapshot produces a new snapshot (`fork`-style).
- **Content-addressed equality**: two scopes are equal iff their hashes match
  (same design idea as Spore’s `sig hash`).
- **Immutable store**: scopes are not mutated in place — forking creates a new
  hash; HEAD moves, history does not rewrite.

Implementation: `crates/cue-daemon/src/actor/scope_store.rs`.

---

## 3. Jobs indexed by scope (“functor” F: S → Set(J))

Every job is created in a fixed **spawn scope**. Define:

```
F(σ)  =  { jobs whose scope_hash = σ }
```

On a scope fork, **existing jobs stay in their spawn scope** — the set
`F(σ)` does not “move” when the shell’s HEAD advances.

Why it matters:

- **Isolation**: interactive `cd` / env changes do not retroactively affect
  already running processes.
- **Stable context**: the same hash always describes the same environment
  footprint; two clients that share a hash see the same contextual story
  (modulo timing of new spawns).
- **Concurrency story**: scope operations can evolve without needing a global
  lock on the entire job table.

---

## 4. Job creation as scoped sequencing (monad-shaped M)

Treat **job-creating** evaluation as a computation that threads **scope** and
may emit a **JobId**:

`M X ≈ Scope → (X, optional JobId)` (informal; exact typing lives in the daemon).

- **Pure value**: no job — no new id.
- **Sequencing**: chain operators connect steps that may each create jobs.

### Pipeline and chain (Kleisli-style reading)

- **`A |> B` inside one job**: piping within a single OS-level job (see pipeline
  vs chain in [`core-types.md`](core-types.md)).
- **Job logical operators** (`&&`, `||`): shell-style short-circuiting inside one
  JobId.
- **Chain operators across jobs** (`->`, `~>`, `|||`, `|?|`): serial and
  parallel composition at the **scheduler** layer; see precedence in
  [`README.md`](README.md#operator-model-two-layers).

Useful mnemonics:

```
a -> b    serial on success
a ~> b    serial, ignore failure of a
a ||| b   parallel, both arms
```

Exact semantics and parser details: [`parser.md`](parser.md).

---

## 5. Tool surface: atomic operations

The CLI/TUI/IPC surface should expose **one clear operation per primitive**, not
bundled “workflows”. In the TUI these are usually `:`-commands; over IPC they
are `Eval`/`Subscribe` + structured requests (see [`ipc-protocol.md`](ipc-protocol.md)).

### On jobs (J)

| Concept | Role | Shape (informal) |
|--------|------|-------------------|
| `run` | Create / schedule work | `Command × Scope → JobId` |
| `status` | Observe state | `JobId → status + metadata` |
| `kill` | Force stop | `Running → Killed` |
| `cancel` | Cooperative cancel | `Pending/Running → Cancelled` |
| `wait` | Block until terminal | `JobId → …` (idempotent) |
| `out` / `err` | Read buffered output | `JobId → bytes/text` |
| `send` | Write stdin | `JobId × input → ()` |

Command names and flags: [`commands-and-modes.md`](commands-and-modes.md).

### On scopes (S)

| Concept | Role |
|--------|------|
| `scopes` / queries | Enumerate or inspect known scopes |
| `env` / `:cd` flow | Read or advance HEAD scope |

### On crons (recurring factories)

| Concept | Role |
|--------|------|
| `cron` | Register schedule × command |
| `crons` | List registrations |
| `cron_pause` / `cron_resume` | Toggle firing |

### System

| Concept | Role |
|--------|------|
| `log` | Historical / event introspection |

**Naming**: repeating a `cue_` prefix usually adds noise — the builtin name
already names the category operation.

---

## 6. Future design hooks

These are sketches, not commitments.

### 6.1 Dry-run as typing

`run --dry-run` validates parse + resolver without committing a morphism:
check that a proposed command is **well-formed** before creating persistent
state.

### 6.2 Scope migration for jobs

A hypothetical:

```
migrate: JobId × ScopeHash → JobId
```

would move **eligibility-checked** workloads to a compatible environment —
requires a clear notion of env/cwd compatibility.

### 6.3 Graph-shaped composition

When two subgraphs funnel into one consumer, the dependency shape is DAG-like.
That is the natural setting for richer `|?|` / race semantics — keep the heavy
DAG runtime (`loom`) separate from cue-shell’s **minimal** chain layer.

### 6.4 Richer deltas

The delta monoid could grow merge/diff primitives (meet, relative complement)
for tooling around scope comparison and merges.
