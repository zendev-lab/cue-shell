# Cue Shell — cue-core 类型设计

> **Update:** current `cue-core` exposes Job / Cron / Chain / Scope identifiers and
> no longer defines agent-specific runtime types.

## 一、ID 类型

```rust
/// Job 序号，显示为 J1, J2, ...
pub struct JobId(pub u32);

/// Cron 序号，显示为 C1, C2, ...
pub struct CronId(pub u32);

/// Scope 内容寻址 hash（blake3），显示为 S@a3f1...
pub struct ScopeHash(pub [u8; 32]);

/// 统一引用：:fg / :kill / :out 等命令的参数
pub enum EntityRef {
    Job(JobId),
    Cron(CronId),
    Scope(ScopeHash),
}
```

## 二、Scope

### 核心定义

```rust
/// 不可变的环境快照，内容寻址
pub struct Scope {
    pub hash: ScopeHash,          // blake3(canonical_bytes(env + cwd + ...))
    pub parent: Option<ScopeHash>, // delta 链的父节点
    pub delta: Option<EnvDelta>,   // 仅存增量（parent 存在时）
    pub snapshot: Option<EnvSnapshot>, // 全量快照（根节点或展平后）
}

pub struct EnvSnapshot {
    pub env: BTreeMap<String, String>,
    pub cwd: PathBuf,
    // 未来扩展：umask, aliases, functions, shell_options, traps
}

pub struct EnvDelta {
    pub set: BTreeMap<String, String>,    // 新增/修改的变量
    pub unset: Vec<String>,               // 删除的变量
    pub cwd: Option<PathBuf>,             // 若 cwd 变化
}
```

### 设计规则

- Scope 是**不可变的** — 创建后不修改
- ID 由 blake3(内容) 决定 — 相同环境 = 相同 hash = 自动去重
- delta 存储 + parent_hash，查询时沿链还原
- "默认 scope" 是一个可移动的 HEAD 指针（类似 git HEAD）
- `:env set` / `:cd` 创建新 scope hash，更新 HEAD 指针，持久化
- 未来：多个保存的命名默认 scope 可切换

### 类比

```
Scope ≈ git commit（不可变，内容寻址）
Job ≈ git diff（从 start_scope 到 end_scope 的变换）
fork ≈ git branch（从某个 scope 开始新分支）
默认 scope ≈ HEAD 指针
```

## 三、Job

### 核心定义

```rust
pub struct Job {
    pub id: JobId,
    pub status: JobStatus,
    pub pipeline: Pipeline,     // 命令（可能是多进程管道）
    pub start_scope: ScopeHash,
    pub end_scope: Option<ScopeHash>,  // None = 未完成
    pub stdout: RingBuffer,
    pub stderr: RingBuffer,
    pub exit_code: Option<i32>,        // None while non-terminal; EXIT_CODE_UNAVAILABLE (-1) means no process status
    pub chain_id: Option<ChainId>,     // 所属 chain（若有）
    pub chain_index: Option<usize>,    // 在 chain 中的位置
    pub created_at: Instant,
    pub started_at: Option<Instant>,
    pub finished_at: Option<Instant>,
}

pub enum JobStatus {
    Pending,
    Running,
    Done,       // exit_code == 0
    Failed,     // exit_code != 0
    Killed,     // 被 :kill 终止
    Cancelled(CancelReason),
}

pub enum CancelReason {
    User,          // :cancel 手动取消
    ChainAborted,  // chain 中前置步骤失败
    Timeout,       // 超时
}
```

### 状态机

```
                    ┌─────────┐
      :cancel ──→   │Cancelled│
                    │(reason) │
                    └─────────┘
                         ↑
┌───────┐  调度  ┌───────┐  完成  ┌──────┐
│Pending│ ────→  │Running│ ────→  │ Done │  (exit 0)
└───────┘        └───────┘        └──────┘
                     │            ┌──────┐
                     ├──────────→ │Failed│  (exit != 0)
                     │            └──────┘
                     │            ┌──────┐
                     └──────────→ │Killed│  (:kill)
                                  └──────┘
```

### 规则

- `start_scope == end_scope` → Job 无环境副作用
- `end_scope` 在 Job 完成前为 None — 无法 fork
- Job 不可变（dispatch 后状态机单向推进，不可回退）
- stdout/stderr 使用环形缓冲区，有界内存

## 四、Cron

### 核心定义

```rust
pub struct CronTask {
    pub id: CronId,
    pub schedule: CronSchedule,
    pub command: String,
    pub scope: ScopeHash,       // 绑定固定环境
    pub label: Option<String>,  // 可选标签
    pub enabled: bool,
    pub last_run: Option<Instant>,
    pub next_run: Option<Instant>,
    pub history: Vec<JobId>,    // 历次触发产生的 Job
}

pub enum CronSchedule {
    Interval(Duration),                        // every 5m
    TimeOfDay { time: NaiveTime, days: Option<DayFilter> },  // at 9am [on weekdays]
    Delay(Duration),                           // in 30s （一次性）
    Preset(CronPreset),                        // daily, hourly, weekly, monthly
    Crontab(CrontabSchedule),                  // validated cron */5 * * * *
}

pub enum CronPreset { Hourly, Daily, Weekly, Monthly }

pub struct DayFilter {
    pub days: Vec<Weekday>,  // mon, tue, ..., weekdays, weekends
}
```

### 规则

- Cron 绑定**固定** scope hash，每次触发用同一环境
- cued 重启后**自动恢复**所有 Cron 任务（持久化到 SQLite）
- `in` 类型是一次性延迟任务，触发后自动移除
- 触发时创建新 Job，Job.start_scope = Cron.scope

## 六、Pipeline 与 Chain

### 两层模型

```
Pipeline（|> |&> |!>）= 单个 Job 内部的进程管道
Job logical（&& ||）   = 单个 Job 内部的短路逻辑
Chain（-> ~> ||| |?|） = Job 之间的编排

合法：a |> b -> c |> d    = Job1(a|>b) -> Job2(c|>d)
非法：(a -> b) |> c       = chain 不能作为管道输入
```

### Pipeline（Job 内部）

```rust
/// 一个 Job 的命令可以是多进程管道
pub struct Pipeline {
    pub steps: Vec<PipelineStep>,  // 至少 1 个
}

pub struct PipelineStep {
    pub command: String,
    pub pipe_to_next: Option<PipeOp>,
}

pub enum PipeOp {
    Stdout,       // |>   stdout → next stdin
    StdoutStderr, // |&>  stdout+stderr → next stdin
    StderrOnly,   // |!>  仅 stderr → next stdin
}
```

### Chain（Job 间编排）

```rust
pub struct ChainId(pub u32);

pub struct Chain {
    pub id: ChainId,
    pub root: ChainNode,
    pub jobs: Vec<JobId>,       // 展平后的 Job 列表
    pub status: ChainStatus,
}

/// 树形 AST — 叶节点是 Pipeline（即单个 Job）
pub enum ChainNode {
    Leaf(Pipeline),
    Serial {
        left: Box<ChainNode>,
        op: SerialOp,
        right: Box<ChainNode>,
    },
    Parallel {
        left: Box<ChainNode>,
        op: ParallelOp,
        right: Box<ChainNode>,
    },
}

pub enum SerialOp {
    Then,       // ->  前者成功才继续
    Always,     // ~>  忽略前者结果
}

pub enum ParallelOp {
    All,        // |||  全部同时发射
    Race,       // |?|  任一成功即可
}

pub enum ChainStatus {
    Running,
    Done,
    Failed,
    Aborted,  // 某步骤失败导致 chain 中止
}
```

### 完整操作符表

| 层级 | 操作符 | 语义 | 优先级 |
|------|--------|------|--------|
| Pipeline（Job 内） | `\|>` | stdout → stdin | 1（最高） |
| Pipeline（Job 内） | `\|&>` | stdout+stderr → stdin | 1 |
| Pipeline（Job 内） | `\|!>` | 仅 stderr → stdin | 1 |
| Job logical（Job 内） | `&&` / `\|\|` | 短路逻辑，同一 JobId | 2 |
| Chain（Job 间） | `\|\|\|` | 并行-全部同时启动 | 3 |
| Chain（Job 间） | `\|?\|` | 并行-任一成功 | 3 |
| Chain（Job 间） | `->` | 串行-前者成功才继续 | 4（最低） |
| Chain（Job 间） | `~>` | 串行-忽略前者结果 | 4 |
| 分组 | `()` | 括号显式分组 | — |

```
解析示例：
  a |> b -> c ||| d ~> e
= (a |> b) -> (c ||| d) ~> e
= Job1(a|>b) -> (Job2(c) ||| Job3(d)) ~> Job4(e)

  cargo build |> grep error -> cargo test ||| cargo clippy
= Job1(cargo build |> grep error) -> (Job2(cargo test) ||| Job3(cargo clippy))
```

### 退出码聚合

```
[0; 0; 0, 1]
 ^   ^   ^^^
 │   │   └── 并行步骤（逗号分隔）
 │   └────── 串行步骤（分号分隔）
 └────────── 串行步骤

Pipeline 内的退出码 = 最后一个进程的退出码（同 bash pipefail 可选）
```

### 重试与 ChainAborted

- `:retry J3` → 手动重试单个 terminal Job，从原 `start_scope` 重新发射 fresh job
- 自动重试尚未实现，因此不提供 `retry` / `retry_delay` mode params，避免调用时看似生效但实际被忽略

## 七、Mode

```rust
pub enum Mode {
    Job,    // ⚡ bare input → :run
    Cron,   // ⏰ bare input → :cron
}
```

Shift+Tab 循环：Job → Cron → Job

## 八、Command 系统

### 参数分类

```rust
/// 模式参数：影响执行行为，可在 daemon.toml 设默认值
/// 语法：:cmd(key=val, key=val) args
pub struct ModeParams {
    pub params: BTreeMap<String, ParamValue>,
}

/// 命令参数：位置/标志参数，纯语法
pub struct CommandArgs {
    pub positional: Vec<String>,
    pub flags: BTreeMap<String, Option<String>>,
}
```

### 有模式参数的命令

| 命令 | 可用模式参数 |
|------|-------------|
| `:run()` | `cwd`, `wrapper`, `scope`, `pty` |
| `:cron()` | `cwd`, `wrapper`, `scope` |

其他命令只有命令参数，无 `()` 语法。

### 解析流程

```
输入 → trim
  ├─ 首字符 ':' → 解析内建命令
  │   ├─ :cmd(k=v) args  → Builtin { cmd, mode_params, args }
  │   └─ :cmd args       → Builtin { cmd, mode_params: default, args }
  └─ 非 ':' → 模式默认包装
      ├─ JOB   → Builtin { cmd: "run", args: input }
      └─ CRON  → Builtin { cmd: "cron", args: input }
```
