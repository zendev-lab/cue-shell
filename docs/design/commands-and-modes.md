# Cue Shell — 基础命令与模式最终设计方案 v2

> 已完成三轮评审、竞品调研和命名调研后的最终设计。
> **Update:** current cue-shell ships only **JOB** and **CRON** modes.

---

## 一、核心语法：`:` 前缀

**所有内建命令以 `:` 开头，后接命令名和参数。**

```
:run cargo build          # 发射 job
:kill J1                  # 终止 job
:jobs                     # 列出所有 job
:scope list --tree        # 列出 scope
:env                      # 查看 env（无参数也用 :）
:env set FOO=bar          # 设置 env
:help                     # 帮助
?                         # 当前模式帮助
:help run                 # run 命令的帮助
```

**设计理由**（`:` 前缀 vs `/` 前缀 vs `cmd:` 分隔符）：
- `:` 在所有模式下零冲突：shell 命令、自然语言、cron 表达式都不以 `:` 开头
- Vim/Helix/lazygit 用户有肌肉记忆（TUI 文化一致）
- `/` 在 JOB 模式下与绝对路径 `/usr/bin/...` 冲突
- 脚本友好：`:` 不是常见 shell 命令前缀，便于和 bare input 区分
- 解析规则极简：**首字符 `:` → 内建命令，否则 → 模式默认包装**

**冒号后空格可选**：`:run cargo build` 和 `:run  cargo build` 都合法（trim）。

> 完整的前缀选择调研与评分见 [research/syntax-decisions.md](../research/syntax-decisions.md)。

---

## 二、模式设计

### 两个主模式

| 模式 | 默认包装 | 含义 | 定位 |
|------|---------|------|------|
| **JOB** ⚡ | → `:run <input>` | 输入即执行 | 核心 |
| **CRON** ⏰ | → `:cron <input>` | 输入即调度 | 核心 |
- Shift+Tab 循环切换：JOB → CRON → JOB
- `:` 前缀在任何模式下都能直接执行内建命令

### 解析规则

```rust
fn dispatch(input: &str, mode: Mode) -> Result<Action> {
    let input = input.trim();
    if input.is_empty() { return Ok(Action::Noop); }

    // Rule 1: `:` 前缀 → 内建命令（所有模式下一致）
    if input.starts_with(':') {
        let rest = input[1..].trim_start();
        let (cmd, args) = split_first_word(rest);
        if BUILTINS.contains(cmd) {
            return Ok(Action::Builtin { cmd, args });
        } else {
            return Err(format!("unknown builtin: :{cmd}"));
        }
    }

    // Rule 2: 模式默认包装
    match mode {
        Mode::JOB   => Ok(Action::Builtin { cmd: "run", args: input }),
        Mode::CRON  => Ok(Action::Builtin { cmd: "cron", args: input }),
    }
}
```

### 模式转换示例

| 输入 | JOB ⚡ | CRON ⏰ |
|------|--------|---------|
| `cargo build` | `:run cargo build` | `:cron cargo build` |
| `run the tests` | `:run run the tests` | `:cron run the tests` |
| `:kill J1` | 内建 kill ✅ | 内建 kill ✅ |
| `:jobs` | 内建 jobs ✅ | 内建 jobs ✅ |
| `/usr/bin/python a.py` | `:run /usr/bin/python a.py` ✅ | `:cron /usr/bin/python a.py` |
| `every 5m cargo test` | `:run every 5m cargo test` | `:cron every 5m cargo test` ✅ |

**零歧义**：`:` 开头 = 内建，否则 = 模式默认。没有命名冲突，没有 fallthrough，没有上下文依赖。

---

## 三、基础命令完整列表

### 3.1 Job 管理

| 命令 | 语法 | 语义 | 定位 |
|------|------|------|------|
| `:run` | `:run <cmd> [chain...]` | 发射 job | 核心 |
| `:jobs` | `:jobs [--json]` | 列出所有 job 摘要 | 核心 |
| `:wait` | `:wait J1` | 等待 job 进入终态 | 核心 |
| `:out` | `:out J1` | 查看 job stdout snapshot | 核心 |
| `:tail` | `:tail J1 [bytes]` | 打开并持续 follow job stdout | 核心 |
| `:err` | `:err J1` | 查看 job stderr snapshot | 核心 |
| `:send` | `:send J1 <input>` | 向 running job 写 stdin | 核心 |
| `:kill` | `:kill J1` | 终止 running job | 核心 |
| `:cancel` | `:cancel J3` | 取消 queued job | 核心 |
| `:fg` | `:fg J2` | Job 进入前台 pty | 核心 |

`:run` / JOB bare input 在发射前会基于当前 scope snapshot 做**显式 word expansion**：支持前导 `~`、`$VAR`、`${VAR}`；仍保持 direct exec，不隐式走 shell，也不做 glob / command substitution / field splitting。

`cue-shell` 现在也支持 **multiline script**：

```text
cat _typos.toml |> rg files
||| cat Cargo.toml |> rg author
```

- JOB 模式下，多行输入默认视为一次 script 提交；显式 `:run` 也可承载同样的多行 body
- 一个 script 会分配新的 `R<n>`，其下每个顶层 item 继续按原有语义解析成 chain / job / cron
- 调度是 **异步提交、按 ack 排序**：item N 创建成功后，才继续发 item N+1；但已经创建的 chain/job 仍按各自异步生命周期运行
- 输出归属不变：脚本卡片只负责展示 `R -> C -> J` 映射，真正 stdout/stderr 仍由 `J<n>` 持有
- 顶层换行只在 **top-level** 作为 script item 分隔；如果当前 chain 明显未结束，则换行继续作为同一条 chain 的格式化空白

另外，`cue-shell` 现在把两类输入当作**原生 scope-transform job** 处理，而不是交给外部 shell：

- `:run cd <path>`
- `:run env set KEY=VALUE ...`

其语义是：

- 立即生成该 job 的 `end_scope`
- **不会**自动移动默认 HEAD
- serial chain 中，后一 leaf 会继承前一 leaf 的 `end_scope`
- parallel / pipeline 中若出现这类 scope-transform leaf，当前直接拒绝，避免作用域歧义

### 3.2 Scope 管理

| 命令 | 语法 | 语义 | 定位 |
|------|------|------|------|
| `:scope list` | `:scope list [--tree]` | 列出所有 scope | 核心 |
| `:scope new` | `:scope new [--profile rust]` | 创建新 scope | 核心 |
| `:scope env` | `:scope env S1` | 查看 scope env | 核心 |
| `:scope fork` | `:scope fork S1 [--name exp]` | 从 scope 派生（delta 存储） | 核心 |
| `:scope close` | `:scope close S1` | 归档 scope | 核心 |

### 3.3 Cron/定时管理

| 命令 | 语法 | 语义 | 定位 |
|------|------|------|------|
| `:cron` | `:cron <schedule> <cmd>` | 添加定时/延迟任务 | 核心 |
| `:crons` | `:crons` | 列出所有定时任务 | 核心 |

- `:crons` 现在展示**持久化 cron 历史**，而不只是在内存中的活跃注册项
- one-shot cron 触发后会保留为 `completed`；daemon 重启时已错过的 one-shot 会保留为 `expired`，都不会再补跑

**`:cron` 内部语法（B+C 混合）**：

全局只有 `:cron` 一个命令，`every`/`at`/`in` 等是其内部关键字，不是独立内建命令。

```
# ── 关键字路径（日常 90%，零冗余） ──
:cron every 5m cargo build               # 间隔：每 5 分钟
:cron every 2h make test                  # 间隔：每 2 小时
:cron at 14:30 ./deploy.sh               # 定时：每天 14:30
:cron at midnight ./backup.sh            # 定时：每天午夜
:cron at 9am on weekdays cargo test      # 组合：工作日 9 点
:cron on mon,wed,fri at 15:00 ./report   # 组合：周一三五下午 3 点
:cron in 5m cargo build                  # 一次性：5 分钟后执行
:cron in 30s cargo test                  # 一次性：30 秒后执行
:cron daily cargo clippy                 # 预设别名
:cron hourly ./health-check.sh           # 预设别名
:cron weekly ./cleanup.sh                # 预设别名
:cron monthly ./report.sh               # 预设别名
:cron cron */5 * * * * curl api/health   # 原生 crontab（cron 后固定 5 字段）

# ── do 分界路径（原生 crontab） ──
:cron */5 * * * * do curl api/health     # 原生 crontab + do 分界
```

**关键字解析规则**：

| 首 token | 消耗 token 数 | 语法 |
|---------|-------------|------|
| `every` | 1（duration） | `every <dur> <cmd...>` |
| `at` | 1-3（time [on dayspec]） | `at <time> [on <days>] <cmd...>` |
| `on` | 3（dayspec at time） | `on <days> at <time> <cmd...>` |
| `in` | 1（duration） | `in <dur> <cmd...>` |
| `cron` | 5（cron fields） | `cron <f1> <f2> <f3> <f4> <f5> <cmd...>` |
| `daily`/`hourly`/... | 0 | `<preset> <cmd...>` |
| 其他 | 扫描 `do` | `<5-field-crontab> do <cmd...>` |

> 完整的 cron 语法设计过程与备选方案对比见 [research/syntax-decisions.md](../research/syntax-decisions.md)。

> 当前 runtime 已支持：`every <dur>`、`in <dur>`、`at <time> [on <days>]`、`on <days> at <time>`、`daily/hourly/weekly/monthly`、`cron <5f>`，以及 `<5-field-crontab> do <cmd>`。
> 更自由的 `do` 回退（如 `every 30m 9am-5pm weekdays do ...` / 动态变量 schedule）目前仍显式保留为后续能力，不再假装已实现。

### 3.4 通用命令

| 命令 | 语法 | 语义 | 定位 |
|------|------|------|------|
| `:env` | `:env` | 查看当前持久化 HEAD env | 核心 |
| `:env set` | `:env set FOO=bar` | 设置 env 并打印实际副作用 | 核心 |
| `:help` | `:help` / `:help run` | 帮助 | 核心 |
| `?` | `?` | 当前 mode 的详细帮助 | 核心 |
| `:config` | `:config` / `:config show` | 查看配置 | 核心 |
| `:wrap` | `:wrap [on/off/status]` | 查看或临时覆盖 runtime wrapper | 核心 |
| `:exit` | `:exit` | 退出 TUI | 核心 |

这里需要区分：

- 顶层 `:cd ...` / `:env set ...`：修改默认 HEAD scope，并持久化
  - 响应返回新的 scope hash，并打印**实际生效的副作用**（如 `cwd old -> new`、`KEY: old -> new`）
  - `:env set` 对重复变量按最终 key 去重，只展示最终实际变化
- `:run cd ...` / `:run env set ...`：只修改该 job 的 `end_scope`

前端补充：

- `?` / `:help` 仍由 daemon 内建处理
- copy、target 切换、页面点按等属于前端本地 UI 语义，不属于 `cued` 内建命令面

---

## 四、Scope 持久化策略

### Delta 存储
- fork 出的 scope 只存储 `parent_id` + `env_delta`
- 读取时沿 parent 链合并得到完整 env
- 大幅减少磁盘开销

### LRU 淘汰 + 引用保护
- 配置 `max_persisted_scopes`
- 超限时淘汰 `last_used` 最旧的 scope
- **引用保护**：被 active/idle 子 scope 依赖的 parent 不可淘汰
- 淘汰前先把依赖它的子 scope 的 delta 展平为全量快照

### Job / Script 历史裁剪

`server.toml` 增加：

```toml
[retention]
max_job_history = 200
max_script_runs = 100
```

- `max_job_history`：保留最近的 job 记录与其输出日志
- `max_script_runs`：保留最近的 script 元数据（`R<n>` 与 item 映射）
- 裁剪时优先删除最旧记录，script 只是提交层摘要，不改变 job 作为输出权威来源的设计

### 生命周期
```
Active → Idle (队列空) → Persisted (TTL 到期，落盘)
                                    ↓
                              Archived (LRU 淘汰 or :scope close)
                                    ↓
                              Deleted (超出保留限制)
```

---

## 五、模式参数 `()` 语法

> v2 新增：用括号分离内建配置与被执行命令的参数，消除歧义。

```
:run(cwd=/repo, retry=3, retry_delay=5s) cargo test --release
:cron(cwd=/repo) every 5m cargo clippy
```

- `()` 紧跟命令名 = 模式参数（执行行为配置）
- `()` 出现在其他位置 = chain 分组括号
- Tokenizer 根据**位置规则**消歧（前一个 token 是 Command → 模式参数）
- 模式参数可在 `server.toml`（迁移期仍兼容旧 `config.toml`）中设置默认值，调用时覆盖

### 支持模式参数的命令

| 命令 | 可用参数 |
|------|---------|
| `:run()` | `cwd`, `retry`, `retry_delay` |
| `:cron()` | `cwd` |
| `:scope new()` | `profile` |

其他命令只有位置/标志参数，无 `()` 语法。

---

## 六、操作符（两层模型）

### Pipeline（Job 内部的进程管道）

Pipeline 连接 **进程**，运行在同一个 Job 内部，类似 bash 的 `|`：

| 操作符 | 语义 | 说明 |
|--------|------|------|
| `\|>` | stdout 管道 | 前者 stdout → 后者 stdin |
| `\|&>` | stdout+stderr 管道 | 前者 stdout+stderr → 后者 stdin |
| `\|!>` | stderr 管道 | 前者仅 stderr → 后者 stdin |

### Chain（Job 间的编排）

Chain 连接 **Job**，由 Scheduler 调度执行：

| 操作符 | 语义 | 说明 |
|--------|------|------|
| `->` | 串行-成功继续 | 前者 exit 0 才执行后者 |
| `~>` | 串行-忽略结果 | 无论前者结果都执行后者 |
| `\|\|` | 并行-全部 | 同时发射所有分支 |
| `\|\|?` | 并行-竞速 | 任一成功即视为成功 |

### 优先级

```
pipe (1, 最高) > parallel (2) > serial (3, 最低)
```

### 解析示例

```
a |> b -> c ||| d ~> e
= (a |> b) -> (c ||| d) ~> e
= Job1(a|>b) -> (Job2(c) ||| Job3(d)) ~> Job4(e)

cargo build |> grep error -> cargo test ||| cargo clippy
= Job1(cargo build |> grep error) -> (Job2(cargo test) ||| Job3(cargo clippy))
```

### 关键语义

- Pipeline (`|>`) = **Job 内部**，共享同一 scope
- Job logical (`&&` `||`) = **单个 Job 内部**，同一个 JobId
- Chain (`->` `|||` `|?|`) = **Job 之间**，Scheduler 调度
- `(a -> b) |> c` **非法** — chain 输出不能作为管道输入
- `()` 在 chain 层面用于分组：`(a ||| b) -> c`

### Exit code 聚合

```
[0; 0; 0, 1]
 ^   ^   ^^^
 │   │   └── 并行步骤（逗号分隔）
 │   └────── 串行步骤（分号分隔）
 └────────── 串行步骤

Pipeline 内退出码 = 最后一个进程的退出码
```

### 重试

- `:run(retry=3) cargo test` → 失败时自动重试，最多 3 次
- 重试成功 → ChainAborted 的后续步骤自动重启
- `:retry J3` → 当前会用原 `start_scope` 和 pipeline 重新发射一个 fresh job
- downstream chain 续接 / 自动重启后继 leaf 仍未完成，语义已显式收窄，避免保留模糊 stub

---

## 八、命令速查表

```
┌── Job ────────────────────────────────────┐
│ :run <cmd>     发射 job                    │
│ :jobs          列出 job                    │
│ :wait <id>     等待完成                    │
│ :out <id>      查看 stdout snapshot        │
│ :tail <id>     follow stdout               │
│ :err <id>      查看 stderr snapshot        │
│ :send <id>     写入 stdin                  │
│ :kill <id>     终止 job                    │
│ :cancel <id>   取消排队 job                │
│ :fg <id>       前台（pty）                 │
│ :retry <id>    重试 failed job             │
│ :log [id]      查看 job 历史日志           │
├── Scope ──────────────────────────────────┤
│ :scope list    列出 scope                  │
│ :scope new     创建 scope                  │
│ :scope env     查看 scope env              │
│ :scope fork    派生 scope（delta）          │
│ :scope close   归档 scope                  │
│ :scopes        列出所有 scope（简写）       │
│ :cd <path>     修改默认 scope 的 cwd       │
├── Control ────────────────────────────────┤
│ :send J<n> <input>  向 job 写 stdin        │
│ :cancel J<n>        取消 queued job        │
├── Cron ───────────────────────────────────┤
│ :cron <sched> <cmd>  添加定时/延迟任务     │
│ :crons         列出定时任务                │
│   内部关键字:                              │
│   every 5m / at 9am / on weekdays         │
│   in 5m / daily / cron */5 * * * *        │
│   */5 * * * * do <cmd>                    │
├── General ────────────────────────────────┤
│ :env           查看 env                    │
│ :env set K=V   设置 env                    │
│ :help [cmd]    帮助                        │
│ ?              当前 mode 详细帮助          │
│ :config [sub]  查看/修改配置               │
│ :wrap [sub]    查看/覆盖 runtime wrapper   │
│ :clear         清空 REPL 区域              │
│ :quit          退出 TUI                    │
└───────────────────────────────────────────┘

总计：28+ 内建命令（含 scope/cron 子命令）
```
