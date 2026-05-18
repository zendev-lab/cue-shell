# Cue Shell 竞品调研报告

> **调研日期**: 2025-07
> **调研范围**: Agent 友好型 Shell、进程管理器、任务运行器、现代 Shell、编码 Agent 工具接口、调度器
> **核心定位**: Cue Shell 是一个异步进程运行时 + TUI + MCP 接口，为人机协作设计，**不是**传统 Shell，**不是**终端复用器，而是带会话/环境管理的异步任务调度器。

---

## 目录

1. [Agent 友好型 Shell / 命令运行器](#1-agent-友好型-shell--命令运行器)
2. [进程管理器与会话概念](#2-进程管理器与会话概念)
3. [任务运行器与异步 / DAG 执行](#3-任务运行器与异步--dag-执行)
4. [现代 Shell](#4-现代-shell)
5. [编码 Agent 工具接口](#5-编码-agent-工具接口)
6. [Cron / 调度器替代方案](#6-cron--调度器替代方案)
7. [命名约定汇总](#7-命名约定汇总)
8. [关键差异化定位](#8-关键差异化定位)

---

## 1. Agent 友好型 Shell / 命令运行器

### 1.1 block/agent-task-queue ⭐ 最相关

| 属性 | 详情 |
|------|------|
| **GitHub** | `block/agent-task-queue` (38★) |
| **语言** | Python |
| **许可证** | Apache 2.0 |

**核心架构**：一个 MCP server，仅暴露单一工具 `run_task`，后端为 SQLite FIFO 队列。多个 Agent 提交耗时命令，server 串行执行——每个队列同时只运行一个任务。

**关键创新**：利用 MCP 的持久连接解决 Shell 超时问题。Agent 在 MCP 调用上无限阻塞等待任务完成，绕过了 CLI wrapper 的 30-120 秒 shell 超时限制。

**对 Cue Shell 的启示**：
- MCP 长连接绕过超时是核心洞见，Cue Shell 应直接内化
- FIFO + 命名队列的概念可直接复用
- **缺失**：无交互式 Shell 支持、无流式输出、无优先级系统、无安全模型

### 1.2 rusiaaman/wcgw (655★)

运行在 `screen` 复用终端中的 MCP server，Agent 和人类可同时 attach 到同一终端。支持交互式输入（方向键、Ctrl-C）和 3 种安全模式。

**对 Cue Shell 的启示**：`screen` 复用让人机共享终端是好思路；交互式输入支持是 Cue Shell 可以考虑的。**缺失**：无队列/协调层、无安全沙箱。

### 1.3 NVIDIA/OpenShell (4,754★)

为自主 AI Agent 提供沙箱执行环境。每个沙箱是 Docker 容器（K3s 管理），策略引擎通过声明式 YAML 实施 4 层安全约束（文件系统/网络/进程/推理）。

**对 Cue Shell 的启示**：4 层安全策略和热可重载策略是好设计模式；TUI 仪表板（k9s 风格）值得参考。**缺失**：重基础设施（K3s + Docker）、无任务队列、无 MCP 接口。

### 1.4 其他项目速览

| 项目 | 星数 | 核心差异 |
|------|------|---------|
| **agent-infra/sandbox** | 4,200★ | 全功能 Docker 容器（浏览器+Shell+IDE+Jupyter+MCP），太重 |
| **sonirico/mcp-shell** | 69★ | Go 编写的轻量 MCP server，安全优先（白名单/黑名单/审计日志），**无异步** |
| **Flux159/agentic-shell (AGIsh)** | 11★ | NL→Shell 翻译 REPL，太早期 |
| **alfranz/hush** | 23★ | CLI wrapper，成功时压缩输出（节省 96-99% token） |
| **xenodium/agent-shell** | 1,019★ | Emacs buffer，通过 ACP 交互 |
| **shellwright** | 31★ | "Playwright for Shell"——终端录制 + 自动化 MCP server |

### 1.5 对比矩阵

| 项目 | MCP? | 异步执行? | 任务队列? | 多Agent协调? | 交互Shell? | 安全模型? |
|------|------|----------|----------|-------------|-----------|----------|
| block/agent-task-queue | ✅ | ✅ | ✅ FIFO | ✅ | ❌ | ❌ |
| NVIDIA/OpenShell | ❌ | ✅(沙箱) | ❌ | ❌ | ✅ | ✅(4层) |
| wcgw | ✅ | 部分 | ❌ | ❌ | ✅ | 部分 |
| sonirico/mcp-shell | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ |

### 1.6 关键结论

**市场空白**：没有任何一个项目同时具备 **MCP Shell 执行 + 异步队列语义 + 交互式终端 + 安全策略**。各个能力分散在不同项目中。Cue Shell 的定位正好填补这一整合空白。

---

## 2. 进程管理器与会话概念

### 2.1 核心发现：**无一支持环境累积**

调研了 7 个主流进程管理器，**没有任何一个原生支持环境累积**（进程 A 的输出 → 进程 B 的环境变量）。这是 UNIX 进程管理器的共性设计。

| 工具 | 环境模型 | 最接近累积的特性 |
|------|---------|----------------|
| **supervisord** | 每进程 `environment=` | 引用父 Shell 变量 |
| **pm2** | 每应用 `env: {}` + `env_production` | `increment_var` 自增 |
| **foreman** | 全局 `.env` 文件 | 扁平共享，无进程间传递 |
| **overmind** ⭐ | `.env` + `OVERMIND_PROCESS_<name>_PORT` | **跨进程端口引用**——最接近累积 |
| **immortal** | 每服务 YAML `env:` | `require:` 确保启动顺序 |
| **circus** | `[env:watcher]` + `copy_env` | 从父进程复制 |
| **goreman** | `.env` 文件 | 同 foreman |

### 2.2 各工具要点

- **supervisord**：最成熟的 XML-RPC API + 事件系统，可扩展但无会话/环境累积
- **pm2**：Cluster 零停机重载、日志管理、进程持久化，但无应用层会话管理
- **overmind** ⭐：用 tmux 为每个进程创建独立窗口，`connect` 实时调试，跨进程端口引用是最接近环境累积的设计
- **immortal**：`require:` 服务依赖 + `require_cmd:` 条件启动
- **circus**：ZeroMQ 通信，Web 仪表板，Socket 预绑定共享

### 2.3 控制接口对比

| 工具 | CLI | HTTP API | Socket | 编程API | Web UI |
|------|-----|----------|--------|---------|--------|
| supervisord | ✅ | XML-RPC | ✅ Unix | Python | ❌ |
| pm2 | ✅ | ❌(云) | IPC | Node.js | pm2.io |
| overmind | ✅ | ❌ | ✅ Unix/TCP | ❌ | tmux |
| circus | ✅ | ✅ Web | ❌ | Python | ✅ |

### 2.4 对 Cue Shell 的启示

1. **环境累积是未被服务的空白**
2. **overmind 的 tmux 会话**最接近 Cue Shell 的 Scope 概念
3. **Cue Shell 的优势**：环境快照 + 不可变链式累积 > 任何现有管理器的预声明环境模型

---

## 3. 任务运行器与异步 / DAG 执行

### 3.1 对比总览

| 特性 | just | task | nx | turborepo | dagger | earthly | mise |
|------|------|------|----|---------|---------|---------|----|
| **并行执行** | `[parallel]` | deps 默认并行 | 完整 DAG | 完整 DAG | 自动 DAG(懒) | 自动 DAG | deps 并行 |
| **DAG 求解** | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ |
| **内容缓存** | ❌ | ❌ | ✅ 本地+远程 | ✅ 本地+远程 | ✅ 内容寻址 | ✅ 层缓存 | ❌ |
| **环境累积** | export/per-recipe | per-task env | per-target | 严格 env | **✅ 不可变链** | ARG/ENV | per-project |
| **宿主原生** | ✅ | ✅ | ✅ | ✅ | ❌(容器) | ❌(容器) | ✅ |
| **配置格式** | justfile | YAML | JSON+插件 | JSON | 代码(Go/Py/TS) | Earthfile | TOML |

### 3.2 Dagger 的容器会话模型 ⭐ 深度解析

Dagger 的核心抽象是**不可变、内容寻址的容器状态链**，与 Cue Shell 环境累积最相关：

**工作原理**：
1. **不可变状态链**：每个 `.With*()` 返回新对象，前一状态永不变更
2. **环境累积**：`.WithEnvVariable()` 追加到容器 env 映射，后续 `.WithExec()` 看到累积环境
3. **懒求值**：链是规格说明，调用终端方法才执行
4. **内容寻址缓存**：链中每个节点被哈希，匹配则跳过
5. **可分叉**：从任意中间状态分支，并行运行独立路径

**对 Cue Shell 的启示**：不可变快照模式、懒求值、内容寻址缓存的思路可直接映射到 Scope env 快照。**缺失**：依赖 Docker/BuildKit，非轻量级。

### 3.3 其他运行器要点

- **just**：v1.42+ 支持 `[parallel]`，每行独立 `sh -cu`，无 DAG/缓存
- **task**：`deps:` 默认并行，`cmds:` 顺序执行，`ignore_error: true`
- **nx**：完整项目依赖图 + 内容寻址缓存 + `affected` 命令
- **turborepo**：Rust 核心，`dependsOn` + `^` 拓扑排序，环境变量管理出色
- **mise**：统一版本管理 + 环境管理 + 任务运行器（替代 direnv）

---

## 4. 现代 Shell

### 4.1 跨 Shell 对比

| 特性 | Nushell | Oils (YSH) | Elvish | Xonsh | Murex | Fish |
|------|---------|-----------|--------|-------|-------|------|
| **结构化管道** | ✅ 原生表格 | ❌ | ✅ 值管道 | ❌ | ✅ 类型元数据 | ❌ |
| **后台任务** | `job spawn` | `fork {}` | `&` | `&` | `bg {}` | `&` |
| **任务间消息** | ✅ `job send/recv` | ❌ | ❌ | ❌ | ❌ | ❌ |
| **并行迭代** | `par-each` | ❌ | `peach` | ❌ | `foreach --parallel` | ❌ |
| **事件系统** | ❌ | ❌ | ❌ | ✅ | ✅ `event` | 有限 |
| **Bash 兼容** | ❌ | ✅ OSH | ❌ | 部分 | ❌ | ❌ |

### 4.2 Nushell 任务控制 ⭐

基于 Actor 模型的邮箱消息传递，所有调研 Shell 中最先进的结构化任务控制系统。

核心命令：`job spawn`（后台生成）、`job list`（表格列出）、`job kill`（杀死）、`job send/recv`（消息传递，支持 tag 过滤）、`par-each`（Rayon 并行迭代）。

**对 Cue Shell 的启示**：Actor 模型消息、显式 `job spawn`、表格化任务列表。**缺失**：无任务依赖/DAG、无队列限制、无持久化。

### 4.3 Oil Shell (Oils) Bash 兼容性 ⭐

双语言架构：OSH（兼容层）+ YSH（现代层），共享同一解释器，`shopt` 标志切换。

关键创新：结构化错误处理（`try`/`_error`/`_pipeline_status`）、显式进程创建（`fork`/`forkwait`）、J8 Notation、渐进迁移路径。

**对 Cue Shell 的启示**：渐进 Bash 迁移、结构化错误处理、`fork`/`forkwait` 显式语义。

### 4.4 其他 Shell

- **Elvish**：双通道管道（值 + 字节），复合异常模型
- **Murex**：类型化管道、事件系统（`onSecondsElapsed` 定时器）、FID 进程追踪
- **Fish**：`wait -n`（select 风格首完成等待）、`$pipestatus`、Fish 4.0 C++→Rust 重写

### 4.5 所有 Shell 共有的缺失（Cue Shell 的机会）

| 空白 | 描述 |
|------|------|
| **任务 DAG / 依赖** | 无 Shell 支持声明式"B 在 A 完成后运行" |
| **任务队列+限制** | 仅 murex 有 `--parallel N`；无真正工作队列 |
| **任务持久化** | 任务随 Shell 退出而死；无检查点/恢复 |
| **跨 Shell 通信** | 无 Shell 实例间结构化 IPC |
| **async/await 语法** | 无 Shell 有 `await` |
| **响应式管道** | 无 Shell 在上游变更时重跑下游 |
| **资源感知调度** | 无 Shell 调度时考虑 CPU/内存 |

---

## 5. 编码 Agent 工具接口

### 5.1 对比矩阵

| 维度 | Claude Code | Codex CLI | Cursor | Aider | Open Interpreter | SWE-agent | OpenHands | Devin |
|------|------------|-----------|--------|-------|-----------------|-----------|-----------|-------|
| **进程模型** | 持久 bash | 每次新建/PTY会话 | IDE 终端 | 每次新建 | 持久子进程 | 持久 pexpect | 持久 tmux | 云 VM |
| **默认超时** | **120s** | **10s** | 未知 | **无** | 无/120s | **25-30s** | **30s** | 未知 |
| **环境持久** | ✅ | ❌/✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ |
| **后台进程** | 3 种模式 | PTY 会话 | Agent 模式 | ❌ | Shell 语法 | ❌ | 部分 | 完整 VM |
| **PTY 支持** | ❌ | ✅ | ✅ | 部分 | ❌ | PTY via pexpect | PTY via tmux | ✅ |
| **沙箱** | FS 白名单+网络 | OS 原生 | 无 | 无 | 可选 Docker | Docker | Docker/K8s | 云 VM |

### 5.2 Claude Code ⭐

- 单个持久 bash 会话，Shell 快照恢复用户环境
- 3 模式后台（模型请求/用户 Ctrl+B/自动 15s）
- 输出限制 30K-150K 字符，溢出到磁盘（64 MB 上限）
- 23 个安全验证器链

### 5.3 Codex CLI ⭐

- 3 种 Shell 工具（`shell`/`shell_command`/`exec_command`），`exec_command` 有 PTY + 会话持久
- OS 原生沙箱（macOS Seatbelt / Linux Landlock+Seccomp）
- tree-sitter-bash 静态分析命令安全

### 5.4 其他 Agent 工具

- **OpenHands**：libtmux 后端 + PS1 元数据块检测
- **SWE-agent**：pexpect + PS1 哨兵，屏蔽 vim/gdb/less 等
- **Open Interpreter**：持久 subprocess + 哨兵检测，Jupyter 模式 LLM 自主决策

### 5.5 关键洞见

1. **同步执行是常态**——仅 Codex CLI 和 IDE 终端提供真正异步
2. **超时默认值差异巨大**（10s → 120s → 无限）——无基于命令类型的自适应超时
3. **环境持久化是全有或全无**——无选择性持久化
4. **无工具支持"命令运行中向模型流式发送部分输出"**
5. **无工具有"Shell 会话多路复用"概念**

---

## 6. Cron / 调度器替代方案

### 6.1 守护进程型 Cron 替代

- **fcron**：`serial` 队列（顺序运行防重叠）、`bootrun`（恢复错过的任务）、负载感知
- **supercronic**：容器优先、优雅关闭（SIGTERM 等待排空）、SIGUSR2 热重载、ENV 保留
- **systemd timers**：`Persistent=true` 重启恢复、`systemd-run --on-calendar` 一次性调度

### 6.2 进程内库 ⭐

- **robfig/cron (Go)**：基线参考，Job Wrapper 链（`SkipIfStillRunning`/`DelayIfStillRunning`），无持久化
- **gocron (Go)**：流式 API `Every(10).Seconds().Do()`，单例模式 + 全局并发限制
- **APScheduler (Python)** ⭐ 最佳参考架构：三触发器（interval/cron/date）、可插拔存储（SQLite/MongoDB/Redis）、pause/resume、EVENT_JOB_MISSED
- **tokio-cron-scheduler (Rust)**：异步原生，可选 PostgreSQL/NATS 持久化

### 6.3 分布式任务队列（参考价值）

- **asynq (Go)**：Redis 后端，优先级队列/重试/死信/去重
- **machinery (Go)**：Chain/Group/Chord 原语 → 直接对应 Cue Shell 的 `->`/`|||`

### 6.4 调度命令命名对比

**涌现的动词模式**：`schedule`/`add`、`remove`/`cancel`、`list`/`entries`、`pause`/`resume`、`enqueue`/`dispatch`

---

## 7. 命名约定汇总

### 7.1 进程/任务生命周期

| 动作语义 | 常见动词 | 使用场景 |
|---------|---------|---------|
| **创建并执行** | `run`, `exec`, `start` | just/task/nx 的 `run`；pm2 的 `start` |
| **创建但延迟执行** | `spawn`, `dispatch`, `enqueue`, `schedule` | nushell `job spawn`；asynq `Enqueue` |
| **停止** | `stop`, `kill`, `cancel`, `abort` | pm2 `stop`；nushell `job kill` |
| **暂停/恢复** | `pause`/`resume`, `freeze`/`unfreeze` | APScheduler |
| **查看状态** | `list`, `status`, `ls`, `ps`, `entries` | pm2 `list` |
| **查看输出** | `logs`, `tail`, `echo`, `cat` | pm2 `logs` |
| **连接/附着** | `connect`, `attach` | overmind `connect`；tmux `attach` |

### 7.2 对 Cue Shell 的命名建议

| Cue Shell 概念 | 建议动词 | 理由 |
|---------------|---------|------|
| 提交任务 | `run` 或 `spawn` | `run` 最直观；`spawn` 暗示异步 |
| 定时任务 | `schedule` 或 `cron` | 压倒性一致 |
| 停止任务 | `kill` | 信号语义精确 |
| 查看状态 | `list` 或 `jobs` | 通用 |
| 查看输出 | `out`/`err` | 分离 stdout/stderr |
| 连接会话 | `attach` 或 `fg` | tmux/screen 血统 |

---

## 8. 关键差异化定位

### 8.1 市场空白

没有任何现有工具同时覆盖：MCP Shell 执行 + 异步队列 + 交互终端 + 安全策略 + 环境累积 + TUI + 定时调度 + DAG 依赖。

### 8.2 Cue Shell 应从各竞品学习的

| 来源 | 应学习的设计 |
|------|------------|
| **block/agent-task-queue** | MCP 长连接绕过超时；FIFO + 命名队列；token 优化 |
| **Dagger** | 不可变状态链 + 环境累积 + 懒求值 + 可分叉 |
| **Nushell** | Actor 模型消息；结构化任务列表；显式 `job spawn` |
| **Oil Shell** | 渐进兼容模式；结构化错误处理 |
| **Claude Code** | 3 模式后台；Shell 快照；输出溢出到磁盘 |
| **Codex CLI** | OS 原生沙箱；tree-sitter 命令分析 |
| **APScheduler** | 可插拔存储 + SQLite；三触发器；pause/resume |
| **supercronic** | 优雅关闭；热重载 |
| **Fish** | `wait -n` 首完成选择；UX 标杆 |
| **machinery** | Chain/Group/Chord 通用调度原语 |

### 8.3 Cue Shell 填补的空白

| 空白 | 现状 | Cue Shell 的解法 |
|------|------|-----------------|
| **MCP + 异步队列 + 会话管理** 整合 | 分散在 3+ 个项目 | 单一运行时整合 |
| **宿主原生环境累积** | 仅 Dagger（需 Docker） | Scope env 快照 + 不可变链 |
| **Agent Shell 超时问题** | 10s-120s 硬超时 | MCP 长连接 + 可配置超时 + 后台模式 |
| **轻量级进程内调度** | robfig/cron 无持久化 | SQLite 持久化 + Rust 原生调度 |
| **人机协作终端** | wcgw 的 screen 最接近 | TUI 原生，人和 Agent 共享任务视图 |
| **任务 DAG + 队列 + 调度一体** | 分散在不同工具 | 统一的 Job → Chain → Schedule 模型 |

### 8.4 定位一句话总结

> **Cue Shell 不是 Shell，不是终端复用器，不是 CI/CD 引擎——它是一个面向人机协作的轻量级异步进程运行时，将 Dagger 的不可变环境累积、Nushell 的结构化任务控制、MCP 队列语义、APScheduler 的可持久化调度、以及 Claude Code 的多模式后台管理整合为一个带 TUI 的统一接口。**

---

## 附录：所有调研项目索引

| # | 项目 | 类别 | 星数 | 语言 | 相关度 |
|---|------|------|------|------|--------|
| 1 | block/agent-task-queue | Agent Shell | 38 | Python | ⭐⭐⭐ |
| 2 | rusiaaman/wcgw | Agent Shell | 655 | Python | ⭐⭐⭐ |
| 3 | NVIDIA/OpenShell | Agent Shell | 4,754 | Rust+Python | ⭐⭐ |
| 4 | agent-infra/sandbox | Agent Shell | 4,200 | 多语言 | ⭐ |
| 5 | sonirico/mcp-shell | Agent Shell | 69 | Go | ⭐⭐ |
| 6 | supervisord | 进程管理 | - | Python | ⭐ |
| 7 | pm2 | 进程管理 | - | Node.js | ⭐⭐ |
| 8 | overmind | 进程管理 | - | Go | ⭐⭐⭐ |
| 9 | just | 任务运行 | - | Rust | ⭐ |
| 10 | task (go-task) | 任务运行 | - | Go | ⭐⭐ |
| 11 | nx | 任务运行 | - | TypeScript | ⭐⭐ |
| 12 | turborepo | 任务运行 | - | Rust | ⭐⭐ |
| 13 | dagger | 任务运行 | - | Go | ⭐⭐⭐ |
| 14 | nushell | 现代 Shell | - | Rust | ⭐⭐⭐ |
| 15 | Oils (YSH/OSH) | 现代 Shell | - | Python/C++ | ⭐⭐ |
| 16 | elvish | 现代 Shell | - | Go | ⭐⭐ |
| 17 | murex | 现代 Shell | - | Go | ⭐⭐ |
| 18 | fish | 现代 Shell | - | Rust | ⭐ |
| 19 | Claude Code | Agent 工具 | 112K+ | TypeScript | ⭐⭐⭐ |
| 20 | Codex CLI | Agent 工具 | 74K+ | Rust | ⭐⭐⭐ |
| 21 | OpenHands | Agent 工具 | - | Python | ⭐⭐ |
| 22 | SWE-agent | Agent 工具 | - | Python | ⭐⭐ |
| 23 | robfig/cron | 调度器 | - | Go | ⭐⭐ |
| 24 | gocron | 调度器 | - | Go | ⭐⭐ |
| 25 | APScheduler | 调度器 | - | Python | ⭐⭐⭐ |
| 26 | tokio-cron-scheduler | 调度器 | - | Rust | ⭐⭐ |
| 27 | supercronic | 调度器 | - | Go | ⭐⭐ |
| 28 | asynq | 调度器 | - | Go | ⭐⭐ |
| 29 | machinery | 调度器 | - | Go | ⭐⭐ |

---

*本文档为 Cue Shell 竞品调研报告，覆盖 29 个相关项目的分析。调研时间 2025-07。*
