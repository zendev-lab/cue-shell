---
description: cue-shell 是一个 bash-like 的 durable async process substrate，把 job、scope、chain、cron 作为一等原语，给上层 agent runtime 与 workflow runtime 提供稳定的进程层底座。
owner: zrr1999
created: 2026-04-26
updated: 2026-05-15
inspired_by:
  - cue-shell
  - bash
  - zsh
  - tmux
  - zellij
  - nushell
  - fish
  - justfile
  - loom
  - weft
  - warp
---

## 起源

cue-shell 现在的定位是一个 bash-like 的 durable async process substrate：
它保留把进程跑起来、串起来、看着输出的直觉，但把重点放在 job、
scope、chain、cron 这些持久化原语上。

早期它确实承载过一条把 agent 也塞进 shell 的路线，也曾把
`:ask` / `:spawn` / `:agents` / `:confirm` / `:probe`、planner/executor
模型、ACP backend、agent transcript 放进 shell 这一层。现在这部分已经从
cue-shell 的命令与模式表面移除：cue-shell 负责 process substrate，weft
负责 agent runtime 与策略。

## 产品/设计目标

cue-shell 想成为「比 bash 更 durable、比 systemd 更顺手、比 tmux 更结构化」的本地进程底座。用户感觉它像 shell：起命令、看输出、串管线、组合并行/串行；但跟 bash 不同的是，每个 job 都是一等对象，daemon (`cued`) 持久托管，TUI 关掉再开还在，事件可以被订阅，scope 可以被快照、fork、回放。

它不试图替代 nushell 那种「数据流式 shell」的语义，也不试图替代 justfile 那种「项目命令登记簿」。它的重点在于：**进程的生命周期**本身被结构化，而不是数据或命令的形式被结构化。一个 job 有 id、有 scope hash、有 stdout/stderr stream、有 exit code、有可订阅的事件序列；多个 job 之间可以用最小的依赖图（serial / parallel / race / ignore-failure）拼起来；cron 则是一个 mechanical timer-to-command，把「时间」这一种触发源接到 job 接口上。

对外暴露三类客户端：TUI 给人用，JSON IPC + event stream 给上层运行时（loom / weft）用，CLI 给脚本和 ad-hoc 调用用。所有客户端面对的都是同一个 cued daemon，同一套 job/scope 语义。

体验上，期望它在「把命令跑起来」这一刻和 bash 一样直接，但在「这个命令昨天跑了什么、留下什么 scope、在什么 chain 里、谁在订阅它」这些问题上，能立刻给出结构化答案。

## 目标用户

- 在终端里长期工作、对 bash/zsh/fish 的肌肉记忆很强、但被 shell 的 ephemeral 特性反复咬过的开发者。
- 在搭建本地 AI / 自动化栈、需要一个稳定 process 层来托起 agent runtime 和 workflow runtime 的工程师——也就是 weft 与 loom 的作者和使用者。
- 喜欢「机制 vs 策略」这种切法、希望底层只做机制、把策略留给上层的人。
- 暂时不是目标用户：想要一个 batteries-included 的 AI shell、希望 shell 直接帮自己决定「该让哪个 agent 做这件事」的用户——这部分体验在 weft。

## 核心原则

- **机制，不是策略**：cue-shell 只回答「怎么把进程跑起来、串起来、留下来」，不回答「该让谁做、该不该升级、用哪个 model」。
- **Job 是一等对象**：每个进程都有稳定 id、scope 快照、事件流、退出态；不是 bash 那种「命令跑完就消失」的 ephemeral 模型。
- **Daemon 决定 durability**：人和 TUI 是客户端，真相在 `cued` 里——socket + SQLite + 进程表。客户端崩溃不影响 job。
- **Scope 不可变 + HEAD 指针**：env/cwd 用快照表达，fork 与 query 廉价；变更通过新建 scope + 移 HEAD 完成，不是就地 mutate。
- **结构化 IPC 优先**：所有外部交互走 JSON IPC + event stream；TUI 是这套协议的一个客户端，没有特权通道。
- **组合大于内置**：上层工具（warp、agent、workflow runner）是被 cue-shell 跑起来的普通可执行，不为它们加专用 builtin。
- **小而稳的原语集合**：Job / Pipeline / Chain / Scope / Cron 五个原语足够覆盖目标场景；新增原语需要先证伪「能不能用现有原语组合出来」。
- **工具面与原子操作对齐**：每一条对外能力应尽量对应某一原语上的单一操作（起 job、观测、杀/取消、scope 读写、cron 启停），避免把编排策略塞回底座；形式化读本见 [`docs/design/conceptual-model.md`](docs/design/conceptual-model.md)。

## 能力地图（方向性）

- **Job 生命周期**：spawn / kill / cancel / wait / fg（PTY attach）/ tail / out / err / status / send（stdin 注入）。
- **Pipeline**：单 job 内部的 pipe 链，语义贴近 shell `|`，但每段仍可被观察。
- **Chain**：跨 job 的最小依赖图——serial（`->`）、parallel（`|||`）、race / any-success（`|?|`）、ignore-failure（`~>`）；`&&` / `||` 保留为单个 job 内的 shell-style 逻辑；不展开成完整 DAG runtime，那是 loom 的事。
- **Scope**：env / cwd 的不可变快照、HEAD 指针、fork、query、diff；scope hash 作为 job 的稳定上下文标识。
- **Cron**：纯机械的 timer→command，把时间作为触发源接到 job 接口上；不承担任何 agent wake / escalation 语义。
- **JSON IPC + event stream**：daemon 暴露的唯一对外契约——argv/cwd/env/stdin → job_id, exit code, stdout/stderr, structured events, scope hash。
- **TUI 客户端**：以 process runtime 客户端身份存在，提供模式切换、命令输入、job 列表、输出 tab 等，便于人类直接观察和操控。
- **Daemon (`cued`)**：持久 Unix socket + SQLite，托管 job 历史、scope 表、cron 定义；TUI 自动重连。

## 成功信号

- 上层 agent runtime（weft）只通过 JSON IPC 与 cue-shell 对话就能完成所有「起进程、读输出、写 stdin、订阅事件、组 chain」的事，不需要 cue-shell 为它加任何 agent 专用接口。
- 用户重启 TUI 之后立刻能看到正在跑的 job、它们的 scope、它们的事件流，没有「丢上下文」的感觉。
- 任何 job 的「为什么会以这个 env / cwd 跑」都能被一个 scope hash 说清楚，能被 fork 出新 scope 重放。
- 当被问到「这个功能该不该进 cue-shell」时，团队能直接用「这是机制还是策略」「现有原语能不能组合出来」回答，而不需要再翻一遍 SPARK。
- 看 cue-shell 的源码和命令集，看不出它知道「agent」「planner」「executor」「model」这些概念。

## 生态关系

cue-shell 在四仓库闭环里处于最底层：

- **prompt / schedule → loom → weft → warp → cue-shell**：上层链路最终都要把「实际跑的进程」落到 cue-shell 上。
- **loom**：durable workflow runtime + automation kernel。多步、长时、需要重试和补偿的 workflow 留在 loom；loom 在需要「跑一个进程」的地方调用 cue-shell。
- **weft**：agent runtime / control plane + superagent CLI。所有 agent 一等原语（AGENT mode、planner/executor、ACP backend lifecycle、agent transcript、agent wake/escalation、`:ask` / `:spawn` / `:agents` / `:confirm` / `:probe`）都迁出到 weft。cue-shell 不再对外暴露这些能力。
- **warp**：项目执行基础层 CLI。对 cue-shell 来说，warp 是一个**普通可执行**——cue-shell 不为 warp 加专门的 builtin，也不感知它的项目模型。
- **bash / zsh / fish 等传统 shell**：cue-shell 不替代它们做交互式通用 shell；它替代的是「把进程长期、可观察、可恢复地跑在某台机器上」这一段。
- **tmux / zellij**：cue-shell 不做终端复用层；TUI 只是 process runtime 的一个 view。

边界一句话：**cue-shell 只暴露 process 层契约——argv/cwd/env/stdin → job_id, exit code, stdout/stderr, structured events, scope hash。再往上的所有语义都属于上层运行时。**

## 什么不是本项目要做的（Non-goals）

- **不再把 agent 作为一等原语**：移除 / 迁出 AGENT mode、planner/executor 权限模型、agent transcript 持久化、agent wake events。
- **不承担 agent-policy 命令**：`:ask` / `:spawn` / `:agents` / `:confirm` / `:escalate` / `:probe` 等不再属于 cue-shell，也不再保留兼容命令面。
- **不管理 ACP backend lifecycle**：哪个 backend、哪个 model、什么时候启动/重连，由 weft 决定；cue-shell 只把它当普通子进程。
- **不做 workflow / DAG runtime**：多步、补偿、长时编排留在 loom；cue-shell 的 chain 只到最小依赖图。
- **不做项目级命令登记簿**：justfile / warp 的角色不接管。
- **不做数据流式 shell**：nushell 那种把命令结果当结构化数据传递的语义不进入。
- **不做远程多机集群调度**：当前定位是单机 daemon；多机由上层运行时通过多个 cue-shell 实例聚合。
- **不为特定上层项目加专用 builtin**：包括 warp、weft、loom 自己。
- **不内置秘密/凭据管理**：scope 只携带 env，秘密策略由上层负责。

## 已考虑的替代方案 & 理由

- **直接用 bash + nohup + tmux**：起步最快，但 job 不是一等对象、scope 不可快照、事件无法订阅，复杂场景维护成本陡升。cue-shell 把这些痛点结构化。
- **基于 systemd / launchd 做 user-level service 托管**：太重，单 job 概念过于「服务化」，对交互式开发流不友好；也很难给 TUI 客户端一个好的事件流模型。
- **直接基于 nushell 扩展**：nushell 的核心价值在结构化数据流，与 cue-shell 想强调的「结构化进程生命周期」是正交问题，强行嵌入会同时拖累两边。
- **保留原方案，把 agent / workflow 都留在 cue-shell**：上一版本就是这样。结果是策略和机制混在一起，AGENT mode 的需求反复挤压 process 层；新增 loom / weft 后这条线已经没必要继续。
- **把 cron 做成「agent wake / scheduler」**：会把策略再次塞回底层。最终选择把 cron 限定为 mechanical timer-to-command，agent wake / schedule 由 loom 负责。
- **把 chain 扩展为完整 DAG runtime（含重试、补偿、状态机）**：与 loom 的定位严重重叠，且会把 cue-shell 的复杂度拉到不可控范围。chain 因此被刻意限制在最小依赖图。
- **在 process_mgr 中引入 `sh -c` 作为 multi-segment pipeline 的兜底**：以 shell 黑盒替代 native pipe，表面上兼容性好，但实际上破坏了 job 的可观测性（每个 segment 不再是一等对象）和 wrapper/forbid/replace 的逐 segment 作用。当前决策：multi-segment pipeline 不做兜底，直接走 native pipe(2) spawn；不支持 pipe semantics 的 segment 应拒绝执行而非退化。
- **multi-segment pipeline 在 `sh -c` 中执行并静默丢弃 segment 信息**：同上一项，已确定不采用。

## 文档与源码索引

- 设计总览与文档目录：[`docs/design/README.md`](docs/design/README.md)
- 概念模型（job/state、scope/hash、fork 语义、原子工具面）：[`docs/design/conceptual-model.md`](docs/design/conceptual-model.md)
- `cued` actor 与存储：[`docs/design/daemon-architecture.md`](docs/design/daemon-architecture.md)

根目录 [`ARCHITECTURE.md`](ARCHITECTURE.md) 仅保留到新文档的入口。

## 开放问题

- JSON IPC 的事件 schema 在「agent 概念迁出 weft」之后，是否需要一次破坏性重整？现有 event 名是否还有泄漏的 agent 语义？<!-- 待确认 -->
- scope 在高频 fork、长生命周期下，SQLite + delta 链（见 daemon 设计文档中的 ScopeStore）是否够用；若出现明显放大，是否引入专用 scope GC/分层存储？<!-- 待确认 -->
- cue-shell 与 weft 之间，agent 输出（stdout/stderr）与 agent 语义事件（turn 开始/结束、tool 调用）的边界——cue-shell 是否完全不感知后者，还是允许一个「passthrough event」通道？<!-- 待确认 -->
- 远程使用场景（当前通过 SSH gateway）是否长期保留，还是收敛成「单机 daemon + 上层负责跨机」？<!-- 待确认 -->
- TUI 是否应该拆成独立 crate / 独立仓库，让 cue-shell 核心更纯粹？<!-- 待确认 -->
- Pipeline 的语义是否完全等价于 shell `|`，还是允许每段单独 attach observer？这影响实现复杂度。<!-- 待确认 -->

## 修订记录

- 2026-05-17：新增「工程原则」章节——MSRV 即唯一测试目标、不写兜底策略、质量优于数量；已考虑的替代方案中明确拒绝 `sh -c` 作为 pipeline 兜底。
- 2026-05-15：文档整理——原根目录长篇架构迁入 [`docs/design/conceptual-model.md`](docs/design/conceptual-model.md)；`ARCHITECTURE.md` 改为入口；README 设计文档链接对齐；补充 SPARK 与概念模型、设计索引的交叉引用。
- 2026-04-26：初稿。从「agent+workflow shell」收缩为 bash-like durable process substrate；agent 一等原语、planner/executor、ACP backend lifecycle、agent transcript、agent wake/escalation、`:ask` / `:spawn` / `:agents` / `:confirm` / `:probe` 等迁出至 weft；多步 workflow 编排归 loom；warp 退化为被 cue-shell 跑起来的普通可执行。
