---
description: cue-shell 是一个 bash-like 的 durable process substrate，把命名 session、job、scope、chain、cron 作为一等原语，让人和 agent 共享同一个稳定的进程层；可选的 cue-flow 客户端层把声明式 workflow plan 编译到这些原语，不引入第二个 daemon。
owner: zrr1999
created: 2026-04-26
updated: 2026-07-22
inspired_by:
  - cue-shell
  - bash
  - zsh
  - tmux
  - zellij
  - nushell
  - fish
  - justfile
  - nanoflow
  - loom
  - warp
---

## 起源

cue-shell 现在的定位是一个 bash-like 的 durable async process substrate：
它保留把进程跑起来、串起来、看着输出的直觉，但把重点放在 job、
session、scope、chain、cron 这些持久化原语上。新方向进一步明确：
cue-shell 要在「命名、持久、可共享的进程 session」这一层替代 zellij，
让人和 agent 能 attach 到同一个现场，而不是各自维护一套终端与进程状态。

早期它确实承载过一条把 agent 也塞进 shell 的路线，也曾把
`:ask` / `:spawn` / `:agents` / `:confirm` / `:probe`、planner/executor
模型、ACP backend、agent transcript 放进 shell 这一层。现在这部分已经从
cue-shell 的命令与模式表面移除：cue-shell 只负责 process substrate；
agent runtime 与策略属于上层运行时。

2026 年 7 月归档 Nanoflow 后，它已经验证过的 TOML workflow、task matrix、
分层依赖、失败重试和 GPU admission 场景成为 Cue 的迁移输入，而不是另一套
executor 的长期维护负担。Cue 不复制 Nanoflow 的 Python 执行器，也不把
workflow 状态机塞进 `cued`；这些声明式语义由同仓库、无常驻服务的
`cue-flow` 客户端层规划，再提交给唯一的 process daemon。

## 产品/设计目标

cue-shell 想成为「比 bash 更 durable、比 systemd 更顺手、比传统终端复用器更面向进程状态」的本地进程底座，并在命名 session、持久托管与共享 attach 这一层替代 zellij。用户感觉它像 shell：起命令、看输出、串管线、组合并行/串行；但跟 bash 不同的是，每个 job 都是一等对象，daemon (`cued`) 持久托管，TUI 或 agent 断开后进程仍在，事件可以被重新订阅，scope 可以被快照、fork、回放。

Session 是 daemon 拥有的一等协作边界：它有稳定身份和可读名称，组织一组相关 job 与默认上下文。人可以从 TUI / CLI attach，agent 可以通过结构化 IPC attach；双方看到同一份 job 清单、输出与状态，并能在明确的只读观察和输入控制边界下交接操作权。客户端断开不等于 session 或其中进程结束。

它不试图替代 nushell 那种「数据流式 shell」的语义，也不试图替代 justfile 那种「项目命令登记簿」。它的重点在于：**进程的生命周期**本身被结构化，而不是数据或命令的形式被结构化。一个 job 有 id、有 scope hash、有 stdout/stderr stream、有 exit code、有可订阅的事件序列；多个 job 之间可以用最小的依赖图（serial / parallel / race / ignore-failure）拼起来；cron 则是一个 mechanical timer-to-command，把「时间」这一种触发源接到 job 接口上。

对外暴露三类客户端：TUI 给人用，JSON IPC + event stream 给上层运行时用，CLI 给脚本和 ad-hoc 调用用。所有客户端面对的都是同一个 cued daemon，同一套 session/job/scope 语义；「人」和「agent」只是不同客户端身份，不形成两套进程模型。

`cue-flow` 是第四种、可选的客户端能力：它读取声明式 workflow，生成可检查的
执行计划，再通过 typed IPC 提交 job / chain。它可以拥有 plan、retry 和聚合
进度语义，但不拥有第二套进程表、资源锁或 daemon 生命周期。

体验上，期望它在「把命令跑起来」这一刻和 bash 一样直接，也能像使用 zellij session 一样按名称离开、列出和重新进入工作现场；但在「这个命令昨天跑了什么、留下什么 scope、在什么 chain 里、谁在订阅或控制它」这些问题上，能立刻给出结构化答案。

## 目标用户

- 在终端里长期工作、对 bash/zsh/fish 的肌肉记忆很强、但被 shell 的 ephemeral 特性反复咬过的开发者。
- 在搭建本地 AI / 自动化栈、需要一个稳定 process 层来托起 agent runtime 和 workflow runtime，并需要在人与 agent 之间交接现场的工程师。
- 喜欢「机制 vs 策略」这种切法、希望底层只做机制、把策略留给上层的人。
- 暂时不是目标用户：想要一个 batteries-included 的 AI shell、希望 shell 直接帮自己决定「该让哪个 agent 做这件事」的用户——这部分体验属于上层 agent runtime。

## 核心原则

- **机制，不是策略**：cue-shell 只回答「怎么把进程跑起来、串起来、留下来」，不回答「该让谁做、该不该升级、用哪个 model」。
- **Session 是共享边界**：session 用稳定身份和名称组织相关 job、上下文与 attachment；人和 agent 使用同一协议观察现场，观察权与输入控制权保持明确。
- **Job 是一等对象**：每个进程都有稳定 id、scope 快照、事件流、退出态；不是 bash 那种「命令跑完就消失」的 ephemeral 模型。
- **Daemon 决定 durability**：人、agent 和 TUI 都是客户端，session 与 job 的真相在 `cued` 里——socket + SQLite + 进程表。客户端崩溃不影响 session 或 job。
- **Scope 不可变 + HEAD 指针**：env/cwd 用快照表达，fork 与 query 廉价；变更通过新建 scope + 移 HEAD 完成，不是就地 mutate。
- **结构化 IPC 优先**：所有外部交互走 JSON IPC + event stream；TUI 是这套协议的一个客户端，没有特权通道。
- **组合大于内置**：上层工具（warp、agent、workflow runner）是被 cue-shell 跑起来的普通可执行，不为它们加专用 builtin。
- **小而稳的原语集合**：Session / Job / Pipeline / Chain / Scope / Cron 六个原语足够覆盖目标场景；新增原语需要先证伪「能不能用现有原语组合出来」。
- **工具面与原子操作对齐**：每一条对外能力应尽量对应某一原语上的单一操作（起 job、观测、杀/取消、scope 读写、cron 启停），避免把编排策略塞回底座；形式化读本见 [`docs/design/conceptual-model.md`](docs/design/conceptual-model.md)。

## 能力地图（方向性）

- **Session**：可命名、可列出、可重新进入的持久进程现场；组织相关 job 与默认上下文，允许人和 agent 通过同一契约 attach、只读观察和显式交接控制；空闲现场可通过可逆 archive/restore 从日常列表收纳或恢复，不隐式删除历史。
- **Job 生命周期**：spawn / kill / cancel / wait / fg（PTY attach）/ tail / out / err / status / send（stdin 注入）。
- **Pipeline**：单 job 内部的 pipe 链，语义贴近 shell `|`，但每段仍可被观察。
- **Chain**：跨 job 的最小依赖图——serial（`->`）、parallel（`|||`）、race / any-success（`|?|`）、ignore-failure（`~>`）；`&&` / `||` 保留为单个 job 内的 shell-style 逻辑；不展开成完整 DAG runtime，那是 loom 的事。
- **Scope**：env / cwd 的不可变快照、HEAD 指针、fork、query、diff；scope hash 作为 job 的稳定上下文标识。
- **Resource admission**：provider-owned `need.*` 负责资源探测、原子预留与释放；方向上补齐 provider 状态变化 / `next_probe_at` 驱动的 pending 唤醒，以及可选的 NVIDIA utilization / memory-used-ratio 筛选。
- **Cron**：纯机械的 timer→command，把时间作为触发源接到 job 接口上；不承担任何 agent wake / escalation 语义。
- **JSON IPC + event stream**：daemon 暴露的唯一对外契约——session create/list/attach/watch，以及 argv/cwd/env/stdin → job_id, exit code, stdout/stderr, structured events, scope hash；断线客户端能够恢复对同一现场的观察。
- **Typed orchestration IPC**：在通用 `Eval` / `RunScript` 之外提供稳定的 `SubmitJob` / `SubmitChain`、`WaitExecution` 和 caller metadata 契约，让上层运行时不必拼接 cue-shell 源码字符串。
- **TUI 客户端**：以 process runtime 客户端身份存在，提供命名 session 选择与 attach、模式切换、命令输入、job 列表、输出 tab 等，便于人类直接观察和操控。
- **Daemon (`cued`)**：持久 Unix socket + SQLite，托管 session 元数据、job 历史、scope 表、cron 定义；客户端可自动重连并回到原 session。

## Nanoflow 迁移路线

目标不是 Python API 或内部实现的 1:1 兼容，而是让现有 Nanoflow 的代表性
workflow 可以在只运行一个 `cued` 的前提下完成 `plan`、`run`、观察与取消。

- **先把 `cued` 做成可靠执行目标**：resource-pending job 由 provider 变化或明确的 `next_probe_at` 唤醒，不再依赖「恰好有另一个 Cue job 结束」；NVIDIA provider 可选择利用率和显存占用策略；typed submit / wait / metadata 成为上层编排的稳定契约。
- **再交付薄 `cue-flow` 兼容层**：解析 workflow / task TOML，展开 matrix，生成稳定 task instance id，验证依赖与环；`plan` 输出规范化、可 diff 的 dry-run。Nanoflow 实际采用的 longest-path 分层 barrier 直接编译为 Cue chain，例如 `(a ||| b) -> (c ||| d)`，不为兼容性先造通用 DAG engine。
- **重试仍是新的 Job attempt**：每次尝试都有独立 job id；backoff 期间释放资源，重试耗尽后阻断下游。`cue-flow` 用 `FlowRun / TaskRun / Attempt` 聚合现有 job、chain 与 event，提供整体进度、取消和 TUI 分组，但不在 `cued` 复制 workflow 状态机。
- **证据触发的兼容面**：只有出现仍在使用 Nanoflow Python SDK 的真实调用方，才增加 JSON 可序列化的函数式适配；closure / pickle / 任意 Python callable 不进入 daemon。持久 `FlowRun`、补偿、外部事件唤醒和一般化 DAG 继续由 loom 承担，除非真实 workflow 证明 chain 编译模型不够。

## 成功信号

- agent 在一个命名 session 中启动 job 后，人可以直接 attach 并看到同一份输出与状态；人启动的进程也能被 agent 通过 IPC 无损接管或观察。
- 所有客户端都离开后，session 与其中仍在运行的 job 继续存在；稍后按名称重新进入时，现场连续而不是新建一个近似副本。
- 只读观察者不会因为客户端实现差异而意外写入 PTY；输入控制权的归属与交接对人和 agent 都清楚可见。
- 上层 agent runtime 只通过 JSON IPC 与 cue-shell 对话就能完成所有「起进程、读输出、写 stdin、订阅事件、组 chain」的事，不需要 cue-shell 为它加任何 agent 专用接口。
- 用户重启 TUI 之后能回到原来的命名 session，立刻看到正在跑的 job、它们的 scope、它们的事件流，没有「丢上下文」的感觉。
- 任何 job 的「为什么会以这个 env / cwd 跑」都能被一个 scope hash 说清楚，能被 fork 出新 scope 重放。
- 当被问到「这个功能该不该进 cue-shell」时，团队能直接用「这是机制还是策略」「现有原语能不能组合出来」回答，而不需要再翻一遍 SPARK。
- 看 cue-shell 的源码和命令集，看不出它知道「agent」「planner」「executor」「model」这些概念。
- 代表性的 Nanoflow TOML（matrix、分层依赖、retry、GPU request）能被 `cue-flow plan` 稳定归一化，并通过同一个 `cued` 执行；迁移不要求常驻第二个 workflow 服务。
- resource-pending job 即使没有其他 job 结束也能在资源条件变化后继续，且 attempt 结束或进入 backoff 时没有遗留 reservation。

## 生态关系

cue-shell 在本地自动化栈里处于最底层：

- **workflow / agent runtime → `cue-flow` / project runner → `cued`**：上层链路最终都把「实际跑的进程」落到同一个 cue-shell daemon 上。
- **cue-flow**：与 cue-shell 同仓库、无独立 daemon 的声明式 workflow 客户端层。它承接 Nanoflow 的 TOML / matrix / plan / retry 迁移面，并把结果编译为 typed job / chain 请求。
- **Nanoflow**：已归档的历史参考实现，不再是运行时依赖或待同步的第二套产品；其已验证用例作为 cue-flow 的兼容样本。
- **loom**：durable workflow runtime + automation kernel。需要持久 workflow 状态、外部事件唤醒、补偿或跨机协调的流程留在 loom；loom 在需要「跑一个进程」的地方调用 cue-shell。
- **agent runtime / control plane**：所有 agent 一等原语（AGENT mode、planner/executor、ACP backend lifecycle、agent transcript、agent wake/escalation、`:ask` / `:spawn` / `:agents` / `:confirm` / `:probe`）都属于上层运行时。上层可以把自己的 conversation / task 绑定到某个 cue-shell process session，但二者不是同一种 session，也不要求一一对应。
- **warp**：项目执行基础层 CLI。对 cue-shell 来说，warp 是一个**普通可执行**——cue-shell 不为 warp 加专门的 builtin，也不感知它的项目模型。
- **bash / zsh / fish 等传统 shell**：cue-shell 不替代它们做交互式通用 shell；它替代的是「把进程长期、可观察、可恢复地跑在某台机器上」这一段。
- **tmux / zellij**：cue-shell 接管其中「命名、持久、可共享的进程 session」这层职责，让人和 agent attach 同一现场；TUI 仍只是 process runtime 的一个 view。pane/tab 几何、layout 语言、插件生态与浏览器终端不属于第一阶段替代范围。

边界一句话：**cue-shell 只暴露命名 process session 与 process 层契约——attach/watch + argv/cwd/env/stdin → job_id, exit code, stdout/stderr, structured events, scope hash。再往上的 agent、workflow 与终端布局语义都属于上层运行时或客户端。**

## 什么不是本项目要做的（Non-goals）

- **不再把 agent 作为一等原语**：移除 / 迁出 AGENT mode、planner/executor 权限模型、agent transcript 持久化、agent wake events。
- **不把 agent task / conversation 与 process session 混为一谈**：人和 agent 共享的是进程现场；agent 身份、对话生命周期和调度策略仍在上层。
- **不承担 agent-policy 命令**：`:ask` / `:spawn` / `:agents` / `:confirm` / `:escalate` / `:probe` 等不再属于 cue-shell，也不再保留兼容命令面。
- **不管理 ACP backend lifecycle**：哪个 backend、哪个 model、什么时候启动/重连，由上层 agent runtime 决定；cue-shell 只把它当普通子进程。
- **`cued` 不做 workflow / DAG runtime**：多步计划可以由 `cue-flow` 编译；持久状态、补偿、长时编排留在 loom，daemon 的 chain 仍只到最小依赖图。
- **不做项目级命令登记簿**：justfile / warp 的角色不接管。
- **不做数据流式 shell**：nushell 那种把命令结果当结构化数据传递的语义不进入。
- **不做远程多机集群调度**：当前定位是单机 daemon；多机由上层运行时通过多个 cue-shell 实例聚合。
- **第一阶段不复制完整终端复用器**：不实现 pane/tab 几何管理、layout 语言、插件生态或 zellij 的全部交互界面；客户端可以自行组织视图。
- **第一阶段不提供 Web 终端分享面**：浏览器 attach、分享 token 与公网访问控制后置；远程场景继续通过现有 SSH gateway 或上层系统进入单机 daemon。
- **不为特定上层项目加专用 builtin**：包括 warp、loom 或任何 agent runtime。
- **不内置秘密/凭据管理**：scope 只携带 env，秘密策略由上层负责。
- **不执行不透明 Python 对象**：不把 closure、pickle 或任意 callable 穿过 IPC；需要 Python 兼容时只接受显式、可审计、JSON 可序列化的输入输出边界。

## 已考虑的替代方案 & 理由

- **直接用 bash + nohup + tmux / zellij**：起步最快，也有成熟的命名 session 与终端 UI，但 job、scope 和事件仍以终端为中心，agent 只能另建控制路径或模拟人的终端操作。cue-shell 选择让 daemon 拥有命名 process session，使人和 agent 共享同一份结构化真相；迁移期间 tmux / zellij 仍可作为外层终端。
- **基于 systemd / launchd 做 user-level service 托管**：太重，单 job 概念过于「服务化」，对交互式开发流不友好；也很难给 TUI 客户端一个好的事件流模型。
- **直接基于 nushell 扩展**：nushell 的核心价值在结构化数据流，与 cue-shell 想强调的「结构化进程生命周期」是正交问题，强行嵌入会同时拖累两边。
- **保留原方案，把 agent / workflow 都留在 cue-shell**：上一版本就是这样。结果是策略和机制混在一起，AGENT mode 的需求反复挤压 process 层；这条线已经没必要继续。
- **把 cron 做成「agent wake / scheduler」**：会把策略再次塞回底层。最终选择把 cron 限定为 mechanical timer-to-command，agent wake / schedule 由 loom 负责。
- **把 chain 扩展为完整 DAG runtime（含重试、补偿、状态机）**：与 loom 的定位严重重叠，且会把 cue-shell 的复杂度拉到不可控范围。chain 因此被刻意限制在最小依赖图。
- **把 Nanoflow executor 原样并入 `cued`**：会复制 Python callable、workflow 状态、资源调度和服务生命周期；最终选择只迁移已验证的声明式能力，由 `cue-flow` 规划、`cued` 执行。
- **在 process_mgr 中引入 `sh -c` 作为 multi-segment pipeline 的兜底**：以 shell 黑盒替代 native pipe，表面上兼容性好，但实际上破坏了 job 的可观测性（每个 segment 不再是一等对象）和 wrapper/forbid/replace 的逐 segment 作用。当前决策：multi-segment pipeline 不做兜底，直接走 native pipe(2) spawn；不支持 pipe semantics 的 segment 应拒绝执行而非退化。
- **multi-segment pipeline 在 `sh -c` 中执行并静默丢弃 segment 信息**：同上一项，已确定不采用。

## 文档与源码索引

- 设计总览与文档目录：[`docs/design/README.md`](docs/design/README.md)
- 概念模型（job/state、scope/hash、fork 语义、原子工具面）：[`docs/design/conceptual-model.md`](docs/design/conceptual-model.md)
- `cued` actor 与存储：[`docs/design/daemon-architecture.md`](docs/design/daemon-architecture.md)

根目录 [`ARCHITECTURE.md`](ARCHITECTURE.md) 仅保留到新文档的入口。

## 开放问题

- session 已确定采用保留身份与历史的可逆 archive/restore，并在有连接、非终态工作或 cron 时拒绝归档；尚待决定的仅是 hard deletion、自动 expiry 与历史 retention 策略，以及它们是否应该存在。<!-- 待确认 -->
- 输出与终端状态需要保留到什么粒度，才能让断线客户端精确追平，同时避免把完整终端模拟器塞进 daemon？<!-- 待确认 -->
- session 的默认上下文与不可变 scope / HEAD 指针如何关联：session 是持有一个可移动的 scope 引用，还是只记录 job 各自的 scope？<!-- 待确认 -->
- JSON IPC 的事件 schema 在「agent 概念迁出 cue-shell」之后，是否需要一次破坏性重整？现有 event 名是否还有泄漏的 agent 语义？<!-- 待确认 -->
- scope 在高频 fork、长生命周期下，SQLite + delta 链（见 daemon 设计文档中的 ScopeStore）是否够用；若出现明显放大，是否引入专用 scope GC/分层存储？<!-- 待确认 -->
- cue-shell 与上层 agent runtime 之间，agent 输出（stdout/stderr）与 agent 语义事件（turn 开始/结束、tool 调用）的边界——cue-shell 是否完全不感知后者，还是允许一个「passthrough event」通道？<!-- 待确认 -->
- 远程使用场景（当前通过 SSH gateway）是否长期保留，还是收敛成「单机 daemon + 上层负责跨机」？<!-- 待确认 -->
- TUI 是否应该拆成独立 crate / 独立仓库，让 cue-shell 核心更纯粹？<!-- 待确认 -->
- Pipeline 的语义是否完全等价于 shell `|`，还是允许每段单独 attach observer？这影响实现复杂度。<!-- 待确认 -->
- typed submit metadata 最小需要哪些字段，才能让 `cue-flow` 关联 `FlowRun / TaskRun / Attempt`，同时不让 `cued` 开始理解 workflow？<!-- 待确认 -->

## 修订记录

- 2026-07-19：归档 Nanoflow，将其已验证的 TOML / matrix / 分层依赖 / retry / resource workflow 能力收敛为同仓库 `cue-flow` 客户端路线；补充 pending-resource 唤醒与 typed orchestration IPC，保持唯一 `cued` daemon 和 process substrate 边界。
- 2026-07-22：确认 session 清理第一阶段只提供可逆 archive/restore；归档必须无连接、无非终态工作且无自有 cron，不提供 force 或 deletion，hard deletion/expiry/retention 留作开放问题。
- 2026-07-22：确认共享 PTY 采用多 observer、单 controller；只允许显式 release/claim，不强制抢占，客户端断开释放控制权但不终止 job。
- 2026-07-22：确认 cue-shell 在命名、持久、共享 attach 的 process session 层替代 zellij；pane/tab 布局、插件生态与 Web 分享保留为首阶段非目标。
- 2026-05-17：新增「工程原则」章节——MSRV 即唯一测试目标、不写兜底策略、质量优于数量；已考虑的替代方案中明确拒绝 `sh -c` 作为 pipeline 兜底。
- 2026-05-15：文档整理——原根目录长篇架构迁入 [`docs/design/conceptual-model.md`](docs/design/conceptual-model.md)；`ARCHITECTURE.md` 改为入口；README 设计文档链接对齐；补充 SPARK 与概念模型、设计索引的交叉引用。
- 2026-04-26：初稿。从「agent+workflow shell」收缩为 bash-like durable process substrate；agent 一等原语、planner/executor、ACP backend lifecycle、agent transcript、agent wake/escalation、`:ask` / `:spawn` / `:agents` / `:confirm` / `:probe` 等迁出至上层 agent runtime；多步 workflow 编排归 loom；warp 退化为被 cue-shell 跑起来的普通可执行。
