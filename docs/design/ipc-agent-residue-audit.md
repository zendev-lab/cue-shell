# cue-shell JSON IPC — Agent 语义残留审计

> 目标：在 SPARK 把 cue-shell 重新定位为 *bash-like durable process substrate* 之后，审计当前 JSON IPC（请求 / 响应 / 事件）协议表面上仍然泄漏的 `agent / planner / executor / model / ACP / transcript / wake / escalate / probe / ask / spawn` 语义。
>
> **本文档只产出文字结论，不修改任何 `.rs` / `Cargo.toml` 文件。** 任何代码改动都应作为后续 PR 单独提出，且需要等到 weft northbound v1 合同稳定后才能真正下刀。

## 1. 范围与方法

### 1.1 关键词

围绕以下关键词全仓 `grep`：

```
agent  planner  executor  acp  model  transcript
wake   escalate probe     ask   spawn  send  cancel  backend
```

（`spawn` 在 Tokio / actor 语义里是合法的——只挑出与 agent session 生命周期相关的命中；`backend` 同理，挑出指 *agent backend* 的用法。）

### 1.2 扫描的 crate / 文件

| Crate | 扫描重点 |
| --- | --- |
| `cue-core` | `src/ipc.rs`、`src/agent.rs`、`src/mode.rs`、`src/id.rs`、`src/lib.rs` |
| `cued` | `src/actor/{mod,gateway,scheduler}.rs`、`src/config.rs`、`src/weft.rs`、`src/parser/{ast,token,tokenizer,parse,resolver}.rs` |
| `cue-cli` | `src/{main,config}.rs`（结果：无残留——只引用透传协议） |
| `cue-client` | `src/*.rs`（结果：无残留） |
| `cue-tui` | `src/app.rs`、`src/component/{sidebar,status_bar,main_view,input_line}.rs` |
| `docs/design/` | `ipc-protocol.md`、`commands-and-modes.md`、`core-types.md`（与协议绑定） |

### 1.3 审计原则

一条命中算「泄漏」当且仅当满足以下任一条件：

1. **非目标直接冲突**：SPARK §「什么不是本项目要做的」明确划走（agent 一等原语 / planner-executor / ACP backend lifecycle / agent transcript / agent wake / `:ask` / `:spawn` / `:agents` / `:confirm` / `:escalate` / `:probe`）。
2. **机制名带策略词**：底层是通用机制（permission / channel / session）但命名嵌入了 agent 语义。
3. **已知兼容桥**：现状是把请求转发到 weft，长期应整体由 weft northbound 接管。

## 2. 现状清单（按文件）

> 行号基于 `cleanup/cueshell-agent-cleanup` 分支 commit `4c9ffa1`。

### 2.1 `crates/cue-core/src/ipc.rs` — IPC schema 单一来源

| 行号 | 类型 / 字段 | 当前语义 | 泄漏判断 |
| --- | --- | --- | --- |
| L10 | `use crate::agent::AgentStatus;` | 把 agent 模块拉进协议层 | core 协议依赖 agent 模块本体 |
| L59–L62 | `RequestPayload::AgentPrompt { id, prompt }` | 向兼容 agent session 注入 prompt | 这是 `:ask`/`:send A<n>` 的结构化对应；语义是 agent turn |
| L63–L65 | `RequestPayload::AgentCancel { id }` | 取消当前 agent turn | 同上，agent turn 概念 |
| L106–L108 | `OkPayload::AgentSpawned { agent_id }` | `:spawn` / `:ask` 成功响应 | agent session 一等响应 |
| L120–L121 | `OkPayload::AgentInfo(AgentInfo)` / `AgentList(Vec<AgentInfo>)` | `:agents` 列表 / 重连快照 | 直接暴露 agent registry |
| L188–L192 | `EventPayload::AgentStateChanged { agent_id, old_state, new_state }` | 走 channel `"agents"` | agent lifecycle 事件，policy 级 |
| L193–L197 | `EventPayload::AgentMessage { agent_id, role, content }` | 转发 agent turn 文本 chunk | conversation 流——明确属于 weft |
| L281–L291 | `struct AgentInfo { id, status, backend, role, transcript, last_role }` | UI 侧 agent registry 的 wire schema | `transcript` / `role` / `backend` 全是 policy 字段 |
| L70–L80（comment） | doc 注释里的 "agents" channel 描述 | 通过 `Subscribe { channels }` 订阅 | 字符串值是 "agents" → 见 §2.6 |

### 2.2 `crates/cue-core/src/agent.rs` — 整模块

| 行号 | 项 | 泄漏判断 |
| --- | --- | --- |
| L13–L19 | `enum AgentStatus { Running, WaitingInput, Done, Failed }` | `WaitingInput` 是 conversation turn 概念，不是 process state |
| L22–L28 | `enum AgentKind { Cli{ command, has_pty }, Api{ model } }` | `Api { model }` 是 LLM-API 抽象——SPARK 明确不做 |
| L31–L37 | `enum AgentRole { Planner, Executor }` | **直接命中** SPARK 非目标：planner/executor 模型迁出 |
| 整文件 | 所有内容 | A 类（应整模块迁出到 weft）|

### 2.3 `crates/cue-core/src/mode.rs`

| 行号 | 项 | 泄漏判断 |
| --- | --- | --- |
| L11 | `Mode::Agent` 变体 | 与 SPARK §「不再把 agent 作为一等原语」直接冲突；目前 indicator 已被改成 "⚡ JOB" 来掩盖（L31, L48），但枚举值仍在协议线上序列化 |
| L21, L23 | `next()` 路径包含 `Mode::Agent` | 同上 |
| L40 | `default_command(Mode::Agent) => "ask"` | 把 mode 直接绑到 `:ask` 命令 |

### 2.4 `crates/cue-core/src/id.rs`

| 行号 | 项 | 泄漏判断 |
| --- | --- | --- |
| L10–L11, L42–L46 | `struct AgentId(pub u32)` + `Display = "A{n}"` | agent 是协议级别的 first-class id 命名空间 |
| L29 | `EntityRef::Agent(AgentId)` | 命令引用语法（`:fg A1` / `:kill A1`）泄漏到 core enum |

### 2.5 `crates/cue-core/src/lib.rs`

| 行号 | 项 | 泄漏判断 |
| --- | --- | --- |
| L6 | `pub mod agent;` | 上游可见 |
| L17 | `pub use id::{AgentId, ..., EntityRef, ...};` | re-export 绑定 |

### 2.6 IPC 通道字符串

| 出现位置 | 值 | 泄漏判断 |
| --- | --- | --- |
| `crates/cued/src/actor/scheduler.rs` L318, L338 | `channel: "agents".into()` | event bus 上的 channel 名字面量是 agent 概念 |
| `docs/design/ipc-protocol.md` §5 | `"agents"` 列在 channel 类型中 | 协议文档绑定 |
| `crates/cue-tui/src/app.rs`（订阅默认集合及处理）| 间接消费 | 客户端默认 subscribe |

### 2.7 `crates/cued/src/actor/mod.rs`

| 行号 | 项 | 泄漏判断 |
| --- | --- | --- |
| L71–L76 | `SchedulerMsg::AgentMessage { agent_id, role, content }` | actor 消息层就携带 conversation 形状 |
| L77–L81 | `SchedulerMsg::AgentStateChanged { agent_id, status: AgentStatus }` | 同 §2.2 |
| L82–L86 | `SchedulerMsg::AgentSessionBound { agent_id, session_id }` | doc-comment 写明 "An agent has been bound to a concrete **ACP** session ID"——ACP 概念直接进 actor 层 |

### 2.8 `crates/cued/src/actor/gateway.rs`

| 行号 | 项 | 泄漏判断 |
| --- | --- | --- |
| L446–L455 | `RequestPayload::AgentPrompt → ResolvedCommand::Send` 路由 | 兼容桥实现细节 |
| L457–L466 | `RequestPayload::AgentCancel → ResolvedCommand::Cancel` 路由 | 同上 |
| L580–L613（tests） | `agent_prompt_routes_through_send_command` 等 | 测试钉住兼容形状 |

### 2.9 `crates/cued/src/actor/scheduler.rs`（最大泄漏面，仅列代表）

| 行号 | 项 | 泄漏判断 |
| --- | --- | --- |
| L18 | `use cue_core::agent::{AgentRole, AgentStatus};` | 调度器对 agent role/status 有 first-class 依赖 |
| L30 | `use crate::config::{AgentBackendConfig, AgentTransport, ...};` | 配置层 backend 概念注入 |
| L115–L129 | `struct AgentEntry { backend, role, status, proxied, control, session_id, model, scope_hash, transcript, last_role }` | scheduler 持久化 agent registry，含 `transcript` / `model` |
| L131–L143 | `enum AgentControl { Prompt, Abort, Shutdown }` / `enum AgentLaunch { Prompt { initial_prompt, requested_session } }` | turn 控制原语 |
| L170, L181, L187, L213–L216 | `next_agent` / `agents` / `agent_waiters` / `alloc_agent` | 调度器内部 agent registry |
| L302–L355 | `SchedulerMsg::Agent*` 三条 handler | 见 §2.7 |
| L635–L666 | `append_agent_transcript` | transcript 持久化逻辑（SPARK 明确迁出）|
| L668–L678 | `parse_agent_role(... AgentRole)` | planner/executor 解析 |
| L681–L702 | `resolve_backend` / `agent_transport_is_weft` | bridge backend 选择 |
| L733–L754 | `weft_validate_agent_params` / `weft_proxy_transcript` | weft proxy specific |
| L785–L860 | `spawn_weft_proxy_agent` | 兼容桥主体 |
| L862–L935 | `send_agent_failure` / `write_agent_request` / `write_agent_response` / `write_agent_error_response` | ACP JSON-RPC 框架代码 |
| L945–L968 | `write_acp_prompt` / `extract_acp_text_block` | **直接 ACP 协议字面量** |
| L987–L1067 | `forward_acp_session_update` / `acp_response_error` | ACP session/update / agent_message_chunk 等 |
| L1078–L1095 | `enum AcpPhase { Initializing, Prompting, Idle }` | ACP turn 状态机 |
| L1098–L1145 | `launch_agent` 启动 ACP backend 子进程 (`--model`, stdin/stdout pipe) | ACP backend lifecycle |
| L1192–L1330 | ACP 主循环（Prompting / Idle 状态切换、`session/load`、`agent_message_chunk` 转发） | ACP runtime 实质上跑在 cue-shell 里 |
| L2765–L2806 | `agent_info_from_entry` / `OkPayload::AgentInfo` 响应构造 | 协议层 agent 投影 |
| L3343, L3399–L3553, L3851–L3938 | `:ask` / `:spawn` / `:agents` / planner/executor escalation 处理分支 | 命令策略 |

### 2.10 `crates/cued/src/config.rs`

| 行号 | 项 | 泄漏判断 |
| --- | --- | --- |
| L15, L111 | `pub agent: AgentConfig` 进顶层配置 | daemon-level config 把 agent 当一等概念 |
| L117–L142 | `struct AgentConfig { transport, default_backend, backends }` + `fill_defaults` | agent backend registry 在 cued 自己维护 |
| L164 | `enum AgentTransport { Weft, Legacy }` | 直接编码 weft 桥过渡状态 |
| L185–L212 | `struct AgentBackendConfig { command, args, model }` + `default_backends` 默认装入 `copilot --acp --stdio` | **cue-shell 二进制内置 Copilot ACP 默认值** —— 与 SPARK §「不为特定上层项目加专用 builtin」直接冲突 |
| L237–L297, L380–L392 | tests 钉死了 `command = "copilot"`, `args = ["--acp", "--stdio"]`, `AgentTransport::Weft/Legacy` | 测试也固化此默认 |

### 2.11 `crates/cued/src/weft.rs`

整文件是 weft 兼容桥客户端，定位明确，但仍出现 agent 字段：

| 行号 | 项 | 泄漏判断 |
| --- | --- | --- |
| L10–L15 | doc 注释：用 `/discover` 作为 capability `probe`、`/sessions/prepare` 转发 `:ask` / `:spawn` | 已自陈是 transitional |
| L89, L110 | `SessionPrepareRequest.agent: String` / `PreparedSessionRequest.agent: String` | 兼容桥 wire 字段 |
| L300, L309, L327 | tests 中 `"agent":"copilot"` 字面量 | 同上 |

### 2.12 `crates/cued/src/parser/*`

| 文件:行号 | 项 | 泄漏判断 |
| --- | --- | --- |
| `token.rs` L68, L106 | `IdKind::Agent` / 显示前缀 `"A"` | parser-level agent id |
| `tokenizer.rs` L335 | `b'A'` 触发 `IdKind::Agent` 词法 | 同上 |
| `ast.rs` L30 | `Argument::Text` doc：`(for ":ask", ":confirm")` | 文档注释泄漏 |
| `ast.rs` L39 | doc：`":jobs", ":agents", ":help"` | 同上 |
| `parse.rs` L286 | `"ask" \| "confirm" \| "escalate" \| "spawn" \| "send" \| "probe"` 共享 text-arg 分支 | 命令名表里直接列 agent 命令 |
| `parse.rs` L292–L297 | 各 missing-arg 错误信息：`":ask requires a prompt"`, `":probe requires a query"`, `":escalate requires a message"`, `":spawn requires a prompt"` | 用户可见报错 |
| `parse.rs` L332 | `"agents"` 列入空-arg 命令组 | `:agents` 命令仍是 first-class |
| `parse.rs` L501 | `["agents", "crons", "scopes", "ask", "spawn", "confirm", "escalate", "probe", "cron", "env", ...]` 命令名集合 | 命令注册表 |
| `parse.rs` L648–L687 | `parse_ask` / `parse_probe` 测试 | 测试钉死命令 |
| `resolver.rs` L26 | `ResolvedCommand::Ask` | A 类核心 |
| `resolver.rs` L34 | `ResolvedCommand::Spawn { text, params }` | A 类核心 |
| `resolver.rs` L58–L59 | `ResolvedCommand::Probe { query }`（注释："planner light query"） | A 类核心 |
| `resolver.rs` L65 | `ResolvedCommand::Agents` | A 类核心 |
| `resolver.rs` L73 | `ResolvedCommand::Escalate { text }`（注释："from executor"） | A 类核心 |
| `resolver.rs` L118, L156, L176, L220, L230, L236 | bare-input 在 `Mode::Agent` 时拼 `:ask`；`ask` / `spawn` / `probe` / `agents` / `escalate` 解析 | mode×command 联动 |
| `resolver.rs` L667 | bare-input 在 `Mode::Agent` 走 "job" 分支（已为兼容） | 残留 mode 分支 |
| `resolver.rs` L706–L886 | `resolve_bare_agent` / `resolve_ask` / `resolve_probe` 等测试 | 测试钉死语义 |

### 2.13 `crates/cue-tui/src/app.rs` & components

| 行号 | 项 | 泄漏判断 |
| --- | --- | --- |
| `app.rs` L18 | `use cue_core::agent::AgentStatus` | 客户端依赖 agent 模块 |
| `app.rs` L21 | imports `AgentInfo` | 同上 |
| `app.rs` L119–L122 | `struct AgentRow { id, label, status: AgentStatus }` | TUI agent registry |
| `app.rs` L142–L147 | `struct AgentSession { status, transcript, ... }` | conversation 状态 in TUI |
| `app.rs` L153, L192, L312–L319 | `FgSessionKind::Agent`, `DisplayTarget::AgentSession`, `FgSession::session(agent_id)` | 前台视图区分 agent vs job |
| `app.rs` L357, L362, L396, L400 | `agents: Vec<AgentRow>` / `agent_sessions: HashMap<...>` | 全局 state |
| `app.rs` L449–L633 | `fg_is_agent` / `fg_agent_content` / `fg_agent_status` / `fg_agent_footer_text` / `render_agent_session_content` | UI 行为按 agent 分叉 |
| `app.rs` L766 | `Mode::Agent =>` 处理路径 | mode 分支 |
| `app.rs` L781 | `agents: self.agents.len() as u32` 进 overview | 状态栏 metric |
| `app.rs` L821–L860 | `ensure_agent_session` / `render_agent_session_content` | conversation 渲染 |
| `component/sidebar.rs` L34 | `pub agents: u32` overview 字段 | 同上 |
| `component/sidebar.rs` L84, L92 | `Mode::Agent => " Jobs "` / `"No jobs yet."` | mode 分支已被空转 |
| `component/main_view.rs` L254, L339；`status_bar.rs` L60, L189；`input_line.rs` L368 | `Mode::Agent` 分支 | 同上 |

### 2.14 `docs/design/ipc-protocol.md`

| 行号 | 项 | 泄漏判断 |
| --- | --- | --- |
| L86–L88 | channel `"agents"` 与 `"output:A2"` 示例 | 文档绑定 |
| L114, L122–L131 | `Mode::Agent` 注释、`AgentPrompt`/`AgentCancel` schema | 同 §2.1 |
| L160, L165, L167, L226–L228 | `AgentSpawned` / `AgentInfo` / `AgentList` / `AgentStateChanged` / `AgentMessage` | 同 §2.1 |
| L181 | `FgAttached { id }` 说明 `A<n> = foreground session view opened` | fg 协议感知 agent kind |
| L246 | `FgExited { id, reason }`（"jobs only"）注释 | 暴露 agent 例外 |
| L319, L324 | error code `NOT_SUPPORTED`：`bridge feature unavailable`；`PERMISSION_DENIED`：`Bridge-only command from a non-bridge client` | 协议错误码语义里嵌 bridge 概念 |

### 2.15 `docs/design/commands-and-modes.md` / `docs/design/core-types.md`

包含 AGENT mode、`:ask` / `:spawn` / `:agents` 段落、planner/executor 描述、`AgentId` 注释。这些是 **文档级镜像**，但既然 SPARK 已收敛，需要在协议冻结时同步重写——本审计不展开行号，只标注 `commands-and-modes.md` §「当前 AGENT bridge 配置落地」整段为冲突区。

## 3. 分类

> 命名约定：`A` = 必删；`B` = 改名/重整（机制保留）；`C` = 兼容桥（等 weft northbound v1）。

### 3.1 A — 必须删除（与 SPARK 非目标直接冲突）

A 项 = **17 条**

1. `cue-core/src/agent.rs` 整模块（含 `AgentStatus`、`AgentKind`、`AgentRole::{Planner,Executor}`、`AgentKind::Api{model}`、`supports_fg`）。
2. `cue-core/src/lib.rs` L6 `pub mod agent;`、L17 `AgentId` re-export。
3. `cue-core/src/mode.rs` `Mode::Agent`（连同 `default_command(Mode::Agent) => "ask"`、`indicator`/`Display` 折叠分支）。
4. `cue-core/src/id.rs` L10–L11、L29、L42–L46（`AgentId`、`EntityRef::Agent`）。
5. `cue-core/src/ipc.rs` `AgentInfo.transcript` / `AgentInfo.role` / `AgentInfo.last_role` / `AgentInfo.backend` 字段（policy 级，禁止留在 process substrate 协议里）。
6. `cued/src/config.rs` `default_backends()` 默认填入 `copilot --acp --stdio`（cue-shell 不内置任何上层产品默认值；改成空 map + 缺省时报错）。
7. `cued/src/config.rs` `AgentBackendConfig.model: Option<String>` —— `model` 是 LLM 概念。
8. `cued/src/actor/scheduler.rs` `parse_agent_role`、`AgentRole` 引入（planner/executor 解析）。
9. `cued/src/actor/scheduler.rs` 全部 `acp_*` / `write_acp_prompt` / `extract_acp_text_block(s)` / `forward_acp_session_update` / `AcpPhase` / `launch_agent`（ACP backend lifecycle 所在地）。
10. `cued/src/actor/scheduler.rs` `AgentEntry.{transcript, role, last_role, model}` 字段、`append_agent_transcript`。
11. `cued/src/actor/mod.rs` `SchedulerMsg::AgentSessionBound`（"ACP session ID" 概念）。
12. `cued/src/parser/resolver.rs` `ResolvedCommand::{Ask, Spawn, Probe, Agents, Escalate, Confirm}` 六个变体。
13. `cued/src/parser/parse.rs` L286（`ask|confirm|escalate|spawn|probe` 文本-arg 分支）、L501 命令注册表里的 agent 命令、L332 `"agents"`。
14. `cue-tui/src/app.rs` `AgentRow` / `AgentSession` / `FgSessionKind::Agent` / `DisplayTarget::AgentSession` / 所有 `fg_agent_*` / `render_agent_session_content` / `ensure_agent_session` / `agent_sessions` / `agents`-overview。
15. `cue-tui/src/component/sidebar.rs` `pub agents: u32` overview 字段及对应 UI 路径。
16. 各 component 内 `Mode::Agent` 分支（删除变体后自然消除）。
17. `docs/design/ipc-protocol.md` 里 `AgentPrompt` / `AgentCancel` / `AgentSpawned` / `AgentInfo` / `AgentList` / `AgentStateChanged` / `AgentMessage` / channel `"agents"` / `Mode::Agent` 全部段落；`commands-and-modes.md` 「当前 AGENT bridge 配置落地」整段。

> 注：A 项实际下刀仍需等 weft northbound v1 提供等价表面后再砍，不然 TUI 失去 `:ask` 入口；但**协议层**（cue-core/ipc.rs）应在那之前先做 deprecation 标注（见 §5）。

### 3.2 B — 改名 / 重整（机制保留，命名含 agent 语义）

B 项 = **6 条**

1. `cue-core/src/ipc.rs` 错误码 `PERMISSION_DENIED` 文案 `"Bridge-only command from a non-bridge client"` —— 机制是 *capability gating*，应改成中性表述（或直接由 weft 自己回错，cue-shell 不知道 bridge 概念）。
2. `cue-core/src/ipc.rs` `OkPayload::Output { id }` 与 `EventPayload::OutputChunk { id }` —— `id` 当前同时承载 `J<n>` / `A<n>`；应收敛到「Job-only」并把 agent session 的 stdout 通道交给 weft。机制（output ring）保留，类型层面去 agent。
3. `cue-core/src/ipc.rs` channel name `"output:<id>"`：保留机制，约束 `<id>` 只能是 `J<n>` / `CH<n>`。
4. `cued/src/actor/mod.rs` `SchedulerMsg::AgentMessage { role, content }` —— 若 `passthrough event` 通道（SPARK open question）最终保留，应改为通用 `BridgeChunk { session_ref, payload: serde_json::Value }`；scheduler 不解读 role。
5. `cued/src/actor/mod.rs` `SchedulerMsg::AgentStateChanged` —— 同上；如果保留通用 lifecycle，重命名为 `BridgeSessionStateChanged { session_ref, status_opaque: String }`。
6. `cued/src/parser/token.rs` / `tokenizer.rs` `IdKind::Agent` / `'A'` 前缀 —— 只要不再有 `A<n>` 实体，词法整段删除即可（属 A 类）；若过渡期仍保留，建议先把它显示标注为 `IdKind::BridgeSession` 仅承担 wire-format 兼容。

### 3.3 C — 兼容桥（等 weft northbound v1 摘除）

C 项 = **8 条**，每条标注它需要 weft 提供什么

| # | 残留点 | 依赖的 weft northbound endpoint |
| --- | --- | --- |
| C1 | `RequestPayload::AgentPrompt { id, prompt }`（`cue-core/src/ipc.rs` L59–L62）+ gateway 路由（`gateway.rs` L446–L455） | `POST /sessions/{id}/prompt`（follow-up turn） |
| C2 | `RequestPayload::AgentCancel { id }`（`cue-core/src/ipc.rs` L63–L65）+ gateway 路由（`gateway.rs` L457–L466） | `POST /sessions/{id}/cancel` |
| C3 | `OkPayload::AgentSpawned { agent_id }`（`ipc.rs` L106–L108） | `POST /sessions/prepare` 已存在；返回 wire id 可由 cue-shell 直接透传 |
| C4 | `OkPayload::AgentInfo` / `AgentList`（`ipc.rs` L120–L121）+ scheduler `agent_info_from_entry`（`scheduler.rs` L2765） | `GET /sessions` / `GET /sessions/{id}` |
| C5 | `EventPayload::AgentStateChanged` / `AgentMessage`（`ipc.rs` L188–L197）+ scheduler 转发（`scheduler.rs` L313, L333） | weft SSE / streaming 事件订阅 |
| C6 | channel `"agents"`（`scheduler.rs` L318, L338） | 客户端直接订阅 weft event stream，cue-shell 不再代订阅 |
| C7 | `cued/src/weft.rs` `WeftClient` + `SessionPrepareRequest` —— 这是当前桥本体 | weft 把 `/discover` + `/sessions/*` 升 v1 后，cue-shell 把整个 weft client 拆出去 |
| C8 | `cued/src/config.rs` `AgentTransport::{Weft, Legacy}` + `[agent.backends.*]` 配置块 | weft 接管 backend 选择后，cue-shell 配置只剩 `[weft] socket_path`（甚至该字段也走客户端发现） |

## 4. 冻结后的目标 schema 建议

> 「看不出 agent 概念」之后，cue-shell 协议表面收敛到下面三个最小集合。

### 4.1 RequestPayload（client → cued）

```rust
enum RequestPayload {
    // 唯一用户命令入口
    Eval        { input: String, mode: Mode },
    // 订阅
    Subscribe   { channels: Vec<String> },
    Unsubscribe { channels: Vec<String> },
    // 前台 PTY 代理（仅 J<n>）
    FgAttach    { id: String /* J<n> | CH<n>:<idx> */ },
    FgDetach    {},
    FgInput     { data: Vec<u8> },
    FgResize    { cols: u16, rows: u16 },
    // 编辑器服务
    Complete    { input: String, cursor: usize, mode: Mode },
    Highlight   { input: String },
    // 系统
    Ping        {},
    Shutdown    {},
}

enum Mode { Job, Cron }   // Agent 删除
```

### 4.2 OkPayload（cued → client）

```rust
enum OkPayload {
    Ack {},
    JobCreated   { job_id, start_scope, open_hint, chain_id, chain_index, chain_total },
    ChainCreated { chain_id, job_ids, chain: ChainInfo },
    CronAdded    { cron_id },
    ScopeCreated { hash, label, summary },

    JobInfo(JobInfo),
    JobList(Vec<JobInfo>),
    CronList(Vec<CronInfo>),
    ScopeInfo(ScopeInfo),
    ScopeList(Vec<ScopeInfo>),

    Output       { id: String /* J<n> only */, data: String, truncated: bool },
    EvalText     { text: String },
    CompletionList { items: Vec<CompletionItem> },
    HighlightResult { spans: Vec<HighlightSpan> },

    FgAttached   { id: String },
    Pong {},
}
```

删除：`AgentSpawned`、`AgentInfo(...)`、`AgentList(...)`、`ConfirmRequest`（confirm 是 agent 策略，迁出）。

### 4.3 EventPayload（cued → client，pushed）

```rust
enum EventPayload {
    // jobs channel
    JobCreated      { job_id, pipeline, start_scope, open_hint, chain_id, chain_index, chain_total },
    JobStateChanged { job_id, old_state, new_state, end_scope, chain_id, chain_index },
    JobRemoved      { job_id },
    ChainStarted    { chain: ChainInfo },
    ChainProgress   { chain: ChainInfo },
    ChainFinished   { chain_id, success },

    // crons channel
    CronTriggered { cron_id, job_id },
    CronRemoved   { cron_id },

    // output:<J<n>|CH<n>:<idx>> channel
    OutputChunk        { id, stream, data },
    OutputChunkBinary  { id, stream, base64 },
    OutputEof          { id },

    // scopes channel
    ScopeCreated { hash, label },
    HeadChanged  { old_hash, new_hash },

    // fg（仅推给 fg-attached client）
    FgOutput { data: Vec<u8> },
    FgExited { id, reason },

    // system channel
    ShuttingDown { reason },
    DaemonReady  {},
}
```

删除：`AgentStateChanged`、`AgentMessage`。

### 4.4 通道集合

`["jobs", "crons", "output:<id>", "scopes", "system"]`，删除 `"agents"`。

### 4.5 EntityRef / Id 命名空间

```rust
enum EntityRef { Job(JobId), Cron(CronId), Scope(ScopeHash), Chain(ChainId) }
```

`AgentId` 删除；前缀 `A` 在词法层不再保留。

### 4.6 错误码

`NOT_FOUND` / `INVALID_STATE` / `INVALID_SCOPE` / `INVALID_SYNTAX` / `ALREADY_EXISTS` / `NOT_SUPPORTED` / `INTERNAL`。删除 `PERMISSION_DENIED`（"bridge-only" 概念外迁）。

## 5. 兼容期策略

### 5.1 协议版本号

cue-shell IPC 当前没有显式版本号——这是 *破坏性重整* 的最小先决条件。建议：

1. 在 `Message` 枚举旁边加 `enum HelloPayload { ProtocolVersion(u32) }`，作为连接握手第一帧（或 `Ping` 响应里附带）。
2. 当前线上协议视作 `v1`；本审计目标 schema 定为 `v2`。
3. cued 同时支持 v1 / v2；客户端协商最高版本。

### 5.2 字段保留 deprecated 的最小集合

下列字段**只能**在 `v1` 兼容头底下出现，必须打 `#[deprecated(note = "moved to weft northbound v1")]`：

- `cue-core/src/ipc.rs`：`RequestPayload::AgentPrompt`、`AgentCancel`；`OkPayload::AgentSpawned`、`AgentInfo`、`AgentList`、`ConfirmRequest`；`EventPayload::AgentStateChanged`、`AgentMessage`；`AgentInfo` struct。
- `cue-core/src/mode.rs`：`Mode::Agent`（serde alias 到 `Mode::Job`，反序列化兼容）。
- `cue-core/src/id.rs`：`AgentId`、`EntityRef::Agent`。
- `cued/src/config.rs`：`AgentTransport::Legacy`（直接拒绝），`AgentBackendConfig.model` 字段忽略并 warn。

### 5.3 v1 → v2 切换条件（gating checklist）

只有当下述 weft northbound 端点齐全且稳定，cue-shell 才发布 v2：

1. `POST /sessions/prepare` ✅（已存在）
2. `POST /sessions/{id}/prompt`（C1）
3. `POST /sessions/{id}/cancel`（C2）
4. `GET  /sessions` / `GET /sessions/{id}`（C3, C4）
5. weft event stream（SSE 或 long-poll）支持 `state_changed` / `message_chunk` 两类语义事件（C5, C6）

完成后：

- cue-shell 在 v2 删除 §3.1 所列 A 项 + §3.2 所列 B 项的旧名称；C 项整体由 weft 取代，cue-shell 仅保留 *进程层 spawn weft 子进程* 的能力。
- `cued/src/weft.rs` 整文件移除（或迁到 `weft` repo 自带的客户端 crate）。

### 5.4 Bridge 期"看不出 agent"的折中

如果 v2 切换之前，希望让 *cue-shell 源码读起来* 已经看不到 agent，可分两段做：

- **第一段（不破协议）**：
  - 在 `cue-core/src/ipc.rs` 上把 `Agent*` 变体加 `#[serde(rename = "...")]` 让 wire 名继续兼容，同时把 Rust 名改成中性（如 `BridgeSessionPrompt`）。
  - 把 `Mode::Agent` serde alias 到 `Job`，Rust 端改名 `Mode::BridgeCompat`（或直接删，让 client 自行映射）。
  - `cued/src/actor/scheduler.rs` 里的 ACP 主循环整体抽到 `cued/src/bridge/acp.rs` 子模块，scheduler 仅依赖一个 `BridgeSessionHandle` trait。
- **第二段（破协议，v2）**：按 §5.3 删除。

## 6. Open questions

- 「passthrough event」通道究竟是否保留——SPARK §「开放问题」原本就在问这个；本审计倾向于「彻底不留」，让 weft 客户端自己订阅 weft 自己的事件流，cue-shell 不再做 fan-out。<!-- 待确认 -->
- `OkPayload::ConfirmRequest` 是否属于 process substrate 策略？目前唯一调用点在 agent confirm 流程；若没有 cron / job 用例，应一同删除。<!-- 待确认 -->
- `EntityRef` 是否仍需要在协议层存在？`:fg`/`:kill`/`:out` 的目标只剩 `J<n>` / `CH<n>` / `S@<hex>`，可以收敛成两个独立类型，避免再被未来某个新 prefix 推动扩张成 `EntityRef::*`。<!-- 待确认 -->
- `Mode` 是否该完全离开协议层？mode 是 TUI 概念，cued 实际上只在 `Eval { mode }` 里用它做 *bare input → default command* 的折叠；可以把折叠提前到 TUI 侧，让 cued 协议只看见已展开的命令字符串。<!-- 待确认 -->
- 错误码 `NOT_SUPPORTED` 在 v2 删除 bridge feature 后是否还需要——若所有协议命令在 cue-shell 里都有实现，`NOT_SUPPORTED` 就只服务于「能力发现」而不是「桥未对接」。<!-- 待确认 -->
- 远程（SSH gateway）路径下 v1 / v2 协商如何穿过——`cued gateway --stdio` 当前是字节透传，是否需要在 gateway 层做版本握手缓冲？<!-- 待确认 -->
- `AgentSessionBound` 携带的 `session_id` 是否在过渡期仍要进 cue-shell 协议（哪怕只是为 TUI 显示）——还是 TUI 直接从 weft 侧取这个 id？<!-- 待确认 -->

---

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
