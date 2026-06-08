# Cue Shell — TUI 设计决策

## 一、架构

| 决策 | 结果 |
|------|------|
| **架构模式** | TEA + Component 混合：全局 `AppState` + `Message` 枚举 + 纯函数 `update`，面板用独立 Widget trait 渲染 |
| **渲染框架** | ratatui 0.30 + crossterm 0.29 |

## 二、布局

### 普通模式

```
┌─────────────────────────────────────────────────────────────┐
│ JOB  J:3 (1 running) C:2  cued:ok  mouse:ui  14:30 ... │ ← Split header
├────────────┬────────────────────────────────────────────────┤
│ Sidebar    │ ┌ stdout J1 × ───────────────────────────────┐ │
│ (mode list)│ │ Compiling cue-core                         │ │
│ 🔄 J1 ...  │ │ Finished dev 0.3s                          │ │
│ ✅ J2 ...  │ └────────────────────────────────────────────┘ │
│ ⏳ J3 ...  │ ┌ Command Log ───────────────────────────────┐ │
│            │ │ J1                                         │ │
│            │ │ status: running                            │ │
│            │ │ start scope: S@...                         │ │
│            │ └────────────────────────────────────────────┘ │
├────────────┴────────────────────────────────────────────────┤
│ [JOB ⚡] > _                                               │ ← Input Line
├─────────────────────────────────────────────────────────────┤
│ JOB: Enter submit • Shift+Enter newline • Tab complete     │ ← Context footer
└─────────────────────────────────────────────────────────────┘
```

### :fg 全屏模式

```
┌─────────────────────────────────────┐
│                                     │
│   (Job pty)                          │
│                                     │
├─────────────────────────────────────┤
│  Job: Ctrl+Z detach                │
└─────────────────────────────────────┘
```

### 布局规则

| 规则 | 值 |
|------|---|
| 侧边栏显示 | 终端 ≥100 列时自动显示，<100 隐藏 |
| 侧边栏切换 | Ctrl+B 手动覆盖 |
| 侧边栏宽度 | 比例 20%~30%，min 20 列 max 40 列 |
| Job :fg 退出键 | Ctrl+Z |
## 三、侧边栏

### 内容跟随模式

| 模式 | 侧边栏内容 |
|------|------------|
| JOB ⚡ | Jobs 列表 |
| CRON ⏰ | Crons 列表 |

全局计数不再放在侧边栏底部，而是提升到顶部 header。

### 列表项格式（单行）

```
🔄 J1 cargo build
✅ J2 cargo test
⏳ J3 sleep 4
```

### 点击行为

- 点击列表项 → 选中高亮
- 选中后智能路由：
  - running 且需要 tty 的 Job → 自动 `:fg`
  - running 的 stream Job → 自动 `:tail`
  - 已结束的 Job → 自动 `:out`
  - CRON → 打开 schedule / command preview
- 列表仍保留隐式滚动与命中映射，但不再额外渲染显式 scrollbar widget

## 四、Main REPL Area

### 上半：Display tabs

- `:out J1`、`:tail J1`、`:err J1` 和显式 inspect preview 都会在上半区域打开一个 tab
- tab 可切换、可关闭
- 输出保留 ANSI 颜色
- 没有打开 display tab 时，上半区域显示占位 help，而不是自动灌入最近 stdout

### 下半：统一命令记录

- 所有执行过的**特命令 / 结构化命令响应**都记录在这里，不再按模式切开
- 每条命令 + 响应 = 一个带边框的卡片（Warp Terminal 风格）
- 无前缀标记，**用颜色区分**：绿色=成功，红色=错误，黄色/青色=进行中
- Job 记录会实时更新状态；`start_scope` 会在创建响应、推送事件和重连后的 jobs snapshot 中立即可见，terminal 状态下会补上 `end_scope`

### 命令输出语义

- `:run cargo build` → 下半记录显示：
  - `J1`
  - `status: running`
  - `start scope: S@...`
- Job 完成时 → 下半记录更新为完成状态：
  - `end scope: S@...`，或
  - `end scope: no side effect (S@...)`
- `:run` / bare JOB 不会自动把 stdout 流进命令卡片；查看输出必须显式 `:out J1` 或 `:tail J1`
- `:out J1` → 上半打开 / 激活 `J1` 的 stdout snapshot tab；下半只记录“opened stdout for J1”
- `:tail J1 4096` → 上半打开 / 激活 `J1` 的 live stdout follow tab；下半只记录“following stdout for J1”
- `:err J1` → 上半打开 / 激活 `J1` 的 stderr snapshot tab
- `:kill J2` → 下半记录显示 kill 请求 / kill 结果
- 错误 → 红色文字
## 五、Input Line

| 特性 | 行为 |
|------|------|
| 默认 | 单行输入 |
| 换行 | Shift+Enter 或 Ctrl+Enter |
| 提交 | Enter |
| 历史 | ↑↓ zsh 风格（单行/首行 ↑ 翻历史，多行非首行 ↑ 移光标） |
| 补全 | Tab |
| 模式切换 | Shift+Tab 在任意非-FG 焦点循环 JOB→CRON |
| 运行中 Job 列表 | Ctrl+C 打开 popup，选择后执行 `:kill J<n>` |
| 退出 TUI | Ctrl+D |
| Prompt 格式 | `[JOB ⚡] > _`（不显示 scope） |

补充：

- focus 不再靠 Tab 轮转，统一改为鼠标点击区域切换
- prompt history 持久化，重启后仍可用上下键取回
- `Ctrl+L` 仅在无 pending request 时清空 display / command log
- `Ctrl+Y` 复制当前前台 / 活动 display tab；若没有 display tab，则回退复制最新 command record

## 六、Split Header + Contextual Footer

### 顶部 Header

左侧是会话状态，右侧是 action pills：

```
JOB  J:3 (1 running)  A:1  C:2  cued:ok  mouse:ui  14:30
                                    [clear] [sidebar ^B] [copy ^Y] [targets ^T] [jobs ^C] [mouse] [quit ^D]
```

- `clear` 只有在当前没有 pending request 时才高亮可用
- `copy` 会把当前前台 / 活动 display 内容写到终端 clipboard（OSC52）
- `targets` 以 toggle 方式打开/关闭 frontend-local 的 target/profile 设置页，列出 `client.toml` 里的 transport profiles；选择后会写回 `transport.default_profile`，可 live reconnect 的目标会提示按 `R` 立即重连，否则在下次 reconnect/restart 时生效
- `mouse` 在 `ui` / `text` 两种模式间切换；`text` 让终端原生选中复制更顺手
- header action pills 支持鼠标点击

### 底部 Footer

footer 不再重复显示计数，而是显示**当前焦点相关的即时提示**：

- Input focus → 当前模式的提交/补全/换行提示
- Sidebar focus → 选择/打开/切换提示
- Main view focus → display tab 或 command log 的操作提示
- target settings 页激活时 → `Up/Down/Home/End` 选 profile、`Enter` 保存默认 target、`Ctrl+R` 从磁盘重载 profile、`Esc` / `Ctrl+T` 关页
- Job picker 打开时 → kill picker 专用提示

## 七、鼠标支持（第一版）

| 操作 | 支持 |
|------|------|
| 点击切换焦点区域 | ✅ |
| 点击 header action pills | ✅ |
| 滚轮滚动 | ✅（命令记录区 + 侧边栏） |
| 点击侧边栏条目选中并打开 | ✅ |
| 点击 display tab 切换 / 关闭 | ✅ |
| 点击命令卡片打开 inspect preview | ✅ |
| 切换 mouse ui/text 模式 | ✅ |
| 拖拽调整面板 | ❌（后续） |
| 双击操作 | ❌（后续） |

## 八、Popup 系统

| 特性 | 设计 |
|------|------|
| 视觉形式 | 居中弹窗 + 清晰边框 |
| Ctrl+C | ✅ 运行中 Job kill picker popup |
| Enter | ✅ 对当前选中 Job 执行 kill |
| Esc / 点击弹窗外 | ✅ 关闭 popup |

## 九、Scope 模型（重新定义）

### 核心概念

- **Scope = 不可变的环境快照**（env + cwd + ...），内容寻址
- **ID = blake3(content) 的 hash**，显示为 `S@a3f1`（短 hash）
- 相同环境 = 相同 hash = 自动去重

### Job 与 Scope 的关系

```
Job 持有两个引用：
  start_scope: ScopeHash  — 执行前的环境
  end_scope: ScopeHash    — 执行后的环境（可能与 start 相同）

规则：
  - start == end → Job 无副作用，合并
  - 只能从已完成的 scope fork（Job 未完成时 end_scope 不存在）
  - `:run cd ...` / `:run env set ...` 生成 **job-local end_scope**，不会自动移动默认 HEAD
  - 顶层 `:cd` / `:env set ...` 修改默认 HEAD，并会持久化到 daemon 存储
  - serial chain 会把前一 leaf 的 `end_scope` 传给下一 leaf
  - parallel / pipeline 中的 scope-transform leaves 当前直接拒绝，避免歧义
  - 未来：多个保存的默认 scope 可切换
```

### 类比

```
Scope ≈ git commit（不可变快照，内容寻址）
Job ≈ git diff（从一个快照到另一个快照的变换）
fork ≈ git branch（从某个快照开始新分支）
默认 scope ≈ HEAD 指针（可移动）
```
