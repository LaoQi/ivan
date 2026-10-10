# 复用 tanya：清单、探针与配合改动

> 状态：规划。tanya 侧现状以 `../tanya/AGENTS.md` 为准；本文只记 ivan 的复用口径。

## 1 复用矩阵

| tanya 包 | ivan 用途 | 依赖纪律 |
|---|---|---|
| `agent` | LLM 双协议、回合循环、工具注册、会话存储、交接、统计、图像 | ivan 核心直接依赖 |
| `tools`（含 `shell`/`image`/`builtin`） | 标准工具集 `run_shell`/`read_image`/内建 | 经 `tools.Standard` 装配 |
| `tools/shell` 的 `CommandNotifier` | 出站通知（`notify_cmd` 模板） | 后置，按需 |
| `ctty` | 零依赖原语：`IsRoot`（拒绝 root）、`EnsureUTF8`、信号面 | 只取原语，不用终端所有权 |
| `render/markdown` | 出站消息的 markdown → 纯文本/ANSI 渲染 | 后置，按需 |
| `config` | 参考其加载与校验口径 | **不直接复用**（见 §3） |

**明确不复用**：`repl`、`readline`、`ctty` 的 `Console`/`Lease`（终端唯一持有者与借出语义）。理由：ivan 常驻且多数时候无 TTY，独占终端的所有权设计会反向绑架架构；终端交互体验不在本期目标内。

## 2 复用到的具体 API

- 构造：`agent.New(cfg *agent.Config, agent.WithSystemPrompt(s), agent.WithTools(...), agent.NoSave(bool))`
- 回合：`Ask(ctx, input, sink)`、`Continue(ctx, sink)`
- 交接：`RequestHandoff(summary, cont)`、`TakeHandoff()` → `Handoff`
- 会话：`NewSession()`、`LoadSession(id)`、`ListSessions()`、`SessionID()`、`SessionFile()`、`Fork()`、`ArchiveSessions(opt)`
- 工作区：`Workspace()`、`SwitchWorkspace(dir)`
- 观测：`Stats()`、`Model()`、`SetModel()`、`ReasoningEffort()`、`SetReasoningEffort()`
- 事件：`agent.Event` / `EventKind`（`EventRequestStart`/`EventReasoning`/`EventContent`/`EventToolStart`/`EventToolEnd`/`EventResponse`/`EventReasoningEnd`/`EventTurnEnd`）+ `agent.Sinks(...)`
- 工具：`agent.Tool`、`agent.NewTool(name, desc, params, fn)`、`agent.ToolResult{Text, Meta, Images}`
- 内建（免费获得）：`next_session`（会话交接）、`agent_custom`（模型运行时自调模型/思考等级）
- 工具装配：`tools.Standard(tools.Options{ShellOverride, Home, Workspace: func() string, Console: nil, ImageMaxBytes, ImageResize, ImageDetail})`

`Console: nil` 时 `tools/shell` 落 `nullConsole`：`LendStdin` 给空租约，`LendFull` 返回 `ErrNoLend`。即 `interactive: true` 的命令在 ivan 中**明确报错而非挂死**，这是无人值守下需要的语义。

## 3 需 tanya 侧配合的改动

按"能不改就不改、要改就小步"处理；每条改动同时更新 tanya 的 `CHANGELOG.md`。

| # | 改动 | 理由 | 影响面 |
|---|---|---|---|
| T1 | `agent.UserAgentProduct` 常量改为可注入（或 ivan 显式配 `user_agent`） | 否则 ivan 出站请求自称 `tanya/<ver>` | `agent`，小 |
| T2 | `config.Load` 泛化：路径与 env 前缀参数化（键名与 `TANYA_*` 行为逐字不变） | ivan 需要 `~/.config/ivan/config.yaml` + `IVAN_*` | `config`，中 |
| T3 | ~~会话按 session key 归置~~ | **P0 结论：不改 tanya**——用"每个会话键一个独立工作区目录"实现隔离（探针 R11），`LoadSession`/`SessionFile` 足够 | 无 |

T1/T2 可等 P1 用到时再做；T3 只有 P0 探针证明现有 `LoadSession` 不足以支撑时才提。

## 4 P0 探针清单

目的：在写业务代码之前，用最小程序验证复用假设，避免架构返工。结论写进 `docs/reuse-probe.md`。

| # | 假设 | 做法 | 判据 |
|---|---|---|---|
| R1 | 无 TTY 下 `tools.Standard(Console: nil)` 可正常执行 shell | 起一个临时程序，`Ask` 让模型跑 `echo`/`pwd` | 回合成功，输出正确 |
| R2 | `interactive: true` 在无终端时不挂死 | 让模型跑 `run_shell(interactive: true)` | 得到 `ErrNoLend` 类错误，进程不阻塞 |
| R3 | 交接可驱动自动续跑 | 让模型调用 `next_session`；回合后 `TakeHandoff()` → `Continue()` | 新会话文件生成，续跑完成，`SessionID()` 变化 |
| R4 | 会话可恢复 | 记录 `SessionID()`，新建 `Agent` + `LoadSession(id)` | history 完整，能继续对话 |
| R5 | 多实例并发安全 | 同 workspace 起两个 `Agent` 实例并发 `Ask` | 会话文件互不覆盖，无数据竞争（`-race`） |
| R6 | 事件面足够驱动通道 | 收集全部事件 → 落 jsonl 并聚合文本 | 能还原出完整回复文本与工具轨迹 |
| R7 | cwd/工作区语义 | 进程 cwd 与 workspace 不同目录时跑 `run_shell` | 命令在 workspace 下执行 |
| R8 | 上下文用量可读 | 读 `EventResponse`/`Stats()` | 能拿到用于自动交接的阈值信号 |
| R9 | 同 workspace 多实例互扰可控 | 两实例并发跑 + 其间 `ListSessions()` | 会话文件互不覆盖；列出结果含双方会话（用于文档说明互扰范围） |
| R10 | per-turn 上下文注入可行 | 每实例一套工具实例 + 回合前写状态 | 工具能读到当轮通道/会话；并发下不串号 |

## 5 注意点

- **缓存不变性**：动态内容（记忆、目标、状态）一律经 user 消息注入，不改 system prompt 快照——沿用 tanya 的口径，否则 prompt cache 全废。
- **事件 sink 不得阻塞**：`agent` 的事件是同步串行投递，ivan 侧必须用缓冲 channel + 独立消费 goroutine，慢消费者自行缓冲。
- **工具装配期冻结**：工具在 `agent.New` 时一次合成，运行期不加减；ivan 的 `schedule` 等工具在装配期就位。
- **一个 `Agent` 实例 = 一个会话文件**：实例内部管理会话（含交接时的 rotate），ivan 负责"会话键 → 实例 → 当前 session id"的映射与恢复。
- **交接是"借机制"，不是"复用语义"**：tanya 的 `next_session` 定位为模型显式行为、连续交接不设上限（给人看着的转段）。ivan 的自动续跑必须自建护栏：轮次上限、token/时长预算、默认关、仅指定工作区启用。
- **root 拒绝**：tanya 在入口拒绝 root；ivan 同样拒绝（复用 `ctty.IsRoot()`），逃生舱口径保持一致。
