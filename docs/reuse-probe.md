# P0 复用探针结论（2026-10-10）

> 目的：在写业务代码前验证"ivan 复用 tanya `agent` + `tools`"的关键假设。结论已回灌 `AGENTS.md`、`docs/sessions.md`、`docs/reuse-core.md`。
> 代码：`probe/probe_test.go`（R1–R11）、`internal/fakellm`（本地假 LLM 服务，SSE）。

## 1 方法

- **假 LLM 驱动**：`internal/fakellm` 起一个 OpenAI chat 兼容的 httptest 服务（SSE，支持 content / reasoning / tool_calls / usage），按脚本逐步返回。探针因此**确定性、零成本、可重复**，可进 `go test -race`。
- **真机冒烟**：用 tanya 配置借来的凭证（本机 LLM 网关、`auto-flash`）跑一次 `ivan run`，验证真实网关下工具调用、事件落盘与回合结算。
- 环境：Go 1.27.1，Linux；tanya 经 `go.work` + `replace ../tanya` 引用。

## 2 结果

| # | 假设 | 结果 | 证据 |
|---|---|---|---|
| R1 | 无 TTY 下 `tools.Standard`（`Console: nil`）能执行 shell | ✅ | `echo hello-ivan` 输出进入 tool_end 事件 |
| R2 | `interactive: true` 无终端时快速失败、不挂死 | ✅ | 结果文本 `error: interactive 不支持（无法借出终端）`，20s 内返回 |
| R3 | 交接（`next_session`）可在回合后驱动自动续跑 | ✅ | Ask 占 2 次请求（工具轮 + 收尾轮）→ `TakeHandoff()` 得 `Continue=true`、新旧会话 id 不同 → `Continue()` 第 3 次请求产出新会话输出 |
| R4 | 会话可恢复 | ✅ | 新实例 `LoadSession(id)` 后消息数与文件路径均一致 |
| R5 | 多实例并发（各自工作区）安全 | ✅ | 两实例并发 Ask，会话文件不同、输出互不污染，`-race` 干净 |
| R6 | 事件面足够驱动通道与日志 | ✅ | content 事件可拼接还原文本；`data.EventLog` 落 jsonl 含 `turn_end`，零丢弃 |
| R7 | 工作区语义 | ✅（需修装配） | `SwitchWorkspace` 后 `run_shell` 在新目录执行——**前提是装配层把 `tools.Options.Workspace` 接到 agent 的当前工作区** |
| R8 | 上下文用量可读 | ✅ | `EventResponse.ContextTokens=100`、`Stats().TotalTokens/Est` 均可用 |
| R9 | 工作区隔离可避免互扰 | ✅ | 各自工作区的实例 `ListSessions()` 各见 1 条 |
| R10 | per-turn 上下文注入可行（每实例一套工具实例） | ✅ | 两实例并发调用各自的 `turn_context` 工具，各读到自己的值，不串号 |
| R11 | 共享工作区多实例并发创建会话 | ❌ **会撞** | 同秒创建 → 落同一文件 `…/sessions/YYYYMMDD-HHMMSS.jsonl`（tanya `nextID` 为秒级时间戳），`ListSessions` 只见 1 条 |

## 3 关键发现与设计影响

1. **会话目录必须按会话键隔离**（R11）。tanya 的会话 id 是秒级时间戳，单个 `Agent` 实例语义是"单进程单工作区"。ivan 多会话并发时若共享工作区，会互相覆盖。**决定**：每个 `SessionKey` 一个独立 agent 工作区目录 `<data_dir>/agents/<key-slug>/`，会话落在该目录下（`session_mode: global`）；`run_shell` 的默认目录另由"项目目录"提供（配置项），不与会话目录耦合。已写入 `docs/sessions.md`。
2. **`tools.Options.Workspace` 必须动态跟随 agent**（R7）。它是调用方闭包，`SwitchWorkspace` 不会自动同步；装配层要写成 `func() string { return ag.Workspace() }`（`ag` 在 `agent.New` 后赋值），否则命令永远跑在启动目录。已修正 `runner/runner.go`。
3. **交接续跑的真实请求节奏**（R3）：一轮"工具调用"回合 = 2 次请求（工具轮 + 收尾轮），`Continue` 再 1 次。ivan 的轮次/预算统计要按"请求"而非"回合"计费，否则预算会低估。

## 4 真机冒烟记录

```
$ cd /tmp/ivan-smoke && ivan run "执行 echo ivan-smoke 并把输出原样告诉我"
▸ run_shell {"command": "echo ivan-smoke"}
stdout: ivan-smoke
…
[回合 4.7s · 上下文 1702 · 累计 3394 tokens · 会话 20261010-183421]
```

- 事件日志：`~/.local/share/ivan/events/2026-10-10.jsonl`（content / response / turn_end 齐全，含 usage 与耗时）。
- 第二轮冒烟（无工具）：`[回合 2.0s · 上下文 1630 · 累计 1642 tokens]`。
- 结论：真实网关（`responses` 协议默认）下工具调用与事件链正常；凭证借用（tanya 配置）可用。

## 5 后续待验证（不阻塞 P1）

| 阶段 | 待验证 |
|---|---|
| P1 | ✅ 已验证（2026-10-10）：`state.json` 映射 + `LoadSession` 重启续聊、cli socket 往返、flock 单实例锁；见 `runner/`、`daemon/` 测试与 `docs/sessions.md` §9 |
| P2 | ✅ 已验证（2026-10-21）：假网关端到端 + 生产凭证真机收发（单聊、群 @、图片内联/分片、去重、重连）；见 `docs/qq-bot.md` §13 |
| P3 | ✅ 已验证（2026-10-20）：作业重启恢复、`schedule` 工具结果回灌原会话（含 QQ 真机提醒）；见 `docs/schedule.md` §8 |
| P4 | ✅ 已验证（2026-10-21）：两档阈值交接触发、跨 handoff 预算累计（由 ivan 自行记账，不依赖 tanya `Stats`）；见 `docs/sessions.md` §10 |
| P5 | ⏸ 保留计划（审批档、危险命令规则、限额、日志轮转、systemd、`ivan doctor` 均未实施，见 `docs/plan.md` §8） |
