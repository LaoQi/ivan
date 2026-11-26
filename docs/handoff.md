# 上下文交接：建议档提醒（钩子）与强制档复位（`ForceHandoff`）

> 归属：`ivan/runner`（判定、文案、闸门）。状态：**已实施**（2026-11-26，P4 迁移）。
> 相邻文档：battlelab `docs/agent-hooks.md`（钩子切点与契约）、`docs/force-handoff.md`（强制复位语义）、`docs/session-handoff.md`（`next_session` 语义）；本仓 `docs/sessions.md` §4。

## 1 定位

上下文接近窗口时 ivan 做两件事，**两档互不替代**：

| 档 | 阈值 | 谁动手 | 时机 | 代价 |
|---|---|---|---|---|
| 建议档 | `handoff_hint_tokens`（默认 180000） | 模型（`next_session`） | 回合内、下一次请求发出前 | 零额外请求 |
| 强制档 | `handoff_force_tokens`（默认 256000） | **runner**（`agent.ForceHandoff`） | 回合间隙 | 一次摘要请求 |

两档判据同源：`contextUsed(stats) = max(上报 prompt tokens, 本地估算 Est)`。旧实现里建议档用 `EventResponse.ContextTokens`、强制档再回退 `Stats()`，两套口径容易打架，本次统一。

**"一次推进"是作用域，不是抽象层**：这里的推进指**一条入站消息触发的 `DispatchTurn` 过程**（`Ask` + 若干 `Continue`/交接续跑）。ivan 没有任务实体（无 id、无存储、不进 `Status`），下文说"一次推进"就指这个函数作用域。

## 2 建议档：`BeforeRequest` 钩子

`runner/handoffhint.go` 的 `handoffHint` 实现 `agent.BeforeRequestHook` + `agent.SessionHook`，**每个会话实例一个**（`acquire` 造，`buildAgent` 经 `agent.WithHooks` 注册）。

| 切点 | 做什么 |
|---|---|
| `BeforeRequest` | 判阈值 → `AppendNotice(建议交接)` → 本回合这次请求就带上它 |
| `SessionEvent` | 任一会话边界（新建/载入/轮换/交接）清 `fired`，新会话重新计 |

- **armed / fired**：runner 在推进开始 `arm()`、结束 `disarm()`（`task.run` 的 `defer`），同一实例锁保证同键串行，无并发问题；`fired` 保证一次推进只注入一条。会话边界只清 `fired`，不动 `armed`。
- **取实例**：`agent.FromContext(ctx)`——钩子边界由库注入，不靠闭包捕获；取不到（库外直接 `Ask`、探针）就不注入。
- **暂停闸门**：`paused` 只挡无人值守路径，故注入前判 `paused()`，命中则跳过并记一条 `paused` 事件（一次推进只记一次）。
- **注入位置**：在 user/工具消息之后、构造请求之前（append-only，system 快照与历史前缀不动，prompt cache 不受影响）。

## 3 强制档：回合间隙 `ForceHandoff`

`runner/handoffforce.go`。回合返回、`TakeHandoff` 未命中之后按顺序判定：

```
used = contextUsed(ag.Stats())
used < handoff_force_tokens            → 正常结束（建议档已由钩子处理，不再多发一轮）
刚复位过（justReset）                    → handoff_forced 诊断并停
paused                                 → paused 事件并停
stopForBudget(turn)                    → budget 事件并停
ag.ForceHandoff(ctx, {Instruction, Continue: auto_continue})
  ├─ 失败 → handoff_forced（带错误原文）并停；会话与文件零变化
  └─ 成功 → handoffs+1 / forced+1 / 写回映射 / handoff_force 事件
             → auto_continue 开则续跑一轮（justReset = true），否则停
```

- **摘要指令由 ivan 给**（`forceHandoffInstruction`），库零文案：整理「已完成与关键结论 / 改动过的文件与路径 / 未完成事项与下一步」，只输出摘要正文、不调工具。
- **不做次数配额**：`justReset` 是局部状态——复位成功后续跑，若下一次判定**仍**超阈值，说明摘要基线本身就超阈值（新会话没有救回来），此时停下并给诊断（"刚强制交接完，新会话基线即超阈值，请调高 `handoff_*_tokens` 或拆分这次推进"），不再来第二次。反复"复位→长回来→复位"由既有的轮次/任务/日预算兜住，不新增限制层。
- **模型自驱动交接不清 `justReset` 的反面**：模型 `next_session` 落地后 `justReset = false`（新会话重新计），两档都按新基线从头判。
- `ForceHandoff` 自带护栏：回合中拒绝、无历史拒绝、指令必填；摘要为空或超长即失败。

## 4 事件与计数

| kind | 何时 | 备注 |
|---|---|---|
| `handoff_hint` | 钩子注入建议提示（`hinted` +1） | 一次推进至多一条 |
| `handoff` | 模型 `next_session` 落地（`handoffs` +1） | 不变 |
| `handoff_force` | 外部强制复位成功（`handoffs` +1、`forced` +1） | 带 `handoff=true`，正文含 `old → new` |
| `handoff_forced` | 强制复位失败，或刚复位过仍超阈值而停（`Failed` 置位表示前者） | 正文含错误原文/诊断 |
| `paused` | 暂停中跳过建议提示或强制交接 | 一次推进各至多一条 |

一次轮转只发**一条**事实事件：模型路径是 `handoff`，强制路径是 `handoff_force`（不再额外补一条 `handoff`，避免同一事实两条日志）。

## 5 预算口径

摘要请求的用量**不计入预算**：库明确不记 `Stats()`（它不属于本会话对话上下文），ivan 也不从别处估算——编个估算值就等于开第二个预算口径（预算只在 runner 一处统计）。它的代价以"一次推进至多一次 + 调用前已过预算闸门"控制。`runner.Status.TokensToday` 因此不含摘要请求的 token。

## 6 相对旧实现的行为变化

1. **建议档不再额外发一轮**：注入落在当回合的请求里，省一次请求；代价是"整回合没有工具轮、模型直接答完"时提示会落到下一次请求（下一条消息的第一次请求）。
2. **工具轮内也能注入**：阈值在工具轮之后才达到时，提示落在该轮之后的请求上。
3. **强制档不再指望模型自愿**：由 runner 直接复位；复位失败即停，不再有"注入强制指令 + 再跑一轮"。
4. 判据统一为 `max(ContextTokens, Est)`（旧实现按 `EventResponse` 优先）。
5. `handoff_force` 的语义从"注入强制指令"变为"强制复位成功"；`handoff_forced` 从"模型没照做而停"变为"复位失败/复位后仍超阈值而停"。

## 7 装配

```go
inst := &instance{...}
inst.hint = r.newHandoffHint(inst)                     // 每会话实例一个
ag, err := agent.New(&cfg.Config,
    agent.WithTools(list...),
    agent.WithHooks(inst.hint),                        // 建议档
    agent.WithGlobalAgentsPath(cfg.GlobalAgentsPath()),
)
```

强制档不需要钩子：`task.run` 在回合间隙直接调 `ag.ForceHandoff`。

## 8 测试

`runner/p4_test.go`（阈值用大数 100000/200000 + 假 LLM 报 `prompt_tokens=150000/250000`，避开 system prompt 估算量）：

| 用例 | 断言 |
|---|---|
| `TestHandoffHintInjectedInsideTurn` | 首轮不带提示、带提示的是工具轮之后的请求（末条消息）、`hinted=1`、`handoffs=1` |
| `TestHandoffHintOncePerDispatch` | 连续两个工具轮只注入一次（第三次请求里提示只出现 1 次） |
| `TestHandoffHintBelowThreshold` | 未达阈只发 1 次请求、`hinted=0` |
| `TestHandoffHintSkippedWhenPaused` | 暂停不注入、不多花请求、`paused` 事件在人类日志里 |
| `TestForceTierResetsSession` | 回合 + 摘要共 2 次请求、摘要请求不带工具且带 ivan 指令、无旧强制文案、会话轮转、`TokensToday` 不含摘要用量 |
| `TestForceTierContinuesAfterReset` | 复位后按 `auto_continue` 续跑（3 次请求、`continued=1`） |
| `TestForceHandoffFailureStops` | 摘要为空 → `handoff_forced`、`forced=0`、会话不轮转（第二条消息仍在同一会话里） |
| `TestHintThenForceEscalation` | 建议档注入后回合结束即超强制阈值 → 强制复位（3 次请求） |
| `TestForceResetOnceThenDiagnostic` | 复位后仍超阈值 → 诊断并停，不再来第二次（3 次请求、`forced=1`） |

既有用例（暂停不续跑、正常续跑、日预算跨进程累计、`CancelTurn`、系统提示含记忆/目标）不变。

## 9 落地记录（2026-11-26）

- 新增 `runner/handoffhint.go`（钩子 + `contextUsed` + 建议档文案）、`runner/handoffforce.go`（强制档复位 + `forceHandoffInstruction` + 两种停止文案）
- `runner/runner.go`：`task.run` 的 `pressure()`/`levelTokens()`/`forcedStop()`/`handoffForceNotice` 全部删除，改为「钩子管建议档、回合间隙管强制档」；`hinted`/`forced` 局部标志改由钩子与 `justReset` 承担
- `runner/pool.go`：`instance.hint`、`acquire` 建钩子、`buildAgent` 挂 `WithHooks`
- `runner/p4_test.go`：8 例改写（见 §8）
- 库侧无改动（`battlelab` 的 `WithHooks`/`FromContext`/`ForceHandoff` 早已就绪）；`go.mod` 的 battlelab 版本仍指 v0.7.0，靠上层工作区 `go.work` 联调，发版后随 P2 一起升
