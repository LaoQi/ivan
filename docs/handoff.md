# 上下文交接：回合内辅助提示（钩子）+ 回合末下令 + 强制档复位（`ForceHandoff`）

> 归属：`ivan/runner`（判定、文案、闸门）。状态：**已实施**（2026-11-27，三次调优后的形态）。
> 相邻文档：battlelab `docs/agent-hooks.md`（钩子切点与契约）、`docs/force-handoff.md`（强制复位语义）、`docs/session-handoff.md`（`next_session` 语义）；本仓 `docs/sessions.md` §4/§15。

## 1 定位

上下文接近窗口时 ivan 有三处动作：**一条主线**（回合内提醒，模型自己交接）加**两条兜底**（回合末下令、强制复位）。

| 档 | 阈值 | 触发时机 | 谁动手 | 代价 |
|---|---|---|---|---|
| 回合内辅助提示 | `handoff_hint_tokens`（默认 180000） | 回合内、下一次请求发出前（`BeforeRequest` 钩子） | 模型（`next_session`） | 零额外请求（提示随当回合的请求带出） |
| 回合末下令 | 同上 | 回合结束、仍 ≥ 阈值且本次推进还没交接过 | 模型（`next_session`）：指令进历史后立刻开一回合 | 一回合（这一回合就是交接回合） |
| 强制交接 | `handoff_force_tokens`（默认 256000） | 回合间隙 | runner（`agent.ForceHandoff`） | 一次摘要请求 |

判据统一：`contextUsed(stats) = max(上报 prompt tokens, 本地估算 Est)`。旧实现里建议档看 `EventResponse`、强制档回退 `Stats()` 的两套口径已取消。

**"一次推进"是作用域，不是抽象层**：指一条入站消息触发的 `task.run` 过程（`Ask` + 若干 `Continue`/交接续跑）。ivan 没有任务实体（无 id、无存储、不进 `Status`），下文说"一次推进"就指这个函数作用域。

三处的关系：回合内提示先到（只提醒，模型可先把手上这步做完）→ 回合结束仍达阈值就下令（开一回合专门交接）→ 回合末已超强制阈值则由 runner 直接复位。实测里回合内提示生效时后两处都不触发（§11.5：`directives=0 forced=0`）。

## 2 回合内辅助提示：`BeforeRequest` 钩子

`runner/handoffhint.go` 的 `handoffHint` 实现 `agent.BeforeRequestHook` + `agent.SessionHook`，**每个会话实例一个**（`acquire` 造，`buildAgent` 经 `agent.WithHooks` 注册）。

| 切点 | 做什么 |
|---|---|
| `BeforeRequest` | 判阈值 → `AppendNotice(辅助提示)` → 本回合这次请求就带上它 |
| `SessionEvent` | 任一会话边界（新建/载入/轮换/交接）清 `fired`，新会话重新计 |

- **armed / fired**：runner 在一次推进开始 `arm()`、结束 `disarm()`（`task.run` 的 `defer`）；`fired` 保证一次推进只提示一条（回合末下令也会 `mark()`，见 §3）。会话边界只清 `fired`，不动 `armed`。
- **取实例**：`agent.FromContext(ctx)`——钩子边界由库注入，不靠闭包捕获；取不到（库外直接 `Ask`、探针）就不注入。
- **暂停闸门**：`paused` 只挡无人值守路径，命中则跳过并记一条 `paused` 事件（一次推进只记一次）。
- **注入位置**：在 user/工具消息之后、构造请求之前（append-only，system 快照与历史前缀不动，prompt cache 不受影响）。
- **文案**（建议语气——回合还在进行，模型可先把当前步骤做完）：

  ```
  系统提示：本会话上下文已到 %d tokens（建议交接阈值 %d）。建议在本阶段收尾后调用 next_session 交接进度（summary 写清已完成内容、关键结论与下一步）；若目标马上就能完成，可先把当前步骤做完再交接。
  ```

## 3 回合末下令：`task.run` 出口

回合（`Ask`/`Continue`）已结束、`TakeHandoff` 未命中、且未达强制档时：

```
used = contextUsed(ag.Stats())          // 回合末真实用量，比钩子里的"上一次响应用量"准
used >= handoff_hint_tokens →
  paused?              → paused 事件并停
  stopForBudget(turn)? → 停
  ag.AppendNotice(指令) + hint.mark()   // mark 让本次推进余下请求不再出辅助提示
  → continue 开一回合（这一回合就是交接回合）
```

- **文案**（命令语气——回合已经结束，不存在"先把这步做完"）：

  ```
  系统提示：本会话上下文已到 %d tokens（建议交接阈值 %d）。现在开始交接进度：调用 next_session，summary 写清已完成内容、关键结论与下一步。
  ```
- **一次推进至多一条**：`task.run` 的局部 `hinted` 布尔。不做重复抑制——历史里已有旧指令也照发新的一条。
- 注入成功才 `continue`；写盘失败/暂停/预算命中都直接停，不白开一回合。
- 它与强制档的顺序：强制档优先（回合末已超 `handoff_force_tokens` 就只复位，不再发提示或指令）。

## 4 强制档：回合间隙 `ForceHandoff`

`runner/handoffforce.go`。回合返回、`TakeHandoff` 未命中之后判定：

```
used = contextUsed(ag.Stats())
used < handoff_force_tokens            → 交给建议档（§2/§3）后正常结束
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
- **模型自驱动交接不清 `justReset` 的反面**：模型 `next_session` 落地后 `justReset = false`（新会话重新计），三处都按新基线从头判。
- `ForceHandoff` 自带护栏：回合中拒绝、无历史拒绝、指令必填；摘要为空或超长即失败。

## 5 事件与计数

| kind | 何时 | 计数 |
|---|---|---|
| `handoff_hint` | 回合内注入辅助提示 | `Status.Hinted`（`ivan status` 的 `hints=`） |
| `handoff_directive` | 回合末发出交接指令并开新回合 | `Status.Directives`（`directives=`） |
| `handoff` | 模型 `next_session` 落地 | `Status.Handoffs` |
| `handoff_force` | 强制复位成功（正文含 `old → new`，带 `handoff=true`） | `Status.Handoffs`、`Status.Forced` |
| `handoff_forced` | 强制复位失败（`Failed` 置位），或刚复位过仍超阈值而停 | 无（正文含错误原文/诊断） |
| `paused` | 暂停中跳过辅助提示 / 下令 / 强制复位 | 无 |

三个计数各说一件事：`hints` 提醒了几次、`directives` 下令交接了几次、`forced` runner 自己动手复位了几次。一次推进最多两条提示事件（辅助 + 下令）。

一次轮转只发**一条**事实事件：模型路径是 `handoff`，强制路径是 `handoff_force`（不再额外补一条 `handoff`，避免同一事实两条日志）。

## 6 预算口径

摘要请求的用量**不计入预算**：库明确不记 `Stats()`（它不属于本会话对话上下文），ivan 也不从别处估算——编个估算值就等于开第二个预算口径（预算只在 runner 一处统计）。它的代价以"一次推进至多一次 + 调用前已过预算闸门"控制。`runner.Status.TokensToday` 因此不含摘要请求的 token。

## 7 相对旧实现的行为变化

1. **回合内只提醒、回合末才下令**：回合内提示不催（模型可先把当前步骤做完），回合结束仍超阈值就发命令语气的一条指令并直接开一回合做交接；旧实现两者只有其一。
2. **回合内提示不再白写**：它在当回合请求里，被无视时回合末还有一次机会。
3. **强制档不再指望模型自愿**：runner 直接 `ForceHandoff`，失败即停。
4. 判据统一为 `max(ContextTokens, Est)`。
5. 事件与计数三分（§5），`Status.Hinted` 不再混指"提示过"这一件事。
6. "一次推进至多一条提示"的边界放宽为"辅助 + 下令各至多一条"，辅助被无视不会再压掉回合末下令。

## 8 装配

```go
inst := &instance{...}
inst.hint = r.newHandoffHint(inst)                     // 每会话实例一个
ag, err := agent.New(&cfg.Config,
    agent.WithTools(list...),
    agent.WithHooks(inst.hint),                        // 回合内辅助提示（BeforeRequest + SessionEvent）
    agent.WithGlobalAgentsPath(cfg.GlobalAgentsPath()),
)
```

回合末下令与强制档都不走钩子：`task.run` 出口直接判（钩子只读快照、不参与控制流）。

## 9 测试

`runner/p4_test.go`（阈值用大数 100000/200000；强制档用例把 `hint` 提到 180000 让回合内不触发；假 LLM 报 `prompt_tokens`，避开 system prompt 估算量）：

| 用例 | 断言 |
|---|---|
| `TestHintInsideTurnThenDirective` | 工具轮里跨阈值 → 后续请求带辅助提示；回合末仍下令 → 新回合首个请求带指令；`hinted=1/directives=1/handoffs=1/continued=1`（4 次请求） |
| `TestHandoffDirectiveAtTurnEnd` | 单请求回合（只在回合末跨阈值）→ 回合内无提示、回合末下令（3 次请求） |
| `TestHandoffDirectiveOncePerDispatch` | 两回合都不交接 → 只下一次令（2 次请求、`directives=1`、`continued=1`） |
| `TestHandoffHintBelowThreshold` | 未达阈只发 1 次请求、`hinted=0` |
| `TestHandoffHintSkippedWhenPaused` | 暂停不提示、不多开回合、`paused` 事件在人类日志里 |
| `TestForceTierResetsSession` | 回合 + 摘要共 2 次请求、摘要请求不带工具且带 ivan 指令、会话轮转、`TokensToday` 不含摘要用量 |
| `TestForceTierContinuesAfterReset` | 复位后按 `auto_continue` 续跑 |
| `TestForceHandoffFailureStops` | 摘要为空 → `handoff_forced`、`forced=0`、会话不轮转（第二条消息仍在同一会话里） |
| `TestForceSkipsHintAndDirective` | 回合末已超强制阈值 → 两处提示都不发，直接 `handoff_force` |
| `TestForceResetOnceThenDiagnostic` | 复位后仍超阈值 → 诊断并停，不再来第二次 |

既有用例（暂停不续跑、正常续跑、日预算跨进程累计、`CancelTurn`、系统提示含记忆/目标）不变。

## 10 落地记录（2026-11-26 → 2026-11-27）

- **2026-11-26**：新增 `runner/handoffhint.go`（`BeforeRequest` 钩子 + 建议档文案 + `contextUsed`）、`runner/handoffforce.go`（强制复位 + `forceHandoffInstruction` + 两种停止文案）；`task.run` 的 `pressure()`/`levelTokens()`/`forcedStop()`/`handoffForceNotice` 删除；`p4_test.go` 8 例改写；新增本文档。库侧无改动。
- **2026-11-27，第一次调优：注入点移到回合出口**——删钩子（`handoffHint`/`arm`/`disarm`/`fired`/`SessionEvent`、`inst.hint`、`WithHooks`），回合末 `AppendNotice` 下令后直接开一回合；`Status.Hinted` 计下令条数。（实测 §11.4）
- **2026-11-27，第二次调优：恢复钩子作辅助提示**——回合内提示（建议语气）与回合末下令（命令语气）并存，`mark()` 让下令后本推进余下请求不再出辅助提示。（实测 §11.5）
- **2026-11-27，同批**：计数拆成 `Hinted`/`Directives`/`Forced`，事件新增 `handoff_directive`，`ivan status` 的 runner 行印 `hints=/directives=/forced=`；`prompt/system_prompt.md` 增加"上下文用量不用自己查，接近上限会自动提醒"。
- 顺带修掉的既有竞态（独立提交）：`instance.worker bool` → `busy int`，回合收尾在 `queueMu` 内先减额度再叫醒等待者，修掉 `RotateSession` 偶发"有回合在跑或消息在排队"。

## 11 真实模型实测

口径：本地临时数据目录 + 真实模型（借本机 tanya 配置的凭证与模型 `vole`），任务全程真 `run_shell` 工具轮——「逐份 `cat` 读完 `docs-src/` 下 8 份 39KB 文档 → 写每份一句要点 → 为每份写 8 条细则 → 合并索引并读回」。跑法：临时测试文件驱动 `runner.Dispatch`（验证后即删，工作区无残留）；阈值一律 `handoff_hint_tokens = 32000`（**注入点 = 上下文 32K**，不是把总窗口压成 32K），要单独验证某一档时在内存里把另一档阈值置 0（配置层 `normalize` 会回落默认值，所以这不是可配置状态）。

§11.1–§11.3 是**旧点位**（只有回合内提示 / 只有强制档）的记录，保留作对照；§11.4、§11.5 是现行设计在两种点位上跑出来的结果。

### 11.1 第一档：注入点 32K、**关掉强制档兜底**、等它自然停止

- `hint = 32000`、`force = 0`（关闭）、`auto_continue = false`。
- 注入发生在 ctx **37328**（`handoff_hint`），比阈值高 5.3K——勾子在**请求发出前**判，判定值取的是上一次响应的用量，所以实际注入点会比阈值高"一个单请求增量"（这次是一份 30KB 文档读进来）。
- 注入后模型又跑了 **16 次工具轮**（stages 全部完成：STAGE1 8 条、STAGE2 8×8=64 条、INDEX 合并读回校验），**没有调用 `next_session`**：`hinted=1 / handoffs=0 / forced=0`，任务自然停止，`turn_end ctx=77180`。
- 结论：**建议档在这套模型上不产生自愿交接**。注入确实省了一次请求、提示确在历史里（后续 16 次请求都带着它），但模型把"阶段收尾后交接"理解成"做完再说"，而它并不知道窗口还剩多少，`32000` 对它没有压力语义。
- 顺带印证：无兜底时上下文一路涨到 77K。若部署窗口真是"注入点 + 32K"（64K），这轮会中途撞上游上限——**单靠建议档挡不住窗口，兜底必须由强制档承担**。

### 11.2 第二档：关掉建议档，仅强制交接 + 外部摘要质量

- `hint = 0`（钩子不做事）、`force = 12000`；回合末 `Stats()` 报 ctx 29968 ≥ 12000 → 摘要请求 → 轮转（`20261126-214123 → 214316`），事件 `handoff_force` 带 `old → new`；`hinted=0 / forced=1 / handoffs=1`。旧会话 27 条消息保留（末条为 `[会话已移交至 …]`），新会话只剩 system + 摘要 2 条。
- **外部摘要质量**：可用。结构自发分成「已完成 / 关键结论 / 改动过的文件 / 未完成与下一步」，写清了每阶段产出文件名与行数、只读未改的输入、下一步可选方案；还如实记了"输入与要求不符（`docs-src` 只有 3 份、没有 `channels.md`）"与"未跨目录取材"。新会话可直接接续，不需要人肉重建。
- **用量口径**：`turn_end total=208505` 与 `tokens_today=208505` 一致——摘要请求的 token 未进预算。

### 11.3 顺带测出的两条事实（只记录，不改行为）

1. **强制档只在回合边界生效**：回合内上下文可以涨过"窗口"（10.1 里涨到 77K），回合中途撞上游上限它救不了。
2. 判据 `max(ContextTokens, Est)`：`turn_end` 记的上报值（如 28929）与强制档判定用的估算值（29968）可能不同，接近阈值时估算值先触发。

### 11.4 只回合末下令（2026-11-27）

- `hint = 32000`、`force = 0`（关兜底）、`auto_continue = false`。
- 首回合把任务全做完（4m28s，回合末上报 `ctx=81318`），**回合内没有任何提示**——判定只在回合边界，单回合内上下文照样涨到 10 万。
- 回合出口 `used=107418`（`max(上报 81318, 估算 107418)`）≥ 32000 → 发出交接指令（当时两种提示共用一个 `handoff_hint` kind）→ 开新回合；**新回合第一次请求（19s）模型直接调 `next_session`**（摘要 18 行，自发分「已完成/关键结论/改动文件/未完成与下一步」），回合末落地 `…230149 → …230643`。
- 计数 `hinted=1 handoffs=1 forced=0 continued=1`。对比 §11.1（回合内提示、同量级任务）模型带着提示又跑 16 次工具轮、`handoffs=0`。
- 暴露的问题：**只判回合边界，单回合可以越阈 3 倍以上**（32K → 107K）。

### 11.5 辅助提示 + 回合末下令（2026-11-27，两局）

两局都是 `hint = 32000`、`force = 0`。

**第一局 `auto_continue = false`**：回合内第 3 份文档后 `ctx=44302` → `handoff_hint` 辅助提示 → **模型当场决定交接**（8 份里读完 3 份就调 `next_session`），回合 45s 结束并轮转；计数 `hints=1 directives=0 handoffs=1 continued=0`。回合末下令没有触发；`auto_continue` 关故交接后停住，任务停在中途。

**第二局 `auto_continue = true`**：一次 `DispatchTurn` 跨 **3 个会话**把任务做完（2m32s，`tokens_today=687716`）——三个会话分别在 ctx 42644 / 43003 / 32167 触发辅助提示后自主交接（`hints=3 directives=0 forced=0 handoffs=3 continued=2`），最后一个会话读完剩余文档并产出 `summary.md`（2605B）/`details.md`/`index.md`，`ctx` 收在 30607。每次交接额外开销约 40s，8 份 40KB 文档的活被切成 3 段。

### 11.6 三点结论

1. **辅助提示是主线，回合末下令是兜底**：两局实测 `directives=0`，下令一次没用上——它的价值在"回合内提示被无视"或"阈值只在最后一次响应后才跨过"（§11.4 那种单请求回合）。
2. **越阈幅度由判定位置决定**：只判回合末能涨到 107K；补了回合内提示后收在 32–45K。
3. 模型会自己盯用量（旧 prompt 下三次调内置 `agent_custom` 查 `usage`）——`prompt/system_prompt.md` 明说"不用自己查，接近上限会自动提醒"就是为了停掉这种无谓探测。
