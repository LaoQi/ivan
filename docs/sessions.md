# 会话模型与推进策略

> 状态：**P1 已实现**（会话键→实例池、映射持久化、回合驱动、重启恢复）；**P4 已实现**（上下文阈值自动交接、跨进程日预算、暂停开关、记忆/目标按需读取、`ivan cancel`）。目标：会话连续、长任务能自主推进、并发可控可恢复；实现记录见 §9（P1）与 §10（P4）。

## 1 会话键 → agent 实例

- 每个 `SessionKey`（见 `docs/channels.md` §4）对应一个 `*agent.Agent` 实例；实例内维护 history 与当前会话文件。
- **每个会话键有独立的工作区目录** `<data_dir>/agents/<key-slug>/`（P0 探针 R11 结论：tanya 会话 id 为秒级时间戳，多实例共享工作区会互相覆盖）。会话文件落该目录下；`run_shell` 的默认目录另由"项目目录"配置提供，不与会话目录耦合。
- 实例懒创建：首个消息到达时按 `SessionKey` 查映射 → 命中则 `LoadSession(id)` 恢复 → 未命中则 `NewSession()`。
- 实例池上限与闲置回收（如 32 个 / 闲置 30 分钟）：回收前确保无回合在跑；回收只释放内存，会话文件不动。
- 装配纪律（P0 探针 R7 结论）：`tools.Options.Workspace` 必须是**动态闭包**指向 `agent.Workspace()`，否则 `SwitchWorkspace` 后命令仍跑在启动目录。

## 2 映射持久化

`data/state.json` 记录 `SessionKey → {session_id, workspace, updated_at}`，每次会话创建/交接/切换后原子写。重启后按映射 `LoadSession` 恢复；映射存在但文件消失时，回退新建并记一条 warn（不静默）。

## 3 回合驱动

```
Inbound → runner.dispatch(msg)
           ├─ 查/建 Agent 实例（按 SessionKey）
           ├─ 同 SessionKey 串行（该会话一个回合在跑时，后续消息排队或合并，见 §5）
           ├─ 注入本轮上下文（当前 channel/chatID/messageID，供 schedule 工具与被动回复使用）
           ├─ agent.Ask(ctx, content, sink)   // sink 把事件分流：文本→出站聚合、工具→事件日志
           └─ 回合后结算（见 §4）
```

事件消费：`sink` 只做两件轻活——写有缓冲的事件 channel、累加文本；真正的落盘与出站在独立 goroutine，**绝不在 sink 里阻塞**（tanya 硬性要求）。

## 4 回合结算与自主推进

回合结束后按顺序：

1. 聚合本轮文本 → `OutboundMessage{ReplyTo: inbound.MessageID}` → Bus（回到来源渠道）。
2. `TakeHandoff()`：若有交接 → 更新 `state.json` 的 session 映射 → 若 `Continue` 则立即 `agent.Continue(ctx, sink)` 继续跑（这就是长任务跨会话推进）。
3. 上下文压力（**已实现，两档**）：以 `ContextTokens`（上次请求的 prompt tokens，缺失回退 `Stats.ContextTokens`/`Est`）对比两个绝对阈值——
   - **一级 建议交接** `handoff_hint_tokens`（默认 180000）：注入"建议在本阶段收尾后调用 next_session 交接"，模型可先把当前步骤做完；每任务只注入一次。
   - **二级 强制交接** `handoff_force_tokens`（默认 256000）：注入"必须立即调用 next_session，本回合不再做其他工作"（事件 `handoff_force`）；若模型仍未交接，**停止本任务**（事件 `handoff_forced`），不再发请求以免撞窗口。
   - 两档都走 `AppendNotice` + `Continue` 一轮，都不受 `auto_continue` 限制（安全阀），但同样受暂停与预算闸门；建议档之后的回合跨到强制档会再升级提示一次（`hinted`/`forced` 各自只触发一次，循环有界）。
   - **交接后重置**：每发生一次交接，`hinted`/`forced` 清零（新会话上下文基线不同，两档重新计），并累加本任务的 `hops`；若交接后新会话基线**仍**超强制阈值、模型又不再交接，则停止并给出诊断（`handoff_forced`，提示"已交接 N 次、新会话即超阈值，请调阈值或拆任务"），而不是笼统说"模型未交接"。
4. 预算（**已实现**）：`max_turns_per_task`（续跑轮次）、`max_tokens_per_task`（单任务）、`max_tokens_per_day`（**全局日预算**，含用户消息/定时作业/续跑，按本地日累计在 `state.json`，跨进程生效）任一超限 → 停止推进，落 `budget` 事件后安静停下。
5. 失败：回合错误 → 记事件、回错误摘要到来源渠道（**不做**自动重试：重试会把无人值守的成本放大）。
6. 暂停（`state.json: paused`，`ivan pause/resume`）只挡**无人值守路径**：定时作业触发、交接后自动续跑、阈值交接提示；用户主动发来的消息照常处理（人在场时不该被挡住）。

**自主推进是受控能力**（配置项 `auto_continue`，**默认关**）：开启后单次 inbound 触发的自动续跑轮次有上限（默认 8 轮），并受 `max_tokens_per_task` / `max_duration_per_task` 约束；超限即停下并把状态写进会话（下次人工/定时消息可继续）。建议只在指定工作区（低风险、只读或报告类任务）启用。

**per-turn 上下文注入**（`schedule` 等工具需要"当前消息来自哪个通道/会话"）：每个 `Agent` 实例配一套工具实例，runner 在该实例的回合开始前写入当轮上下文（通道/chatID/messageID），回合结束清空；同实例回合串行保证不串号，上下文只由 runner 写入、模型不可伪造。

## 5 并发与背压

- 同 SessionKey **严格按到达顺序**串行；跨 SessionKey 并发上限可配（默认 2，`daemon.max_concurrent`）。
- **取并发槽的顺序是硬约束**：先排队（每会话 FIFO 队列，`runner.session_queue` 默认 8）→ 轮到了再取会话锁 → 最后才取跨会话并发槽。**反了就是缺陷**：同一会话的第二条消息会先抢到并发槽、再阻塞等会话锁，`max_concurrent=2` 时同会话连发两条即可占满两个槽、其他会话全部堵死（2026-10-30 修，见 §11）。
- 队列上限 `runner.session_queue`（默认 8，`0`/负值回落默认）：超限**不静默吞掉**——`DispatchTurn` 返回 `ErrQueueFull`、计 `queue_dropped`（`status` 可见）、人类日志记一行 `busy`，总线层不再重复计丢弃。
- **同会话顺序由队列保证**，不依赖 goroutine 抢锁：旧实现里两条消息谁先进 `DispatchTurn` 取决于调度，顺序不确定。
- 运行中任务的取消（**已实现**）：`ivan cancel <会话键>` 走 socket → `runner.CancelTurn(key)` 取消该会话当前回合的 ctx（agent 支持 ctx 取消，`run_shell` 子进程按 tanya 既有语义处理）；没有在跑的回合时明确报错。
- 停机：SIGTERM → 停止入站 → 等在跑回合收尾（超时则取消）→ 刷出站 → 落状态。

## 6 重启恢复

- 未完成的回合在崩溃后**不自动续跑**（避免半截状态的副作用重复执行）；启动时把相关会话标记为 `interrupted` 并落事件，等下一次消息自然继续（会话历史本身是完整的 append-only 文件）。
- 队列不持久化（首期）；重启丢队列里的待处理消息，但来源通道的消息本身仍在平台上，用户可重发。

## 7 记忆与目标（P4 已实现：按需读取）

- 记忆：`<data_dir>/memory/<key-slug>.md`（每个会话键一份）。**不做每轮注入**：路径写进实例的 system prompt（静态字符串，保 prompt cache 不变性），模型需要背景时自己 `run_shell` 读取、有新结论时追加一条带日期的要点。
- 目标：会话工作区内的 `GOALS.md`，人在场可编辑，路径同样进 system prompt；模型按它推进、完成项自行打勾，不替人删未确认条目。
- 为什么不做注入/工具：注入会吃掉上下文并破坏缓存，专用 `remember` 工具只省一次 shell 调用却多一份维护面；shell + 路径约定足够，且人可以直接编辑同一批文件。
- 记忆文件不自动创建/裁剪（首期）：避免长歪的自动摘要污染；需要时由人清理。

## 7.1 中间态投递（2026-10-22 实现）

回合执行期间由 `runner` 侧的 `streamState` 负责让用户看到"还在跑"，形态是**一条消息原地更新**，不做逐里程碑的进度消息：

- 逻辑消息 id 形如 `<会话键>#<自增序号>`，整条回复只占一条消息（`Stream` 字段，经总线到 `Manager`）。
- 正文就绪前（模型思考/工具执行）用"输入中"（`KindTyping`）覆盖静默期，间隔 45s 一次；正文就绪后按 `stream_every_seconds`（默认 1s）流式跟进，正文变化即时推送（不额外等待节流）。
- 内容达到 `stream_max_chars`（默认 2048）后停流，收尾时把余下内容作为普通消息补发（不丢内容）。
- 回合结束由 `streamState.finish` 投递终局（带原始 `Kind`），失败/取消同样收尾。
- 配置：`runner.stream`（总开关）、`stream_first_seconds`、`stream_every_seconds`、`stream_max_chars`，按通道可用 `channels.<name>.stream` 单独关（默认 cli/qq 都开）。
- 覆盖范围：只对**用户主动消息**生效；定时作业回投走直投路径，不做流式（`direct` 分支）。

## 8 验收

- 同一 QQ 单聊连续对话共享历史；重启 daemon 后继续对话历史不丢。
- 一个多步任务被单条消息触发后，`next_session` 交接 + 自动续跑完成，事件日志里能看到会话 id 的切换。
- 上下文超过阈值时自动交接，不出现"上下文超限"报错。
- 自动续跑轮次/预算超限时安静停下并留下可见状态。
- （P4 验收）在指定工作区开启 `auto_continue` 后，单条消息能跨会话推进，且被轮次/日预算/暂停三处闸门约束住；上下文接近窗口时自动提示交接，不出现超限报错。

## 9 P1 实现记录（2026-10-10）

代码：`runner/`（`runner.go` 回合驱动 + `pool.go` 实例池）、`data/state.go`；测试：`runner/runner_test.go`（假 LLM，9 例）、`daemon/daemon_test.go`（端到端，5 例）。

1. **实例池**：`acquire(key)` 懒建 `instance{key, ws, mu, ag}`，`ag` 首次回合时构造；池上限 `runner.max_agents`（默认 8）满则淘汰最久未用的**空闲**实例（`mutex.TryLock` 失败即视为在跑，跳过）；`runner.idle_timeout_minutes`（默认 30）到期由后台 goroutine 回收。回收只释放内存，会话文件不动。
2. **工作区**：`<data_dir>/agents/<KeySlug>`；`agent.New` 会先按进程 cwd 建会话目录，随后 `SwitchWorkspace(inst.ws)` 并清理空残留目录（`cleanStrayDirs`），因此 cwd 不会污染数据目录。
3. **回合驱动**：`Runner.Run(ctx)` 只做入站消费与并发闸门（`daemon.max_concurrent`，默认 2），每条消息一个 goroutine；`Dispatch(ctx, msg)` 内持 `instance.mu` 保证**同键串行**。事件 sink 只做三件轻活：累加文本、写缓冲事件 channel、渲染前台流；落盘与出站都不在 sink 里阻塞。
4. **出站**：任务级聚合（`auto_continue` 的多轮也合成一条），`Channel/ChatID/ReplyTo` 复制自入站消息 → 回到来源渠道；出错时 `Kind=error` 且失败文本兜底。
5. **映射**：`state.json` 记 `SessionKey → {session_id, workspace, updated_at}`，回合结束与交接后原子写（临时文件 + fsync + rename，0600）；解析失败**拒绝启动**并保留原文件。
6. **交接**：`TakeHandoff()` → 立即更新映射。`auto_continue`（默认**关**）开启时才 `Continue`，且受 `max_turns_per_task` 与 `max_tokens_per_task`（0=不限）双重熔断，超限落 `budget` 日志后安静停下。
7. **重启恢复**：映射恢复走 `LoadSession`；映射存在但会话文件丢失（`SessionDir` 下找不到）时回退新建并记 warn + 清映射。崩溃回合的 `interrupted` 标记已补齐（见 §10.7）。
8. **仍待办（P4）**：上下文阈值自动交接、跨 handoff 的预算累计（tanya `Stats` 在交接后 reset）、`ivan cancel <session>`。（P4 已全部补齐，见 §10）

## 10 P4 实现记录（2026-10-20）

代码：`runner/runner.go`（阈值交接提示、预算/暂停闸门、`CancelTurn`、`Budget`）、`runner/pool.go`（实例回合取消、记忆/目标路径进 prompt）、`prompt/`（`Info` 结构 + 记忆/目标段）、`data/state.go`（`usage` 日累计）、`config`（`runner.max_tokens_per_day`、`MemoryDir`）、`daemon`（暂停/预算接线、`cancel` 控制指令）、`commands.go`（`ivan cancel`、status 的 budget 行）。

1. **两档阈值交接**：`task.pressure()` 返回 `none|hint|force`（绝对值 `handoff_hint_tokens`/`handoff_force_tokens`，默认 180K/256K，配置里强制档必须大于建议档否则启动报错）；建议档注入"收尾后交接"提示，强制档注入"立即交接、本回合别做别的"指令。`hinted`/`forced` 各自每任务只触发一次（事件 `handoff_hint`/`handoff_force`），**每次交接后清零**（并记 `hops`，新会话基线可能仍超阈值）；强制档提示后模型仍不交接 → 停止任务并记 `handoff_forced`（不再发请求），已交接过的任务给出"新会话基线即超阈值"的诊断文案。`auto_continue` 关时只多做提示轮（把上下文安全交出去），交接后仍不续跑；`status` 的 `budget` 行显示两档阈值与 `hints/forced` 计数。
2. **预算熔断**（三处闸门 + 一处持久化）：`max_turns_per_task` / `max_tokens_per_task` / `max_tokens_per_day`，前两者按任务、后者按**本地日**；日用量由 `data.State` 的 `usage{date,tokens}` 记录（原子写，随 `state.json`），`DispatchTurn` 结束时由 `runner.chargeTokens` 累加，`runner.Budget()` 对外暴露给调度器。调度器不再自带计数器（P3 的 `schedule.max_tokens_per_day` 已删除，统一到 runner，避免两个预算口径）。
3. **暂停开关语义**：`paused` 只挡无人值守路径（定时作业、交接续跑、阈值交接提示），用户消息照常处理；事件分别记 `paused`（runner）与 `job_paused`（调度器）。
4. **取消**：实例持当前回合 `context.CancelFunc`，`CancelTurn(key)` 取消并返回是否有在跑回合；`ivan cancel <会话键>` 经 socket 调用。
5. **记忆/目标**：`prompt.Info{Workspace,DataDir,Session,Memory,Goals}`；记忆路径 `<data_dir>/memory/<key-slug>.md`、目标 `<workspace>/GOALS.md`；`system_prompt.md` 增"记忆与目标"行为约定（按需读、一行一条追加、不删未确认条目）。
6. **测试**：`runner/p4_test.go`（阈值提示注入到第二轮且首轮不带、每任务只提示一次、未超阈值不提示、暂停不提示、暂停不续跑、正常续跑、日预算停续跑 + 跨进程累计、`CancelTurn` 真取消 httptest 阻塞请求、系统提示含记忆/目标路径）、`data`（日用量累加/跨日清零/重启保留）、`daemon`（`cancel` 参数与空闲报错、`pause/resume` 反映到 `status.runner.paused`）。
7. **P4 遗留的收口（2026-10-21 补齐）**：
   - **崩溃回合 `interrupted` 标记**：`state.json` 增 `running{<会话键>: {session,channel,chat_id,started_at}}`，`DispatchTurn` 开始 `BeginTurn`、结束 `EndTurn`（原子写）。启动时 `recoverInterrupted()` 用 `DrainRunning()` 清残留并逐条记事件 `turn_interrupted`（人类日志同名行）+ `Status.interrupted` 计数；该会话**下一次回合**在 `Ask` 前 `AppendNotice` 注入"上次回合因进程退出中断、结果未知、先核对实际进展"的提示（只注入一次）。进程被 `SIGKILL`/OOM 后重启即可看到，不再靠猜。文件：`data/state.go`、`runner/runner.go`、`runner/recover_test.go`、`data/filetail_test.go`。
   - **记忆文件自动裁剪**：`runner.memory_max_bytes`（默认 **0 = 不裁剪**）。回合结束后按行边界保留最新内容到上限，文件头加一行 `<!-- 记忆已裁剪 <日期>：只保留最近 N 字节（原 M 字节） -->`，`0600` 原子写；裁剪量记事件 `memory_trim`。实现 `data.TrimFileTail`。
   - **按会话的工作区白名单**：**判定不适用**——会话工作区恒为 `<data_dir>/agents/<key-slug>/`，ivan 未向模型暴露任何切换工作区的工具（`tools.Standard` 只有 `run_shell`/`read_image`/内建），"指定低风险工作区"实际靠 `data_dir` 与项目目录配置。若将来暴露工作区切换能力，再补白名单。

## 11 并发槽语义修复（2026-10-30）

**问题**：`Runner.Run` 与 `Runner.TriggerJob` 都是**先抢跨会话并发槽**（`r.sem`），再进 `DispatchTurn` 内部拿会话实例锁（`inst.mu`）。同一会话的第二条消息因此会：

1. 先占住一个并发槽（`max_concurrent` 默认 2，直接吃掉一半额度）；
2. 再阻塞在 `inst.mu` 上等同会话的前一轮跑完。

`max_concurrent=2` 时，同一会话连发两条消息即可把两个槽全占满，**其他所有会话被无限期堵死**。另外同会话的顺序靠 goroutine 抢锁决定，**不保证按到达顺序执行**。

这是早期设计缺陷：把"跨会话并发"与"会话内串行"两层闸门的获取顺序搞反了。会话内排队是逻辑上的第一层，并发槽是第二层。

**修法**（`runner/pool.go`、`runner/runner.go`、`config`）：

| 项 | 变化 |
|---|---|
| 会话内排队 | `instance` 增 FIFO 队列 `queue []queuedTurn` + 单 worker（`drain`）。`enqueue` 只登记并唤醒 worker（`worker` 标志保证每实例最多一个 worker），`dequeue` 取队头 |
| 获取顺序 | `runQueued` 先 `inst.mu.Lock()`，**之后**才 `r.sem <- struct{}{}`；`Run` / `TriggerJob` 外层不再抢槽 |
| 顺序保证 | 同会话严格 FIFO，不再依赖 goroutine 调度 |
| 队列上限 | `runner.session_queue`（默认 8）：超限返回新增的 `ErrQueueFull`、计 `queue_dropped`、日志记 `busy`；`Run` 里对 `ErrQueueFull` 单独分支（不计入 `error`） |
| 可观测 | `Status.QueueDropped`，`ivan status` 的 runner 行末尾 `queue_dropped=N` |
| 停机 | `submit` 等待结果时同时监听 `r.turnCtx`，停机时立即返回而不是等队列 |

**测试**（`runner/queue_test.go`，变异复验三处全部变红）：

- `TestQueuedMessageDoesNotHoldSem`：`max_concurrent=2`，A 会话首回合卡在上游；A 的第二条消息**不得**让 B 会话被堵（新实现下假服务端收到 2 个请求，旧实现只收到 1 个）。
- `TestSameSessionFIFOOrder`：第一条在跑时再顺序投递两条（队列长度 > 1，LIFO 与 FIFO 才会不同），三个请求的 user 文本必须依次为第一、第二、第三问。
- `TestQueueFullDropsAndCounts`：`session_queue=1`，第三条必须**立即**被拒并返回 `ErrQueueFull`、`queue_dropped=1`（无上限时会一直挂起，用例自带 3s 超时判定）。

**顺带修掉的测试基建缺陷**：`internal/fakellm` 的 `Gate/Hold/Release` 此前是**死代码**（`handle` 里从未读 `s.gate`），导致任何想制造"上游卡住"的用例都拿不到阻塞点。现在 `handle` 在写响应前等待 gate，并响应 `r.Context()` 取消。

**未做**（明确不在本次范围）：跨会话并发槽的公平性（谁先抢到谁先跑，无 FIFO 保证）。

## 12 实例池上界修复（2026-10-30）

**问题**：`acquire` 在池满时只**尝试**淘汰一个实例，`evictLocked` 返回 0（没有可淘汰的）时**仍然照常插入**新实例——`runner.max_agents` 实际上没有上限，忙会话一多池就无限增长（每个实例 = 一个 `*agent.Agent` + 一个工作区目录 + sessions/，长期跑会把 `data_dir` 撑大）。

**队列化改造后新增的风险**（§11）：原淘汰条件只判 `inst.mu.TryLock()`（即"锁空闲"）。但队列化的实例可能**锁空闲却队列还有活**（worker 正在取下一个任务），淘汰它会话键会再建一个**同工作区**的新实例，两个实例并发写同一批会话文件——正是探针 R11 记录过的覆盖问题。

**修法**（`runner/pool.go`）：

| 项 | 变化 |
|---|---|
| 空闲判定 | 新增 `instance.idleLocked()`：`mu` 可获取 **且** `working()` 为假 **且** `pending() == 0`。队列状态与非队列状态一并纳入 |
| 上限执行 | `acquire` 改为返回 `(*instance, error)`；池满且 `evictLocked(0)` 返回 0 时返回新增的 `ErrPoolFull`，**不再静默超限** |
| 计数 | `Status.PoolFull`（`ivan status` 的 runner 行末尾 `pool_full=N`） |
| 日志 | `Run` 里 `ErrPoolFull` 与 `ErrQueueFull` 同走 `busy` 分支，不计入 `error` |
| 回收 | `evictLocked` 统一改判 `idleLocked()`，`reap`（闲置回收）与 `acquire`（腾位置）共用同一口径 |

**测试**（`runner/pool_limit_test.go`，变异复验三处全部变红）：

- `TestPoolFullRejectsNewSession`：`max_agents=1`，唯一实例卡在回合里时新会话必须返回 `ErrPoolFull`、池大小仍为 1、`pool_full=1`（变异：恢复"淘汰失败也照插" → 报「满池应返回 ErrPoolFull，实际 <nil>」）。
- `TestPoolDoesNotEvictInstanceWithQueuedWork`：A 有排队消息、B 在跑时第三个会话进不来，A 不得被淘汰。
- `TestPoolIdleCheckConsidersQueue`：直接构造「锁空闲 + worker 在 + 队列非空」的状态，断言 `idleLocked()` 为假（变异：只判 `TryLock` → 报「带排队消息（worker 在跑）的实例不应被判为空闲」）。这条是必须的——靠真实回合卡住来验证时 `TryLock` 就先失败了，走不到队列判定。
- `TestReapSkipsInstanceWithQueuedWork`：闲置回收同样不能动带排队任务的实例（变异：`reap` 改回只判锁 → 报「带排队消息的实例不应被闲置回收，实际回收 1 个」）。
- `TestPoolEvictsIdleWithWorkerFlagCleared`：反向条件，回合结束、worker 退出、队列为空的实例可被淘汰腾位。

**行为变化（部署需注意）**：`max_agents`（默认 8）从"软上限"变成**硬上限**。并发活跃会话超过该值时，新会话的消息会被丢弃并回 `busy`（`ErrPoolFull`），而不是继续建实例。多来源并发较大（如多个外部服务同时 `/request`）时应调高它。
