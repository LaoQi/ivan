# 会话模型与推进策略

> 状态：规划。目标：会话连续、长任务能自主推进、并发可控可恢复。

## 1 会话键 → agent 实例

- 每个 `SessionKey`（见 `docs/channels.md` §4）对应一个 `*agent.Agent` 实例；实例内维护 history 与当前会话文件。
- 实例懒创建：首个消息到达时按 `SessionKey` 查映射 → 命中则 `LoadSession(id)` 恢复 → 未命中则 `NewSession()`。
- 实例池上限与闲置回收（如 32 个 / 闲置 30 分钟）：回收前确保无回合在跑；回收只释放内存，会话文件不动。
- 实例绑定 workspace（配置 `workspaces` 段：`SessionKey 前缀 → 目录`，默认 `data_dir/workspaces/<name>`），`run_shell` 默认在该目录执行。

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
3. 上下文压力：`EventResponse.ContextTokens` 或 `Stats()` 超过阈值（默认模型窗口的 70%）且本轮未交接 → 注入一条"请调用 next_session 交接"的 user 消息触发交接（下一轮生效），防止撑爆上下文。
4. 预算：累计 token/时长/续跑轮次超限 → 停止推进，落事件并（可选）通知人。
5. 失败：回合错误 → 记事件、按策略重试（有限次）、必要时回一条错误摘要到来源渠道。

**推进上限**是一个硬保护：单次 inbound 触发的自动续跑轮次有上限（默认 8 轮），超限则停下并把状态写进会话（下次人工/定时消息可继续），避免一次触发烧穿预算。

## 5 并发与背压

- 同 SessionKey 串行；跨 SessionKey 并发上限可配（默认 2）。
- 会话忙碌时到达的新消息：默认**排队**（每会话队列上限，如 8），超限丢弃并回一条"忙"提示。
- 运行中任务的取消：`ivan cancel <session>` 走 socket → 取消该会话当前 `context`（agent 支持 ctx 取消，`run_shell` 子进程按 tanya 既有语义处理）。
- 停机：SIGTERM → 停止入站 → 等在跑回合收尾（超时则取消）→ 刷出站 → 落状态。

## 6 重启恢复

- 未完成的回合在崩溃后**不自动续跑**（避免半截状态的副作用重复执行）；启动时把相关会话标记为 `interrupted` 并落事件，等下一次消息自然继续（会话历史本身是完整的 append-only 文件）。
- 队列不持久化（首期）；重启丢队列里的待处理消息，但来源通道的消息本身仍在平台上，用户可重发。

## 7 记忆与目标（P4）

- 记忆：`data/memory/<session>.md`（或其他约定文件），由 `remember` 工具追加；每轮注入为 user 消息（不动 system prompt，保 prompt cache 不变性）。
- 目标：workspace 内 `GOALS.md`/`TODO.md` 的轻量约定，人在场可编辑，agent 每轮读取自己推进；心跳自省（后置）在空闲时据此唤醒。
- 注入内容有长度上限与去重（同一片段不重复注入），避免上下文被记忆吃掉。

## 8 验收

- 同一 QQ 单聊连续对话共享历史；重启 daemon 后继续对话历史不丢。
- 一个多步任务被单条消息触发后，`next_session` 交接 + 自动续跑完成，事件日志里能看到会话 id 的切换。
- 上下文超过阈值时自动交接，不出现"上下文超限"报错。
- 自动续跑轮次/预算超限时安静停下并留下可见状态。
