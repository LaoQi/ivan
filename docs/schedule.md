# 定时与计划任务

> 状态：规划。这是 ivan 相对 tanya 的核心特化能力：模型能自己安排将来的事，且安排得住（持久化、重启恢复、结果回灌）。

## 1 作业模型

```json
{
  "id": "job_20261010_1",
  "name": "每日汇报",
  "enabled": true,
  "schedule": { "kind": "every", "every_ms": 86400000, "at_ms": null, "expr": "", "tz": "Asia/Shanghai" },
  "payload": { "prompt": "汇总昨天的 git 提交并发我", "channel": "qq", "chat_id": "…", "session_key": "qq:c2c:…" },
  "state": { "next_run_at_ms": 0, "last_run_at_ms": 0, "last_status": "ok|failed|running|missed", "last_error": "", "runs": 0 },
  "created_at_ms": 0,
  "updated_at_ms": 0,
  "delete_after_run": false
}
```

- `kind`：`at`（一次性，`at_ms`）/ `every`（周期，`every_ms`）；`expr`（cron 表达式）**本期只留字段不实现**（D5）。
- `payload.channel` + `chat_id` + `session_key`：结果回灌的目标。模型创建作业时若不指定，默认继承**创建它的那个会话**（这正是"提醒我/每天汇报"的正确语义）。
- `delete_after_run`：一次性作业执行后自删（避免 jobs.json 膨胀）。

## 2 调度器

- 单 goroutine 循环 + 一个定时器 + 一个 `wake` 通道：新增/修改/删除作业立即 `wake` 重算最近到期时间，**不轮询空转**。
- 到期处理：置 `running` → 构造 `bus.InboundMessage{Channel: "schedule", SessionKey: payload.session_key, ChatID: payload.chat_id, Content: payload.prompt}` 投递 → 等回执（或超时）→ 写 `last_status`/`last_error`/`next_run_at_ms`。
- 不重入：同一作业上一次未结束则本次跳过并记 `skipped`（不排队堆积）。`every` 的下次时间**基于上次计划时间**推进（避免漂移累积），落后超过一个周期则按 missed 处理。
- 错过策略：进程停止期间到期的 `at` 作业，启动时按配置 `missed_policy: run|skip`（默认 `run`，一次性提醒不该被吞）；`every` 只重算 next，不补跑。
- 时间：内部一律 UTC 毫秒时间戳；`tz` 只在将来支持 `expr` 时用于解释表达式与展示。

## 3 存储

- `data/jobs.json`：整体原子写（临时文件 + `fsync` + rename）；文件不大（个人量级：几十个作业）。
- 解析失败：保留原文件（改名 `.corrupt.<ts>`）、显式报错、**不静默重建**；新作业仍可创建（内存态 + 新文件），并对用户可见。
- 变更即落盘（add/update/remove/执行后状态更新），避免崩溃丢作业。

## 4 模型工具 `schedule`

一个工具覆盖增删改查与手动触发（对齐 picoclaw 的 `cron` 工具做法，但参数用相对量，避免模型算错绝对时间）：

```json
{
  "action": "add | list | update | remove | run_now",
  "name": "每日汇报",
  "at_seconds": 600,
  "every_seconds": 86400,
  "when": "2026-10-11T09:00:00+08:00",
  "prompt": "汇总昨天的 git 提交并发我",
  "channel": "qq",
  "chat_id": "",
  "job_id": ""
}
```

- 描述里明确写：**用户说"提醒我/每天/过一会儿"时必须调用本工具**，并给出相对→秒的示例（防止模型把"10 分钟后"算成绝对时间）。
- `list` 返回紧凑表格（id/名称/下次时间/状态）；`remove` 用 `job_id`。
- 未指定 target 时，由 runner 注入当前会话信息（工具实现拿到"当前回合的会话上下文"）——需要一个 per-turn 的上下文载体，落在 runner 侧，模型不可伪造。

## 5 与通道的关系

调度器不认识 QQ：它只投 Bus。出站时 `Manager` 按 `channel` 把结果发到真实通道（`qq`）或本地（`cli`）。这样"定时任务"和"用户消息"共用一条处理路径，会话历史连续（提醒内容接在对话里，而不是孤立的一条）。

## 6 可观测

- `ivan jobs`：列出作业（id/名称/下次时间/上次状态/错误）。
- `ivan jobs rm <id>`、`ivan jobs run <id>`：人工干预（读改写 `jobs.json`，与 daemon 通过 socket 协调）。
- 事件日志：作业触发、完成、失败、跳过都落 `events/<date>.jsonl`。

## 7 验收

1. 新增 `every` 作业 → 按时触发 → 结果发回指定会话；`job state` 更新。
2. 重启 daemon → 作业仍在、`next_run_at_ms` 正确重算；停止期间的 `at` 作业按策略补跑。
3. 模型在 QQ 里说"10 分钟后提醒我喝水" → 自动建作业 → 到点收到 QQ 消息。
4. 长作业超过周期 → 跳过而非堆积，日志可见。
