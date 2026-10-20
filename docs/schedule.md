# 定时与计划任务

> 状态：**P3 已实现**（`schedule/` 模型+调度循环、`schedule/sqlite` 存储实现、`schedule` 工具、daemon 接线、`ivan jobs`/`pause`/`resume`；实现记录见 §8）。这是 ivan 相对 tanya 的核心特化能力：模型能自己安排将来的事，且安排得住（持久化、重启恢复、结果回灌）。

## 1 作业模型

```json
{
  "id": "job_20261020_233000_ab12",
  "name": "每日汇报",
  "enabled": true,
  "schedule": { "kind": "every", "every_ms": 86400000, "at_ms": 0, "expr": "", "tz": "" },
  "payload": { "prompt": "汇总昨天的 git 提交并发我", "channel": "qq", "chat_id": "…", "session_key": "qq:c2c:…" },
  "state": {
    "next_run_at_ms": 0, "last_run_at_ms": 0,
    "last_status": "ok|failed|missed|running|interrupted|budget",
    "last_error": "", "last_notify_error": "", "last_tokens": 0, "runs": 0
  },
  "delete_after_run": false,
  "created_at_ms": 0,
  "updated_at_ms": 0
}
```

- `kind`：`at`（一次性，`at_ms`）/ `every`（周期，`every_ms`，下限 1000ms）；`expr`（cron 表达式）**本期只留字段不实现**（D5，`Validate` 见到 `expr` 直接报错）。
- `payload.channel` + `chat_id` + `session_key`：结果回灌的目标。**由 runner 注入、模型不可指定**：默认继承创建作业的那个会话（"提醒我/每天汇报"的正确语义）。
- `state`：`last_status` 只反映**作业执行**结果；`last_notify_error` 单独记投递失败（通道不可达、频控），通知失败不影响执行判定。
- `delete_after_run`：一次性作业执行后自删（默认 true，`every` 默认 false）。

## 2 调度器

- 单 goroutine 循环 + 一个定时器 + 一个 `wake` 通道：增删改作业、执行完成立即 `wake` 重算最近到期时间，**不轮询空转**（无作业时睡 60s 兜底）。
- 到期处理：`launch` 置 `running` 落盘 → 起 goroutine 派发（`Dispatcher.TriggerJob`，超时 `job_timeout_seconds` 默认 900）→ 回执后写 `runs`/`last_*`/`next_run_at_ms`。
- 派发是**同步调用 runner**（不经 Bus）：只有这样作业状态才能拿到真实回合结果与 token 用量（决策见 §8.1）。
- 不重入：同一作业在飞时不再派发（不排队堆积）；`every` 的下次时间**基于计划时间**推进（`planned+every`，不漂移），落后超过一个周期记 `missed` 并跳到未来。
- 错过策略：进程停止期间到期的 `at` 作业，启动时按 `missed_policy: run|skip`（默认 `run`，一次性提醒不该被吞）；`every` 只重算 next、不补跑（记 `missed` 事件）。
- 崩溃语义：`running` 状态在下次启动时归一为 `interrupted`（不假装跑过）。
- 时间：内部一律毫秒时间戳（UTC 语义），展示按本地时区；`tz` 只在将来支持 `expr` 时用于解释表达式。

## 3 存储（抽象层 + sqlite 实现）

存储是**接口 + 实现**两层，不把文件布局写死：

```go
// schedule.Store：消费者定义的窄接口，实现必须并发安全
type Store interface {
    List() ([]Job, error)
    Get(id string) (Job, bool, error)
    Put(j Job) error
    Delete(id string) error
    Close() error
}
```

- 首个生产实现：`schedule/sqlite`（`modernc.org/sqlite`，**纯 Go 无 cgo**），文件默认 `<data_dir>/jobs.db`。
- 打开时 `PRAGMA user_version` 做 schema 迁移；库版本高于程序 → 显式报错退出（不降级写）；`jobs` 表按列存（不是 JSON blob），带 `(enabled, next_run_at_ms)` 索引。
- 可靠性：WAL + `synchronous(FULL)` + `busy_timeout(5000)`，单连接串行化写；写入由 sqlite 保证原子提交，进程崩溃不回滚已提交作业、不产生半条记录。
- 损坏/解析失败：**保留原文件、显式报错、不静默重建**。daemon 打不开作业库时降级为"无调度器"（daemon 仍跑通道），错误进事件日志与 `ivan status` 的 `schedule_error`；文件留在原处等人工处理。
- 为什么不用 jobs.json：整文件重写好于追加写，但仍然要自己实现原子性、并发访问与索引；作业状态是"频繁小写 + 需要一致性"的数据，交给事务型存储更稳（决策见 `docs/plan.md` D8）。
- 变更即落盘（add/update/remove/执行后状态更新），避免崩溃丢作业。

## 4 模型工具 `schedule`

一个工具覆盖增删改查与手动触发（对齐 picoclaw 的 `cron` 工具做法，但参数用相对量，避免模型算错绝对时间）：

```json
{
  "action": "add | list | update | remove | run_now",
  "job_id": "",
  "name": "每日汇报",
  "prompt": "汇总昨天的 git 提交并发我",
  "at_seconds": 600,
  "every_seconds": 86400,
  "when": "2026-10-21T09:00:00+08:00",
  "enabled": true,
  "delete_after_run": true
}
```

- 描述里明确写：**用户说"提醒我/每天/过一会儿"时必须调用本工具**，并给出相对→秒的示例（防止模型把"10 分钟后"算成绝对时间）。
- **目标会话不可由模型指定**：`add`/`update` 都没有 `channel`/`chat_id`/`session_key` 参数，目标固定取当前实例的会话目标（`runner` 在每次 `DispatchTurn` 时记录），模型无法伪造。
- `every_seconds` 工具层下限 60s（更短的一次性提醒用 `at_seconds`）；`at_seconds` 上限 365 天；`when` 支持 RFC3339 与 `"2006-01-02 15:04"`（本地时区），过去时间报错。
- `list` 返回紧凑列表（id/名称/类型/下次/上次/次数/错误）；`remove` 用 `job_id`；`run_now` 立即执行一次且**不改原计划**（`at` 也不会被消耗）。
- **只有模型能从对话里建作业**（`ivan jobs` 只做 list/rm/run），人工建作业请改库或经模型发起。

## 5 与通道的关系

调度器不认识 QQ，也不碰 `Channel` 接口：

- 触发 → 构造 `bus.InboundMessage{Channel: "schedule", ReplyChannel: payload.channel, ChatID, SessionKey, Content: prompt, Raw{trigger, job_id}}` → `runner.TriggerJob`（复用会话与并发闸）。
- 出站：`ReplyChannel` 非空表示"回投目标不是本条来源通道"，`runner` 据此**直投** `Manager.Route`（拿到真实错误），不再走 Bus；普通用户消息仍走 Bus。这样作业的 `last_notify_error` 是可信的（决策见 §8.1）。
- 结果回灌是 **best-effort**：作业目标的平台限制（QQ 主动消息频控/日上限/用户开关）会导致失败；`ErrUnroutable`（如 `cli` 的请求连接已关）按 `unmatched` 计数，不算作业失败。
- 作业执行不因通知失败而判定失败：`last_status` 反映**作业执行**结果，通知失败另记 `last_notify_error`。
- 结果接在原会话里（会话历史连续），而不是孤立的一条：会话键取 `payload.session_key`，与用户对话共用同一份历史。

## 6 可观测与人工干预

- `ivan jobs [list] [--json]`：列出作业（daemon 在走 socket，不在则直接读作业库）。
- `ivan jobs rm <id>`、`ivan jobs run <id>`：经 socket 让 daemon 执行（`run` 异步执行，结果看日志）。
- `ivan pause` / `ivan resume`：暂停/恢复调度（状态落 `state.json` 的 `paused`；daemon 在走 socket，不在则直接改状态文件）。
- `ivan status` 的 `schedule` 行：作业数/启用数/在跑数/暂停态/触发次数/失败数/错过数/当日 tokens/下次触发/最近错误。
- 事件日志：作业触发、完成、失败、错过、暂停跳过、库错误都落 `events/<date>.jsonl`（`job_trigger`/`job_done`/`job_failed`/`job_missed`/`job_paused`/`job_store` 等），人类日志同步一行。

## 7 验收

1. 新增 `every` 作业 → 按时触发 → 结果发回指定会话；`job state` 更新。
2. 重启 daemon → 作业仍在、`next_run_at_ms` 正确重算；停止期间的 `at` 作业按策略补跑。
3. 模型在 QQ 里说"10 分钟后提醒我喝水" → 自动建作业 → 到点收到 QQ 消息。
4. 长作业超过周期 → 跳过而非堆积，日志可见。

状态：1/2/4 已由单测与 daemon 端到端覆盖（§8.5）；**3 已真机通过**（2026-10-20，生产凭证）：单聊说"1 分钟后提醒我喝水" → 模型建 `at` 作业（目标继承 `qq:c2c:<openid>`）→ 到点触发 → 结果以主动消息回投原单聊（`qq sent=2 send_errors=0`），`at` 作业默认执行后自删。

## 8 P3 实现记录（2026-10-20）

代码：`schedule/`（模型/存储抽象/调度循环，**只依赖标准库**）、`schedule/sqlite`（存储实现，唯一引入 sqlite 驱动的地方）、`runner/scheduletool.go`（工具）、`runner` 的 `TriggerJob`/`DispatchTurn`、`daemon` 接线、`commands.go` 的 `jobs`/`pause`/`resume`；测试：`schedule`（假时钟 + 假派发）、`schedule/sqlite`（CRUD/重启/损坏/版本过新/并发）、`runner`（工具与回投）、`daemon`（端到端 + 模型建作业 + 重启恢复）。

### 8.1 关键决策（与原规划的差异）

| # | 议题 | 结论 |
|---|---|---|
| 1 | 存储形态 | 原规划 `jobs.json` 原子写 → 改为**存储抽象接口 + sqlite 实现**（用户拍板：文件存储不可靠，抽象层先落 sqlite）。接口窄（List/Get/Put/Delete/Close）、实现须并发安全，进程内 `NewMemStore` 供测试 |
| 2 | 触发路径 | 原规划"投 Bus" → 改为 **runner 同步直调**（`Runner.TriggerJob`）。理由：Bus 是单向非阻塞流，拿不到回合结果与 token，`last_status`/预算都会不可信 |
| 3 | 回投路由 | `bus.InboundMessage` 增 `ReplyChannel`；非空即"直投"（`Manager.Route` + 错误回报），避免作业结果在 Bus 里丢失来源信息 |
| 4 | 目标会话 | 工具不暴露 `channel`/`chat_id`/`session_key`，只能继承当前实例的会话目标（防伪造、语义正确） |
| 5 | 作业创建入口 | 只由模型经工具创建（`ivan jobs` 只 list/rm/run），避免人与模型两套写入路径 |
| 6 | 配额 | `schedule.max_tokens_per_day` 默认 **0（不限）**；统计范围仅作业回合，进程内按 UTC 日累计（重启清零，跨重启的全局预算留给 P4 的统一预算层） |
| 7 | 暂停开关 | `data.State.Paused`（`state.json`）+ `ivan pause/resume`；暂停时 `at` 保留待恢复、`every` 照常推进（不堆积） |

### 8.2 调度细节

- `plan()` 每次从存储重读全量作业（个人量级：几十条），按 `next_run_at_ms` 取最近到期；在跑作业不参与取时（完成时 `Wake`）。
- `every` 推进：`nextEvery(planned, every, now)`；运行结束用"计划时间 + every"推进，落后则连续跳并记 `missed`；启动补偿用 `skipEvery(planned, every, now)`（不补跑）。
- 暂停/预算阻塞时 `at` 保留、`every` 用 `skipEvery` 跳到未来，并向事件日志各报一次（同一作业同一原因不重复刷）。
- `RunNow` 标记 in-flight 后同步执行，完成后回写 `runs/last_*` 但**不动**未到期的 `next_run_at_ms`（`at` 也不消耗）。
- 停机：`Run` 收到 ctx 取消后等执行中作业收尾（上限 5s），daemon 侧再走原有收尾顺序。

### 8.3 存储实现要点

- `schedule/sqlite.Open(path)`：建目录 0700、URI 参数 `journal_mode(WAL)`/`synchronous(FULL)`/`busy_timeout(5000)`、`MaxOpenConns(1)` 串行写。
- schema v1：`jobs` 表 22 列 + `(enabled, next_run_at_ms)` 索引；`PRAGMA user_version` 迁移；`ErrSchemaTooNew` 拒降级。
- CRUD 一律 upsert（`on conflict(id) do update`），单次操作 5s 超时；`Job` 全字段往返有单测。
- 损坏文件：`pragma user_version` 阶段即报错，原文件不被改写（单测断言字节不变）。

### 8.4 接线与命令

- `config.schedule`：`enabled`/`jobs_db`/`missed_policy`/`job_timeout_seconds`/`max_tokens_per_day`；`IVAN_*` 覆盖沿用。
- daemon：打开作业库 → 装配 scheduler（`Dispatcher=runner.TriggerJob`、`Paused=state.Paused`、`Eventf` 落人类日志 + 事件日志）→ `runner.SetScheduler` 让工具可用；打不开库则降级（`schedule_error` 可见）。
- `channel/cli` 控制指令签名改为 `(op, arg)`，新增 `jobs/list`、`jobs/remove`、`jobs/run`、`pause`、`resume`；`cli.Call(path, op, arg, timeout)` 通用客户端。

### 8.5 测试覆盖

- `schedule`：到期触发一次并推进、长作业不重入 + 记错过、一次性作业自删、`missed_policy` run/skip、启动补偿、暂停保留 `at`、预算阻塞并推进 `every`、失败/通知失败落状态、`RunNow` 不改计划、Update/Remove、`running` → `interrupted`。
- `schedule/sqlite`：全字段往返、重开持久化、损坏文件显式报错且原文件不变、版本过新拒绝、8 并发读写（`-race`）。
- `daemon`：播种作业 → 到点触发 → `last_status=ok` + 下次推进 + `unmatched` 计数 + `status.schedule` + `jobs/list` 控制指令 + 暂停/恢复；停机重启后作业恢复；**模型经 `schedule` 工具建 `at` 作业 → 到点触发 → 结果回灌**（假 LLM）；`schedule.enabled=false` 时明确报错。
- 真机（生产 QQ 凭证，临时数据目录）：单聊"1 分钟后提醒我喝水" → 模型建作业 → `job_trigger`/`turn_start channel=schedule`/`job_done runs=1 tokens=3053` → 主动消息回投成功（`qq sent=2 send_errors=0`），用户侧收到提醒；验完 `SIGTERM` 停机并清理临时目录。
- `runner`：工具继承当前会话目标、参数校验、`TriggerJob` 直投 `ReplyChannel` 且不回 Bus、通知失败不影响执行判定。
