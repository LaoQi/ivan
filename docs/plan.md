# ivan 规划总纲

> 状态：**规划阶段**（未开工）。本文件是权威总纲，实现细节见各分册；现行约束另见根 `AGENTS.md`。

## 1 定位

ivan 是常驻的极简 agent：**特化在定时触发与计划任务**，全部输入输出收敛到 channel 抽象（IM 只是其中一个 channel）。对标 claw 一类"常驻个人代理"的形态，但只取三条线：通道、调度、会话；不做 Web UI、不做多厂商花活、不做 skills/MCP 生态。

与 tanya 相对：tanya 是人在终端里对话的前台工具，ivan 是无人值守的后台代理；一人在场、一机器在场。

## 2 目标与非目标

目标

1. 常驻单进程，消息可来自 QQ 机器人、本地 CLI、定时作业；答复回到来源渠道。
2. 模型可自建计划任务（`schedule` 工具），作业持久化、重启恢复、结果回灌原会话。
3. 长任务跨会话自主推进：复用 tanya 的交接机制，无人干预也能续跑，并受预算与暂停开关约束。
4. 复用 tanya 的 agent 核心，不复制代码；需要的能力缺口在 tanya 侧小步补齐，两仓库同步推进。

非目标（本期不做）

- Web UI / 桌面 UI / Canvas 之类可视化
- 多渠道全家桶（首期只做 QQ 一个 IM 通道 + 本地 CLI）
- 语音、视频、文件等富媒体（首期只做文本 + 图片）
- 插件市场、skills、MCP
- 多用户多租户、权限体系、云端部署

## 3 与 tanya 的关系

- 共用 tanya 的 `agent`（LLM 双协议、回合循环、工具注册、会话存储、交接、统计）与 `tools`（`run_shell`/`read_image`/内建）。
- 开发期用 `go.work` 把 `../tanya` 与 `../ivan` 放进同一 workspace；发布按 tag。
- **不复用** `repl`/`readline`/`ctty` 的终端所有权（常驻无 TTY 且独占终端语义会反向绑架架构）；仅按需复用 `ctty` 的零依赖原语（信号/`IsRoot`/UTF-8）与 `render/markdown`。
- 复用清单、探针与需 tanya 侧配合的改动：`docs/reuse-tanya.md`。
- **后续抽库**：产生第二个复用者（如 QQ 协议、通道抽象、调度器）后，把公共模块抽成独立 module 再引入；抽库边界与判据见 §5 与 `docs/qq-bot.md` §8。

## 4 总体架构

```
Channel 层    qq / cli / log —— 唯一的输入输出面（IM、本地、日志）
   ↕  MessageBus（inbound / outbound 两条流，带缓冲、非阻塞投递）
Runner 层     会话键 → agent 实例；入站消息 → 回合；事件 → 出站消息 / 事件日志
   ↕
Core 层       tanya 的 agent（LLM/回合/工具/会话/交接）+ ivan 特化工具
Schedule 层   作业存储 + 调度循环 → 以 inbound 消息注入（channel 指向目标渠道）
```

一切输入都是 inbound 消息：QQ 用户消息、CLI 命令、定时作业触发、（后置）webhook 与心跳。区别只在 `Channel` 字段，之后走同一条路径：会话键路由 → agent 回合 → 出站消息回到来源渠道。"IM 只是一个 channel"即由此落地。

事件流（`agent.Event`）在 runner 侧被消费：文本增量聚合为出站消息，工具/回合事件落事件日志，回合结束触发通知。

## 5 模块与目录

```
main.go            CLI：run(前台单发) / send / daemon / status / jobs / logs / config / version
config/            内嵌 agent.Config + channels + schedule + daemon + policy 段（yaml + IVAN_*）
bus/               InboundMessage / OutboundMessage / Bus（带缓冲、非阻塞、可观测丢弃计数）
channel/           Channel 接口 + Manager + 注册表；子包 cli/ log/ qq/
schedule/          Job 模型 / 存储 / 调度循环 / 触发投递；配套 schedule 工具
runner/            会话键 → *agent.Agent 池、回合驱动、事件 → bus、交接自动续跑
data/              作业/事件/状态/会话索引的原子落盘（jsonl + flock 单实例锁）
prompt/            system 提示词基座 + 环境段（动态内容走 user 消息，保 prompt cache 不变性）
qqbot/             QQ 开放平台协议实现（token / gateway / websocket / 开放 API / 事件类型）
```

**抽库候选**（隔离规则：这些包只依赖标准库与自身类型，不得依赖 ivan 其他包与 tanya）

| 候选 | 依赖纪律 | 抽库形态 |
|---|---|---|
| `qqbot/` | 纯协议，零 ivan 依赖，只暴露自身类型与回调 | `github.com/LaoQi/qqbot` |
| `bus/` | 纯消息结构 + 通道，零依赖 | 随通道抽象一并抽 |
| `channel/` | 仅依赖 `bus` | 随通道抽象一并抽 |
| `schedule/` | 仅依赖标准库 + 自身 Job 类型 | `github.com/LaoQi/schedule`（视复用情况） |

`channel/qq/` 是适配器：把 `qqbot/` 的协议事件翻译成 `bus.InboundMessage`，把 `bus.OutboundMessage` 翻回协议调用。**协议与业务的分界固定在 `qqbot/` 边界上**，抽库时 ivan 只改 import 路径。

## 6 数据布局

```
<data_dir>/                       默认 ~/.local/share/ivan
  ivan.sock                       daemon 的 unix socket（cli 通道）
  state.json                      运行状态：会话映射、暂停开关、预算计数
  jobs.json                       计划任务（原子写）
  events/<date>.jsonl             结构化事件日志（回合/工具/错误）
  logs/ivan.log                   进程日志
  media/                          收发媒体缓存（图片）
  inbox/                          （后置）文件收件箱
  workspaces/<ws-id>/sessions/    tanya 格式会话文件（由 agent 管理，可被 tanya 打开）
```

会话文件沿用 tanya 的 jsonl 格式与目录约定，保证同一工作区里 tanya 与 ivan 的会话可互相看见、继续。

## 7 运行形态

- `ivan daemon`：常驻前台进程；systemd user unit 托管（示例随 P5 交付）；单实例锁（`data/` 下 flock，第二实例明确报错退出）。
- `ivan run "prompt"`：前台单发（不起 daemon，直接跑一个回合），调试与无 QQ 时的主入口。
- `ivan send "..."`：投递给运行中的 daemon（走 `cli` 通道），由 daemon 异步处理与回复。
- `ivan status` / `jobs` / `logs`：只读查询（socket 或直接读文件）。
- 信号：SIGTERM/SIGHUP 优雅停机（等在跑的回合收尾或按配置取消），退出码由主入口统一给出。

## 8 阶段路线

| 阶段 | 内容 | 验收 |
|---|---|---|
| P0 | 复用探针 + 仓库骨架：go.work、config、`ivan run` 前台单发 + 事件日志 | 无 TTY 下 `run_shell` 正常、`interactive` 明确报错不卡、交接自动续跑、`LoadSession` 恢复、多实例并发；产出 `docs/reuse-probe.md` |
| P1 | `bus` + `channel` 抽象 + `cli`/`log` 通道 + daemon（socket、单实例锁、优雅停机、status） | 两个通道跑通；daemon 重启后状态可查、会话可续 |
| P2 | `qqbot/` 协议包 + `channel/qq` 适配器：token/gateway/ws/收发/切分/去重/被动回复/图片 | 协议单测 + 假网关集成测试通过；沙箱机器人真机互通（凭证实施时提供） |
| P3 | 定时与计划任务：Job 存储、调度循环、`schedule` 工具、结果回原渠道 | 重启后作业恢复；模型自建任务按时触发并把结果发回会话 |
| P4 | 自主续跑与记忆：交接自动续跑、上下文阈值自动交接、记忆/目标文件、预算熔断、暂停开关 | 无人干预跨多会话推进一个多步目标，且被预算/暂停约束 |
| P5 | 安全与运维：审批档、危险命令规则、限额、日志轮转、systemd unit、`ivan doctor` | 危险命令被拦；服务随 systemd 起停 |

## 9 决策记录

| # | 议题 | 结论 |
|---|---|---|
| D1 | 定位 | 精简版 claw，特化定时触发与计划任务；不参考 openclaw |
| D2 | 通道 | IM 只是一个 channel；所有输入输出抽象到 channel；首期只实现 QQ 官方机器人通道（非 OneBot） |
| D3 | QQ 协议实现 | **自实现**，不使用 `tencent-connect/botgo`（长期未更新、实现质量差）；先落在 ivan 内 `qqbot/`，稳定后抽成独立 module 引入 |
| D4 | 首期消息形态 | 仅文本 + 图片 |
| D5 | 定时表达式 | `at`/`every` 先做，`cron_expr` 视需要再规划 |
| D6 | 与 tanya 关系 | 共用 agent 核心，两仓库同步推进；公共模块后续按 §5 判据抽库 |
| D7 | 触发入口 | QQ 入站、计划任务、本地 CLI/socket 为必做；webhook、心跳为后置可选 |

## 10 风险与红线

- **成本失控**：自主续跑 + 定时任务会持续烧 token。预算熔断与暂停开关属于 P4 的硬性交付项，不得后置；建议 P3 前先落最小限额。
- **静默失败**：无人值守时错误容易无人知晓。事件日志必须落盘，`task/job` 失败要有状态与通知出口。
- **无人审批执行 shell**：默认 `auto` 档风险高，P5 提供 `guard` 档；危险命令规则先以最小黑名单起步。
- **协议漂移**：QQ 开放平台接口可能变动，客户端需要可观测的协议错误（日志 + `ivan doctor`）。
- **队列/作业文件损坏**：一律原子写（临时文件 + rename）；解析失败时保留原文件并显式报错，不静默重建。
- **不越界**：部署态只做只读探测；功能验证在本地或临时环境完成。

## 11 文档索引

- `docs/plan.md`（本文件）总纲与路线
- `docs/reuse-tanya.md` 复用清单、P0 探针与需 tanya 配合的改动
- `docs/channels.md` 通道抽象、Bus 与消息模型、cli 通道
- `docs/qq-bot.md` QQ 官方机器人协议设计、实现分层与抽库路径
- `docs/schedule.md` 定时与计划任务设计
- `docs/sessions.md` 会话模型、agent 实例池与推进策略
- `docs/refs-picoclaw.md` 参考调研快照与取舍
- 根 `AGENTS.md` 现行约束与结构；`CHANGELOG.md` 变更记录
