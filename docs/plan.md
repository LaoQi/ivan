# ivan 规划总纲

> 状态：**P0–P4 已完成**（P0 骨架与复用探针、P1 bus/channel/本地通道/daemon、P2 `qqbot/` + `channel/qq` 真机全通、P3 定时与计划任务、P4 受控推进）；**P5（安全与运维）降级为保留计划，暂无实施规划**（见 §8）。P4 遗留的崩溃回合标记、记忆裁剪已补齐（`docs/sessions.md` §10.7）。本文件是权威总纲，实现细节见各分册；现行约束另见根 `AGENTS.md`。

## 1 定位

ivan 是常驻的极简 agent：**特化在定时触发与计划任务**，全部输入输出收敛到 channel 抽象（IM 只是其中一个 channel）。对标 claw 一类"常驻个人代理"的形态，但只取三条线：通道、调度、会话；不做 Web UI、不做多厂商花活、不做 skills/MCP 生态。

与 tanya 相对：tanya 是人在终端里对话的前台工具，ivan 是无人值守的后台代理；一人在场、一机器在场。

## 2 目标与非目标

目标

1. 常驻单进程，消息可来自 QQ 机器人、本地 CLI、定时作业；答复回到来源渠道。
2. 模型可自建计划任务（`schedule` 工具），作业持久化、重启恢复、结果回灌原会话（**通知 best-effort**，见红线）。
3. 长任务跨会话推进：借用 tanya 的交接机制 + 自建护栏（默认关、仅指定工作区、轮次与预算硬上限、结果待人复核）——不承诺无人值守完成开放目标。
4. 复用 tanya 的 agent 核心，不复制代码；需要的能力缺口在 tanya 侧小步补齐，两仓库同步推进。
5. 通知按配置 fan-out：启用哪些出站通道就发到哪些通道，任一通道失败不影响其余，不做必达承诺。

非目标（本期不做）

- Web UI / 桌面 UI / Canvas 之类可视化
- 多渠道全家桶（首期只做 QQ 一个 IM 通道 + 本地 CLI）
- 语音、视频、文件等富媒体（首期只做文本 + 图片）
- 插件市场、skills、MCP
- 多用户多租户、权限体系、云端部署
- **subagent / 多 agent 会话编排**（父子任务派生、子任务隔离运行、层级预算、结果回传）：**2026-10-30 明确废弃**。
  常驻代理的真实需求（外部请求、定时作业、长任务推进）全部落在「单一线性会话 + 定时触发 + 交接续跑」上，
  派生并行子 agent 不解决任何已提出的问题，却要额外引入父子模型、静默出站、层级预算、级联取消四套机制。
  需要并行时用**各自独立的会话键**（不同来源派生不同键即可），不做从属关系。理由见 D12。

## 3 与核心库、tanya 的关系

- 共用核心库 `github.com/LaoQi/battlelab` 的 `agent`（LLM 双协议、回合循环、工具注册、会话存储、交接、统计）、`tools`（`run_shell`/`read_image`/内建）与 `ctty`（零依赖终端原语：信号/`IsRoot`/UTF-8）。三包 2026-10-22 从 tanya 抽出，**ivan 已不直接依赖 tanya**（仅运行期借用其配置文件里的凭证）。
- 开发期用 `go.work` 把 `../battlelab` 与 `../ivan` 放进同一 workspace；发布按 tag。
- **不复用** tanya 的 `repl`/`readline`/`views` 与任何终端所有权（常驻无 TTY 且独占终端语义会反向绑架架构）；`render/markdown` 按需、仍在 tanya 仓。
- 复用清单、探针与需库侧配合的改动：`docs/reuse-core.md`。
- **后续抽库**：产生第二个复用者（如 QQ 协议、通道抽象、调度器）后，把公共模块抽成独立 module 再引入；抽库边界与判据见 §5 与 `docs/qq-bot.md` §8。

## 4 总体架构

```
Channel 层    qq / cli —— 外部端点
Trigger 层    schedule / http（外部入口：通知转发 + agent 请求）/（后置）heartbeat —— 产出 inbound 消息或直投出站
Sink 层       events jsonl + 人类可读日志 —— 只出
   ↕  MessageBus（inbound / outbound 两条流，带缓冲、非阻塞投递）
Runner 层     会话键 → agent 实例；入站消息 → 回合；事件 → 出站消息 / 事件日志
   ↕
Core 层       tanya 的 agent（LLM/回合/工具/会话/交接）+ ivan 特化工具
```

一切输入都是 inbound 消息：QQ 用户消息、CLI 命令、定时作业触发、外部服务的 HTTP 请求（`/request`）、（后置）心跳；外部通知（`/notify`）只出站、不经总线。区别只在来源字段，之后走同一条路径：会话键路由 → agent 回合 → 出站消息按配置 fan-out 回目标通道。"IM 只是一个 channel"即由此落地。

事件流（`agent.Event`）在 runner 侧被消费：文本增量聚合为出站消息，工具/回合事件落事件日志，回合结束触发通知。

## 5 模块与目录

```
main.go            CLI：run(前台单发) / send / daemon / status / jobs / logs / config / version
config/            内嵌 battlelab agent.Config + channels + schedule + daemon + policy 段（yaml + IVAN_*）
bus/               InboundMessage / OutboundMessage / Bus（带缓冲、非阻塞、可观测丢弃计数）
channel/           Channel 接口 + Manager + 注册表；子包 cli/ log/ qq/
schedule/          Job 模型 / 存储抽象 + 进程内实现 / 调度循环（at|every|daily）
runner/            会话键 → *agent.Agent 池（battlelab）、回合驱动、事件 → bus、交接自动续跑
data/              作业/事件/状态/会话索引的原子落盘（jsonl + flock 单实例锁）
prompt/            system 提示词基座 + 环境段（动态内容走 user 消息，保 prompt cache 不变性）；全局说明读 ivan 自己的 ~/.config/ivan/AGENTS.md，与 tanya 隔离
qqbot/             QQ 开放平台协议实现（token / gateway / websocket / 开放 API / 事件类型）
schedule/sqlite/   作业库实现（modernc.org/sqlite，唯一引入 sqlite 驱动的包）
```

**抽库候选**（隔离规则：这些包只依赖标准库与自身类型，不得依赖 ivan 其他包与 battlelab/tanya）

| 候选 | 依赖纪律 | 抽库计划 |
|---|---|---|
| `qqbot/` | 纯协议，零 ivan 依赖，只暴露自身类型与回调 | **首个抽出对象**：P2 真机跑通、接口稳定后抽为 `github.com/LaoQi/qqbot` |
| `bus/` | 纯消息结构 + 通道，零依赖 | 暂留（只有一个消费者，抽库是纯成本） |
| `channel/` | 仅依赖 `bus` | 暂留，接口随 P1–P4 演进 |
| `schedule/` | 仅依赖标准库 + 自身 Job 类型（存储实现另放子包，驱动依赖不下沉） | 暂留（视是否出现第二消费者） |

`channel/qq/` 是适配器：把 `qqbot/` 的协议事件翻译成 `bus.InboundMessage`，把 `bus.OutboundMessage` 翻回协议调用。**协议与业务的分界固定在 `qqbot/` 边界上**，抽库时 ivan 只改 import 路径。

## 6 数据布局

```
<data_dir>/                       默认 ~/.local/share/ivan（与 tanya 分离，互不干扰）
  ivan.sock                       daemon 的 unix socket（cli 通道）
  state.json                      运行状态：会话映射、暂停开关、当日 token 用量、回合进行中标记（崩溃后据此判定 interrupted）
  memory/<key-slug>.md            每会话记忆（模型按需读、追加；路径进 system prompt）
  jobs.db                         计划任务（sqlite：WAL+FULL，schema 版本随程序迁移）
  events/<date>.jsonl             结构化事件日志（回合/工具/错误）
  logs/ivan.log                   进程日志
  media/                          收发媒体缓存（图片）
  workspaces/<ws-id>/sessions/    tanya 格式会话文件（由 agent 管理）
```

**数据目录与 tanya 分离**（决策）：各自的 `data_dir` 独立，避免归档/活跃会话统计互相干扰。会话文件仍是 tanya 的 jsonl 格式，需要互见时用配置显式指向同一目录 + 同一工作区路径（默认不开）。

## 7 运行形态

- `ivan daemon`：常驻前台进程；单实例锁（`data/` 下 flock，第二实例明确报错退出）；托管方式自定（systemd user unit 示例属 P5 保留计划，当前未提供）。
- `ivan run "prompt"`：前台单发（不起 daemon，直接跑一个回合），调试与无 QQ 时的主入口。
- `ivan send "..."`：投递给运行中的 daemon（走 `cli` 通道），由 daemon 异步处理与回复。
- `ivan status` / `jobs` / `logs`：只读查询（socket 或直接读文件）。
- 信号：SIGTERM/SIGHUP 优雅停机（等在跑的回合收尾或按配置取消），退出码由主入口统一给出。

## 8 阶段路线

| 阶段 | 内容 | 验收 |
|---|---|---|
| P0 | 复用探针 + 仓库骨架：go.work、config、`ivan run` 前台单发 + 事件日志 | 无 TTY 下 `run_shell` 正常、`interactive` 明确报错不卡、交接自动续跑、`LoadSession` 恢复、多实例并发；产出 `docs/reuse-probe.md` |
| P1 | `bus` + `channel` 抽象 + `cli`/`log` 通道 + daemon（socket、单实例锁、优雅停机、status） | ✅ 两个通道跑通；daemon 重启后状态可查、会话可续（`docs/channels.md` §10、`docs/sessions.md` §9） |
| P2 | `qqbot/` 协议包 + `channel/qq` 适配器：token/gateway/ws/收发/切分/去重/被动回复/图片 | ✅ 协议单测 + 假网关集成测试通过；生产凭证真机**单聊全链路已通**（入站→回合→回复），群 @/图片待验（`docs/qq-bot.md` §13） |
| P3 | 定时与计划任务：Job 存储（抽象 + sqlite）、调度循环、`schedule` 工具、结果回原渠道 | ✅ 重启后作业恢复；模型自建 `at`/`every` 作业按时触发并把结果发回原会话（本地端到端全绿 + **QQ 真机提醒通过**；`docs/schedule.md` §8） |
| P4 | 受控推进：交接自动续跑（**默认关**）、上下文阈值自动交接（建议 180K / 强制 256K 两档）、按需读取的记忆/目标文件、预算熔断、暂停开关 | ✅ 开启 `auto_continue` 后单条消息可跨会话推进，且被轮次/任务预算/日预算/暂停四处闸门约束；上下文达建议档提示交接、达强制档要求立即交接（不交接则停）（`docs/sessions.md` §10） |
| P5 | ⏸ **保留计划（暂无实施规划）**：审批档、危险命令规则、限额、日志轮转、systemd unit、`ivan doctor` | 未验收。P5 各子项当前**均未实施**；「危险命令被拦」「服务随 systemd 起停」不构成本阶段的现行承诺。重新启动时先补设计（审批在常驻无 TTY 场景下的交互路径是首要分歧点） |

## 9 决策记录

| # | 议题 | 结论 |
|---|---|---|
| D1 | 定位 | 精简版 claw，特化定时触发与计划任务；不参考 openclaw |
| D2 | 通道 | IM 只是一个 channel；所有输入输出抽象到 channel；首期只实现 QQ 官方机器人通道（非 OneBot） |
| D3 | QQ 协议实现 | **自实现**，不使用 `tencent-connect/botgo`（长期未更新、实现质量差）；先落在 ivan 内 `qqbot/`，稳定后抽成独立 module 引入 |
| D4 | 首期消息形态 | 仅文本 + 图片 |
| D5 | 定时表达式 | **修订（2026-10-21）**：`at`/`every`/`daily`（每天固定时刻 + 时区）已做，`daily` 覆盖"每日定时汇报/提醒"的实际需求；**完整 cron 表达式不做**（`expr` 字段保留但 `Validate` 报错），将来要"每周/每月"时按需扩展 `daily` 的周期维度，不引通用解析器 |
| D6 | 与核心库关系 | 共用 `battlelab` 的 `agent`/`tools`/`ctty`（2026-10-22 从 tanya 抽出），库改动与调用方 CHANGELOG 同步；公共模块后续按 §5 判据抽库 |
| D7 | 触发入口 | QQ 入站、计划任务、本地 CLI/socket 为必做；**修订（2026-10-23/24）**：HTTP 外部入口已做，拆成两个端点——`/notify` 纯转发通知（fan-out `http.targets`，不经总线、不计 token）与 `/request` 与 agent 沟通（**异步**：受理即返回会话键 `http:<source>`，结果经 `GET /request?session=` 轮询 runner 结果表，无回执管道），**不做模式开关**；心跳仍后置可选 |
| D11 | 外部入口的分层与目标权限 | 归 **Trigger 层**而非 `Channel` 层（`Channel` 只拿得到 Bus，出站拿不到投递错误，而通知必须能回执）；出站尽力而为广播、目标只由配置决定（请求体不得指定 channel/chat_id/route，`send_message` 也只广播全部 `http.targets`、不指定出口）；当前不做鉴权（仅内网，默认监听 `127.0.0.1`），不预留半成品字段 |
| D8 | 作业存储 | **存储抽象接口 + sqlite 实现**（`modernc.org/sqlite`，纯 Go 无 cgo）：文件级重写不够可靠，作业状态需要事务与并发安全；接口保持窄（List/Get/Put/Delete/Close），换实现不动调度器 |
| D9 | 预算与暂停口径 | 预算**只在 runner 一处**统计（`max_turns_per_task`/`max_tokens_per_task`/`max_tokens_per_day`，日量落 `state.json`），调度器通过 `Budget` 回调复用，不再自带计数器；`paused` 只挡无人值守路径（定时触发、自动续跑、阈值交接提示），用户消息照常处理 |
| D10 | 全局说明（AGENTS.md） | **机制保留、位置隔离**：库侧 `agent` 在 system prompt 末尾拼"全局说明 + 项目说明"，**默认不读任何全局文件**；ivan 经 `agent.WithGlobalAgentsPath` 读自己的 `~/.config/ivan/AGENTS.md`（`global_agents` 可改路径，`off` 禁用），避免 tanya 的偏好串入无人值守代理；项目说明仍读会话工作区下的 `AGENTS.md` |
| D12 | subagent / 多 agent 会话 | **明确不做（2026-10-30 废弃该路线）**。曾把「`/request` 回合内等用户批准」的缺失归因于「缺多 agent 会话/subagent 能力」，**该归因是错的**：审批真正缺的是**回合可挂起**（`runner` 的并发槽 `max_concurrent` 与会话实例锁在回合期间一直被占）与**答复可关联**（无待答复表、无 correlation），这两条都在 `runner` 自身，与有没有多 agent 无关。评估结论是：**「跑第二个 agent」现在就能跑**（`acquire` 按会话键懒建独立实例 + 独立工作区，探针 R5/R9/R10 已验证），差的不是能力而是调度语义；而 subagent 要补的父子模型 / 静默出站 / 层级预算 / 级联取消四套机制**不对应任何已提出的需求**，纯属自造复杂度。**结论**：不引入父子关系，需要并行就让来源派生不同会话键、彼此平等独立。审批类需求若将来重启，前置是修 `runner` 的并发槽语义与等待态（§10 已知缺陷），**不要再挂到 subagent 名下** |

## 10 风险与红线

- **成本失控**：自主续跑 + 定时任务会持续烧 token。预算熔断与暂停开关属于 P4 的硬性交付项，不得后置；建议 P3 前先落最小限额。
- **静默失败**：无人值守时错误容易无人知晓。事件日志必须落盘，`task/job` 失败要有状态与通知出口。
- **无人审批执行 shell**：默认 `auto` 档风险高；`guard` 档与危险命令黑名单属 P5 保留计划，**当前无拦截**，缓解手段只有「限定工作区 + 预算上限 + 暂停开关 + 事件审计」。
- **协议漂移**：QQ 开放平台接口可能变动，客户端需要可观测的协议错误（日志已有结构化分类与计数；`ivan doctor` 属 P5 保留计划，未实施）。
- **队列/作业文件损坏**：一律原子写（临时文件 + rename）；解析失败时保留原文件并显式报错，不静默重建。
- ~~**并发槽语义缺陷**~~（2026-10-30 **已修**，见 `docs/sessions.md` §11）：`Runner.Run` 曾在取到并发槽之前不按会话排队，同键串行靠 `DispatchTurn` 内部的实例锁——同一会话的第二条消息会先抢到并发槽再阻塞等锁，`max_concurrent=2` 时同会话连发两条即可占满两个槽、其他会话全部被堵。修法：每会话 FIFO 队列 + 单 worker，**轮到了取会话锁、最后才取并发槽**；顺序由队列保证而非 goroutine 抢锁。注意「回合内审批」仍**不做**（与 subagent 无关，见 D12），但并发槽已不再是它的阻塞点，剩下的阻塞是「回合可挂起」与「答复可关联」两块设计。
- **不越界**：部署态只做只读探测；功能验证在本地或临时环境完成。

## 11 文档索引

- `docs/plan.md`（本文件）总纲与路线
- `docs/reuse-core.md` 复用清单、P0 探针与需 tanya 配合的改动
- `docs/channels.md` 通道抽象、Bus 与消息模型、cli 通道
- `docs/qq-bot.md` QQ 官方机器人协议设计、实现分层与抽库路径
- `docs/schedule.md` 定时与计划任务设计与 P3 实现记录（§8）
- `docs/sessions.md` 会话模型、agent 实例池与推进策略
- `docs/notify.md` 外部入口（`/notify` 与 `/request`、`send_message`、`ivan notify`）
- `docs/refs-picoclaw.md` 参考调研快照与取舍
- 根 `AGENTS.md` 现行约束与结构；`CHANGELOG.md` 变更记录

## 附录 A 评估修订（2026-10-10）

对外部事实与口径的修订（依据官方文档实测 + 用户拍板）：

1. **QQ 被动过滤窗口纠正**：单聊 60 分钟 / 每条消息 4 次（原方案误写 5 分钟 / 5 条）；群聊 5 分钟 / 5 次。`msg_seq` 以入站消息 id 为键维护。
2. **主动消息可用但非必达**：QQ 主动消息有频控与日上限（单聊 1000 条/用户/天），且用户可关闭"允许主动发送" → 通知恒为 best-effort。
3. **通知 fan-out 可配**：启用哪些出站通道就发到哪些通道，互不影响，不保证投达。
4. **数据目录分离**：ivan 独立 `data_dir`，会话互见改为可选配置。
5. **抽库推迟到 P2 之后，且首抽只抽 `qqbot`**。
6. **受控推进**：自动续跑默认关、仅指定工作区、硬预算上限、结果待人复核。
7. **记忆按需读取**：记忆/目标文件路径写入提示词由模型自读，长期压缩依赖交接摘要，不做每轮全量注入。
8. **新增 ivan 侧配置**：交接阈值（P4 起为 `handoff_hint_tokens` 180K / `handoff_force_tokens` 256K 两档绝对值）、`auto_continue`、预算项。
9. **概念三分**：`Channel`（外部端点）/ `Trigger`（内部触发源）/ `Sink`（观测出口），不再把所有 IO 硬套通道接口。
10. **沙箱降级为可选增强**：安全主线是审批白/黑名单 + 预算 + 只读纪律。
