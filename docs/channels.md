# 通道与消息总线

> 状态：**P1 已实现**（`bus`/`channel`/`channel/cli`/`channel/log` + daemon 接线）。目标是把**所有**输入输出收敛到 channel，IM 只是其中一个实现；实现记录见 §10。

## 1 原则与术语

三种东西分开，不硬套同一接口：

| 概念 | 含义 | 本期实现 |
|---|---|---|
| `Channel` | 能收能发的**外部端点** | `qq`、`cli`、`webhook`（后置） |
| `Trigger` | 内部**触发源**，只产出 inbound 消息 | `schedule`、`heartbeat`（后置） |
| `Sink` | 只出的**观测出口** | 事件 jsonl、人类可读日志 |

原则：

1. 外部输入输出一律经 `Channel`；触发源一律实现为 `Trigger` 并产出 `InboundMessage`；观测一律走 `Sink`。
2. 通道之间不认识彼此，也不认识业务；它们只认识 `bus` 的消息结构。
3. 通道不持有 agent、不解析指令、不做会话路由（那是 runner 的事）。
4. 新增通道 = 新增一个包 + 注册表登记一行；不改 runner。
5. **出站投递按配置 fan-out**：配置里启用了哪些出站通道，通知就发到哪些通道（`qq`/`cli`/…可同时启用），任一通道失败不影响其余；**不保证投达**（平台限制见 §6.1），失败计数在 `ivan status` 可见。

## 2 消息模型

```go
type InboundMessage struct {
    Channel   string    // "qq" | "cli" | "schedule" | "webhook" | "heartbeat"
    Account   string    // 同一通道多账号时区分（首期空）
    ChatID    string    // 通道内的会话标识：qq 为 openid / group_openid
    ChatType  string    // "direct" | "group"
    SenderID  string    // 发送者标识（作业触发时为 job creator）
    SenderName string
    MessageID string    // 平台消息 id（QQ 被动回复需要）
    Content   string
    Media     []MediaRef // 图片等附件引用（本地缓存路径 + mime）
    Mentioned bool       // 群里是否 @ 了机器人
    SessionKey string    // 由通道或 runner 归一：见 §4
    ReplyChannel string  // 回投通道（触发源专用：非空表示出站直投该通道而非来源通道）
    ReceivedAt time.Time
    Raw       map[string]string // 平台原始字段（调试与被动回复用，如 msg_id）
}

type OutboundMessage struct {
    Channel  string
    ChatID   string
    ReplyTo  string      // 被动回复用的平台消息 id（QQ 需要）
    Content  string
    Media    []MediaRef
    Kind     string      // "reply" | "notice" | "error"
}
```

设计取舍：**保留平台需要的少量原始字段**（`MessageID`/`Raw`），因为 QQ 的被动回复必须带 `msg_id` 与自增 `msg_seq`；但通道的业务语义仍统一在公共字段上。复杂平台特性（编辑消息、流式、typing）不做成核心接口，需要时用可选接口扩展（参照 picoclaw 的能力接口做法）。

## 3 Bus 与 Manager

```
Channel ──inbound──▶ Bus.Inbound ──▶ Runner（会话路由 + 回合）
Runner ──outbound──▶ Bus.Outbound ──▶ Manager ──▶ Channel.Send
```

- `Bus` 是两条带缓冲的 channel（默认容量 64），投递**非阻塞**：满了按策略丢并计数（`DroppedTotal`/`LastDropAt`），暴露到 `ivan status`。绝不因为某个通道慢而拖住 agent 回合。
- `Manager` 负责：`Start/Stop` 全部通道、按 `Outbound.Channel` 路由到对应通道、长消息切分（见 §6）、发送失败重试与上报、通道运行状态汇总。
- 关闭顺序：先停入站（不再收新消息）→ 等在跑的回合收尾 → 刷出站队列 → 停通道。

## 4 会话键（SessionKey）

由 runner 归一，规则固定、可读、可持久化：

| 场景 | SessionKey |
|---|---|
| QQ 单聊 | `qq:c2c:<user_openid>` |
| QQ 群 @ | `qq:group:<group_openid>` |
| 本地 CLI | `cli:default` |
| 定时作业 | 默认继承创建者的 SessionKey；可用 `--session` 或作业字段覆盖 |
| webhook | `webhook:<name>` |

同一 SessionKey 的对话共享一个 agent 会话（历史连续），这是"跟机器人聊天像跟一个人聊"的基础。作业触发用创建者的 SessionKey，意味着定时任务的结果自然接在原对话里。

## 5 内置通道

### cli

两种形态，共用同一套会话与回合路径：

- 前台：`ivan run [--session=键] "prompt"`，进程内直接构造 inbound 消息并跑一轮，**不走总线**、结果流式打到 stdout（不起 daemon）。
- 后台：`ivan send [--session=键] [--timeout=秒] [--no-wait] "prompt"` 经 `<data_dir>/ivan.sock` 投给 daemon，daemon 用 `cli` 通道把回复写回同一连接。

socket 协议：**JSON Lines**，一行一帧（单帧上限 1 MiB），字段 `{id, op, text, session, data, error}`。

| 方向 | op | 说明 |
|---|---|---|
| 客户端→daemon | `send` | `text` 必填；`session` 为空取 `cli:default`，无 `:` 前缀的短名补 `cli:` |
| | `status` | 读取 daemon 状态（`data` 为 `daemon.Status` JSON） |
| | `ping` | 连通性探测 |
| daemon→客户端 | `ack` | 已受理并入总线 |
| | `reply` / `error` | 回合结果（`error` 亦用于入站被丢弃、帧解析失败等） |
| | `pong` / `status` | 对应 `ping` / `status` |

`ChatID` 即请求 id：出站按 `ChatID` 找连接，发出一次回复后即释放该请求（`--no-wait` 时连接已关，计 `unmatched` 而非失败）。协议极小，可 `nc -U` 手工调试。

### log

只出不进，由**两个 Sink** 组成（**不建 `channel/log` 之外的通道语义**，且不经总线）：

- 结构化：`data.EventLog` → `events/<date>.jsonl`（`kind`/`tool`/`usage`/`dur_ms`，带缓冲 + 丢弃计数）。
- 人类可读：`channel/log.Sink`（stdlib + `io.Writer`）→ `logs/ivan.log`，一行一条 `时间 kind 字段=值`（工具调用、回合结算、错误、停机）。

它是"无人值守时唯一的眼睛"：`ivan status` 里的 `events.written/dropped` 与 `logs.lines/errors` 就是它的健康度。

### schedule / heartbeat（触发源，不是传统通道）

调度器产生的消息以 `Channel: "schedule"` 标记来源，`ChatID`/`SessionKey` 指向目标会话，`ReplyChannel` 记真实回投通道（P3 起）。**它不经 Bus**：`runner.TriggerJob` 同步执行，出站按 `ReplyChannel` 直投 `Manager.Route` 以便拿到投递错误（见 `docs/schedule.md` §8.1）。心跳（后置）同构，只是触发者是无人在场时的定时自省。

### webhook（后置）

本地 HTTP 端口收事件，`Channel: "webhook"`，入站可带签名校验；出站需要在配置里声明其可回投的目标通道。

## 6 长消息与出站规则

- 出站前统一：去控制字符、（可选）markdown → 纯文本、按通道上限切分（每通道声明 `MaxLen`，默认 1200 字符，QQ 实施时按官方上限核对）。
- 切分按语义边界（段落 > 行 > 句 > 硬切），并保证不把代码块切得七零八落（首期简单实现：优先空行，其次换行）。
- 被动回复窗口：QQ 平台限制（同一 `msg_id` 限次数与时限）由 `channel/qq` 自己管 `msg_seq` 与回退到主动消息；Manager 不感知平台细节。

## 7 入站安全

- 通道级白名单：配置里可指定允许的 `SenderID`（QQ 的 openid）或群，空表示不限（首期默认收紧：只允许配置中的账号）。
- 去重：按 `MessageID` 做 TTL 缓存（重复投递丢弃），QQ 侧必需。
- 长度与速率限制：单条入站最大长度、每会话每分钟上限，超限丢弃并计数。

## 8 验收

- P1（**已达成**）：`cli` + `log` 两通道跑通；daemon 启停、socket 往返、丢弃计数可见；单实例锁与优雅停机顺序落地（见 §10）。
- P2：`qq` 通道真机双向；长消息切分正确；断线自动重连且不丢消息（去重生效）。

## 9 出站策略（被动 vs 主动）

- 通道自己决定用被动还是主动：QQ 的被动回复需带 `msg_id`（单聊 60 分钟/4 次、群聊 5 分钟/5 次），超窗或非回复场景走主动消息（频控与日上限见 `docs/qq-bot.md`）。
- Manager 只做路由与切分，不感知平台窗口；通道把"被动失败→主动重试→彻底失败"的结果回报给 Manager 计数。
- 出站消息带 `ReplyTo`（来源消息 id）时应优先试被动；定时/告警类消息（无来源）直接走主动。
- 通知类消息 best-effort：失败只记事件与计数，不重试到阻塞，不影响回合结算。

## 10 P1 实现记录（2026-10-10）

代码：`bus/`（零依赖）、`channel/`、`channel/cli/`、`channel/log/`、`daemon/`；测试：`bus`/`channel`/`channel/cli`/`channel/log` 单测 + `daemon` 端到端（假 LLM + 真 socket，覆盖 send 往返、status、重启续聊、锁互斥、损坏状态拒绝启动）。

### 10.1 bus

```go
type Bus struct{ ... }                          // 两条带缓冲 chan（默认 64，可配）
func New(inCap, outCap int) *Bus
func (b *Bus) Inbound() <-chan InboundMessage   // 消费者侧只读
func (b *Bus) Outbound() <-chan OutboundMessage
func (b *Bus) Publish(InboundMessage) bool      // 非阻塞；满/已关闭 → false 并计数
func (b *Bus) Post(OutboundMessage) bool
func (b *Bus) CloseInbound() / CloseOutbound()  // 先投入口、后投出口，顺序即停机顺序
func (b *Bus) Stats() Counters                  // 计数 + 队列深度 + 最后丢弃时间
```

- 会话键归一：`bus.SessionKey(msg)`（显式 `SessionKey` 优先 → `qq:c2c|group:<id>` → `cli:default` → `<channel>:<chatID>`）；目录名：`bus.KeySlug(key)`（非法字符转 `-`，超 64 字符追加摘要防碰撞）。放 `bus` 是为了保持纯函数可测、无 ivan 依赖。
- 关闭语义用互斥量保护"关闭后再投递"的竞态；关闭后的投递计入 `*Dropped`，不会 panic。

### 10.2 channel

- `Channel` 接口 = `Name/Start/Stop/Send`；可选 `Limiter{MaxLen()}`。
- `Manager`：注册（重名报错）→ 顺序 `Start`（失败回滚已启动者）→ `Consume`（唯一出站消费者，出站流关闭或 ctx 结束即返回）→ 逆序 `Stop`。
- 出站处理：`Sanitize`（去控制字符）→ `Split`（段落 > 行 > 句 > 硬切，上限取通道 `MaxLen`，默认 1200；`<=0` 不切分）→ 发送失败**重试 1 次**（默认 200ms 间隔）。
- 计数：`sent` / `failed`（含 `last_error`）/ `unmatched`（`ErrUnroutable`：无目标，不重试）。`Status()` 供 `ivan status` 渲染。

### 10.3 daemon 停机顺序

`SIGTERM/SIGINT` → 停止收新入站（关入站流）→ 等在跑回合收尾（`shutdown_timeout_seconds`，超时取消）→ 关出站流并等 `Manager` 刷完 → 逆序停通道（清 socket）→ 关事件日志与人类日志 → 释放 flock。
