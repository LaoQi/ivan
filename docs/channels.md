# 通道与消息总线

> 状态：规划。目标是把**所有**输入输出收敛到 channel，IM 只是其中一个实现。

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

两种形态，共用同一套逻辑：

- 前台：`ivan run "prompt"`，进程内直接构造 inbound 消息并跑一轮，结果打到 stdout（不起 daemon）。
- 后台：`ivan send "prompt"` 通过 `data/ivan.sock` 投递给 daemon；daemon 用 `cli` 通道回发结果。

socket 协议：**JSON Lines**，一行一帧，字段 `{id, op, text, session}`；daemon 回 `{id, op:"reply", text}` 或 `{id, op:"error", text}`，并提供 `status`/`jobs` 等只读 op。协议极小、可 `nc -U` 手工调试。

### log

只出不进：把回合、工具调用、错误写成 `events/<date>.jsonl`，同时提供人类可读的 `logs/ivan.log`（经 `ivan logs -f` 查看）。它是"无人值守时唯一的眼睛"。

### schedule / heartbeat（触发源，不是传统通道）

调度器产生的消息以 `Channel: "schedule"` 注入 Bus，但 **`ChatID`/`SessionKey` 指向目标会话**，出站时由 Manager 改投到真实通道（QQ/cli）。心跳（后置）同构，只是触发者是无人在场时的定时自省。

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

- P1：`cli` + `log` 两通道跑通；daemon 启停、socket 往返、丢弃计数可见。
- P2：`qq` 通道真机双向；长消息切分正确；断线自动重连且不丢消息（去重生效）。

## 9 出站策略（被动 vs 主动）

- 通道自己决定用被动还是主动：QQ 的被动回复需带 `msg_id`（单聊 60 分钟/4 次、群聊 5 分钟/5 次），超窗或非回复场景走主动消息（频控与日上限见 `docs/qq-bot.md`）。
- Manager 只做路由与切分，不感知平台窗口；通道把"被动失败→主动重试→彻底失败"的结果回报给 Manager 计数。
- 出站消息带 `ReplyTo`（来源消息 id）时应优先试被动；定时/告警类消息（无来源）直接走主动。
- 通知类消息 best-effort：失败只记事件与计数，不重试到阻塞，不影响回合结算。
