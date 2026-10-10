# 参考调研：picoclaw（快照 2026-10-10）

> 调研方式：浅克隆 `sipeed/picoclaw` 到 `/tmp/picoclaw-ref` 只读查看，未复制任何代码。结论用于 ivan 的通道/调度设计取舍。

## 结论

| 主题 | picoclaw 做法 | ivan 取舍 |
|---|---|---|
| 规模 | 27 万行，多 provider + Web UI + skills + MCP + 迁移器（含 openclaw 迁移源） | 只取通道/调度/会话三条线；定位分歧点 |
| 通道抽象 | `channels.Channel` 接口（`Name/Start/Stop/Send/IsRunning/IsAllowed/ReasoningChannelID`）+ `pkg/bus` 双向 `MessageBus`（inbound/outbound/media/audio 多条流，带丢弃计数与退避预算）+ 能力用可选接口（Typing/EditMessage/Delete/Reaction/Placeholder/Streaming） | 采纳其**核心形状**（接口 + 总线 + 可选能力），砍掉流式/语音/多账号维度；消息结构保留平台必需字段（`msg_id` 等） |
| 消息结构 | `InboundMessage{Context(channel/account/chatID/chatType/sender/messageID/mentioned/replyTo/raw), Content, Media, SessionKey}`、`OutboundMessage{Channel, ChatID, Content, ReplyTo, ContextUsage}` | 精简同构字段（见 `docs/channels.md` §2） |
| 会话 | `SessionKey` 归一 + 会话管理包 | 采纳：`SessionKey → agent 实例 → 会话文件`（见 `docs/sessions.md`） |
| QQ 通道 | 官方开放平台 API，用 `tencent-connect/botgo` 走 WebSocket：token 自动刷新、`/gateway`、`C2C_MESSAGE_CREATE` + `GROUP_AT_MESSAGE_CREATE`、`PostC2CMessage`/`PostGroupMessage`、`msg_id`+`msg_seq` 自增、5 分钟去重 TTL、消息切分、语音时长探测 | **不采纳 SDK**（D3）：协议自实现，细节见 `docs/qq-bot.md`；去重/切分/被动回复语义照采纳 |
| 定时任务 | `pkg/cron`：`CronSchedule{kind: at|every|expr, atMs/everyMs/expr/tz}` + `CronPayload{kind, message/command, channel, to}` + `JobState{nextRunAt/lastRunAt/lastStatus/lastError}`，JSON 存储，模型经 `cron` 工具（add/list/update/remove/run_now）自建 | **采纳模型与语义**（含 `payload.channel/to` 回灌原渠道），表达式解析后置（D5）；参数改用相对量防止模型算错时间 |
| 心跳 | `pkg/heartbeat`：定时读工作区文件里的待办段并唤醒 agent | 后置可选（`docs/plan.md` §8 P4） |
| 依赖观 | 依赖极多（各 IM SDK、providers、web 框架…） | 与 tanya 一致：极简优先，新增依赖先讨论；QQ 协议自实现，仅需一个 websocket 客户端库 |

## 关键文件位置（克隆内）

- `pkg/channels/interfaces.go`：能力接口集合
- `pkg/channels/base.go`（`Channel` 接口）、`pkg/channels/manager.go`（2130 行，Manager 路由/切分/占位）
- `pkg/bus/types.go`：`InboundMessage`/`OutboundMessage`/`MediaPart`
- `pkg/channels/qq/qq.go`（1022 行）：QQ 官方机器人通道
- `pkg/cron/service.go`：定时作业模型与调度循环
- `pkg/tools/cron.go`：模型侧 `cron` 工具
- `docs/channels/qq/README.zh.md`：QQ 接入流程（含 `q.qq.com/qqbot/openclaw` 一键创建入口）
