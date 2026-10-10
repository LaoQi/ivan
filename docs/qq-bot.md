# QQ 官方机器人通道

> 状态：规划。接入 QQ 开放平台**官方机器人**（不走 OneBot）。协议自实现（D3）。
> 标 **[核对]** 的细节以官方文档为准，实装时逐条验证并回填本文。

## 1 范围

首期只做：单聊（C2C）与群聊 @ 的文本 + 图片收发。不做频道（guild）、不做语音/视频/文件、不做主动推送营销类能力。

## 2 分层与抽库路径

```
qqbot/            纯协议客户端：token / gateway / websocket / REST / 事件与消息类型
channel/qq/       适配器：qqbot 事件 → bus.InboundMessage；bus.OutboundMessage → qqbot 调用
```

硬约束：`qqbot/` **不得依赖 ivan 的任何包**（不 import `bus`、`channel`、`config`），只吃自己的配置结构体、只吐自己的事件类型。这样抽库时 ivan 只改 import 路径与 `go.mod`：

- 本期：`github.com/LaoQi/ivan/qqbot`
- 抽库后：`github.com/LaoQi/qqbot`（ivan 用 `go.work` 联调，稳定后按 tag 引入）

`channel/qq` 的职责边界要窄：会话标识映射（openid/group_openid → ChatID）、图片落盘与引用、`msg_seq` 计数与被动回复窗口、配置到 `qqbot.Config` 的翻译。**协议细节（重连、心跳、限流、错误码）一律留在 `qqbot/` 内。**

## 3 鉴权

- `POST https://bots.qq.com/app/getAppAccessToken`，body `{appId, clientSecret}` → `{access_token, expires_in}`。**[核对]** 域名与字段名
- 之后的 REST 调用带 `Authorization: QQBot <access_token>`。**[核对]** 前缀格式，沙箱环境是否同一套
- token 后台自动刷新：按 `expires_in` 提前（如 80%）换新；刷新失败按指数退避重试并记日志，**不阻塞消息收发**（旧 token 到点前的请求仍可发）。
- AppSecret 只从配置文件读（权限 0600），不进日志、不进事件、不出现在错误信息里。

## 4 网关与 WebSocket 生命周期

- `GET /gateway` → `{url}`（wss）。API base 生产 `https://api.sgroup.qq.com`、沙箱 `https://sandbox.api.sgroup.qq.com`。**[核对]**
- opcode：`0` dispatch / `1` heartbeat / `2` identify / `6` resume / `7` reconnect / `9` invalid session / `10` hello / `11` heartbeat ack。**[核对]**
- 流程：连上 → `op10 hello{heartbeat_interval}` → `op2 identify{token, intents, shard:[0,1], properties}` → `op0 READY`（记 `session_id` 与 `s`）→ 定时 `op1`（带最新 `s`）→ `op11` ack。
- 心跳：按 `heartbeat_interval` 的 80% 发；连续未收到 ack 判定连接僵死 → 主动重连。
- 重连策略：
  - 网络断开/写失败 → 尝试 `op6 resume{token, session_id, seq}`；resume 成功则事件补投（去重仍兜底）。
  - `op7 reconnect` → 清 session，重新走 `/gateway` + identify。
  - `op9 invalid session` → 丢弃 session，重新 identify。
  - 退避：指数退避 + 抖动（1s 起、上限 60s），连续失败计数与最后错误暴露给 `ivan status`。
- 单分片（`shard:[0,1]`）即可覆盖个人机器人；预留分片字段不做多分片调度。

## 5 订阅事件（intents）

- 订阅位：群与 C2C 事件 `1<<25`（`GROUP_AND_C2C_EVENT`）。**[核对]**
- 首期处理：
  - `C2C_MESSAGE_CREATE`：单聊消息，`d.author.user_openid`、`d.content`、`d.id`（被动回复用）
  - `GROUP_AT_MESSAGE_CREATE`：群内 @ 机器人，`d.group_openid`、`d.author.member_openid`、`d.content`、`d.id`
- 未处理事件（成员变动、表情表态等）只记 debug 日志，不投递业务。

## 6 发送

- 单聊：`POST /v2/users/{openid}/messages`；群聊：`POST /v2/groups/{group_openid}/messages`。**[核对]**
- 文本：`{content, msg_type: 0, msg_id?, msg_seq?}`。
- **被动回复**：带最近一条入站消息的 `msg_id`，且 5 分钟内条数有限（[核对] 具体上限，按 5 条实现），`msg_seq` 从 1 递增；`msg_seq` 由适配器按 `ChatID` 记数（内存 + 落 state，重启重置可从 1 起）。
- 超出被动窗口 → 走主动消息（需要平台授权与配额）；配置可关闭主动消息，超窗即失败并记日志。
- 图片：先上传 `/v2/users/{openid}/files`（或群对应接口），`file_type: 1`，得到 `file_info`（`file_uuid`/`ttl`），再以富媒体消息发送（`msg_type: 7` + `media`，或图片+文本组合，[核对] 具体形态）。`ttl` 过期需重传，适配器缓存 `file_uuid` 并处理失效重传。
- 消息长度：按官方上限切分（[核对]），默认保守 1200 字符，走通用切分器（见 `docs/channels.md` §6）。
- 限流：发送失败带平台错误码时，区分「限流」「权限不足」「参数错误」「凭证失效」，分别退避重试 / 直接失败 / 记录待查。

## 7 入站处理

- 去重：`MessageID` TTL 缓存（5 分钟，容量上限），重复事件丢弃。
- 图片：入站附件下载到 `data/media/`，转成 `MediaRef` 交给 runner（模型按需 `read_image` 读取；与 tanya 的口径一致）。
- 群聊触发：只在被 @ 时处理；`Mentioned` 字段记录，将来按配置支持"群里任意消息"。

## 8 配置项（`channels.qq`）

```yaml
channels:
  qq:
    enabled: true
    app_id: ""
    app_secret: ""          # 0600，不进日志
    sandbox: true           # true 走沙箱域名
    allow_from: []          # 允许的 openid 白名单，空 = 允许所有（首期建议非空）
    allow_groups: []        # 允许的 group_openid 白名单
    max_message_len: 1200
    allow_proactive: false  # 超被动窗口是否允许主动消息
    reconnect_max_sec: 60
```

## 9 测试策略

1. **协议单测**：token 刷新（含提前刷新与失败退避）、identify/resume 帧构造、心跳定时、opcode 分发、错误码分类、`msg_seq` 递增与窗口过期、去重 TTL。
2. **假网关集成测试**：本地起 httptest 网关服务（模拟 `/gateway` 与 ws），跑真实 `qqbot.Client`：覆盖正常收发、断线 resume、`op9` 重连、心跳超时、限流退避。
3. **沙箱真机**：`q.qq.com` 沙箱机器人 + 测试账号，验证真实收发、图片上传、群 @（凭证实施阶段提供）。
4. 不引入 `tencent-connect/botgo` 作为参照实现（D3），协议行为以官方文档 + 真机抓包为准。

## 10 待实装核对清单

- [ ] token 端点域名/字段名/`Authorization` 前缀格式
- [ ] 生产与沙箱 API base 域名
- [ ] intents 位定义与是否需要额外 flag
- [ ] `msg_id`/`msg_seq` 规则与被动回复条数/时限
- [ ] 文本长度上限与图片发送的消息体形态（`msg_type` 取值）
- [ ] 图片上传接口路径、`file_type` 枚举、`ttl` 行为
- [ ] 主动消息的权限与配额
- [ ] 错误码表（限流/权限/参数/凭证）
