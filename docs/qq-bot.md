# QQ 官方机器人通道

> 状态：规划。接入 QQ 开放平台**官方机器人**（不走 OneBot）。协议自实现（D3）。
> 状态：**协议事实已核对**（官方文档抓取 + 真机凭证实测，2026-10-20），结论见 §10；凭证来源见 §12。

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

- `POST https://api.bot.qq.com/app/getAppAccessToken`（官方文档口径），body `{appId, clientSecret}` → `{access_token, expires_in}`。
  **实测同时**：旧域名 `https://bots.qq.com/app/getAppAccessToken` 仍可用（botgo 用的就是它，返回相同体）。
  `expires_in` 实测是**字符串**（如 `"6468"`），文档写 number → 解析用兼容类型。
- 之后的 REST 调用带 `Authorization: QQBot <access_token>`（**实测通过**：`/users/@me`、`/gateway`、`/gateway/bot` 均 200）。
  `X-Union-Appid` **非必需**（带与不带都 200）。沙箱环境已不可用，不再考虑。
- token 后台自动刷新：按 `expires_in` 提前（如 80%）换新；刷新失败按指数退避重试并记日志，**不阻塞消息收发**（旧 token 到点前的请求仍可发）。
- AppSecret 只从配置文件读（权限 0600），不进日志、不进事件、不出现在错误信息里。

## 4 网关与 WebSocket 生命周期

- `GET /gateway` → `{url}`（实测 `wss://api.sgroup.qq.com/websocket`）；`GET /gateway/bot` → `{url, shards, session_start_limit{total,remaining,reset_after,max_concurrency}}`
  （实测该 appid `shards:1`、`max_concurrency:1`、`remaining:1500`/天）。API base 生产 `https://api.sgroup.qq.com`；**沙箱已下线**，只做生产。
- opcode（已核对）：`0` dispatch / `1` heartbeat / `2` identify / `6` resume / `7` reconnect / `9` invalid session / `10` hello / `11` heartbeat ack / `12` http 回调 ack / `13` 回调地址验证。
- 流程：连上 → `op10 hello{heartbeat_interval}` → `op2 identify{token, intents, shard:[0,1], properties}` → `op0 READY`（记 `session_id` 与 `s`）→ 定时 `op1`（带最新 `s`）→ `op11` ack。
- 心跳：按 `heartbeat_interval` 的 80% 发；连续未收到 ack 判定连接僵死 → 主动重连。
- 重连策略：
  - 网络断开/写失败 → 尝试 `op6 resume{token, session_id, seq}`；resume 成功则事件补投（去重仍兜底）。
  - `op7 reconnect` → 清 session，重新走 `/gateway` + identify。
  - `op9 invalid session` → 丢弃 session，重新 identify。
  - 退避：指数退避 + 抖动（1s 起、上限 60s），连续失败计数与最后错误暴露给 `ivan status`。
- 单分片（`shard:[0,1]`）即可覆盖个人机器人；预留分片字段不做多分片调度。

## 5 订阅事件（intents）

- 订阅位（已核对）：群与 C2C 事件 `1<<25`（`GROUP_AND_C2C_EVENT`）= **33554432**；`C2C_MESSAGE_CREATE` 与 `GROUP_AT_MESSAGE_CREATE` 同属该位。
- 首期处理：
  - `C2C_MESSAGE_CREATE`：单聊消息，`d.author.user_openid`、`d.content`、`d.id`（被动回复用）
  - `GROUP_AT_MESSAGE_CREATE`：群内 @ 机器人，`d.group_openid`、`d.author.member_openid`、`d.content`、`d.id`
- 群内**全量消息** `GROUP_MESSAGE_CREATE` 需额外权限，本期不做
- 未处理事件（成员变动、表情表态等）只记 debug 日志，不投递业务。

## 6 发送

- 单聊：`POST /v2/users/{user_openid}/messages`（100 QPS）；群聊：`POST /v2/groups/{group_openid}/messages`（已核对）。
- 文本：`{content, msg_type: 0, msg_id?, msg_seq?}`；`msg_type`（已核对）：`0` 文本（`content`）/ `2` markdown（`markdown.content`，此时 `content` 必须为空）/ `6` 输入中（`input_notify`）/ `7` 富媒体（`media.file_info`）。
- `msg_seq` 不填默认 `1`；`msg_id` 文档写「5 分钟有效」（与 §11 实测 60 分钟冲突 → 以实测为准，窗口内以服务端返回 `40034005 回复消息msg_id已过期` 为终止条件）。
- **被动回复**（官方文档实测，2026-10-21 版）：
  - 单聊：有效期 **60 分钟**，每条消息最多回复 **4 次**
  - 群聊：有效期 **5 分钟**，最多 **5 次**
  - **相同 `msg_id` + `msg_seq` 重复发送会失败**，递增 `msg_seq` 才可多次回复 → `msg_seq` 必须**以入站消息 id 为键**维护计数（TTL 随窗口清理，不按 ChatID 记），进程重启后从 1 重新计数，失败则递增重试
- **主动消息**（无 `msg_id`）：官方频控——单聊认证 10/qps & 20/qpm、未认证 5/qps & 30/qpm，日上限 **1000 条/用户**；群聊 60/qpm（未认证 30/qpm），日上限 **1000 条/群**。
  - **用户可在 QQ 客户端关闭"允许主动发送"，关闭后一律发送失败** → 通知定位为 **best-effort**，失败必须计数并可见，不得承诺投达。
  - 互动召回（`is_wakeup: true`）：用户对话后 30 天内仅 4 个周期各 1 条，**不作为常规通知手段**。
- **功能前提**：官方要求机器人保持 WebSocket 在线才能发送消息 → 重连健壮性是功能前提而非体验优化。
- 图片：先上传 `/v2/users/{openid}/files`（单聊）或群对应接口（**单聊与群聊上传接口不互通**），`file_type: 1`，得到 `file_info`（`file_uuid`/`ttl`），再以 `msg_type: 7` + `media.file_info` 发送。官方推荐**分片上传**（无需公网 CDN），整文件上传为备选；`ttl` 过期需重传，适配器缓存 `file_uuid` 并处理失效重传。
- 消息长度：官方未给硬上限（picoclaw 配置用 2000），本期按 `max_message_len`（默认 1200）保守切分，走通用切分器（见 `docs/channels.md` §6）。
- 图片上传（已核对）：`POST /v2/users/{user_openid}/files`（50 QPS）与 `/v2/groups/{group_openid}/files`；`file_type: 1` 图片（png/jpg）；三种传法——`file_data` 二进制 ≤5MB、`url`（http 开头，平台转存）、分片（`upload_prepare` → `upload_part` → `upload_part_finish`）；`srv_send_msg: false` 只返回 `file_info`，再以 `msg_type: 7` + `media.file_info` 发。
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
3. **真机验证**：生产凭证（§12）+ 本人 QQ 账号，验证单聊收发、群 @、图片上传（**沙箱环境已下线**，不做沙箱）。
4. 不引入 `tencent-connect/botgo` 作为参照实现（D3），协议行为以官方文档 + 真机抓包为准。

## 10 核对结论（2026-10-20 完成）

真机凭证实测（app_id 见 §12）+ 官方文档抓取（`bot.q.qq.com/wiki/develop/api-v2/`）逐条核对，全部完成：

- [x] token 端点：`https://api.bot.qq.com/app/getAppAccessToken`（旧 `bots.qq.com` 亦可用）；字段 `appId`/`clientSecret` → `access_token`/`expires_in`（**字符串**）
- [x] `Authorization: QQBot <access_token>` 前缀正确；`X-Union-Appid` 非必需
- [x] 生产 base `https://api.sgroup.qq.com`；**沙箱环境已下线**（本机器人只在生产跑）
- [x] intents `1<<25 = 33554432`（`GROUP_AND_C2C_EVENT`），`shard: [0,1]`
- [x] 被动回复：以服务端错误码 `40034005`（msg_id 已过期）判定窗口结束；条数以失败即停为准
- [x] `msg_type`：`0` 文本 / `2` markdown / `6` 输入中 / `7` 富媒体；文本上限官方未给，取配置
- [x] 图片上传：`/v2/users/{openid}/files`、`/v2/groups/{group_openid}/files`，`file_type: 1`，`srv_send_msg: false`，二进制 ≤5MB / url / 分片
- [x] 错误码：body 用 `err_code`（非 `code`）+ `message` + `trace_id`；HTTP 429 = 限流；401 认证失败；常见业务码 `11251/11261` appid 错、`11253` 无权限、`11254` 接口封禁
- [x] 频控：消息接口 100 QPS（单聊/群聊一致），文件接口 50 QPS；网关会话启动 `1500` 次/天（`max_concurrency: 1`）

## 11 官方规则速查（文档抓取 2026-10-10，2026-10-20 复核）

| 项 | 单聊 | 群聊 |
|---|---|---|
| 接收事件 | `C2C_MESSAGE_CREATE` | `GROUP_AT_MESSAGE_CREATE` |
| 被动有效期 | 60 分钟 | 5 分钟 |
| 每条消息可回复次数 | 4 | 5 |
| 主动频控（认证/未认证） | 10/qps & 20/qpm ｜ 5/qps & 30/qpm | 60/qpm ｜ 30/qpm |
| 主动日上限 | 1000 条/用户 | 1000 条/群 |
| 上传接口 | 独立，不互通 | 独立，不互通 |

其他：`msg_type` `0` 文本 / `2` markdown / `7` 富媒体；相同 `msg_id`+`msg_seq` 重复发送失败；机器人须 WebSocket 在线才能发消息；用户可关闭"允许主动发送"。

## 12 凭证与现网参数

- 机器人：`app_id = <app_id>`（用户名「机器人账号」），走**生产**环境。
- `app_secret` **不写入仓库、不写入文档、不进日志**。取值路径（NAS 上，`0600`）：
  `ssh 内网主机` → `~/原部署/.security.yml` 的 `channel_list.qq.settings.app_secret`。
  ivan 侧配置通过 `IVAN_QQ_APP_SECRET` 环境变量或 `channels.qq.app_secret`（文件 0600）注入。
- 现网占用情况：picoclaw 的 QQ 通道**自 2026-10-19 起启动失败**（`/gateway/bot` 返回 `401 code 11241 请求头Authorization参数格式错误`，botgo v0.2.1 与新网关不兼容），
  其 launcher/gateway 进程虽在跑但**未占用 ws 连接**，ivan 可直接使用该凭证。
  若日后修复 picoclaw 并重新启用 QQ 通道，两边会争抢事件与 `msg_seq` → **同一 appid 只允许一个在线实现**，启用 ivan 前先停 picoclaw 的 qq 通道。
- 该凭证的历史日志（`~/原部署/logs/gateway.log`）里曾**明文出现 app_secret 与 access_token**（botgo debug 日志），已是既成事实，但更说明 ivan 侧必须守住「凭证不进日志」。
