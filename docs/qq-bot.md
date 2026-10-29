# QQ 官方机器人通道

> 状态：规划。接入 QQ 开放平台**官方机器人**（不走 OneBot）。协议自实现（D3）。
> 状态：**P2 已实现**（`qqbot/` 协议包 + `channel/qq` 适配器 + 配置/daemon 接线）。协议事实已核对（文档抓取 + 真机实测），结论见 §10；凭证来源见 §12；
> 实现记录见 §13。真机**P2 全部能力已验证**：单聊全链路（入站→runner 回合→被动回复）、群 @、图片收发（内联 + 分片）、去重、重连。

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
- **接入方信息（identify `properties`）**：`$os` 取 `runtime.GOOS`、`$browser` 取 `UserAgent` 首段（如 `ivan/dev`）、`$device` 固定 `ivan`；
  日志在 identify 时打印 `browser=`/`os=` 便于核对。REST（取 token / 发消息 / 上传）透传同一 `User-Agent`，不设时才是 Go 默认的 `Go-http-client/2.0`。
- **关键实测补充（官方文档未写）**：`op2 identify` / `op6 resume` 的 **`d.token` 必须带 `QQBot ` 前缀**（与 REST 的 `Authorization` 头同格式）。
  裸 token 会被服务端**直接判 `op9 invalid session`**（`d:false`，即"可重连"），且**不区分 intents**（`intents:0` 与 `1<<25` 表现一致）——这正是 picoclaw/botgo 失败的同类症状。
  `READY.d` 含 `version`/`session_id`/`user{id,username,bot,status}`/`shard`。
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
- 图片：先上传 `/v2/users/{openid}/files`（单聊）或群对应接口（**单聊与群聊上传接口不互通**），`file_type: 1`，得到 `file_info`（`file_uuid`/`ttl`），再以 `msg_type: 7` + `media.file_info` 发送。`ttl` 过期需重传。
- **分片上传（已实现，官方推荐）**：`≤5MB` 走 `file_data`（base64 内联）；`>5MB` 走四步——
  ①`POST /v2/{users\|groups}/{openid}/upload_prepare`（10 QPS）传 `{file_type, file_size(字符串), file_name, md5, sha1, md5_10m}`
  （`md5_10m` = 前 **10002432** 字节的 MD5）→ 返回 `{upload_id, block_size(字符串), parts:[{index, presigned_url, block_size}], upload_config}`；
  ②每片 `PUT presigned_url`（裸字节，不带 `Authorization`）；③`POST .../upload_part_finish` 传 `{upload_id, part_index, block_size, md5}`；
  ④全部分片完成后 `POST .../files` 传 `{file_type, file_name, upload_id, srv_send_msg:false}` 合并 → `file_info`。
  **实测坑**：`parts[].index` **从 1 开始**（文档写"从 0 开始"）→ 不能用 `index × block_size` 算偏移，必须按 index 排序后**顺序累计**切分，
  否则首片为空、合并报 `40034002 富媒体文件格式不支持`。
- 消息长度：官方未给硬上限（picoclaw 配置用 2000），本期按 `max_message_len`（默认 **2048**）保守切分，走通用切分器（见 `docs/channels.md` §6）。
- 图片上传（已核对）：`POST /v2/users/{user_openid}/files`（50 QPS）与 `/v2/groups/{group_openid}/files`；`file_type: 1` 图片（png/jpg），软限制 20MB / 硬限制 200MB；三种传法——`file_data`（base64）≤5MB、`url`（http 开头，平台转存）、分片（`upload_prepare` → 分片 `PUT` → `upload_part_finish` → `files` 合并，见 §6）；`srv_send_msg: false` 只返回 `file_info`，再以 `msg_type: 7` + `media.file_info` 发。
  **实测补充**：除体积外还有**像素尺寸**限制——700×3486（2.44M 像素）的合法 PNG 被拒 `850019/40034002 富媒体文件格式不支持`，1300×1300 与 1300×1600 均正常。
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
    max_message_len: 2048
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

- [x] `identify`/`resume` 的 `d.token` 必须带 `QQBot ` 前缀（**真机实测**：裸 token → `op9`；带前缀 → `READY`）

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

## 11.1 流式消息与输入状态（官方文档抓取 2026-10-22）

- **流式发送单聊消息**：`POST /v2/users/{user_openid}/stream_messages`（50 QPS）。
  - 请求体：`stream_msg_id`（首条由服务端生成并返回，后续分片必须携带上一分片返回的 id）、`index`（从 0 递增）、`input_state`（1=生成中、10=生成结束）、`input_mode`（`append` 默认 / `replace` 须以上游已下发前缀 `SentContent` 开头）、`content_type`（`text` / `markdown`）、`content_raw`、`msg_id`/`event_id`（被动回复二选一）、`msg_seq`、`is_wakeup`。
  - 响应：`id`（首条即 `stream_msg_id`）、`timestamp`、`ext_info`、`remain_msg_len`（剩余长度，字符数）。
  - 错误码：`40007` 已下发内容前缀不可修改、`50001` 服务内部错误、`50002` 频率限制。
  - **`replace` 语义决定了流式消息里只能放"正文"**：不能先写"处理中…"再改成正文（那不是已有内容的前缀，会报 `40007`）。故 ivan 的实现是——静默期用下面的 `msg_type=6` 覆盖，正文出现后才开流并逐次替换为更长的前缀。
- **群聊官方明确"不支持流式参数"**（发送群聊消息页原文）→ 群聊自动退化：不开流，走"输入中" + 普通消息。
- **输入中状态**：`POST /v2/{users|groups}/{openid}/messages`，`msg_type=6` + `input_notify{input_type:1, input_second≤60}`；与被动回复同用 `msg_id`/`msg_seq`，因此**会占用被动回复次数**——ivan 只在剩余额度 > 2 时才发（给最终结果留足），额度不足自动跳过。
- 观测：`ivan status` 的 qq 行含 `md=`（markdown 出站条数，普通消息与流式首片都计），用于确认 markdown 通路是否真的生效。
- 已核对：`msg_type` 取值 `0` 文本 / `2` markdown / `6` 输入中 / `7` 富媒体；群聊 `msg_type` 只有 `0/2/7`（无 6）。

## 11.2 出站 Markdown（2026-10-22 实现）

官方「Markdown 消息」页（文档抓取 2026-10-22，站点 v1.42.0）：**2026/04/23 起单聊与群聊的自定义 Markdown 已对所有机器人开放，无需申请模板**（频道场景才需内邀）。

- **支持格式（官方明确列出）**：标题（`#`/`##`）、文字样式（`**加粗**`、`__下划线加粗__`、`_斜体_`、`*星号斜体*`、`***加粗斜体***`、`~~删除线~~`）、链接（`[文字](url)`、`<url>`）、图片（**公网 URL**，或上传素材后的 `qqbot-media://` 引用）、有序列表、无序列表、列表嵌套（二级需 4 空格缩进）、块引用（`>`）、水平分割线（`***`）、空行换行（`\u200B` 亦可）。
- **未列出**：**代码块/行内代码、表格**——官方支持格式清单里没有这两项，属于未验证项（下方降级策略即为此设）。
- 发送约束：`msg_type=2` 时 `content` 必须为空、`markdown` 必填；失败为明确可判定的错误码——`40034011` 无效的 markdown 内容、`40034008/40034009/40034010` markdown 参数问题、`40034124/40034127`、`304036`、`50055/50056/50057`（统一归为「markdown 内容不可用」，`qqbot.Error.MarkdownRejected`/`IsMarkdownRejected`）。
- 流式接口的 `content_type` 支持 `markdown`（`qqbot.ContentTypeMarkdown`）。

### ivan 的出站策略

配置项 `channels.qq.markdown`（**默认开**），实现在 `channel/qq/md.go`：

1. **只在正文含 markdown 语法特征时才用 `msg_type=2`**（`hasMarkdown`：标题/列表/引用/围栏/强调/行内代码/链接/图片/水平线）。纯文本回合继续走 `msg_type=0` —— 把渲染差异与错误码风险压到只有真正用到语法的消息上。
2. **降级到 QQ 支持的子集**（`toQQMarkdown`）：表格 → 要点行（首列作条目名、其余列 `列名=值`，` · ` 分隔）；代码围栏 → 剥围栏 + 4 空格缩进、语言标记转 `> lang` 引用行；无法公网访问的图片（本地路径/附件）→ `[图片: alt]` 文字。公网 URL 与 `qqbot-media://` 原样保留。
3. **被拒即退回纯文本**：`SendMarkdown` 返回 markdown 类错误码时，记一条日志并以**下一个 `msg_seq`** 走 `SendText` 重发（同 `msg_id`+`msg_seq` 不可重复发送），不把失败暴露给上层。
4. **切分**：`channel/qq` 实现可选能力 `channel.Splitter`（`SplitOutbound`），markdown 模式下按行边界切分并在围栏内断开时**补闭合标记**、下一片重开围栏，保证每片自身是合法 markdown；未声明该能力的通道仍走通用切分器。
5. **流式**：单聊流式**只要 `markdown` 开关打开就声明 `content_type=markdown`，不按正文判定**——开流发生在正文刚有首字时（实测首片常是「我这条回复就带…」这类说明文字），而 `content_type` 在首片即锁定、后续分片无法更改，按片段判定会让整条消息以纯文本渲染（**2026-10-22 实测踩坑：markdown 渲染整体丢失**）。官方流式示例本身也以 `content_type=markdown` 发送纯文本正文（`"content_raw": "正在生成回答，请稍候"`），证实该用法安全。中间态每片补闭合围栏（`streamHandle`，只追加尾部，不破坏 `replace` 的前缀约束）。**流式不做表格/围栏改写**——那些改写不是「已下发内容的前缀」，会触发 `40007`；被平台拒收时开流失败，Manager 退化为普通消息投递（那条路径才走第 2 步的降级链）。
6. 群聊不走流式（官方限制），普通 markdown 消息同样适用上述 1–4。

**未做真机验证**（本轮无可用凭证）：代码块/表格在 QQ 客户端的实际渲染、`content_type=markdown` 流式的接受度、降级后是否仍被拒。实现按「未列出即降级 + 拒收即回退纯文本」做保守处理，真机试用后再按实际渲染调整降级形态。

## 12 凭证与部署参数

- **凭证一律不进仓库、不进文档、不进日志**：`app_id` 与 `app_secret` 由部署方自行提供，
  ivan 侧经 `IVAN_QQ_APP_ID` / `IVAN_QQ_APP_SECRET` 环境变量，或 `channels.qq.app_id` /
  `channels.qq.app_secret`（配置文件 0600）注入。文档与示例配置只留空值占位。
- **同一 appid 只允许一个在线实现**：同一个机器人凭证不能有两处同时在线的 WebSocket 实现，
  否则会争抢事件与 `msg_seq`。部署新实例前先停掉旧实例。
- 排障时若怀疑凭证形态，先用只读请求核对 `POST /app/getAppAccessToken` 与 `/gateway/bot` 的返回，
  不要把 token / secret 打进日志。

## 12.1 流式实现（2026-10-22）

`qqbot/stream.go`：`OpenStream`（首片 `input_state=1, index=0`）/ `Stream.Update`（`replace` 全量、`done` 时 `input_state=10`）/ `Client.Typing`；协议细节（分片序号、`stream_msg_id` 传递、`input_state`）全部留在协议包内。`channel/qq` 实现 `channel.StreamOpener` 与 `channel.Typist`：单聊开流（群聊明确报错 → Manager 退化）、`StreamMaxLen()`（`stream_max_len`，默认 2048）、`Typing` 前检查被动额度剩余（`typingReserve=2`，不足则跳过）。配置：`channels.qq.stream`、`stream_max_len`、`typing`。**未做真机验证**（按用户口径"配额按文档默认参数限制、可配置、不做测试"）：分片是否计被动额度、`remain_msg_len` 实际取值待真机时确认；实现上已做两处保守处理——流式分片共用一个 `msg_id`/`msg_seq`（首片取一次被动序号），`input_notify` 在额度不足时跳过。

## 13 P2 实现记录（2026-10-20）

代码：`qqbot/`（纯协议，仅依赖标准库 + `gorilla/websocket`，零 ivan 依赖）、`channel/qq/`（适配器）、`config` 增 `channels.qq`、`daemon` 接线与 `status` 展示。

### 13.1 qqbot 包

```go
qqbot.Config{AppID, AppSecret, Intents, Shard, APIBase, TokenURL, GatewayURL, UserAgent, HTTPClient, Logf, MinBackoff, MaxBackoff}
qqbot.New(Config) (*Client, error)                 // 缺凭证报错
(*Client).Start(ctx, Handler) error                // 异步；token 后台刷新 + 网关循环
(*Client).Stop(ctx) error
(*Client).SendText / SendMarkdown / SendImage / SendTextReply / UploadImageBytes / UploadImageURL
(*Client).Gateway(ctx) (*GatewayInfo, error)       // /gateway/bot
(*Client).Stats() Stats                            // 连接/会话/计数/最后错误/最后心跳
qqbot.Handler{OnReady, OnMessage, OnError}  /  HandlerFunc
```

- `types.go`：`Config`/`Target`/`MessageEvent`/`Attachment`/`ReadyInfo`/`GatewayInfo`/opcode 与 `msg_type` 常量；`flexInt` 兼容 `expires_in` 的**字符串/数字**两种形态。
- `token.go`：`getAppAccessToken`（默认 `api.bot.qq.com`），REST 统一透传 `UserAgent`（接入方标识），**80% TTL 提前刷新**，失败指数退避重试且**旧 token 到期前不阻塞收发**；AppSecret 不进错误信息。
- `errors.go`：`Error{Status,Code,Message,TraceID,Class,Op}` + `classify`（429→限流、401→认证、403→权限、`40034005`→窗口过期、5xx→服务端），`Retryable/AuthFailed/WindowExpired` 助手；响应体含 `secret`/`token` 等字样时**整体打码**。
- `gateway.go`：hello→identify→READY→心跳（interval 80%）→ack 超时（2×间隔）判定僵死重连；`op7`→清 session 重连、`op9`→丢弃 session 重 identify、`op6 resume` 补投；指数退避 + 抖动（默认 1s→60s）；单分片。
  **`d.token` 自动补 `QQBot ` 前缀**（见 §4）。
- `dedup.go`：`MessageID` TTL（5 分钟）+ 容量上限（4096）去重。
- `api.go`：REST（`/gateway/bot`、`/v2/users|groups/{id}/messages`、`/files`），统一带 `Authorization: QQBot <token>`。

### 13.2 channel/qq 适配器

- `Config{AppID, AppSecret, AllowFrom, AllowGroups, MaxMessageLen, AllowProactive, ReconnectMax, MediaDir, MediaMaxBytes, Timeout, APIBase/TokenURL/GatewayURL, Logf, HTTPClient}`。
- 入站：`C2C_MESSAGE_CREATE`→`ChatType=direct`（`ChatID=user_openid`）、`GROUP_AT_MESSAGE_CREATE`→`group`（`ChatID=group_openid`，`Mentioned=true`）；`Raw` 带 `msg_id`；白名单（`allow_from`/`allow_groups`，空=放行）；附件下载到 `media/` 转 `MediaRef`（`MediaMaxBytes` 截断、0600）。
- 出站：解析目标（优先用入站登记的 openid/group）；**`msg_seq` 以入站 `msg_id` 为键**计数，单聊窗口 60 分钟/上限 4、群聊 5 分钟/上限 5，超限且未开 `allow_proactive` 则报错；有 `msg_id` 走被动回复，否则（`allow_proactive`）走主动消息。
- 图片：出站先 `/files` 上传（`file_type:1`、`srv_send_msg:false`）取 `file_info`，再 `msg_type:7` 发送；`≤5MB` 走 `file_data` 内联，`>5MB` 走**四步分片上传**（`upload_prepare` → 逐片 `PUT` → `upload_part_finish` → `files` 合并，见 §13.1；分片 `index` 实测从 **1** 开始）；媒体上限默认 20MB。
- `MaxLen()=max_message_len`（默认 2048），出站走通用切分器。

### 13.3 测试

- 协议单测（`qqbot/`）：token 字符串/数字兼容、错误分类与打码、助手函数、去重 TTL/容量、**假网关**（`httptest` + 真 ws）覆盖 READY/identify 帧、单聊与群事件映射、重复事件丢弃、心跳与 ack、断线 `resume`、`op9` 重 identify、心跳超时重连、发送（`Authorization`/路径/`msg_id`/`msg_seq`/`msg_type`）、窗口过期、认证失败、无目标、停止。
- 适配器单测（`channel/qq/`）：单聊/群翻译与会话键、白名单、缺标识拒绝、被动 `msg_seq` 递增与**按 msg_id 隔离**、单聊 4/群 5 上限、主动消息无 `msg_id`、图片上传+发送三步、附件下载落盘、`MaxLen` 默认。
- `go test ./...` 与 `-race` 全绿。

### 13.4 真机验证（生产凭证）

- **出站（主动消息）通过**：`channel/qq` → `qqbot` 全链路向本人 openid 发送文本，返回消息 id（HTTP 200）。
- **入站 + 全链路（daemon 端到端）通过**：用户向机器人实发单聊消息后，daemon 侧 `qq.messages=1`、`runner.turns=1`、`failures=0`、`qq.sent=1`；
  日志显示 `turn_start session=qq:c2c:<openid> channel=qq` → `run_shell` 在**该会话独立工作区** `<data_dir>/agents/qq-c2c-<openid>/` 执行 → `turn_end`，
  用户侧收到回复（被动回复 `msg_seq=1`）。
- **图片入站已通**：用户发图 → 下载落 `media/`（`idDigest-序号.jpg`）→ 提示词注入本地路径 → 模型 `read_image` 读取 → 回复。
  期间修掉两个 bug：①runner 拒收「无正文仅附件」消息（`入站消息内容为空`）→ 改为附件非空也受理，并把附件路径/类型/字节数注入提示词（`promptOf`）；
  ②附件文件名用 `msg id` 前 8 字符 → QQ 消息 id 均以 `ROBOT1.0_` 开头导致**同名互相覆盖** → 改用 `sha256(msg id)` 前 12 位十六进制。
- **群 @ 已通**：用户把机器人拉群并 @ 后 `turn_start session=qq:group:<group_openid>`，正常回复。
- **图片出站已通**：`≤5MB` 走 `file_data`、`>5MB` 走分片（6.2MB → 2 片，`upload_prepare`/分片 `PUT`/`upload_part_finish`/`files` 合并全 200），用户侧收到图片。
- **P2 全部能力已真机验证**：单聊文本、群 @、图片收发（内联 + 分片）、被动回复、去重、重连。
- 期间发现并修正了 §4 的 `token` 前缀问题（裸 token 直接被 `op9` 拒绝）。

## 14 通知出口与目标认领（2026-10-30）

QQ 通道实现 `channel.Noticer`（`Notice(ctx, content) []channel.NoticeResult`），成为 `/notify`、`ivan notify`、`send_message` 的默认通知出口之一。**目标无法手工配置**：`user_openid` 是随 app 变化的平台标识，开发者后台也看不到，只能从入站事件或 `state.json` 的会话键里取，所以改成**通道自己认领**。

### 14.1 官方能力边界（核对结论）

- 发消息只有三个目标：单聊 `POST /v2/users/{user_openid}/messages`、群聊 `POST /v2/groups/{group_openid}/messages`、频道 `/channels/{channel_id}/messages` + 频道私信。**没有**「发给机器人管理者/管理员」的接口。
- `keyboard.permission.type=1`（管理员）只是**内嵌键盘按钮的点击权限**，与消息目标无关；`GET /users/@me` 返回的是**机器人自己**。
- 请求体里**没有任何收件人字段**：收件人完全由 URL 里的 openid 决定。可用的关联字段只有 `msg_id`+`msg_seq`（被动回复，60 分钟 / 4 次）、`event_id`（回复事件）、`is_wakeup`（互动召回，30 天内每周期 1 条）。
- 主动消息前提：用户未关闭「允许主动发送」（关闭后一律失败，`40054013` 用户拒收）、需好友关系（`40054004` 无好友关系）；频控单关系 20/qpm、每用户每天 1000 条，超限 `40034100`。

### 14.2 认领机制

通道状态文件 `<data_dir>/qq/channel.json`（0600，临时文件 + fsync + rename 原子写；解析失败保留原文件并报错，不静默重建）：

```json
{
  "version": 1,
  "claim_token": "K7QM-3XPZ",
  "notice_to": ["<user_openid>"],
  "claimed_at": "2026-10-30T00:12:00+08:00"
}
```

- **首启生成令牌**：`Start` 时若文件无 `claim_token`，用 `crypto/rand` 生成 `XXXX-XXXX`（32 字符表，去掉 `0/O/1/I` 便于手机输入）并原子写回；已有令牌则沿用（重启不轮换）。
- **令牌可见**：写一行日志 `qq 通知认领令牌: K7QM-3XPZ（私聊发送该令牌即把该账号设为通知目标）`；令牌不进事件日志、不进记忆、不入会话。
- **认领**：私聊消息 trim + 转大写后与令牌做 `subtle.ConstantTimeCompare`，命中则：写入 `notice_to=[该 openid]`、记录 `claimed_at`、**轮换新令牌并写回文件**、用被动回复回一条确认（含新令牌）；**该条消息不进入 agent**（不耗 token、令牌不进会话）。未命中则照常入站。
- **白名单**：认领同样受 `allow_from` 约束；白名单外的令牌消息被吞掉（不回确认、不认领），不影响主流程。
- **覆盖**：新账号用新令牌认领会覆盖原目标（换账号的场景），记一条日志；文件里始终有有效令牌，所以不需要额外子命令。

### 14.3 投递语义

- 目标 = 状态文件 `notice_to`（去重、trim）；为空 → `Notice` 返回空结果 → daemon 如实回执 `no_targets`，不猜、不兜底。
- **主动消息**：`SendText(target, content, "", 0)`（`msg_id`/`msg_seq` 为 `omitempty`，自动省略）。通知不参与 `allow_proactive` 门禁——有目标本身就是主动推送的开关，避免"配了却发不出"。
- **纯文本 `msg_type=0`**：通知是运维文本，走最稳的通路，绕开 markdown 拒收降级链。
- **通道自切分**：按 `max_message_len` 用 `channel.Split` 逐片发送；单片失败记该目标失败（首错为准）但继续发余下分片，回执照实。
- **超时**：整轮投递（所有目标 + 分片）受 `context.WithTimeout(ctx, cfg.Timeout)`（默认 30s）约束。
- **回执**：`NoticeResult{Target: 完整 openid, Err}`；平台错误（拒收/频控/超长等）原样上报，`/notify` 表现为 `failed` 或 `partial`。

### 14.4 测试

`channel/qq`：首启生成令牌且重启不轮换、文件权限 0600、认领写入目标 + 轮换令牌 + 被动回复确认、错误令牌/同长度异内容令牌/群聊令牌均不认领、容忍大小写与空白、白名单外拒绝认领且不回确认、新账号覆盖旧目标、无目标与空内容返回空结果、主动消息无 `msg_id` 且 `msg_type=0`、超长切分（总字符数不变）、单目标失败如实回报、状态文件损坏报错且不重建、`Manager.All()` 能发现 Noticer 并真实投递。

变异复验（均应变红）：令牌比较恒真、不切分、不轮换令牌——其中「令牌比较恒真」首轮逃逸（错误用例靠长度差异提前返回），补「同长度不同内容」用例后准确拦截。
