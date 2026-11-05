# ivan agent 接口指南

本指南供自动化 agent 使用 ivan 的外部 HTTP 入口。路径均为**去除 host 的相对路径**，
实际请求时拼接接入地址（见服务目录条目 `endpoints` 里 `request` / `notify` 的 url）。

## 能力边界

- ivan 是无人值守后台 agent；`/request` 为**异步**入口：受理即返回会话键，结果靠轮询，没有回调/推送
- 能委托给 ivan 做的事见下节「能让 ivan 做什么」；它在**自己所在主机**上执行，不是你本机的执行代理
- 通知与请求结果的**投递目标由 ivan 侧决定**，请求方不指定、也无需知道任何收件人标识：只需发内容，发给谁由 ivan 负责
- 通知是尽力而为：可能全部送达、部分送达或未送达，**以回执为准**（见 `/notify` 的 `status` 与 `targets[].result`）
- 无鉴权，仅限内网使用，不要暴露公网
- JSON 请求需 `Content-Type: application/json`；错误统一 `{"status":"rejected","message":"<原因>"}` + 对应状态码

## 能让 ivan 做什么

`/request` 提交的是一段自然语言任务，ivan 在自己的主机上用下列能力完成它：

| 能力 | 说明 |
|---|---|
| `run_shell` | 在 ivan 所在主机执行命令；读写文件、改代码、跑测试都走它 |
| `read_image` | 查看图片 |
| `send_file` | 把 ivan 主机上的文件作为附件发给用户（见下） |
| `send_message` | 主动给用户发一条消息 |
| `schedule` | 建 `at` / `every` / `daily` 定时作业（长期留存，需人清理） |
| `next_session` | 长任务交接上下文 |
| `get_time` / `get_env` / `calc` | 只读辅助 |

- **你的机器它看不到**：要它处理的文件要么已经在 ivan 主机上（绝对路径最稳），要么让它自己下载/生成。
- 具体能力随部署可能收窄（例如作业库不可用时没有 `schedule`）；以 agent 的实际回复为准。
- 涉及写操作、删除、部署、改配置时，ivan 会先用 `send_message` 找用户确认再动手，
  因此轮询可能长时间停在 `running`，**这不等于卡死**。

### 发文件（`send_file`）

- 能力：把 **ivan 所在主机上**的文件作为附件发给用户；一次 1–3 个；只做投递，不做传输。
- 路径：必须是 ivan 主机上存在、可读、非目录的普通文件（绝对路径最稳，相对路径按会话工作区解析）。
  **你自己机器上的文件发不了**——先放到 ivan 主机，或让 ivan 自己下载/生成。
- 大小：单文件上限 200MB；图片/视频/语音等媒体类另按 20MB，超出自动改按普通文件发送。
- 怎么触发：`/request` 正文里给出路径并说明要发，例如
  `把 /path/to/report.zip 发给用户`，agent 会调用 `send_file`。
- `/notify` **没有附件入口**（纯文本）。要发文件只能走 `/request`。

## 端点总览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/notify` | 发通知给用户（纯转发，不经过模型） |
| POST | `/request` | 提交 agent 任务（异步受理） |
| GET | `/request?session=<键>` | 轮询任务结果 |
| GET | `/healthz` | 健康检查 |
| GET | `/.well-known/service.json` | 服务自述（service-manifest/v1） |
| GET | `/guide.md` | 本指南 |

## POST /notify

把一条通知交给 ivan 投递给用户。尽力而为，不保证送达；投递目标由 ivan 决定，请求方不指定。

请求体（`source` 必填；`title` 与 `body` 至少一项非空）：

```json
{"source":"ci","title":"构建失败","body":"job #123 exit 1"}
```

- `source`（来源服务名，必填）正文末尾追加 `— <source>` 脚注
- 长度按字符计：`title` ≤ 200、`body` ≤ 8192，超出截断并回执 `truncated: true`
- `body` 缺省时可写 `text`（`body` 优先）
- **只能发文本**：没有附件入口，要发文件走 `/request`

回执：

```json
{"status":"delivered","truncated":false,
 "targets":[{"name":"…","channel":"…","result":"sent"}]}
```

- `status` 取值：`delivered`（全部送达）/ `partial`（部分送达，均 200）/ `failed`（未送达，503）
- `targets[].result` 取值：`sent`（已发）/ `failed`（发送失败，`error` 给原因）/ `no_targets`（当前没有可投递的收件人）
- **怎么处理**：`delivered`/`partial` 视为已送达，无需动作；`failed` 可退避重试；`no_targets` 重试无效，不要重试
- 超过限额（默认 30 次/分钟）429；缺 `source` 400
- 不做去重与分级：同样的内容重复提交就会重复投递，需要防重复请在调用侧自行控制
- 批量通知请自行聚合后发送，不要把 `/notify` 当日志管道

## POST /request

提交一个 agent 任务。**受理 ≠ 完成**：ivan 在会话中执行，结果经轮询获取。

请求体（`source` / `purpose` / `text` 必填，`identity` / `session` 可选）：

```json
{"source":"ci","identity":"build-bot@runner-1","purpose":"部署回报","text":"部署 release-2.3.1 到 staging，跑完冒烟测试回报结果"}
```

- 来源、身份、目的必须说清（`source` 服务名、`identity` 调用者身份、`purpose` 本次目的）
- 能委托什么见「能让 ivan 做什么」；要发文件就在 `text` 里写清 ivan 主机上的路径
- `session` 缺省按来源派生 `http:<source>`；同一来源复用同一会话（保留上下文）
- 受理回执（202）：`{"status":"accepted","session":"http:ci","id":"<请求 id>"}`
- 缺字段 400；超过限额（默认 5 次/分钟）429；ivan 正在停机 503

## GET /request?session=<键>

轮询任务结果（不限频、不记事件）：

- 进行中：`{"status":"running","session":"http:ci"}`
- 完成：`{"status":"done","session":"http:ci","reply":"<最终答复>","finished_at":"<RFC3339>"}`
- 失败：`{"status":"error","session":"…","reply":"…","finished_at":"…"}`
- 会话不存在或尚无回合结果：404 `{"status":"unknown","session":"…","message":"…"}`

读结果前请知道三条：

- 返回的是该会话**最近一次完成回合**的结果，**与受理回执里的 `id` 无关**
- 同一会话再发一次，后一次会**覆盖**前一次的查询结果：同一来源请**串行**（拿到结果再发下一次）；
  需要并发就为每次请求显式指定不同的 `session`（唯一性由你保证）
- 结果只在内存，**ivan 重启后旧会话一律 404 `unknown`**（即使当时已跑完）；重发请求即可

## GET /healthz

`{"status":"ok","channel":"http"}` —— 进程可应答即健康，服务目录探活打这里。

## GET /.well-known/service.json

service-manifest/v1 自述（name / version / summary / capabilities / usage / endpoints / detail_url）。
地址字段用 `{{base}}` 占位符，由服务目录抓取时按实际地址展开；直接访问看到占位符属预期。

## 本地入口（非 HTTP）

ivan 所在主机上可经 CLI 走同一通知管线：`ivan notify --title=构建失败 --source=ci "job #123 exit 1"`。
