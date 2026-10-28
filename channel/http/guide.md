# ivan agent 接口指南

本指南供自动化 agent 使用 ivan 的外部 HTTP 入口。路径均为**去除 host 的相对路径**，
实际请求时拼接接入地址（见服务目录条目 `endpoints` 里 `request` / `notify` 的 url）。

## 能力边界

- ivan 是无人值守后台 agent；`/request` 为**异步**入口：受理即返回会话键，结果靠轮询，没有回调/推送
- 通知与请求结果的**出站目标由各通道的默认通知出口自决**（通道决定发给谁），请求方不能指定发给谁，也无需预先知道任何收件人标识
- 无鉴权，仅限内网使用，不要暴露公网
- JSON 请求需 `Content-Type: application/json`；错误统一 `{"status":"rejected","message":"<原因>"}` + 对应状态码

## 端点总览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/notify` | 通知转发（广播给 ivan 配置的目标） |
| POST | `/request` | 提交 agent 任务（异步受理） |
| GET | `/request?session=<键>` | 轮询任务结果 |
| GET | `/healthz` | 健康检查 |
| GET | `/.well-known/service.json` | 服务自述（service-manifest/v1） |
| GET | `/guide.md` | 本指南 |

## POST /notify

把一条通知广播给 ivan 的各通道通知出口。尽力而为，不保证送达。

请求体（`source` 必填；`title` 与 `body` 至少一项非空）：

```json
{"source":"ci","severity":"error","title":"构建失败","body":"job #123 exit 1","dedupe_key":"build-123"}
```

- `severity`：`info`（默认，无前缀）；其他值渲染为 `[<severity>] 标题`
- `source`（来源服务名，必填）正文末尾追加 `— <source>` 脚注
- 长度按字符计：`title` ≤ 200、`body` ≤ 8192，超出截断并回执 `truncated: true`
- `dedupe_key`：`source|dedupe_key` 去重窗口（默认 600s）内重复投递返回 `deduped`，不重复发送
- `body` 缺省时可写 `text`（`body` 优先）

回执：

```json
{"status":"delivered","deduped":false,"truncated":false,
 "targets":[{"name":"main","channel":"qq","result":"sent"}]}
```

- `status` 取值：`delivered` / `partial`（均 200）/ `failed`（503，含逐目标失败明细）/ `deduped`
- 回执 `targets[].name` 形如 `<通道名>:<通道内目标标识>`（由通道自决）
- 没有通道实现通知出口或全部无可投递目标时 503 `failed`；超过限额（默认 30 次/分钟）429；缺 `source` 400

## POST /request

提交一个 agent 任务。**受理 ≠ 完成**：ivan 在会话中执行，结果经轮询获取。

请求体（`source` / `purpose` / `text` 必填，`identity` / `session` 可选）：

```json
{"source":"ci","identity":"build-bot@runner-1","purpose":"部署回报","text":"部署 release-2.3.1 到 staging，跑完冒烟测试回报结果"}
```

- 来源、身份、目的必须说清（`source` 服务名、`identity` 调用者身份、`purpose` 本次目的）
- `session` 缺省按来源派生 `http:<source>`；同一来源复用同一会话（保留上下文）
- 受理回执（202）：`{"status":"accepted","session":"http:ci","id":"<请求 id>"}`
- 缺字段 400；超过限额（默认 5 次/分钟）429；ivan 正在停机 503

## GET /request?session=<键>

轮询任务结果（不限频、不记事件）：

- 进行中：`{"status":"running","session":"http:ci"}`
- 完成：`{"status":"done","session":"http:ci","reply":"<最终答复>","finished_at":"<RFC3339>"}`
- 失败：`{"status":"error","session":"…","reply":"…","finished_at":"…"}`
- 会话不存在或尚无回合结果：404 `{"status":"unknown","session":"…","message":"…"}`

## GET /healthz

`{"status":"ok","channel":"http"}` —— 进程可应答即健康，服务目录探活打这里。

## GET /.well-known/service.json

service-manifest/v1 自述（name / version / summary / capabilities / usage / endpoints / detail_url）。
地址字段用 `{{base}}` 占位符，由服务目录抓取时按实际地址展开；直接访问看到占位符属预期。

## 本地入口（非 HTTP）

ivan 所在主机上可经 CLI 走同一通知管线：`ivan notify --title=构建失败 --source=ci "job #123 exit 1"`。
