# 外部入口（通知中心与 agent 请求）

> 状态：**已实现**（`channel/http` 通道 + `send_message` 工具 + `ivan notify` 子命令；实现记录见 §7）。这一节把 ivan 从"只有 IM 与定时触发"扩展成**内网通知中心**：其他服务能发通知给用户，也能找 agent 办事。

## 1 定位与两个端点

一个监听、两个端点，**路径即语义**，不做模式开关：

| 端点 | 语义 | 走 agent 回合 | 计 token |
|---|---|---|---|
| `POST /notify` | 纯转发：把通知投给用户 | 否 | 否 |
| `POST /request` | 与 agent 沟通：受理即返回会话键 | 是 | 是 |
| `GET /request?session=` | 查询该会话的状态与回合结果 | 否 | 否 |
| `GET /healthz` | 存活探测 | 否 | 否 |

两者的差别不是配置项，而是**一个只出站、一个只进站**：`/notify` 不需要回复也不该慢；`/request` 下游不选出口、不收推送——发请求、拿会话键、自己轮询。回合的出站（终局回复、确认问题、告警）一律**尽力而为广播到可用渠道**，不定向回调用方。

**为什么归 Trigger 层而不是 `Channel` 层**（术语修订）：`Channel` 接口只拿得到 `Bus`，出站只能 `bus.Post`（丢弃不可知、拿不到投递错误），而 `/notify` 必须能给调用方回执。所以它和 `schedule` 同属"产出/转发 + 同步回执"这一类，按 `docs/plan.md` §4 的三分法归 Trigger 层；实现上仍复用 `channel.Manager.Route` 做真实投递，只是**直投而不经总线**（与 `runner.TriggerJob` 同构）。

## 2 `POST /notify`

请求体（JSON；也接受 `text/plain`，正文即 `body`）：

```json
{
  "title": "构建失败",
  "body": "job #123 exit 1",
  "severity": "error",
  "source": "ci",
  "dedupe_key": "job-123"
}
```

- **只有这些字段**：没有目标、没有 route、没有 session。发到哪由 ivan 自己的 `http.targets` 决定（**全部 fan-out**）；对其他服务而言只是"我有个通知"，不关心也不该关心 ivan 有哪些通道。
- `severity` 为空或 `info` 不加前缀，其余渲染为 `[error] 构建失败`；`source` 非空时正文末尾追加 `— ci` 脚注。
- 长度：`title` ≤ 200 字符、`body` ≤ 8192 字符（**按 rune 计**），超出截断并在回执里给 `truncated: true`。
- 去重：`source|dedupe_key` 内存 TTL 表（`http.dedupe_seconds`，默认 600s），命中返回 `200 {"status":"deduped"}` 且不投递。**重启丢失**（不落盘，通知是 best-effort）。
- 限额：`http.notify_per_minute`（默认 30）滑动窗口，超限 `429`。
- 回执逐目标列出结果：

```json
{"status":"delivered","deduped":false,"truncated":false,
 "targets":[{"name":"main","channel":"qq","result":"sent"},
            {"name":"ops","channel":"cli","result":"failed","error":"通道 cli 当前没有可投递目标"}]}
```

`status` 取值：`delivered`（全部成功）/ `partial`（部分成功，HTTP 200）/ `failed`（全失败，HTTP 503）/ `deduped`。**任一目标失败不影响其余**。

**"没有可投递目标"要如实回报**：`Manager.Route` 对无目标的通道只计 `unmatched` 并返回 `nil`（cli 请求连接已关时就是这样），通知回执若照搬就会撒谎说"已投递"。所以 daemon 注入的是 `deliverStrict`——调 `Route` 前后比对 `unmatched` 计数，涨了就返回 `channel.ErrUnroutable`（比对在互斥量内串行，计数差可靠；通知投递低频，吞吐不是目标）。

## 3 `POST /request`

请求体：

```json
{
  "source": "ci",
  "identity": "build-bot@runner-1",
  "purpose": "把测试环境部署到新版本",
  "text": "部署 release-2.3.1 到 staging，跑完冒烟测试回报结果",
  "session": ""
}
```

- `source`（必填，服务名）、`purpose`（必填，这次请求的目的）、`text`（必填，要 agent 做的事）、`identity`（可选，调用者身份）——**来源、身份、目的必须说清**，这是 agent 判断影响面的依据。
- 拼成注入给模型的用户消息（与用户本人输入完全同一条路径）：

```
[外部服务请求] 来源=ci 身份=build-bot@runner-1 目的=把测试环境部署到新版本
部署 release-2.3.1 到 staging，跑完冒烟测试回报结果
```

- 会话键：`session` 显式指定 > `http.request.session` 配置 > 默认 `http:<source-slug>`。**默认按来源派生**，每个服务一个连续上下文、互不串味；想让所有外部请求进同一上下文就配 `session: cli:default`。
- **异步、不回执**：提交即受理，返回会话键；不挂连接、没有等待超时、结果也不定向回调用方：

```json
202 {"status":"accepted","session":"http:ci","id":"req-1234-…"}
```

**`GET /request?session=<会话键>`**：

```json
{"status":"running","session":"http:ci"}
{"status":"done","session":"http:ci","reply":"已部署，冒烟通过","finished_at":"2026-10-24T01:00:00+08:00"}
{"status":"error","session":"http:ci","reply":"error: 上游不可用","finished_at":"…"}
404 {"status":"unknown","message":"会话不存在或尚无回合结果"}
```

- `running`＝该会话有回合在跑；`done`/`error`＝最近一次回合的终局正文（失败时含原因）；`unknown`＝会话不存在或尚无结果。查询不限频；结果是 runner 内存态，进程重启后回 `unknown`，调用方重发即可。
- **结果与投递解耦**：查询返回的是回合真实终局文本（runner 结果表，回合结束时写入一次），不依赖任何通道投递成功。
- 总线满或停机 `503`；缺字段 `400`；超限 `429`（`http.request_per_minute`，默认 5——每条都是一次真实模型回合）。
- **确认流程异步**：agent 需要用户确认时用 `send_message` 广播（见 §4）；用户答复走用户自己的会话，不会自动接回 `http:ci` 上下文。agent 会把待确认的问题写进回合终局正文，调用方查询即可看到"在等确认"。

## 4 `send_message` 工具

给任何会话里的 agent 一个主动出站口：

```
send_message(text)
```

- **不选择出口**：广播到配置 `http.targets` 的全部目标，尽力而为。目标是人在配置里登记的，模型不能给任意账号发消息——这是本能力的底线。
- **投递汇总如实**：全部成功 `ok: 已广播到 N 个目标（…）`；部分成功 `partial: 已发 N/M 个目标（成功：…；失败：…原因）`；全部失败 `error: 全部目标投递失败：…`。投递经 daemon 的 `deliverStrict`，"通道当前没有可投递目标"（如 cli 目标无在等请求）如实计入失败，不谎报 ok，模型可据此改口。
- `Kind = bus.KindNotice`；出站切分、markdown、重试仍由 `Manager` 与各通道负责。
- 未配置 `http.targets` 时工具仍注册（返回"没有可用的输出目标"），避免模型以为没有这个能力而改用 run_shell 绕路。

## 5 `ivan notify`（本地入口）

```bash
ivan notify --title=构建失败 --severity=error --source=ci "job #123 exit 1"
echo "磁盘 92%" | ivan notify --source=node-exporter
```

- 走现有 daemon socket（新 op `notify`，arg 为 JSON），**复用同一 fan-out 管线**；正文取参数，无参数且 stdin 非 TTY 时读 stdin。
- 用途：本地 cron、systemd 单元、脚本——不必知道 HTTP 端口，也不需要在每台机器上装 HTTP 客户端。
- daemon 未运行时明确报错（`cli.Call` 返回 `ErrNotRunning`），不静默失败。
- `http.enabled=false` 时返回"HTTP 入口未启用"——通知管线与入口同开关，不搞两套。

## 6 配置与运维

```yaml
http:
  enabled: true             # 默认开；targets 为空时 /notify 明确报错、/request 照常可用
  listen: 127.0.0.1:8787    # IVAN_HTTP_LISTEN 可覆盖
  max_body_bytes: 65536
  dedupe_seconds: 600       # 0 = 关闭去重
  notify_per_minute: 30
  request_per_minute: 5
  targets:
    - {name: main, channel: qq, chat_id: "<openid>"}
  request:
    session: ""             # 空 = 按来源派生 http:<source>
```

- **鉴权：当前不做**（仅内网）。入口默认开启、默认监听 `127.0.0.1`；内网要可达就改 `listen`。**切勿暴露公网**——将来要暴露再加鉴权，不预留半成品字段。
- `targets` 的通道需支持主动投递：QQ 要 `channels.qq.allow_proactive: true`（平台频控 + 用户可关"允许主动发送" → **通知恒为 best-effort**）；`cli` 目标只有在有在等请求时才投得出去，一般不用作通知目标。
- `targets` 为空是合法状态：`/request` 与查询端点不依赖目标照常可用；`/notify` 与 `ivan notify` 返回明确的"未配置通知目标"（503），`send_message` 对模型同样明确报错。`ValidateHTTP` 只校验格式（缺 name/channel、重名拒绝启动）。
- 端口被占会**启动失败**（默认 `127.0.0.1:8787`；改 `listen` 或显式 `enabled: false` 关闭入口）。
- 观测：`ivan status` 的 `http` 行给 `notify=`（受理）/`delivered`/`partial`/`failed`/`deduped`/`rejected` 与 `request=`（受理）/`rejected`，以及 `last_error`；结构化事件 kind：`ext_notify`/`ext_notify_deduped`/`ext_notify_rejected`/`ext_request`/`ext_request_rejected`（均含 source 与结果，不含正文）。查询端点不记事件、不限频。
- 停机：HTTP 监听随 daemon 启停；已受理的回合按 runner 停机语义收尾（等待或取消），查询端点随进程消失（连接拒绝），未落任何回执悬挂。

## 7 实现记录

代码：`channel/http/`（`http.go` 入口与提交/查询端点、`forward.go` 通知 fan-out 与格式化）、`runner/runner.go`（每会话最近回合结果表 `SessionState`，回合结束时写入一次，与投递解耦）、`runner/notifytool.go`（`send_message` 广播）、`daemon/`（装配 `Query` 回调、`NotifyStrict`=`deliverStrict`、status 段、socket op）、`commands.go`（`ivan notify` + status 渲染）、`config/`（`HTTPConfig` + `ValidateHTTP`）。依赖纪律：`channel/http` 只依赖 `bus`/`channel` + 标准库（自己的 `Config`/`Target`/`RequestState`，daemon 负责转换与注入查询回调），不 import runner；`send_message` 读 `cfg.HTTP.Targets`、走 `Options.NotifyStrict`（回落 `Notify`）逐目标投递并汇总——**不动 `Manager`、不动 runner 核心路径**；`http` 通道的 `Send` 恒为 `ErrUnroutable`（回合回复落空即尽力而为语义，无人等待不算失败）。

测试：`channel/http`（fan-out 到全部目标、部分失败与全失败的逐目标结果、纯文本入口、空内容拒绝、去重与 source 隔离、限额、截断与体积上限、提交受理返回会话键与入站字段、查询四态 running/done/error/unknown 与缺参 400、缺字段校验、总线满 503、限额与方法、`Send` 恒 unroutable、`SendNotice` 本地入口、`formatNotice` 边界、统计计数）、`runner`（广播全部目标、部分失败如实 partial、全部失败报错、回落 `Notify`、参数校验、无目标明确报错、`SessionState` 结果表与未知会话）、`daemon`（HTTP 通知逐目标结果含"没有可投递目标"、`/request` 异步受理 + 轮询拿到回合终局正文 + 会话映射落盘、回合里 `send_message` 广播且不可达目标如实回报给模型、socket op `notify` 回执与未启用报错、默认不启用）、`config`（默认值、校验矩阵、YAML/env 覆盖、启用无 targets 拒绝启动）。

验证：`gofmt -l` 干净、`go build ./...`、`go vet ./...`、`go test ./...` 全绿（含 `-race`）。
