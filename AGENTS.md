# ivan

常驻的极简 agent：**特化在定时触发与计划任务**，全部输入输出收敛到 channel 抽象（IM 只是其中一个 channel）。与 tanya 相对——tanya 是人在终端里对话的前台工具，ivan 是无人值守的后台代理。模块路径 `github.com/LaoQi/ivan`。
本文件只记现行约束，细节见 `docs/`；变更记录一律写根目录 `CHANGELOG.md`（新条目置顶）。

> 状态：**P2 已实现**（`qqbot/` 协议包 + `channel/qq` 适配器 + 配置/daemon 接线；生产凭证真机出站已通、入站待确认）；下一步 P3（定时与计划任务）。阶段与验收见 `docs/plan.md` §8，实现记录见 `docs/qq-bot.md` §13、`docs/channels.md` §10 与 `docs/sessions.md` §9。

## 设计约束

- 定位只做三件事：通道、调度、会话；不做 Web UI、多渠道全家桶、语音/视频、skills/MCP、多租户
- 复用 tanya：以库方式依赖 `agent` 与 `tools`（本地 `../tanya`，`go.work` 联调）；**不复用** `repl`/`readline`/终端所有权（常驻无 TTY 场景）。需要的能力缺口在 tanya 侧小步补齐，两仓库 CHANGELOG 同步
- 通道是一等公民：一切输入输出都实现为 `Channel`（IM、本地 CLI、日志、定时触发源）；通道之间互不认识，也不认识业务；所有输入都是 inbound 消息，区别只在 `Channel` 字段
- **抽库纪律**：`bus` 只依赖标准库；`channel`（含子包）只依赖 `bus`；`qqbot`/`schedule` 只依赖标准库与自身类型。四者都不得依赖 ivan 其他包或 tanya（`bus`+`channel` 抽库时成对抽出）
- 协议与业务的分界固定在 `qqbot/` 边界：协议细节（token 刷新、重连、心跳、限流、错误码）留在协议包内；`channel/qq` 只做消息翻译、图片落盘、`msg_seq` 计数
- 依赖极简：新增依赖先讨论；QQ 协议**自实现**（不用 `tencent-connect/botgo`，D3）；WebSocket 用 `github.com/gorilla/websocket`
- `qqbot/` 只依赖标准库 + `gorilla/websocket`，不得依赖 ivan 其他包或 tanya（抽库对象）
- 事件 sink 不得阻塞：tanya 的事件是同步串行投递，落盘/出站一律走带缓冲 channel + 独立 goroutine
- 缓存不变性：动态内容（记忆、目标、状态、作业结果）经 user 消息注入，不改 system prompt 快照
- 无人值守安全：预算熔断、暂停开关、单实例锁是硬性交付项（不得后置）；危险命令最小黑名单拦截；AppSecret 不进日志/事件/错误信息
- 持久化一律原子写（临时文件 + fsync + rename）；解析失败保留原文件并显式报错，**不静默重建**
- 启动拒绝 root（复用 `ctty.IsRoot()`），逃生舱口径与 tanya 保持一致
- 部署纪律：部署态只做只读探测；功能验证在本地或临时环境完成
- **会话目录按会话键隔离**：每个 `SessionKey` 一个工作区目录 `<data_dir>/agents/<key-slug>/`（tanya 会话 id 为秒级时间戳，多实例共享工作区会互相覆盖，见 `docs/reuse-probe.md` R11）
- **`tools.Options.Workspace` 必须是动态闭包**指向 `agent.Workspace()`，不得用静态 `os.Getwd()` 值（`SwitchWorkspace` 不会自动同步，见 R7）
- **会话映射持久化在 `state.json`**（原子写，损坏即报错拒绝启动，不静默重建）；实例池懒建、同键串行、跨键并发上限、按 LRU/闲置回收
- **单实例锁**：`<data_dir>/ivan.lock` 用 flock（释放不解锁文件，避免 unlink 竞争）；daemon 停机后清理 socket 文件
- **出站无目标不算失败**：`channel.ErrUnroutable`（如 `--no-wait` 的 cli 请求）只记 `unmatched`，不重试；失败才重试一次并记 `failed`+`last_error`
- **cli 通道不切分长消息**（`MaxLen()==0`），切分只对声明上限的通道（默认 1200 字符）生效
- **`ivan run` 不走总线**：前台直接流式打到 stdout（`Options.Stream`），不起 daemon；`Bus==nil` 时跳过出站投递
- 配置借用：ivan 的 `api_key` 为空时从 `~/.config/tanya/config.yaml`（可经 `IVAN_TANYA_CONFIG` 指定）借用 `base_url`/`api_key`/`model`；`IVAN_*` 显式设置优先
- 代码不添加注释，除非用户明确要求

## 结构（现行，见 `docs/plan.md` §5）

```
main.go               CLI：run / daemon / send / status / config / version；--session 指定会话键 [已实现]
commands.go           子命令实现：flag 解析、状态渲染、磁盘回退、信号上下文 [已实现]
config/               ivan 配置（内嵌 agent.Config + runner/daemon/channels 段；yaml + IVAN_*；tanya 凭证借用）[已实现]
bus/                  消息模型 + Bus（两条缓冲流、非阻塞投递、丢弃计数、关闭语义）+ 会话键归一/KeySlug [已实现，零依赖]
channel/              Channel 接口 + Manager（路由、清理、切分、重试、状态）+ ErrUnroutable [已实现]
channel/cli/          cli 通道：unix socket（JSON Lines，send/status/ping）+ 客户端（ivan send/status 用）[已实现]
channel/log/          人类可读 Sink（io.Writer，stdlib 依赖）[已实现]
data/                 EventLog（结构化 events jsonl）/ State（state.json 原子写）/ Lock（flock）/ LogFile / ProcessAlive [已实现]
prompt/               内置 system 提示词（system_prompt.md 嵌入）+ 环境段 [已实现]
runner/               Options/Dispatch/Run/Status + pool（实例池、懒建、LRU/闲置回收）+ 回合驱动与事件分流 [已实现]
daemon/               daemon.New/Run（装配、优雅停机顺序、Control(status)）+ Status 结构 [已实现]
probe/                复用探针 R1–R11（假 LLM 驱动）[已实现]
internal/fakellm/     本地假 LLM（SSE，chat 协议）供探针与测试用 [已实现]
qqbot/                QQ 协议客户端：token 刷新 / gateway+ws 生命周期 / REST 收发 / 去重 / 错误分类（仅依 stdlib + gorilla/websocket）[已实现]
channel/qq/           qq 适配器：事件→bus.InboundMessage、出站翻译、msg_seq 计数、图片下载/上传、白名单 [已实现]
schedule/             规划中（见 docs/plan.md §5，P3）
```

## 文档

- `docs/plan.md` 总纲：定位、架构、抽库判据、阶段路线、决策记录 D1–D7、风险红线
- `docs/reuse-tanya.md` 复用清单、P0 探针 R1–R8、需 tanya 配合的改动 T1–T3
- `docs/channels.md` 通道抽象、Bus 与消息模型、会话键、cli/log 通道、切分与入站安全
- `docs/qq-bot.md` QQ 官方机器人协议设计、分层与抽库路径、测试策略、待核对清单
- `docs/schedule.md` 作业模型、调度循环、`schedule` 工具、结果回灌、验收
- `docs/sessions.md` 会话键→实例、映射持久化、回合结算与自主推进、并发背压、重启恢复
- `docs/refs-picoclaw.md` 参考调研快照与取舍

## 构建与测试

```bash
go build ./... && go vet ./...              # 构建 + 静态检查
go test ./... && go test -race ./...        # 全量测试 + 竞态（probe/runner/daemon 用假 LLM，无需网络）
go test ./probe/ -v -timeout 120s           # 复用探针细项（R1–R11）
go run . run "你好"                          # 前台单发冒烟（需 api_key，可借用 tanya 配置）

# daemon 冒烟（临时数据目录，别动正式目录）
IVAN_DATA_DIR=/tmp/ivan-smoke go run . daemon &
IVAN_DATA_DIR=/tmp/ivan-smoke go run . send "用 run_shell 跑 pwd 并只回答目录名"
IVAN_DATA_DIR=/tmp/ivan-smoke go run . status
pkill -TERM -x ivan                      # 优雅停机（socket 与锁自动收尾）
```

依赖：`github.com/LaoQi/tanya` 经 `go.work`（`use ./ ../tanya`）与 `go.mod` 的 `replace ../tanya` 本地联调。
