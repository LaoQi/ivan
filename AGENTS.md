# ivan

常驻的极简 agent：**特化在定时触发与计划任务**，全部输入输出收敛到 channel 抽象（IM 只是其中一个 channel）。与 tanya 相对——tanya 是人在终端里对话的前台工具，ivan 是无人值守的后台代理。模块路径 `github.com/LaoQi/ivan`。
本文件只记现行约束，细节见 `docs/`；变更记录一律写根目录 `CHANGELOG.md`（新条目置顶）。

> 状态：**P0–P4 已完成**（P4：两档阈值自动交接 建议 180K / 强制 256K、三档预算熔断（轮次/任务/日）+ 跨进程日用量、暂停开关只挡无人值守路径、记忆/目标按需读取、`ivan cancel`；P4 遗留的崩溃回合 `interrupted` 标记与记忆文件裁剪已补齐）。P0–P3：骨架与探针、通道与 daemon、QQ 真机全链路、定时与计划任务（`schedule` + sqlite + QQ 真机提醒）。**P5（安全与运维：审批档、危险命令规则、限额、日志轮转、systemd、`ivan doctor`）为保留计划，暂无实施规划**——相关子项均未实施，不要按"已交付"引用。阶段与验收见 `docs/plan.md` §8，实现记录见 `docs/sessions.md` §10、`docs/schedule.md` §8、`docs/qq-bot.md` §13、`docs/channels.md` §10。

## 设计约束

- 定位只做三件事：通道、调度、会话；不做 Web UI、多渠道全家桶、语音/视频、skills/MCP、多租户
- 复用核心库 `github.com/LaoQi/battlelab`（`agent`/`tools`/`ctty`，本地 `../battlelab`，`go.work` 联调；2026-10-22 从 tanya 抽出）；**不复用** tanya 的 `repl`/`readline`/`views` 与任何终端所有权（常驻无 TTY 场景）。库侧能力缺口在 battlelab 小步补齐，两仓 CHANGELOG 同步
- **请求重试**：由库侧 `agent.Config.MaxRetries` 提供（`max_retries`，默认 2、`IVAN_MAX_RETRIES` 覆盖、`0` = 关闭、上限 20 越界回落），重试在库的 `Client.ChatStream` 内层完成、已输出内容后失败不再重试；无人值守路径同样生效（重试期间对通道静默），事件日志落 `retry` 条目（含错误原文与 attempt/max/delay），预算口径不变——**不要再包一层自己的重试**
- 通道是一等公民：一切输入输出都实现为 `Channel`（IM、本地 CLI、日志、定时触发源）；通道之间互不认识，也不认识业务；所有输入都是 inbound 消息，区别只在 `Channel` 字段
- **抽库纪律**：`bus` 只依赖标准库；`channel`（含子包）只依赖 `bus`；`qqbot`/`schedule` 只依赖标准库与自身类型。四者都不得依赖 ivan 其他包或 battlelab/tanya（`bus`+`channel` 抽库时成对抽出）
- **存储抽象**：`schedule.Store` 是消费者定义的窄接口（`List/Get/Put/Delete/Close`，实现须并发安全）；具体存储放子包，**驱动依赖隔离在 `schedule/sqlite`**，`schedule` 本体仍只依赖标准库
- **文本长度一律按字符（rune）计**：`bus.RuneLen/Clip/Truncate/OneLine` 是唯一口径（`clip`/`options`/`brief`/`oneLine`/`quote`/`shortID`/`KeySlug`/`taskSlug` 一律按字符截断）；**不得用 `len(s)` 判定文本长度、不得用 `s[:n]` 切文本**（中文 3 字节/字，字节判定配 rune 切片会 panic 或切出乱码）——字节口径只用于文件大小（`data.TrimFileTail`）；`schedule` 只依赖标准库，包内自持同语义实现
- **富文本出站由通道声明**：通道决定出站形态与语法子集（QQ `channels.qq.markdown` 默认开，只在正文含 markdown 语法特征时走 `msg_type=2`），降级（表格→要点行、围栏→缩进、本地图片→文字）与被平台拒收后的纯文本回退都在 `channel/qq` 内；长文本切分经可选能力 `channel.Splitter` 由通道接管（markdown 感知 + 补闭合围栏），Manager 不感知平台语法
- 协议与业务的分界固定在 `qqbot/` 边界：协议细节（token 刷新、重连、心跳、限流、错误码）留在协议包内；`channel/qq` 只做消息翻译、图片落盘、`msg_seq` 计数
- 依赖极简：新增依赖先讨论；QQ 协议**自实现**（不用 `tencent-connect/botgo`，D3）；WebSocket 用 `github.com/gorilla/websocket`；作业存储用 `modernc.org/sqlite`（纯 Go 无 cgo，D8）
- `qqbot/` 只依赖标准库 + `gorilla/websocket`，不得依赖 ivan 其他包或 battlelab/tanya（抽库对象）
- 事件 sink 不得阻塞：库侧（battlelab `agent`）的事件是同步串行投递，落盘/出站一律走带缓冲 channel + 独立 goroutine
- **中间态投递（更新型出站）**：长回合的"还在跑"反馈实现为**同一条消息的原位更新**（`bus.OutboundMessage.Stream`/`Final`，`channel.StreamOpener`/`Typist` 可选能力），**不做逐里程碑进度消息**；正文就绪前用 `KindTyping`（QQ `msg_type=6`、cli typing 帧）覆盖静默期，正文就绪后流式跟进，超出 `runner.stream_max_chars` 收尾并补发余下内容；不支持流式的通道自动退化（中间态丢弃、终局照常），中间态不计预算、失败只计数不重试
- 缓存不变性：动态内容（记忆、目标、状态、作业结果）经 user 消息注入，不改 system prompt 快照
- **全局说明与实际隔离**：system prompt 的"全局说明"段只读 ivan 自己的 `~/.config/ivan/AGENTS.md`（配置项 `global_agents`，`off` 禁用），**不读 tanya 的 `~/.config/tanya/AGENTS.md`**（经 battlelab `agent.WithGlobalAgentsPath` 注入；机制保留、位置隔离）
- 无人值守安全：预算熔断、暂停开关、单实例锁是硬性交付项（不得后置）；AppSecret 不进日志/事件/错误信息。**危险命令黑名单与审批档属 P5 保留计划，当前未实施**（缓解只有限定工作区 + 预算 + 暂停 + 事件审计）
- 崩溃回合：`DispatchTurn` 期间在 `state.json` 留 `running` 标记，启动时 `DrainRunning` 清残留、记 `turn_interrupted`、并在该会话下一次回合注入"上次回合被中断、先核对实际状态"的提示（只注入一次）
- 记忆裁剪：`runner.memory_max_bytes`（默认 0=不裁剪），回合结束后按行边界保留尾部 + 头部裁剪标记（`data.TrimFileTail`）
- 交接阈值两档（绝对值，`handoff_hint_tokens` 180K / `handoff_force_tokens` 256K）：建议档只提示、强制档要求立即交接且不交接就停（不再发请求）；两档每任务各触发一次，且都受暂停/预算闸门
- 预算只在一处统计：`runner`（`max_turns_per_task`/`max_tokens_per_task`/`max_tokens_per_day`，日用量落 `state.json` 跨进程累计），调度器用注入的 `Budget` 回调复用；不要再加第二个预算口径
- 暂停语义：`paused` 只挡**无人值守路径**（定时作业触发、交接后自动续跑、阈值交接提示），用户主动消息照常处理
- 记忆与目标**按需读取**：路径写进 system prompt（静态、保缓存），模型自己 `run_shell` 读/追加，不做每轮注入、不建专用工具
- 调度类型：`at` / `every`（固定间隔）/ `daily`（每天固定时刻 `at_time_ms` + `tz`，按自然日锚定、不漂移）；**不做通用 cron 表达式**（`expr` 字段保留但 `Validate` 报错）
- 任务提示词落文件：`add` 时写 `<data_dir>/tasks/<job_id>.md`（0600 原子写）并由 `payload.file` 引用，触发**优先读文件**、不可读则回退 `payload.prompt` 并记 `job_task_file`；`update` 同步重写、`remove` 删自有文件
- 定时作业：触发由 runner **同步**执行（`Runner.TriggerJob`），作业状态以真实回合结果为准；出站按 `InboundMessage.ReplyChannel` **直投**（不走 Bus）以便拿到投递错误；作业目标会话只由 runner 注入，模型不可指定；配额默认不限
- 持久化一律原子写（临时文件 + fsync + rename）；解析失败保留原文件并显式报错，**不静默重建**
- 启动拒绝 root（复用 battlelab `ctty.IsRoot()`），逃生舱口径与 tanya 保持一致
- 部署纪律：部署态只做只读探测；功能验证在本地或临时环境完成。生产部署的具体形态（主机、路径、单元、作业）属于部署方环境，**不写进本仓库**；生产上的写操作允许直接执行，但要有明确目的、做完收拾干净
- **会话目录按会话键隔离**：每个 `SessionKey` 一个工作区目录 `<data_dir>/agents/<key-slug>/`（库侧会话 id 为秒级时间戳，多实例共享工作区会互相覆盖，见 `docs/reuse-probe.md` R11）
- **`tools.Options.Workspace` 必须是动态闭包**指向 `agent.Workspace()`，不得用静态 `os.Getwd()` 值（`SwitchWorkspace` 不会自动同步，见 R7）
- **会话映射持久化在 `state.json`**（原子写，损坏即报错拒绝启动，不静默重建）；实例池懒建、同键串行、跨键并发上限、按 LRU/闲置回收
- **单实例锁**：`<data_dir>/ivan.lock` 用 flock（释放不解锁文件，避免 unlink 竞争）；daemon 停机后清理 socket 文件
- **出站无目标不算失败**：`channel.ErrUnroutable`（如 `--no-wait` 的 cli 请求）只记 `unmatched`，不重试；失败才重试一次并记 `failed`+`last_error`
- **cli 通道不切分长消息**（`MaxLen()==0`），切分只对声明上限的通道（默认 2048 字符）生效
- **`ivan run` 不走总线**：前台直接流式打到 stdout（`Options.Stream`），不起 daemon；`Bus==nil` 时跳过出站投递
- 配置借用：ivan 的 `api_key` 为空时从 `~/.config/tanya/config.yaml`（可经 `IVAN_TANYA_CONFIG` 指定）借用 `base_url`/`api_key`/`model`；`IVAN_*` 显式设置优先
- 代码不添加注释，除非用户明确要求

## 结构（现行，见 `docs/plan.md` §5）

```
main.go               CLI：run / daemon / send / status / config / version；--session 指定会话键 [已实现]
commands.go           子命令实现：flag 解析、状态渲染、磁盘回退、信号上下文 [已实现]
config/               ivan 配置（内嵌 battlelab agent.Config + runner/daemon/channels 段；yaml + IVAN_*；tanya 凭证借用）[已实现]
bus/                  消息模型 + Bus（两条缓冲流、非阻塞投递、丢弃计数、关闭语义）+ 会话键归一/KeySlug [已实现，零依赖]
channel/              Channel 接口 + Manager（路由、清理、切分、重试、状态）+ 可选能力（Limiter/StreamOpener/Typist/StreamLimit/Splitter）+ ErrUnroutable [已实现]
channel/cli/          cli 通道：unix socket（JSON Lines，send/status/ping/chunk/typing）+ 客户端（ivan send/status 用，TTY 下显示增量）[已实现]
channel/log/          人类可读 Sink（io.Writer，stdlib 依赖）[已实现]
data/                 EventLog（结构化 events jsonl）/ State（state.json 原子写，含会话映射、暂停、日用量、回合进行中标记）/ Lock（flock）/ LogFile / ProcessAlive / TrimFileTail（按行裁剪）[已实现]
prompt/               内置 system 提示词（system_prompt.md 嵌入）+ 环境段（含会话键、记忆文件、目标文件路径）；全局说明由 `config.global_agents` 注入（默认 `~/.config/ivan/AGENTS.md`）[已实现]
runner/               Options/Dispatch/Run/Status + pool（实例池、懒建、LRU/闲置回收、回合取消）+ 回合驱动与事件分流 + 阈值交接提示/预算暂停闸门/schedule 工具 + 崩溃回合恢复（`interrupted`）/记忆裁剪 [已实现]
daemon/               daemon.New/Run（装配、优雅停机顺序、Control(status)）+ Status 结构 [已实现]
probe/                复用探针 R1–R11（假 LLM 驱动）[已实现]
internal/fakellm/     本地假 LLM（SSE，chat 协议）供探针与测试用 [已实现]
qqbot/                QQ 协议客户端：token 刷新 / gateway+ws 生命周期 / REST 收发 / 流式消息与输入状态 / 去重 / 错误分类（仅依 stdlib + gorilla/websocket）[已实现]
channel/qq/           qq 适配器：事件→bus.InboundMessage、出站翻译、markdown 出站与降级（md.go）、msg_seq 计数、图片下载/上传、白名单 [已实现]
schedule/             作业模型（at/every）、存储抽象 Store + 进程内实现、调度循环（wake/timer、错过策略、暂停与预算闸门）[已实现]
schedule/sqlite/      作业库实现（modernc.org/sqlite，WAL+FULL、user_version 迁移、损坏不重建）[已实现]
```

## 文档

- `docs/plan.md` 总纲：定位、架构、抽库判据、阶段路线、决策记录 D1–D7、风险红线
- `docs/reuse-core.md` 复用清单、P0 探针 R1–R8、需库侧配合的改动 T1–T3
- `docs/channels.md` 通道抽象、Bus 与消息模型、会话键、cli/log 通道、切分与入站安全
- `docs/qq-bot.md` QQ 官方机器人协议设计、分层与抽库路径、测试策略、待核对清单
- `docs/schedule.md` 作业模型、调度循环、`schedule` 工具、结果回灌、验收
- `docs/sessions.md` 会话键→实例、映射持久化、回合结算与自主推进、并发背压、重启恢复
- `docs/refs-picoclaw.md` 参考调研快照与取舍

## 构建与测试

```bash
go build ./... && go vet ./...              # 构建 + 静态检查（首次需拉 modernc.org/sqlite 及其依赖）
go test ./... && go test -race ./...        # 全量测试 + 竞态（probe/runner/daemon 用假 LLM，无需网络）
go test ./probe/ -v -timeout 120s           # 复用探针细项（R1–R11）
go run . run "你好"                          # 前台单发冒烟（需 api_key，可借用 tanya 配置）

# daemon 冒烟（临时数据目录，别动正式目录）
IVAN_DATA_DIR=/tmp/ivan-smoke go run . daemon &
IVAN_DATA_DIR=/tmp/ivan-smoke go run . send "用 run_shell 跑 pwd 并只回答目录名"
IVAN_DATA_DIR=/tmp/ivan-smoke go run . status
IVAN_DATA_DIR=/tmp/ivan-smoke go run . jobs        # 作业列表（直接读作业库）
IVAN_DATA_DIR=/tmp/ivan-smoke go run . pause       # 暂停无人值守触发（定时作业 + 自动续跑）
IVAN_DATA_DIR=/tmp/ivan-smoke go run . cancel cli:default   # 取消某会话在跑的回合
IVAN_DATA_DIR=/tmp/ivan-smoke go run . send "说点长的"       # TTY 下原地刷新显示增量（重定向则只出最终答案）
pkill -TERM -x ivan                      # 优雅停机（socket 与锁自动收尾，作业库随进程关闭）
```

依赖：`github.com/LaoQi/battlelab` 经 `go.work`（`use ./ ../battlelab`）与 `go.mod` 的 `replace ../battlelab` 本地联调（**已不直接依赖 tanya**，仅运行期借用其配置文件）；需 **Go ≥ 1.26**（`modernc.org/sqlite v1.60.1` 的 go 指令要求，`go.mod`/`go.work` 已同步）。
