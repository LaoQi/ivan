# ivan

常驻的极简 agent：**特化在定时触发与计划任务**，全部输入输出收敛到 channel 抽象（IM 只是其中一个 channel）。与 tanya 相对——tanya 是人在终端里对话的前台工具，ivan 是无人值守的后台代理。模块路径 `github.com/LaoQi/ivan`。
本文件只记现行约束，细节见 `docs/`；变更记录一律写根目录 `CHANGELOG.md`（新条目置顶）。

> 状态：**规划阶段，尚无代码**。开工顺序与验收见 `docs/plan.md` §8。

## 设计约束

- 定位只做三件事：通道、调度、会话；不做 Web UI、多渠道全家桶、语音/视频、skills/MCP、多租户
- 复用 tanya：以库方式依赖 `agent` 与 `tools`（本地 `../tanya`，`go.work` 联调）；**不复用** `repl`/`readline`/终端所有权（常驻无 TTY 场景）。需要的能力缺口在 tanya 侧小步补齐，两仓库 CHANGELOG 同步
- 通道是一等公民：一切输入输出都实现为 `Channel`（IM、本地 CLI、日志、定时触发源）；通道之间互不认识，也不认识业务；所有输入都是 inbound 消息，区别只在 `Channel` 字段
- **抽库纪律**：`qqbot`/`bus`/`channel`/`schedule` 四个候选包只依赖标准库与自身类型，不得依赖 ivan 其他包或 tanya——保证后续抽成独立 module 时只改 import 路径与 `go.mod`
- 协议与业务的分界固定在 `qqbot/` 边界：协议细节（token 刷新、重连、心跳、限流、错误码）留在协议包内；`channel/qq` 只做消息翻译、图片落盘、`msg_seq` 计数
- 依赖极简：新增依赖先讨论；QQ 协议**自实现**（不用 `tencent-connect/botgo`，D3）
- 事件 sink 不得阻塞：tanya 的事件是同步串行投递，落盘/出站一律走带缓冲 channel + 独立 goroutine
- 缓存不变性：动态内容（记忆、目标、状态、作业结果）经 user 消息注入，不改 system prompt 快照
- 无人值守安全：预算熔断、暂停开关、单实例锁是硬性交付项（不得后置）；危险命令最小黑名单拦截；AppSecret 不进日志/事件/错误信息
- 持久化一律原子写（临时文件 + fsync + rename）；解析失败保留原文件并显式报错，**不静默重建**
- 启动拒绝 root（复用 `ctty.IsRoot()`），逃生舱口径与 tanya 保持一致
- 部署纪律：部署态只做只读探测；功能验证在本地或临时环境完成
- 代码不添加注释，除非用户明确要求

## 结构（规划，见 `docs/plan.md` §5）

```
main.go     CLI：run / send / daemon / status / jobs / logs / config / version
config/     ivan 配置（内嵌 agent.Config + channels/schedule/daemon/policy 段，yaml + IVAN_*）
bus/        消息结构与 Bus（抽库候选：零依赖）
channel/    Channel 接口 + Manager + 注册表；子包 cli/ log/ qq/（抽库候选：仅依赖 bus）
qqbot/      QQ 开放平台协议实现（抽库候选：零 ivan 依赖，只依赖一个 websocket 库）
schedule/   作业模型 / 存储 / 调度循环 / 触发投递（抽库候选：仅标准库）
runner/     会话键 → agent 实例池、回合驱动、事件 → bus、交接自动续跑
data/       作业/事件/状态/会话索引的原子落盘、单实例锁
prompt/     system 提示词基座 + 环境段
docs/       规划与设计文档（plan / reuse-tanya / channels / qq-bot / schedule / sessions / refs-picoclaw）
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

规划阶段无代码；P0 起固定为：

```bash
go build ./... && go vet ./...          # 构建 + 静态检查
go test ./... && go test -race ./...    # 全量测试 + 竞态
go run . run "你好"                      # 前台单发冒烟（需配置 api_key）
```
