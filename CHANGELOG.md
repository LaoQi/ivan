# Changelog

ivan 变更记录。新条目加在最上方的日期分节内（没有当天分节就新建一个）；本文件记变更，AGENTS.md 只保留现行约束。

## 2026-10-10
- docs(plan,docs,ags): **规划落地**（用户要求：先出规划文档再开工）——新增 `docs/plan.md`（定位/目标与非目标/与 tanya 关系/总体架构/模块与目录/**抽库候选与隔离纪律**/数据布局/运行形态/阶段路线 P0–P5/决策记录 D1–D7/风险与红线/文档索引）、`docs/reuse-tanya.md`（复用矩阵、具体 API 清单、不复用边界、需 tanya 侧配合的 T1–T3、P0 探针 R1–R8 与判据、注意点）、`docs/channels.md`（通道原则、消息模型、Bus 与 Manager、会话键规则、内置通道 cli/log、长消息切分、入站安全）、`docs/qq-bot.md`（分层与抽库路径、鉴权、网关与 ws 生命周期、intents 与事件、发送与被动回复/图片、去重切分限流、配置项、测试策略、待实装核对清单）、`docs/schedule.md`（作业模型、调度循环、存储、`schedule` 工具 schema、与通道关系、可观测、验收）、`docs/sessions.md`（会话键→实例、映射持久化、回合驱动与结算、**交接自动续跑与上下文阈值**、并发背压、重启恢复、记忆与目标）、`docs/refs-picoclaw.md`（参考调研快照与取舍表）；`AGENTS.md` 按 tanya 家族口径重写为现行约束；`CHANGELOG.md`、`.gitignore` 建立。**决策记录**：D3 协议自实现（不用 botgo）、D4 首期仅文本+图片、D5 `cron_expr` 后置、D6 公共模块按判据抽库。**未写代码**（规划阶段）。
