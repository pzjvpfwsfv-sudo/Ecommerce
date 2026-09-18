# 实时湖仓电商行为数据平台 + AI 指标分析助手

这是一个面向秋招展示与能力训练的实战项目，目标不是堆技术名词，而是做出一条能解释、能运行、能调优、能写进简历的数据工程主链路。

## 毕设与校招版（2026-09）

新增方向为“基于实时湖仓与检索增强智能体的电商行为分析系统”。真实数据、业务可视化、知识库和受控 Agent 是核心范围，以下历史章节不是新版功能完成声明。

- [毕设总设计与功能边界](docs/superpowers/specs/2026-09-17-graduation-system-design.md)
- [G1 真实数据实施计划](docs/superpowers/plans/2026-09-17-graduation-phase-1-real-data.md)
- [G1 跨月完整文件与稳定用户采样](docs/superpowers/plans/2026-09-17-graduation-cross-month-data.md)
- [数据准备命令、真实画像与剩余验收](docs/graduation/data-readiness.md)
- [G2-A 固定速率与可恢复历史回放](docs/graduation/replay-runbook.md)
- [G2-B Java 真实事件质量入口](docs/graduation/real-event-quality-runbook.md)
- [G2-C 真实事件统一落湖与 Trino 验收](docs/graduation/real-event-lakehouse-runbook.md)

已新增不依赖 Docker 的真实数据准备工具：`python -m generators.real_data --help`。它与旧模拟生成器隔离，不自动写 Kafka，不把历史前缀样本当作完整分析窗口。

当前真实数据依据：已完整扫描 2019 年 10/11 月共 109,950,743 条源记录，按稳定用户抽样得到 2,199,938 条事件，覆盖 61 天。G2-A 历史回放、G2-B Java 真实事件质量入口和 G2-C Iceberg 明细落湖均已完成隔离动态验收；正式表已由 Trino 核对 1,002 条真实事件，下一步进入 G2-D 分层指标与版本化 API，业务页面和 RAG 尚未完成。GitHub 备份使用 `codex/chapter-10-controlled-tools` 开发分支；真实数据、密钥和服务卷不包含在 Git 中。

G2-A 已提供 `python -m generators.real_data.replay`：默认离线试跑，显式 Kafka 模式只允许真实数据专用 Topic，支持限速、确认后保存进度和 Ctrl+C 恢复。真实样本 400+600 条离线恢复与真实 Kafka 续传均通过，独立消费核对 1,000 条的业务字段、事件 ID、顺序与 key 全部一致。证据见运行手册，不等同于 220 万条全量回放或新 Flink/湖仓链路已完成。

G2-B 新增独立 Java 清洗入口，严格保留真实字段和稳定 ID，支持带 Kafka 坐标的拒绝、有限时间去重及历史时间迟到分流。真实样本 1,000 条 `read_committed` 基线、重复注入和持久化 Checkpoint 恢复均已通过；恢复后新增记录无遗漏，恢复前后重复均被状态拦截。验收中还复现并修复了历史事件时间被写成 Kafka 存储时间、导致记录触发保留清理的问题。该结论不等于 220 万条容量、湖仓落地或永久去重已经完成。

G2-C 使用两个严格 Kafka JSON Source 将 clean/late 统一为一套列，并由单一 Flink SQL writer 写入 `lakehouse.analytics.real_behavior_detail_v1`。正式表验收为 `总数=1002、clean=1001、late=1、不同 event_id=1002`，五类字段质量违规均为 0；17 个源字段逐事件对账 17,034 次零差异。该结论不等于 220 万条容量或长期 exactly-once 验收，完整证据与重跑边界见 G2-C 运行手册。

## 项目目标

- 用真实工程化方式搭建电商行为实时数据链路
- 逐步补齐湖仓、查询服务、AI 指标问答与调优实验
- 在过程中沉淀 README、架构图、压测记录、调优日志和面试话术

## 最小主链路

第 0 阶段我们先只盯住这条最小链路：

`数据生成器 -> Kafka -> Flink -> Doris -> FastAPI -> 看板`

先把这条链路讲清楚、目录搭好、工程边界定好，再逐步加入：

- Iceberg + MinIO 作为湖仓底座
- Flink CDC 同步业务库维表
- Trino 作为即席查询引擎
- AI 指标分析助手
- 压测、调优、数据质量与故障恢复

## 为什么先做最小链路

- `数据生成器`：没有稳定输入，就没法验证整条链路
- `Kafka`：让上游采集和下游计算解耦
- `Flink`：负责实时清洗、聚合、窗口计算和状态管理
- `Doris`：给看板提供低延迟查询
- `FastAPI`：把数据服务包装成前后端都能消费的接口
- `看板`：把结果变成可展示、可讲解的成果

## 当前阶段

当前已经完成：

- 第 0 章：项目认知与主链路拆解
- 第 1 章：分阶段 Compose 基础设施骨架
- 第 2 章：Python 数据生成器接入 Kafka
- 第 3 章：Flink SQL 最小实时计算链路
- 第 4 章：Doris + FastAPI 最小查询链路骨架
- 第 5 章：MinIO + Iceberg 行为明细落湖骨架
- 第 6 章：Trino + Iceberg 湖表查询
- 第 7 章：Kafka 从 ZooKeeper 演进到 KRaft `controller + broker` 双角色拓扑
- 第 8 章：基于 Doris 与 Trino 可信证据的 AI 指标分析助手
- 第 9 章：Java DataStream 数据质量治理、影子验证、受控切流与安全回滚

其中第 3 章当前已经验证到：

- Flink SQL 作业可以成功提交到本地 Flink 集群
- `http://localhost:8081/overview` 可以看到 `RUNNING` 的作业
- 这条链路沉淀了多条真实排障记录，可直接转化成面试故事

第 4 章当前已经补齐：

- Doris FE / BE 的 Compose 运行骨架
- Flink Doris sink SQL 与提交脚本
- FastAPI 实时指标查询接口
- Doris 初始化脚本与运行说明

第 5 章当前目标为：

- MinIO 作为对象存储底座
- Iceberg 作为行为明细湖表
- 与 Doris 指标层并行的明细落湖出口

第 5 章当前已经验证到：

- `./scripts/run_chapter_5_iceberg_pipeline.ps1` 可成功提交 MinIO 版 Iceberg 作业
- Flink `/jobs/overview` 中可见 `RUNNING` 的 `lakehouse.analytics.user_behavior_detail` 作业
- MinIO `warehouse/iceberg` 下已经生成 Iceberg metadata 文件
- 同时保留了本地 filesystem warehouse 验证链路，方便后续回归和对照实验

## 目录规划

```text
.
├─ docs/                     # 文档、设计说明、流程和调优记录
├─ infra/                    # Docker Compose、基础设施配置
├─ generators/               # 电商行为与交易数据生成器
├─ jobs/                     # Flink 作业与 SQL
├─ services/                 # FastAPI、AI 服务、看板后端
└─ scripts/                  # 初始化、启动、检查脚本
```

## 第 3 章当前可用命令

### 启动 Kafka 最小链路

```powershell
docker compose --env-file infra/.env.example -f infra/docker-compose.yml --profile core up -d
```

### 运行 Flink SQL 作业

```powershell
./scripts/run_flink_sql_job.ps1
```

### 检查 Flink 作业状态

```powershell
(Invoke-WebRequest -UseBasicParsing 'http://localhost:8081/jobs/overview').Content
```

更详细的第 3 章说明见 [jobs/README.md](/D:/桌面/实时湖仓电商行为数据平台 + AI 指标分析助手项目/jobs/README.md)。

## 第 4 章当前可用命令

### 初始化 Doris 指标表

先确认 Docker Desktop 已启动，并且 docker version 不再报 docker_engine 连接错误。

```powershell
./scripts/init_doris_realtime_metrics.ps1
```

### 提交 Flink -> Doris 实时链路

```powershell
./scripts/run_chapter_4_pipeline.ps1
```

### 启动 FastAPI 查询服务

```powershell
docker compose --env-file infra/.env.example -f infra/docker-compose.yml --profile serving up -d api
```

### 查询实时指标

```text
GET /health
GET /metrics/realtime
GET /metrics/{metric_name}
```

## 第 5 章当前可用命令

### 提交 Flink -> Iceberg on MinIO 明细落湖链路

```powershell
./scripts/run_chapter_5_iceberg_pipeline.ps1
```

### 用 filesystem warehouse 做本地 Iceberg 验证

```powershell
./scripts/run_chapter_5_local_iceberg_validation.ps1
```

### 做第 5 章端到端收尾验证

```powershell
./scripts/verify_chapter_5_end_to_end.ps1
```

### 做第 5 章查询回读验证

```powershell
./scripts/verify_chapter_5_readback.ps1
```

这一条本地验证链路会把 Iceberg warehouse 指到 `file:///workspace/tmp/iceberg-warehouse`，用于先证明 `Flink -> Iceberg` 本身可以跑通，再把剩余问题收敛到 MinIO / S3A 集成层。

MinIO 版当前已经通过真实验证，关键修复点是把 S3A 配置下沉到 Flink 容器挂载的 Hadoop `core-site.xml`，而不是只写在 `CREATE CATALOG` 的 SQL 属性里。

这一章的目标不是替代第 4 章的 Doris 指标层，而是并行补上 `MinIO + Iceberg` 明细数据底座。

第 5/6 章后续已经从单引擎 `HadoopCatalog` 演进到共享 `Hive Metastore` catalog：

- Flink 负责写入 Iceberg 并更新共享元数据
- Trino 负责通过同一个 metastore 查询湖表

当前 Kafka 基础设施也已经不再依赖 ZooKeeper，而是采用：

- 内部 `kafka-controller` 负责 KRaft 控制面
- 对外保持 `ecom-kafka` / `kafka:29092` / `localhost:9092` 兼容语义的 `kafka-broker`

这让项目既保住了前面章节已经跑通的生成器、Flink 和验证脚本入口，又形成了一段完整的 `ZooKeeper -> KRaft` 架构演进故事。

## 第 6 章：Trino + Iceberg 湖表查询

这一章不再扩展新的落湖链路，而是把第 5 章沉下来的 Iceberg 明细表重新拉回到查询层，补上“能查、好查、查得准”的最后一段。

- 复用第 5 章已经写入 MinIO + Iceberg 的 `lakehouse.analytics.user_behavior_detail`
- 用 Trino 作为即席查询引擎，直接读湖表做按事件类型聚合
- 用 `./scripts/verify_chapter_6_trino_queries.ps1` 串起第 5 章端到端验证、Trino 启动、SQL 结果校验

### 第 6 章当前可用命令

```powershell
./scripts/verify_chapter_6_trino_queries.ps1
```

### 第 6 章叙事

> 第 5 章先把行为明细沉到 MinIO + Iceberg，第 6 章再让 Trino 直接读这张湖表，把“可落湖”推进到“可查询、可验证”。

### 第 6 章当前真实状态

真实运行最初暴露出一个重要边界：第 5 章最小闭环采用的 `HadoopCatalog` 无法被 `Trino 458` 直接共享。项目随后已经完成共享 Catalog 演进：

- Flink 与 Trino 共用 `thrift://hive-metastore:9083`
- Flink 继续把明细写入 MinIO 上的 Iceberg 表
- Trino 已能查询同一张表并返回非零 `event_count` 与 `event_type` 聚合结果

因此第 6 章已经从“接入查询脚手架”推进到“多引擎共享元数据并真实读通”，形成了完整的 `HadoopCatalog -> Hive Metastore` 架构演进故事。

## 第 8 章：可信指标 AI 分析助手

第一版由后端执行预定义查询，把 Doris 实时 PV/UV 与 Trino 历史聚合组装为 `evidence`。模型只能返回严格枚举的四类分析选择 claim ID，最终叙事由后端自有模板渲染；默认 `rule_based` 模式不需要 API Key，模型也始终不能生成或执行 SQL。真实端到端验证使用：

```powershell
./scripts/verify_chapter_8_analysis.ps1
```

接口：`POST /analysis/realtime`

### 严格可信模式与安全边界

- 叙事中的数字只允许来自响应 `evidence` 或后端预定义派生值，主分析器与回退分析器使用同一个数字来源守卫。
- 守卫先做 NFKC 归一化，再按 fail-closed 策略拒绝无法确认的数字表达；目前只支持可见中文/英文数字分隔语义，自然语言词典并不完备，完整保证来自 claim ID 与模板而不是词典枚举。
- 模型分析选择必须显式完整提供 `summary`、`insights`、`risks`、`actions` 四字段，且每个值都必须属于允许的 claim ID 枚举；任意自由文本、额外字段或未知枚举都会触发降级。异常边界的实际保证是固定安全响应、普通日志不含异常消息或 stack，并以 `from None` 抑制默认 traceback context；Python `__context__` 对象仍可能存在，因此不宣称递归擦除异常链对象。
- 数值可追溯不等于整句语义正确。当前边界防止无依据数字进入响应，但不能证明因果、趋势或建议合理；后续仍需离线评测和结构化 claim 校验。

### 演进故事

第 8 章先完成“预定义查询 -> 可信 evidence -> 规则或模型解读 -> 同证据返回”的最小闭环，避免一开始把数据库权限交给模型。后续按“趋势与异常 -> 受控工具调用 -> 受控 NL2SQL -> 产品化评测”演进，每一步继续保留查询白名单、审计、成本和证据边界。

## 第 9 章：Java DataStream 数据质量治理

第 9 章已完成从影子验证到正式受控切流的收口：三条正式 Flink 作业保持 `RUNNING`，Doris 与 Iceberg 正式作业消费 `user_behavior_clean`。原始 Topic、影子 Topic、旧 raw Source、Checkpoint、Savepoint 与回滚现场均保留。

- Phase A 已完成影子对账、五类 DLQ 原因码、重复事件去重与 TaskManager 故障恢复验证。
- Phase B 使用 manifest 固化原始 Topic 的停流边界，完成受控切流与生产验收。
- 安全回滚先暂停流量并按 manifest 核验 Job ID、名称与状态；回滚 dry-run 已验证入口和边界，但不把已写入 Doris/Iceberg 的数据宣称为可自动撤销。

## 第 10 章：受控工具调用与审计

`POST /analysis/tools` 默认使用无需 API Key 的 `rule_based` planner 和叙事模式，只能调用 Doris 实时指标、Trino 历史行为和 Flink 数据质量三个后端注册的只读工具。可选 `openai_compatible` 模型模式仅能选择这三个工具和受控 claim，不能生成或执行 SQL；第 10 章仍不是 NL2SQL。

真实验收只读调用 5 次接口，严格检查工具白名单、顺序、evidence、唯一 `audit_id`、`degraded=false` 和提示注入边界：

```powershell
./scripts/verify_chapter_10_tool_analysis.ps1
```

运行前须让已有的 FastAPI、Doris、Trino、Flink REST 和第 9 章唯一生产质量 Job 处于可用状态。验收通过时最后一行输出 `{"status":"PASS","requests":5,"tools_verified":3,"prompt_injection_blocked":true}`；完整前提、失败排障和面试叙事见 [第 10 章运行手册](docs/chapter-10-controlled-tool-calling-runbook.md)。

## 第 10.5 章：工程可靠性加固

进入第 11 章前，使用 `./scripts/bootstrap_chapter_10_5.ps1` 作为唯一推荐启动入口。它统一处理 PostgreSQL Metastore、五个持久化命名卷、MinIO 湖数据与 Flink 状态、Catalog/Job 恢复、功能性 readiness 和严格验收；迁移、reset 与排障命令见 [第 10.5 章运行手册](docs/chapter-10-5-engineering-hardening-runbook.md)。当前边界仍是本地单机、非生产 HA、非公网安全部署。

## 章节路线

1. 第 0 章：项目认知 + 环境准备 + 最小主链路设计
2. 第 1 章：分阶段 Compose 基础设施初始化
3. 第 2 章：先跑通数据生成器 + Kafka
4. 第 3 章：接入 Flink 做实时计算
5. 第 4 章：接入 Doris + FastAPI + 最小查询链路
6. 第 5 章：加入 MinIO + Iceberg 行为明细落湖
7. 第 6 章：Trino + Iceberg 湖表查询
8. 第 7 章：ZooKeeper -> KRaft 架构演进
9. 第 8 章：可信指标 AI 分析助手
10. 第 9 章：Java DataStream 数据质量治理、影子验证、受控切流与安全回滚
11. 第 10 章：受控工具调用与审计
12. 第 10.5 章：持久化、冷启动与运行可靠性加固
13. 第 11 章：受控 NL2SQL
14. 后续：产品化评测、可观测性、压测与状态调优

第 10.5 章完成并通过隔离动态验收后才进入第 11 章。第 10 章仍不生成或执行 SQL；模型只能选择后端注册的只读工具。

详细流程见 [docs/PROJECT_FLOW.md](/D:/桌面/实时湖仓电商行为数据平台 + AI 指标分析助手项目/docs/PROJECT_FLOW.md)。

## 最终审查加固

- 生产模型路径的完整 SQL/代码输出硬边界来自严格枚举 claim ID 与后端自有模板：模型自由文本不会进入 API。主分析器与回退分析器仍共用 SQL/代码输出守卫作为 defense-in-depth，但安全保证不依赖正则，也不宣称正则能识别全部代码。系统没有引入 NL2SQL。
- 输入和叙事在 NFKC 后全局拒绝 Cc/Cf 控制或格式字符；数字 token 只接受 ASCII 0-9 的明确格式，并拒绝残留 Unicode 数字、中英文数字词或数量词及无穷等数值符号。
- 历史 evidence 由单条 Trino statement 返回总数、事件类型计数和最新时间；`try(from_iso8601_timestamp(event_time))` 使无效时间变为 NULL 后不参与 MAX。构造 evidence 前校验计数非负且分组和等于总数，否则按 Trino 不可用降级。
- `TRINO_CATALOG` 与 `TRINO_SCHEMA` 只接受严格 ASCII 标识符白名单，查询表名逐段安全双引号，statement 与 Trino header 使用同一配置。
- 真实脚本是隔离验证：先要求 PV、UV、updated_at 连续 3 次稳定，再启动独立 consumer group 的 latest-offset Iceberg 审计作业并确认 RUNNING，然后发布两个唯一用户。成功必须同时满足 PV/UV 精确增长 2、updated_at 推进，以及 Trino 按两个精确 event_id 返回事件数/不同事件 ID/不同用户均为 2；这是 Iceberg 明细 runId 审计 + Doris 聚合双证据，并发或 backlog 导致聚合 overshoot 会失败。
