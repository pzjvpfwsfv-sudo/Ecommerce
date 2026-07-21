# 实时湖仓电商行为数据平台 + AI 指标分析助手

这是一个面向秋招展示与能力训练的实战项目，目标不是堆技术名词，而是做出一条能解释、能运行、能调优、能写进简历的数据工程主链路。

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
- 第 7 章：Kafka 从 ZooKeeper 演进到 KRaft `controller + broker` 双角色拓扑
- 第 8 章：基于 Doris 与 Trino 可信证据的 AI 指标分析助手

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

第一版由后端执行预定义查询，把 Doris 实时 PV/UV 与 Trino 历史聚合组装为 `evidence`，再交给分析器生成四字段叙事；默认 `rule_based` 模式不需要 API Key，也不允许模型生成或执行 SQL。真实端到端验证使用：

```powershell
./scripts/verify_chapter_8_analysis.ps1
```

接口：`POST /analysis/realtime`

### 严格可信模式与安全边界

- 叙事中的数字只允许来自响应 `evidence` 或后端预定义派生值，主分析器与回退分析器使用同一个数字来源守卫。
- 守卫先做 NFKC 归一化，再按 fail-closed 策略拒绝无法确认的数字表达；目前只支持可见中文/英文数字分隔语义。
- 模型结果必须显式完整提供 `summary`、`insights`、`risks`、`actions` 四字段；降级与失败使用结构化日志，并对异常链脱敏。
- 数值可追溯不等于整句语义正确。当前边界防止无依据数字进入响应，但不能证明因果、趋势或建议合理；后续仍需离线评测和结构化 claim 校验。

### 演进故事

第 8 章先完成“预定义查询 -> 可信 evidence -> 规则或模型解读 -> 同证据返回”的最小闭环，避免一开始把数据库权限交给模型。后续按“趋势与异常 -> 受控工具调用 -> 受控 NL2SQL -> 产品化评测”演进，每一步继续保留查询白名单、审计、成本和证据边界。

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
10. 后续：压测、调优、数据质量和产品化评测

详细流程见 [docs/PROJECT_FLOW.md](/D:/桌面/实时湖仓电商行为数据平台 + AI 指标分析助手项目/docs/PROJECT_FLOW.md)。
