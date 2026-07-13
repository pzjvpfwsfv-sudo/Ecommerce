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

这一章已经把 Trino 服务、查询 SQL 和验证脚本都补齐了，但真实运行后也暴露出一个很重要的架构边界：

- 第 5 章当前写侧使用的是 `HadoopCatalog`
- `Trino 458` 不支持直接使用 `iceberg.catalog.type=hadoop`
- 所以当前 Chapter 6 的价值，不只是“接上 Trino”，更是确认了后续必须演进到共享 catalog

这也是后续继续升级成 `Hive Metastore` 或其他共享 catalog 方案的直接原因，能自然形成一段“从单引擎可用走向多引擎共享”的架构演进故事。

## 章节路线

1. 第 0 章：项目认知 + 环境准备 + 最小主链路设计
2. 第 1 章：分阶段 Compose 基础设施初始化
3. 第 2 章：先跑通数据生成器 + Kafka
4. 第 3 章：接入 Flink 做实时计算
5. 第 4 章：接入 Doris + FastAPI + 最小查询链路
6. 第 5 章：加入 MinIO + Iceberg 行为明细落湖
7. 第 6 章：Trino + Iceberg 湖表查询
8. 第 7 章：ZooKeeper -> KRaft 架构演进
9. 第 8 章：压测、调优、数据质量和简历沉淀

详细流程见 [docs/PROJECT_FLOW.md](/D:/桌面/实时湖仓电商行为数据平台 + AI 指标分析助手项目/docs/PROJECT_FLOW.md)。
