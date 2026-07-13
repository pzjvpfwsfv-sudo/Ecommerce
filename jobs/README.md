# 第 3 章与第 4 章 Flink 作业说明

## 第 3 章：Flink SQL 最小实时计算闭环

这一章先使用 Flink SQL 跑通最小实时计算闭环：

- 从 Kafka 的 `user_behavior_events` topic 读取事件
- 先做最基础的累计型 `PV / UV` 聚合
- 先输出到 `print` sink 做调试验证

当前阶段不直接接 Doris，也不先上复杂窗口、水位线和迟到数据处理。

### 第 3 章文件说明

- `sql/01_source_user_behavior.sql`：定义 Kafka source 表
- `sql/02_sink_print_metrics.sql`：定义 print sink
- `sql/03_pv_uv_metrics.sql`：执行累计型 PV / UV 聚合
- `../scripts/run_flink_sql_job.ps1`：自动下载 connector、等待 Kafka/Flink 就绪、合并 SQL 并提交流式作业

### 第 3 章运行方式

```powershell
./scripts/run_flink_sql_job.ps1
```

### 第 3 章检查结果

- 浏览器打开 `http://localhost:8081/overview`
- REST 检查：`(Invoke-WebRequest -UseBasicParsing 'http://localhost:8081/jobs/overview').Content`
- TaskManager 日志：`docker logs --tail 200 ecom-flink-taskmanager`

## 第 4 章：Flink -> Doris -> FastAPI 查询链路

这一章把第 3 章的实时指标从 `print` sink 推进到可查询层：

- 保留 Kafka source
- 新增 Doris sink
- 把累计型 `PV / UV` 写入 `analytics.realtime_metrics`
- 通过 FastAPI 暴露 HTTP 查询接口

### 第 4 章文件说明

- `sql/04_sink_doris_metrics.sql`：定义 Doris sink 表
- `sql/05_pv_uv_to_doris.sql`：把累计型 PV / UV 写入 Doris sink
- `../scripts/init_doris_realtime_metrics.ps1`：初始化 Doris 数据库与指标表
- `../scripts/run_chapter_4_pipeline.ps1`：下载 Doris connector、合并 SQL 并提交 Flink 作业

### 第 4 章运行方式

先确认 Docker Desktop 已启动，且 docker version 可以正常连到 daemon。

先确保 Kafka 和数据生成器已经可用，再执行：

```powershell
./scripts/init_doris_realtime_metrics.ps1
./scripts/run_chapter_4_pipeline.ps1
```

API 服务启动命令：

```powershell
docker compose --env-file infra/.env.example -f infra/docker-compose.yml --profile serving up -d api
```

### 第 4 章接口说明

- `GET /health`
- `GET /metrics/realtime`
- `GET /metrics/pv`
- `GET /metrics/uv`

### 第 4 章面试表达

这一章适合这样概括：

> 第 3 章我先证明实时计算链路可以跑通；第 4 章我再把指标结果落到 Doris，并通过 FastAPI 暴露查询接口，让链路从“内部能算”推进到“外部能用”。

## 第 5 章：Flink -> Iceberg on MinIO 明细落湖

这一章不再扩展实时查询层，而是补上行为明细的湖仓底座：

- 继续复用 Kafka source
- 新增 MinIO 作为对象存储
- 新增 Iceberg catalog 与行为明细表
- 把 `user_behavior_source` 明细写入 `lakehouse.analytics.user_behavior_detail`

### 第 5 章文件说明

- `sql/00_enable_iceberg_checkpointing.sql`：为 Iceberg streaming sink 打开 checkpoint，触发 data file commit
- `sql/06_create_iceberg_catalog.sql`：定义 MinIO 版 Iceberg catalog、database 和行为明细表
- `sql/07_sink_user_behavior_to_iceberg.sql`：把行为流写入 MinIO 版 Iceberg 明细表
- `sql/08_create_iceberg_catalog_local.sql`：定义 filesystem warehouse 版 Iceberg catalog、database 和行为明细表
- `sql/09_sink_user_behavior_to_iceberg_local.sql`：把行为流写入本地 filesystem warehouse 明细表
- `../infra/compose/flink/conf/core-site.xml`：为 HadoopCatalog 提供 MinIO 所需的 S3A 配置
- `../scripts/run_chapter_5_iceberg_pipeline.ps1`：下载 Iceberg 运行时依赖、启动 MinIO / Flink 并提交 SQL 作业
- `../scripts/run_chapter_5_local_iceberg_validation.ps1`：只启动 Flink，并用 filesystem warehouse 验证 `Flink -> Iceberg` 基础链路
- `../scripts/verify_chapter_5_end_to_end.ps1`：重放一批验证事件，并等待 MinIO 中出现 Iceberg data file 与新 metadata
- `sql/10_readback_iceberg_user_behavior.sql`：以 batch 模式读取 Iceberg 表，查询明细总数
- `../scripts/verify_chapter_5_readback.ps1`：先跑端到端落湖，再执行 Iceberg 回读查询验证

### 第 5 章运行方式

先确保 Docker Desktop、Kafka 和 Flink 基础链路可用，再执行：

```powershell
./scripts/run_chapter_5_iceberg_pipeline.ps1
```

如果要先隔离 MinIO / S3A 问题，可以先跑本地 filesystem warehouse 验证：

```powershell
./scripts/run_chapter_5_local_iceberg_validation.ps1
```

### 第 5 章端到端收尾验证

```powershell
./scripts/verify_chapter_5_end_to_end.ps1
```

### 第 5 章查询回读验证

```powershell
./scripts/verify_chapter_5_readback.ps1
```

### 第 5 章当前验证结果

- MinIO 版脚本已经可以成功提交 Flink 作业
- `warehouse/iceberg/analytics/user_behavior_detail/metadata` 下已经生成 Iceberg metadata 文件
- 本地 filesystem warehouse 版本仍然保留，作为后续回归基线

### 第 5 章端到端收尾验证说明

这一轮验证的目标不是只看作业 `RUNNING`，而是继续确认：

- Kafka 中确实有真实行为事件进入
- Iceberg streaming sink 因 checkpoint 触发 commit
- MinIO 中最终出现 `.parquet` data file 和新的 metadata 版本

这一步通过后，第 5 章才能算从“metadata 建起来了”推进到“明细数据真的落湖了”。

### 第 5 章查询回读验证说明

这一轮验证的目标是继续确认：

- 已经落到 MinIO 的 Iceberg 表可以被重新查询
- 查询不是只看到流式 changelog，而是以 batch 方式拿到稳定结果
- `event_count` 能证明前一步写入的明细确实可读

这一步通过后，第 5 章就从“能写”推进到了“能写、能读”的最小闭环。

### 第 5 章排障结论

这次最关键的排障结论不是“多改了几个 S3A 参数”，而是确认了：

- `HadoopCatalog` 实际读取的是 Hadoop `Configuration`
- 只把 `fs.s3a.*` 写在 `CREATE CATALOG` 的 SQL 属性里，并不能稳定注入到 HadoopCatalog
- 正式修复方式是给 Flink 容器挂载 Hadoop `core-site.xml`，并设置 `HADOOP_CONF_DIR`

### 第 5 章面试表达

这一章适合这样概括：

> 第 4 章我先把实时指标服务化，第 5 章我再把行为明细沉到 MinIO + Iceberg，让项目同时具备“实时指标层”和“可追溯明细层”。

### 第 5/6 章共享 Catalog 演进

- 第 5 章先用 `HadoopCatalog` 跑通最小落湖闭环
- 为了让 Trino 读取同一张 Iceberg 表，后续升级成共享 `Hive Metastore`
- 当前 Flink 与 Trino 共用 `thrift://hive-metastore:9083`

## 第 6 章：Trino + Iceberg 湖表查询

这一章不再新增写入链路，而是把第 5 章已经沉下去的 Iceberg 明细表重新暴露到查询层，让湖表真正“可查、可验、可复用”。

- 复用 `lakehouse.analytics.user_behavior_detail` 作为查询目标
- 用 Trino 做即席查询和事件类型聚合
- 通过 `./scripts/verify_chapter_6_trino_queries.ps1` 串起第 5 章回放、Trino 启动和 SQL 校验

### 第 6 章文件说明

- `sql/11_trino_read_iceberg_user_behavior.sql`：读取 Iceberg 行为明细并按 `event_type` 聚合 `event_count`
- `../scripts/verify_chapter_6_trino_queries.ps1`：先跑第 5 章端到端验证，再启动 Trino 并执行两条 SQL 语句

### 第 6 章运行方式

```powershell
./scripts/verify_chapter_6_trino_queries.ps1
```

### 第 6 章叙事

> 第 5 章先把行为明细沉到 MinIO + Iceberg，第 6 章再让 Trino 直接读这张湖表，把“可落湖”推进到“可查询、可验证”。

### 第 6 章真实排障结论

- 第 5 章最初使用 `HadoopCatalog` 跑通 Flink 单引擎写入，但 `Trino 458` 不能直接共享这套元数据
- 项目已经升级到 `Hive Metastore`，Flink 与 Trino 共用 `thrift://hive-metastore:9083`
- 第 6 章验证脚本现已能查询同一张 Iceberg 表，并校验非零 `event_count` 和 `event_type` 聚合结果
