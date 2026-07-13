# 第 5 章：MinIO + Iceberg 行为明细落湖实现记录

## 1. 目标

本章的实现目标不是一次性把整个湖仓体系做满，而是完成一条最小、可验证、可继续演进的链路：

- 复用现有 Kafka 行为流 source
- 新增 MinIO 作为对象存储底座
- 新增 Iceberg catalog 和行为明细表
- 让 Flink SQL 可以把行为事件落到 Iceberg
- 在遇到集成问题时，补出独立验证支线，而不是停留在“理论上应该能跑”

## 2. 当前交付物

### 基础产物

- `infra/.env.example`
- `infra/docker-compose.yml`
- `jobs/sql/06_create_iceberg_catalog.sql`
- `jobs/sql/07_sink_user_behavior_to_iceberg.sql`
- `scripts/run_chapter_5_iceberg_pipeline.ps1`
- `tests/test_chapter_5_artifacts.py`
- `README.md`
- `jobs/README.md`

### 排障与验证支线产物

- `jobs/sql/08_create_iceberg_catalog_local.sql`
- `jobs/sql/09_sink_user_behavior_to_iceberg_local.sql`
- `scripts/run_chapter_5_local_iceberg_validation.ps1`
- `tests/test_chapter_5_local_validation.py`
- `jobs/sql/00_enable_iceberg_checkpointing.sql`
- `scripts/verify_chapter_5_end_to_end.ps1`
- `tests/test_chapter_5_end_to_end_validation.py`
- `jobs/sql/10_readback_iceberg_user_behavior.sql`
- `scripts/verify_chapter_5_readback.ps1`
- `tests/test_chapter_5_readback_validation.py`

## 3. 实施过程

### 3.1 先补 Chapter 5 基础骨架

先完成了第 5 章最小产物：

- Compose 中加入 `lakehouse` profile
- 增加 `minio` 与 `minio-init` 服务
- 增加 Iceberg catalog SQL 和 sink SQL
- 增加 Chapter 5 提交脚本
- 增加测试与 README 说明

这一步的目标是先把“项目里有第 5 章”做出来。

### 3.2 首次运行后，暴露真实依赖问题

运行 `run_chapter_5_iceberg_pipeline.ps1` 后，并没有直接成功。实际先后遇到的问题包括：

- 缺少 Hadoop 基础类
- `No FileSystem for scheme \"s3\"`
- 缺少 AWS SDK bundle

因此对 Flink 运行时 jar 进行了补齐，最终脚本会自动准备：

- `iceberg-flink-runtime-1.19-1.6.1.jar`
- `iceberg-aws-bundle-1.6.1.jar`
- `hadoop-client-api-3.3.6.jar`
- `hadoop-client-runtime-3.3.6.jar`
- `hadoop-aws-3.3.6.jar`
- `aws-java-sdk-bundle-1.12.262.jar`

同时，`infra/docker-compose.yml` 中也把这些 jar 挂载到了 `flink-jobmanager`、`flink-taskmanager` 和 `flink-sql-client`。

### 3.3 调整 catalog 路线，收敛到 Hadoop S3A

初版尝试过走 Iceberg S3FileIO 路线，但为了降低本地调试不确定性，后续改成了更明确的 HadoopCatalog + S3A 方案。

最终保留的关键配置包括：

- `warehouse = s3a://warehouse/iceberg`
- `fs.s3a.impl = org.apache.hadoop.fs.s3a.S3AFileSystem`
- `fs.s3a.endpoint = minio:9000`
- `fs.s3a.access.key = minioadmin`
- `fs.s3a.secret.key = minioadmin123`
- `fs.s3a.aws.credentials.provider = org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider`
- `fs.s3a.endpoint.region = us-east-1`
- `fs.s3a.path.style.access = true`
- `fs.s3a.connection.ssl.enabled = false`

同时，为了避免 Flink 在容器里走到错误的默认凭证路径，还给 Flink 服务补充了这些环境变量：

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`
- `AWS_EC2_METADATA_DISABLED`

### 3.4 单独验证 MinIO bucket 可写

为了排除 MinIO 本身没起好、bucket 没建好的可能，又通过 `minio-init` sidecar 执行 `mc cp` 做了单独验证。

实际结论：

- `warehouse` bucket 可以成功创建
- sidecar 能成功往 bucket 里写对象
- MinIO 服务不是完全不可写状态

这一步非常重要，因为它把问题从“MinIO 整体有问题”收敛成“Flink SQL + Iceberg + S3A 集成访问还有问题”。

### 3.5 明确当前剩余错误是 403 Forbidden

在 jar 补齐、catalog 调整、环境变量补齐之后，MinIO 版链路已经出现了更清晰的运行状态：

- `CREATE TABLE user_behavior_source` 成功
- `CREATE CATALOG lakehouse` 成功
- 真正访问 warehouse 时失败
- 剩余报错为 `AmazonS3Exception: 403 Forbidden`

这说明问题已经不在最外层，而是集中在 warehouse 访问阶段。

### 3.6 增加本地 filesystem warehouse 验证支线

为了彻底回答“到底是 Iceberg 没通，还是 MinIO / S3A 没通”，新增了一条本地验证支线：

- `08_create_iceberg_catalog_local.sql`
- `09_sink_user_behavior_to_iceberg_local.sql`
- `run_chapter_5_local_iceberg_validation.ps1`

这条支线只保留 Flink + Iceberg，不再依赖 MinIO，而是把 warehouse 指向：

- `file:///workspace/tmp/iceberg-warehouse`

## 4. 实际验证结果

### 4.1 测试结果

以下测试已通过：

- `tests.test_chapter_5_artifacts`
- `tests.test_chapter_5_local_validation`
- `tests.test_chapter_5_end_to_end_validation`
- `tests.test_chapter_5_readback_validation`

总计 16 个用例通过，说明当前仓库中的 Chapter 5 产物、文档、落湖验证入口和回读验证入口都已经齐全。

### 4.2 本地 filesystem warehouse 运行结果

执行：

```powershell
./scripts/run_chapter_5_local_iceberg_validation.ps1
```

实际结果：

- Flink SQL 作业成功提交
- Job ID: `44b81e3e37fa0e17f8bc91aa1de47de5`
- Flink `/jobs/overview` 返回 `RUNNING`
- 本地生成了 `tmp/iceberg-warehouse/analytics/user_behavior_detail/metadata`

这条证据链可以明确说明：

- `Flink -> Iceberg` 基础链路已成立
- Iceberg 表 metadata 已经真实落盘
- 当前没有必要再怀疑 “Iceberg 根本没跑起来”

### 4.3 MinIO 版当前状态

MinIO 版现在已经完成关键打通。

已确认的事实是：

- MinIO 服务能启动
- bucket 可创建且 sidecar 可写
- Flink 能识别 Iceberg catalog
- `run_chapter_5_iceberg_pipeline.ps1` 已可成功提交作业
- MinIO `warehouse/iceberg/analytics/user_behavior_detail/metadata` 下已生成 metadata 文件

真正的根因是：

- `HadoopCatalog` 使用的是 Hadoop `Configuration`
- 单纯把 `fs.s3a.*` 写在 `CREATE CATALOG` SQL 里，并不能稳定注入到 HadoopCatalog
- 正式修复方式是给 Flink 容器挂载 Hadoop `core-site.xml`，并设置 `HADOOP_CONF_DIR`

### 4.4 第 5 章收尾验证结果

在 403 修复后，又继续验证了真正的数据提交链路。

先做了一次对照实验：

- 手工向 Kafka `user_behavior_events` 发送 30 条事件
- 观察到 MinIO 中仍然只有 metadata，没有 data 文件
- TaskManager 日志里没有 checkpoint 完成迹象

这说明 Iceberg streaming sink 还差 checkpoint 触发 commit。

因此又补了：

- `jobs/sql/00_enable_iceberg_checkpointing.sql`
- `scripts/verify_chapter_5_end_to_end.ps1`

然后重新执行端到端验证，最终结果为：

- MinIO 中出现 `data/00000-...parquet`
- metadata 从 `v1.metadata.json` 推进到 `v2.metadata.json`
- 同时生成 manifest / snapshot 相关 avro 文件

这条证据链说明第 5 章已经不只是“metadata 建出来了”，而是“真实行为事件已经写成 Iceberg data file 并提交到 MinIO”。

### 4.5 第 5 章查询回读验证结果

在完成落湖验证后，又继续把“可读回”这一段补齐。

新增内容：

- `jobs/sql/10_readback_iceberg_user_behavior.sql`
- `scripts/verify_chapter_5_readback.ps1`

这里没有直接沿用默认 SQL Client 输出模式，而是专门做了两点收敛：

- `SET 'execution.runtime-mode' = 'batch';`
- `SET 'sql-client.execution.result-mode' = 'TABLEAU';`

这样做的原因是：

- `COUNT(*)` 更适合一次性 batch 收敛结果
- 非交互式执行时，tableau 结果更稳定，便于脚本判断成功

实际验证时，脚本会先跑：

- `verify_chapter_5_end_to_end.ps1`

确保新的验证事件已经写入 MinIO，再执行：

- `SELECT COUNT(*) AS event_count FROM lakehouse.analytics.user_behavior_detail;`

最终成功输出过稳定结果：

- `event_count = 279`
- `1 row in set`

这一步说明第 5 章已经从“能写 metadata、能写 data file”，进一步推进到“能稳定查询回读 Iceberg 明细表”。

### 4.6 第 5 章额外排障记录：Kafka 反复重建触发 `NodeExistsException`

在反复执行 Chapter 5 验证脚本的过程中，还复现出一个很真实的 ZooKeeper Kafka 问题：

- `docker compose up -d --force-recreate` 会重建 broker
- ZooKeeper 中旧的 `/brokers/ids/1` 临时节点可能尚未释放
- Kafka 重新启动时会报 `NodeExistsException`
- 容器状态直接变成 `Exited (1)`

这会导致一个很典型的错觉：

- 表面上看像是 Iceberg 或写湖脚本又坏了
- 实际上是 Kafka 基础设施层没有恢复好

为此，`verify_chapter_5_end_to_end.ps1` 又新增了 `Wait-ForKafkaReady` 逻辑，做了三件事：

- 轮询 Kafka 容器状态
- 如果容器已经 `exited`，等待后主动 `docker start ecom-kafka`
- 用 `kafka-topics --list` 作为 broker ready 探针

这一层处理让第 5 章在当前 ZooKeeper 模式下的验证更稳，也为后续第 7 章升级 KRaft 提供了非常真实的演进动机。

## 5. 当前结论

到这一步，第 5 章已经有了一个非常清晰的阶段性结论：

### 已经证明的事情

- Flink SQL 可以与 Iceberg 正常协作
- Chapter 5 的 SQL、脚本、测试、README 已经成型
- 本地 filesystem warehouse 可以作为可靠的对照实验
- MinIO 版已经完成从 403 修复到 data file 落湖的闭环验证
- Flink SQL 已经可以 batch 查询回读 Iceberg 表中的事件总数

### 还需要继续攻克的事情

- 第 6 章 Trino / CDC 接入后的多引擎联调
- 更长时间运行下的 checkpoint 与文件滚动验证
- Kafka 从 ZooKeeper 向 KRaft 的架构升级

## 6. 推荐的下一步

下一步建议按这个顺序推进：

1. 保留本地 filesystem warehouse 作为 Chapter 5 回归基线
2. 继续验证 MinIO 版 data 文件持续写入
3. 为第 6 章准备 Trino / CDC 接入点
4. 后续第 7 章推进 ZooKeeper -> KRaft 架构演进

## 7. 面试故事沉淀

这一章很适合沉淀成一段“我不是只会搭环境，我会把问题拆开验证”的面试故事：

“我在做行为明细落湖时，没有把 MinIO、Iceberg、Flink 全部混在一起盲调，而是先补齐运行时依赖，再把 catalog 路线收敛到 Hadoop S3A。等问题缩小到 403 之后，我又单独做了一条本地 filesystem warehouse 验证链路，先证明 `Flink -> Iceberg` 没问题；接着又用 Hadoop `FsShell` 直接验证 S3A 访问 MinIO 也没问题。最后我把根因定位到 `HadoopCatalog` 的配置注入方式，正式修复成 Hadoop `core-site.xml + HADOOP_CONF_DIR`。修完之后我继续补 checkpoint、端到端落湖脚本和 batch 回读脚本，最终不仅在 MinIO 中看到了 `.parquet` data file 和 `v8.metadata.json`，也能稳定查回 `event_count = 279`。中间还顺手处理了 ZooKeeper 模式下 Kafka 反复重建会报 `NodeExistsException` 的问题，所以我后面会顺势把 Kafka 升级到 KRaft，把这段经历讲成完整的架构演进故事。”

这段表达会非常适合后面串到你的“架构演进”和“工程排障能力”故事里。

## 8. 2026-07-14 KRaft 架构下的收尾复验

第 7 章完成 KRaft 迁移后，又重新执行了第 5 章 filesystem warehouse 支线，确认原有 Iceberg 回归基线没有被 Kafka 架构升级破坏。

### 8.1 对历史 ZooKeeper 记录的说明

前文的 `NodeExistsException` 与 ZooKeeper znode 残留是当时真实发生的历史问题，也是后续迁移 KRaft 的直接动机。当前 Compose 已经采用 `controller + broker` 分角色 KRaft，不再依赖 ZooKeeper；这些内容保留用于解释架构演进，而不是描述当前运行状态。

### 8.2 本地验证脚本加固

KRaft 重建时，controller 需要先恢复 metadata log，broker 随后完成注册、追平并解除 fenced 状态。容器显示 Running 并不代表 broker 已可处理业务请求，因此本次给 `run_chapter_5_local_iceberg_validation.ps1` 增加了两层就绪治理：

- 在提交 Flink 作业前，使用 `kafka-topics --create --if-not-exists` 幂等确保 `user_behavior_events` 存在，并把该命令同时作为 broker ready 探针。
- 等待 `http://localhost:8081/overview` 返回 HTTP 200 后，再调用 Flink SQL Client。

### 8.3 本次复验结果

- 第 5 章四组测试共 16 个用例全部通过。
- Compose `flink` profile 配置解析通过。
- KRaft controller、broker 与 Flink 三类容器重建成功。
- filesystem warehouse 作业 `f211e3f7b4a82c491d01057e1bd59623` 成功提交并保持 `RUNNING`。
- `tmp/iceberg-warehouse/analytics/user_behavior_detail/metadata` 中的 Iceberg metadata 可被当前作业正常加载。

这次复验把故事线补成了闭环：第 5 章先暴露 ZooKeeper 状态残留痛点，第 7 章完成 KRaft 迁移，再回到第 5 章证明原有落湖验证支线仍可运行。
