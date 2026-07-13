# 第 5 章：MinIO + Iceberg 行为明细落湖设计

## 1. 背景

第 4 章已经完成了 `Kafka -> Flink -> Doris -> FastAPI` 的最小实时指标查询闭环，说明项目已经具备“实时算、实时查”的主链路能力。

但如果项目只停留在实时指标层，会缺少一个非常关键的数据底座能力：

- 行为明细无法稳定留存
- 历史数据无法回放
- 后续离线分析、回溯分析与数仓建模没有可靠源头

因此，第 5 章不再继续扩展实时查询层，而是补上湖仓底座中的第一块基石：把行为明细沉到 Iceberg，并以 MinIO 作为对象存储目标。

## 2. 本章目标

本章聚焦一个窄而真实的目标：

- 在本地 Compose 中加入 MinIO 对象存储
- 让 Flink SQL 可以把 `user_behavior_source` 明细写入 Iceberg 表
- 形成一条“实时指标走 Doris，行为明细走 Iceberg”的双轨数据出口
- 为后续的离线分析、Trino 查询、AI 助手历史追问和架构演进提供数据底座

更重要的是，本章不只是“把文件写出来”，而是要沉淀一段真实的工程验证与排障过程。

## 3. 范围

本章包含：

- MinIO 单机开发环境
- Iceberg 最小 catalog 与明细表定义
- Flink SQL 到 Iceberg 的行为明细写入
- Chapter 5 的运行脚本、文档和测试
- 一条本地 filesystem warehouse 验证支线
- 一条 Iceberg batch 查询回读验证支线
- MinIO / S3A 集成排障记录

本章不包含：

- Flink CDC
- Trino 即席查询
- Iceberg 分区演进、快照回滚和复杂表优化
- 湖仓多引擎读写一致性治理

这些内容后续分章推进，避免第 5 章失焦。

## 4. 设计原则

### 4.1 只做一条最小可验证明细链路

本章只要求把一张行为明细表稳定落入 Iceberg，不追求一次性补齐维表、宽表、订单表和多主题落湖。

### 4.2 与第 4 章形成清晰分工

- Doris：承接实时聚合结果，服务低延迟指标查询
- Iceberg：承接行为明细，服务历史回放与湖仓分析

这两个出口并行存在，职责清晰，不互相替代。

### 4.3 优先使用现有 Flink SQL 主线

本章继续沿用现有 `Kafka -> Flink SQL` 处理主线，不提前切到 DataStream API。后续会保留 DataStream API 演进空间，但这一章先以最小可用为准。

### 4.4 先证明 Iceberg 本身可用，再收敛对象存储问题

第 5 章的一个核心设计决策是：

- 不把所有失败都归因于 “Iceberg 没跑通”
- 要把问题拆成 `Flink -> Iceberg` 和 `Iceberg -> MinIO / S3A` 两层
- 先用本地 filesystem warehouse 证明 Iceberg 基础链路成立
- 再把剩余故障聚焦到 MinIO / S3A 集成层

这样排障过程更像真实工程，而不是盲目改参数。

### 4.5 为后续演进埋点

第 5 章完成后，项目能够自然衔接：

- 第 6 章：CDC / Trino / 更完整湖仓查询
- 第 7 章：ZooKeeper -> KRaft 架构演进
- 后续：DataStream API、AI 助手、压测与调优

## 5. 架构设计

本章新增后的链路如下：

1. 数据生成器将电商行为事件写入 Kafka
2. Flink SQL 从 Kafka `user_behavior_events` 读取行为流
3. 一条分支继续做实时聚合并服务 Doris
4. 另一条分支将原始行为明细写入 Iceberg 表
5. MinIO 作为 Iceberg 表文件的对象存储底座
6. 本地 filesystem warehouse 作为独立验证支线，用于隔离 MinIO / S3A 问题

这样项目从第 5 章开始，不再只有“指标结果层”，而是具备“明细层 + 指标层”双层结构。

## 6. MinIO 设计

MinIO 在本章中承担对象存储角色。

设计要求：

- 采用本地单机模式，满足开发与演示需要
- 通过 Compose 暴露 API 端口与 Console 端口
- 启动后自动准备一个用于 Iceberg warehouse 的 bucket
- 与 Flink SQL Client 运行在同一套本地开发环境中，方便观察和排障

本章不引入分布式 MinIO，也不处理生产级对象存储高可用问题。

## 7. Iceberg 设计

本章当前行为明细表采用与现有事件流一致的字段，保持理解门槛低：

- `event_id`
- `user_id`
- `product_id`
- `event_type`
- `event_time`
- `channel`
- `device_type`
- `page_id`

设计目标：

- 保留行为事件最关键的分析字段
- 让后续可以自然扩展到分区、宽表、快照查询和历史重放
- 保持字段与 Kafka 行为流结构尽量一致，降低理解成本

## 8. Flink SQL 落湖设计

本章继续复用已有 `user_behavior_source` 表定义，避免重复定义事件入口。

新增内容：

- 创建 Iceberg catalog
- 创建行为明细 sink 表
- 将 Kafka 行为流写入 Iceberg 表

当前设计同时保留两套 catalog 路径：

- MinIO 版：`jobs/sql/06_create_iceberg_catalog.sql`
- 本地 filesystem 版：`jobs/sql/08_create_iceberg_catalog_local.sql`

对应也有两套 sink SQL：

- MinIO 版：`jobs/sql/07_sink_user_behavior_to_iceberg.sql`
- 本地 filesystem 版：`jobs/sql/09_sink_user_behavior_to_iceberg_local.sql`

这样可以做到“同一套 source，不同的 warehouse 目标”，适合排障和演示。

## 9. 实际排障设计与证据链

这一章最有价值的地方，不是单纯新增了 MinIO，而是沉淀了一条真实的排障链路。

### 9.1 第一阶段：先补齐 Iceberg 运行时依赖

在最初执行 `run_chapter_5_iceberg_pipeline.ps1` 时，Flink SQL 作业并不能直接跑起来。实际遇到过的依赖问题包括：

- 缺少 Hadoop 基础类
- `No FileSystem for scheme \"s3\"`
- 缺少 AWS SDK 相关类

为了解决这几类问题，最终在 Flink 容器中补齐了这些 jar：

- `iceberg-flink-runtime-1.19-1.6.1.jar`
- `iceberg-aws-bundle-1.6.1.jar`
- `hadoop-client-api-3.3.6.jar`
- `hadoop-client-runtime-3.3.6.jar`
- `hadoop-aws-3.3.6.jar`
- `aws-java-sdk-bundle-1.12.262.jar`

这一步证明了一个现实问题：只写 Iceberg SQL 不够，本地可运行的 Flink SQL + Iceberg 还依赖完整的 Hadoop / AWS 运行时。

### 9.2 第二阶段：从 S3FileIO 尝试切到 Hadoop S3A 路线

最初方案尝试过基于 Iceberg `S3FileIO` 的写法，但在本地 MinIO 环境下并不稳定。后续为了更贴近 Flink 本地开发和 Hadoop 文件系统栈，改成了 `HadoopCatalog + s3a://` 路线。

核心调整包括：

- `warehouse` 从早期的 S3 风格写法收敛到 `s3a://warehouse/iceberg`
- 明确配置 `fs.s3a.impl = org.apache.hadoop.fs.s3a.S3AFileSystem`
- 保留 `path-style` 访问
- 显式补充 credentials provider 和 region

最终 MinIO 版 catalog SQL 里保留了这些关键配置：

- `fs.s3a.endpoint = minio:9000`
- `fs.s3a.access.key = minioadmin`
- `fs.s3a.secret.key = minioadmin123`
- `fs.s3a.aws.credentials.provider = org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider`
- `fs.s3a.endpoint.region = us-east-1`
- `fs.s3a.path.style.access = true`
- `fs.s3a.connection.ssl.enabled = false`

### 9.3 第三阶段：证明 MinIO bucket 本身可写

为了避免把所有失败都归咎于 Flink SQL，我们单独验证了 MinIO bucket 是否真的可访问。

通过 `minio-init` 容器执行 `mc cp` 写入 `warehouse` bucket 后，确认：

- bucket 可以正常创建
- sidecar 可以正常写入对象
- MinIO 本身不是“完全不可写”状态

这一步的意义是：

- 说明 MinIO 服务本身启动正常
- 说明 bucket 与根账号配置不是完全错误
- 将问题范围进一步收缩到 `Flink SQL + Iceberg + S3A` 集成层

### 9.4 第四阶段：定位到真实剩余问题是 403 Forbidden

在补齐 jar、切换到 S3A 路线、补充 AWS 环境变量后，MinIO 版链路已经不是完全起不来，而是出现了更具体的现象：

- `CREATE TABLE user_behavior_source` 成功
- `CREATE CATALOG lakehouse` 成功
- 真正访问 warehouse 时失败
- 剩余报错集中为 `AmazonS3Exception: 403 Forbidden`

这意味着：

- SQL 语法层面已经通了
- Flink 已经能识别 Iceberg catalog
- 问题不在最外层的 connector 装载
- 问题收敛到 warehouse 访问阶段

这就是一个很适合写进面试表达的“故障逐层收敛”过程。

### 9.5 第五阶段：引入本地 filesystem warehouse 作为对照实验

为了回答一个关键问题：

“到底是 Iceberg 没跑通，还是 MinIO / S3A 没配通？”

本章新增了一条本地验证支线：

- `jobs/sql/08_create_iceberg_catalog_local.sql`
- `jobs/sql/09_sink_user_behavior_to_iceberg_local.sql`
- `scripts/run_chapter_5_local_iceberg_validation.ps1`

这条支线不再依赖 MinIO，而是直接把 warehouse 指到：

- `file:///workspace/tmp/iceberg-warehouse`

实际验证结果已经确认：

- Flink SQL 作业可成功提交
- Flink `/jobs/overview` 中可见 `RUNNING` 作业
- 本地 `tmp/iceberg-warehouse/analytics/user_behavior_detail/metadata` 已生成 Iceberg metadata 文件

因此可以明确得出中间结论：

- `Flink -> Iceberg` 基础链路是通的
- 当前未解决的问题只剩 `MinIO / S3A` 集成层

这一点非常关键，因为它把“功能没做完”和“集成层还在排障”清晰地区分开了。

### 9.6 第六阶段：确认根因在 HadoopCatalog 的配置注入方式

在完成对照实验后，又继续做了一个更小的验证：

- 直接在 Flink 容器里使用 Hadoop `FsShell`
- 带上与 MinIO 相同的 `fs.s3a.*` 参数访问 `s3a://warehouse/iceberg`
- 结果可以正常 `mkdir` 和 `ls`

这说明：

- MinIO 凭证本身没有问题
- Hadoop S3A 基础访问没有问题
- 403 并不是“整个 S3A 都不通”

进一步结合 Iceberg 运行时实现可得出根因：

- `HadoopCatalog` 真正使用的是 Hadoop `Configuration`
- 只把 `fs.s3a.*` 写在 `CREATE CATALOG` 的 SQL 属性里，不能稳定注入到 `HadoopCatalog` 所持有的 `Configuration`
- 所以 SQL 层看起来有配置，真正访问 warehouse 时仍可能按缺省配置访问 MinIO，最终触发 403

### 9.7 第七阶段：正式修复方案

最终采用的修复方案是：

- 在 `infra/compose/flink/conf/core-site.xml` 中显式写入 MinIO 所需的 S3A 配置
- 在 Compose 中把 `conf/` 挂载到 Flink 各服务的 `/opt/hadoop-conf`
- 为 `flink-jobmanager`、`flink-taskmanager`、`flink-sql-client` 设置 `HADOOP_CONF_DIR=/opt/hadoop-conf`
- 把 MinIO 版 `06_create_iceberg_catalog.sql` 简化为只保留 Iceberg catalog 本身需要的属性

修复后实际验证结果为：

- `./scripts/run_chapter_5_iceberg_pipeline.ps1` 可成功提交作业
- Flink `/jobs/overview` 中可见 `RUNNING` 的 `insert-into_lakehouse.analytics.user_behavior_detail`
- MinIO `warehouse/iceberg/analytics/user_behavior_detail/metadata/v1.metadata.json` 已生成

### 9.8 第八阶段：补齐 checkpoint 与 data file 收尾验证

在 MinIO 版 403 修复完成后，又继续做了第 5 章收尾验证。

当时先向 Kafka 发送了一批真实行为事件，但 MinIO 中仍然只有 metadata，没有 data 文件。结合 TaskManager 日志可观察到：

- sink 作业已经 `RUNNING`
- Kafka source 已经开始消费 `user_behavior_events`
- 但日志中没有 checkpoint 成功迹象

这进一步说明：对于 Iceberg streaming sink，仅仅“作业在跑”和“metadata 已建好”还不够，真正的数据提交还依赖 checkpoint 触发 commit。

因此又增加了：

- `jobs/sql/00_enable_iceberg_checkpointing.sql`
- `scripts/verify_chapter_5_end_to_end.ps1`

其中 checkpoint SQL 负责：

- `SET 'execution.checkpointing.interval' = '10 s';`
- `SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';`

端到端验证脚本负责：

- 重提 Chapter 5 MinIO 版作业
- 向 Kafka `user_behavior_events` 写入一批验证事件
- 轮询 MinIO `warehouse/iceberg/analytics/user_behavior_detail`
- 等待 `.parquet` data file 和 `v2.metadata.json` 同时出现

最终验证成功，证明第 5 章已经从“能建表”推进到了“能真实落湖”。

### 9.9 第九阶段：补齐查询回读验证，形成最小读写闭环

完成 data file 落湖后，第 5 章还差最后一个很关键的问题：

“这些已经写进 MinIO 的 Iceberg 明细，能不能被稳定读回来？”

为此又补了：

- `jobs/sql/10_readback_iceberg_user_behavior.sql`
- `scripts/verify_chapter_5_readback.ps1`

这里专门把回读查询设计成 batch 模式，而不是继续沿用默认 streaming 结果模式，原因是：

- `SELECT COUNT(*)` 这种验证语句更适合一次性收敛成稳定结果
- 非交互式 SQL Client 在 changelog 模式下不利于自动化校验
- batch + tableau 输出更容易直接判断“表是否真的可读”

最终回读验证链路会：

- 先执行 `verify_chapter_5_end_to_end.ps1`
- 确保新的验证事件已经真实落到 MinIO Iceberg 表
- 再通过 Flink SQL Client 读取 `lakehouse.analytics.user_behavior_detail`
- 执行 `SELECT COUNT(*) AS event_count`
- 以 `1 row in set` 作为成功标志

实际验证结果已经出现稳定返回，例如：

- `event_count = 279`

这一步通过后，第 5 章就不再只是“能写入 metadata / data file”，而是形成了“能写、能读、能验证”的最小闭环。

### 9.10 第十阶段：把 ZooKeeper 模式下 Kafka 重建不稳定，收敛成脚本内的就绪治理

在多次重复执行第 5 章验证脚本时，还暴露出一个很真实的基础设施问题：

- `docker compose up -d --force-recreate` 反复重建 Kafka
- ZooKeeper 中 broker 临时 znode 释放不及时
- Kafka 启动时抛出 `NodeExistsException`
- 容器表现为 `ecom-kafka Exited (1)`

这类问题本质上不是 Iceberg 本身故障，而是当前项目仍处在 ZooKeeper Kafka 阶段时的中间件状态残留问题。

因此第 5 章的验证脚本又补了一层就绪治理：

- 在 `verify_chapter_5_end_to_end.ps1` 中加入 `Wait-ForKafkaReady`
- 先检查 Kafka 容器状态
- 如果发现 `exited`，先等待再执行 `docker start ecom-kafka`
- 重试 topic 列表探测，直到 broker 真正 ready

这一步的意义有两层：

- 短期内让第 5 章验证脚本在当前 ZooKeeper 架构下更稳
- 长期上也为后续第 7 章推进 `ZooKeeper -> KRaft` 架构演进埋下明确动机

## 10. 文件规划

本章当前涉及的主要文件如下：

- `infra/.env.example`
- `infra/docker-compose.yml`
- `jobs/sql/06_create_iceberg_catalog.sql`
- `jobs/sql/07_sink_user_behavior_to_iceberg.sql`
- `jobs/sql/08_create_iceberg_catalog_local.sql`
- `jobs/sql/09_sink_user_behavior_to_iceberg_local.sql`
- `scripts/run_chapter_5_iceberg_pipeline.ps1`
- `scripts/run_chapter_5_local_iceberg_validation.ps1`
- `scripts/verify_chapter_5_end_to_end.ps1`
- `scripts/verify_chapter_5_readback.ps1`
- `tests/test_chapter_5_artifacts.py`
- `tests/test_chapter_5_local_validation.py`
- `tests/test_chapter_5_end_to_end_validation.py`
- `tests/test_chapter_5_readback_validation.py`
- `README.md`
- `jobs/README.md`

文档文件：

- `docs/superpowers/specs/2026-07-08-chapter-5-minio-iceberg-design.md`
- `docs/superpowers/plans/2026-07-08-chapter-5-minio-iceberg-implementation.md`

## 11. 当前状态判断

截至目前，第 5 章可以分成两部分看：

### 已完成

- Chapter 5 所需 Compose、脚本、SQL、README、测试骨架已补齐
- 本地 filesystem warehouse 验证已成功
- Flink 作业已能把明细写入本地 Iceberg warehouse
- MinIO 版已验证生成 `.parquet` data file 与 `v2.metadata.json`
- MinIO 版已验证可通过 batch 查询读回 `event_count`
- 整体已经形成“可运行 + 可解释 + 可读写验证 + 可继续排障”的工程状态

### 未完成

- 后续仍可以继续补充更长时间持续写入验证
- 可以再增加 Trino 查询入口，形成多引擎读写闭环

## 12. 验证标准

本章当前更真实的验证标准如下：

- Compose 中可定义并启动 MinIO
- 环境变量中有完整的 MinIO / Iceberg 运行配置
- Flink SQL 脚本中能定义 Iceberg catalog 和明细表
- 提交脚本能合并 source + catalog + sink SQL 并提交作业
- 本地 filesystem warehouse 验证可以成功提交并产生 metadata
- MinIO warehouse 中能真实看到 Iceberg 表 metadata 与 data 文件
- 回读脚本能查询到 `event_count` 结果
- README 与作业说明中能反映第 5 章的运行方式
- 对应测试通过

为了进一步把第 5 章做得更扎实，还可以继续增加一条增强标准：

- 在更长时间运行下持续观察 checkpoint、snapshot 与 data file 滚动是否稳定

## 13. 面试表达

这一章很适合用“先实现，再定位，再收敛”的方式来讲：

“第 5 章我给项目补上了 MinIO + Iceberg 明细落湖链路，但真正有价值的不是把配置文件写完，而是我把问题拆成了几层。先补齐 Flink + Iceberg 所需 jar，再把 catalog 路线收敛到 Hadoop S3A。之后我发现 source 和 catalog 都能创建成功，但访问 warehouse 时会报 403。为了确认不是 Iceberg 本身有问题，我又单独加了一条本地 filesystem warehouse 验证链路，先证明 `Flink -> Iceberg` 是通的。再往下一层，我直接用 Hadoop `FsShell` 验证 S3A 本身也能访问 MinIO，最后才定位到根因是 `HadoopCatalog` 没有真正吃到 SQL 里的 `fs.s3a.*`，正式修复方案是把配置下沉到 Hadoop `core-site.xml`。修完之后我继续补 checkpoint、端到端验证和 batch 回读验证，最终不仅在 MinIO 中看到了 `.parquet` data file 和新版本 metadata，也能稳定查回 `event_count`。中间我还顺手处理了 ZooKeeper 模式下 Kafka 反复重建会报 `NodeExistsException` 的问题，这也正好为后续升级 KRaft 埋下了架构演进动机。”

这种表达会比“我配置了 MinIO 和 Iceberg”更像真实做过工程的人。

## 14. KRaft 迁移后的回归设计约束

第 7 章完成 `controller + broker` 分角色 KRaft 迁移后，第 5 章的验证设计需要区分“历史故障背景”和“当前运行机制”。ZooKeeper znode 残留仍作为迁移动机保留，但当前脚本不再包含 ZooKeeper 恢复逻辑。

KRaft 下的 Chapter 5 回归遵循以下约束：

- **broker 业务就绪必须主动探测**：controller 恢复 metadata log、broker 注册和解除 fenced 状态需要时间，不能只检查容器 Running。
- **topic 必须幂等保障**：验证脚本在 Flink 提交前显式执行 `--create --if-not-exists`，同时兼容 KRaft 持久元数据中 topic 已存在的情况。
- **Flink REST 先于 SQL 提交就绪**：只有 JobManager REST 返回成功，才允许 SQL Client 提交 filesystem Iceberg 作业。
- **验证支线保持独立价值**：MinIO + Hive Metastore 是当前主线，本地 filesystem warehouse 仍作为不依赖对象存储和共享 metastore 的最小 Iceberg 回归基线。
- **架构升级后必须回归旧链路**：KRaft 迁移不能只验证 Kafka 自身，还要回到 Iceberg 消费链路确认 source、catalog 和 sink 作业仍能持续运行。

这使第 5 章不只是一次性的落湖实现，也成为后续基础设施升级时可重复使用的回归测试入口。
