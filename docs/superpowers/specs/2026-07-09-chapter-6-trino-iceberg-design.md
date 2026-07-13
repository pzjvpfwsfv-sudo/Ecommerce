# 第 6 章：Trino + Iceberg 湖表查询设计

## 1. 背景

第 5 章已经完成了一个很关键的里程碑：

- Flink SQL 可以把行为明细稳定写入 MinIO 上的 Iceberg 表
- `lakehouse.analytics.user_behavior_detail` 已经能通过 Flink SQL 做回读验证
- MinIO `warehouse/iceberg` 下已经产生真实的 metadata 与 data file

但如果查询能力仍然只停留在 Flink SQL Client，这条湖仓链路还不算完整。项目还缺一段非常重要的能力证明：

- 湖表不只是“能写进去”
- 湖表还要“能被独立查询引擎消费”
- 后续 AI 指标分析助手也应该优先依赖清晰的查询入口，而不是直接绑定 Flink SQL Client

因此第 6 章的原始目标，是给第 5 章补上一条独立查询链路：让 Trino 查询 MinIO 上的 Iceberg 明细表。

## 2. 原始目标

本章最初希望完成：

- 在本地 Compose 中加入 Trino 单机服务
- 通过 Iceberg catalog 让 Trino 读取 `lakehouse.analytics.user_behavior_detail`
- 提供一条自动化脚本，完成：
  - `SELECT COUNT(*)`
  - `SELECT event_type, COUNT(*) ... GROUP BY event_type`
- 让项目从“Flink 可写 Iceberg”推进到“Flink 可写、Trino 可读”

## 3. 本章真实结论

经过真实运行与排障，第 6 章得到了一个比“直接跑通”更重要的结论：

**Trino 458 不能直接消费当前第 5 章采用的 Iceberg HadoopCatalog。**

也就是说：

- 第 5 章写侧当前是 `catalog-type = hadoop`
- 第 6 章原本假设 Trino 也可以通过 `iceberg.catalog.type=hadoop` 直接读同一份 warehouse
- 这个假设在真实运行时被证伪

所以第 6 章目前不是“脚本小故障未修完”，而是暴露出一个真实的架构边界：

**Flink 现有 HadoopCatalog 写法，并不能被 Trino 458 原样共享。**

## 4. 证据链

### 4.1 写侧现状

第 5 章当前的 Flink Iceberg catalog 创建方式如下：

```sql
CREATE CATALOG lakehouse WITH (
    'type' = 'iceberg',
    'catalog-type' = 'hadoop',
    'warehouse' = 's3a://warehouse/iceberg',
    'property-version' = '1'
);
```

这说明第 5 章已经明确采用了 HadoopCatalog + MinIO S3A 路线。

### 4.2 读侧尝试

第 6 章已经补齐了以下基础产物：

- Compose 中的 `trino` 服务
- `infra/compose/trino/catalog/lakehouse.properties`
- `jobs/sql/11_trino_read_iceberg_user_behavior.sql`
- `scripts/verify_chapter_6_trino_queries.ps1`
- Chapter 6 README 与测试

也就是说，Trino 接入框架和查询验证链路已经搭起来了。

### 4.3 真实报错

在 `trinodb/trino:458` 上启动 `lakehouse` catalog 时，容器日志给出了关键错误：

- `Invalid value 'hadoop' for type CatalogType (property 'iceberg.catalog.type')`
- `iceberg.hadoop.warehouse` was not used
- 多个 `s3.*` 参数 was not used

这个报错的含义非常明确：

- `hadoop` 不是当前 Trino Iceberg connector 支持的 catalog type
- 因为 catalog type 本身不成立，后续 warehouse 与 S3 相关参数都不会真正生效

### 4.4 官方约束

结合 Trino 官方文档，Iceberg connector 需要接入受支持的 metadata catalog，例如：

- `hive_metastore`
- `glue`
- `jdbc`
- `rest`
- `nessie`
- `snowflake`

其中并没有 `hadoop` 这个 catalog type。

因此这里不是配置名写错，而是路线假设本身不成立。

## 5. 本章范围重定义

基于上面的证据，第 6 章现在应该被重新理解为两个阶段。

### 5.1 已完成部分

第 6 章已经完成：

- Trino 服务脚手架接入 Compose
- Trino 查询 SQL 与 PowerShell 验证脚本
- Chapter 6 文档与测试骨架
- 真实运行验证
- 根因定位

### 5.2 当前阻塞点

第 6 章尚未完成的部分只有一个，但它是架构级阻塞：

- Trino 不能直接查询第 5 章的 HadoopCatalog Iceberg 表

### 5.3 不再假装“再修一修就能通”

本章设计需要明确拒绝一种错误心态：

- 继续在 `iceberg.catalog.type=hadoop` 上反复试参数
- 把启动失败误判为脚本等待问题
- 把 catalog 不兼容误判为 MinIO 鉴权问题

这些都不是根因。

## 6. 正确的后续演进方向

要让 Flink 和 Trino 共享同一套 Iceberg 表元数据，后续必须引入一个 **Trino 支持的共享 catalog / metastore 层**。候选方向包括：

- Hive Metastore
- Iceberg REST catalog
- Nessie
- JDBC catalog

对于本项目当前阶段，最容易讲清楚、也最像传统数据平台演进路线的方案，是：

**从第 5 章单独可用的 HadoopCatalog，演进到“Flink + Trino 共用的 metastore catalog”。**

这会自然形成一段很好的面试故事：

1. 先用 HadoopCatalog 把 Flink -> Iceberg 的最小落湖闭环跑通
2. 再引入 Trino 时，发现多引擎共享元数据的现实约束
3. 由此推动架构从“单引擎可用”升级到“多引擎共享 catalog”
4. 最终再衔接后续 AI 查询入口和更规范的湖仓治理

## 7. 第 6 章交付物定义

从“真实工程”角度看，第 6 章当前已经有两类有效交付。

### 7.1 工程交付

- Trino Compose 服务骨架
- Query SQL 文件
- 自动化验证脚本
- 测试与文档

### 7.2 认知交付

- 证明当前方案的边界在哪里
- 证明这个边界是 catalog 架构问题，而不是脚本问题
- 为下一步共享 metastore 演进提供清晰输入

## 8. 脚本设计要求

由于当前阻塞点已经定位，本章验证脚本不应该再“静默等待直到超时”，而应该：

- 先运行第 5 章验证，确保表里确实有数据
- 启动 Trino 服务
- 在 Trino 没有就绪时主动检查容器状态和日志
- 如果命中 `Invalid value 'hadoop' for type CatalogType`，直接抛出架构级错误说明

这样脚本承担的职责就不只是“验证成功”，还包括“在失败时给出可执行的根因”。

## 9. 文件规划

本章已经涉及或将继续维护的主要文件：

- `infra/.env.example`
- `infra/docker-compose.yml`
- `infra/compose/trino/catalog/lakehouse.properties`
- `jobs/sql/11_trino_read_iceberg_user_behavior.sql`
- `scripts/verify_chapter_6_trino_queries.ps1`
- `tests/test_chapter_6_trino_artifacts.py`
- `README.md`
- `jobs/README.md`
- `docs/superpowers/specs/2026-07-09-chapter-6-trino-iceberg-design.md`
- `docs/superpowers/plans/2026-07-09-chapter-6-trino-iceberg-implementation.md`

## 10. 当前完成标准

第 6 章现阶段更诚实的完成标准应当是：

- Compose 中存在 Trino 服务定义
- Chapter 6 查询 SQL、脚本、README、测试都已补齐
- 验证脚本能在失败时给出真实根因
- 文档已明确记录 `Trino 458` 与 `HadoopCatalog` 不兼容
- 下一阶段演进目标已经清楚指向共享 metastore / catalog

而不是继续把“Trino 已经可以读当前 HadoopCatalog”当作完成标准。

## 11. 风险与边界

本章已经确认的最大风险不是网络、端口或脚本，而是：

- 多引擎共享 Iceberg 元数据时，catalog 选型必须前置考虑

因此本章边界必须保持清楚：

- 不把 `HadoopCatalog` 的单引擎成功误认为多引擎成功
- 不把 Trino 问题和 Kafka/Flink 写入问题混在一起排查
- 不为了“把这一章跑通”而在错误架构上继续加补丁

## 12. 面试表达

这一章非常适合形成一段真实的项目故事：

“第 5 章我先把 Flink 写 Iceberg 的最小闭环跑通，当时为了收敛变量，用的是 HadoopCatalog。到了第 6 章，我想接入 Trino 做独立查询验证，结果真实排障发现 Trino 458 并不支持直接消费这个 catalog type。这个阶段最大的收获不是硬把查询跑通，而是明确识别出多引擎共享湖表时，元数据 catalog 必须统一设计。这个问题定位清楚后，后续我就可以顺势把架构演进到共享 metastore，这样不仅项目更完整，也能讲出一段很真实的工程演进故事。”

## 13. 后续演进结果

这份设计文档记录的是第 6 章最初暴露出来的架构阻塞点，而后续章节已经把这个阻塞真正推进到落地结果：

- 共享 catalog 方案最终选定为 Hive Metastore
- Flink catalog 已从 `hadoop` 切到 `hive`
- Trino catalog 已切到 `hive_metastore`
- Chapter 6 自动化验证已经能查到非零 `event_count` 与分组结果

也就是说，第 6 章并没有停留在“发现问题”，而是自然演进成了后续共享 metastore 架构升级的输入。
