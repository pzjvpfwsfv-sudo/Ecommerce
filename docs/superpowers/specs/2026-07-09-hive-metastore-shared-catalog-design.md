# 共享 Hive Metastore Catalog 演进设计

## 1. 背景

第 5 章先用 Flink SQL + Iceberg + MinIO 跑通了明细落湖最小闭环，第 6 章又引入 Trino 做独立查询验证。真实排障之后，我们得到一个非常重要的结论：

- Flink 侧用 `HadoopCatalog` 可以先把写入链路跑通
- 但 Trino 458 不能直接共享这套 `HadoopCatalog`
- 因此项目必须从“单引擎可用”升级到“多引擎共享元数据”

这次设计的目标不是继续给旧方案打补丁，而是正式引入共享 metastore，把第 5 章和第 6 章接成同一条湖仓链路。

## 2. 问题定义

原始第 5 章使用的是：

```sql
CREATE CATALOG lakehouse WITH (
    'type' = 'iceberg',
    'catalog-type' = 'hadoop',
    'warehouse' = 's3a://warehouse/iceberg',
    'property-version' = '1'
);
```

这个方案的优点是简单，便于先收敛 MinIO、S3A、Iceberg 写入问题；但它有一个天然边界：

- 元数据没有进入 Trino 可直接共享的 catalog 层
- Trino 458 不支持 `iceberg.catalog.type=hadoop`
- 多引擎共享同一张 Iceberg 表时，必须统一 metadata plane

所以这次演进的本质，是把“Flink 自己能写”升级为“Flink 能写，Trino 能查，二者共享同一套表定义”。

## 3. 设计目标

本轮设计需要达成下面几个目标：

- 保留 MinIO 作为对象存储
- 引入 Hive Metastore 作为共享 Iceberg 元数据服务
- Flink 与 Trino 共用同一个 `lakehouse` catalog
- Chapter 5 脚本验证写入成功
- Chapter 6 脚本验证 Trino 查询成功
- 文档中保留真实排障与演进痕迹，方便复盘和面试表达

## 4. 方案选择

### 4.1 备选方案

可选路线包括：

- Hive Metastore
- Iceberg REST Catalog
- Nessie
- JDBC Catalog

### 4.2 为什么选 Hive Metastore

当前阶段最合适的是 Hive Metastore，原因有三点：

- 与 Flink + Trino + MinIO 组合最容易对齐
- 最适合讲“从单引擎 catalog 演进到共享 catalog”的工程故事
- 本地 Compose 落地复杂度最低，验证闭环最直接

因此本轮正式采用：

**MinIO + Iceberg data files + Hive Metastore + Trino/Flink 共享 catalog**

## 5. 目标架构

演进后的主链路如下：

`Kafka -> Flink SQL -> Iceberg(MinIO) -> Hive Metastore -> Trino`

各组件职责：

- Kafka：承接行为事件流
- Flink SQL：消费事件流并写入 Iceberg
- MinIO：存储 Iceberg data/metadata 文件
- Hive Metastore：存储共享表定义
- Trino：作为独立查询引擎读取湖表

## 6. 关键设计点

### 6.1 Compose 侧

在 `lakehouse` profile 中新增 `hive-metastore` 服务：

- 默认仅容器内可见
- 不增加新的业务对外入口
- 与 `minio`、`flink`、`trino` 同网络运行

### 6.2 Flink 侧

把 Iceberg catalog 从 `hadoop` 切到 `hive`：

```sql
CREATE CATALOG lakehouse WITH (
    'type' = 'iceberg',
    'catalog-type' = 'hive',
    'uri' = 'thrift://hive-metastore:9083',
    'warehouse' = 's3a://warehouse/iceberg',
    'property-version' = '1'
);
```

这里继续保留 `warehouse = s3a://warehouse/iceberg`，避免同时改变对象存储路径和 catalog 机制。

### 6.3 Trino 侧

Trino catalog 切换到：

```properties
connector.name=iceberg
iceberg.catalog.type=hive_metastore
hive.metastore.uri=thrift://hive-metastore:9083
fs.native-s3.enabled=true
```

同时继续保留 MinIO 所需 S3 参数，让 Trino 能通过共享 metastore 找到表定义后再去读取对象存储中的真实文件。

## 7. 验证设计

### 7.1 Chapter 5 验证

第 5 章不再只检查 MinIO 里是否出现新文件，还要确认：

- 通过共享 metastore 成功建表
- 新写入事件真的落到 Iceberg 表中
- MinIO 中出现新增 metadata/data objects

### 7.2 Chapter 6 验证

第 6 章需要确认：

- Trino 服务已就绪
- 能通过 `lakehouse.analytics.user_behavior_detail` 查到正数 `event_count`
- 能跑通 `GROUP BY event_type`

这里的验证目标是“最终可查”，而不是强求 Flink 刚写完的瞬时强一致可见。

## 8. 本轮真实排障结论

这次演进里有几条非常值得记录的真实经验：

- Trino 458 不能使用 `fs.s3.enabled=true`，必须改为 `fs.native-s3.enabled=true`
- PowerShell 在处理单行单列 REST 结果时会出现单元素数组的形状陷阱，不能直接依赖隐式转换
- Flink 写入完成后，Trino 查询需要允许短暂的最终一致重试窗口
- Compose 多次重建后可能残留哈希前缀容器，需要主动清理以避免命名冲突

这些都不是“理论配置项”，而是已经在本项目里实际遇到并修过的问题。

## 9. 完成标准

本轮设计完成后，应满足：

- Compose 中存在 `hive-metastore`
- Flink catalog 已切到 Hive Metastore
- Trino catalog 已切到 `hive_metastore`
- Chapter 5 验证通过
- Chapter 6 验证通过
- 文档已记录这次共享 catalog 演进及真实故障

## 10. 面试表达

这段非常适合形成一段成熟的架构演进故事：

“我先用 HadoopCatalog 跑通了 Flink 写 Iceberg 的最小闭环，但在接入 Trino 时发现多引擎没法直接共享这套 catalog。于是我没有继续在错误假设上打补丁，而是把架构升级成共享 Hive Metastore，让 Flink 和 Trino 共用同一套 Iceberg 元数据。这个过程和后面 Kafka 从 ZooKeeper 升级到 KRaft 一样，都是项目里真实发生过的架构演进。”
