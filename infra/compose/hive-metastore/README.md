# Hive Metastore Compose 说明

该目录用于记录第 5 章和第 6 章共用的 Iceberg 元数据服务。

- `hive-metastore` 仅在 `lakehouse` profile 内部运行
- PostgreSQL 16 持久化 Hive Metastore 元数据；JDBC 驱动固定为 Task 2 锁定的 `postgresql-42.7.4.jar`
- MinIO 继续作为对象存储层，并在初始化成功后提供 `warehouse` 与 `flink-state` bucket
- Flink 与 Trino 共用 `thrift://hive-metastore:9083`
