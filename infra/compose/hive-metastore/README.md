# Hive Metastore Compose 说明

该目录用于记录第 5 章和第 6 章共用的 Iceberg 元数据服务。

- `hive-metastore` 仅在 `lakehouse` profile 内部运行
- MinIO 继续作为对象存储层
- Flink 与 Trino 共用 `thrift://hive-metastore:9083`
