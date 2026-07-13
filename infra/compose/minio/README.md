# MinIO Compose 说明

这一目录用于承接第 5 章的 MinIO 对象存储配置。

当前职责：

- 为 Iceberg 提供本地开发环境下的对象存储底座
- 通过 `warehouse` bucket 存放 `s3://warehouse/iceberg` 路径下的数据文件
- 配合 Flink SQL 形成最小 `Kafka -> Flink -> Iceberg on MinIO` 明细落湖链路

这一章只做单机开发版，不追求生产级高可用。