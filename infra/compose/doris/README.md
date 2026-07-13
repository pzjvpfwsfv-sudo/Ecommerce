# Doris Compose 说明

该目录保存第 4 章 Doris 实时指标层的初始化资源。

- Compose 通过 `serving` profile 启动 Doris FE / BE。
- `init/01_create_realtime_metrics.sql` 创建 `analytics.realtime_metrics`。
- Flink SQL 将 PV / UV 更新写入该表。
- FastAPI 通过 Doris MySQL 协议读取实时指标。
