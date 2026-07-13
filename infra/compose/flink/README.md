# Flink Compose 说明

这一目录保存本项目 Flink 本地运行所需的补充依赖和配置。

当前已经包含：

- `lib/`：Kafka、Doris、Iceberg、Hadoop、AWS SDK 等运行时 jar
- `conf/core-site.xml`：给 HadoopCatalog 注入 MinIO 所需的 S3A 配置
- `conf/hdfs-site.xml`：提供最小 Hadoop 配置骨架，保证容器内 Hadoop 组件可正常读取配置目录

第 5 章的关键排障结论之一是：

- `CREATE CATALOG ... WITH ('fs.s3a.*' = ...)` 并不能稳定让 Iceberg `HadoopCatalog` 读到 MinIO S3A 配置
- 正式修复方式是通过 Compose 挂载 `conf/` 到 Flink 容器，并设置 `HADOOP_CONF_DIR=/opt/hadoop-conf`

这也是为什么现在 MinIO 版 Iceberg 落湖链路已经能够成功提交并写出 metadata 文件。
