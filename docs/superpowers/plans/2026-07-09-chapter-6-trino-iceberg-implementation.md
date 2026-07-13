# 第 6 章：Trino + Iceberg 湖表查询实现记录

## 1. 本章定位

这一章原本要完成的是：

- 接入 Trino
- 让 Trino 查询第 5 章写入 MinIO + Iceberg 的行为明细表
- 用自动化脚本完成查询验证

但实际执行后，本章的性质已经从“直接实现查询闭环”变成了：

**先完成 Trino 接入脚手架，再通过真实运行把架构阻塞点定位清楚。**

这份文档记录的不是理想化计划，而是已经发生过的真实实现与排障过程。

## 2. 已完成实现

### 2.1 基础接入

已经完成以下工程改动：

- `infra/.env.example`
  - 增加 `TRINO_PORT=8088`
  - 增加 `TRINO_CONTAINER_NAME=ecom-trino`
- `infra/docker-compose.yml`
  - 增加 `trino` 服务
  - 挂载 `./compose/trino/catalog:/etc/trino/catalog:ro`
- `infra/compose/trino/catalog/lakehouse.properties`
  - 新增 `lakehouse` catalog 配置
- `jobs/sql/11_trino_read_iceberg_user_behavior.sql`
  - 新增 count 与 group by 两条验证 SQL
- `scripts/verify_chapter_6_trino_queries.ps1`
  - 新增 Chapter 6 查询验证脚本
- `README.md` / `jobs/README.md`
  - 补齐 Chapter 6 使用说明
- `tests/test_chapter_6_trino_artifacts.py`
  - 补齐 Chapter 6 产物测试

### 2.2 已通过验证的部分

当前已经确认通过的内容包括：

- Chapter 6 相关文件都已创建并接入工程
- Python 产物测试可通过
- Trino 镜像可拉取并启动容器流程
- PowerShell 验证脚本可以串起：
  - Chapter 5 数据准备
  - Trino 服务启动
  - `/v1/info` 就绪探测
  - `/v1/statement` 查询探测

## 3. 真实排障过程

### 3.1 第一层问题：镜像拉取慢

最开始脚本看起来像“卡住”，其实第一层原因是：

- `trinodb/trino:458` 首次拉取镜像较大
- Docker pull 时间较长

这个问题后来已经消除，不再是主要阻塞。

### 3.2 第二层问题：statement API 先于 catalog 完全可用

后续排查中又发现：

- `/v1/info` 返回，不代表 `/v1/statement` 一定已经能稳定执行
- 脚本最开始只等 `info`，会出现 `Trino server is still initializing`

因此验证脚本后来补了两层保护：

- `Wait-ForTrinoStatementReady`
- `Invoke-TrinoStatement` 内部的 `still initializing` 重试

这一步解决的是“假就绪”问题，但不是最终根因。

### 3.3 第三层问题：真正的架构阻塞

最终从 Trino 容器日志里拿到了根因：

- `Invalid value 'hadoop' for type CatalogType (property 'iceberg.catalog.type')`
- `iceberg.hadoop.warehouse` was not used
- 多个 `s3.*` 参数 was not used

这说明：

- Trino 458 不接受 `iceberg.catalog.type=hadoop`
- 第 6 章最初采用的 catalog 方案在 Trino 侧根本无法建立
- 脚本等待、HTTP 探测、S3 参数都只是外围症状

根因是：**当前目录下的 Trino catalog 配置试图复用 HadoopCatalog，但 Trino 458 不支持这样做。**

## 4. 结论

到这里为止，本章已经得到一个非常明确的结论：

**第 5 章的 HadoopCatalog 写侧方案，不能直接作为第 6 章的 Trino 读侧方案。**

换句话说：

- 单引擎写侧闭环已经成立
- 多引擎共享查询闭环还没有成立

这不是失败，而是一次很有价值的架构边界发现。

## 5. 当前代码状态应如何理解

### 5.1 已完成

已经可以认为完成的部分：

1. Chapter 6 Trino 服务脚手架
2. Chapter 6 查询 SQL
3. Chapter 6 验证脚本
4. Chapter 6 文档与产物测试
5. 真实运行排障与根因确认

### 5.2 尚未完成

还不能宣称完成的部分：

1. Trino 成功查询当前 MinIO 上的 Iceberg 表
2. Chapter 6 全量端到端 PASS

原因不是“还没调完”，而是 **当前架构前提不成立**。

## 6. 更严格的工程处理

既然根因已经找到，当前这章接下来要做的，不应该是继续盲试参数，而应该是：

1. 在文档中明确记录这次真实排障结果
2. 在脚本中把失败信息做得更直白，避免用户误判为卡住
3. 把下一步架构演进目标收敛到“共享 metastore / catalog”

也就是说，本章最重要的收尾不是“把失败藏起来”，而是把失败变成有效知识。

## 7. 下一步实现方向

下一步如果要真正打通 Trino 查询，需要引入 **Trino 支持的共享 catalog**。建议方向：

- 优先考虑 `Hive Metastore`
- 或者评估 `REST catalog`

对于这个项目当前阶段，更推荐：

**Flink 和 Trino 共同切到共享 metastore catalog。**

这样做的价值有三层：

1. 技术上能真正形成多引擎共享 Iceberg 元数据
2. 工程上能把“第 5 章单引擎可用”升级到“第 6 章多引擎可查”
3. 面试表达上能形成一段自然的架构演进故事

## 8. 对第 7 章与后续故事线的帮助

用户已经明确希望后面一定要升级到 KRaft，形成“架构演进”的面试故事。

这一章其实正好补上了另一段同样真实的演进线：

- Kafka 侧会从 ZooKeeper 走向 KRaft
- 湖仓侧会从单引擎 HadoopCatalog 走向共享 metastore

两条演进线分别代表：

- 消息系统架构演进
- 湖仓元数据架构演进

这会让整个项目不只是“把工具串起来”，而是能讲出连续的工程决策。

## 9. 当前收尾标准

本章当前更严格也更诚实的收尾标准是：

- 文档已改为真实状态，不再误写“Trino 已可直接读 HadoopCatalog”
- 脚本已能在失败时暴露根因
- 测试与文档保持一致
- 下一步共享 metastore 方案已具备实现前提

## 10. 下一步建议

如果继续推进，最合理的顺序是：

1. 先把 Chapter 6 文档、测试、验证脚本都改成真实排障后的状态
2. 再进入 “Chapter 6.5 / Chapter 7 前置” 式的共享 metastore 改造
3. 等 Trino 真正读通后，再补一次完整端到端验证

这样推进会更稳，也更符合真实工程习惯。

## 11. 后续落地结果

这一步后续已经真实完成，关键结果如下：

- 共享 Hive Metastore 服务已加入 Compose
- Trino 458 侧改用 `fs.native-s3.enabled=true`
- Chapter 5 写入验证已通过
- Chapter 5 回读验证已通过
- Chapter 6 查询验证已通过，并输出了非零 `event_count`

也就是说，这份实现记录里识别出的“架构级阻塞”并没有被搁置，而是被继续推进成了真正的共享 catalog 闭环。
