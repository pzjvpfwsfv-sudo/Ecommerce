# 共享 Hive Metastore Catalog 实现记录

## 1. 本轮目标

本轮实现的目标，是把第 5 章和第 6 章从“Flink 单独可用的 HadoopCatalog”升级为“Flink + Trino 共享的 Hive Metastore catalog”。

最终要求不是只改配置，而是要让下面两件事都真正成立：

- Flink 能持续把行为明细写入 MinIO 上的 Iceberg 表
- Trino 能通过共享 metastore 查询同一张 Iceberg 表

## 2. 实际完成的改动

### 2.1 基础配置与 Compose

已完成：

- `infra/.env.example`
  - 增加 Hive Metastore 相关默认值
- `infra/docker-compose.yml`
  - 增加 `hive-metastore` 服务
  - 将 Flink、Trino 与 MinIO 串到共享 catalog 依赖链上
- `infra/compose/trino/catalog/lakehouse.properties`
  - 切换为 `iceberg.catalog.type=hive_metastore`
  - 配置 `hive.metastore.uri=thrift://hive-metastore:9083`

### 2.2 Flink / SQL / 脚本

已完成：

- `jobs/sql/06_create_iceberg_catalog.sql`
  - 从 `catalog-type = hadoop` 改为 `catalog-type = hive`
- `scripts/run_chapter_5_iceberg_pipeline.ps1`
  - 启动链路中显式带上 `hive-metastore`
  - 增加清理哈希前缀残留容器逻辑
- `scripts/verify_chapter_5_end_to_end.ps1`
  - 从“比较 metadata 版本号”升级成“比较新增对象文件”
- `scripts/verify_chapter_5_readback.ps1`
  - 继续复用共享 catalog 做回读验证
- `scripts/verify_chapter_6_trino_queries.ps1`
  - 接入 Trino readiness、statement readiness、查询重试与结果归一化

### 2.3 测试与文档

已完成：

- `tests/test_chapter_5_artifacts.py`
- `tests/test_chapter_5_end_to_end_validation.py`
- `tests/test_chapter_6_trino_artifacts.py`
- `README.md`
- `jobs/README.md`
- 第 5/6 章文档补充共享 catalog 演进叙事

## 3. 真实排障过程

### 3.1 Trino 458 的 S3 配置键并不是旧写法

最早切到 Hive Metastore 后，Trino 仍然起不来。查看容器日志后发现：

- `fs.s3.enabled`
- `s3.endpoint`
- `s3.region`
- `s3.aws-access-key`
- `s3.aws-secret-key`

这套参数在当前配置下被判定为“未使用”。

最终确认：

- Trino 458 需要启用 `fs.native-s3.enabled=true`

修复后，日志里出现了：

- `Added catalog lakehouse using connector iceberg`
- `SERVER STARTED`

这说明 Trino catalog 配置终于真正生效。

### 3.2 Compose 多次重建后会残留哈希前缀容器

在多次 `docker compose up` / 中断重试之后，环境里出现了类似：

- `529cd75b2300_ecom-minio`
- `e5203d2283d6_ecom-zookeeper`

这样的残留容器，进一步引发命名冲突和服务重建异常。

最终修复：

- 在 `scripts/run_chapter_5_iceberg_pipeline.ps1` 中增加清理残留哈希前缀容器逻辑

这一步很重要，因为不先把环境清干净，后面的故障很容易被假象污染。

### 3.3 Chapter 5 不能再依赖 metadata 编号递增

早期第 5 章验证脚本是用 metadata version 是否递增来判断 Iceberg 新提交是否成功。但在实际重建表和反复验证时，这个判断会失真，因为：

- 表重建后 metadata 文件编号可能重新开始
- 版本号不一定能稳定代表“这次脚本触发了新提交”

最终修复：

- 改成比较基线对象集合和新对象集合
- 只要出现新增 metadata/data object，就认为这次提交确实发生

这个改法比单纯依赖数字递增更稳，也更符合实际对象存储验证思路。

### 3.4 PowerShell 对单元素查询结果的数组形状很“怪”

Trino 手工 REST 查询其实已经能查到 `COUNT(*)`，但脚本里却一直判断成“零行”。根因不是 Trino，而是 PowerShell：

- 顶层结果是 `System.Object[]`
- 单行单列结果在脚本里会表现成单元素数组
- `ConvertTo-Json` 还会把它显示成带 `value` 和 `Count` 的形状

最终修复：

- 显式把 Trino 查询结果归一化成二维表结构
- 再从第一行第一列安全提取标量

### 3.5 第 6 章需要验证“最终可查”，而不是“瞬时可查”

即便结果解析逻辑修好，Flink 刚写完后 Trino 也不一定立刻看到大于 0 的数据。

这不是链路坏了，而是本地开发环境下的短暂可见性窗口。

最终修复：

- 在 `scripts/verify_chapter_6_trino_queries.ps1` 中加入非 0 count 的轮询等待

这样脚本验证的是“Chapter 6 最终读通”，而不是“强行要求毫秒级瞬时一致”。

## 4. 最终验证结果

本轮已经得到以下真实验证结果：

### 4.1 测试通过

执行：

```powershell
python -m unittest tests.test_chapter_5_artifacts tests.test_chapter_5_end_to_end_validation tests.test_chapter_6_trino_artifacts -v
```

结果通过。

### 4.2 Chapter 5 端到端通过

执行：

```powershell
./scripts/verify_chapter_5_end_to_end.ps1
```

结果通过，脚本能检测到 MinIO 中新增的 metadata 与 parquet objects。

### 4.3 Chapter 5 回读通过

执行：

```powershell
./scripts/verify_chapter_5_readback.ps1
```

结果通过，能得到正数 `event_count`。

### 4.4 Chapter 6 查询通过

执行：

```powershell
./scripts/verify_chapter_6_trino_queries.ps1
```

最终输出示例：

```text
[chapter6-verify] event_count=813
[chapter6-verify] top_event_type=view count=288
```

这说明共享 Hive Metastore 之后，Flink 写入和 Trino 查询已经真正闭环。

## 5. 这轮实现的工程价值

这轮工作的价值，不只是“Trino 终于查到了数据”，而是把项目从：

- 单引擎 Iceberg 写入演示

升级成了：

- 多引擎共享 catalog 的真实湖仓闭环

同时，我们还积累了三类非常有价值的工程经验：

- 配置兼容性问题要以运行日志为准
- 验证脚本不能过度依赖理想化假设
- 本地教学环境也要按真实工程方式处理最终一致与环境清理

## 6. 对后续章节的帮助

这轮共享 metastore 演进，会和后续 Kafka 从 ZooKeeper 升级到 KRaft 形成两条很清晰的故事线：

- 湖仓侧：`HadoopCatalog -> Hive Metastore`
- 消息侧：`ZooKeeper -> KRaft`

这两条线都不是“为了堆技术名词”，而是项目里真实遇到限制之后推动出来的架构演进。

## 7. 面试表达

这一段可以很自然地讲成：

“我一开始先用 HadoopCatalog 跑通了 Flink 写 Iceberg 的最小闭环，但接入 Trino 时发现多引擎不能直接共享这套元数据。于是我把架构升级到共享 Hive Metastore，让 Flink 和 Trino 共用同一个 Iceberg catalog。过程中我还处理了 Trino 458 的 S3 配置差异、Compose 残留容器、PowerShell 查询结果形状、以及最终一致等待问题。最后把 Chapter 5 写入和 Chapter 6 查询都验证通过了。”
