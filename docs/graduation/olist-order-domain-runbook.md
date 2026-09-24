# G2-E Olist 订单域真实验收与运行手册

## 结论

2026-09-24，官方 Olist Brazilian E-Commerce Public Dataset Version 2 的九文件订单域完成真实动态验收，状态为 `PASS`。本轮不是模拟数据：44,717,580 字节官方 ZIP 经 SHA-256 固定，九个 CSV 共 1,550,922 条逻辑记录全部规范化并逐表写入 Iceberg；九张主题事实/维表、六类 Doris 指标及八个 FastAPI 合同均绑定同一数据 bundle、Snapshot 集与不可变发布 run。

这是一轮本机正确性和可恢复性验收，不是生产容量或实时 CDC 结论。Olist 是匿名化历史订单快照，REES46 是独立的匿名行为事件，两者没有可证明的共同用户、商品、会话或订单身份，系统不会强行拼接。

## 实测身份

| 字段 | 实测值 |
| --- | --- |
| `dataset_id` | `olist-brazilian-ecommerce-v2` |
| 许可 | `CC BY-NC-SA 4.0` |
| 获取时间 | `2026-09-23T01:13:05.3438116Z` |
| ZIP SHA-256 | `967e41e04fc306fe604e2a693f488995a8b41e5047418f8a5c8e4abd6deca784` |
| ZIP 大小 | 44,717,580 字节 |
| 九文件原始大小 | 126,186,995 字节 |
| 九文件逻辑记录 | 1,550,922 |
| `source_bundle_sha256` | `3e0119b83f4ae47a992a6b2dcf30a6ed57403ef798413209ed52d2d4e624c3c3` |
| `source_snapshot_set_sha256` | `175188d8bb64dad62cb42031e1594bda1e7c295d99f31f4cc0278c6549bcff3f` |
| `metric_version` | `orders-v1` |
| `metric_run_id` | `orders-v1-b3e0119b83f4ae47a992a6b2dcf30a6ed57403ef798413209ed52d2d4e624c3c3` |
| 实现修订 | `839bb1b47b1b30991039a9d479c91519a64540a3` |
| 订单数 / 窗口 | 99,441 / `2016-09-04` 至 `2018-10-17` |
| Doris 状态 | `PUBLISHED` |
| 最终验收时间 | `2026-09-24T21:20:13.040715+08:00` |

原始文件、规范化 JSONL、运行报告和数据库卷均保留在 D 盘，不进入 Git。来源与重新准备步骤见 [Olist 官方订单数据获取与规范化](olist-source-acquisition.md)。

## 入湖与主题层证据

每个源实体都由独立有限 Flink SQL 作业处理，作业达到 `FINISHED` 后才验证并进入下一实体。源 CSV 行数、规范化 JSONL 行数、Iceberg 行数、不同 `source_row_id` 数、顺序摘要、业务键和正 Snapshot ID 必须全部一致。

| 源实体 | 行数 | Iceberg Snapshot |
| --- | ---: | --- |
| `customers` | 99,441 | `6476368525662595773` |
| `geolocation` | 1,000,163 | `3835813025350723910` |
| `order_items` | 112,650 | `4820297996919472284` |
| `order_payments` | 103,886 | `7848428143017747577` |
| `order_reviews` | 99,224 | `3345049485018152300` |
| `orders` | 99,441 | `2306747135250026540` |
| `products` | 32,951 | `6880872970175923890` |
| `sellers` | 3,095 | `7523495536916301844` |
| `category_translation` | 71 | `5368812179443702820` |

主题层按固定顺序建立五张维表和四张事实表。每张表的预期行数、实际行数和不同业务键数完全一致：

| 主题表 | 粒度 | 行数 | Snapshot |
| --- | --- | ---: | --- |
| `customer_dim_v1` | `customer_id` | 99,441 | `3242553942658864678` |
| `category_dim_v1` | 品类名称键 | 71 | `2124051559117307924` |
| `product_dim_v1` | `product_id` | 32,951 | `2913382658234885776` |
| `seller_dim_v1` | `seller_id` | 3,095 | `2850455552738550006` |
| `geolocation_dim_v1` | 邮编前缀 | 19,015 | `940667031475316666` |
| `order_fact_v1` | `order_id` | 99,441 | `7007108038420768286` |
| `order_item_fact_v1` | `order_id + order_item_id` | 112,650 | `3110695158349908422` |
| `payment_fact_v1` | `order_id + payment_sequential` | 103,886 | `5668481217297052539` |
| `review_fact_v1` | 源评价记录 | 99,224 | `4493448580549588133` |

反扇出对账中，订单、商品明细、支付和评价的源/事实行数分别为 `99,441/99,441`、`112,650/112,650`、`103,886/103,886`、`99,224/99,224`。商品值、运费值和支付值的源/事实和分别保持 `13,591,643.70`、`2,251,909.54`、`16,008,872.12`，但源数据没有已核实币种、退款、折扣、成本和会计结算语义，因此这些值不能称为利润、净收入或审计 GMV。

## 数据质量

18 项发布硬门禁全部为 0，包括重复订单/客户/商品/卖家/品类/明细/支付键，七类事实与维度孤儿键，空关键 ID、混合 bundle、负金额和非法时间。以下非零问题被保留并通过 `/api/v1/orders/quality` 对外报告，没有删除或伪造修复：

| 可报告问题 | 实测数量 |
| --- | ---: |
| 重复 `review_id` | 789 |
| 缺失商品品类 | 610 |
| 缺失品类翻译 | 13 |
| 一单多评价 | 547 |
| 缺失可选时间 | 2,980 |
| 生命周期时间异常 | 1,382 |
| 支付与商品加运费不一致订单 | 576 |

未知订单状态和未知支付类型均为 0。评价事实使用稳定源记录粒度，因此重复 `review_id` 是可报告质量现象，而不是允许破坏事实主键的重复键。

## Doris 发布证据

所有 Trino 候选先生成规范 CSV 和 SHA-256，再由 Doris Stream Load 写入；六族全部回读并重算摘要成功后，最后插入唯一 `PUBLISHED` 行。已发布 run 不覆盖，身份相同但内容不同的重试会失败关闭。

| 指标族 | 行数 | SHA-256 |
| --- | ---: | --- |
| `overview` | 660 | `946a9c975b6be9fdfb496586952de7b9e0c7ced910976739adb01f273c5c106b` |
| `delivery` | 660 | `fea0a4ed6947245c7737de022f6b42b856858027e68bdfe6eca829d9dddf23be` |
| `payment` | 3,031 | `189a1ccad6d67f5df29b8f096354f98760b67173ae83ca8af005180f68640dbc` |
| `ranking` | 316,153 | `98f96c37cb378aebc1d5abbbb72c915ae537e76dd00dfbcb03067099406bb496` |
| `review` | 660 | `6ea2bfe6fa26a40b133b9b107c86a6e1f4ebf67bce8e26a5a8427ab72aba49ce` |
| `quality` | 1 | `a8f94a0a147cbc14bdc0948ec1c447bfa884a2f1e2d1444e9886d2d9d58d99e1` |

指标提供 `DAY`、`MONTH`、`FULL` 窗口，覆盖订单概览、履约时长与晚到、支付方式、品类/商品/卖家/客户地域排名、评价和质量。公开目录共 54 个精确定义，逐项记录公式、分子分母、窗口、NULL 策略、排除项、限制与禁止声明。

## API 验收

以下八个请求均返回 HTTP 200，且响应元数据与 Trino Snapshot、Doris 发布行、行数和摘要一致：

```text
/api/v1/orders/publication
/api/v1/orders/overview?window=full
/api/v1/orders/delivery?window=full
/api/v1/orders/payments?window=full
/api/v1/orders/rankings?dimension=category&window=full&sort_by=item_value&limit=20
/api/v1/orders/reviews?window=full
/api/v1/orders/quality
/api/v1/metrics/definitions?domain=orders&version=orders-v1
```

额外实测 DAY 范围为 `2016-09-04`，MONTH 查询范围为 `2016-09-04` 至 `2016-09-15`。非法窗口、日期、维度、排序和 limit 返回 422；离线合同测试确认不存在的发布身份返回安全 503，且不会修改实时数据。

## 重现命令

PowerShell 5.1 可直接执行。Docker 数据、临时目录与 Olist 数据均位于 D 盘；恢复已有 Hive Metastore 时必须显式开启已验证的 resume 开关。

```powershell
$env:PATH='D:\DockerProgram\Docker\resources\bin;' + $env:PATH
$env:TEMP='D:\EcommerceDev\temp'
$env:TMP=$env:TEMP
$env:HIVE_METASTORE_IS_RESUME='true'
$manifest='D:\EcommerceData\olist\prepared\3e0119b83f4ae47a992a6b2dcf30a6ed57403ef798413209ed52d2d4e624c3c3\source-bundle.json'

$entities=@(
  'orders','order_items','order_payments','order_reviews','customers',
  'products','sellers','geolocation','category_translation'
)
foreach ($entity in $entities) {
  .\scripts\run_g2e_olist_source.ps1 -ManifestPath $manifest -Entity $entity -TimeoutSeconds 600
}

.\scripts\build_g2e_olist_curated.ps1 -ManifestPath $manifest -TimeoutSeconds 600
.\scripts\refresh_g2e_order_metrics.ps1 -ManifestPath $manifest -TimeoutSeconds 600
.\scripts\verify_g2e_order_domain.ps1 -ManifestPath $manifest -ApiBaseUrl 'http://localhost:8000' -TimeoutSeconds 600
```

最终验证器约运行 9 分 30 秒，只在所有断言通过后原子写入被 Git 忽略的 `tmp/graduation/g2e/<bundle>/acceptance.json`；已有证据文件不会被覆盖。

## 自动化验证

- 计划指定的 G2-E 离线组合通过 100 项，耗时 48.739 秒；1 项仅因当前 Windows 主机不支持符号链接而按设计跳过。
- 最终全仓 `python -m unittest discover -s tests -v` 通过 658 项，耗时 237.357 秒；同一符号链接能力项跳过，无失败。
- 全仓首轮暴露出 G2-E 新增的 `HIVE_METASTORE_IS_RESUME` 未进入第 10.5 章隔离冷启动环境。修复后隔离模式强制 `false`，聚焦测试 2/2、冷启动模块 34/34 和上述全仓回归均通过；普通恢复仍须显式设置 `true`。
- `docker compose --env-file infra/.env.example -f infra/docker-compose.yml config --quiet` 与 `git diff --check` 通过。没有 Java/DataStream 文件变化，按计划未重复 Maven 测试。

## 资源与排障记录

- Docker Desktop 分配上限为 7.66 GiB。有限入湖采样记录中，Trino 最高 3.548 GiB、Flink TaskManager 约 1.024 GiB、JobManager 612.4 MiB、Hive Metastore 586.1 MiB、MinIO 202.6 MiB；这些是离散采样观测值，不冒充严格峰值测量。
- 89 stage 的排名查询在 Flink 和 Doris 同时驻留时使 Trino 接近 5 GiB并触发 exit 137。最终流程先停止空闲 Flink/Doris，完成 Trino 查询并停止 Trino，再启动 Doris 发布；只停容器，不删除卷、表、Snapshot 或 Checkpoint。
- 316,153 行排名 CSV 原解析方式约占 5 GiB。改为单遍解析、先验表头后，实测工作集约 1.30 GiB，解析耗时 35.5 秒。
- Trino HTTP 信息端点会早于查询能力可用，readiness 因此改为用同一 CLI 执行 `SELECT 1 AS ready`。24 个履约窗口没有合格时长，CLI 的 `\N` 与带引号空 NULL 只在 Trino 指标传输边界规范化。
- Windows PowerShell 5.1 会改变原生命令双引号并把 Compose 正常 stderr 提升为终止错误。脚本仅在原生命令边界转义参数和暂时调整错误策略，非零退出仍保留 stdout/stderr 并失败关闭。
- 官方卖家城市含逗号和反斜杠。Trino 使用 RFC CSV；Doris 上传副本只在 Stream Load 边界转义引号与非 NULL 反斜杠，精确 `\N` 保持 NULL，解决了 9 行卖家名称摘要不一致问题。
- Docker Desktop 曾因 `%LOCALAPPDATA%\Docker\run\dockerInference` 残留 socket 无法启动。恢复时退出 Docker、保留并改名故障运行目录，未使用 factory reset，D 盘镜像、卷和项目数据均未删除。

## 恢复与边界

源表和主题表都有逐阶段报告。重跑会先核对 bundle、Snapshot、粒度和摘要：完全一致则安全复用，空表、未记录表、混合身份或最新 Snapshot 不一致均停止，不自动清理。指标生成中断时保留已验证候选；恢复仍须重新校验候选文件摘要、Doris 回读摘要和完整发布身份，发布行始终最后写入。

验收证明本机能以分阶段方式处理 155 万条真实订单域记录并提供稳定 API，不证明生产 HA、持续 CDC、全链路 exactly-once、并发容量或 SLA。下一阶段是 G3 业务可视化和 G4 版本绑定的知识库/RAG；RAG 只能引用 `orders-v1` 定义和已发布 run，不能跨 Olist 与 REES46 推断用户旅程。
