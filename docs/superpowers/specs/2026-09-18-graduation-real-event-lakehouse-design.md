# G2-C 真实事件湖仓落地设计

日期：2026-09-18。承接已完成的 G2-A 可恢复回放与 G2-B Java 真实事件质量入口。用户已确认采用“正常与迟到事件统一事实表”的方案。本文只设计真实事件落湖与 Trino 对账，不把后续指标、接口、前端或 RAG 提前混入。

## 1. 目标与范围

本轮链路为：

```text
real_behavior_clean_v1_* ----\
                              > Flink SQL 规范化 -> Iceberg real_behavior_detail_v1 -> Trino
real_behavior_late_v1_* -----/
```

正常流和迟到流都来自 G2-B 严格校验后的有效真实事件。两路先统一为同一行结构，再通过单一 Iceberg sink 写入独立事实表；`quality_route` 保存 `clean` 或 `late`，避免业务查询长期手工合并两张表。

DLQ 中包含解析失败和重复记录，不进入业务事实表。旧 `user_behavior_detail`、旧 Topic、旧 SQL 和旧页面保持不变。G2-D 再基于本表设计 Doris 指标和版本化 API，本轮不新增常驻服务或修改现有 Agent。

## 2. 方案取舍

- 不采用只落正常流：合法迟到事件仍是历史事实，只读取 clean 会形成不可见漏数。
- 不采用 clean/late 两张业务事实表：所有指标、Trino 查询和 RAG 工具都要重复 `UNION`，容易产生口径漂移。
- 采用统一事实表：两路来源在写入前规范化，表内保留质量路由和迟到诊断。下游默认查询完整有效事实，需要质量分析时再按路由筛选。
- 不把 DLQ 并入事实表：重复或非法记录只能作为质量证据，不能参与业务统计。

## 3. 表与字段契约

新表固定为 `lakehouse.analytics.real_behavior_detail_v1`，与旧八字段演示表隔离。验收可使用带安全后缀的临时表，不能删除或覆盖正式表。run-id 只接受小写字母、数字和连字符，禁止下划线，保证 run-id 转换成 SQL 表后缀时是一一映射。

表中保留 G2-B 正常载荷的全部 17 个原始字段。`event_time`、`price` 和 `replayed_at` 继续保存原字符串，便于逐字段审计；额外生成以下查询字段：

- `event_ts TIMESTAMP_LTZ(3)`：由已经通过 G2-B 校验的 UTC `event_time` 转换，不替换原字符串。
- `event_date DATE`：由 `event_ts` 得到，作为 Iceberg 日分区；61 天真实窗口不会产生过多小分区。
- `price_decimal DECIMAL(20,2)`：由已校验的定点金额字符串转换，禁止使用浮点数。
- `quality_route STRING`：只允许 `clean` 或 `late`。
- `landing_topic,landing_partition,landing_offset,landing_timestamp`：从当前 clean/late Kafka 记录元数据取得，两路都必须存在。
- `origin_topic,origin_partition,origin_offset,quality_job_version,quality_observed_at,watermark_ms,lateness_ms`：来自迟到信封；clean 不携带原输入坐标，因此这些列为 null，不能伪造。

`event_id` 是跨层稳定身份和对账键，但首版表保持 append-only，不声明 Iceberg 主键或永久全局去重。G2-B 的有限 TTL 去重、Flink/Iceberg checkpoint 提交和表内 `COUNT(DISTINCT event_id)` 对账共同界定本轮保证。因为 `earliest-offset` 重提会从头追加，runner 必须同时拒绝已有同名作业历史和非空目标表；需要重新验收时只能使用新的 run-id 与隔离表，不能把正式表当作重放目标。

## 4. Flink SQL 落湖

新增独立 SQL 模板，包含严格的 clean 顶层 JSON source、late 嵌套 JSON source、共享 Hive Iceberg catalog 和目标表。模板中的 Topic、消费组、作业名及验收表名由 PowerShell 脚本以已校验参数渲染，不把一次验收后缀写死到仓库。

Kafka source 使用 `earliest-offset` 和 `read_committed`。JSON 缺字段、类型错误或解析错误必须使作业失败并暴露契约问题，不能再次静默清洗；数据合法性的唯一入口仍是 G2-B。两路通过 `UNION ALL` 形成一个规范化视图，再执行一个 `INSERT INTO`，避免两个独立 writer 同时提交同一 Iceberg 表。

Flink checkpoint 保持 10 秒、EXACTLY_ONCE，并复用现有 Hive Metastore、MinIO、S3A 与 Iceberg 1.6.1 运行依赖。该语义只覆盖此 SQL 作业已验证的 Kafka 到 Iceberg 边界，不扩大为回放器、G2-B 和湖仓之间的全局事务。

## 5. 查询与对账

Trino 查询必须至少验证：

1. 总行数等于 clean 与 late 路由行数之和，且两种路由之外没有第三种值。
2. 总行数等于 `COUNT(DISTINCT event_id)`；若不等则验收失败，不用 `DISTINCT` 掩盖重复。
3. 原始 `event_time`、`price`、来源文件、原始行号和事件 ID 与 Kafka 输入一致；派生时间、日期和十进制金额可逆且非 null。
4. clean 行的迟到诊断为空；late 行保存原输入坐标、Watermark 和非负 `lateness_ms`。
5. clean/late 落地 Topic 与 run-id 精确匹配，late 原始 Topic 也必须匹配同一次 G2-B 输入。
6. 验证器绑定 runner 返回的精确 Flink Job ID；最新 Iceberg Snapshot 必须晚于该作业启动时间。
7. Kafka 侧按 `event_id:quality_route` 排序计算的 SHA-256 必须与湖表聚合摘要一致，不能用相同数量的其他合法事件替代预期输入。
8. Iceberg 快照和 MinIO metadata 已生成，Trino 通过共享 Hive Metastore 读取同一张表，而不是查询复制结果。

动态验收使用真实跨月样本。基线可以读取 G2-B 已验证的正常输出；迟到分支必须通过调整两条不同真实事件的到达顺序触发，不编造商品、用户、金额或业务时间。测试注入与正式展示范围使用隔离 run-id、Topic、消费组和表名，并记录事件 ID。

## 6. 本机与运行边界

- 继续在 `codex/chapter-10-controlled-tools` 工作树实施，不纳入主工作区未提交修改。
- 复用现有 Flink SQL、Kafka、MinIO、Hive Metastore 和 Trino 容器；按阶段启动，16 GB 本机不同时常驻 Doris、API 和前端。
- Docker 程序和 WSL 数据位于 D 盘；临时 SQL、日志和依赖缓存也使用 D 盘。不得为了验收清空旧卷或重置正式表。
- 当前 Docker Desktop 的 Windows AF_UNIX 冷启动问题尚未证明彻底消失。运行中不主动停止 Docker Desktop；若环境故障阻塞，保留失败证据，不把代码测试写成动态通过。
- 本轮先以约 1,000 条和少量真实乱序事件验证正确性，不声称已经完成 2,199,938 条容量验收。百万级吞吐和资源测量留到 G5。

## 7. 交付与退出标准

1. SQL/脚本契约测试先失败再通过，覆盖 Topic/表名隔离、单 sink、DLQ 排除、17 字段、日分区和 Trino 对账语句。
2. 现有 Java 真实事件测试与相关 Python 数据测试保持通过；本轮不修改 G2-B 业务实现。
3. 隔离动态验收产生 clean 与 late 两种真实行，Trino 的计数、唯一 ID、字段和诊断规则全部通过。
4. 运行手册记录准确命令、输入范围、作业/表身份、计数、Iceberg 快照及未覆盖边界；临时真实数据和运行报告不提交 Git。
5. 更新 README 进度并推送当前开发分支。完成这些门禁后，下一阶段才进入 G2-D 真实指标定义、Doris 聚合与版本化 API。

## 8. 设计自检

- 职责清楚：DataStream 负责质量和事件时间路由，Flink SQL 负责稳定结构落湖，Iceberg 保存事实，Trino 负责历史查询。
- 真实性清楚：迟到测试只改变真实记录到达顺序，不修改业务内容；DLQ 不伪装成事实。
- 复杂度受控：一张新表、一个 SQL 作业、一个提交入口和一个对账入口，不引入新框架。
- 口径可演进：G2-D 可直接从完整有效事实计算指标，G4 的知识库与 RAG 再引用最终版本化指标定义。
- 当前状态：设计与实现均已完成；1,002 条真实事件动态验收通过，2,199,938 条全量容量仍留待 G5。
