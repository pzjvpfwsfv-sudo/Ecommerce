# G2-B 真实事件清洗实施计划

> 使用 executing-plans 逐项实施，并在交付前独立复核。用户已确认设计并要求继续，不再重复询问执行方式。

**目标：** 在旧 Maven 工程内新增真实事件质量入口，不改变旧作业。

**架构：** Kafka 字节和坐标信封经过严格契约解析，转换为包含完整 JSON 与路由字段的 Flink POJO；依次去重、按历史事件时间分流，输出独立正常、拒绝、迟到 Topic。

**技术：** Java 17、Flink 1.19.2、现有 Jackson/JUnit；不新增依赖。

**设计：** `docs/superpowers/specs/2026-09-18-graduation-real-event-quality-design.md`。

## 全局约束

- 继续在 `codex/chapter-10-controlled-tools` 工作，旧 `DataQualityJob` 和默认 JAR 入口不改。
- 全部新类位于 `jobs/datastream-quality/src/main/java/com/ecommerce/quality/real/`，对应测试位于同模块 `src/test/java/com/ecommerce/quality/real/`。
- 输入 17 字段；ID、业务时间、金额字符串、缺失值和来源必须与 Python 一致。使用独立 Topic、消费组、事务前缀、Checkpoint。
- 临时文件使用工作树外的 `D:\EcommerceDev\temp`，依赖缓存位于 D 盘；不清理或迁移旧卷。
- C 盘不足 1 GiB 时不扩大服务运行，代码测试与真实动态验收分开记录。

## 任务 1：契约和追踪信息

文件：`KafkaEnvelope.java`、`RealEvent.java`、`RealEventCodec.java`、`RealKafkaDeserializer.java`；测试 `RealEventCodecTest.java`、`RealKafkaDeserializerTest.java`。

接口：`KafkaEnvelope(String topic, int partition, long offset, byte[] payload)` 保留 Kafka 原始位置；`RealEventCodec.parse(KafkaEnvelope)` 返回 `RealEvent`，非法消息抛出携带固定原因码的 `InvalidEvent`。`RealEvent` 为无参 public 字段 POJO，保存完整 `json`、`datasetId,eventId,userId,eventTimeMillis` 与 Kafka 坐标，不以 Jackson 树对象作为 Flink 状态。

- [x] 先写测试，固定 Python 生成的正常、null、Unicode、最大金额和源行号样例；补额外字段、重复键、尾随 JSON、错误类型、非法日期/价格/版本、错误事件 ID、超长输入测试。
- [x] 运行测试，确认因新类缺失而失败；实现严格 UTF-8、字段集合、类型、规范化字符串与身份校验。
- [x] Python 身份算法按以下语义复现，使用小写 Unicode 转义且按业务字段键排序：

```python
identity = json.dumps([dataset_id, source_file, row_number, event],
                      sort_keys=True, separators=(",", ":"))
event_id = "real_" + sha256(identity.encode("utf-8")).hexdigest()
```

- [x] 正常输出 JSON 保留 17 个字段的值；异常信封包含坐标、固定原因码、最多 2,048 字符原文预览、是否截断、字节数和 SHA-256。验证 tombstone 和非法 UTF-8 也可追踪。

## 任务 2：解析、去重和历史时间分流

文件：`ParseRealEventFunction.java`、`DeduplicateRealEventFunction.java`、`RouteRealEventFunction.java`；测试 `RealEventFunctionsTest.java`。

接口：解析函数 `ProcessFunction<KafkaEnvelope, RealEvent>`，异常旁路为 JSON 字符串；去重函数 `KeyedProcessFunction<String, RealEvent, RealEvent>`，按数据集和事件 ID 分键，重复旁路为 JSON 字符串；迟到函数 `ProcessFunction<RealEvent, String>`，正常输出完整业务 JSON，迟到旁路为带事件和坐标的 JSON 信封。

- [x] 先写算子 harness 测试：解析失败、首次记录、同 ID 重发、TTL 到期、snapshot/restore 后去重，以及 2019 年正常与 Watermark 边界迟到。
- [x] 运行失败测试后实现；TTL 默认 24 小时，只在首次写入刷新，不根据重复访问延长；明确过期后可能再次输出。
- [x] 去重在迟到判断前；配置 Watermark 10 秒与空闲检测 30 秒，时间来自 `eventTimeMillis`，绝不来自系统日期。
- [x] 覆盖正常、非法、重复、迟到计数器。禁止吞掉状态存储或运行时故障；只有明确的输入校验错误进入拒绝分支。

## 任务 3：隔离配置和作业组装

文件：`RealJobConfig.java`、`RealDataQualityJob.java`；测试 `RealJobConfigTest.java`、`RealDataQualityJobTest.java`。

接口：`RealJobConfig.fromArgs(String[])`、`RealDataQualityJob.build(StreamExecutionEnvironment, RealJobConfig)` 与 `main(String[])`。

- [x] 先写配置测试：只接受真实命名空间；三个输出名称互异且不能等于输入；要求显式 bootstrap、独立 run-id、合法 Checkpoint；拒绝旧 Topic、旧状态路径和任意本机临时 Checkpoint。
- [x] Checkpoint 限定 `s3a://flink-state/checkpoints/graduation-g2b/<run-id>`；消费组、事务前缀与算子 UID 从同一已校验 run-id 派生，防止与旧作业混用。
- [x] 先写拓扑测试，验证单并行度、Checkpoint 10 秒、60 秒超时、最大并发 1、事务模式与三个 sink 的精确 UID/节点集合；代码复核取消时保留外部 Checkpoint 及有限重启配置。真实恢复效果仍属任务 4 的动态验收。
- [x] 组装 source -> parse -> keyBy/dedup -> watermark -> route -> 三个输出；消费者关闭自动建 Topic。部署前通过管理客户端核验 Topic 存在，不主动创建；运行期间输出 Topic 被删除的 broker 自动重建竞态不在此保证内。
- [x] 新入口通过 `flink run -c com.ecommerce.quality.real.RealDataQualityJob` 选择；不替换旧 Fat JAR 默认入口。

## 任务 4：证据、复核与备份

文件：`docs/graduation/real-event-quality-runbook.md`、README 进度、本文检查项。

- [x] 同一 Java 17 环境执行新旧 Maven 测试并打包。读取真实 JSONL 前 1,000 条，将回放元数据加入临时验证输入，以实际 Java parser 比对原始字段与事件 ID，标明离线验证。
- [x] 独立复核跨语言 ID、超长输入、时间边界、状态恢复和隔离配置；两轮限定范围复核未发现具体重要缺陷，契约复核后补充字节/码点/截断边界并通过已有 29 项测试。收尾只读复核没有新增代码改动，不代表动态验收通过。
- [ ] 资源条件满足后才启动必要依赖，用专属 Topic 完成 1,000 条 `read_committed` 对账、指定重复注入、Checkpoint 恢复；不满足则明确列为未完成，禁止把离线测试冒充动态验收。
- [x] 更新运行命令和实际结果，检查改动边界与 `git diff --check`；真实数据、构建产物和临时报告保持忽略，主工作区用户修改不纳入。

提交流程：完成复核后提交到当前开发分支，使用当前 Windows 代理推送，并以远端 SHA 对比确认备份。不合并 main；最终提交号及远端一致性由交付消息报告，避免在提交自身内容中预先宣称推送成功。

## 自检记录

契约 POJO 与 codec 属于任务 1，任务 2 只消费验证后的路由字段与完整 JSON；任务 3 使用相同算子接口，不重新实现契约。任务 4 的真实动态门禁不能被任务 1/2 的夹具测试替代。设计要求均有对应任务；仅文档、测试与实现增加，不修改旧链路。

2026-09-18 已有 Java 17 Surefire 报告汇总 29 项，失败、错误、跳过均为 0；JAR 包含新入口，Manifest 默认入口仍为旧作业。真实 1,000 条离线核验通过，Python 真实数据回归 63 项通过。最终复跑时 Docker 引擎已停止，命令在连接引擎时退出，未启动 Maven；不能把该复跑记为成功。此时 C 盘约 0.88 GiB、D 盘约 74.90 GiB，不额外启动服务、不自动删除文件。动态事务和恢复验收继续保留未完成状态。
