# G2-B Java 真实事件质量入口

## 当前交付与边界

新增入口 `com.ecommerce.quality.real.RealDataQualityJob`，位于原 `jobs/datastream-quality` Maven 工程。旧 `DataQualityJob`、Fat JAR 默认入口、旧 SQL/Topic/表均未改变；没有启动新的常驻服务。

新入口严格校验 G1 的 14 字段和 G2-A 的 3 个回放字段，重算 Python 规则的事件 ID，保留 Kafka topic/partition/offset。解析失败、重复和迟到均有可追踪输出。状态去重先于迟到判断，时间使用 2019 年业务时间而非当前系统日期。

**真实 Kafka 事务交付、受控重发和持久化 Checkpoint 恢复均已完成隔离动态验收。** 本轮仍只验证 1,000 条基线及少量故障记录，不将结果写成湖仓落地、220 万条容量或永久去重已经完成。

## 构建

在 `.worktrees/chapter-10-controlled-tools` 执行。使用已有 Java 17 Maven 镜像，不切换本机其他版本的 Java；缓存和产物均位于 D 盘。动态验收前已将 Docker Desktop 程序、WSL 数据盘和交换盘迁至 D 盘，容器、镜像和卷数量核对无损。

```powershell
$env:TEMP = 'D:\EcommerceDev\temp'
$env:TMP = $env:TEMP
$env:PIP_CACHE_DIR = 'D:\EcommerceDev\cache\pip'
New-Item -ItemType Directory -Path D:\EcommerceDev\temp,D:\EcommerceDev\cache\maven -Force | Out-Null
docker run --pull never --rm --memory=1g --cpus=2 --volume "${PWD}:/workspace" --volume 'D:\EcommerceDev\cache\maven:/root/.m2' --workdir /workspace/jobs/datastream-quality maven:3.9.9-eclipse-temurin-17 mvn -B -q test package
```

产物为 `jobs/datastream-quality/target/datastream-quality-1.0.0.jar`。选择新作业必须显式传入 `-c com.ecommerce.quality.real.RealDataQualityJob`；不传入口类时仍是旧作业。

## 部署前检查

1. 核对 C 盘与 D 盘余量。动态验收前 C 盘约 4.97 GiB、D 盘约 46 GiB，满足本轮最小栈运行；Docker 数据位于 D 盘仍不等于 Docker Desktop 完全不使用系统盘。
2. 核对已有 Kafka/Flink/MinIO 容器名称、网络、S3 插件、凭据及 `flink-state` bucket。仅启动必要依赖；不得重建旧卷、重置旧链路或直接使用容器临时目录存放 Checkpoint。
3. 显式创建专属的输入、正常、拒绝、迟到 Topic。已有名称先调查，不删除重建。单 broker 验证还需核对 Kafka 事务状态日志的副本/ISR 配置，不能只靠普通 producer 发送成功推断事务可用。
4. 使用新 run-id、空输出 Topic 和新消费组做首次验收，检查 Flink 没有相同 run-id 的活动实例；消费组不是全局互斥锁，不能依靠它阻止重复部署。
5. 宿主 Kafka 地址本次为 `localhost:32600`。Flink 容器必须使用集群内可解析的 advertised listener，不得把宿主 localhost 地址直接传给容器内作业。

资源核验通过后，以下为提交参数形态；`$Flink`、`$InternalKafka`、`$Jar` 必须取自当前部署的只读检查结果，不猜测为默认容器或端口：

```powershell
$Run = 'verify-20260918'
docker exec $Flink flink run -d -c com.ecommerce.quality.real.RealDataQualityJob $Jar --bootstrap-servers $InternalKafka --run-id $Run --checkpoint-uri "s3a://flink-state/checkpoints/graduation-g2b/$Run" --input-topic "real_behavior_events_v1_$Run"
```

默认输出为 `real_behavior_clean_v1_<run-id>`、`real_behavior_dlq_v1_<run-id>`、`real_behavior_late_v1_<run-id>`；组名为 `graduation-g2b-<run-id>`。UID 和事务前缀也从 run-id 派生。run-id 只能含小写字母、数字、下划线、连字符，最长 32 字符，首字符须是字母或数字。

Checkpoint 路径必须与 run-id 完全匹配。输入只允许 `real_behavior_events_v1` 命名空间，三个输出分别只允许对应命名空间，不接受旧 `user_behavior_*`。未知参数直接拒绝，避免拼写错误被静默忽略。

启动前通过 Kafka Admin 查询四个 Topic 的存在性，消费者关闭自动建 Topic；本入口不会替用户创建 Topic。运行期间不得删改 Topic：启动检查不是持续锁，不能保证阻止 broker 在输出 Topic 被删后自动重建它的竞态。

## 对账和恢复

- 首轮仅发送 1,000 条已验证真实事件，使用 `read_committed` 独立消费者读取三个输出。基线应为正常 1,000、拒绝 0、迟到 0；按事件 ID 比较全部 17 字段，不依赖经过 keyBy 后的全局顺序。
- 记录输入 offset 范围和输出 ID 集合，再在标记清楚的故障测试范围重发指定事件。有效状态内正常输出不得增加，重复输出的坐标应指向新输入 offset。故障注入不当作新增真实业务记录。
- 从完成的兼容 Checkpoint 恢复时，保留相同 run-id、参数和目标资源。只重新启动消费组不等于恢复 Flink 去重状态；保存并核对恢复路径，不手工改 offset。
- 默认状态 TTL 为处理时间 24 小时，可用 `--state-ttl-hours` 设为 1～168 小时。重复读取不会刷新 TTL。过期、无状态启动或丢失恢复点后可能再输出，不提供永久幂等保证。
- 首版固定并行度 1、Watermark 10 秒、空闲检测 30 秒；Checkpoint 间隔 10 秒、超时 60 秒，保留取消时的外部状态。该配置用于小批量验证，不是百万级容量结论。
- 合法历史迟到记录保存在迟到 Topic，不自动回灌也不丢弃。后续入湖需明确纳入历史事实的规则，不能直接把正常流当作所有有效事实。
- 业务 `event_time` 保留在 JSON 并用于 Watermark；Kafka 输出记录不沿用该历史时间作为 `CreateTime`，由 Kafka 按当前写入时间赋值，避免历史回放消息立即触发保留清理。
- 超过 65,536 字节、非 UTF-8、重复 JSON 键、尾随 JSON、字段/版本/身份不符均拒绝。异常输出只保留最多 2,048 个码点的预览、字节数和 SHA，不复制无限原文。

## 2026-09-18 验证记录

最终 Java 17 新旧测试的 Surefire XML 报告共 30 项，失败、错误、跳过均为 0；同轮 JAR 已生成。JAR Manifest 默认入口仍为 `com.ecommerce.quality.DataQualityJob`，新入口类也已打入包内，未修改 POM 或旧 Java 源码。

Docker 迁移前的收尾复跑曾因 Linux 引擎管道不存在而在 Maven 启动前退出，该失败日志继续保留。Docker 恢复后重新执行完全相同的 Java 17 容器命令，测试与打包退出码为 0；不能用后一次成功覆盖前一次环境故障证据。

真实源文件 SHA 为 `18a3202d380c8f6c53ad767ec2043d0717df1d3604d2211639bb0fc4699a5437`。使用最终解析器读取该文件前 1,000 条，添加单独的离线回放元数据后，全部通过 Java 身份校验，14 个源字段保持一致。

事件 ID 序列 SHA 为 `6d0bf50f0f193632245329107cfc0b02d37a0121e7120e74b94c39513d21cdd7`，与 G2-A 的独立 Kafka 消费核对一致。报告为 `tmp/graduation/g2b-real-java-audit.json`；核对脚本为 `tmp/graduation/AuditRealEvents.java`，二者仅保存在本机，不上传原始数据。

Python 真实数据相关回归收尾复跑 63 项通过，耗时 0.686 秒，退出码 0；未修改 Python 业务实现，没有重复执行无关的全部 472 项。收尾日志为 `tmp/graduation/g2b-python-data-tests-final.log`，此前 0.758 秒通过记录仍保留在 `tmp/graduation/g2b-python-data-tests.log`。

首次 Java 拓扑断言发现 Flink 会为 Kafka 事务提交器自动生成 `Sink Committer: <sink UID>`。按实际 StreamGraph 精确核对全部 UID 和节点数后通过，未修改旧业务拓扑或放宽命名隔离规则。

独立契约复核未发现可复现的重要问题；随后补充消息字节上限、Unicode 码点上限与诊断截断边界。动态验收又发现并修复了一个仅靠离线测试无法暴露的问题：默认 Kafka 序列化器把 Flink 的 2019 年事件时间写成 Kafka `CreateTime`，在 7 天保留策略下使正常 Topic 的 earliest offset 从 0 推进到 524。先新增失败测试，确认旧实现返回时间戳 `1569898974000`；再让 sink 显式不设置 Kafka 时间戳，测试转绿，业务 JSON 和 Watermark 逻辑不变。失败运行 `g2b-20260918a` 只保留为排障证据，不计为通过。

最终使用全新隔离资源 `g2b-20260918b` 验收。基线输入 offset `[0,1000)`，独立 `read_committed` 消费得到正常 1,000、拒绝 0、迟到 0；全部 17 字段逐条一致，1,000 个事件 ID 唯一，序列 SHA 仍为 `6d0bf50f0f193632245329107cfc0b02d37a0121e7120e74b94c39513d21cdd7`。正常 Topic 首条 Kafka 时间戳为本轮写入时间 `1789699761217`，JSON 内 `event_time` 仍为 2019 年，earliest offset 保持 0。

首次重发真实首条事件后，正常流不增加，DLQ 记录 `DUPLICATE_EVENT` 并指向输入 offset 1000。随后取消作业 `b8eb920baae8569a9c81b693a39d683d`，从 MinIO 路径 `s3a://flink-state/checkpoints/graduation-g2b/g2b-20260918b/b8eb920baae8569a9c81b693a39d683d/chk-21` 恢复为作业 `0b9f3684395eaaaef7fff17da5497f7c`；REST 记录 `restored=1`。恢复后续传一条新真实事件并再次重发首条事件，最终输入 1,003、正常 1,001、重复 2、迟到 0，消费组 offset 为 1,003、lag 为 0；两个重复坐标分别为 1000 和 1002，证明恢复后没有漏处理且去重状态仍在。

本地证据为 `tmp/graduation/g2b-dynamic-audit-20260918b.json`、`g2b-dedup-audit-20260918b.json` 和 `g2b-dynamic-recovery-audit-20260918b.json`；真实数据、消息导出、Checkpoint 和临时报告均不提交 Git。验收后已取消作业，保留隔离 Topic 与外部 Checkpoint 供复查。下一步可以进入真实 clean/late 事件落湖设计，但本轮结果不代表 220 万条全量容量、跨 Kafka/湖仓全局事务或永久去重已经验证。
