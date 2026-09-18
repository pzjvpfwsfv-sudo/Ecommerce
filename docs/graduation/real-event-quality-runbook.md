# G2-B Java 真实事件质量入口

## 当前交付与边界

新增入口 `com.ecommerce.quality.real.RealDataQualityJob`，位于原 `jobs/datastream-quality` Maven 工程。旧 `DataQualityJob`、Fat JAR 默认入口、旧 SQL/Topic/表均未改变；没有启动新的常驻服务。

新入口严格校验 G1 的 14 字段和 G2-A 的 3 个回放字段，重算 Python 规则的事件 ID，保留 Kafka topic/partition/offset。解析失败、重复和迟到均有可追踪输出。状态去重先于迟到判断，时间使用 2019 年业务时间而非当前系统日期。

**当前只完成代码、算子测试和真实样本离线契约核验。新作业的真实 Kafka 事务交付、受控重发和动态 Checkpoint 恢复尚未验收。** 不把 G2-A 已通过的回放验收算作 G2-B 已通过，也不将本次结果写成湖仓落地、全量处理或永久去重已经完成。

## 构建

在 `.worktrees/chapter-10-controlled-tools` 执行。使用已有 Java 17 Maven 镜像，不切换本机其他版本的 Java；缓存和产物均位于 D 盘。没有重装 Docker 或变更系统全局环境。

```powershell
$env:TEMP = 'D:\EcommerceDev\temp'
$env:TMP = $env:TEMP
$env:PIP_CACHE_DIR = 'D:\EcommerceDev\cache\pip'
New-Item -ItemType Directory -Path D:\EcommerceDev\temp,D:\EcommerceDev\cache\maven -Force | Out-Null
docker run --pull never --rm --memory=1g --cpus=2 --volume "${PWD}:/workspace" --volume 'D:\EcommerceDev\cache\maven:/root/.m2' --workdir /workspace/jobs/datastream-quality maven:3.9.9-eclipse-temurin-17 mvn -B -q test package
```

产物为 `jobs/datastream-quality/target/datastream-quality-1.0.0.jar`。选择新作业必须显式传入 `-c com.ecommerce.quality.real.RealDataQualityJob`；不传入口类时仍是旧作业。

## 部署前检查

1. 核对 C 盘与 D 盘余量。构建期间 C 盘约 0.69 GiB；收尾复查时约 0.88 GiB、D 盘约 74.90 GiB，Docker 引擎已停止，因此不启动更多常驻服务。Docker 数据位于 D 盘不等于 Docker Desktop 不需要系统盘余量。
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
- 超过 65,536 字节、非 UTF-8、重复 JSON 键、尾随 JSON、字段/版本/身份不符均拒绝。异常输出只保留最多 2,048 个码点的预览、字节数和 SHA，不复制无限原文。

## 2026-09-18 验证记录

Java 17 新旧测试的 Surefire XML 报告共 29 项，失败、错误、跳过均为 0，报告时间为北京时间 01:37；同轮 JAR 已生成。另行读取 JAR Manifest，默认入口仍为 `com.ecommerce.quality.DataQualityJob`，新入口类也已打入包内，未修改 POM 或旧 Java 源码。

收尾时再次执行相同 Maven 容器命令，发现 Docker Linux 引擎管道不存在，命令退出码为 1，尚未进入 Maven。这是最后一次重跑的实际结果，不能算作构建通过，也不能据此推断 Java 测试失败；保留此前 XML 报告与 JAR 为已有证据，并核对新增 Java 源码与测试的修改时间均早于该通过报告。`tmp/graduation/g2b-java-test-final.log` 当前记录该引擎连接失败，后续恢复环境后须重新验证。

真实源文件 SHA 为 `18a3202d380c8f6c53ad767ec2043d0717df1d3604d2211639bb0fc4699a5437`。使用最终解析器读取该文件前 1,000 条，添加单独的离线回放元数据后，全部通过 Java 身份校验，14 个源字段保持一致。

事件 ID 序列 SHA 为 `6d0bf50f0f193632245329107cfc0b02d37a0121e7120e74b94c39513d21cdd7`，与 G2-A 的独立 Kafka 消费核对一致。报告为 `tmp/graduation/g2b-real-java-audit.json`；核对脚本为 `tmp/graduation/AuditRealEvents.java`，二者仅保存在本机，不上传原始数据。

Python 真实数据相关回归收尾复跑 63 项通过，耗时 0.686 秒，退出码 0；未修改 Python 业务实现，没有重复执行无关的全部 472 项。收尾日志为 `tmp/graduation/g2b-python-data-tests-final.log`，此前 0.758 秒通过记录仍保留在 `tmp/graduation/g2b-python-data-tests.log`。

首次 Java 拓扑断言发现 Flink 会为 Kafka 事务提交器自动生成 `Sink Committer: <sink UID>`。按实际 StreamGraph 精确核对全部 UID 和节点数后通过，未修改旧业务拓扑或放宽命名隔离规则。

独立契约复核未发现可复现的重要问题；随后补充消息字节上限、Unicode 码点上限与诊断截断边界。收尾另对配置隔离、反序列化、三个算子、作业组装及对应测试进行只读复核，限定范围内未发现具体重要缺陷；该复核没有启动 Maven/Docker，也不替代运行验证。动态 Kafka/Flink 验收门禁仍未完成，后续先解决系统盘空间风险再执行，不提前进入湖仓接入。
