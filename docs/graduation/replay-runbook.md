# G2-A 历史回放运行手册

## 当前交付

已实现固定速率、Ctrl+C 暂停、断点恢复、源文件及事件身份校验、专用 Kafka 发送适配器。复用 Python 3.12 和 `generators/requirements.txt` 中的 kafka-python 2.1.2，没有增加依赖。旧模拟生成器、Java Flink 作业和数据库表未改变。

开发目录为项目下的 `.worktrees/chapter-10-controlled-tools`，以下命令在该目录执行。原始数据与断点不在 GitHub 中；代码及本手册随开发分支备份。

## 离线试跑

默认模式为 `dry-run`，不建立 Kafka 连接。下面先处理 400 条，再用相同断点继续 600 条；文件已存在时会继续已有进度，不会自动重置。要重复独立验收，请换一个新的断点文件名，不覆盖已有任务。

```powershell
python -m generators.real_data.replay --input data/rees46/oct-nov-users-2pct-verified.jsonl --checkpoint tmp/graduation/my-dry-replay.json --max-events 400 --rate 100
python -m generators.real_data.replay --input data/rees46/oct-nov-users-2pct-verified.jsonl --checkpoint tmp/graduation/my-dry-replay.json --max-events 600 --rate 100
```

命令默认每次最多处理 1,000 条，`--max-events` 表示这次最多新增多少条，不是任务累计目标。`--rate` 是速率上限，实际值还受确认延迟和磁盘影响；程序不追赶暂停造成的积压。每次启动重新核验约 1 GiB 样本的 SHA，恢复时有界扫描已确认前缀，不额外建立索引。

`status=limit_reached` 表示本次限量结束，不是整个 220 万条样本完成；`complete` 才是读完声明的样本。`confirmed_records` 是任务累计确认位置；dry-run 的确认只代表本地通过，响应同时标记 `delivery=not_sent_to_kafka`。

## Kafka 发送

先确认 Docker 引擎及现有 Kafka 可用，并通过 `docker ps -a` 与 `docker port <容器名> 9092/tcp` 核对实际容器和宿主端口，不假定是 `ecom-kafka` 或 `9092`。本次现有环境的宿主端口为 `32600`，容器内管理端口仍为 `29092`。

只允许 `real_behavior_events_v1` 或带后缀的专用 Topic，旧 `user_behavior_*` 目标会被拒绝；发送器不会自动创建 Topic。下面使用新的 Topic 名和断点，创建命令仅用于本机单 broker 验证，不是生产副本配置：

```powershell
$KafkaContainer = 'chapter105-acceptance-c6ee18cd5bba-kafka'
$BootstrapServers = 'localhost:32600'
$Topic = 'real_behavior_events_v1_my_verify_01'
docker exec $KafkaContainer kafka-topics --bootstrap-server localhost:29092 --create --topic $Topic --partitions 1 --replication-factor 1 --config min.insync.replicas=1
python -m generators.real_data.replay --input data/rees46/oct-nov-users-2pct-verified.jsonl --checkpoint tmp/graduation/my-kafka-replay.json --mode kafka --bootstrap-servers $BootstrapServers --topic $Topic --max-events 400 --rate 100
python -m generators.real_data.replay --input data/rees46/oct-nov-users-2pct-verified.jsonl --checkpoint tmp/graduation/my-kafka-replay.json --mode kafka --bootstrap-servers $BootstrapServers --topic $Topic --max-events 600 --rate 100
```

已有同名 Topic 时先检查其中的用途和数据，不删除重建。独立消费者必须按本轮 `replay_id` 核对实际接收条数、原始业务字段、事件 ID 与 key，不能只看发送端进度。

消息保留 G1 全部字段，另外添加 `replay_schema_version=1`、`replay_id`、UTC `replayed_at`。历史 `event_time` 仍为 2019 年，不伪装成今天。Kafka key 为 `dataset_id:user_id`；分区数不变时同用户进入同分区，不承诺跨分区全局顺序。

## 恢复与失败

- Ctrl+C 会保存已确认前缀并返回退出码 130。恢复使用相同输入、断点、模式、连接地址和 Topic，可调整速率及本次限量。
- 每 100 条确认保存一次断点，可用 `--checkpoint-every` 调整；确认计数与最后事件 ID 一起更新，避免暂停打断两字段更新。
- 发送失败返回非零，不跳过未确认事件；已经进入 Kafka、但未保存断点的记录可能再次发送。下游须按稳定业务 `event_id` 去重，不能只按任务 ID 去重。
- `acks=all` 加 future 确认不等于端到端 exactly-once，也不保证单副本 Kafka 抗磁盘故障。已有发送确认与本地断点不是同一个事务。
- 断点与输入 SHA/大小/总行数、数据集、模式、地址和 Topic 绑定；错误断点不自动清空，dry-run 断点不能用于 kafka。
- `.lock` 由操作系统持有，退出自动释放；锁文件留下是正常现象，不要手工删除运行中的锁文件。不同断点文件是不同任务，不提供全局互斥。
- 不要在运行中编辑源文件、清单或断点。文件大小/修改时间变化会中止；本地清单不是签名认证，不能把外部伪造清单当可信来源。
- Kafka 日志清理、同名 Topic 重建或集群数据丢失后，不得直接沿用旧断点。此版不自动检测这些存储生命周期变化，须重新建任务并核对下游状态。

## 2026-09-17 实测证据

真实文件 SHA：`18a3202d380c8f6c53ad767ec2043d0717df1d3604d2211639bb0fc4699a5437`。CLI 离线试跑先 400、再 600 条，任务 ID 保持一致，累计确认 1,000 条。独立离线审计逐条比较输出消息与原始 JSONL，业务内容和 key 一致；事件 ID 序列（每行 ID 加 LF）的 SHA 为 `6d0bf50f0f193632245329107cfc0b02d37a0121e7120e74b94c39513d21cdd7`。

本地报告：`tmp/graduation/replay-dry-400.json`、`replay-dry-600.json`、`replay-offline-audit.json`。它们明确属于离线验证，不是 Kafka 接收证据。

### Docker 启动排障

首次 Kafka 尝试因 Docker 引擎未启动而连接失败，退出码 1，`replay-kafka-checkpoint.json` 的确认位置保持为 0，未记录成功发送。随后 Docker Desktop 启动失败，命令行查询无响应；后续探针改用有超时的子进程，避免无限等待。

本机 Docker Desktop 4.79.0 的日志先后指向 `%LOCALAPPDATA%\Docker\run\dockerInference` 与 `%LOCALAPPDATA%\docker-secrets-engine\engine.sock`，均为启动监听前移除 socket 失败。仅改名 `run` 不能处理另一个目录。检查时 `EnableDockerAI` 已为 false，未再修改配置；未将中文用户名或磁盘余量推断为已证实的根因。

19:07 确认 Docker Desktop 和 backend 已退出、两个目录分别只有一个 0 字节 socket 后，重命名为同级 `.quarantine-20260917-190730` 备份。逐个确认旧路径消失、备份存在且文件 ID 不变，再重新启动。`docker version`、`docker info` 均成功，引擎版本 29.5.3，原有 13 个容器和 11 个镜像可见。本次恢复未重置、重装或清理 Docker 数据；不保证其他机器或后续版本均适用。

### 真实 Kafka 发送与独立消费

只启动现有 `chapter105-acceptance-c6ee18cd5bba-kafka-controller` 和 `chapter105-acceptance-c6ee18cd5bba-kafka`，没有重建容器、修改既有集群配置或启动其他湖仓服务。只读检查确认原有 Topic 为 `user_behavior_events`，再显式创建 `real_behavior_events_v1_verify_20260917`，1 分区、1 副本、`min.insync.replicas=1`，并确认开始和结束 offset 均为 0。

实际连接地址为 `localhost:32600`，因此使用新断点 `tmp/graduation/replay-kafka-32600-checkpoint.json`，保留绑定旧地址且确认位置为 0 的失败断点，不手工篡改身份。

- 任务 ID：`c701c1a3-bf13-4098-ab67-421c43462989`；两次 CLI 分别新增 400 和 600 条，累计 1,000 条，任务 ID 保持一致。
- 独立消费者使用手工分配分区、无消费组、关闭自动提交和自动建 Topic，在两阶段分别核对 400/1,000 条；最终 offset 为 `[0, 1000)`，事件 ID 去重后仍为 1,000 个。
- 逐条比较源 JSONL 的全部 14 个字段、顺序与 UTF-8 key，全部一致。原始 `event_time` 保留 2019 年，2026 年发送时间仅写入 `replayed_at`。
- 最终事件 ID 序列 SHA 与前述离线审计一致：`6d0bf50f0f193632245329107cfc0b02d37a0121e7120e74b94c39513d21cdd7`。
- 本地证据：`tmp/graduation/replay-kafka-400.json`、`replay-kafka-600.json`、`replay-kafka-audit-400.json`、`replay-kafka-audit-1000.json`；独立核对脚本为同目录 `audit_kafka_replay.py`，不依赖回放器内部验证函数。

本次完成真实 Kafka 交付和正常退出后的断点续传验证，不代表故障强杀重试、端到端 exactly-once、220 万条全量回放或 Flink/湖仓落地已验收。Topic 和断点保留用于检查，未删除。后续重跑须另建任务，不能把已有 1,000 条的 Topic 当作空 Topic。

Python 全回归 472 项通过，其中新增回放测试 23 项，耗时 169.508 秒；网络用可控适配器测试，文件锁、原子断点、暂停恢复、源文件校验使用实际实现。Java 新事件适配、湖仓落地和 RAG 不在本次验收范围内。

Kafka 动态验收后再次运行 `python -m unittest discover -s tests -p 'test_real_data_replay*.py' -q`，23 项通过，耗时 0.607 秒。本次未改生产代码。

完整复验遇到两个环境约束：默认 C 盘临时目录只剩约 0.67 GiB，使月度下载测试触发 1 GiB 安全余量保护；将临时目录放进当前 `.worktrees` 后，又使一项旧迁移测试触发“主仓库不得是工作树”的路径保护。两项保护均未放宽；改为工作树外的 D 盘临时目录后，相关 5 项测试通过。测试夹具使用受控网络/命令替身，没有真的执行旧链路迁移或重置。

本机完整复验使用以下临时环境设置，仅作用于当前 PowerShell 进程及其子进程，不修改用户/系统全局设置：

```powershell
New-Item -ItemType Directory -Path D:\CodexTemp\ecommerce-tests-20260917 -Force | Out-Null
$env:TEMP = 'D:\CodexTemp\ecommerce-tests-20260917'
$env:TMP = $env:TEMP
$env:PYTHONUTF8 = '1'
python -m unittest discover -s tests -q
```

最终完整复验 472 项全部通过，耗时 169.690 秒；日志为 `tmp/graduation/replay-post-kafka-regression-final.log`。前两次受环境影响的日志也保留，分别为 `replay-post-kafka-regression.log` 和 `replay-post-kafka-regression-d-drive.log`，不覆盖失败证据。

C 盘空间不足仍是环境风险，测试切换临时目录不等于解决磁盘容量问题。扩大湖仓运行或升级 Docker 前应先释放空间；本次没有自动清理用户文件、镜像或数据卷。

独立代码复核未发现可复现的重要问题。另用实际子进程核验 Windows 文件锁：子进程持锁时另一进程被拒绝，强制结束该测试子进程后锁自动释放。该检查不等于实际 Kafka 故障重试、断电持久性或完整强杀回放已经验收。
