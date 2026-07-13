# Chapter 7 KRaft Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the current ZooKeeper-based Kafka setup with a KRaft `controller + broker` topology while keeping the existing producer, Flink, and validation entrypoints working.

**Architecture:** Remove `zookeeper` from Compose, split Kafka into an internal `kafka-controller` service and a broker service that preserves the existing `ecom-kafka` / `kafka:29092` / `localhost:9092` interface, then update scripts, tests, and docs around that new control plane. Runtime verification should prove both KRaft startup and end-to-end compatibility with the existing Kafka consumers and producers.

**Tech Stack:** Docker Compose, Kafka KRaft, PowerShell, Python unittest, Markdown

## Global Constraints

- Remove ZooKeeper completely from the Chapter 7 target state.
- Use a `1 controller + 1 broker` KRaft topology only; do not expand to multi-controller or multi-broker simulation.
- Keep the external broker entrypoint at `localhost:9092`.
- Keep the in-network broker entrypoint at `kafka:29092`.
- Keep the broker container compatible with existing references to `ecom-kafka`.
- Do not expose the controller port to the host.
- Preserve compatibility for the generator, Flink SQL source, and Kafka-dependent validation scripts.

---

## File Structure

- Modify: `infra/.env.example`
  Responsibility: replace ZooKeeper settings with KRaft controller and broker environment defaults.
- Modify: `infra/docker-compose.yml`
  Responsibility: remove ZooKeeper and define `kafka-controller` plus KRaft broker services.
- Modify: `infra/compose/kafka/README.md`
  Responsibility: explain the new KRaft topology and retained broker entrypoints.
- Modify: `README.md`
  Responsibility: update the project architecture narrative from ZooKeeper staging to KRaft evolution.
- Modify: `scripts/run_flink_sql_job.ps1`
  Responsibility: keep Kafka readiness checks valid after the KRaft migration.
- Modify: `scripts/run_chapter_5_iceberg_pipeline.ps1`
  Responsibility: stop referencing ZooKeeper containers and align service startup with KRaft.
- Modify: `scripts/verify_chapter_5_end_to_end.ps1`
  Responsibility: keep Kafka broker readiness and validation publishing working against KRaft.
- Modify: `tests/test_flink_sql_job.py`
  Responsibility: lock in the KRaft broker assumptions used by the Flink runner.
- Create: `tests/test_chapter_7_kraft_artifacts.py`
  Responsibility: lock in the new KRaft service topology, env vars, and docs.
- Modify: `docs/superpowers/specs/2026-07-07-chapter-1-compose-design.md`
  Responsibility: append that the planned KRaft evolution has now landed.
- Modify: `docs/superpowers/plans/2026-07-07-chapter-1-compose-implementation.md`
  Responsibility: append the actual Chapter 7 migration outcome.
- Create: `docs/superpowers/specs/2026-07-10-chapter-7-kraft-migration-design.md`
  Responsibility: already written spec; keep as implementation reference.
- Create: `docs/superpowers/plans/2026-07-10-chapter-7-kraft-migration-implementation.md`
  Responsibility: this plan.

### Task 1: Add failing artifact tests for the KRaft target state

**Files:**
- Create: `tests/test_chapter_7_kraft_artifacts.py`
- Modify: `tests/test_flink_sql_job.py`
- Test: `tests/test_chapter_7_kraft_artifacts.py`
- Test: `tests/test_flink_sql_job.py`

**Interfaces:**
- Consumes: `infra/.env.example`, `infra/docker-compose.yml`, `infra/compose/kafka/README.md`, `README.md`, `scripts/run_flink_sql_job.ps1`
- Produces: failing tests that require KRaft controller/broker services and forbid ZooKeeper references

- [ ] **Step 1: Write the failing Chapter 7 artifact test file**

```python
from pathlib import Path
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = REPO_ROOT / "infra" / ".env.example"
COMPOSE_FILE = REPO_ROOT / "infra" / "docker-compose.yml"
KAFKA_README = REPO_ROOT / "infra" / "compose" / "kafka" / "README.md"
TOP_LEVEL_README = REPO_ROOT / "README.md"


class Chapter7KRaftArtifactsTest(unittest.TestCase):
    def test_env_replaces_zookeeper_with_kraft_settings(self) -> None:
        text = ENV_FILE.read_text(encoding="utf-8")

        self.assertNotIn("ZOOKEEPER_CONTAINER_NAME", text)
        self.assertNotIn("ZOOKEEPER_PORT", text)
        self.assertIn("KAFKA_CONTROLLER_CONTAINER_NAME=", text)
        self.assertIn("KAFKA_CONTROLLER_PORT=", text)
        self.assertIn("KAFKA_BROKER_ID=", text)
        self.assertIn("KAFKA_CONTROLLER_NODE_ID=", text)
        self.assertIn("KAFKA_CLUSTER_ID=", text)

    def test_compose_defines_controller_and_broker_without_zookeeper(self) -> None:
        text = COMPOSE_FILE.read_text(encoding="utf-8")

        self.assertIn("kafka-controller:", text)
        self.assertIn("kafka-broker:", text)
        self.assertNotIn("zookeeper:", text)
        self.assertIn("KAFKA_PROCESS_ROLES: controller", text)
        self.assertIn("KAFKA_PROCESS_ROLES: broker", text)
        self.assertIn("container_name: ${KAFKA_CONTAINER_NAME}", text)
        self.assertIn("hostname: kafka", text)

    def test_kafka_docs_describe_kraft_evolution(self) -> None:
        kafka_text = KAFKA_README.read_text(encoding="utf-8")
        readme_text = TOP_LEVEL_README.read_text(encoding="utf-8")

        self.assertIn("KRaft", kafka_text)
        self.assertNotIn("ZooKeeper + Kafka", kafka_text)
        self.assertIn("controller + broker", kafka_text)
        self.assertIn("KRaft", readme_text)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Update the Flink runner artifact test to reject ZooKeeper assumptions**

```python
    def test_runner_script_prepares_connector_waits_for_kafka_and_flink_and_combines_sql(self):
        text = (REPO_ROOT / "scripts" / "run_flink_sql_job.ps1").read_text(encoding="utf-8")

        self.assertIn('$kafkaContainerName = "ecom-kafka"', text)
        self.assertIn("docker exec $kafkaContainerName kafka-topics --bootstrap-server kafka:29092 --list", text)
        self.assertNotIn("zookeeper", text.lower())
```

- [ ] **Step 3: Run the focused tests to verify they fail**

Run: `python -m unittest tests.test_chapter_7_kraft_artifacts tests.test_flink_sql_job -v`
Expected: FAIL with missing `kafka-controller`, present `zookeeper`, and missing KRaft env variables.

- [ ] **Step 4: Commit**

```bash
git add tests/test_chapter_7_kraft_artifacts.py tests/test_flink_sql_job.py
git commit -m "test: require chapter 7 kraft topology"
```

### Task 2: Migrate Compose and env defaults from ZooKeeper to KRaft

**Files:**
- Modify: `infra/.env.example`
- Modify: `infra/docker-compose.yml`
- Modify: `infra/compose/kafka/README.md`
- Test: `tests/test_chapter_7_kraft_artifacts.py`

**Interfaces:**
- Consumes: KRaft env defaults from `infra/.env.example`
- Produces: a `kafka-controller` service plus a broker service that still exposes `ecom-kafka`, `kafka:29092`, and `localhost:9092`

- [ ] **Step 1: Replace ZooKeeper env defaults with KRaft defaults**

```dotenv
KAFKA_CONTROLLER_CONTAINER_NAME=ecom-kafka-controller
KAFKA_CONTROLLER_PORT=9093

KAFKA_CONTAINER_NAME=ecom-kafka
KAFKA_PORT=9092
KAFKA_BROKER_ID=2
KAFKA_CONTROLLER_NODE_ID=1
KAFKA_CLUSTER_ID=4L6g3nShT-eMCtK--X86sw
KAFKA_TOPIC_USER_BEHAVIOR=user_behavior_events
KAFKA_TOPIC_ORDER_EVENTS=order_events
```

- [ ] **Step 2: Replace the ZooKeeper and single-broker Compose blocks with KRaft controller and broker services**

```yaml
  kafka-controller:
    profiles: ["core", "flink"]
    image: confluentinc/cp-kafka:7.6.1
    container_name: ${KAFKA_CONTROLLER_CONTAINER_NAME}
    hostname: kafka-controller
    environment:
      KAFKA_NODE_ID: ${KAFKA_CONTROLLER_NODE_ID}
      KAFKA_PROCESS_ROLES: controller
      KAFKA_LISTENERS: CONTROLLER://0.0.0.0:${KAFKA_CONTROLLER_PORT}
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: ${KAFKA_CONTROLLER_NODE_ID}@kafka-controller:${KAFKA_CONTROLLER_PORT}
      KAFKA_CLUSTER_ID: ${KAFKA_CLUSTER_ID}
      CLUSTER_ID: ${KAFKA_CLUSTER_ID}
    networks:
      - platform-net

  kafka-broker:
    profiles: ["core", "flink"]
    image: confluentinc/cp-kafka:7.6.1
    container_name: ${KAFKA_CONTAINER_NAME}
    hostname: kafka
    depends_on:
      - kafka-controller
    environment:
      KAFKA_NODE_ID: ${KAFKA_BROKER_ID}
      KAFKA_PROCESS_ROLES: broker
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:${KAFKA_PORT}
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: ${KAFKA_CONTROLLER_NODE_ID}@kafka-controller:${KAFKA_CONTROLLER_PORT}
      KAFKA_CLUSTER_ID: ${KAFKA_CLUSTER_ID}
      CLUSTER_ID: ${KAFKA_CLUSTER_ID}
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    ports:
      - "${KAFKA_PORT}:9092"
    networks:
      - platform-net
```

- [ ] **Step 3: Rewrite the Kafka README around KRaft**

```markdown
# Kafka Compose Notes

这一目录保存 Kafka 相关的 Compose 配置说明和演进记录。

当前阶段：

- 使用 `KRaft controller + broker` 作为消息基础设施
- controller 只走容器内控制面，不对宿主机暴露端口
- broker 继续保留 `localhost:9092` 和 `kafka:29092`

这次迁移的核心目标是：

- 去掉 ZooKeeper
- 保持数据生成器和 Flink 的 broker 接入方式不变
- 让项目形成一段真实的 `ZooKeeper -> KRaft` 架构演进故事
```

- [ ] **Step 4: Run the KRaft artifact test to verify it passes**

Run: `python -m unittest tests.test_chapter_7_kraft_artifacts -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add infra/.env.example infra/docker-compose.yml infra/compose/kafka/README.md
git commit -m "feat: migrate compose kafka stack to kraft"
```

### Task 3: Keep scripts and Flink-facing behavior compatible with the KRaft broker

**Files:**
- Modify: `scripts/run_flink_sql_job.ps1`
- Modify: `scripts/run_chapter_5_iceberg_pipeline.ps1`
- Modify: `scripts/verify_chapter_5_end_to_end.ps1`
- Modify: `tests/test_flink_sql_job.py`
- Test: `tests/test_flink_sql_job.py`

**Interfaces:**
- Consumes: broker container name `ecom-kafka`, broker endpoint `kafka:29092`
- Produces: Kafka readiness checks that still validate only the broker business entrypoint after the KRaft migration

- [ ] **Step 1: Update stale service references in the Chapter 5 pipeline runner**

```powershell
    $staleNames = @(
        "ecom-kafka-controller",
        "ecom-kafka",
        "ecom-minio",
        "ecom-minio-init",
        "ecom-hive-metastore",
        "ecom-trino"
    )
```

- [ ] **Step 2: Keep the Flink runner broker readiness probe and remove any ZooKeeper dependency wording**

```powershell
$kafkaContainerName = "ecom-kafka"

function Wait-ForKafkaReady {
    param([int]$TimeoutSeconds = 60)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            docker exec $kafkaContainerName kafka-topics --bootstrap-server kafka:29092 --list | Out-Null
            if ($LASTEXITCODE -eq 0) {
                return
            }
        } catch {
        }

        Start-Sleep -Seconds 2
    }

    throw "Kafka broker 未在预期时间内就绪。"
}
```

- [ ] **Step 3: Keep Chapter 5 validation publishing on the broker entrypoint**

```powershell
        docker exec ecom-kafka kafka-topics --bootstrap-server kafka:29092 --list | Out-Null
```

```powershell
    Get-Content $EventsFile | docker exec -i ecom-kafka kafka-console-producer --bootstrap-server kafka:29092 --topic user_behavior_events | Out-Null
```

- [ ] **Step 4: Run the Flink runner artifact test**

Run: `python -m unittest tests.test_flink_sql_job -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/run_flink_sql_job.ps1 scripts/run_chapter_5_iceberg_pipeline.ps1 scripts/verify_chapter_5_end_to_end.ps1 tests/test_flink_sql_job.py
git commit -m "refactor: keep kafka scripts compatible with kraft broker"
```

### Task 4: Record the KRaft evolution in project docs and chapter retrospectives

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-07-07-chapter-1-compose-design.md`
- Modify: `docs/superpowers/plans/2026-07-07-chapter-1-compose-implementation.md`
- Modify: `docs/superpowers/specs/2026-07-07-chapter-4-doris-fastapi-design.md`
- Modify: `docs/superpowers/plans/2026-07-07-chapter-4-doris-fastapi-implementation.md`
- Test: `tests/test_chapter_7_kraft_artifacts.py`

**Interfaces:**
- Consumes: the final KRaft topology and the real ZooKeeper pain points from earlier chapters
- Produces: documentation that turns the earlier “future KRaft migration” note into an implemented architecture evolution

- [ ] **Step 1: Update the top-level README architecture narrative**

```markdown
- 第 7 章：Kafka 从 ZooKeeper 演进到 KRaft `controller + broker` 双角色拓扑
```

```markdown
当前 Kafka 基础设施已经不再依赖 ZooKeeper，而是采用：

- 内部 `kafka-controller`
- 对外保持不变的 `kafka-broker`
```

- [ ] **Step 2: Append the landed KRaft outcome to the Chapter 1 compose docs**

```markdown
## 后续演进回写

第 1 章当时预留的 `ZooKeeper -> KRaft` 路线已经在第 7 章正式落地：

- ZooKeeper 已移除
- Kafka 已切到 KRaft
- 外部入口仍保持 `localhost:9092`
```

- [ ] **Step 3: Append the KRaft outcome to the Chapter 4 narrative**

```markdown
后续第 7 章已经把这里暴露出来的 ZooKeeper 残留状态问题，正式演进为 KRaft 双角色拓扑。这样第 4 章里出现的 `NodeExistsException` 不再只是排障记录，而是后续架构升级的真实动机。
```

- [ ] **Step 4: Run the Chapter 7 artifact tests again**

Run: `python -m unittest tests.test_chapter_7_kraft_artifacts -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add README.md docs/superpowers/specs/2026-07-07-chapter-1-compose-design.md docs/superpowers/plans/2026-07-07-chapter-1-compose-implementation.md docs/superpowers/specs/2026-07-07-chapter-4-doris-fastapi-design.md docs/superpowers/plans/2026-07-07-chapter-4-doris-fastapi-implementation.md
git commit -m "docs: record chapter 7 kraft evolution"
```

### Task 5: Run KRaft runtime verification and compatibility checks

**Files:**
- Modify: `infra/docker-compose.yml` only if runtime evidence requires it
- Modify: `scripts/run_flink_sql_job.ps1` only if runtime evidence requires it
- Modify: `scripts/verify_chapter_5_end_to_end.ps1` only if runtime evidence requires it
- Test: runtime commands only

**Interfaces:**
- Consumes: the new `kafka-controller` and `ecom-kafka` broker services
- Produces: evidence that KRaft boots successfully and preserves the Chapter 2/3/5 Kafka-facing workflows

- [ ] **Step 1: Start the core and flink profile with the new KRaft services**

Run: `docker compose --env-file infra/.env.example -f infra/docker-compose.yml --profile core --profile flink up -d kafka-controller kafka-broker`
Expected: both services reach `running`

- [ ] **Step 2: Verify KRaft broker readiness from the business entrypoint**

Run: `docker exec ecom-kafka kafka-topics --bootstrap-server kafka:29092 --list`
Expected: exits `0` and prints available topics or an empty list without connection failure

- [ ] **Step 3: Inspect controller and broker logs for KRaft startup evidence**

Run: `docker logs --tail 100 ecom-kafka-controller`
Expected: log lines referencing controller startup without ZooKeeper

Run: `docker logs --tail 100 ecom-kafka`
Expected: log lines showing broker startup in KRaft mode without `zookeeper.connect`

- [ ] **Step 4: Re-run an existing Kafka-dependent workflow**

Run: `python -m unittest tests.test_chapter_7_kraft_artifacts tests.test_flink_sql_job -v`
Expected: PASS

Run: `./scripts/verify_chapter_5_end_to_end.ps1`
Expected: PASS through Kafka event publishing and Iceberg commit verification

- [ ] **Step 5: Apply the minimal runtime fix if needed and re-run the failing command**

Allowed edit scope:

```text
- KRaft listeners and quorum settings in Compose
- broker/container naming compatibility
- PowerShell readiness timing or service names
```

Expected: the previously failing runtime verification now PASSes.

- [ ] **Step 6: Commit**

```bash
git add infra/docker-compose.yml scripts/run_flink_sql_job.ps1 scripts/verify_chapter_5_end_to_end.ps1
git commit -m "fix: validate chapter 7 kraft migration"
```

## Self-Review

- Spec coverage:
  - Remove ZooKeeper and split into controller + broker: Task 2
  - Preserve `localhost:9092`, `kafka:29092`, and `ecom-kafka`: Task 2 and Task 3
  - Keep generator/Flink/script compatibility: Task 3 and Task 5
  - Record migration motivation and outcome in docs: Task 4
  - Runtime proof and end-to-end compatibility: Task 5
- Placeholder scan:
  - No `TODO`, `TBD`, or “implement later” placeholders remain.
- Type consistency:
  - The plan consistently uses `kafka-controller`, `kafka-broker`, `ecom-kafka`, `kafka:29092`, and `localhost:9092`.

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-07-10-chapter-7-kraft-migration-implementation.md`. Two execution options:

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach?

---

## 实际执行回写

本计划后续已经按“先产物约束，再运行时验证”的顺序真实落地完成，下面补回最终实施记录。

## 1. 最终落地结果

第 7 章最终完成了这些关键结果：

- ZooKeeper 已从 Chapter 7 目标态 Compose 中移除。
- Kafka 已迁移为 `1 controller + 1 broker` 的 KRaft 双角色拓扑。
- broker 继续保持 `ecom-kafka`、`kafka:29092`、`localhost:9092` 兼容语义。
- 第 3 章 Flink runner 与第 5 章 Iceberg 验证脚本保持 broker-facing，不直接依赖 controller。
- README、第 1 章文档、第 4 章文档都已经回写到“现在时”的 KRaft 演进结果。

## 2. 真实运行时排障记录

这次迁移最有价值的部分，不只是“把 Compose 改对”，而是把运行时真正遇到的坑摸清楚了。

### 2.1 Docker Desktop 实际可用，但默认 Docker pipe 会卡

宿主机上最开始出现了一个很迷惑的现象：

- `Docker Desktop.exe` 和 `com.docker.backend.exe` 进程都在
- `wsl -l -v` 里 `docker-desktop` 也是 `Running`
- 但直接执行 `docker version` 会卡住

最终定位发现，不是 Docker engine 真没起来，而是当前默认 context 走的 `npipe:////./pipe/docker_engine` 这条连接路径不稳定；显式走 `desktop-linux` context 或把 `DOCKER_HOST` 指到 `npipe:////./pipe/dockerDesktopLinuxEngine` 后，CLI 就恢复正常。

这次经验很适合保留在项目里，因为它说明：

- 本地 Docker 故障不一定是“服务没开”
- 要区分 Desktop 进程、WSL backend、CLI context、named pipe 这几层

### 2.2 旧的 ZooKeeper 时代容器会先挡住 KRaft broker 重建

第一次拉起 KRaft broker 时，并不是配置先报错，而是直接撞上容器名冲突：

- 老的 `ecom-kafka`
- 老的 `ecom-zookeeper`

这说明从 ZooKeeper 形态迁移到 KRaft 形态时，本地环境里不仅要改 Compose，还要显式清理旧时代的基础设施残留。否则验证结果会被“旧容器占名”这种非目标问题污染。

### 2.3 broker 首次启动失败的真实根因是 listener 映射不完整

controller 很快就启动成功，并在日志里明确给出：

- `Running in KRaft mode`
- `QuorumController`
- `KafkaRaftServer`

但 broker 首次启动失败，报错是：

```text
Controller listener with name CONTROLLER defined in controller.listener.names not found in listener.security.protocol.map
```

这不是网络问题，也不是 quorum 地址写错，而是 KRaft broker 在预检时要求：

- `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` 中显式包含 `CONTROLLER`
- 且当前镜像实际还会消费对应的 `KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP`

最终修复是在 broker 侧补齐：

```yaml
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
```

同时 controller 侧也补齐了对应的 `KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT`，保证两边语义一致。

这是本章最核心的一次真实 KRaft 配置级排障。

## 3. 运行验证证据

### 3.1 KRaft controller 验证

`ecom-kafka-controller` 成功启动，并在日志中出现：

- `Running in KRaft mode`
- `Completed transition to Leader`
- `QuorumController id=1`
- `KafkaRaftServer nodeId=1`

这说明 controller 已经形成了有效的 KRaft 控制面。

### 3.2 KRaft broker 验证

修复 listener 映射后，`ecom-kafka` 成功启动，并在日志中出现：

- `process.roles = [broker]`
- `Awaiting socket connections on 0.0.0.0:9092`
- `Awaiting socket connections on 0.0.0.0:29092`
- `KafkaRaftServer nodeId=2`

这说明 broker 已经以 KRaft broker 身份稳定运行，并同时保留了宿主机和容器内两套既有入口。

### 3.3 broker 业务入口验证

执行：

```powershell
docker --context desktop-linux exec ecom-kafka kafka-topics --bootstrap-server kafka:29092 --list
```

命令可成功返回，说明业务侧关注的 broker 入口仍然可用。

### 3.4 产物测试验证

执行：

```powershell
python -m unittest tests.test_chapter_7_kraft_artifacts tests.test_flink_sql_job -v
```

结果：

- 10/10 通过

这说明 KRaft 拓扑、文档叙事、脚本契约和 Flink runner 假设都与最终实现一致。

### 3.5 Chapter 5 兼容性验证

执行：

```powershell
$env:DOCKER_HOST='npipe:////./pipe/dockerDesktopLinuxEngine'
./scripts/verify_chapter_5_end_to_end.ps1
```

验证过程里确认了这些事实：

- Chapter 5 需要的 Kafka、Flink、MinIO、Hive Metastore 能被重新拉起
- Flink SQL Iceberg 作业可以成功提交
- Kafka source 仍然使用 `kafka:29092`
- MinIO 中能观察到新的 Iceberg metadata/data 产物

中途有一条很典型的运行时信号：

```text
UNKNOWN_TOPIC_OR_PARTITION
```

它出现在作业刚提交、验证事件开始投递的瞬间。这恰好说明 broker 已经在响应请求，而 topic/consumer 侧处于重新建立状态的短暂窗口；后续 Iceberg 数据提交验证成功，说明这条链路最终恢复到了可用状态，而不是停留在“服务看起来启动了”。

## 4. 顺手收敛的脚本可靠性修复

为了让这次运行验证更可信，还顺手补了两处脚本质量问题：

- `scripts/run_flink_sql_job.ps1`
  - Compose 启动失败时立即报错
  - Flink SQL 提交失败时立即报错
- `scripts/run_chapter_5_iceberg_pipeline.ps1`
  - 在写 `tmp/chapter_5_flink_job.sql` 前显式创建 `tmp` 目录

同时 `tests/test_flink_sql_job.py` 也补了对应约束，避免这些问题以后回归。

## 5. 这章现在能怎么讲

如果从面试角度表达，第 7 章现在已经可以很自然地讲成一段完整故事：

“项目早期我先用 ZooKeeper 模式把 Kafka 拉起来，目的是尽快跑通 `生成器 -> Kafka -> Flink` 主链路。后面在 Doris、Iceberg、Hive Metastore、Trino 一步步接进来后，我真实遇到了 ZooKeeper 模式下 broker 状态残留、`NodeExistsException`、topic 恢复不稳定这些问题。到第 7 章，我没有继续只在脚本层做兜底，而是把 Kafka 正式迁移到 KRaft `controller + broker` 双角色拓扑。迁移时我保留了 `ecom-kafka`、`kafka:29092`、`localhost:9092` 这些既有入口，所以上层 Flink 和验证脚本基本不用改。运行验证里我又实际解决了 Docker Desktop context 卡住、旧容器占名、以及 broker 缺少 `CONTROLLER` listener 协议映射导致起不来的问题。最后 controller 和 broker 都给出了明确的 KRaft 启动日志，第 5 章端到端链路也重新验证通过，这样整段架构演进故事就完整了。” 
