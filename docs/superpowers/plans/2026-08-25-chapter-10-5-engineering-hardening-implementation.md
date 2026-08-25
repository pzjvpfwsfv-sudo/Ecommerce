# 第 10.5 章工程可靠性加固实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把当前依赖历史运行现场的本地实时湖仓，升级为事件 ID 跨进程唯一、状态可持久化、固定 Iceberg 表可恢复、依赖可校验、可一键冷启动并能真实判断 readiness 的工程系统，同时为第 10 章工具调用增加统一端到端 deadline 和固定线程容量。

**Architecture:** 保留 `Generator -> Kafka -> Flink -> Doris / Iceberg -> Trino -> FastAPI` 主链路；用 PostgreSQL 持久化 Hive Metastore，Kafka/Doris/PostgreSQL 使用 Compose 命名卷，MinIO 使用稳定宿主机目录并新增 `flink-state` bucket。PowerShell Bootstrap 以“预检、依赖准备、基础设施、幂等初始化、catalog 恢复、Job 恢复、严格验收”分层执行；API readiness 复用三个只读 repository，工具分析在一个 ContextVar deadline 内完成，并由有界线程池隔离不响应取消的同步调用。

**Tech Stack:** Python 3.12、FastAPI 0.115、Pydantic v2、httpx 0.27、PowerShell 7/Windows PowerShell 5.1、Docker Compose、Kafka KRaft、Flink 1.19、Doris、MinIO、Iceberg、Hive Metastore、PostgreSQL 16、Trino、Maven、unittest。

**Spec:** `docs/superpowers/specs/2026-08-25-chapter-10-5-engineering-hardening-design.md`

## Global Constraints

- 仅支持 Windows PowerShell + Docker Desktop；本章不增加 Bash、CI 或 Kubernetes 路径。
- 默认 Bootstrap 必须幂等、非破坏，不得调用 `docker compose down -v`、`docker rm`、`docker volume prune` 或删除 MinIO 目录。
- Kafka 历史消息/offset、Doris 实时指标和旧 Flink state 的一次性放弃只能发生在显式迁移或 reset 脚本中，并要求确认开关；Flink state 只允许清理 `flink-state` bucket 下第 9 章的两个固定前缀。
- 迁移和 reset 永远不得删除 MinIO `warehouse` 湖数据；固定表恢复只允许 `lakehouse.analytics.user_behavior_detail`。
- Connector/JDBC 下载必须来自锁文件中的 HTTPS URL，校验 SHA-256 后原子替换；日志不得打印密码、完整连接串或模型原始响应。
- `/health` 只表示 API 进程存活；`/ready` 必须同时验证 Doris、Trino 固定表、唯一 RUNNING 的第 9 章生产 Job 和新鲜 checkpoint。
- 一个 `/analysis/tools` 请求的 planner、repository、executor、narrative 共用同一总 deadline，不允许每阶段重新获得完整预算。
- Python 无法安全终止已经进入阻塞系统调用的线程；超时后任务继续占用容量，直到真实返回。有界执行器必须拒绝过载，不能继续创建线程。
- API 默认只绑定 `127.0.0.1`；本章不声称已经提供公网认证边界。
- 每个任务先写失败测试，再实现最小变更，完成指定验证后只提交该任务列出的文件。
- 不修改或暂存主工作区中的 `.superpowers/sdd/task-1-report.md`，也不清理用户已有容器、卷或工作树。

---

## 文件结构

### 新建文件

- `infra/runtime-dependencies.lock.json`：Flink Connector 与 PostgreSQL JDBC 驱动的 URL、目标路径和 SHA-256 锁。
- `scripts/lib/Chapter105.Common.psm1`：安全读取 `.env`、原生命令检查、重试、稳定路径和原子报告公共函数。
- `scripts/install_runtime_dependencies.ps1`：锁文件驱动、带哈希校验和原子替换的统一下载入口。
- `scripts/restore_chapter_10_5_catalog.ps1`：固定 Iceberg 表 metadata 选择、校验和幂等注册。
- `scripts/bootstrap_chapter_10_5.ps1`：分层冷启动总入口。
- `scripts/migrate_chapter_10_5.ps1`：从 Derby/临时卷到新拓扑的一次性、显式确认迁移。
- `scripts/reset_chapter_10_5_realtime.ps1`：仅重建实时层命名卷的显式 reset。
- `scripts/verify_chapter_10_5_cold_start.ps1`：隔离 Compose project 的冷启动与重启恢复验收。
- `services/api/app/readiness_service.py`：Doris、Trino、Flink readiness 聚合。
- `services/api/app/tool_runner.py`：固定容量、超时后不释放占用槽位的同步工具执行器。
- `tests/test_chapter_10_5_artifacts.py`
- `tests/test_chapter_10_5_dependency_installer.py`
- `tests/test_chapter_10_5_catalog_recovery.py`
- `tests/test_chapter_10_5_bootstrap.py`
- `tests/test_chapter_10_5_cold_start_verifier.py`
- `tests/test_readiness_service.py`
- `tests/test_tool_runner.py`
- `docs/chapter-10-5-engineering-hardening-runbook.md`

### 修改文件

- `generators/user_behavior_generator.py`、`tests/test_generators.py`：UUID 事件 ID 与可注入工厂。
- `infra/docker-compose.yml`、`infra/.env.example`：PostgreSQL Metastore、命名卷、稳定 MinIO 路径、healthcheck、localhost API 和新配置。
- `infra/compose/flink/conf/core-site.xml`：独立 state bucket 与环境变量凭据。
- `infra/compose/trino/catalog/lakehouse.properties`：环境变量凭据和固定表注册过程。
- `jobs/datastream-quality/src/main/java/com/ecommerce/quality/config/JobConfig.java` 及其测试：MinIO checkpoint/savepoint 默认路径。
- `scripts/run_chapter_9_production_cutover.ps1`、`scripts/verify_chapter_9_recovery.ps1`：新状态路径和 Compose service 定位。
- `services/api/app/config.py`、`services/api/app/dependencies.py`、`services/api/app/main.py`：readiness、checkpoint 新鲜度、线程容量和生命周期注入。
- `services/api/app/flink_quality_repository.py`、`tests/test_flink_quality_repository.py`：checkpoint 新鲜度 fail-closed。
- `services/api/app/tool_executor.py`、`services/api/app/tool_analysis_service.py` 及相关测试：有界执行器和统一 deadline。
- `services/api/app/analyzers.py`、`services/api/app/tool_planners.py`、`services/api/app/tool_narratives.py` 及相关测试：模型 HTTP 调用消费剩余预算。
- `README.md`、`jobs/README.md`、第 10.5 章设计文档：入口、架构演进、运行手册和真实验收结果。

---

### Task 1: 将 Generator 事件 ID 改为跨进程唯一 UUID

**Files:**
- Modify: `generators/user_behavior_generator.py`
- Modify: `tests/test_generators.py`

**Interfaces:**
- `EventIdFactory = Callable[[], UUID]`
- `UserBehaviorGenerator(seed=None, event_id_factory=uuid4)`
- 输出格式固定为 `evt_<32 lowercase hex>`，业务随机字段仍由 `seed` 控制。

- [ ] **Step 1: 写 UUID 格式、可注入性和跨实例唯一性失败测试**

```python
from uuid import UUID


def test_event_id_uses_injectable_uuid_factory(self):
    fixed = UUID("12345678-1234-5678-1234-567812345678")
    event = UserBehaviorGenerator(event_id_factory=lambda: fixed).generate_event()
    self.assertEqual("evt_12345678123456781234567812345678", event["event_id"])


def test_default_event_ids_are_unique_across_generator_instances(self):
    first = UserBehaviorGenerator(seed=7).generate_event()["event_id"]
    second = UserBehaviorGenerator(seed=7).generate_event()["event_id"]
    self.assertRegex(first, r"^evt_[0-9a-f]{32}$")
    self.assertRegex(second, r"^evt_[0-9a-f]{32}$")
    self.assertNotEqual(first, second)
```

- [ ] **Step 2: 运行测试并确认旧序号实现红灯**

Run: `python -m unittest tests.test_generators -v`

Expected: FAIL，构造器不接受 `event_id_factory`，默认值仍为 `evt_000001`。

- [ ] **Step 3: 用注入的 UUID 工厂替换进程内 counter**

```python
from collections.abc import Callable
from uuid import UUID, uuid4

EventIdFactory = Callable[[], UUID]

@dataclass
class UserBehaviorGenerator:
    seed: int | None = None
    event_id_factory: EventIdFactory = uuid4
    randomizer: random.Random = field(init=False)

    def generate_event(self) -> dict[str, str]:
        event_id = self.event_id_factory()
        if not isinstance(event_id, UUID):
            raise TypeError("event_id_factory must return UUID")
        # Keep the existing business fields; only replace their event_id value.
        return {
            "event_id": f"evt_{event_id.hex}",
            "user_id": f"u_{self.randomizer.randint(1000, 9999)}",
            "product_id": f"p_{self.randomizer.randint(1000, 9999)}",
            "event_type": self.randomizer.choice(["view", "click", "cart"]),
            "event_time": datetime.now(UTC).isoformat(),
            "channel": self.randomizer.choice(["app", "web", "mini_program"]),
            "device_type": self.randomizer.choice(["ios", "android", "pc"]),
            "page_id": self.randomizer.choice(
                ["home", "search_result", "product_detail", "cart_page"]
            ),
        }
```

- [ ] **Step 4: 运行 Generator 回归测试**

Run: `python -m unittest tests.test_generators -v`

Expected: PASS，且原有 schema、单次发送和 seed 测试不回归。

- [ ] **Step 5: 提交事件 ID 修复**

```powershell
git add generators/user_behavior_generator.py tests/test_generators.py
git commit -m "fix: generate restart-safe event ids"
```

---

### Task 2: 建立统一运行依赖锁与原子下载器

**Files:**
- Create: `infra/runtime-dependencies.lock.json`
- Create: `scripts/lib/Chapter105.Common.psm1`
- Create: `scripts/install_runtime_dependencies.ps1`
- Create: `tests/test_chapter_10_5_dependency_installer.py`
- Modify: `tests/test_chapter_10_5_artifacts.py`

**Interfaces:**
- 锁文件每项固定包含 `name`、`url`、`destination`、`sha256`；禁止额外字段和重复目标路径。
- `Install-RuntimeDependencies -LockFile <path> -RepositoryRoot <path>`：已有正确文件跳过，错误文件重新下载，失败不覆盖旧文件。
- 锁定现有九个 Flink JAR 和 `postgresql-42.7.4.jar`，目标只能位于 `infra/compose/flink/lib` 或 `infra/compose/hive-metastore/lib`。

- [ ] **Step 1: 写锁文件结构和安全边界失败测试**

```python
def test_runtime_lock_has_unique_https_artifacts_with_real_hashes(self):
    lock = json.loads((ROOT / "infra/runtime-dependencies.lock.json").read_text("utf-8"))
    self.assertEqual(1, lock["version"])
    self.assertEqual(10, len(lock["artifacts"]))
    destinations = set()
    for artifact in lock["artifacts"]:
        self.assertEqual({"name", "url", "destination", "sha256"}, set(artifact))
        self.assertTrue(artifact["url"].startswith("https://"))
        self.assertRegex(artifact["sha256"], r"^[0-9a-f]{64}$")
        self.assertNotIn(artifact["destination"], destinations)
        destinations.add(artifact["destination"])
```

另用临时 HTTP fixture 测试：正确文件不下载、错误 hash 不替换旧文件、成功下载经 `.partial.<guid>` 原子移动、`../` 目标被拒绝。

- [ ] **Step 2: 运行测试并确认锁文件和安装器不存在**

Run: `python -m unittest tests.test_chapter_10_5_dependency_installer tests.test_chapter_10_5_artifacts -v`

Expected: FAIL，锁文件和安装脚本缺失。

- [ ] **Step 3: 从现有章节脚本归并坐标并固定真实 SHA-256**

对每个 Maven Central URL 读取同路径 `.sha256`，校验返回值为 64 位小写十六进制；再下载一次 artifact 并用 `Get-FileHash -Algorithm SHA256` 二次比对后写入锁文件。锁文件中不得使用 `latest`、版本范围、重定向短链或占位 hash。

- [ ] **Step 4: 实现 fail-closed 原子安装器**

关键流程固定为：解析 JSON -> 校验 schema/HTTPS/目标根目录 -> 检查现有 hash -> 下载到同目录临时文件 -> 校验临时 hash -> `Move-Item -Force` 替换。`finally` 只删除本次创建且仍位于允许目录内的临时文件；输出仅含 artifact name 和 `cached/installed` 状态。

- [ ] **Step 5: 运行依赖安装器单测和真实缓存校验**

Run: `python -m unittest tests.test_chapter_10_5_dependency_installer tests.test_chapter_10_5_artifacts -v`

Run: `powershell -NoProfile -ExecutionPolicy Bypass -File scripts/install_runtime_dependencies.ps1`

Expected: 全部 PASS；真实执行的 10 项均为 `cached` 或 `installed`，第二次执行全部为 `cached`。

- [ ] **Step 6: 提交依赖锁**

```powershell
git add infra/runtime-dependencies.lock.json scripts/lib/Chapter105.Common.psm1 scripts/install_runtime_dependencies.ps1 tests/test_chapter_10_5_dependency_installer.py tests/test_chapter_10_5_artifacts.py
git commit -m "build: lock runtime connector dependencies"
```

---

### Task 3: 将 Compose 状态迁移到显式持久化拓扑

**Files:**
- Modify: `infra/docker-compose.yml`
- Modify: `infra/.env.example`
- Modify: `infra/compose/flink/conf/core-site.xml`
- Modify: `infra/compose/trino/catalog/lakehouse.properties`
- Modify: `infra/compose/hive-metastore/README.md`
- Modify: `tests/test_chapter_10_5_artifacts.py`

**Interfaces:**
- 新服务 `metastore-postgres`，镜像 `postgres:16.4-alpine`，healthcheck 使用 `pg_isready`。
- 命名卷：`kafka-controller-data`、`kafka-broker-data`、`doris-fe-meta`、`doris-be-storage`、`metastore-postgres-data`。
- MinIO 挂载 `${MINIO_DATA_DIR}:/data`，`MINIO_DATA_DIR` 必须由 Bootstrap 解析为主仓库稳定绝对路径。
- API 端口为 `${API_BIND_HOST:-127.0.0.1}:${API_PORT:-8000}:8000`。

- [ ] **Step 1: 写 Compose 配置失败测试**

测试读取 Compose 文本并运行 `docker compose config --format json`，断言 PostgreSQL 健康依赖、Hive `DB_DRIVER=postgres`、五个命名卷、MinIO bind、`flink-state` bucket 初始化、API localhost bind、所有新增环境变量都存在；同时断言不存在 Derby 连接和 `tail -f /dev/null`。

- [ ] **Step 2: 运行静态测试并确认红灯**

Run: `python -m unittest tests.test_chapter_10_5_artifacts -v`

Expected: FAIL，Compose 仍为 Derby/临时容器目录，MinIO init 常驻。

- [ ] **Step 3: 增加 PostgreSQL Metastore 与持久卷**

Hive 使用 `SERVICE_OPTS` 指向 `jdbc:postgresql://metastore-postgres:5432/metastore`，挂载 Task 2 的 PostgreSQL JDBC JAR，并在 PostgreSQL 与 MinIO healthy 后启动。Kafka Controller/Broker、Doris FE/BE 分别挂载到镜像实际数据目录；卷只声明，不设置跨项目固定 `name`，以便隔离验收使用 Compose project 前缀。

- [ ] **Step 4: 收口 MinIO、Trino 与 API 配置**

`minio-init` 用 `mc mb --ignore-existing` 创建 `warehouse` 和 `flink-state` 后正常退出。Trino catalog 启用 `iceberg.register-table-procedure.enabled=true`，访问凭据从容器环境变量展开；API 绑定 localhost 并透传后续 readiness/deadline 配置。

- [ ] **Step 5: 验证全部 profile 可解析**

Run: `docker compose --env-file infra/.env.example -f infra/docker-compose.yml --profile flink --profile serving --profile lakehouse config --format json | Out-Null`

Expected: exit 0，无未解析变量、空 service 或循环依赖。

- [ ] **Step 6: 提交持久化拓扑**

```powershell
git add infra/docker-compose.yml infra/.env.example infra/compose/flink/conf/core-site.xml infra/compose/trino/catalog/lakehouse.properties infra/compose/hive-metastore/README.md tests/test_chapter_10_5_artifacts.py
git commit -m "feat: persist chapter 10.5 infrastructure state"
```

---

### Task 4: 将第 9 章 checkpoint/savepoint 迁移到 MinIO

**Files:**
- Modify: `jobs/datastream-quality/src/main/java/com/ecommerce/quality/config/JobConfig.java`
- Modify: `jobs/datastream-quality/src/test/java/com/ecommerce/quality/config/JobConfigTest.java`
- Modify: `scripts/run_chapter_9_production_cutover.ps1`
- Modify: `scripts/verify_chapter_9_recovery.ps1`
- Modify: `tests/test_chapter_9_phase_b_artifacts.py`
- Modify: `tests/test_chapter_10_5_artifacts.py`

**Interfaces:**
- 默认 checkpoint：`s3a://flink-state/checkpoints/chapter-9`
- 默认 savepoint：`s3a://flink-state/savepoints/chapter-9`
- 可用环境变量覆盖，但必须保持 `s3a://flink-state/` 前缀；本章不接受 `file://` 生产恢复路径。

- [ ] **Step 1: 写 Java 默认值和脚本约束失败测试**

在 `JobConfigTest` 断言两个默认 URI；在 Python artifact 测试断言 cutover/recovery 不再使用 `.state/chapter-9` 或工作树绝对路径。

- [ ] **Step 2: 运行测试并确认旧本地路径红灯**

Run: `mvn -f jobs/datastream-quality/pom.xml -Dtest=JobConfigTest test`

Run: `python -m unittest tests.test_chapter_9_phase_b_artifacts tests.test_chapter_10_5_artifacts -v`

Expected: 至少一项 FAIL，默认状态仍指向宿主机路径。

- [ ] **Step 3: 更新 Java 配置与 PowerShell 调用参数**

Java 只负责校验和传递状态 URI；脚本通过 Compose service `flink-jobmanager` 提交/查询，不再依赖固定工作树目录。保留显式 savepoint 恢复能力，但找不到 savepoint 时不得伪装为已恢复。

- [ ] **Step 4: 运行 Java 与 artifact 回归**

Run: `mvn -f jobs/datastream-quality/pom.xml test`

Run: `python -m unittest tests.test_chapter_9_artifacts tests.test_chapter_9_phase_b_artifacts tests.test_chapter_10_5_artifacts -v`

Expected: PASS。

- [ ] **Step 5: 提交远端状态路径**

```powershell
git add jobs/datastream-quality/src/main/java/com/ecommerce/quality/config/JobConfig.java jobs/datastream-quality/src/test/java/com/ecommerce/quality/config/JobConfigTest.java scripts/run_chapter_9_production_cutover.ps1 scripts/verify_chapter_9_recovery.ps1 tests/test_chapter_9_phase_b_artifacts.py tests/test_chapter_10_5_artifacts.py
git commit -m "feat: store flink recovery state in minio"
```

---

### Task 5: 实现固定 Iceberg 表的 fail-closed catalog 恢复

**Files:**
- Create: `scripts/restore_chapter_10_5_catalog.ps1`
- Create: `tests/test_chapter_10_5_catalog_recovery.py`
- Modify: `tests/test_chapter_10_5_artifacts.py`

**Interfaces:**
- `Select-IcebergMetadataCandidate([string[]]$Names)`：仅接受 `00000-<uuid>.metadata.json`，选择唯一最高版本；同版本多文件直接失败。
- `Assert-IcebergMetadata([object]$Metadata)`：校验 `location`、`table-uuid`、`current-snapshot-id`，location 必须等于固定表根路径。
- `Restore-Chapter105Catalog`：表已存在且可查询时 no-op；表缺失时仅注册固定表；其他错误失败退出。
- 脚本支持 `-FunctionsOnly` 供无 Docker 单测导入。

- [ ] **Step 1: 写 metadata 选择和安全边界失败测试**

覆盖：无 metadata、非法文件名、最高版本唯一、最高版本冲突、location 指向其他 bucket/table、表已存在 no-op、恢复 SQL 中 identifiers 全为常量、错误消息不包含 metadata 正文或凭据。

- [ ] **Step 2: 运行测试并确认恢复脚本不存在**

Run: `python -m unittest tests.test_chapter_10_5_catalog_recovery tests.test_chapter_10_5_artifacts -v`

Expected: FAIL。

- [ ] **Step 3: 实现三段式固定恢复**

1. 先执行 `SELECT 1 FROM lakehouse.analytics.user_behavior_detail LIMIT 1`；成功则返回 `already_registered`。
2. 仅在 Trino 明确报告表不存在时，通过一次性 `minio/mc` Compose run 列出固定 metadata 目录，选择并读取唯一最高版本 JSON。
3. 校验后执行下列固定过程调用，再查询 `count(*)` 和 `max(event_time)` 验证；不得接受 catalog/schema/table/location 参数。`$metadataFileName` 只能来自前一步通过严格文件名正则的 basename。

```sql
CALL lakehouse.system.register_table(
    schema_name => 'analytics',
    table_name => 'user_behavior_detail',
    table_location => 's3a://warehouse/iceberg/analytics.db/user_behavior_detail',
    metadata_file_name => '$metadataFileName'
)
```

- [ ] **Step 4: 运行恢复单测**

Run: `python -m unittest tests.test_chapter_10_5_catalog_recovery tests.test_chapter_10_5_artifacts -v`

Expected: PASS。

- [ ] **Step 5: 提交 catalog 恢复器**

```powershell
git add scripts/restore_chapter_10_5_catalog.ps1 tests/test_chapter_10_5_catalog_recovery.py tests/test_chapter_10_5_artifacts.py
git commit -m "feat: restore fixed iceberg catalog safely"
```

---

### Task 6: 增加 checkpoint 新鲜度与真实 `/ready`

**Files:**
- Create: `services/api/app/readiness_service.py`
- Create: `tests/test_readiness_service.py`
- Modify: `services/api/app/flink_quality_repository.py`
- Modify: `services/api/app/config.py`
- Modify: `services/api/app/dependencies.py`
- Modify: `services/api/app/main.py`
- Modify: `tests/test_flink_quality_repository.py`
- Modify: `tests/test_api_service.py`
- Modify: `infra/.env.example`
- Modify: `infra/docker-compose.yml`

**Interfaces:**
- `FLINK_CHECKPOINT_MAX_AGE_SECONDS` 默认 `120`，范围 `1..3600`。
- `ReadinessService.check()` 成功返回 `{"status":"ready","dependencies":{"doris":"ready","trino":"ready","flink":"ready"}}`。
- `/ready` 任一依赖失败统一返回 503 和 `{"detail":"service is not ready"}`；不得泄露 SQL、URL、响应正文或异常消息。

- [ ] **Step 1: 写 checkpoint 新鲜度失败测试**

向 `FlinkQualityRepository` 注入 UTC `clock`，覆盖：无 completed checkpoint、latest 缺失、checkpoint 超过 120 秒、未来时间、恰好在边界内。只有唯一 RUNNING Job 且 checkpoint 新鲜时返回 evidence。

- [ ] **Step 2: 写 readiness 聚合和路由失败测试**

用三个 mock repository 断言调用 Doris `fetch_all_metrics()`、Trino `fetch_summary()`、Flink `fetch_health()`；每个依赖分别失败都得到同一 503 安全响应；`/health` 在所有 mock 抛错时仍返回 200 且不调用依赖。

- [ ] **Step 3: 运行目标测试并确认红灯**

Run: `python -m unittest tests.test_flink_quality_repository tests.test_readiness_service tests.test_api_service -v`

Expected: FAIL，缺少新鲜度配置、service 和 `/ready`。

- [ ] **Step 4: 实现 fail-closed 新鲜度检查**

`fetch_health()` 在组装 evidence 前要求 `completed >= 1`、`latest_completed_at is not None`，并计算 `age = clock() - latest_completed_at`；`age < 0` 或 `age.total_seconds() > max_age` 均抛固定 `ValueError`。

- [ ] **Step 5: 注入 ReadinessService 与路由**

`build_readiness_service()` 构造固定 Doris/Trino/Flink repository；`create_app(repository=None, analysis_service=None, tool_analysis_service=None, readiness_service=None, settings=None)` 支持测试注入。路由只返回固定状态字典，捕获任意依赖异常后记录安全 `error_type` 并返回固定 503。

- [ ] **Step 6: 运行 API 回归测试**

Run: `python -m unittest tests.test_flink_quality_repository tests.test_readiness_service tests.test_api_service tests.test_analysis_api tests.test_tool_analysis_api -v`

Expected: PASS。

- [ ] **Step 7: 提交 readiness**

```powershell
git add services/api/app/readiness_service.py services/api/app/flink_quality_repository.py services/api/app/config.py services/api/app/dependencies.py services/api/app/main.py tests/test_readiness_service.py tests/test_flink_quality_repository.py tests/test_api_service.py infra/.env.example infra/docker-compose.yml
git commit -m "feat: add dependency-aware api readiness"
```

---

### Task 7: 用固定容量执行器替换一次一线程

**Files:**
- Create: `services/api/app/tool_runner.py`
- Create: `tests/test_tool_runner.py`
- Modify: `services/api/app/tool_executor.py`
- Modify: `services/api/app/config.py`
- Modify: `services/api/app/dependencies.py`
- Modify: `services/api/app/tool_analysis_service.py`
- Modify: `tests/test_tool_executor.py`
- Modify: `tests/test_tool_analysis_service.py`
- Modify: `infra/.env.example`
- Modify: `infra/docker-compose.yml`

**Interfaces:**
- `AI_TOOL_EXECUTOR_MAX_WORKERS` 默认 `3`，范围 `1..3`。
- `BoundedToolRunner.run(operation, timeout_seconds)`；满载或超时抛 `ToolRunnerUnavailableError`。
- timeout 后 semaphore slot 由 Future done callback 保持占用，只有阻塞调用真实结束才释放。
- `close()` 拒绝新任务并 `shutdown(wait=False, cancel_futures=True)`；不能声称终止正在运行的线程。

- [ ] **Step 1: 写有界容量并发失败测试**

使用 `threading.Event` 阻塞两个 worker：前两次调用超时后仍占容量，第三次立即以 saturated 失败且 active thread 数不增长；释放 Event 后 slot 可再次使用。另测 `close()` 后拒绝任务、异常统一且不包含底层消息。

- [ ] **Step 2: 运行测试并确认旧执行器无限建线程**

Run: `python -m unittest tests.test_tool_runner tests.test_tool_executor -v`

Expected: FAIL，`tool_runner` 不存在，旧 `_run_with_timeout` 每次创建 daemon Thread。

- [ ] **Step 3: 实现 BoundedToolRunner**

```python
class BoundedToolRunner:
    def __init__(self, max_workers: int) -> None:
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="tool-worker")
        self._capacity = BoundedSemaphore(max_workers)
        self._closed = False

    def run(self, operation, timeout_seconds):
        if not self._capacity.acquire(blocking=False):
            raise ToolRunnerUnavailableError("tool runner unavailable")
        future = self._executor.submit(operation)
        future.add_done_callback(lambda _: self._capacity.release())
        try:
            return future.result(timeout=timeout_seconds)
        except FutureTimeoutError:
            future.cancel()
            raise ToolRunnerUnavailableError("tool runner unavailable") from None
```

实现时用锁保护 `_closed` 与 submit 的竞态；若 submit 失败且未创建 Future，当前线程负责释放 slot，不能与 callback 双重释放。

- [ ] **Step 4: ToolExecutor 注入 runner 并删除 Thread/Queue**

`ToolExecutor` 只向 runner 提交固定 registry operation；将 runner 的 saturated/timeout 映射为现有 `ToolExecutionUnavailableError` 和安全审计类型。新增 `close()` 委托 runner，`ToolAnalysisService.close()` 再委托 executor；FastAPI shutdown hook 只调用一次 close。

- [ ] **Step 5: 运行执行器与 API 回归**

Run: `python -m unittest tests.test_tool_runner tests.test_tool_executor tests.test_tool_analysis_service tests.test_tool_analysis_api -v`

Expected: PASS，且测试结束无持续创建的 `tool-execution` 线程。

- [ ] **Step 6: 提交有界执行器**

```powershell
git add services/api/app/tool_runner.py services/api/app/tool_executor.py services/api/app/config.py services/api/app/dependencies.py services/api/app/tool_analysis_service.py tests/test_tool_runner.py tests/test_tool_executor.py tests/test_tool_analysis_service.py infra/.env.example infra/docker-compose.yml
git commit -m "fix: bound blocked tool execution threads"
```

---

### Task 8: 把 deadline 扩展为完整工具分析请求预算

**Files:**
- Modify: `services/api/app/tool_analysis_service.py`
- Modify: `services/api/app/tool_executor.py`
- Modify: `services/api/app/analyzers.py`
- Modify: `services/api/app/tool_planners.py`
- Modify: `services/api/app/tool_narratives.py`
- Modify: `services/api/app/dependencies.py`
- Modify: `tests/test_tool_analysis_service.py`
- Modify: `tests/test_tool_executor.py`
- Modify: `tests/test_openai_compatible_analyzer.py`
- Modify: `tests/test_tool_planners.py`
- Modify: `tests/test_tool_narratives.py`

**Interfaces:**
- `ToolAnalysisService` 构造器新增 `total_timeout_seconds` 和 `timer=perf_counter`；在生成 audit ID 后、调用 planner 前建立唯一 `use_tool_deadline`。
- 所有 HTTP 调用在开始 transport 前调用 `remaining_timeout(component_cap)`。
- deadline 耗尽统一抛 `ToolAnalysisUnavailableError("tool analysis is unavailable")`，不进入 fallback 重新获取预算。

- [ ] **Step 1: 写跨阶段预算失败测试**

用可控 monotonic timer 证明 planner 消耗 18 秒后，20 秒总预算下 executor 只能拿到约 2 秒；planner 已耗尽预算时 repository/analyzer 均不调用；primary planner 失败转 fallback 时仍共享原 deadline；narrative 模型 timeout 使用剩余值而非完整 `AI_REQUEST_TIMEOUT_SECONDS`。

- [ ] **Step 2: 运行 deadline 目标测试并确认红灯**

Run: `python -m unittest tests.test_tool_analysis_service tests.test_tool_executor tests.test_openai_compatible_analyzer tests.test_tool_planners tests.test_tool_narratives -v`

Expected: FAIL，当前 deadline 只包裹单个 executor operation，planner/narrative 不消费总预算。

- [ ] **Step 3: 在服务边界建立唯一 deadline**

将现有 `analyze()` 主流程抽为 `_analyze_in_budget()`；外层只建立一次 context。`ToolExecutor` 删除自己的 `use_tool_deadline`，每个 runner 调用使用 `remaining_timeout(self._per_operation_cap_seconds)`；repository 已有的 `remaining_timeout` 继续收窄 transport timeout。

- [ ] **Step 4: 让三个模型适配器消费剩余预算**

`OpenAICompatibleAnalyzer`、`OpenAICompatibleToolPlanner`、`OpenAICompatibleToolNarrativeAnalyzer` 在每个 `client.post` 调用前计算 `remaining_timeout(self._timeout_seconds)`；deadline 异常不得被包装成带底层内容的新消息。

- [ ] **Step 5: 运行第 8/10 章完整 API 测试**

Run: `python -m unittest discover -s tests -p "test_*analysis*.py" -v`

Run: `python -m unittest tests.test_tool_executor tests.test_tool_planners tests.test_tool_narratives -v`

Expected: PASS，旧 `/analysis/realtime` 语义不变，新接口总耗时测试稳定。

- [ ] **Step 6: 提交统一 deadline**

```powershell
git add services/api/app/tool_analysis_service.py services/api/app/tool_executor.py services/api/app/analyzers.py services/api/app/tool_planners.py services/api/app/tool_narratives.py services/api/app/dependencies.py tests/test_tool_analysis_service.py tests/test_tool_executor.py tests/test_openai_compatible_analyzer.py tests/test_tool_planners.py tests/test_tool_narratives.py
git commit -m "fix: enforce end-to-end tool deadlines"
```

---

### Task 9: 实现分层、幂等、非破坏 Bootstrap

**Files:**
- Create: `scripts/bootstrap_chapter_10_5.ps1`
- Create: `tests/test_chapter_10_5_bootstrap.py`
- Modify: `scripts/lib/Chapter105.Common.psm1`
- Modify: `tests/test_chapter_10_5_artifacts.py`

**Interfaces:**
- `bootstrap_chapter_10_5.ps1 [-EnvFile infra/.env] [-SkipBuild] [-ReportPath tmp/chapter-10-5/bootstrap-report.json]`
- 阶段固定为 `preflight`、`dependencies`、`infrastructure`、`initialization`、`catalog`、`jobs`、`acceptance`。
- 每阶段写 `started_at`、`completed_at`、`status`、`details`；报告用临时文件原子替换，失败也必须落盘。

- [ ] **Step 1: 写 FunctionsOnly 与安全词法失败测试**

测试模块函数：`.env` 解析拒绝重复键/空键、稳定 MinIO 路径总是解析到主仓库而非 `.worktrees`、native command 非零退出会抛错、重试达到上限退出、报告写入不暴露 `PASSWORD/SECRET/API_KEY`。脚本文本断言不含 `down -v`、`docker rm`、`volume prune`、`Remove-Item -Recurse`。

- [ ] **Step 2: 运行测试并确认 Bootstrap 不存在**

Run: `python -m unittest tests.test_chapter_10_5_bootstrap tests.test_chapter_10_5_artifacts -v`

Expected: FAIL。

- [ ] **Step 3: 实现 preflight 与 dependencies**

预检明确验证 Docker daemon、`docker compose`、PowerShell、Java 17、Maven、Python、端口变量、env 必需键和磁盘目录可写。依赖阶段只调用 Task 2 安装器；任何失败立即停止，不启动半套基础设施。

- [ ] **Step 4: 实现 infrastructure、initialization 与 catalog**

只使用 `docker compose up -d` 启动所需 profile，并轮询 Compose health/一次性 init exit 0。Topic、Doris DDL、MinIO bucket 均使用幂等命令；catalog 阶段只调用 Task 5 固定恢复器。

- [ ] **Step 5: 实现唯一 Job 恢复与 acceptance**

先查询 Flink `/jobs/overview`：恰好一个目标 RUNNING Job 时 no-op；零个时构建并提交；多个同名或其他模糊状态直接失败。验收依次检查 `/ready`、Doris 指标、Trino 固定表、唯一 Job、新鲜 checkpoint 和 `/analysis/tools` 规则模式；不得通过日志关键字猜测成功。

- [ ] **Step 6: 运行 Bootstrap 单测**

Run: `python -m unittest tests.test_chapter_10_5_bootstrap tests.test_chapter_10_5_artifacts -v`

Expected: PASS。

- [ ] **Step 7: 提交冷启动入口**

```powershell
git add scripts/bootstrap_chapter_10_5.ps1 scripts/lib/Chapter105.Common.psm1 tests/test_chapter_10_5_bootstrap.py tests/test_chapter_10_5_artifacts.py
git commit -m "feat: add idempotent chapter 10.5 bootstrap"
```

---

### Task 10: 增加显式迁移与实时层 reset 安全护栏

**Files:**
- Create: `scripts/migrate_chapter_10_5.ps1`
- Create: `scripts/reset_chapter_10_5_realtime.ps1`
- Modify: `tests/test_chapter_10_5_bootstrap.py`
- Modify: `tests/test_chapter_10_5_artifacts.py`

**Interfaces:**
- `migrate_chapter_10_5.ps1 -TrafficPaused -ConfirmRealtimeReset`，两个 switch 缺一均在任何 Docker 写操作前失败。
- `reset_chapter_10_5_realtime.ps1 -ConfirmReset` 只允许删除当前 Compose project 的五个可重建命名卷，以及 `flink-state/checkpoints/chapter-9`、`flink-state/savepoints/chapter-9` 两个固定对象前缀。
- 两个脚本都先输出计划和保护对象，报告中记录 MinIO warehouse 前后 object count/size 与固定表快照 ID。

- [ ] **Step 1: 写 destructive allowlist 失败测试**

断言卷删除目标集合严格等于 Kafka Controller/Broker、Doris FE/BE、PostgreSQL Metastore 五个卷；对象删除目标集合严格等于 `flink-state` bucket 的 checkpoint/savepoint 两个第 9 章前缀。`warehouse` bucket、MinIO host path、其他 `flink-state` 前缀及任何未知卷都不在集合。无确认参数时 mock Docker 调用次数必须为 0。

- [ ] **Step 2: 运行测试并确认脚本缺失**

Run: `python -m unittest tests.test_chapter_10_5_bootstrap tests.test_chapter_10_5_artifacts -v`

Expected: FAIL。

- [ ] **Step 3: 实现一次性迁移流程**

迁移顺序固定为：确认流量已停 -> 记录湖数据证据 -> 停止旧实时服务 -> 按 allowlist 删除/重建实时卷 -> 启动 PostgreSQL Metastore -> 恢复固定 catalog -> 启动第 9 章 Job -> 运行严格验收 -> 再记录湖数据证据。任何失败保留现场并打印安全的下一条诊断命令，不自动回滚或删除数据。

- [ ] **Step 4: 实现单独 reset 流程**

reset 不做 catalog 恢复之外的“智能迁移”，不接受用户输入卷名或对象前缀；卷名由 Compose project name + 固定逻辑名计算，并逐个用 `docker volume inspect` 验证 label 后才删除。MinIO 删除命令只由代码内两个固定前缀生成，删除前后分别确认 `warehouse` object count/size 不变。

- [ ] **Step 5: 运行安全护栏测试**

Run: `python -m unittest tests.test_chapter_10_5_bootstrap tests.test_chapter_10_5_artifacts -v`

Expected: PASS。

- [ ] **Step 6: 提交迁移工具**

```powershell
git add scripts/migrate_chapter_10_5.ps1 scripts/reset_chapter_10_5_realtime.ps1 tests/test_chapter_10_5_bootstrap.py tests/test_chapter_10_5_artifacts.py
git commit -m "feat: guard chapter 10.5 migration and reset"
```

---

### Task 11: 建立隔离冷启动与重启恢复动态验收

**Files:**
- Create: `scripts/verify_chapter_10_5_cold_start.ps1`
- Create: `tests/test_chapter_10_5_cold_start_verifier.py`
- Modify: `infra/docker-compose.yml`
- Modify: `infra/.env.example`
- Modify: `scripts/bootstrap_chapter_10_5.ps1`
- Modify: `tests/test_chapter_10_5_artifacts.py`

**Interfaces:**
- verifier 自动生成唯一 Compose project、容器名、端口、测试 subnet 和临时 MinIO 路径；不得复用默认项目卷或默认 MinIO 目录。
- 支持 `-KeepOnFailure`，默认成功后仅清理本次隔离项目和临时目录；清理前验证绝对路径位于 `tmp/chapter-10-5/acceptance/<run-id>`。
- 动态报告固定包含 `cold_start`、`idempotent_second_run`、`restart_recovery`、`data_continuity`、`readiness`、`tool_analysis`。

- [ ] **Step 1: 写隔离参数与清理边界失败测试**

覆盖 project name 白名单、端口/子网不等于默认值、所有 container name 可由 env 覆盖、临时 MinIO 路径边界、失败默认保留现场、成功只清理带当前 project label 的资源。

- [ ] **Step 2: 运行 verifier 单测并确认红灯**

Run: `python -m unittest tests.test_chapter_10_5_cold_start_verifier tests.test_chapter_10_5_artifacts -v`

Expected: FAIL。

- [ ] **Step 3: 参数化 Compose 中影响隔离的固定值**

为 MinIO/minio-init 和仍固定的容器名、宿主端口、Doris 静态 IP、Docker subnet 提供 env 默认值；默认值保持现有开发体验。Bootstrap 通过 Compose service 名通信，不能把默认 `ecom-*` 名写进逻辑。

- [ ] **Step 4: 实现三阶段真实验收**

1. 空卷/空临时目录执行 Bootstrap，等待生产 Job 和首个新鲜 checkpoint。
2. 不停止服务再次执行 Bootstrap，断言同名 Job 仍恰好一个且报告无重复初始化。
3. 记录 Trino row count、snapshot ID 和 checkpoint ID，重建 Hive/Trino/Flink 容器但保留卷与 MinIO，再执行 Bootstrap，断言表可查、row count 不回退、checkpoint 恢复推进、`/ready` 和 `/analysis/tools` 成功。

- [ ] **Step 5: 运行无 Docker 单测**

Run: `python -m unittest tests.test_chapter_10_5_cold_start_verifier tests.test_chapter_10_5_artifacts -v`

Expected: PASS。

- [ ] **Step 6: 运行隔离动态验收**

Run: `powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify_chapter_10_5_cold_start.ps1`

Expected: exit 0；报告六项均为 `passed`，默认开发项目的容器、卷和 MinIO object 计数不变。

- [ ] **Step 7: 提交冷启动验证器**

```powershell
git add scripts/verify_chapter_10_5_cold_start.ps1 tests/test_chapter_10_5_cold_start_verifier.py infra/docker-compose.yml infra/.env.example scripts/bootstrap_chapter_10_5.ps1 tests/test_chapter_10_5_artifacts.py
git commit -m "test: verify isolated cold start recovery"
```

---

### Task 12: 完成运行手册、全量回归与真实迁移记录

**Files:**
- Create: `docs/chapter-10-5-engineering-hardening-runbook.md`
- Modify: `README.md`
- Modify: `jobs/README.md`
- Modify: `docs/superpowers/specs/2026-08-25-chapter-10-5-engineering-hardening-design.md`
- Modify: `tests/test_chapter_10_5_artifacts.py`

**Interfaces:**
- 运行手册必须分别提供：全新克隆、已有环境迁移、日常幂等启动、实时层 reset、catalog 恢复、readiness 诊断、线程池饱和诊断。
- README 明确第 10.5 章完成后才进入第 11 章，并保留“本地单机非生产 HA/非公网安全部署”边界。

- [ ] **Step 1: 写文档契约失败测试**

断言 README/runbook 包含准确脚本名、显式确认参数、`/health` 与 `/ready` 区别、两个 MinIO bucket、五个命名卷、PostgreSQL Metastore、统一 deadline、有界线程池和不得删除 warehouse 的警告；断言无旧 Derby/current worktree checkpoint 指引。

- [ ] **Step 2: 运行文档测试并确认红灯**

Run: `python -m unittest tests.test_chapter_10_5_artifacts -v`

Expected: FAIL，运行手册尚未创建。

- [ ] **Step 3: 编写中文运行手册并更新章节状态**

将设计文档状态改为“已实现并完成隔离动态验收”，仅记录 Task 11 真实报告中的时间、row count、snapshot/checkpoint 变化和通过项；不得预填成功结果。README 使用一个推荐入口，不复制整份手册。

- [ ] **Step 4: 执行 Python 全量回归**

Run: `python -m unittest discover -s tests -v`

Expected: PASS，0 failures/errors。

- [ ] **Step 5: 执行 Java 全量回归与 Compose 解析**

Run: `mvn -f jobs/datastream-quality/pom.xml test`

Run: `docker compose --env-file infra/.env.example -f infra/docker-compose.yml --profile flink --profile serving --profile lakehouse config --quiet`

Expected: 两条命令 exit 0，Java 测试 0 failures/errors，Compose 无警告性缺失变量。

- [ ] **Step 6: 再次执行隔离动态验收并核对报告**

Run: `powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify_chapter_10_5_cold_start.ps1`

Expected: exit 0；冷启动、第二次幂等启动、容器重建恢复、数据连续性、readiness、工具分析全部通过。

- [ ] **Step 7: 检查变更范围和凭据泄漏**

Run: `git diff --check`

Run: `git status --short`

Run: `$unfinishedPatterns = @(('TO' + 'DO'), ('T' + 'BD'), ('PLACE' + 'HOLDER')); Select-String -Path README.md,docs/*.md,scripts/*.ps1,infra/*.yml,infra/*.json -Pattern (@('password\s*=\s*[^$<]','api[_-]?key\s*=\s*[^$<]') + $unfinishedPatterns) -CaseSensitive:$false`

Expected: `git diff --check` 无输出；status 只含本任务文件；扫描无真实凭据或未完成标记。

- [ ] **Step 8: 提交第 10.5 章收口**

```powershell
git add README.md jobs/README.md docs/chapter-10-5-engineering-hardening-runbook.md docs/superpowers/specs/2026-08-25-chapter-10-5-engineering-hardening-design.md tests/test_chapter_10_5_artifacts.py
git commit -m "docs: complete chapter 10.5 hardening"
```

---

## 最终验收清单

- [ ] 两个默认 Generator 实例即使 seed 相同也生成不同且格式合法的事件 ID。
- [ ] 运行依赖锁包含十个真实 SHA-256；错误下载永不覆盖已有正确 JAR。
- [ ] PostgreSQL Metastore、Kafka、Doris 在容器重建后从显式卷恢复。
- [ ] MinIO `warehouse` 使用主仓库稳定路径，Flink state 独立写入 `flink-state` bucket。
- [ ] 仅固定 Iceberg 表可恢复；metadata 缺失、冲突或 location 不一致时 fail closed。
- [ ] Bootstrap 连续运行两次不会重复 Job、Topic、表或破坏数据。
- [ ] 迁移/reset 无确认参数时不会发出任何 Docker 写操作，也绝不删除 MinIO warehouse。
- [ ] `/health` 只验证进程，`/ready` 在 Doris/Trino/Flink/checkpoint 任一异常时返回安全 503。
- [ ] planner、工具、repository、narrative 共用一个总 deadline。
- [ ] 连续阻塞请求最多占用配置的 worker 数；饱和后立即拒绝且不继续增线程。
- [ ] 隔离冷启动、幂等二次启动、容器重建恢复与数据连续性动态验收全部通过。
- [ ] Python、Java、Compose config、`git diff --check` 全部通过且无凭据泄漏。
