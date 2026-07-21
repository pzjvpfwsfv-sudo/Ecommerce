# 第 8 章可信指标 AI 分析助手实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在现有 FastAPI 中实现一个基于 Doris 实时指标与 Trino 历史聚合证据的结构化 AI 分析接口，并在模型不可用时自动降级为规则分析。

**Architecture:** Repository 负责受控查询，`AnalysisService` 负责组装 `AnalysisContext` 与降级编排，`MetricAnalyzer` 负责把可信证据转成结构化解读。第一版默认使用规则分析器，可通过配置切换 OpenAI-compatible HTTP 适配器，但模型永远不能生成或执行 SQL。

**Tech Stack:** Python 3.12、FastAPI 0.115、Pydantic v2、PyMySQL、HTTPX、Doris、Trino、unittest、Docker Compose、PowerShell

## Global Constraints

- 数据库查询必须由后端预定义，禁止模型生成或执行 SQL。
- 所有业务数字必须来自 `AnalysisContext.evidence`，响应必须原样携带 evidence。
- 默认 `AI_ANALYZER_MODE=rule_based`，自动化测试不得依赖真实 API Key。
- Trino 失败时保留 Doris 分析并添加 warning；Doris 失败时接口返回 503。
- 模型超时、网络错误或结构解析失败时必须降级为规则分析器。
- 真实密钥不得写入仓库、响应或日志。
- 用户问题去除首尾空白后不能为空，最大长度由 `AI_MAX_QUESTION_LENGTH` 控制，默认 500。
- 第一版不实现 NL2SQL、多轮 Agent、RAG、自动运营动作或聊天前端。
- 保留现有 `/health`、`/metrics/realtime`、`/metrics/{metric_name}` 行为兼容。

## 严格可信模式加固记录

- 数字只允许来自 `AnalysisContext.evidence` 或后端预定义派生值，所有分析器输出都必须通过来源集合校验。
- 校验先执行 NFKC 归一化，再按 fail-closed 策略处理无法识别的数字表达；当前只支持可见中文/英文数字分隔语义。
- 主分析器与回退分析器共用同一个数字来源守卫，任何路径出现无依据数字都不得返回给调用方。
- 日志通过结构化 `LogRecord.extra` 记录 request ID、analyzer、阶段、错误类型和耗时，避免字符串拼接敏感上下文。
- 异常链脱敏递归覆盖 `__cause__` 与 `__context__`，不得记录密钥、Authorization、Prompt、模型原始响应或内部堆栈。
- 模型叙事的 `summary`、`insights`、`risks`、`actions` 四字段显式完整，不能依赖模型校验层的默认值静默补齐。
- 边界声明：数值可追溯不等于整句语义正确；本章守卫不验证因果和建议质量，后续必须增加离线评测、回归数据集与结构化 claim。

---

## File Structure

- Create: `services/api/app/analysis_models.py`
  Responsibility: 定义分析请求、证据上下文、叙事结果和 API 响应模型。
- Create: `services/api/app/analyzers.py`
  Responsibility: 定义 `MetricAnalyzer` 协议、规则分析器和 OpenAI-compatible 分析器。
- Create: `services/api/app/trino_repository.py`
  Responsibility: 通过 Trino HTTP API 执行固定只读 SQL，并转换为 `HistoricalEvidence`。
- Create: `services/api/app/analysis_service.py`
  Responsibility: 查询 Doris/Trino、组装上下文、执行分析器并处理降级。
- Create: `services/api/app/dependencies.py`
  Responsibility: 根据 `ApiSettings` 构建 Repository、分析器和 `AnalysisService`。
- Modify: `services/api/app/config.py`
  Responsibility: 增加 Trino 与 AI 配置。
- Modify: `services/api/app/main.py`
  Responsibility: 增加 `POST /analysis/realtime` 并保持旧接口兼容。
- Modify: `services/api/requirements.txt`
  Responsibility: 显式加入 HTTPX 运行时依赖。
- Modify: `infra/.env.example`
  Responsibility: 提供无密钥的安全默认配置。
- Modify: `infra/docker-compose.yml`
  Responsibility: 把 Trino 与 AI 配置传入 API 容器。
- Create: `scripts/verify_chapter_8_analysis.ps1`
  Responsibility: 用真实 Doris、Trino 和规则分析器验证完整接口。
- Create: `tests/test_analysis_models_and_rules.py`
  Responsibility: 验证输入约束和规则分析边界。
- Create: `tests/test_trino_repository.py`
  Responsibility: 验证固定 SQL、分页结果与错误处理。
- Create: `tests/test_analysis_service.py`
  Responsibility: 验证证据编排和降级策略。
- Create: `tests/test_openai_compatible_analyzer.py`
  Responsibility: 验证模型请求、结构解析和失败行为，不访问外网。
- Create: `tests/test_analysis_api.py`
  Responsibility: 验证新接口、422 和 503 映射。
- Create: `tests/test_chapter_8_artifacts.py`
  Responsibility: 锁定配置、Compose、验证脚本、README 和占位目录清理。
- Modify: `README.md`
  Responsibility: 增加第 8 章运行命令、接口示例和后续演进路线。
- Delete: `infra/compose/app/default.conf`
- Delete: `infra/compose/app/index.html`
- Delete: `infra/compose/app/README.md`
  Responsibility: 删除已被真实 FastAPI 服务替代的第 1 章占位产物。

---

### Task 1: Define analysis models and deterministic rule analyzer

**Files:**
- Create: `services/api/app/analysis_models.py`
- Create: `services/api/app/analyzers.py`
- Create: `tests/test_analysis_models_and_rules.py`

**Interfaces:**
- Produces: `AnalysisRequest`, `RealtimeEvidence`, `HistoricalEvidence`, `AnalysisEvidence`, `AnalysisContext`, `AnalysisNarrative`, `AnalysisResponse`.
- Produces: `MetricAnalyzer.name: str`, `MetricAnalyzer.analyze(context: AnalysisContext) -> AnalysisNarrative`.
- Produces: `RuleBasedAnalyzer` with `name = "rule_based"`.

- [ ] **Step 1: Write failing model and rule tests**

```python
from datetime import datetime, timezone
import unittest

from pydantic import ValidationError

from app.analysis_models import (
    AnalysisContext,
    AnalysisEvidence,
    AnalysisRequest,
    HistoricalEvidence,
    RealtimeEvidence,
)
from app.analyzers import RuleBasedAnalyzer


class AnalysisModelsAndRulesTest(unittest.TestCase):
    def test_request_strips_question_and_rejects_blank(self):
        self.assertEqual("活跃情况", AnalysisRequest(question="  活跃情况  ").question)
        with self.assertRaises(ValidationError):
            AnalysisRequest(question="   ")

    def test_rule_analyzer_uses_only_evidence(self):
        context = AnalysisContext(
            question="当前用户活跃情况如何？",
            generated_at=datetime(2026, 7, 18, tzinfo=timezone.utc),
            evidence=AnalysisEvidence(
                realtime=RealtimeEvidence(pv=120, uv=80),
                historical=HistoricalEvidence(
                    event_count=1000,
                    event_type_counts={"view": 700, "click": 200, "cart": 70, "purchase": 30},
                ),
            ),
        )

        result = RuleBasedAnalyzer().analyze(context)

        self.assertIn("120", result.summary)
        self.assertTrue(any("1.5" in item for item in result.insights))
        self.assertTrue(any("累计" in item for item in result.risks))

    def test_rule_analyzer_handles_zero_uv_without_division(self):
        context = AnalysisContext(
            question="分析活跃度",
            generated_at=datetime(2026, 7, 18, tzinfo=timezone.utc),
            evidence=AnalysisEvidence(realtime=RealtimeEvidence(pv=10, uv=0)),
        )

        result = RuleBasedAnalyzer().analyze(context)

        self.assertTrue(any("UV" in item and "不足" in item for item in result.risks))
        self.assertFalse(any("人均" in item for item in result.insights))
```

- [ ] **Step 2: Run the tests and verify RED**

Run: `python -m unittest tests.test_analysis_models_and_rules -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'app.analysis_models'`.

- [ ] **Step 3: Implement the models**

```python
# services/api/app/analysis_models.py
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, field_validator


class AnalysisRequest(BaseModel):
    question: str

    @field_validator("question")
    @classmethod
    def normalize_question(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("question must not be blank")
        return normalized


class RealtimeEvidence(BaseModel):
    pv: int | None = None
    uv: int | None = None
    updated_at: datetime | None = None


class HistoricalEvidence(BaseModel):
    event_count: int | None = None
    event_type_counts: dict[str, int] = Field(default_factory=dict)
    latest_event_time: datetime | None = None


class AnalysisEvidence(BaseModel):
    realtime: RealtimeEvidence
    historical: HistoricalEvidence | None = None


class AnalysisContext(BaseModel):
    question: str
    generated_at: datetime
    evidence: AnalysisEvidence
    warnings: list[str] = Field(default_factory=list)


class AnalysisNarrative(BaseModel):
    summary: str
    insights: list[str] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)
    actions: list[str] = Field(default_factory=list)


class AnalysisResponse(AnalysisNarrative):
    evidence: AnalysisEvidence
    warnings: list[str] = Field(default_factory=list)
    analyzer: str
    generated_at: datetime
```

- [ ] **Step 4: Implement the analyzer protocol and rule analyzer**

```python
# services/api/app/analyzers.py
from __future__ import annotations

from typing import Protocol

from app.analysis_models import AnalysisContext, AnalysisNarrative


class MetricAnalyzer(Protocol):
    name: str

    def analyze(self, context: AnalysisContext) -> AnalysisNarrative: ...


class RuleBasedAnalyzer:
    name = "rule_based"

    def analyze(self, context: AnalysisContext) -> AnalysisNarrative:
        realtime = context.evidence.realtime
        historical = context.evidence.historical
        summary = self._summary(realtime.pv, realtime.uv)
        insights: list[str] = []
        risks = ["当前实时指标为累计值，缺少时间窗口对照时不能判断上涨或下降。"]
        actions = ["补充分钟级或小时级窗口指标后，再判断变化趋势。"]

        if realtime.pv is not None and realtime.uv is not None and realtime.uv > 0:
            insights.append(f"人均访问次数约为 {realtime.pv / realtime.uv:.1f} 次。")
        elif realtime.uv == 0:
            risks.append("UV 为 0，当前证据不足以计算人均访问次数。")

        if historical and historical.event_count is not None:
            insights.append(f"历史明细共包含 {historical.event_count} 条行为事件。")
            if historical.event_type_counts:
                top_type, top_count = max(historical.event_type_counts.items(), key=lambda item: item[1])
                share = top_count / historical.event_count if historical.event_count > 0 else 0
                insights.append(f"历史行为以 {top_type} 为主，占比约 {share:.1%}。")

        return AnalysisNarrative(summary=summary, insights=insights, risks=risks, actions=actions)

    @staticmethod
    def _summary(pv: int | None, uv: int | None) -> str:
        if pv is None or uv is None:
            return "实时 PV/UV 指标不完整，暂时只能提供有限分析。"
        return f"当前累计访问 {pv} 次，覆盖 {uv} 名用户。"
```

- [ ] **Step 5: Run tests and commit**

Run: `python -m unittest tests.test_analysis_models_and_rules -v`

Expected: 3 tests PASS.

```powershell
git add services/api/app/analysis_models.py services/api/app/analyzers.py tests/test_analysis_models_and_rules.py
git commit -m "feat: add grounded rule analyzer"
```

---

### Task 2: Add a fixed-query Trino repository

**Files:**
- Create: `services/api/app/trino_repository.py`
- Create: `tests/test_trino_repository.py`
- Modify: `services/api/requirements.txt`

**Interfaces:**
- Consumes: `HistoricalEvidence` from Task 1.
- Produces: `TrinoAnalyticsRepository.fetch_summary() -> HistoricalEvidence`.
- Produces fixed SQL constants `EVENT_COUNT_SQL`, `EVENT_TYPE_COUNTS_SQL`, and `LATEST_EVENT_TIME_SQL`; no user input enters SQL.

- [ ] **Step 1: Write failing repository tests**

```python
import unittest

import httpx

from app.trino_repository import TrinoAnalyticsRepository


class TrinoAnalyticsRepositoryTest(unittest.TestCase):
    def test_fetch_summary_follows_next_uri_and_maps_rows(self):
        requests: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(str(request.url))
            if request.method == "POST" and request.content == b"SELECT COUNT(*) AS event_count FROM lakehouse.analytics.user_behavior_detail":
                return httpx.Response(200, json={"data": [[1000]]})
            if request.method == "POST" and request.content == b"SELECT MAX(event_time) AS latest_event_time FROM lakehouse.analytics.user_behavior_detail":
                return httpx.Response(200, json={"data": [["2026-07-18T10:00:00Z"]]})
            if request.method == "POST":
                return httpx.Response(200, json={"data": [["view", 700]], "nextUri": "http://trino:8080/v1/next/1"})
            return httpx.Response(200, json={"data": [["click", 200], ["purchase", 100]]})

        transport = httpx.MockTransport(handler)
        repository = TrinoAnalyticsRepository(
            base_url="http://trino:8080",
            timeout_seconds=5,
            client_factory=lambda: httpx.Client(transport=transport),
        )

        result = repository.fetch_summary()

        self.assertEqual(1000, result.event_count)
        self.assertEqual({"view": 700, "click": 200, "purchase": 100}, result.event_type_counts)
        self.assertEqual("2026-07-18T10:00:00+00:00", result.latest_event_time.isoformat())
        self.assertTrue(any("/v1/next/1" in url for url in requests))

    def test_trino_error_response_raises_runtime_error(self):
        transport = httpx.MockTransport(
            lambda request: httpx.Response(200, json={"error": {"message": "catalog unavailable"}})
        )
        repository = TrinoAnalyticsRepository(
            base_url="http://trino:8080",
            client_factory=lambda: httpx.Client(transport=transport),
        )

        with self.assertRaisesRegex(RuntimeError, "catalog unavailable"):
            repository.fetch_summary()
```

- [ ] **Step 2: Run the tests and verify RED**

Run: `python -m unittest tests.test_trino_repository -v`

Expected: FAIL because `app.trino_repository` does not exist.

- [ ] **Step 3: Add HTTPX and implement the repository**

Append to `services/api/requirements.txt`:

```text
httpx==0.27.2
```

```python
# services/api/app/trino_repository.py
from __future__ import annotations

from collections.abc import Callable
from typing import Any

import httpx

from app.analysis_models import HistoricalEvidence


EVENT_COUNT_SQL = "SELECT COUNT(*) AS event_count FROM lakehouse.analytics.user_behavior_detail"
EVENT_TYPE_COUNTS_SQL = (
    "SELECT event_type, COUNT(*) AS event_count "
    "FROM lakehouse.analytics.user_behavior_detail "
    "GROUP BY event_type ORDER BY event_count DESC, event_type ASC"
)
LATEST_EVENT_TIME_SQL = (
    "SELECT MAX(event_time) AS latest_event_time "
    "FROM lakehouse.analytics.user_behavior_detail"
)
ClientFactory = Callable[[], httpx.Client]


class TrinoAnalyticsRepository:
    def __init__(
        self,
        base_url: str,
        user: str = "ecommerce-ai",
        catalog: str = "lakehouse",
        schema: str = "analytics",
        timeout_seconds: float = 10,
        client_factory: ClientFactory | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._headers = {
            "X-Trino-User": user,
            "X-Trino-Source": "chapter-8-ai-analysis",
            "X-Trino-Catalog": catalog,
            "X-Trino-Schema": schema,
        }
        self._timeout_seconds = timeout_seconds
        self._client_factory = client_factory or (lambda: httpx.Client())

    def fetch_summary(self) -> HistoricalEvidence:
        count_rows = self._execute(EVENT_COUNT_SQL)
        type_rows = self._execute(EVENT_TYPE_COUNTS_SQL)
        latest_rows = self._execute(LATEST_EVENT_TIME_SQL)
        event_count = int(count_rows[0][0]) if count_rows else 0
        counts = {str(row[0]): int(row[1]) for row in type_rows}
        latest_event_time = latest_rows[0][0] if latest_rows and latest_rows[0] else None
        return HistoricalEvidence(
            event_count=event_count,
            event_type_counts=counts,
            latest_event_time=latest_event_time,
        )

    def _execute(self, sql: str) -> list[list[Any]]:
        rows: list[list[Any]] = []
        with self._client_factory() as client:
            response = client.post(
                f"{self._base_url}/v1/statement",
                headers=self._headers,
                content=sql,
                timeout=self._timeout_seconds,
            )
            while True:
                response.raise_for_status()
                payload = response.json()
                if payload.get("error"):
                    raise RuntimeError(f"Trino query failed: {payload['error']['message']}")
                rows.extend(payload.get("data", []))
                next_uri = payload.get("nextUri")
                if not next_uri:
                    return rows
                response = client.get(next_uri, timeout=self._timeout_seconds)
```

- [ ] **Step 4: Run tests and commit**

Run: `python -m unittest tests.test_trino_repository -v`

Expected: 2 tests PASS.

```powershell
git add services/api/app/trino_repository.py services/api/requirements.txt tests/test_trino_repository.py
git commit -m "feat: add fixed trino analytics queries"
```

---

### Task 3: Compose trusted evidence and implement degradation

**Files:**
- Create: `services/api/app/analysis_service.py`
- Create: `tests/test_analysis_service.py`

**Interfaces:**
- Consumes: `RealtimeMetricsRepository.fetch_all_metrics() -> dict[str, Any]`.
- Consumes: `TrinoAnalyticsRepository.fetch_summary() -> HistoricalEvidence`.
- Consumes: primary and fallback `MetricAnalyzer` implementations.
- Produces: `AnalysisService.analyze(question: str) -> AnalysisResponse`.
- Produces: `RealtimeDataUnavailableError` for the API layer to map to 503.

- [ ] **Step 1: Write failing service tests**

```python
from datetime import datetime, timezone
import unittest
from unittest.mock import Mock

from app.analysis_models import AnalysisNarrative, HistoricalEvidence
from app.analysis_service import AnalysisService, RealtimeDataUnavailableError
from app.analyzers import RuleBasedAnalyzer


class AnalysisServiceTest(unittest.TestCase):
    def setUp(self):
        self.realtime = Mock()
        self.realtime.fetch_all_metrics.return_value = {
            "pv": 12,
            "uv": 5,
            "updated_at": "2026-07-18T10:00:00",
        }
        self.trino = Mock()
        self.trino.fetch_summary.return_value = HistoricalEvidence(
            event_count=20,
            event_type_counts={"view": 15, "click": 5},
            latest_event_time="2026-07-18T09:59:00Z",
        )
        self.clock = lambda: datetime(2026, 7, 18, tzinfo=timezone.utc)

    def test_service_returns_evidence_and_primary_analyzer_name(self):
        analyzer = Mock(name="primary")
        analyzer.name = "model"
        analyzer.analyze.return_value = AnalysisNarrative(summary="结论")
        service = AnalysisService(self.realtime, self.trino, analyzer, RuleBasedAnalyzer(), self.clock)

        response = service.analyze("分析活跃度")

        self.assertEqual(12, response.evidence.realtime.pv)
        self.assertEqual(20, response.evidence.historical.event_count)
        self.assertEqual("model", response.analyzer)

    def test_trino_failure_adds_warning_and_keeps_realtime_analysis(self):
        self.trino.fetch_summary.side_effect = RuntimeError("trino down")
        service = AnalysisService(self.realtime, self.trino, RuleBasedAnalyzer(), RuleBasedAnalyzer(), self.clock)

        response = service.analyze("分析活跃度")

        self.assertIsNone(response.evidence.historical)
        self.assertTrue(any("历史数据" in warning for warning in response.warnings))

    def test_primary_failure_falls_back_to_rule_analyzer(self):
        primary = Mock()
        primary.name = "openai_compatible"
        primary.analyze.side_effect = TimeoutError("timeout")
        service = AnalysisService(self.realtime, self.trino, primary, RuleBasedAnalyzer(), self.clock)

        response = service.analyze("分析活跃度")

        self.assertEqual("rule_based", response.analyzer)
        self.assertTrue(any("模型分析" in warning for warning in response.warnings))

    def test_service_logs_request_id_timings_and_analyzer(self):
        service = AnalysisService(self.realtime, self.trino, RuleBasedAnalyzer(), RuleBasedAnalyzer(), self.clock)

        with self.assertLogs("app.analysis_service", level="INFO") as captured:
            service.analyze("分析活跃度")

        joined = " ".join(captured.output)
        self.assertIn("request_id=", joined)
        self.assertIn("doris_ms=", joined)
        self.assertIn("trino_ms=", joined)
        self.assertIn("analyzer_ms=", joined)
        self.assertIn("analyzer=rule_based", joined)

    def test_realtime_failure_is_fatal(self):
        self.realtime.fetch_all_metrics.side_effect = RuntimeError("doris down")
        service = AnalysisService(self.realtime, self.trino, RuleBasedAnalyzer(), RuleBasedAnalyzer(), self.clock)

        with self.assertRaises(RealtimeDataUnavailableError):
            service.analyze("分析活跃度")
```

- [ ] **Step 2: Run the tests and verify RED**

Run: `python -m unittest tests.test_analysis_service -v`

Expected: FAIL because `app.analysis_service` does not exist.

- [ ] **Step 3: Implement the orchestration service**

```python
# services/api/app/analysis_service.py
from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
import logging
from time import perf_counter
from typing import Any, Protocol
from uuid import uuid4

from app.analysis_models import (
    AnalysisContext,
    AnalysisEvidence,
    AnalysisResponse,
    HistoricalEvidence,
    RealtimeEvidence,
)
from app.analyzers import MetricAnalyzer


logger = logging.getLogger(__name__)


class RealtimeRepository(Protocol):
    def fetch_all_metrics(self) -> dict[str, Any]: ...


class HistoricalRepository(Protocol):
    def fetch_summary(self) -> HistoricalEvidence: ...


class RealtimeDataUnavailableError(RuntimeError):
    pass


class AnalysisService:
    def __init__(
        self,
        realtime_repository: RealtimeRepository,
        historical_repository: HistoricalRepository,
        primary_analyzer: MetricAnalyzer,
        fallback_analyzer: MetricAnalyzer,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._realtime = realtime_repository
        self._historical = historical_repository
        self._primary = primary_analyzer
        self._fallback = fallback_analyzer
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def analyze(self, question: str) -> AnalysisResponse:
        request_id = str(uuid4())
        warnings: list[str] = []
        started_at = perf_counter()
        try:
            raw_realtime = self._realtime.fetch_all_metrics()
        except Exception as exc:
            logger.exception(
                "analysis_doris_failed request_id=%s doris_ms=%.2f error_type=%s",
                request_id,
                (perf_counter() - started_at) * 1000,
                type(exc).__name__,
            )
            raise RealtimeDataUnavailableError("Doris realtime metrics are unavailable") from exc
        doris_ms = (perf_counter() - started_at) * 1000

        realtime = RealtimeEvidence(
            pv=self._optional_int(raw_realtime.get("pv")),
            uv=self._optional_int(raw_realtime.get("uv")),
            updated_at=raw_realtime.get("updated_at"),
        )

        historical = None
        trino_started_at = perf_counter()
        try:
            historical = self._historical.fetch_summary()
        except Exception as exc:
            warnings.append("历史数据暂不可用，本次仅基于 Doris 实时指标分析。")
            logger.warning(
                "analysis_trino_degraded request_id=%s trino_ms=%.2f error_type=%s",
                request_id,
                (perf_counter() - trino_started_at) * 1000,
                type(exc).__name__,
            )
        trino_ms = (perf_counter() - trino_started_at) * 1000

        generated_at = self._clock()
        context = AnalysisContext(
            question=question,
            generated_at=generated_at,
            evidence=AnalysisEvidence(realtime=realtime, historical=historical),
            warnings=warnings,
        )

        analyzer = self._primary
        analyzer_started_at = perf_counter()
        try:
            narrative = analyzer.analyze(context)
        except Exception as exc:
            warnings.append("模型分析暂不可用，已降级为规则分析。")
            logger.warning(
                "analysis_model_degraded request_id=%s analyzer=%s error_type=%s",
                request_id,
                analyzer.name,
                type(exc).__name__,
            )
            analyzer = self._fallback
            narrative = analyzer.analyze(context.model_copy(update={"warnings": warnings}))
        analyzer_ms = (perf_counter() - analyzer_started_at) * 1000

        logger.info(
            "analysis_complete request_id=%s analyzer=%s doris_ms=%.2f trino_ms=%.2f analyzer_ms=%.2f degraded=%s",
            request_id,
            analyzer.name,
            doris_ms,
            trino_ms,
            analyzer_ms,
            bool(warnings),
        )

        return AnalysisResponse(
            **narrative.model_dump(),
            evidence=context.evidence,
            warnings=warnings,
            analyzer=analyzer.name,
            generated_at=generated_at,
        )

    @staticmethod
    def _optional_int(value: Any) -> int | None:
        return None if value is None else int(value)
```

- [ ] **Step 4: Run tests and commit**

Run: `python -m unittest tests.test_analysis_service -v`

Expected: 5 tests PASS.

```powershell
git add services/api/app/analysis_service.py tests/test_analysis_service.py
git commit -m "feat: orchestrate grounded metric analysis"
```

---

### Task 4: Add optional OpenAI-compatible analyzer and configuration

**Files:**
- Modify: `services/api/app/analyzers.py`
- Modify: `services/api/app/config.py`
- Create: `services/api/app/dependencies.py`
- Create: `tests/test_openai_compatible_analyzer.py`

**Interfaces:**
- Produces: `OpenAICompatibleAnalyzer(api_key, base_url, model, timeout_seconds, client_factory)`.
- Produces: `build_analysis_service(settings, realtime_repository) -> AnalysisService`.
- Extends: `ApiSettings` with Trino and AI fields.

- [ ] **Step 1: Write failing adapter and configuration tests**

```python
from datetime import datetime, timezone
import unittest

import httpx

from app.analysis_models import AnalysisContext, AnalysisEvidence, RealtimeEvidence
from app.analyzers import OpenAICompatibleAnalyzer
from app.config import load_settings


class OpenAICompatibleAnalyzerTest(unittest.TestCase):
    def test_adapter_sends_evidence_and_parses_structured_narrative(self):
        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["body"] = request.read().decode("utf-8")
            captured["authorization"] = request.headers["Authorization"]
            return httpx.Response(
                200,
                json={"choices": [{"message": {"content": '{"summary":"可信结论","insights":[],"risks":[],"actions":[]}'}}]},
            )

        analyzer = OpenAICompatibleAnalyzer(
            api_key="secret",
            base_url="http://model.local/v1",
            model="demo-model",
            client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
        )
        context = AnalysisContext(
            question="分析活跃度",
            generated_at=datetime(2026, 7, 18, tzinfo=timezone.utc),
            evidence=AnalysisEvidence(realtime=RealtimeEvidence(pv=12, uv=5)),
        )

        result = analyzer.analyze(context)

        self.assertEqual("可信结论", result.summary)
        self.assertIn('"pv":12', captured["body"].replace(" ", ""))
        self.assertEqual("Bearer secret", captured["authorization"])

    def test_load_settings_defaults_to_rule_mode(self):
        settings = load_settings(environ={})
        self.assertEqual("rule_based", settings.ai_analyzer_mode)
        self.assertEqual(500, settings.ai_max_question_length)
```

- [ ] **Step 2: Run the tests and verify RED**

Run: `python -m unittest tests.test_openai_compatible_analyzer -v`

Expected: FAIL because `OpenAICompatibleAnalyzer` and new settings do not exist.

- [ ] **Step 3: Extend configuration with injectable environment mapping**

Replace `load_settings()` with `load_settings(environ: Mapping[str, str] | None = None)` and add fields:

```python
from collections.abc import Mapping
from os import environ as os_environ


@dataclass(frozen=True)
class ApiSettings:
    doris_host: str = "127.0.0.1"
    doris_port: int = 9030
    doris_database: str = "analytics"
    doris_username: str = "root"
    doris_password: str = ""
    trino_base_url: str = "http://localhost:8088"
    trino_user: str = "ecommerce-ai"
    trino_catalog: str = "lakehouse"
    trino_schema: str = "analytics"
    trino_request_timeout_seconds: float = 10
    ai_analyzer_mode: str = "rule_based"
    ai_api_key: str = ""
    ai_base_url: str = ""
    ai_model: str = ""
    ai_request_timeout_seconds: float = 15
    ai_max_question_length: int = 500


def load_settings(environ: Mapping[str, str] | None = None) -> ApiSettings:
    values = os_environ if environ is None else environ
    return ApiSettings(
        doris_host=values.get("DORIS_HOST", "127.0.0.1"),
        doris_port=int(values.get("DORIS_PORT", "9030")),
        doris_database=values.get("DORIS_DATABASE", "analytics"),
        doris_username=values.get("DORIS_USERNAME", "root"),
        doris_password=values.get("DORIS_PASSWORD", ""),
        trino_base_url=values.get("TRINO_BASE_URL", "http://localhost:8088"),
        trino_user=values.get("TRINO_USER", "ecommerce-ai"),
        trino_catalog=values.get("TRINO_CATALOG", "lakehouse"),
        trino_schema=values.get("TRINO_SCHEMA", "analytics"),
        trino_request_timeout_seconds=float(values.get("TRINO_REQUEST_TIMEOUT_SECONDS", "10")),
        ai_analyzer_mode=values.get("AI_ANALYZER_MODE", "rule_based"),
        ai_api_key=values.get("AI_API_KEY", ""),
        ai_base_url=values.get("AI_BASE_URL", ""),
        ai_model=values.get("AI_MODEL", ""),
        ai_request_timeout_seconds=float(values.get("AI_REQUEST_TIMEOUT_SECONDS", "15")),
        ai_max_question_length=int(values.get("AI_MAX_QUESTION_LENGTH", "500")),
    )
```

- [ ] **Step 4: Implement the HTTP adapter**

Add to `services/api/app/analyzers.py`:

```python
import json
from collections.abc import Callable

import httpx

from app.analysis_models import AnalysisNarrative


class OpenAICompatibleAnalyzer:
    name = "openai_compatible"

    def __init__(
        self,
        api_key: str,
        base_url: str,
        model: str,
        timeout_seconds: float = 15,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        if not api_key or not base_url or not model:
            raise ValueError("AI_API_KEY, AI_BASE_URL and AI_MODEL are required")
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._model = model
        self._timeout = timeout_seconds
        self._client_factory = client_factory or (lambda: httpx.Client())

    def analyze(self, context: AnalysisContext) -> AnalysisNarrative:
        payload = {
            "model": self._model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "你是电商指标分析助手。只能使用用户消息中的 evidence；"
                        "不得生成 SQL，不得声称访问其他数据。只返回 JSON，字段为 "
                        "summary、insights、risks、actions。"
                    ),
                },
                {"role": "user", "content": context.model_dump_json()},
            ],
            "temperature": 0,
        }
        with self._client_factory() as client:
            response = client.post(
                f"{self._base_url}/chat/completions",
                headers={"Authorization": f"Bearer {self._api_key}"},
                json=payload,
                timeout=self._timeout,
            )
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]
        return AnalysisNarrative.model_validate(json.loads(content))
```

- [ ] **Step 5: Implement dependency construction**

```python
# services/api/app/dependencies.py
from app.analysis_service import AnalysisService
from app.analyzers import OpenAICompatibleAnalyzer, RuleBasedAnalyzer
from app.config import ApiSettings
from app.repository import RealtimeMetricsRepository
from app.trino_repository import TrinoAnalyticsRepository


def build_analysis_service(
    settings: ApiSettings,
    realtime_repository: RealtimeMetricsRepository,
) -> AnalysisService:
    fallback = RuleBasedAnalyzer()
    primary = fallback
    if settings.ai_analyzer_mode == "openai_compatible":
        primary = OpenAICompatibleAnalyzer(
            api_key=settings.ai_api_key,
            base_url=settings.ai_base_url,
            model=settings.ai_model,
            timeout_seconds=settings.ai_request_timeout_seconds,
        )
    elif settings.ai_analyzer_mode != "rule_based":
        raise ValueError(f"unsupported AI_ANALYZER_MODE: {settings.ai_analyzer_mode}")

    historical = TrinoAnalyticsRepository(
        base_url=settings.trino_base_url,
        user=settings.trino_user,
        catalog=settings.trino_catalog,
        schema=settings.trino_schema,
        timeout_seconds=settings.trino_request_timeout_seconds,
    )
    return AnalysisService(realtime_repository, historical, primary, fallback)
```

- [ ] **Step 6: Run tests and commit**

Run: `python -m unittest tests.test_openai_compatible_analyzer tests.test_analysis_service -v`

Expected: all tests PASS without network access.

```powershell
git add services/api/app/analyzers.py services/api/app/config.py services/api/app/dependencies.py tests/test_openai_compatible_analyzer.py
git commit -m "feat: add optional model analyzer with fallback"
```

---

### Task 5: Expose the grounded analysis API without breaking existing endpoints

**Files:**
- Modify: `services/api/app/main.py`
- Modify: `tests/test_api_service.py`
- Create: `tests/test_analysis_api.py`

**Interfaces:**
- Consumes: `AnalysisService.analyze(question) -> AnalysisResponse`.
- Produces: `POST /analysis/realtime`.
- Preserves: existing `create_app(repository=...)` tests by adding optional keyword dependencies.

- [ ] **Step 1: Write failing endpoint tests**

```python
import unittest
from unittest.mock import Mock

from fastapi.testclient import TestClient

from app.analysis_models import AnalysisResponse, AnalysisEvidence, RealtimeEvidence
from app.analysis_service import RealtimeDataUnavailableError
from app.config import ApiSettings
from app.main import create_app


class AnalysisApiTest(unittest.TestCase):
    def test_analysis_endpoint_returns_service_response(self):
        service = Mock()
        service.analyze.return_value = AnalysisResponse(
            summary="可信结论",
            evidence=AnalysisEvidence(realtime=RealtimeEvidence(pv=12, uv=5)),
            analyzer="rule_based",
            generated_at="2026-07-18T00:00:00Z",
        )
        client = TestClient(
            create_app(
                repository=Mock(),
                analysis_service=service,
                settings=ApiSettings(ai_max_question_length=20),
            )
        )

        response = client.post("/analysis/realtime", json={"question": "  分析活跃度  "})

        self.assertEqual(200, response.status_code)
        self.assertEqual("可信结论", response.json()["summary"])
        service.analyze.assert_called_once_with("分析活跃度")

    def test_question_over_configured_limit_returns_422(self):
        client = TestClient(
            create_app(
                repository=Mock(),
                analysis_service=Mock(),
                settings=ApiSettings(ai_max_question_length=3),
            )
        )

        response = client.post("/analysis/realtime", json={"question": "超过长度"})
        self.assertEqual(422, response.status_code)

    def test_doris_failure_returns_503(self):
        service = Mock()
        service.analyze.side_effect = RealtimeDataUnavailableError("unavailable")
        client = TestClient(create_app(repository=Mock(), analysis_service=service))

        response = client.post("/analysis/realtime", json={"question": "分析活跃度"})
        self.assertEqual(503, response.status_code)
        self.assertEqual({"detail": "realtime metrics are temporarily unavailable"}, response.json())
```

- [ ] **Step 2: Run the tests and verify RED**

Run: `python -m unittest tests.test_analysis_api -v`

Expected: FAIL because `create_app` does not accept `analysis_service` and the route is missing.

- [ ] **Step 3: Wire the route and dependencies**

Update `create_app` to this dependency shape while retaining old routes:

```python
from fastapi import FastAPI, HTTPException, status

from app.analysis_models import AnalysisRequest, AnalysisResponse
from app.analysis_service import AnalysisService, RealtimeDataUnavailableError
from app.config import ApiSettings, load_settings
from app.dependencies import build_analysis_service


def create_app(
    repository: RealtimeMetricsRepository | Any | None = None,
    analysis_service: AnalysisService | Any | None = None,
    settings: ApiSettings | None = None,
) -> FastAPI:
    settings = settings or load_settings()
    if repository is None:
        repository = RealtimeMetricsRepository.from_settings(settings)
    if analysis_service is None:
        analysis_service = build_analysis_service(settings, repository)

    app = FastAPI(title="Realtime Metrics API", version="0.2.0")

    @app.post("/analysis/realtime", response_model=AnalysisResponse)
    def analyze_realtime(request: AnalysisRequest) -> AnalysisResponse:
        if len(request.question) > settings.ai_max_question_length:
            raise HTTPException(status_code=422, detail="question is too long")
        try:
            return analysis_service.analyze(request.question)
        except RealtimeDataUnavailableError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="realtime metrics are temporarily unavailable",
            ) from exc
```

Keep the existing `/health`, `/metrics/realtime`, and `/metrics/{metric_name}` route bodies unchanged below this route.

- [ ] **Step 4: Extend old artifact assertions**

Add to `tests/test_api_service.py`:

```python
self.assertIn('@app.post("/analysis/realtime"', text)
self.assertIn("build_analysis_service", text)
```

- [ ] **Step 5: Run API regressions and commit**

Run: `python -m unittest tests.test_api_service tests.test_analysis_api -v`

Expected: existing 7 API tests plus 3 new tests PASS.

```powershell
git add services/api/app/main.py tests/test_api_service.py tests/test_analysis_api.py
git commit -m "feat: expose grounded analysis endpoint"
```

---

### Task 6: Configure Compose, add real verification, remove the placeholder, and document Chapter 8

**Files:**
- Modify: `infra/.env.example`
- Modify: `infra/docker-compose.yml`
- Create: `scripts/verify_chapter_8_analysis.ps1`
- Create: `tests/test_chapter_8_artifacts.py`
- Modify: `README.md`
- Delete: `infra/compose/app/default.conf`
- Delete: `infra/compose/app/index.html`
- Delete: `infra/compose/app/README.md`

**Interfaces:**
- Consumes: `POST /analysis/realtime` from Task 5.
- Produces: safe Compose defaults and a repeatable rule-mode integration command.

- [ ] **Step 1: Write failing artifact tests**

```python
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent.parent


class Chapter8ArtifactsTest(unittest.TestCase):
    def test_env_and_compose_define_safe_ai_defaults(self):
        env_text = (ROOT / "infra/.env.example").read_text(encoding="utf-8")
        compose_text = (ROOT / "infra/docker-compose.yml").read_text(encoding="utf-8")
        for key in (
            "TRINO_BASE_URL=",
            "AI_ANALYZER_MODE=rule_based",
            "AI_API_KEY=",
            "AI_BASE_URL=",
            "AI_MODEL=",
            "AI_REQUEST_TIMEOUT_SECONDS=",
            "AI_MAX_QUESTION_LENGTH=",
        ):
            self.assertIn(key, env_text)
        self.assertIn("TRINO_BASE_URL: ${TRINO_BASE_URL}", compose_text)
        self.assertIn("AI_ANALYZER_MODE: ${AI_ANALYZER_MODE}", compose_text)

    def test_verification_script_calls_real_analysis_endpoint(self):
        text = (ROOT / "scripts/verify_chapter_8_analysis.ps1").read_text(encoding="utf-8")
        self.assertIn("verify_chapter_6_trino_queries.ps1", text)
        self.assertIn("run_chapter_4_pipeline.ps1", text)
        self.assertIn("/analysis/realtime", text)
        self.assertIn("rule_based", text)
        self.assertIn("evidence", text)

    def test_obsolete_nginx_placeholder_is_removed(self):
        self.assertFalse((ROOT / "infra/compose/app").exists())

    def test_readme_documents_chapter_8(self):
        text = (ROOT / "README.md").read_text(encoding="utf-8")
        self.assertIn("第 8 章", text)
        self.assertIn("verify_chapter_8_analysis.ps1", text)
        self.assertIn("POST /analysis/realtime", text)
```

- [ ] **Step 2: Run the tests and verify RED**

Run: `python -m unittest tests.test_chapter_8_artifacts -v`

Expected: 4 failures for missing config, script, docs, and stale placeholder.

- [ ] **Step 3: Add safe environment defaults**

Append to `infra/.env.example`:

```dotenv
TRINO_BASE_URL=http://trino:8080
TRINO_USER=ecommerce-ai
TRINO_CATALOG=lakehouse
TRINO_SCHEMA=analytics
TRINO_REQUEST_TIMEOUT_SECONDS=10

AI_ANALYZER_MODE=rule_based
AI_API_KEY=
AI_BASE_URL=
AI_MODEL=
AI_REQUEST_TIMEOUT_SECONDS=15
AI_MAX_QUESTION_LENGTH=500
```

Add to the API service environment in `infra/docker-compose.yml`:

```yaml
      TRINO_BASE_URL: ${TRINO_BASE_URL}
      TRINO_USER: ${TRINO_USER}
      TRINO_CATALOG: ${TRINO_CATALOG}
      TRINO_SCHEMA: ${TRINO_SCHEMA}
      TRINO_REQUEST_TIMEOUT_SECONDS: ${TRINO_REQUEST_TIMEOUT_SECONDS}
      AI_ANALYZER_MODE: ${AI_ANALYZER_MODE}
      AI_API_KEY: ${AI_API_KEY}
      AI_BASE_URL: ${AI_BASE_URL}
      AI_MODEL: ${AI_MODEL}
      AI_REQUEST_TIMEOUT_SECONDS: ${AI_REQUEST_TIMEOUT_SECONDS}
      AI_MAX_QUESTION_LENGTH: ${AI_MAX_QUESTION_LENGTH}
```

- [ ] **Step 4: Delete the stale placeholder with apply_patch**

Delete exactly these files and no other user files:

```text
infra/compose/app/default.conf
infra/compose/app/index.html
infra/compose/app/README.md
```

- [ ] **Step 5: Add the integration verification script**

```powershell
# scripts/verify_chapter_8_analysis.ps1
[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$chapter6Verify = Join-Path $repoRoot "scripts/verify_chapter_6_trino_queries.ps1"
$chapter4Run = Join-Path $repoRoot "scripts/run_chapter_4_pipeline.ps1"
$analysisUrl = "http://localhost:8000/analysis/realtime"

Push-Location $repoRoot
try {
    Write-Host "[chapter8-verify] preparing Iceberg history and Trino..."
    & $chapter6Verify
    if ($LASTEXITCODE -ne 0) { throw "Chapter 6 verification failed." }

    Write-Host "[chapter8-verify] preparing Doris realtime metrics and API..."
    & $chapter4Run
    if ($LASTEXITCODE -ne 0) { throw "Chapter 4 pipeline failed." }

    $events = @(
        '{"event_id":"chapter8-001","user_id":"user-001","product_id":"product-001","event_type":"view","event_time":"2026-07-18T10:00:00+08:00","channel":"app","device_type":"mobile","page_id":"home"}',
        '{"event_id":"chapter8-002","user_id":"user-002","product_id":"product-002","event_type":"click","event_time":"2026-07-18T10:00:01+08:00","channel":"web","device_type":"desktop","page_id":"detail"}'
    )
    $events | docker exec -i ecom-kafka kafka-console-producer --bootstrap-server kafka:29092 --topic user_behavior_events | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Failed to publish Chapter 8 events." }

    $deadline = (Get-Date).AddSeconds(90)
    do {
        try {
            $body = @{ question = "当前用户活跃情况如何？" } | ConvertTo-Json
            $response = Invoke-RestMethod -Method Post -Uri $analysisUrl -ContentType "application/json" -Body $body
            if ($response.analyzer -eq "rule_based" -and $response.evidence.realtime.pv -gt 0) {
                break
            }
        } catch {
            Start-Sleep -Seconds 3
        }
        Start-Sleep -Seconds 3
    } while ((Get-Date) -lt $deadline)

    if (-not $response -or $response.analyzer -ne "rule_based") {
        throw "Chapter 8 analysis endpoint did not return rule-based evidence in time."
    }
    if (-not $response.evidence.historical -or $response.evidence.historical.event_count -le 0) {
        throw "Chapter 8 analysis endpoint did not return Trino historical evidence."
    }

    Write-Host "[chapter8-verify] analyzer=$($response.analyzer)"
    Write-Host "[chapter8-verify] pv=$($response.evidence.realtime.pv) uv=$($response.evidence.realtime.uv)"
    Write-Host "[chapter8-verify] historical_event_count=$($response.evidence.historical.event_count)"
} finally {
    Pop-Location
}
```

- [ ] **Step 6: Document Chapter 8 in README**

Add a section containing these exact commands and boundaries:

````markdown
## 第 8 章：可信指标 AI 分析助手

第一版由后端查询 Doris 与 Trino，再让分析器只基于 evidence 生成解读；默认规则模式不需要 API Key，也不允许模型生成 SQL。

```powershell
./scripts/verify_chapter_8_analysis.ps1
```

接口：`POST /analysis/realtime`

后续按“趋势与异常 -> 受控工具调用 -> 受控 NL2SQL -> 产品化评测”演进。
````

- [ ] **Step 7: Run all Chapter 8 tests and Compose validation**

Run:

```powershell
python -m unittest tests.test_analysis_models_and_rules tests.test_trino_repository tests.test_analysis_service tests.test_openai_compatible_analyzer tests.test_analysis_api tests.test_api_service tests.test_chapter_8_artifacts -v
docker compose --env-file infra/.env.example -f infra/docker-compose.yml --profile serving --profile lakehouse config --quiet
```

Expected: all tests PASS and Compose exits 0.

- [ ] **Step 8: Run real rule-mode integration verification**

Run:

```powershell
$env:DOCKER_HOST='npipe:////./pipe/dockerDesktopLinuxEngine'
./scripts/verify_chapter_8_analysis.ps1
```

Expected output contains:

```text
[chapter8-verify] analyzer=rule_based
[chapter8-verify] pv=<positive integer> uv=<positive integer>
[chapter8-verify] historical_event_count=<positive integer>
```

- [ ] **Step 9: Commit the integration and documentation**

```powershell
git add infra/.env.example infra/docker-compose.yml services/api/requirements.txt scripts/verify_chapter_8_analysis.ps1 tests/test_chapter_8_artifacts.py README.md
git add -u infra/compose/app
git commit -m "feat: complete chapter 8 grounded ai analysis"
```

---

## Final Verification

- [ ] Run all API and Chapter 8 tests from a fresh process:

```powershell
python -m unittest tests.test_api_service tests.test_analysis_models_and_rules tests.test_trino_repository tests.test_analysis_service tests.test_openai_compatible_analyzer tests.test_analysis_api tests.test_chapter_8_artifacts -v
```

- [ ] Compile the API package:

```powershell
python -m compileall -q services/api
```

- [ ] Validate Compose:

```powershell
docker compose --env-file infra/.env.example -f infra/docker-compose.yml --profile serving --profile lakehouse config --quiet
```

- [ ] Run the real rule-mode verification and capture `analyzer`, PV/UV and historical count.
- [ ] Review `git diff --check` and confirm `.superpowers/sdd/task-1-report.md` is not staged.
- [ ] Request code review, fix Critical/Important findings, then push the completed commits.
