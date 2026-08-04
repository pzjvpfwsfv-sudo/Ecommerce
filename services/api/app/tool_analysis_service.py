from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
import logging
from uuid import UUID, uuid4

from app.analysis_models import AnalysisNarrative
from app.analysis_service import validate_narrative_numbers
from app.tool_executor import ToolExecutor
from app.tool_models import ToolAnalysisContext, ToolAnalysisResponse, ToolAnalysisSelection, ToolPlan
from app.tool_narratives import (
    RuleBasedToolNarrativeAnalyzer,
    ToolNarrativeAnalyzer,
    allowed_tool_narrative_numbers,
    render_tool_selection,
)
from app.tool_planners import ToolPlanner


LOGGER = logging.getLogger(__name__)


class ToolAnalysisUnavailableError(RuntimeError):
    pass


class ToolAnalysisService:
    def __init__(
        self,
        primary_planner: ToolPlanner,
        fallback_planner: ToolPlanner,
        executor: ToolExecutor,
        primary_analyzer: ToolNarrativeAnalyzer,
        fallback_analyzer: ToolNarrativeAnalyzer,
        clock: Callable[[], datetime] | None = None,
        audit_id_factory: Callable[[], UUID] | None = None,
    ) -> None:
        self._primary_planner = primary_planner
        self._fallback_planner = fallback_planner
        self._executor = executor
        self._primary_analyzer = primary_analyzer
        self._fallback_analyzer = fallback_analyzer
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._audit_id_factory = audit_id_factory or uuid4

    def analyze(self, question: str) -> ToolAnalysisResponse:
        audit_id = self._new_audit_id()
        warnings: list[str] = []
        degraded = False
        planner = self._primary_planner
        try:
            plan = self._validated_plan(planner.plan(question))
        except Exception:
            degraded = True
            warnings.append("规划模型不可用，已降级为规则规划。")
            self._audit(audit_id, "plan", planner, None, True, "plan_failure", logging.WARNING)
            planner = self._fallback_planner
            try:
                plan = self._validated_plan(planner.plan(question))
            except Exception:
                self._audit(audit_id, "plan", planner, None, True, "plan_failure", logging.ERROR)
                raise ToolAnalysisUnavailableError("tool analysis is unavailable") from None

        try:
            execution = self._executor.execute(plan, str(audit_id))
        except Exception:
            self._audit(audit_id, "execute", planner, None, degraded, "execution_failure", logging.ERROR)
            raise ToolAnalysisUnavailableError("tool analysis is unavailable") from None
        if execution.degraded:
            degraded = True
            warnings.append("部分请求证据暂不可用，已基于可用证据继续分析。")

        try:
            context = ToolAnalysisContext(
                question=question,
                generated_at=self._clock(),
                evidence=execution.evidence,
                warnings=warnings,
            )
        except Exception:
            self._audit(audit_id, "claims", planner, None, degraded, "context_failure", logging.ERROR)
            raise ToolAnalysisUnavailableError("tool analysis is unavailable") from None

        analyzer = self._primary_analyzer
        try:
            narrative = self._render_selected_claims(analyzer, context)
        except Exception:
            degraded = True
            warnings.append("叙事模型不可用，已降级为规则叙事。")
            self._audit(audit_id, "claims", planner, analyzer, True, "narrative_failure", logging.WARNING)
            analyzer = self._fallback_analyzer
            try:
                narrative = self._render_selected_claims(analyzer, context)
            except Exception:
                self._audit(audit_id, "narrative", planner, analyzer, True, "narrative_failure", logging.ERROR)
                raise ToolAnalysisUnavailableError("tool analysis is unavailable") from None

        self._audit(audit_id, "complete", planner, analyzer, degraded, None, logging.INFO)
        return ToolAnalysisResponse(
            **narrative.model_dump(),
            evidence=execution.evidence,
            tool_calls=execution.tool_calls,
            warnings=warnings,
            planner=self._safe_name(planner),
            analyzer=self._safe_name(analyzer),
            degraded=degraded,
            audit_id=audit_id,
            generated_at=context.generated_at,
        )

    @staticmethod
    def _validated_plan(plan: ToolPlan) -> ToolPlan:
        if not isinstance(plan, ToolPlan):
            raise ValueError("tool plan is invalid")
        return ToolPlan.model_validate(plan.model_dump())

    @staticmethod
    def _validated_selection(selection: ToolAnalysisSelection) -> ToolAnalysisSelection:
        if not isinstance(selection, ToolAnalysisSelection):
            raise ValueError("tool claim selection is invalid")
        return ToolAnalysisSelection.model_validate(selection.model_dump())

    def _render_selected_claims(
        self,
        analyzer: ToolNarrativeAnalyzer,
        context: ToolAnalysisContext,
    ) -> AnalysisNarrative:
        selection = self._validated_selection(analyzer.select(context))
        narrative = render_tool_selection(selection, context)
        validate_narrative_numbers(narrative, allowed_tool_narrative_numbers(context))
        return narrative

    def _new_audit_id(self) -> UUID:
        try:
            audit_id = self._audit_id_factory()
        except Exception:
            raise ToolAnalysisUnavailableError("tool analysis is unavailable") from None
        if not isinstance(audit_id, UUID):
            raise ToolAnalysisUnavailableError("tool analysis is unavailable")
        return audit_id

    @staticmethod
    def _safe_name(component: object | None) -> str:
        name = getattr(component, "name", None)
        return name if name in {"rule_based", "openai_compatible"} else "redacted"

    def _audit(
        self,
        audit_id: UUID,
        stage: str,
        planner: ToolPlanner,
        analyzer: ToolNarrativeAnalyzer | None,
        degraded: bool,
        error_type: str | None,
        level: int,
    ) -> None:
        LOGGER.log(
            level,
            "tool analysis event",
            extra={
                "audit_id": str(audit_id),
                "stage": stage,
                "planner": self._safe_name(planner),
                "analyzer": self._safe_name(analyzer),
                "degraded": degraded,
                "error_type": error_type,
            },
        )
