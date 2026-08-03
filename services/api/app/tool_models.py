from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.analysis_models import AnalysisNarrative, HistoricalEvidence, RealtimeEvidence


QUALITY_COUNTERS = frozenset(
    {
        "valid_events_total",
        "dlq_events_total",
        "late_events_total",
        "duplicate_events_total",
        "parse_errors_total",
        "validation_errors_total",
    }
)


class ToolId(StrEnum):
    REALTIME = "get_realtime_metrics"
    HISTORICAL = "get_historical_behavior_summary"
    DATA_QUALITY = "get_data_quality_health"


class ToolCall(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tool_id: ToolId


class ToolPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    calls: list[ToolCall] = Field(min_length=1, max_length=3)

    @model_validator(mode="after")
    def reject_duplicates(self) -> ToolPlan:
        ids = [call.tool_id for call in self.calls]
        if len(ids) != len(set(ids)):
            raise ValueError("tool plan contains duplicate tools")
        return self


class DataQualityEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: str
    job_state: Literal["RUNNING"]
    completed_checkpoints: int = Field(ge=0)
    failed_checkpoints: int = Field(ge=0)
    latest_completed_at: datetime | None
    counters: dict[str, int]

    @field_validator("counters", mode="before")
    @classmethod
    def validate_counters(cls, value: dict[str, int]) -> dict[str, int]:
        if set(value) != QUALITY_COUNTERS:
            raise ValueError("counters must contain exactly the supported quality counters")
        if any(type(count) is not int or count < 0 for count in value.values()):
            raise ValueError("quality counters must be non-negative integers")
        return value


class ToolEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    realtime: RealtimeEvidence | None = None
    historical: HistoricalEvidence | None = None
    data_quality: DataQualityEvidence | None = None

    @model_validator(mode="after")
    def require_evidence(self) -> ToolEvidence:
        if self.realtime is None and self.historical is None and self.data_quality is None:
            raise ValueError("tool evidence must contain at least one partition")
        return self


class ToolCallSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tool_id: ToolId
    status: Literal["success", "failed"]
    duration_ms: float = Field(ge=0)
    error_type: str | None = None


class ToolExecutionResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    evidence: ToolEvidence
    tool_calls: list[ToolCallSummary] = Field(min_length=1, max_length=3)
    warnings: list[str] = Field(default_factory=list)
    degraded: bool = False


class ToolAnalysisContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    question: str
    generated_at: datetime
    evidence: ToolEvidence
    warnings: list[str] = Field(default_factory=list)


class ToolAnalysisSelection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    summary: Literal["realtime_only", "historical_only", "quality_only", "combined"]
    insights: list[
        Literal[
            "visits_per_user",
            "historical_event_count",
            "top_event_type_share",
            "quality_event_counts",
            "checkpoint_status",
        ]
    ]
    risks: list[
        Literal[
            "cumulative_metric_limit",
            "zero_uv",
            "failed_checkpoints",
            "quality_rejections",
            "partial_evidence",
        ]
    ]
    actions: list[
        Literal[
            "add_time_window_metrics",
            "inspect_quality_rejections",
            "inspect_failed_checkpoints",
        ]
    ]

    @field_validator("insights", "risks", "actions")
    @classmethod
    def reject_duplicate_claims(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)):
            raise ValueError("tool analysis selection contains duplicate claims")
        return value


class ToolAnalysisResponse(AnalysisNarrative):
    model_config = ConfigDict(extra="forbid")

    evidence: ToolEvidence
    tool_calls: list[ToolCallSummary] = Field(min_length=1, max_length=3)
    warnings: list[str] = Field(default_factory=list)
    planner: str
    analyzer: str
    degraded: bool
    audit_id: UUID
    generated_at: datetime
