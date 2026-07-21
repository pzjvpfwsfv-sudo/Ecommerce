from __future__ import annotations

from datetime import datetime
import unicodedata

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class AnalysisRequest(BaseModel):
    question: str

    @field_validator("question")
    @classmethod
    def normalize_question(cls, value: str) -> str:
        normalized = unicodedata.normalize("NFKC", value)
        if any(unicodedata.category(character).startswith("C") for character in normalized):
            raise ValueError("question contains an invisible control character")
        normalized = normalized.strip()
        if not normalized or not any(
            not character.isspace() and unicodedata.category(character) not in {"Mn", "Me"}
            for character in normalized
        ):
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


class AnalysisSelection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    summary: Literal["realtime_overview", "realtime_incomplete"]
    insights: list[
        Literal["visits_per_user", "historical_event_count", "top_event_type_share"]
    ]
    risks: list[Literal["cumulative_metric_limit", "zero_uv"]]
    actions: list[Literal["add_time_window_metrics"]]


class AnalysisResponse(AnalysisNarrative):
    evidence: AnalysisEvidence
    warnings: list[str] = Field(default_factory=list)
    analyzer: str
    generated_at: datetime
