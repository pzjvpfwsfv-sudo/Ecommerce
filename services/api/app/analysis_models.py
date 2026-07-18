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
