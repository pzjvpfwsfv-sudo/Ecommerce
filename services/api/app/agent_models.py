from __future__ import annotations

from threading import Lock
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class ToolEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    evidence_id: str
    kind: Literal["knowledge", "behavior", "orders"]
    meta: dict[str, str | int | bool | list[str] | None]
    text: str | None = Field(default=None, max_length=500)
    rows: list[dict[str, str | int | bool | None]] = Field(default_factory=list, max_length=10)
    target: str


class ToolTrace(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: Literal[
        "search_knowledge", "get_published_behavior_metrics", "get_published_order_metrics"
    ]
    status: Literal["ok", "error"]
    elapsed_ms: float = Field(ge=0)
    evidence_ids: list[str] = Field(default_factory=list)


class Insight(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str = Field(min_length=1, max_length=240)
    evidence_ids: list[str] = Field(min_length=1, max_length=5)


class AgentAnswer(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["answered", "evidence_only", "refused"]
    insights: list[Insight] = Field(default_factory=list, max_length=3)
    evidence: list[ToolEvidence] = Field(default_factory=list)
    trace: list[ToolTrace] = Field(default_factory=list)
    model_name: str | None = None
    retrieval_version: str = "g4-a-hybrid-v1"
    fallback_reason: str | None = None
    fallback_summary: str | None = None


class ToolBudget:
    def __init__(self) -> None:
        self._lock = Lock()
        self.calls = 0
        self.evidence: list[ToolEvidence] = []
        self.trace: list[ToolTrace] = []

    def consume(self) -> None:
        with self._lock:
            if self.calls >= 5:
                raise RuntimeError("agent tool call budget exceeded")
            self.calls += 1

    def record(self, evidence: ToolEvidence) -> None:
        self.record_many([evidence])

    def record_many(self, items: list[ToolEvidence]) -> None:
        with self._lock:
            existing = {item.evidence_id for item in self.evidence}
            incoming = [item.evidence_id for item in items]
            if len(set(incoming)) != len(incoming) or existing.intersection(incoming):
                raise ValueError("duplicate evidence ID")
            if sum(item.kind == "knowledge" for item in (*self.evidence, *items)) > 5:
                raise RuntimeError("knowledge excerpt budget exceeded")
            self.evidence.extend(items)

    def remaining_knowledge_excerpts(self) -> int:
        with self._lock:
            return max(0, 5 - sum(item.kind == "knowledge" for item in self.evidence))

    def record_trace(self, trace: ToolTrace) -> None:
        with self._lock:
            self.trace.append(trace)
