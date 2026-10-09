from __future__ import annotations

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


class ToolBudget:
    def __init__(self) -> None:
        self.calls = 0
        self.evidence: list[ToolEvidence] = []

    def consume(self) -> None:
        if self.calls >= 5:
            raise RuntimeError("agent tool call budget exceeded")
        self.calls += 1

    def record(self, evidence: ToolEvidence) -> None:
        self.record_many([evidence])

    def record_many(self, items: list[ToolEvidence]) -> None:
        existing = {item.evidence_id for item in self.evidence}
        incoming = [item.evidence_id for item in items]
        if len(set(incoming)) != len(incoming) or existing.intersection(incoming):
            raise ValueError("duplicate evidence ID")
        self.evidence.extend(items)

    def remaining_knowledge_excerpts(self) -> int:
        return max(0, 5 - sum(item.kind == "knowledge" for item in self.evidence))
