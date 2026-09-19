from __future__ import annotations

import json
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict

from app.behavior_models import MetricDefinition


REQUIRED_PUBLIC_METRICS = frozenset(
    {
        "event_count",
        "view_count",
        "cart_count",
        "purchase_count",
        "unique_user_count",
        "session_count",
        "product_count",
        "purchase_amount_proxy",
        "view_sessions",
        "view_to_cart_sessions",
        "completed_sessions",
        "view_to_cart_rate",
        "cart_to_purchase_rate",
        "full_conversion_rate",
        "clean_event_count",
        "late_event_count",
        "distinct_event_count",
        "duplicate_event_count",
        "missing_session_count",
        "unknown_category_count",
        "unknown_brand_count",
        "invalid_event_type_count",
        "empty_key_id_count",
        "invalid_price_count",
        "invalid_derived_date_count",
    }
)
PROXY_FORBIDDEN_CLAIMS = frozenset({"GMV", "销售额", "收入"})


class _MetricDefinitionDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    domain: Literal["behavior"]
    dataset_id: Literal["rees46-multicategory"]
    metric_version: Literal["behavior-v1"]
    definitions: list[MetricDefinition]


class MetricDefinitionCatalog:
    def __init__(self, document: _MetricDefinitionDocument) -> None:
        self.domain = document.domain
        self.dataset_id = document.dataset_id
        self.metric_version = document.metric_version
        self._definitions = tuple(document.definitions)
        self._by_name = {definition.metric_name: definition for definition in self._definitions}

    @classmethod
    def load(cls, path: str | Path) -> MetricDefinitionCatalog:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        document = _MetricDefinitionDocument.model_validate(payload)
        names = [definition.metric_name for definition in document.definitions]
        duplicate_names = sorted({name for name in names if names.count(name) > 1})
        if duplicate_names:
            raise ValueError(f"duplicate metric names: {', '.join(duplicate_names)}")
        if set(names) != REQUIRED_PUBLIC_METRICS:
            raise ValueError("catalog must contain the exact public metric set")
        proxy = next(
            definition
            for definition in document.definitions
            if definition.metric_name == "purchase_amount_proxy"
        )
        if set(proxy.forbidden_claims) != PROXY_FORBIDDEN_CLAIMS:
            raise ValueError("purchase_amount_proxy forbidden claims must be exact")
        return cls(document)

    def get(self, name: str) -> MetricDefinition:
        return self._by_name[name]

    def all(self) -> list[MetricDefinition]:
        return list(self._definitions)
