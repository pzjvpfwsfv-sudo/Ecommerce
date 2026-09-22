from __future__ import annotations

import json
from pathlib import Path
from typing import Literal

from pydantic import ConfigDict, Field, StrictBool

from app.order_models import NonBlankString, StrictOrderModel


OrderSourceField = Literal[
    "amount fields",
    "curated hard gate",
    "curated keys",
    "customer_id",
    "customer_state",
    "customer_unique_id",
    "dimension_id",
    "dimension_type",
    "freight_value",
    "is_known_status",
    "is_lifecycle_order_valid",
    "item_value",
    "manifest.files.orders.row_count",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
    "order_fact_snapshot",
    "order_id",
    "order_item_id",
    "order_purchase_timestamp",
    "order_status",
    "payment_installments",
    "payment_sequential",
    "payment_type",
    "payment_value",
    "product_id",
    "purchase_date",
    "review_id",
    "review_score",
    "seller_id",
    "seller_state",
    "valid_review_score",
]

REQUIRED_ORDER_METRICS = frozenset(
    {
        "order_count",
        "delivered_order_count",
        "canceled_order_count",
        "unavailable_order_count",
        "status_eligible_order_count",
        "status_excluded_order_count",
        "delivered_rate",
        "canceled_rate",
        "unique_customer_count",
        "repeat_customer_count",
        "repeat_customer_rate",
        "item_value_sum",
        "freight_value_sum",
        "payment_value_sum",
        "items_per_order_avg",
        "delivery_eligible_order_count",
        "delivery_days_avg",
        "delivery_days_p50",
        "delivery_days_p90",
        "late_delivery_order_count",
        "late_delivery_rate",
        "payment_row_count",
        "payment_order_count",
        "installment_order_count",
        "payment_type_order_count",
        "payment_type_value_sum",
        "ranking_order_count",
        "ranking_item_row_count",
        "ranking_customer_count",
        "ranking_item_value_sum",
        "ranking_freight_value_sum",
        "ranking_payment_value_sum",
        "ranking_late_delivery_order_count",
        "ranking_late_delivery_rate",
        "review_row_count",
        "reviewed_order_count",
        "review_coverage_rate",
        "review_score_avg",
        "low_score_order_count",
        "low_score_rate",
        "multi_review_order_count",
        "source_row_count",
        "iceberg_row_count",
        "duplicate_key_count",
        "orphan_key_count",
        "invalid_value_count",
        "temporal_anomaly_count",
        "amount_comparable_order_count",
        "amount_reconciled_order_count",
        "amount_mismatch_order_count",
        "amount_reconciliation_rate",
        "payment_item_freight_abs_difference_avg",
        "payment_item_freight_abs_difference_p50",
        "payment_item_freight_abs_difference_p90",
    }
)

_MONETARY_CLAIMS = frozenset({"利润", "净收入", "审计 GMV", "退款后收入", "币种换算"})
_EXACT_FORBIDDEN_CLAIMS = {
    "canceled_order_count": frozenset({"退款订单金额"}),
    "item_value_sum": _MONETARY_CLAIMS,
    "freight_value_sum": _MONETARY_CLAIMS,
    "payment_value_sum": _MONETARY_CLAIMS,
    "items_per_order_avg": frozenset({"件数均值"}),
    "payment_order_count": frozenset({"结算成功订单数"}),
    "payment_type_value_sum": _MONETARY_CLAIMS,
    "ranking_item_row_count": frozenset({"商品件数"}),
    "ranking_item_value_sum": _MONETARY_CLAIMS,
    "ranking_freight_value_sum": _MONETARY_CLAIMS,
    "ranking_payment_value_sum": _MONETARY_CLAIMS,
    "payment_item_freight_abs_difference_avg": _MONETARY_CLAIMS,
    "payment_item_freight_abs_difference_p50": _MONETARY_CLAIMS,
    "payment_item_freight_abs_difference_p90": _MONETARY_CLAIMS,
}


class OrderMetricDefinition(StrictOrderModel):
    metric_name: NonBlankString
    display_name: NonBlankString
    formula: NonBlankString
    numerator: NonBlankString
    denominator: NonBlankString | None
    source_fields: list[OrderSourceField] = Field(min_length=1)
    allowed_windows: list[Literal["DAY", "MONTH", "FULL"]] = Field(min_length=1)
    additive: StrictBool
    null_policy: NonBlankString
    exclusions: list[NonBlankString]
    limitations: list[NonBlankString] = Field(min_length=1)
    forbidden_claims: list[NonBlankString]


class OrderMetricDefinitionsResponse(StrictOrderModel):
    domain: Literal["orders"]
    dataset_id: Literal["olist-brazilian-ecommerce-v2"]
    metric_version: Literal["orders-v1"]
    definitions: list[OrderMetricDefinition]


class _OrderMetricDefinitionDocument(OrderMetricDefinitionsResponse):
    model_config = ConfigDict(extra="forbid")


class OrderMetricDefinitionCatalog:
    def __init__(self, document: _OrderMetricDefinitionDocument) -> None:
        self.domain = document.domain
        self.dataset_id = document.dataset_id
        self.metric_version = document.metric_version
        self._definitions = tuple(document.definitions)
        self._by_name = {definition.metric_name: definition for definition in self._definitions}

    @classmethod
    def load(cls, path: str | Path) -> OrderMetricDefinitionCatalog:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        document = _OrderMetricDefinitionDocument.model_validate(payload)
        names = [definition.metric_name for definition in document.definitions]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ValueError(f"duplicate metric names: {', '.join(duplicates)}")
        if set(names) != REQUIRED_ORDER_METRICS:
            raise ValueError("catalog must contain the exact public order metric set")
        for definition in document.definitions:
            expected_claims = _EXACT_FORBIDDEN_CLAIMS.get(definition.metric_name, frozenset())
            actual_claims = frozenset(definition.forbidden_claims)
            if len(definition.forbidden_claims) != len(actual_claims) or actual_claims != expected_claims:
                raise ValueError(f"{definition.metric_name} forbidden claims must be exact")
        seller_state = next(
            definition
            for definition in document.definitions
            if definition.metric_name == "ranking_payment_value_sum"
        )
        limitation_text = " ".join(seller_state.limitations).lower()
        if seller_state.additive or "seller_state" not in limitation_text or "non-additive" not in limitation_text:
            raise ValueError("ranking_payment_value_sum must document seller_state non-additivity")
        return cls(document)

    def get(self, name: str) -> OrderMetricDefinition:
        return self._by_name[name]

    def all(self) -> list[OrderMetricDefinition]:
        return list(self._definitions)
