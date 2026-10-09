from __future__ import annotations

from datetime import date
import json
from time import perf_counter
from typing import Annotated, Any, Literal, Self
from uuid import uuid4

from langchain_core.tools import BaseTool, StructuredTool
from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

from app.agent_models import ToolBudget, ToolEvidence, ToolTrace
from app.auth_service import Principal
from app.behavior_service import BehaviorMetricsService
from app.knowledge_search import KnowledgeSearchService
from app.order_repository import RANK_SORT_COLUMNS
from app.order_service import OrderMetricsService


class _StrictArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SearchKnowledgeArgs(_StrictArgs):
    query: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=500)]
    limit: int = Field(default=5, strict=True, ge=1, le=5)


class BehaviorMetricsArgs(_StrictArgs):
    view: Literal["overview", "funnel", "rankings", "quality"]
    window: Literal["day", "full"] = "full"
    dimension: Literal["product", "category", "brand"] | None = None
    sort_by: Literal["views", "carts", "purchases", "users", "amount"] = "purchases"
    limit: int = Field(default=10, strict=True, ge=1, le=10)

    @model_validator(mode="after")
    def validate_shape(self) -> Self:
        if self.view == "rankings":
            if self.dimension is None:
                raise ValueError("rankings require a dimension")
        elif self.dimension is not None or self.sort_by != "purchases":
            raise ValueError("dimension and sort_by require rankings")
        if self.view == "quality" and self.window != "full":
            raise ValueError("quality requires full window")
        return self


class OrderMetricsArgs(_StrictArgs):
    view: Literal["overview", "delivery", "payments", "rankings", "reviews", "quality"]
    window: Literal["day", "month", "full"] = "full"
    start_date: date | None = None
    end_date: date | None = None
    dimension: Literal["product", "category", "seller", "customer_state", "seller_state"] | None = None
    sort_by: Literal["order_count", "item_value", "freight_value", "payment_value", "late_rate"] = "order_count"
    limit: int = Field(default=10, strict=True, ge=1, le=10)

    @model_validator(mode="after")
    def validate_shape(self) -> Self:
        if (self.start_date is None) != (self.end_date is None):
            raise ValueError("start_date and end_date must be provided together")
        if self.start_date is not None and (
            self.window == "full" or self.start_date > self.end_date
        ):
            raise ValueError("invalid order date range")
        if self.view == "quality" and (self.window != "full" or self.start_date is not None):
            raise ValueError("quality requires full window")
        if self.view == "rankings":
            if self.dimension is None or self.sort_by not in RANK_SORT_COLUMNS[self.dimension]:
                raise ValueError("unsupported ranking dimension/sort combination")
        elif self.dimension is not None or self.sort_by != "order_count":
            raise ValueError("dimension and sort_by require rankings")
        return self


# Only these aggregate fields may enter model-visible metric evidence.
_WINDOW_FIELDS = ("window_type", "window_start", "window_end")
_BEHAVIOR_FIELDS = {
    "overview": (
        *_WINDOW_FIELDS, "event_count", "view_count", "cart_count", "purchase_count",
        "unique_user_count", "session_count", "product_count", "purchase_amount_proxy",
    ),
    "funnel": (
        *_WINDOW_FIELDS, "missing_session_event_count", "view_sessions",
        "view_to_cart_sessions", "completed_sessions", "view_to_cart_rate",
        "cart_to_purchase_rate", "full_conversion_rate",
    ),
    "rankings": (
        *_WINDOW_FIELDS, "dimension_type", "dimension_id", "dimension_name", "is_unknown",
        "view_count", "cart_count", "purchase_count", "unique_user_count",
        "purchase_amount_proxy",
    ),
    "quality": (
        *_WINDOW_FIELDS, "source_event_count", "clean_event_count", "late_event_count",
        "clean_event_rate", "late_event_rate", "distinct_event_count",
        "duplicate_event_count", "missing_session_count", "unknown_category_count",
        "unknown_brand_count", "invalid_event_type_count", "empty_key_id_count",
        "invalid_price_count", "invalid_derived_date_count", "overview_event_count",
        "reconciliation_status",
    ),
}
_ORDER_FIELDS = {
    "overview": (
        *_WINDOW_FIELDS, "order_count", "delivered_order_count", "canceled_order_count",
        "unavailable_order_count", "delivered_rate", "canceled_rate",
        "unique_customer_count", "repeat_customer_count", "repeat_customer_rate",
        "item_value_sum", "freight_value_sum", "payment_value_sum",
    ),
    "delivery": (
        *_WINDOW_FIELDS, "delivery_eligible_order_count", "delivery_days_avg",
        "delivery_days_p50", "delivery_days_p90", "late_delivery_order_count",
        "late_delivery_eligible_order_count", "late_delivery_rate",
    ),
    "payments": (
        *_WINDOW_FIELDS, "payment_type", "is_all", "global_order_count",
        "payment_order_count", "payment_row_count", "installment_order_count",
        "payment_value_sum",
    ),
    "rankings": (
        *_WINDOW_FIELDS, "dimension_type", "dimension_id", "dimension_name", "is_unknown",
        "ranking_order_count", "ranking_item_row_count", "ranking_customer_count",
        "ranking_item_value_sum", "ranking_freight_value_sum", "ranking_payment_value_sum",
        "ranking_late_delivery_rate", "payment_value_is_additive",
    ),
    "reviews": (
        *_WINDOW_FIELDS, "review_row_count", "reviewed_order_count", "all_order_count",
        "review_coverage_rate", "review_score_avg", "low_score_order_count",
        "low_score_rate", "multi_review_order_count",
    ),
    "quality": (
        *_WINDOW_FIELDS, "source_row_count", "iceberg_row_count", "duplicate_key_count",
        "orphan_key_count", "invalid_value_count", "temporal_anomaly_count",
        "amount_comparable_order_count", "amount_reconciled_order_count",
        "amount_mismatch_order_count", "amount_reconciliation_rate", "reconciliation_status",
    ),
}
_BEHAVIOR_TARGETS = {
    "overview": "/behavior", "funnel": "/behavior",
    "rankings": "/rankings", "quality": "/quality",
}
_ORDER_TARGETS = {
    "overview": "/orders", "delivery": "/fulfillment", "payments": "/orders",
    "rankings": "/rankings", "reviews": "/fulfillment", "quality": "/quality",
}
_METRIC_META = (
    "dataset_id", "metric_version", "metric_run_id", "window_start", "window_end",
    "calculated_at", "warnings",
)
_BEHAVIOR_META = (
    *_METRIC_META, "source_table", "source_snapshot_id", "data_scope", "source_event_count",
)
_ORDER_META = (
    *_METRIC_META, "source_bundle_sha256", "source_timezone", "source_currency",
    "source_order_count",
)


def _encoded(evidence: list[ToolEvidence]) -> str:
    return json.dumps([item.model_dump(mode="json") for item in evidence], ensure_ascii=False)


def _metric_evidence(
    response: Any, *, kind: Literal["behavior", "orders"], view: str,
    window: str, budget: ToolBudget,
) -> list[ToolEvidence]:
    points = response.data if isinstance(response.data, list) else [response.data]
    if not points:
        return []
    fields = (_BEHAVIOR_FIELDS if kind == "behavior" else _ORDER_FIELDS)[view]
    if len(points) <= 10:
        selected, selection = points, "all"
    elif view == "rankings":
        selected, selection = points[:10], "top_10_ranked"
    else:
        selected = points[-10:]
        selection = "latest_10_windows" if view != "payments" else "latest_10_rows"
    rows = []
    for point in selected:
        full = point.model_dump(mode="json")
        rows.append({field: full[field] for field in fields if field in full})
    raw_meta = response.meta.model_dump(mode="json")
    meta_fields = _BEHAVIOR_META if kind == "behavior" else _ORDER_META
    meta = {field: raw_meta[field] for field in meta_fields if field in raw_meta}
    meta.update({
        "view": view, "requested_window": window, "total_rows": len(points),
        "truncated": len(points) > 10, "selection": selection,
    })
    evidence = ToolEvidence(
        evidence_id=f"ev-{uuid4().hex}", kind=kind, meta=meta, rows=rows,
        target=(_BEHAVIOR_TARGETS if kind == "behavior" else _ORDER_TARGETS)[view],
    )
    budget.record(evidence)
    return [evidence]


def build_tools(
    principal: Principal,
    knowledge: KnowledgeSearchService,
    behavior: BehaviorMetricsService,
    orders: OrderMetricsService,
    budget: ToolBudget,
) -> list[BaseTool]:
    if principal.role not in {"admin", "analyst"}:
        raise PermissionError("agent tools require analyst or admin")

    def traced(name: str, operation):
        def invoke(**kwargs: Any) -> str:
            started = perf_counter()
            try:
                result = operation(**kwargs)
            except Exception:
                budget.record_trace(ToolTrace(
                    name=name, status="error", elapsed_ms=(perf_counter() - started) * 1000,
                ))
                raise
            budget.record_trace(ToolTrace(
                name=name, status="ok", elapsed_ms=(perf_counter() - started) * 1000,
                evidence_ids=[item["evidence_id"] for item in json.loads(result)],
            ))
            return result
        return invoke

    def search_knowledge(**kwargs: Any) -> str:
        args = SearchKnowledgeArgs.model_validate(kwargs)
        budget.consume()
        limit = min(args.limit, budget.remaining_knowledge_excerpts())
        if not limit:
            return "[]"
        result = knowledge.search(args.query, principal, limit=limit)
        evidence = []
        for hit in result.hits[:limit]:
            item = ToolEvidence(
                evidence_id=f"ev-{uuid4().hex}", kind="knowledge",
                meta={
                    "chunk_id": str(hit.chunk_id), "document_id": str(hit.document_id),
                    "version_id": str(hit.version_id), "section": hit.section,
                    "page": hit.page, "source_label": hit.source_label,
                    "source_ref": hit.source_ref, "search_mode": result.mode,
                },
                text=hit.text[:500], target=hit.locator,
            )
            evidence.append(item)
        budget.record_many(evidence)
        return _encoded(evidence)

    def get_behavior(**kwargs: Any) -> str:
        args = BehaviorMetricsArgs.model_validate(kwargs)
        budget.consume()
        if args.view == "quality":
            response = behavior.get_quality()
        elif args.view == "rankings":
            response = behavior.get_rankings(args.dimension, args.window, args.sort_by, args.limit)
        elif args.view == "funnel":
            response = behavior.get_funnel(args.window)
        else:
            response = behavior.get_overview(args.window)
        return _encoded(_metric_evidence(
            response, kind="behavior", view=args.view, window=args.window, budget=budget,
        ))

    def get_orders(**kwargs: Any) -> str:
        args = OrderMetricsArgs.model_validate(kwargs)
        budget.consume()
        if args.view == "quality":
            response = orders.get_quality()
        elif args.view == "rankings":
            response = orders.get_rankings(
                args.dimension, args.window, args.start_date, args.end_date,
                args.sort_by, args.limit,
            )
        else:
            method = {
                "overview": orders.get_overview,
                "delivery": orders.get_delivery,
                "payments": orders.get_payments,
                "reviews": orders.get_reviews,
            }[args.view]
            response = method(args.window, args.start_date, args.end_date)
        return _encoded(_metric_evidence(
            response, kind="orders", view=args.view, window=args.window, budget=budget,
        ))

    return [
        StructuredTool.from_function(
            func=traced("search_knowledge", search_knowledge), name="search_knowledge",
            description="Search up to five authorized published knowledge excerpts for a question.",
            args_schema=SearchKnowledgeArgs,
        ),
        StructuredTool.from_function(
            func=traced("get_published_behavior_metrics", get_behavior),
            name="get_published_behavior_metrics",
            description="Read published REES46 aggregate behavior metrics; no row-level events or SQL.",
            args_schema=BehaviorMetricsArgs,
        ),
        StructuredTool.from_function(
            func=traced("get_published_order_metrics", get_orders),
            name="get_published_order_metrics",
            description="Read published Olist aggregate order metrics; no row-level orders or SQL.",
            args_schema=OrderMetricsArgs,
        ),
    ]
