from __future__ import annotations

from datetime import date, datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints


DecimalString = Annotated[str, Field(pattern=r"^[0-9]+\.[0-9]{2,6}$")]
Sha256String = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
NonBlankString = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class StrictBehaviorModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class BehaviorMetricMeta(StrictBehaviorModel):
    dataset_id: Literal["rees46-multicategory"]
    metric_version: Literal["behavior-v1"]
    metric_run_id: str = Field(pattern=r"^behavior-v1-s[1-9][0-9]*$")
    source_snapshot_id: str = Field(pattern=r"^[1-9][0-9]*$")
    window_start: date
    window_end: date
    calculated_at: datetime
    data_scope: Literal["g2c-correctness-subset", "stable-user-2pct-full"]
    source_event_count: int = Field(gt=0)
    warnings: list[str]


class OverviewPoint(StrictBehaviorModel):
    window_type: Literal["DAY", "FULL"]
    window_start: date
    window_end: date
    event_count: int = Field(ge=0)
    view_count: int = Field(ge=0)
    cart_count: int = Field(ge=0)
    purchase_count: int = Field(ge=0)
    unique_user_count: int = Field(ge=0)
    session_count: int = Field(ge=0)
    product_count: int = Field(ge=0)
    purchase_amount_proxy: DecimalString | None


class FunnelPoint(StrictBehaviorModel):
    window_type: Literal["DAY", "FULL"]
    window_start: date
    window_end: date
    missing_session_event_count: int = Field(ge=0)
    view_sessions: int = Field(ge=0)
    view_to_cart_sessions: int = Field(ge=0)
    completed_sessions: int = Field(ge=0)
    view_to_cart_rate: DecimalString | None
    cart_to_purchase_rate: DecimalString | None
    full_conversion_rate: DecimalString | None


class DimensionRanking(StrictBehaviorModel):
    window_type: Literal["DAY", "FULL"]
    window_start: date
    window_end: date
    dimension_type: Literal["product", "category", "brand"]
    dimension_id: str
    dimension_name: str | None
    is_unknown: bool
    view_count: int = Field(ge=0)
    cart_count: int = Field(ge=0)
    purchase_count: int = Field(ge=0)
    unique_user_count: int = Field(ge=0)
    purchase_amount_proxy: DecimalString | None


class QualityMetrics(StrictBehaviorModel):
    window_type: Literal["FULL"]
    window_start: date
    window_end: date
    source_event_count: int = Field(ge=0)
    clean_event_count: int = Field(ge=0)
    late_event_count: int = Field(ge=0)
    clean_event_rate: DecimalString | None
    late_event_rate: DecimalString | None
    distinct_event_count: int = Field(ge=0)
    duplicate_event_count: int = Field(ge=0)
    missing_session_count: int = Field(ge=0)
    unknown_category_count: int = Field(ge=0)
    unknown_brand_count: int = Field(ge=0)
    invalid_event_type_count: int = Field(ge=0)
    empty_key_id_count: int = Field(ge=0)
    invalid_price_count: int = Field(ge=0)
    invalid_derived_date_count: int = Field(ge=0)
    overview_event_count: int = Field(ge=0)
    reconciliation_status: Literal["PASS"]


class PublicationData(StrictBehaviorModel):
    published_at: datetime
    overview_row_count: int = Field(ge=0)
    overview_sha256: Sha256String
    funnel_row_count: int = Field(ge=0)
    funnel_sha256: Sha256String
    dimension_row_count: int = Field(ge=0)
    dimension_sha256: Sha256String
    quality_row_count: int = Field(ge=0)
    quality_sha256: Sha256String
    status: Literal["PUBLISHED"]


class PublicationResponse(StrictBehaviorModel):
    meta: BehaviorMetricMeta
    data: PublicationData


class OverviewResponse(StrictBehaviorModel):
    meta: BehaviorMetricMeta
    data: list[OverviewPoint]


class FunnelResponse(StrictBehaviorModel):
    meta: BehaviorMetricMeta
    data: list[FunnelPoint]


class RankingsResponse(StrictBehaviorModel):
    meta: BehaviorMetricMeta
    data: list[DimensionRanking]


class QualityResponse(StrictBehaviorModel):
    meta: BehaviorMetricMeta
    data: QualityMetrics


class MetricDefinition(StrictBehaviorModel):
    metric_name: str
    display_name: str
    formula: str
    numerator: str
    denominator: str | None
    source_fields: list[
        Literal[
            "event_id",
            "event_time",
            "event_type",
            "user_id",
            "user_session",
            "product_id",
            "category_id",
            "category_code",
            "brand",
            "price",
            "price_decimal",
            "quality_route",
            "event_date",
            "event_ts",
            "source_file",
            "source_row_number",
            "lateness_ms",
        ]
    ] = Field(min_length=1)
    allowed_windows: list[Literal["DAY", "FULL"]] = Field(min_length=1)
    additive: bool
    null_policy: str
    limitations: list[NonBlankString] = Field(min_length=1)
    forbidden_claims: list[str]


class MetricDefinitionsResponse(StrictBehaviorModel):
    domain: Literal["behavior"]
    dataset_id: Literal["rees46-multicategory"]
    metric_version: Literal["behavior-v1"]
    definitions: list[MetricDefinition]
