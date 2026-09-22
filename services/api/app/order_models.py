from __future__ import annotations

from datetime import date, datetime
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StringConstraints, model_validator


Sha256String = Annotated[str, Field(strict=True, pattern=r"^[0-9a-f]{64}$")]
PositiveSnapshotId = Annotated[str, Field(strict=True, pattern=r"^[1-9][0-9]*$")]
NonBlankString = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
MoneyString = Annotated[str, Field(strict=True, pattern=r"^[0-9]+\.[0-9]{2}$")]
DecimalSixString = Annotated[str, Field(strict=True, pattern=r"^[0-9]+\.[0-9]{6}$")]
NonNegativeInt = Annotated[int, Field(strict=True, ge=0)]
PositiveInt = Annotated[int, Field(strict=True, gt=0)]
OrderWindowType = Literal["DAY", "MONTH", "FULL"]
OrderDimensionType = Literal["product", "category", "seller", "customer_state", "seller_state"]

SOURCE_SNAPSHOT_TABLES = frozenset(
    {
        "orders_src_v1",
        "order_items_src_v1",
        "order_payments_src_v1",
        "order_reviews_src_v1",
        "customers_src_v1",
        "products_src_v1",
        "sellers_src_v1",
        "geolocation_src_v1",
        "category_translation_src_v1",
    }
)
CURATED_SNAPSHOT_TABLES = frozenset(
    {
        "customer_dim_v1",
        "category_dim_v1",
        "product_dim_v1",
        "seller_dim_v1",
        "geolocation_dim_v1",
        "order_fact_v1",
        "order_item_fact_v1",
        "payment_fact_v1",
        "review_fact_v1",
    }
)


def _require_snapshot_keys(values: dict[str, str], expected: frozenset[str], label: str) -> None:
    if set(values) != expected:
        raise ValueError(f"{label} must contain the exact snapshot table set")


class StrictOrderModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class OrderMetricMeta(StrictOrderModel):
    dataset_id: Literal["olist-brazilian-ecommerce-v2"]
    metric_version: Literal["orders-v1"]
    metric_run_id: str = Field(pattern=r"^orders-v1-b[0-9a-f]{64}$")
    source_bundle_sha256: Sha256String
    source_snapshots: dict[str, PositiveSnapshotId]
    curated_snapshots: dict[str, PositiveSnapshotId]
    window_start: date
    window_end: date
    source_timezone: Literal["unspecified"]
    source_currency: NonBlankString | None
    source_order_count: PositiveInt
    calculated_at: datetime
    implementation_revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    warnings: list[NonBlankString] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        _require_snapshot_keys(self.source_snapshots, SOURCE_SNAPSHOT_TABLES, "source_snapshots")
        _require_snapshot_keys(self.curated_snapshots, CURATED_SNAPSHOT_TABLES, "curated_snapshots")
        if self.window_start > self.window_end:
            raise ValueError("window_start must not be after window_end")
        return self


class OrderOverviewPoint(StrictOrderModel):
    window_type: OrderWindowType
    window_start: date
    window_end: date
    order_count: NonNegativeInt
    delivered_order_count: NonNegativeInt
    canceled_order_count: NonNegativeInt
    unavailable_order_count: NonNegativeInt
    status_eligible_order_count: NonNegativeInt
    status_excluded_order_count: NonNegativeInt
    delivered_rate: DecimalSixString | None
    canceled_rate: DecimalSixString | None
    unique_customer_count: NonNegativeInt
    repeat_customer_count: NonNegativeInt
    repeat_customer_rate: DecimalSixString | None
    item_row_count: NonNegativeInt
    item_value_sum: MoneyString
    freight_value_sum: MoneyString
    payment_value_sum: MoneyString
    items_per_order_avg: DecimalSixString | None


class OrderDeliveryPoint(StrictOrderModel):
    window_type: OrderWindowType
    window_start: date
    window_end: date
    delivery_eligible_order_count: NonNegativeInt
    delivery_excluded_order_count: NonNegativeInt
    delivery_days_avg: DecimalSixString | None
    delivery_days_p50: DecimalSixString | None
    delivery_days_p90: DecimalSixString | None
    late_delivery_order_count: NonNegativeInt
    late_delivery_eligible_order_count: NonNegativeInt
    late_delivery_excluded_order_count: NonNegativeInt
    late_delivery_rate: DecimalSixString | None


class OrderPaymentPoint(StrictOrderModel):
    window_type: OrderWindowType
    window_start: date
    window_end: date
    payment_type: NonBlankString
    is_all: StrictBool
    global_order_count: NonNegativeInt
    payment_order_count: NonNegativeInt
    payment_row_count: NonNegativeInt
    installment_order_count: NonNegativeInt
    payment_value_sum: MoneyString


class OrderRankingPoint(StrictOrderModel):
    window_type: OrderWindowType
    window_start: date
    window_end: date
    dimension_type: OrderDimensionType
    dimension_id: NonBlankString
    dimension_name: NonBlankString
    is_unknown: StrictBool
    ranking_order_count: NonNegativeInt
    ranking_item_row_count: NonNegativeInt | None
    ranking_customer_count: NonNegativeInt
    ranking_item_value_sum: MoneyString | None
    ranking_freight_value_sum: MoneyString | None
    ranking_payment_value_sum: MoneyString | None
    ranking_late_delivery_order_count: NonNegativeInt | None
    ranking_late_delivery_eligible_order_count: NonNegativeInt | None
    ranking_late_delivery_rate: DecimalSixString | None
    payment_value_is_additive: StrictBool | None

    @model_validator(mode="after")
    def validate_payment_additivity(self) -> Self:
        expected = {
            "customer_state": True,
            "seller_state": False,
            "product": None,
            "category": None,
            "seller": None,
        }[self.dimension_type]
        if self.payment_value_is_additive is not expected:
            raise ValueError("payment_value_is_additive does not match the ranking dimension")
        return self


class OrderReviewPoint(StrictOrderModel):
    window_type: OrderWindowType
    window_start: date
    window_end: date
    review_row_count: NonNegativeInt
    reviewed_order_count: NonNegativeInt
    all_order_count: NonNegativeInt
    review_coverage_rate: DecimalSixString | None
    review_score_avg: DecimalSixString | None
    low_score_order_count: NonNegativeInt
    low_score_rate: DecimalSixString | None
    multi_review_order_count: NonNegativeInt


class OrderQualityMetrics(StrictOrderModel):
    window_type: Literal["FULL"]
    window_start: date
    window_end: date
    source_row_count: NonNegativeInt
    iceberg_row_count: NonNegativeInt
    duplicate_key_count: NonNegativeInt
    orphan_key_count: NonNegativeInt
    invalid_value_count: NonNegativeInt
    temporal_anomaly_count: NonNegativeInt
    amount_comparable_order_count: NonNegativeInt
    amount_reconciled_order_count: NonNegativeInt
    amount_mismatch_order_count: NonNegativeInt
    amount_reconciliation_rate: DecimalSixString | None
    payment_item_freight_abs_difference_avg: DecimalSixString | None
    payment_item_freight_abs_difference_p50: DecimalSixString | None
    payment_item_freight_abs_difference_p90: DecimalSixString | None
    raw_row_counts: dict[NonBlankString, NonNegativeInt]
    normalized_row_counts: dict[NonBlankString, NonNegativeInt]
    iceberg_row_counts: dict[NonBlankString, NonNegativeInt]
    normalized_sha256: dict[NonBlankString, Sha256String]
    source_snapshots: dict[str, PositiveSnapshotId]
    curated_snapshots: dict[str, PositiveSnapshotId]
    fact_reconciliations: dict[NonBlankString, NonNegativeInt]
    reportable_quality: dict[NonBlankString, NonNegativeInt]
    reconciliation_status: Literal["PASS"]

    @model_validator(mode="after")
    def validate_snapshot_sets(self) -> Self:
        _require_snapshot_keys(self.source_snapshots, SOURCE_SNAPSHOT_TABLES, "source_snapshots")
        _require_snapshot_keys(self.curated_snapshots, CURATED_SNAPSHOT_TABLES, "curated_snapshots")
        return self


class OrderPublicationData(StrictOrderModel):
    published_at: datetime
    overview_row_count: NonNegativeInt
    overview_sha256: Sha256String
    delivery_row_count: NonNegativeInt
    delivery_sha256: Sha256String
    payment_row_count: NonNegativeInt
    payment_sha256: Sha256String
    ranking_row_count: NonNegativeInt
    ranking_sha256: Sha256String
    review_row_count: NonNegativeInt
    review_sha256: Sha256String
    quality_row_count: NonNegativeInt
    quality_sha256: Sha256String
    status: Literal["PUBLISHED"]


class OrderPublicationResponse(StrictOrderModel):
    meta: OrderMetricMeta
    data: OrderPublicationData


class OrderOverviewResponse(StrictOrderModel):
    meta: OrderMetricMeta
    data: list[OrderOverviewPoint]


class OrderDeliveryResponse(StrictOrderModel):
    meta: OrderMetricMeta
    data: list[OrderDeliveryPoint]


class OrderPaymentsResponse(StrictOrderModel):
    meta: OrderMetricMeta
    data: list[OrderPaymentPoint]


class OrderRankingsResponse(StrictOrderModel):
    meta: OrderMetricMeta
    data: list[OrderRankingPoint]


class OrderReviewsResponse(StrictOrderModel):
    meta: OrderMetricMeta
    data: list[OrderReviewPoint]


class OrderQualityResponse(StrictOrderModel):
    meta: OrderMetricMeta
    data: OrderQualityMetrics
