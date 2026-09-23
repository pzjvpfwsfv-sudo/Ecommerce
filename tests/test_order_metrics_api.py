from copy import deepcopy
from datetime import UTC, date, datetime
from decimal import Decimal
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import MagicMock, Mock, patch

from fastapi.testclient import TestClient
from pydantic import ValidationError


ROOT = Path(__file__).resolve().parent.parent
API_ROOT = ROOT / "services" / "api"
DEFINITIONS = ROOT / "configs" / "metrics" / "orders-v1.json"
sys.path.insert(0, str(API_ROOT.resolve()))

from app.config import ApiSettings, load_settings  # noqa: E402
from app.order_metric_definitions import (  # noqa: E402
    REQUIRED_ORDER_METRICS,
    OrderMetricDefinitionCatalog,
    OrderMetricDefinitionsResponse,
)
from app.order_models import (  # noqa: E402
    OrderDeliveryPoint,
    OrderDeliveryResponse,
    OrderMetricMeta,
    OrderOverviewPoint,
    OrderOverviewResponse,
    OrderPaymentPoint,
    OrderPaymentsResponse,
    OrderPublicationData,
    OrderPublicationResponse,
    OrderQualityMetrics,
    OrderQualityResponse,
    OrderRankingPoint,
    OrderRankingsResponse,
    OrderReviewPoint,
    OrderReviewsResponse,
)
from app.dependencies import build_order_metrics_service  # noqa: E402
from app.order_repository import OrderMetricsRepository  # noqa: E402
from app.order_service import (  # noqa: E402
    OrderMetricsRequestError,
    OrderMetricsService,
    OrderMetricsUnavailableError,
)
from app.main import create_app  # noqa: E402


SOURCE_TABLES = {
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
CURATED_TABLES = {
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
SOURCE_ENTITIES = {
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "customers",
    "products",
    "sellers",
    "geolocation",
    "category_translation",
}
FACT_RECONCILIATION_KEYS = {
    "order_fact_expected_count",
    "order_fact_row_count",
    "order_item_fact_expected_count",
    "order_item_fact_row_count",
    "payment_fact_expected_count",
    "payment_fact_row_count",
    "review_fact_expected_count",
    "review_fact_row_count",
}
REPORTABLE_QUALITY_KEYS = {
    "unknown_order_status_count",
    "unknown_payment_type_count",
    "multi_review_order_count",
    "lifecycle_order_anomaly_count",
    "payment_item_total_mismatch_count",
}


def metric_meta(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "dataset_id": "olist-brazilian-ecommerce-v2",
        "metric_version": "orders-v1",
        "metric_run_id": f"orders-v1-b{'a' * 64}",
        "source_bundle_sha256": "a" * 64,
        "source_snapshots": {name: str(index) for index, name in enumerate(sorted(SOURCE_TABLES), 1)},
        "curated_snapshots": {name: str(index) for index, name in enumerate(sorted(CURATED_TABLES), 101)},
        "window_start": "2016-09-04",
        "window_end": "2018-10-17",
        "source_timezone": "unspecified",
        "source_currency": None,
        "source_order_count": 99_441,
        "calculated_at": "2026-09-22T08:00:00Z",
        "implementation_revision": "1" * 40,
        "warnings": ["源数据币种尚未核验，不展示货币符号或执行换算。"],
    }
    value.update(overrides)
    return value


def overview_point(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "window_type": "FULL",
        "window_start": "2016-09-04",
        "window_end": "2018-10-17",
        "order_count": 99_441,
        "delivered_order_count": 96_478,
        "canceled_order_count": 625,
        "unavailable_order_count": 609,
        "status_eligible_order_count": 99_441,
        "status_excluded_order_count": 0,
        "delivered_rate": "0.970203",
        "canceled_rate": "0.006285",
        "unique_customer_count": 96_096,
        "repeat_customer_count": 2_997,
        "repeat_customer_rate": "0.031188",
        "item_row_count": 112_650,
        "item_value_sum": "13591643.70",
        "freight_value_sum": "2251909.54",
        "payment_value_sum": "16008872.12",
        "items_per_order_avg": "1.132838",
    }
    value.update(overrides)
    return value


def delivery_point(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "window_type": "FULL",
        "window_start": "2016-09-04",
        "window_end": "2018-10-17",
        "delivery_eligible_order_count": 96_470,
        "delivery_excluded_order_count": 2_971,
        "delivery_days_avg": "12.503000",
        "delivery_days_p50": "10.000000",
        "delivery_days_p90": "25.000000",
        "late_delivery_order_count": 6_665,
        "late_delivery_eligible_order_count": 96_470,
        "late_delivery_excluded_order_count": 2_971,
        "late_delivery_rate": "0.069088",
    }
    value.update(overrides)
    return value


def payment_point(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "window_type": "FULL",
        "window_start": "2016-09-04",
        "window_end": "2018-10-17",
        "payment_type": "credit_card",
        "is_all": False,
        "global_order_count": 99_441,
        "payment_order_count": 76_795,
        "payment_row_count": 76_795,
        "installment_order_count": 40_000,
        "payment_value_sum": "12542084.19",
    }
    value.update(overrides)
    return value


def ranking_point(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "window_type": "FULL",
        "window_start": "2016-09-04",
        "window_end": "2018-10-17",
        "dimension_type": "customer_state",
        "dimension_id": "SP",
        "dimension_name": "SP",
        "is_unknown": False,
        "ranking_order_count": 41_746,
        "ranking_item_row_count": None,
        "ranking_customer_count": 40_000,
        "ranking_item_value_sum": None,
        "ranking_freight_value_sum": None,
        "ranking_payment_value_sum": "5998226.96",
        "ranking_late_delivery_order_count": 2_200,
        "ranking_late_delivery_eligible_order_count": 40_000,
        "ranking_late_delivery_rate": "0.055000",
        "payment_value_is_additive": True,
    }
    value.update(overrides)
    return value


def review_point(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "window_type": "FULL",
        "window_start": "2016-09-04",
        "window_end": "2018-10-17",
        "review_row_count": 99_224,
        "reviewed_order_count": 98_673,
        "all_order_count": 99_441,
        "review_coverage_rate": "0.992276",
        "review_score_avg": "4.086421",
        "low_score_order_count": 14_000,
        "low_score_rate": "0.141882",
        "multi_review_order_count": 551,
    }
    value.update(overrides)
    return value


def quality_metrics(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "window_type": "FULL",
        "window_start": "2016-09-04",
        "window_end": "2018-10-17",
        "source_row_count": 99_441,
        "iceberg_row_count": 99_441,
        "duplicate_key_count": 0,
        "orphan_key_count": 0,
        "invalid_value_count": 1,
        "temporal_anomaly_count": 0,
        "amount_comparable_order_count": 98_000,
        "amount_reconciled_order_count": 90_000,
        "amount_mismatch_order_count": 8_000,
        "amount_reconciliation_rate": "0.918367",
        "payment_item_freight_abs_difference_avg": "4.250000",
        "payment_item_freight_abs_difference_p50": "0.000000",
        "payment_item_freight_abs_difference_p90": "10.000000",
        "raw_row_counts": {"orders": 99_441},
        "normalized_row_counts": {"orders": 99_441},
        "iceberg_row_counts": {"orders_src_v1": 99_441},
        "normalized_sha256": {"orders": "b" * 64},
        "source_snapshots": {name: str(index) for index, name in enumerate(sorted(SOURCE_TABLES), 1)},
        "curated_snapshots": {name: str(index) for index, name in enumerate(sorted(CURATED_TABLES), 101)},
        "fact_reconciliations": {"order_fact_row_count": 99_441},
        "reportable_quality": {"multi_review_order_count": 551},
        "reconciliation_status": "PASS",
    }
    value.update(overrides)
    return value


def publication_data(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "published_at": "2026-09-22T08:01:00Z",
        "overview_row_count": 1,
        "overview_sha256": "1" * 64,
        "delivery_row_count": 1,
        "delivery_sha256": "2" * 64,
        "payment_row_count": 6,
        "payment_sha256": "3" * 64,
        "ranking_row_count": 20,
        "ranking_sha256": "4" * 64,
        "review_row_count": 1,
        "review_sha256": "5" * 64,
        "quality_row_count": 1,
        "quality_sha256": "6" * 64,
        "status": "PUBLISHED",
    }
    value.update(overrides)
    return value


def _canonical_json(values: dict[str, object]) -> str:
    return json.dumps(values, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def order_publication_row(**overrides: object) -> dict[str, object]:
    source_snapshots = {
        name: str(index) for index, name in enumerate(sorted(SOURCE_TABLES), 1)
    }
    curated_snapshots = {
        name: str(index) for index, name in enumerate(sorted(CURATED_TABLES), 101)
    }
    row: dict[str, object] = {
        "metric_run_id": f"orders-v1-b{'a' * 64}",
        "dataset_id": "olist-brazilian-ecommerce-v2",
        "metric_version": "orders-v1",
        "source_bundle_sha256": "a" * 64,
        "source_snapshots_json": _canonical_json(source_snapshots),
        "curated_snapshots_json": _canonical_json(curated_snapshots),
        "implementation_revision": "1" * 40,
        "source_order_count": 2,
        "window_start": date(2017, 1, 1),
        "window_end": date(2017, 1, 2),
        "calculated_at": datetime(2026, 9, 22, 8, 0, 0),
        "published_at": datetime(2026, 9, 22, 8, 1, 0),
        "overview_row_count": 1,
        "overview_sha256": "1" * 64,
        "delivery_row_count": 1,
        "delivery_sha256": "2" * 64,
        "payment_row_count": 1,
        "payment_sha256": "3" * 64,
        "ranking_row_count": 1,
        "ranking_sha256": "4" * 64,
        "review_row_count": 1,
        "review_sha256": "5" * 64,
        "quality_row_count": 1,
        "quality_sha256": "6" * 64,
        "status": "PUBLISHED",
    }
    row.update(overrides)
    return row


def order_identity_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "metric_run_id": f"orders-v1-b{'a' * 64}",
        "dataset_id": "olist-brazilian-ecommerce-v2",
        "metric_version": "orders-v1",
        "window_type": "FULL",
        "window_start": date(2017, 1, 1),
        "window_end": date(2017, 1, 2),
    }
    row.update(overrides)
    return row


def order_overview_row(**overrides: object) -> dict[str, object]:
    row = order_identity_row(
        order_count=2,
        delivered_order_count=1,
        canceled_order_count=1,
        unavailable_order_count=0,
        status_eligible_order_count=2,
        status_excluded_order_count=0,
        delivered_rate=Decimal("0.500000"),
        canceled_rate=Decimal("0.500000"),
        unique_customer_count=2,
        repeat_customer_count=0,
        repeat_customer_rate=Decimal("0.000000"),
        item_row_count=2,
        item_value_sum=Decimal("30.00"),
        freight_value_sum=Decimal("5.00"),
        payment_value_sum=Decimal("35.00"),
        items_per_order_avg=Decimal("1.000000"),
    )
    row.update(overrides)
    return row


def order_delivery_row(**overrides: object) -> dict[str, object]:
    row = order_identity_row(
        delivery_eligible_order_count=1,
        delivery_excluded_order_count=1,
        delivery_days_avg=Decimal("3.000000"),
        delivery_days_p50=Decimal("3.000000"),
        delivery_days_p90=Decimal("3.000000"),
        late_delivery_order_count=0,
        late_delivery_eligible_order_count=1,
        late_delivery_excluded_order_count=1,
        late_delivery_rate=Decimal("0.000000"),
    )
    row.update(overrides)
    return row


def order_payment_row(**overrides: object) -> dict[str, object]:
    row = order_identity_row(
        payment_type="__ALL__",
        is_all=1,
        global_order_count=2,
        payment_order_count=2,
        payment_row_count=2,
        installment_order_count=1,
        payment_type_order_count=2,
        payment_type_value_sum=Decimal("35.00"),
    )
    row.update(overrides)
    return row


def order_ranking_row(**overrides: object) -> dict[str, object]:
    row = order_identity_row(
        dimension_type="customer_state",
        dimension_id="SP",
        dimension_name="SP",
        is_unknown=0,
        ranking_order_count=2,
        ranking_item_row_count=None,
        ranking_customer_count=2,
        ranking_item_value_sum=None,
        ranking_freight_value_sum=None,
        ranking_payment_value_sum=Decimal("35.00"),
        ranking_late_delivery_order_count=0,
        ranking_late_delivery_eligible_order_count=1,
        ranking_late_delivery_rate=Decimal("0.000000"),
        payment_value_is_additive=1,
    )
    row.update(overrides)
    return row


def order_review_row(**overrides: object) -> dict[str, object]:
    row = order_identity_row(
        review_row_count=2,
        reviewed_order_count=2,
        all_order_count=2,
        review_coverage_rate=Decimal("1.000000"),
        review_score_avg=Decimal("4.000000"),
        low_score_order_count=1,
        low_score_rate=Decimal("0.500000"),
        multi_review_order_count=0,
    )
    row.update(overrides)
    return row


def order_quality_row(**overrides: object) -> dict[str, object]:
    raw_counts = {name: 2 for name in SOURCE_ENTITIES}
    normalized_counts = {name: 2 for name in SOURCE_ENTITIES}
    iceberg_counts = {name: 2 for name in SOURCE_TABLES}
    normalized_hashes = {name: "b" * 64 for name in SOURCE_ENTITIES}
    source_snapshots = {
        name: str(index) for index, name in enumerate(sorted(SOURCE_TABLES), 1)
    }
    curated_snapshots = {
        name: str(index) for index, name in enumerate(sorted(CURATED_TABLES), 101)
    }
    fact_reconciliations = {name: 2 for name in FACT_RECONCILIATION_KEYS}
    reportable_quality = {name: 0 for name in REPORTABLE_QUALITY_KEYS}
    row = order_identity_row(
        source_row_count=2,
        iceberg_row_count=2,
        duplicate_key_count=0,
        orphan_key_count=0,
        invalid_value_count=0,
        temporal_anomaly_count=0,
        amount_comparable_order_count=2,
        amount_reconciled_order_count=2,
        amount_mismatch_order_count=0,
        amount_reconciliation_rate=Decimal("1.000000"),
        payment_item_freight_abs_difference_avg=Decimal("0.000000"),
        payment_item_freight_abs_difference_p50=Decimal("0.000000"),
        payment_item_freight_abs_difference_p90=Decimal("0.000000"),
        raw_row_counts_json=_canonical_json(raw_counts),
        normalized_row_counts_json=_canonical_json(normalized_counts),
        iceberg_row_counts_json=_canonical_json(iceberg_counts),
        normalized_sha256_json=_canonical_json(normalized_hashes),
        source_snapshots_json=_canonical_json(source_snapshots),
        curated_snapshots_json=_canonical_json(curated_snapshots),
        fact_reconciliations_json=_canonical_json(fact_reconciliations),
        reportable_quality_json=_canonical_json(reportable_quality),
        reconciliation_status="PASS",
    )
    row.update(overrides)
    return row


def make_order_repository(
    *, rows: list[dict[str, object]] | None = None, row: dict[str, object] | None = None
) -> tuple[OrderMetricsRepository, MagicMock, Mock]:
    cursor = MagicMock()
    cursor.fetchall.return_value = [] if rows is None else rows
    cursor.fetchone.return_value = row
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    connect = Mock(return_value=connection)
    return OrderMetricsRepository(connect), cursor, connect


def make_order_service(**repository_overrides: object) -> tuple[OrderMetricsService, Mock]:
    repository = Mock()
    repository.fetch_latest_publication.return_value = order_publication_row()
    repository.fetch_family_row_count.return_value = 1
    repository.fetch_overview.return_value = [order_overview_row()]
    repository.fetch_delivery.return_value = [order_delivery_row()]
    repository.fetch_payments.return_value = [order_payment_row()]
    repository.fetch_rankings.return_value = [order_ranking_row()]
    repository.fetch_reviews.return_value = [order_review_row()]
    repository.fetch_quality.return_value = order_quality_row()
    for name, value in repository_overrides.items():
        getattr(repository, name).return_value = value
    catalog = OrderMetricDefinitionCatalog.load(DEFINITIONS)
    return OrderMetricsService(repository, catalog), repository


class OrderModelAndCatalogTests(unittest.TestCase):
    def setUp(self) -> None:
        self.catalog_payload = json.loads(DEFINITIONS.read_text(encoding="utf-8"))

    def _write_catalog(self, payload: dict[str, object]) -> Path:
        temporary = tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", encoding="utf-8", delete=False, dir=ROOT / "tmp"
        )
        with temporary:
            json.dump(payload, temporary, ensure_ascii=False)
        self.addCleanup(Path(temporary.name).unlink, missing_ok=True)
        return Path(temporary.name)

    def test_models_accept_exact_contract_and_forbid_extra_fields(self):
        meta = OrderMetricMeta.model_validate(metric_meta())
        points = (
            OrderOverviewPoint.model_validate(overview_point()),
            OrderDeliveryPoint.model_validate(delivery_point()),
            OrderPaymentPoint.model_validate(payment_point()),
            OrderRankingPoint.model_validate(ranking_point()),
            OrderReviewPoint.model_validate(review_point()),
            OrderQualityMetrics.model_validate(quality_metrics()),
        )
        publication = OrderPublicationData.model_validate(publication_data())
        responses = (
            OrderPublicationResponse(meta=meta, data=publication),
            OrderOverviewResponse(meta=meta, data=[points[0]]),
            OrderDeliveryResponse(meta=meta, data=[points[1]]),
            OrderPaymentsResponse(meta=meta, data=[points[2]]),
            OrderRankingsResponse(meta=meta, data=[points[3]]),
            OrderReviewsResponse(meta=meta, data=[points[4]]),
            OrderQualityResponse(meta=meta, data=points[5]),
        )

        for model in (meta, *points, publication, *responses):
            with self.subTest(model=type(model).__name__), self.assertRaises(ValidationError):
                type(model).model_validate({**model.model_dump(), "unexpected": True})

    def test_meta_requires_full_lowercase_identity_exact_snapshots_and_currency_warning(self):
        invalid_values = (
            {"metric_run_id": f"orders-v1-b{'a' * 63}"},
            {"source_bundle_sha256": "A" * 64},
            {"implementation_revision": "A" * 40},
            {"source_snapshots": {**metric_meta()["source_snapshots"], "extra": "1"}},
            {"curated_snapshots": {name: "1" for name in list(CURATED_TABLES)[:-1]}},
            {"source_snapshots": {name: 1 for name in SOURCE_TABLES}},
            {"warnings": []},
            {"source_order_count": True},
        )
        for override in invalid_values:
            with self.subTest(override=override), self.assertRaises(ValidationError):
                OrderMetricMeta.model_validate(metric_meta(**override))

    def test_points_require_fixed_decimals_strict_counts_and_supported_literals(self):
        cases = (
            (OrderOverviewPoint, overview_point(order_count=True)),
            (OrderOverviewPoint, overview_point(item_value_sum="1.0")),
            (OrderOverviewPoint, overview_point(delivered_rate="0.5")),
            (OrderDeliveryPoint, delivery_point(window_type="WEEK")),
            (OrderPaymentPoint, payment_point(payment_value_sum="12.345")),
            (OrderRankingPoint, ranking_point(dimension_type="city")),
            (OrderReviewPoint, review_point(review_row_count=-1)),
            (OrderQualityMetrics, quality_metrics(raw_row_counts={"orders": True})),
            (OrderQualityMetrics, quality_metrics(reconciliation_status="FAILED")),
            (OrderPublicationData, publication_data(overview_sha256="A" * 64)),
        )
        for model, payload in cases:
            with self.subTest(model=model.__name__), self.assertRaises(ValidationError):
                model.model_validate(payload)

    def test_ranking_payment_additivity_is_dimension_bound(self):
        valid = (
            ranking_point(dimension_type="customer_state", payment_value_is_additive=True),
            ranking_point(dimension_type="seller_state", payment_value_is_additive=False),
            ranking_point(
                dimension_type="product",
                ranking_payment_value_sum=None,
                ranking_late_delivery_order_count=None,
                ranking_late_delivery_eligible_order_count=None,
                ranking_late_delivery_rate=None,
                payment_value_is_additive=None,
            ),
        )
        for payload in valid:
            OrderRankingPoint.model_validate(payload)
        invalid = (
            ranking_point(dimension_type="customer_state", payment_value_is_additive=False),
            ranking_point(dimension_type="seller_state", payment_value_is_additive=True),
            ranking_point(dimension_type="product", payment_value_is_additive=True),
        )
        for payload in invalid:
            with self.assertRaises(ValidationError):
                OrderRankingPoint.model_validate(payload)

    def test_catalog_loads_exact_task4_metric_set_and_strict_definition_response(self):
        catalog = OrderMetricDefinitionCatalog.load(DEFINITIONS)
        names = [definition.metric_name for definition in catalog.all()]
        self.assertEqual(REQUIRED_ORDER_METRICS, frozenset(names))
        self.assertEqual(len(names), len(set(names)))
        response = OrderMetricDefinitionsResponse(
            domain="orders",
            dataset_id="olist-brazilian-ecommerce-v2",
            metric_version="orders-v1",
            definitions=catalog.all(),
        )
        self.assertEqual(names, [definition.metric_name for definition in response.definitions])
        with self.assertRaises(ValidationError):
            OrderMetricDefinitionsResponse.model_validate({**response.model_dump(), "extra": True})

    def test_catalog_rejects_identity_duplicates_fields_windows_and_limitations(self):
        mutations = []
        wrong_identity = deepcopy(self.catalog_payload)
        wrong_identity["metric_version"] = "orders-v2"
        mutations.append(wrong_identity)
        duplicate = deepcopy(self.catalog_payload)
        duplicate["definitions"][-1]["metric_name"] = duplicate["definitions"][0]["metric_name"]
        mutations.append(duplicate)
        missing = deepcopy(self.catalog_payload)
        missing["definitions"].pop()
        mutations.append(missing)
        unknown_field = deepcopy(self.catalog_payload)
        unknown_field["definitions"][0]["source_fields"].append("credit_card_number")
        mutations.append(unknown_field)
        unknown_window = deepcopy(self.catalog_payload)
        unknown_window["definitions"][0]["allowed_windows"] = ["WEEK"]
        mutations.append(unknown_window)
        no_limitations = deepcopy(self.catalog_payload)
        no_limitations["definitions"][0]["limitations"] = []
        mutations.append(no_limitations)

        for index, payload in enumerate(mutations):
            with self.subTest(index=index), self.assertRaises((ValidationError, ValueError)):
                OrderMetricDefinitionCatalog.load(self._write_catalog(payload))

    def test_catalog_requires_exact_forbidden_claims_and_seller_state_warning(self):
        monetary = deepcopy(self.catalog_payload)
        target = next(item for item in monetary["definitions"] if item["metric_name"] == "item_value_sum")
        target["forbidden_claims"].pop()
        with self.assertRaisesRegex(ValueError, "forbidden claims"):
            OrderMetricDefinitionCatalog.load(self._write_catalog(monetary))

        seller_state = deepcopy(self.catalog_payload)
        target = next(
            item for item in seller_state["definitions"]
            if item["metric_name"] == "ranking_payment_value_sum"
        )
        target["limitations"] = ["Currency is unspecified."]
        with self.assertRaisesRegex(ValueError, "seller_state"):
            OrderMetricDefinitionCatalog.load(self._write_catalog(seller_state))

    def test_settings_container_wiring_and_factory_are_order_specific(self):
        expected = DEFINITIONS.resolve()
        original_cwd = Path.cwd()
        try:
            os.chdir(ROOT / "tmp")
            defaults = load_settings({})
        finally:
            os.chdir(original_cwd)
        self.assertEqual(expected, defaults.order_metric_definitions_path)
        configured = load_settings({"ORDER_METRIC_DEFINITIONS_PATH": "/app/configs/metrics/orders-v1.json"})
        self.assertEqual(Path("/app/configs/metrics/orders-v1.json"), configured.order_metric_definitions_path)
        with self.assertRaisesRegex(ValueError, "ORDER_METRIC_DEFINITIONS_PATH"):
            ApiSettings(order_metric_definitions_path="")

        env_text = (ROOT / "infra" / ".env.example").read_text(encoding="utf-8")
        compose_text = (ROOT / "infra" / "docker-compose.yml").read_text(encoding="utf-8")
        self.assertIn("ORDER_METRIC_DEFINITIONS_PATH=/app/configs/metrics/orders-v1.json", env_text)
        self.assertIn("ORDER_METRIC_DEFINITIONS_PATH: ${ORDER_METRIC_DEFINITIONS_PATH}", compose_text)

        repository_module = types.ModuleType("app.order_repository")
        service_module = types.ModuleType("app.order_service")

        class FakeRepository:
            @classmethod
            def from_settings(cls, settings: ApiSettings) -> object:
                return ("repository", settings.doris_database)

        class FakeService:
            def __init__(self, repository: object, catalog: object) -> None:
                self.repository = repository
                self.catalog = catalog

        repository_module.OrderMetricsRepository = FakeRepository
        service_module.OrderMetricsService = FakeService
        settings = ApiSettings(order_metric_definitions_path=DEFINITIONS)
        with patch.dict(
            sys.modules,
            {
                "app.order_repository": repository_module,
                "app.order_service": service_module,
            },
        ):
            service = build_order_metrics_service(settings)
        self.assertEqual(("repository", "analytics"), service.repository)
        self.assertEqual(REQUIRED_ORDER_METRICS, frozenset(item.metric_name for item in service.catalog.all()))


class OrderMetricsRepositoryTests(unittest.TestCase):
    def test_repository_uses_fixed_queries_bound_values_and_physical_sort_columns(self):
        repository, cursor, _ = make_order_repository(row=order_publication_row())
        publication = repository.fetch_latest_publication()
        self.assertEqual("PUBLISHED", publication["status"])
        query, parameters = cursor.execute.call_args.args
        self.assertIn("FROM order_metric_publications WHERE status = %s", query)
        self.assertIn("ORDER BY published_at DESC, metric_run_id DESC LIMIT 1", query)
        self.assertEqual(("PUBLISHED",), parameters)

        cursor.reset_mock()
        cursor.fetchall.return_value = [order_overview_row(window_type="DAY")]
        rows = repository.fetch_overview(
            f"orders-v1-b{'a' * 64}", "day", date(2017, 1, 1), date(2017, 1, 2)
        )
        self.assertEqual(1, len(rows))
        query, parameters = cursor.execute.call_args.args
        self.assertIn("FROM order_metric_overview", query)
        self.assertIn("window_start >= %s AND window_end <= %s", query)
        self.assertEqual(
            (f"orders-v1-b{'a' * 64}", "DAY", date(2017, 1, 1), date(2017, 1, 2)),
            parameters,
        )

        cursor.reset_mock()
        cursor.fetchall.return_value = [order_ranking_row(dimension_type="product")]
        repository.fetch_rankings(
            f"orders-v1-b{'a' * 64}",
            "product",
            "full",
            None,
            None,
            "item_value",
            20,
        )
        query, parameters = cursor.execute.call_args.args
        self.assertIn(
            "ORDER BY ranking_item_value_sum DESC, dimension_id ASC, window_start ASC LIMIT %s",
            query,
        )
        self.assertEqual(
            (f"orders-v1-b{'a' * 64}", "product", "FULL", 20), parameters
        )

        cursor.reset_mock()
        cursor.fetchone.return_value = {"row_count": 7}
        self.assertEqual(
            7,
            repository.fetch_family_row_count(f"orders-v1-b{'a' * 64}", "quality"),
        )
        query, parameters = cursor.execute.call_args.args
        self.assertEqual(
            "SELECT COUNT(*) AS row_count FROM order_metric_quality WHERE metric_run_id = %s",
            query,
        )
        self.assertEqual((f"orders-v1-b{'a' * 64}",), parameters)

    def test_repository_rejects_untrusted_query_shapes_before_connecting(self):
        repository, _, connect = make_order_repository()
        run_id = f"orders-v1-b{'a' * 64}"
        invalid_calls = (
            lambda: repository.fetch_overview(run_id, "week", None, None),
            lambda: repository.fetch_overview(run_id, "day", date(2017, 1, 1), None),
            lambda: repository.fetch_overview(
                run_id, "full", date(2017, 1, 1), date(2017, 1, 2)
            ),
            lambda: repository.fetch_overview(
                run_id, "day", date(2017, 1, 2), date(2017, 1, 1)
            ),
            lambda: repository.fetch_rankings(
                run_id, "product; DROP TABLE x", "full", None, None, "order_count", 20
            ),
            lambda: repository.fetch_rankings(
                run_id, "product", "full", None, None, "payment_value", 20
            ),
            lambda: repository.fetch_rankings(
                run_id, "customer_state", "full", None, None, "item_value", 20
            ),
            lambda: repository.fetch_rankings(
                run_id, "product", "full", None, None, "order_count", True
            ),
            lambda: repository.fetch_rankings(
                run_id, "product", "full", None, None, "order_count", "20"
            ),
            lambda: repository.fetch_family_row_count(run_id, "quality; DROP TABLE x"),
        )
        for operation in invalid_calls:
            with self.subTest(operation=operation), self.assertRaises(ValueError):
                operation()
        connect.assert_not_called()


class OrderMetricsServiceTests(unittest.TestCase):
    def test_all_responses_use_one_published_identity_and_typed_values(self):
        service, repository = make_order_service()

        publication = service.get_publication()
        overview = service.get_overview("full", None, None)
        delivery = service.get_delivery("full", None, None)
        payments = service.get_payments("full", None, None)
        rankings = service.get_rankings(
            "customer_state", "full", None, None, "payment_value", 20
        )
        reviews = service.get_reviews("full", None, None)
        quality = service.get_quality()
        definitions = service.get_definitions()

        for response in (
            publication,
            overview,
            delivery,
            payments,
            rankings,
            reviews,
            quality,
        ):
            self.assertEqual(f"orders-v1-b{'a' * 64}", response.meta.metric_run_id)
            self.assertEqual("unspecified", response.meta.source_timezone)
            self.assertIsNone(response.meta.source_currency)
            self.assertTrue(response.meta.warnings)
        self.assertEqual("30.00", overview.data[0].item_value_sum)
        self.assertEqual("35.00", payments.data[0].payment_value_sum)
        self.assertTrue(rankings.data[0].payment_value_is_additive)
        self.assertEqual(2, quality.data.raw_row_counts["orders"])
        self.assertEqual(REQUIRED_ORDER_METRICS, frozenset(d.metric_name for d in definitions.definitions))
        repository.fetch_overview.assert_called_once_with(
            f"orders-v1-b{'a' * 64}", "full", None, None
        )

    def test_publication_rejects_every_identity_snapshot_and_range_gap(self):
        canonical_source = order_publication_row()["source_snapshots_json"]
        incomplete_source = json.loads(canonical_source)
        incomplete_source.pop(next(iter(incomplete_source)))
        cases: tuple[dict[str, object] | None, ...] = (
            None,
            {"status": "FAILED"},
            {"dataset_id": "other"},
            {"metric_version": "orders-v2"},
            {"metric_run_id": f"orders-v1-b{'b' * 64}"},
            {"source_bundle_sha256": "A" * 64},
            {"source_snapshots_json": json.dumps(json.loads(canonical_source))},
            {"source_snapshots_json": _canonical_json(incomplete_source)},
            {"source_order_count": 0},
            {"window_start": date(2017, 1, 3)},
            {"implementation_revision": "A" * 40},
            {"overview_sha256": "A" * 64},
        )
        for mutation in cases:
            repository = Mock()
            repository.fetch_latest_publication.return_value = (
                None if mutation is None else order_publication_row(**mutation)
            )
            repository.fetch_family_row_count.return_value = 1
            service = OrderMetricsService(
                repository, OrderMetricDefinitionCatalog.load(DEFINITIONS)
            )
            with self.subTest(mutation=mutation), self.assertRaises(
                OrderMetricsUnavailableError
            ) as raised:
                service.get_publication()
            self.assertNotIn("SELECT", str(raised.exception))

    def test_family_integrity_decimal_quality_and_ordering_fail_closed(self):
        failures = (
            ("overview", {"fetch_overview": [order_overview_row(dataset_id="other")]}),
            ("overview", {"fetch_overview": [order_overview_row(item_value_sum="30.00")]}),
            ("overview", {"fetch_family_row_count": 2}),
            ("overview", {"fetch_overview": [
                order_overview_row(window_type="DAY", window_start=date(2017, 1, 2), window_end=date(2017, 1, 2)),
                order_overview_row(window_type="DAY", window_start=date(2017, 1, 1), window_end=date(2017, 1, 1)),
            ]}),
            ("quality", {"fetch_quality": order_quality_row(reconciliation_status="FAILED")}),
            ("quality", {"fetch_quality": order_quality_row(raw_row_counts_json=_canonical_json({"orders": -1}))}),
        )
        for family, overrides in failures:
            service, repository = make_order_service()
            for method_name, value in overrides.items():
                getattr(repository, method_name).return_value = value
            operation = (
                service.get_quality
                if family == "quality"
                else lambda: service.get_overview("day", None, None)
            )
            with self.subTest(family=family, overrides=overrides), self.assertRaises(
                OrderMetricsUnavailableError
            ):
                operation()

    def test_request_ranges_are_rejected_before_family_queries(self):
        invalid = (
            ("day", date(2017, 1, 1), None),
            ("full", date(2017, 1, 1), date(2017, 1, 2)),
            ("day", date(2017, 1, 2), date(2017, 1, 1)),
            ("day", date(2016, 12, 31), date(2017, 1, 1)),
        )
        for window, start, end in invalid:
            service, repository = make_order_service()
            with self.subTest(window=window, start=start, end=end), self.assertRaises(
                OrderMetricsRequestError
            ):
                service.get_overview(window, start, end)
            repository.fetch_overview.assert_not_called()


class OrderMetricsRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        real_service, _ = make_order_service()
        self.service = Mock()
        self.service.get_publication.return_value = real_service.get_publication()
        self.service.get_overview.return_value = real_service.get_overview("full", None, None)
        self.service.get_delivery.return_value = real_service.get_delivery("full", None, None)
        self.service.get_payments.return_value = real_service.get_payments("full", None, None)
        self.service.get_rankings.return_value = real_service.get_rankings(
            "customer_state", "full", None, None, "payment_value", 20
        )
        self.service.get_reviews.return_value = real_service.get_reviews("full", None, None)
        self.service.get_quality.return_value = real_service.get_quality()
        self.service.get_definitions.return_value = real_service.get_definitions()
        self.behavior_service = Mock()
        app = create_app(
            repository=Mock(),
            analysis_service=Mock(),
            tool_analysis_service=Mock(),
            readiness_service=Mock(),
            behavior_service=self.behavior_service,
            order_service=self.service,
        )
        self.client = TestClient(app)

    def test_all_eight_order_contracts_use_the_injected_service(self):
        responses = (
            self.client.get("/api/v1/orders/publication"),
            self.client.get("/api/v1/orders/overview"),
            self.client.get("/api/v1/orders/delivery"),
            self.client.get("/api/v1/orders/payments"),
            self.client.get(
                "/api/v1/orders/rankings",
                params={"dimension": "customer_state", "sort_by": "payment_value"},
            ),
            self.client.get("/api/v1/orders/reviews"),
            self.client.get("/api/v1/orders/quality"),
            self.client.get(
                "/api/v1/metrics/definitions",
                params={"domain": "orders", "version": "orders-v1"},
            ),
        )
        self.assertTrue(all(response.status_code == 200 for response in responses))
        self.assertTrue(all(response.json().get("meta", {}).get("metric_version") == "orders-v1" for response in responses[:7]))
        self.assertEqual("orders-v1", responses[-1].json()["metric_version"])
        self.service.get_rankings.assert_called_once_with(
            "customer_state", "full", None, None, "payment_value", 20
        )

    def test_invalid_query_combinations_return_422_before_service_calls(self):
        invalid_requests = (
            ("/api/v1/orders/overview", {"start_date": "not-a-date", "end_date": "2017-01-02"}),
            ("/api/v1/orders/overview", {"window": "day", "start_date": "2017-01-01"}),
            ("/api/v1/orders/overview", {"window": "full", "start_date": "2017-01-01", "end_date": "2017-01-02"}),
            ("/api/v1/orders/rankings", {"dimension": "product", "sort_by": "payment_value"}),
            ("/api/v1/orders/rankings", {"dimension": "product", "limit": 101}),
            ("/api/v1/metrics/definitions", {"domain": "behavior", "version": "orders-v1"}),
            ("/api/v1/metrics/definitions", {"domain": "orders", "version": "behavior-v1"}),
        )
        for path, parameters in invalid_requests:
            with self.subTest(path=path, parameters=parameters):
                response = self.client.get(path, params=parameters)
                self.assertEqual(422, response.status_code)
        self.service.get_overview.assert_not_called()
        self.service.get_rankings.assert_not_called()
        self.service.get_definitions.assert_not_called()
        self.behavior_service.get_definitions.assert_not_called()

    def test_order_failures_return_safe_logged_503(self):
        self.service.get_overview.side_effect = OrderMetricsUnavailableError(
            "SELECT password FROM internal.host"
        )
        with patch("app.main.logger.error") as log_error:
            response = self.client.get("/api/v1/orders/overview")
        self.assertEqual(503, response.status_code)
        self.assertEqual(
            {"detail": "order metrics are temporarily unavailable"}, response.json()
        )
        log_error.assert_called_once_with(
            "order_metrics_unavailable",
            extra={
                "stage": "order_overview",
                "error_type": "OrderMetricsUnavailableError",
            },
        )

        self.service.get_definitions.side_effect = OrderMetricsUnavailableError(
            "invalid catalog /secret/path"
        )
        response = self.client.get(
            "/api/v1/metrics/definitions",
            params={"domain": "orders", "version": "orders-v1"},
        )
        self.assertEqual(503, response.status_code)
        self.assertNotIn("secret", response.text)


if __name__ == "__main__":
    unittest.main()
