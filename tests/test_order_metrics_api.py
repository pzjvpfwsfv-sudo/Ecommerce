from copy import deepcopy
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

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


if __name__ == "__main__":
    unittest.main()
