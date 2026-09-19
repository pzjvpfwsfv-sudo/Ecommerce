from copy import deepcopy
from datetime import UTC, date, datetime
import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

from pydantic import ValidationError


ROOT = Path(__file__).resolve().parent.parent
DEFINITIONS = ROOT / "configs" / "metrics" / "behavior-v1.json"

sys.path.insert(0, str((ROOT / "services" / "api").resolve()))

from app.behavior_models import (  # noqa: E402
    BehaviorMetricMeta,
    DimensionRanking,
    FunnelPoint,
    FunnelResponse,
    MetricDefinitionsResponse,
    OverviewPoint,
    OverviewResponse,
    PublicationData,
    PublicationResponse,
    QualityMetrics,
    QualityResponse,
    RankingsResponse,
)
from app.config import ApiSettings, load_settings  # noqa: E402
from app.metric_definitions import MetricDefinitionCatalog  # noqa: E402


class BehaviorModelsAndCatalogTests(unittest.TestCase):
    def setUp(self) -> None:
        self.catalog_payload = json.loads(DEFINITIONS.read_text(encoding="utf-8"))

    def _write_catalog(self, payload: dict) -> Path:
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "behavior-v1.json"
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        return path

    def _meta(self) -> BehaviorMetricMeta:
        return BehaviorMetricMeta(
            dataset_id="rees46-multicategory",
            metric_version="behavior-v1",
            metric_run_id="behavior-v1-s3854376992136224865",
            source_snapshot_id="3854376992136224865",
            window_start=date(2019, 10, 1),
            window_end=date(2019, 11, 30),
            calculated_at=datetime(2026, 9, 18, tzinfo=UTC),
            data_scope="g2c-correctness-subset",
            source_event_count=1002,
            warnings=["correctness subset; not the full 2% user sample"],
        )

    def test_metric_meta_preserves_snapshot_and_subset_warning(self):
        payload = self._meta().model_dump(mode="json")

        self.assertEqual("3854376992136224865", payload["source_snapshot_id"])
        self.assertEqual(
            ["correctness subset; not the full 2% user sample"], payload["warnings"]
        )

    def test_response_models_preserve_decimal_strings_and_reject_binary_floats(self):
        overview = OverviewPoint(
            window_type="FULL",
            window_start=date(2019, 10, 1),
            window_end=date(2019, 11, 30),
            event_count=1002,
            view_count=700,
            cart_count=200,
            purchase_count=100,
            unique_user_count=500,
            session_count=600,
            product_count=300,
            purchase_amount_proxy="1000.00",
        )
        funnel = FunnelPoint(
            window_type="FULL",
            window_start=date(2019, 10, 1),
            window_end=date(2019, 11, 30),
            missing_session_event_count=2,
            view_sessions=500,
            view_to_cart_sessions=150,
            completed_sessions=75,
            view_to_cart_rate="0.300000",
            cart_to_purchase_rate="0.500000",
            full_conversion_rate="0.150000",
        )

        payload = FunnelResponse(meta=self._meta(), data=[funnel]).model_dump(mode="json")

        self.assertEqual("0.300000", payload["data"][0]["view_to_cart_rate"])
        self.assertEqual(
            "1000.00",
            OverviewResponse(meta=self._meta(), data=[overview]).model_dump(mode="json")[
                "data"
            ][0]["purchase_amount_proxy"],
        )
        with self.assertRaises(ValidationError):
            OverviewPoint(
                window_type="FULL",
                window_start=date(2019, 10, 1),
                window_end=date(2019, 11, 30),
                event_count=1002,
                view_count=700,
                cart_count=200,
                purchase_count=100,
                unique_user_count=500,
                session_count=600,
                product_count=300,
                purchase_amount_proxy=1000.0,
            )

    def test_all_task_4_response_models_forbid_extra_fields(self):
        ranking = DimensionRanking(
            window_type="FULL",
            window_start=date(2019, 10, 1),
            window_end=date(2019, 11, 30),
            dimension_type="brand",
            dimension_id="brand-a",
            dimension_name="Brand A",
            is_unknown=False,
            view_count=10,
            cart_count=3,
            purchase_count=1,
            unique_user_count=8,
            purchase_amount_proxy="12.00",
        )
        quality = QualityMetrics(
            window_type="FULL",
            window_start=date(2019, 10, 1),
            window_end=date(2019, 11, 30),
            source_event_count=1002,
            clean_event_count=1001,
            late_event_count=1,
            clean_event_rate="0.999002",
            late_event_rate="0.000998",
            distinct_event_count=1002,
            duplicate_event_count=0,
            missing_session_count=2,
            unknown_category_count=3,
            unknown_brand_count=4,
            invalid_event_type_count=0,
            empty_key_id_count=0,
            invalid_price_count=0,
            invalid_derived_date_count=0,
            overview_event_count=1002,
            reconciliation_status="PASS",
        )
        publication = PublicationData(
            published_at=datetime(2026, 9, 18, tzinfo=UTC),
            overview_row_count=61,
            overview_sha256="a" * 64,
            funnel_row_count=61,
            funnel_sha256="b" * 64,
            dimension_row_count=12,
            dimension_sha256="c" * 64,
            quality_row_count=1,
            quality_sha256="d" * 64,
            status="PUBLISHED",
        )
        catalog = MetricDefinitionCatalog.load(DEFINITIONS)
        responses = (
            PublicationResponse(meta=self._meta(), data=publication),
            RankingsResponse(meta=self._meta(), data=[ranking]),
            QualityResponse(meta=self._meta(), data=quality),
            MetricDefinitionsResponse(
                domain="behavior",
                dataset_id="rees46-multicategory",
                metric_version="behavior-v1",
                definitions=catalog.all(),
            ),
        )

        for response in responses:
            with self.subTest(model=type(response).__name__), self.assertRaises(
                ValidationError
            ):
                type(response).model_validate({**response.model_dump(), "unexpected": True})

    def test_catalog_preserves_order_and_requires_proxy_forbidden_claims(self):
        catalog = MetricDefinitionCatalog.load(DEFINITIONS)

        self.assertEqual(
            [definition["metric_name"] for definition in self.catalog_payload["definitions"]],
            [definition.metric_name for definition in catalog.all()],
        )
        proxy = catalog.get("purchase_amount_proxy")
        self.assertEqual({"GMV", "销售额", "收入"}, set(proxy.forbidden_claims))

    def test_catalog_rejects_duplicate_metric_names(self):
        payload = deepcopy(self.catalog_payload)
        payload["definitions"][-1]["metric_name"] = payload["definitions"][0]["metric_name"]

        with self.assertRaisesRegex(ValueError, "duplicate metric names"):
            MetricDefinitionCatalog.load(self._write_catalog(payload))

    def test_catalog_rejects_wrong_domain_version_or_dataset(self):
        cases = {
            "domain": "orders",
            "dataset_id": "wrong-dataset",
            "metric_version": "behavior-v2",
        }
        for field, value in cases.items():
            with self.subTest(field=field):
                payload = deepcopy(self.catalog_payload)
                payload[field] = value
                with self.assertRaises(ValidationError):
                    MetricDefinitionCatalog.load(self._write_catalog(payload))

    def test_catalog_rejects_missing_public_metric(self):
        payload = deepcopy(self.catalog_payload)
        payload["definitions"].pop()

        with self.assertRaisesRegex(ValueError, "public metric set"):
            MetricDefinitionCatalog.load(self._write_catalog(payload))

    def test_catalog_rejects_unknown_source_field(self):
        payload = deepcopy(self.catalog_payload)
        payload["definitions"][0]["source_fields"].append("credit_card_number")

        with self.assertRaises(ValidationError):
            MetricDefinitionCatalog.load(self._write_catalog(payload))

    def test_catalog_rejects_missing_limitations(self):
        payload = deepcopy(self.catalog_payload)
        payload["definitions"][0]["limitations"] = []

        with self.assertRaises(ValidationError):
            MetricDefinitionCatalog.load(self._write_catalog(payload))

    def test_catalog_rejects_missing_proxy_forbidden_claims(self):
        payload = deepcopy(self.catalog_payload)
        payload["definitions"][7]["forbidden_claims"] = []

        with self.assertRaisesRegex(ValueError, "purchase_amount_proxy forbidden claims"):
            MetricDefinitionCatalog.load(self._write_catalog(payload))

    def test_catalog_rejects_extra_json_keys(self):
        cases = ((None, "unexpected"), (0, "metric_version"))
        for definition_index, field in cases:
            with self.subTest(definition_index=definition_index, field=field):
                payload = deepcopy(self.catalog_payload)
                target = payload if definition_index is None else payload["definitions"][definition_index]
                target[field] = "not allowed"
                with self.assertRaises(ValidationError):
                    MetricDefinitionCatalog.load(self._write_catalog(payload))

    def test_settings_default_catalog_path_is_absolute_and_cwd_independent(self):
        expected = DEFINITIONS.resolve()
        original_cwd = Path.cwd()
        with TemporaryDirectory() as directory:
            try:
                import os

                os.chdir(directory)
                settings = load_settings({})
            finally:
                os.chdir(original_cwd)

        self.assertTrue(settings.behavior_metric_definitions_path.is_absolute())
        self.assertEqual(expected, settings.behavior_metric_definitions_path)

    def test_settings_accept_environment_catalog_path_and_reject_empty_path(self):
        configured = load_settings(
            {"BEHAVIOR_METRIC_DEFINITIONS_PATH": "/app/configs/metrics/behavior-v1.json"}
        )

        self.assertEqual(
            Path("/app/configs/metrics/behavior-v1.json"),
            configured.behavior_metric_definitions_path,
        )
        with self.assertRaisesRegex(ValueError, "BEHAVIOR_METRIC_DEFINITIONS_PATH"):
            ApiSettings(behavior_metric_definitions_path="")
        with self.assertRaisesRegex(ValueError, "BEHAVIOR_METRIC_DEFINITIONS_PATH"):
            load_settings({"BEHAVIOR_METRIC_DEFINITIONS_PATH": ""})


if __name__ == "__main__":
    unittest.main()
