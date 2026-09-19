from copy import deepcopy
from datetime import UTC, date, datetime
from decimal import Decimal
import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import MagicMock, Mock, patch

from fastapi.testclient import TestClient
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
from app.behavior_repository import BehaviorMetricsRepository  # noqa: E402
from app.behavior_service import (  # noqa: E402
    BehaviorMetricsService,
    BehaviorMetricsUnavailableError,
)
from app.main import create_app  # noqa: E402
from app.metric_definitions import MetricDefinitionCatalog  # noqa: E402


def publication_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "metric_run_id": "behavior-v1-s3854376992136224865",
        "dataset_id": "rees46-multicategory",
        "metric_version": "behavior-v1",
        "data_scope": "g2c-correctness-subset",
        "source_snapshot_id": 3854376992136224865,
        "source_event_count": 1002,
        "window_start": date(2019, 10, 1),
        "window_end": date(2019, 11, 30),
        "calculated_at": datetime(2026, 9, 18, 1, 2, 3, 456000),
        "published_at": datetime(2026, 9, 18, 2, 3, 4, 567000),
        "overview_row_count": 61,
        "overview_sha256": "a" * 64,
        "funnel_row_count": 61,
        "funnel_sha256": "b" * 64,
        "dimension_row_count": 12,
        "dimension_sha256": "c" * 64,
        "quality_row_count": 1,
        "quality_sha256": "d" * 64,
        "status": "PUBLISHED",
    }
    row.update(overrides)
    return row


def overview_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "metric_run_id": "behavior-v1-s3854376992136224865",
        "dataset_id": "rees46-multicategory",
        "metric_version": "behavior-v1",
        "window_type": "FULL",
        "window_start": date(2019, 10, 1),
        "window_end": date(2019, 11, 30),
        "event_count": 1002,
        "view_count": 700,
        "cart_count": 200,
        "purchase_count": 100,
        "unique_user_count": 500,
        "session_count": 600,
        "product_count": 300,
        "purchase_amount_proxy": Decimal("1234.50"),
    }
    row.update(overrides)
    return row


def funnel_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "metric_run_id": "behavior-v1-s3854376992136224865",
        "dataset_id": "rees46-multicategory",
        "metric_version": "behavior-v1",
        "window_type": "FULL",
        "window_start": date(2019, 10, 1),
        "window_end": date(2019, 11, 30),
        "missing_session_event_count": 2,
        "view_sessions": 3,
        "view_to_cart_sessions": 2,
        "completed_sessions": 1,
    }
    row.update(overrides)
    return row


def ranking_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "metric_run_id": "behavior-v1-s3854376992136224865",
        "dataset_id": "rees46-multicategory",
        "metric_version": "behavior-v1",
        "window_type": "FULL",
        "window_start": date(2019, 10, 1),
        "window_end": date(2019, 11, 30),
        "dimension_type": "brand",
        "dimension_id": "brand-a",
        "dimension_name": "Brand A",
        "is_unknown": 0,
        "view_count": 10,
        "cart_count": 3,
        "purchase_count": 1,
        "unique_user_count": 8,
        "purchase_amount_proxy": Decimal("12.30"),
    }
    row.update(overrides)
    return row


def quality_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "metric_run_id": "behavior-v1-s3854376992136224865",
        "dataset_id": "rees46-multicategory",
        "metric_version": "behavior-v1",
        "window_type": "FULL",
        "window_start": date(2019, 10, 1),
        "window_end": date(2019, 11, 30),
        "source_event_count": 1002,
        "clean_event_count": 1001,
        "late_event_count": 1,
        "clean_event_rate": Decimal("0.99900199"),
        "late_event_rate": Decimal("0.00099800"),
        "distinct_event_count": 1002,
        "duplicate_event_count": 0,
        "missing_session_count": 2,
        "unknown_category_count": 3,
        "unknown_brand_count": 4,
        "invalid_event_type_count": 0,
        "empty_key_id_count": 0,
        "invalid_price_count": 0,
        "invalid_derived_date_count": 0,
        "overview_event_count": 1002,
        "reconciliation_status": "PASS",
    }
    row.update(overrides)
    return row


def make_repository(
    *, rows: list[dict[str, object]] | None = None, row: dict[str, object] | None = None
) -> tuple[BehaviorMetricsRepository, MagicMock, Mock]:
    cursor = MagicMock()
    cursor.fetchall.return_value = [] if rows is None else rows
    cursor.fetchone.return_value = row
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    connect = Mock(return_value=connection)
    return BehaviorMetricsRepository(connect), cursor, connect


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

    def test_catalog_rejects_duplicate_proxy_forbidden_claim(self):
        payload = deepcopy(self.catalog_payload)
        payload["definitions"][7]["forbidden_claims"].append("GMV")

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

    def test_config_import_supports_the_shallow_container_mount_layout(self):
        source = ROOT / "services" / "api" / "app" / "config.py"
        shallow_path = Path("D:/app/app/config.py")
        namespace = {"__file__": str(shallow_path), "__name__": __name__}

        exec(
            compile(source.read_text(encoding="utf-8-sig"), str(shallow_path), "exec"),
            namespace,
        )

        self.assertEqual(
            Path("D:/app/configs/metrics/behavior-v1.json"),
            namespace["_DEFAULT_BEHAVIOR_METRIC_DEFINITIONS_PATH"],
        )

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


class BehaviorMetricsRepositoryTests(unittest.TestCase):
    def test_latest_publication_selects_only_latest_published_row_with_bound_status(self):
        expected = publication_row()
        repository, cursor, _ = make_repository(row=expected)

        actual = repository.fetch_latest_publication()

        self.assertIs(expected, actual)
        sql, parameters = cursor.execute.call_args.args
        self.assertIn("FROM behavior_metric_publications", sql)
        self.assertIn("WHERE status = %s", sql)
        self.assertIn("ORDER BY published_at DESC, metric_run_id DESC", sql)
        self.assertIn("LIMIT 1", sql)
        self.assertEqual(("PUBLISHED",), parameters)

    def test_rankings_use_whitelisted_order_expression_and_bound_values(self):
        repository, cursor, _ = make_repository(rows=[])

        repository.fetch_rankings(
            metric_run_id="behavior-v1-s1",
            dimension="category",
            window="full",
            sort_by="purchases",
            limit=20,
        )

        sql, parameters = cursor.execute.call_args.args
        self.assertIn("ORDER BY purchase_count DESC", sql)
        self.assertNotIn("category", sql.replace("dimension_type = %s", ""))
        self.assertEqual(("behavior-v1-s1", "category", "FULL", 20), parameters)

    def test_every_data_query_binds_the_exact_publication_run_id(self):
        run_id = "behavior-v1-s3854376992136224865"
        cases = (
            ("fetch_overview", (run_id, "day"), (run_id, "DAY")),
            ("fetch_funnel", (run_id, "full"), (run_id, "FULL")),
            (
                "fetch_rankings",
                (run_id, "brand", "full", "amount", 7),
                (run_id, "brand", "FULL", 7),
            ),
            ("fetch_quality", (run_id,), (run_id, "FULL")),
        )
        for method_name, arguments, expected_parameters in cases:
            with self.subTest(method=method_name):
                repository, cursor, _ = make_repository(rows=[])
                getattr(repository, method_name)(*arguments)
                _, parameters = cursor.execute.call_args.args
                self.assertEqual(expected_parameters, parameters)

    def test_unknown_identifiers_are_rejected_before_opening_a_connection(self):
        cases = (
            ("fetch_overview", ("behavior-v1-s1", "week")),
            ("fetch_funnel", ("behavior-v1-s1", "FULL")),
            (
                "fetch_rankings",
                ("behavior-v1-s1", "brand;drop table x", "full", "purchases", 20),
            ),
            (
                "fetch_rankings",
                ("behavior-v1-s1", "brand", "quarter", "purchases", 20),
            ),
            (
                "fetch_rankings",
                ("behavior-v1-s1", "brand", "full", "purchase_count desc;--", 20),
            ),
        )
        for method_name, arguments in cases:
            with self.subTest(method=method_name, arguments=arguments):
                repository, _, connect = make_repository(rows=[])
                with self.assertRaises(ValueError):
                    getattr(repository, method_name)(*arguments)
                connect.assert_not_called()

    def test_repository_exposes_only_fixed_behavior_queries(self):
        self.assertFalse(hasattr(BehaviorMetricsRepository, "query"))
        self.assertFalse(hasattr(BehaviorMetricsRepository, "execute"))


class BehaviorMetricsServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repository = Mock()
        self.catalog = MetricDefinitionCatalog.load(DEFINITIONS)
        self.service = BehaviorMetricsService(self.repository, self.catalog)

    def test_overview_loads_publication_once_and_uses_its_exact_run(self):
        self.repository.fetch_latest_publication.return_value = publication_row()
        self.repository.fetch_overview.return_value = [overview_row()]

        response = self.service.get_overview("full")

        self.repository.fetch_latest_publication.assert_called_once_with()
        self.repository.fetch_overview.assert_called_once_with(
            "behavior-v1-s3854376992136224865", "full"
        )
        self.assertEqual("3854376992136224865", response.meta.source_snapshot_id)
        self.assertEqual(UTC, response.meta.calculated_at.tzinfo)
        self.assertEqual("1234.50", response.data[0].purchase_amount_proxy)
        self.assertEqual(
            ["correctness subset; not the full 2% user sample"], response.meta.warnings
        )

    def test_day_overview_and_funnel_responses_are_ordered_by_date(self):
        early = date(2019, 10, 1)
        late = date(2019, 10, 2)
        self.repository.fetch_latest_publication.return_value = publication_row()
        self.repository.fetch_overview.return_value = [
            overview_row(window_type="DAY", window_start=late, window_end=late),
            overview_row(window_type="DAY", window_start=early, window_end=early),
        ]
        self.repository.fetch_funnel.return_value = [
            funnel_row(window_type="DAY", window_start=late, window_end=late),
            funnel_row(window_type="DAY", window_start=early, window_end=early),
        ]

        overview = self.service.get_overview("day")
        funnel = self.service.get_funnel("day")

        self.assertEqual([early, late], [point.window_start for point in overview.data])
        self.assertEqual([early, late], [point.window_start for point in funnel.data])

    def test_funnel_rates_use_decimal_rounding_and_null_zero_denominators(self):
        self.repository.fetch_latest_publication.return_value = publication_row()
        self.repository.fetch_funnel.return_value = [
            funnel_row(),
            funnel_row(
                window_type="DAY",
                window_start=date(2019, 10, 2),
                window_end=date(2019, 10, 2),
                view_sessions=0,
                view_to_cart_sessions=0,
                completed_sessions=0,
            ),
        ]

        response = self.service.get_funnel("full")

        self.assertEqual("0.666667", response.data[0].view_to_cart_rate)
        self.assertEqual("0.500000", response.data[0].cart_to_purchase_rate)
        self.assertEqual("0.333333", response.data[0].full_conversion_rate)
        self.assertIsNone(response.data[1].view_to_cart_rate)
        self.assertIsNone(response.data[1].cart_to_purchase_rate)
        self.assertIsNone(response.data[1].full_conversion_rate)

    def test_full_scope_requires_exact_count_and_omits_subset_warning(self):
        self.repository.fetch_latest_publication.return_value = publication_row(
            data_scope="stable-user-2pct-full", source_event_count=2_199_938
        )

        response = self.service.get_publication()

        self.assertEqual(2_199_938, response.meta.source_event_count)
        self.assertEqual([], response.meta.warnings)

    def test_publication_identity_and_scope_count_must_be_exact(self):
        invalid_rows = (
            publication_row(dataset_id="other"),
            publication_row(metric_version="behavior-v2"),
            publication_row(metric_run_id="behavior-v1-s9"),
            publication_row(source_event_count=1001),
            publication_row(data_scope="stable-user-2pct-full"),
        )
        for row in invalid_rows:
            with self.subTest(row=row):
                self.repository.reset_mock()
                self.repository.fetch_latest_publication.return_value = row
                with self.assertRaises(BehaviorMetricsUnavailableError):
                    self.service.get_publication()

    def test_missing_publication_and_database_errors_become_safe_unavailable_errors(self):
        for failure in (None, RuntimeError("SELECT password FROM internal.host")):
            with self.subTest(failure=failure):
                self.repository.reset_mock()
                if failure is None:
                    self.repository.fetch_latest_publication.return_value = None
                else:
                    self.repository.fetch_latest_publication.side_effect = failure
                with self.assertRaisesRegex(
                    BehaviorMetricsUnavailableError,
                    "^behavior metrics are unavailable$",
                ):
                    self.service.get_overview("full")

    def test_rankings_and_quality_preserve_stable_decimal_strings(self):
        self.repository.fetch_latest_publication.return_value = publication_row()
        self.repository.fetch_rankings.return_value = [ranking_row()]
        self.repository.fetch_quality.return_value = quality_row()

        rankings = self.service.get_rankings("brand", "full", "purchases", 20)
        quality = self.service.get_quality()

        self.assertEqual("12.30", rankings.data[0].purchase_amount_proxy)
        self.assertEqual("0.999002", quality.data.clean_event_rate)
        self.assertEqual("0.000998", quality.data.late_event_rate)

    def test_rankings_support_full_doris_decimal_precision(self):
        amount = "123456789012345678901234567890123456.78"
        self.repository.fetch_latest_publication.return_value = publication_row()
        self.repository.fetch_rankings.return_value = [
            ranking_row(purchase_amount_proxy=Decimal(amount))
        ]

        response = self.service.get_rankings("brand", "full", "amount", 20)

        self.assertEqual(amount, response.data[0].purchase_amount_proxy)

    def test_day_rankings_are_ordered_by_date_and_keep_rank_order_within_each_day(self):
        early = date(2019, 10, 1)
        late = date(2019, 10, 2)
        self.repository.fetch_latest_publication.return_value = publication_row()
        self.repository.fetch_rankings.return_value = [
            ranking_row(
                window_type="DAY",
                window_start=late,
                window_end=late,
                dimension_id="late",
                purchase_count=99,
            ),
            ranking_row(
                window_type="DAY",
                window_start=early,
                window_end=early,
                dimension_id="early-high",
                purchase_count=10,
            ),
            ranking_row(
                window_type="DAY",
                window_start=early,
                window_end=early,
                dimension_id="early-low",
                purchase_count=5,
            ),
        ]

        response = self.service.get_rankings("brand", "day", "purchases", 20)

        self.assertEqual(
            ["early-high", "early-low", "late"],
            [ranking.dimension_id for ranking in response.data],
        )

    def test_quality_revalidates_publication_source_count(self):
        self.repository.fetch_latest_publication.return_value = publication_row()
        self.repository.fetch_quality.return_value = quality_row(source_event_count=1001)

        with self.assertRaises(BehaviorMetricsUnavailableError):
            self.service.get_quality()

    def test_definitions_use_validated_catalog_without_querying_doris(self):
        response = self.service.get_definitions()

        self.assertEqual("behavior", response.domain)
        self.assertEqual("behavior-v1", response.metric_version)
        self.assertEqual(25, len(response.definitions))
        self.repository.assert_not_called()


class BehaviorMetricsRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        repository = Mock()
        catalog = MetricDefinitionCatalog.load(DEFINITIONS)
        real_service = BehaviorMetricsService(repository, catalog)
        repository.fetch_latest_publication.return_value = publication_row()
        repository.fetch_overview.return_value = [overview_row()]
        repository.fetch_funnel.return_value = [funnel_row()]
        repository.fetch_rankings.return_value = [ranking_row()]
        repository.fetch_quality.return_value = quality_row()
        self.service = Mock()
        self.service.get_publication.return_value = real_service.get_publication()
        self.service.get_overview.return_value = real_service.get_overview("full")
        self.service.get_funnel.return_value = real_service.get_funnel("full")
        self.service.get_rankings.return_value = real_service.get_rankings(
            "brand", "full", "purchases", 20
        )
        self.service.get_quality.return_value = real_service.get_quality()
        self.service.get_definitions.return_value = real_service.get_definitions()
        app = create_app(
            repository=Mock(),
            analysis_service=Mock(),
            tool_analysis_service=Mock(),
            readiness_service=Mock(),
            behavior_service=self.service,
        )
        self.client = TestClient(app)

    def test_all_six_versioned_routes_use_the_injected_service(self):
        responses = {
            "publication": self.client.get("/api/v1/behavior/publication"),
            "overview": self.client.get("/api/v1/behavior/overview"),
            "funnel": self.client.get("/api/v1/behavior/funnel"),
            "rankings": self.client.get(
                "/api/v1/behavior/rankings",
                params={
                    "dimension": "brand",
                    "window": "full",
                    "sort_by": "purchases",
                    "limit": 20,
                },
            ),
            "quality": self.client.get("/api/v1/behavior/quality"),
            "definitions": self.client.get("/api/v1/behavior/definitions"),
        }

        self.assertTrue(all(response.status_code == 200 for response in responses.values()))
        self.assertEqual(
            "behavior-v1", responses["rankings"].json()["meta"]["metric_version"]
        )
        self.assertEqual(
            "1234.50",
            responses["overview"].json()["data"][0]["purchase_amount_proxy"],
        )
        self.service.get_overview.assert_called_once_with("full")
        self.service.get_funnel.assert_called_once_with("full")
        self.service.get_rankings.assert_called_once_with("brand", "full", "purchases", 20)
        self.service.get_publication.assert_called_once_with()
        self.service.get_quality.assert_called_once_with()
        self.service.get_definitions.assert_called_once_with()

    def test_empty_rankings_are_a_successful_empty_list(self):
        empty = self.service.get_rankings.return_value.model_copy(update={"data": []})
        self.service.get_rankings.return_value = empty

        response = self.client.get(
            "/api/v1/behavior/rankings", params={"dimension": "product"}
        )

        self.assertEqual(200, response.status_code)
        self.assertEqual([], response.json()["data"])

    def test_invalid_ranking_parameters_return_422_before_service_call(self):
        response = self.client.get(
            "/api/v1/behavior/rankings",
            params={"dimension": "brand;drop table x", "limit": 101},
        )

        self.assertEqual(422, response.status_code)
        self.service.get_rankings.assert_not_called()

    def test_only_behavior_unavailable_is_mapped_to_safe_logged_503(self):
        self.service.get_overview.side_effect = BehaviorMetricsUnavailableError(
            "SELECT password FROM internal.host"
        )
        with patch("app.main.logger.error") as log_error:
            response = self.client.get("/api/v1/behavior/overview")

        self.assertEqual(503, response.status_code)
        self.assertEqual(
            {"detail": "behavior metrics are temporarily unavailable"}, response.json()
        )
        log_error.assert_called_once_with(
            "behavior_metrics_unavailable",
            extra={
                "stage": "behavior_overview",
                "error_type": "BehaviorMetricsUnavailableError",
            },
        )

        self.service.get_overview.side_effect = RuntimeError("programming defect")
        with self.assertRaisesRegex(RuntimeError, "programming defect"):
            self.client.get("/api/v1/behavior/overview")

    def test_missing_publication_and_database_failure_return_the_same_safe_503(self):
        repository = Mock()
        service = BehaviorMetricsService(repository, MetricDefinitionCatalog.load(DEFINITIONS))
        client = TestClient(
            create_app(
                repository=Mock(),
                analysis_service=Mock(),
                tool_analysis_service=Mock(),
                readiness_service=Mock(),
                behavior_service=service,
            )
        )
        for failure in (None, RuntimeError("password=secret host=internal")):
            with self.subTest(failure=failure):
                repository.reset_mock()
                if failure is None:
                    repository.fetch_latest_publication.return_value = None
                else:
                    repository.fetch_latest_publication.side_effect = failure
                response = client.get("/api/v1/behavior/overview")
                self.assertEqual(503, response.status_code)
                self.assertEqual(
                    {"detail": "behavior metrics are temporarily unavailable"},
                    response.json(),
                )


if __name__ == "__main__":
    unittest.main()
