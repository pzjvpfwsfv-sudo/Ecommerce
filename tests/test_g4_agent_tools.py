import json
import sys
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock
from uuid import UUID

from pydantic import ValidationError


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "services" / "api"))

from app.agent_models import ToolBudget  # noqa: E402
from app.agent_tools import build_tools  # noqa: E402
from app.auth_service import Principal  # noqa: E402
from app.behavior_service import BehaviorMetricsService, BehaviorMetricsUnavailableError  # noqa: E402
from app.knowledge_models import KnowledgeHit, SearchResult  # noqa: E402
from app.metric_definitions import MetricDefinitionCatalog  # noqa: E402
from app.order_service import OrderMetricsUnavailableError  # noqa: E402
from tests.test_behavior_metrics_api import (  # noqa: E402
    funnel_row, overview_row, publication_row, quality_row, ranking_row,
)
from tests.test_order_metrics_api import make_order_service  # noqa: E402


class G4AgentToolsTest(unittest.TestCase):
    def setUp(self):
        self.principal = Principal(id=17, username="analyst", role="analyst", csrf_token="private-csrf")
        self.knowledge = Mock()
        self.knowledge.search.return_value = SearchResult(
            hits=[KnowledgeHit(
                chunk_id=UUID("00000000-0000-0000-0000-000000000001"),
                document_id=UUID("00000000-0000-0000-0000-000000000002"),
                version_id=UUID("00000000-0000-0000-0000-000000000003"),
                section="订单口径", page=2, text="订单只按已发布数据计算。" * 50,
                source_label="项目文档", source_ref="orders-v1.md",
                keyword_rank=1, vector_rank=1, score=0.03,
                locator="/knowledge/documents/00000000-0000-0000-0000-000000000002/versions/00000000-0000-0000-0000-000000000003#chunk-00000000-0000-0000-0000-000000000001",
            )], mode="hybrid", elapsed_ms=8,
        )
        self.behavior_repo = Mock()
        self.behavior_repo.fetch_latest_publication.return_value = publication_row()
        self.behavior_repo.fetch_funnel.return_value = [funnel_row()]
        self.behavior_repo.fetch_overview.return_value = [overview_row()]
        self.behavior_repo.fetch_rankings.return_value = [ranking_row()]
        self.behavior_repo.fetch_quality.return_value = quality_row()
        catalog = MetricDefinitionCatalog.load(ROOT / "configs/metrics/behavior-v1.json")
        self.behavior = BehaviorMetricsService(self.behavior_repo, catalog)
        self.orders, self.order_repo = make_order_service()
        self.budget = ToolBudget()
        self.tools = {tool.name: tool for tool in build_tools(
            self.principal, self.knowledge, self.behavior, self.orders, self.budget,
        )}

    def invoke(self, name, values):
        return json.loads(self.tools[name].invoke(values))

    def test_only_three_read_only_tools_are_exposed(self):
        self.assertEqual(set(self.tools), {
            "search_knowledge", "get_published_behavior_metrics", "get_published_order_metrics",
        })
        for tool in self.tools.values():
            self.assertNotIn("sql", tool.name)
            self.assertNotIn("user_id", tool.args)
            self.assertNotIn("role", tool.args)

    def test_search_uses_bound_principal_and_bounded_excerpt(self):
        result = self.invoke("search_knowledge", {"query": "订单口径", "limit": 1})
        self.knowledge.search.assert_called_once_with("订单口径", self.principal, limit=1)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["kind"], "knowledge")
        self.assertEqual(result[0]["meta"]["source_ref"], "orders-v1.md")
        self.assertEqual(result[0]["meta"]["chunk_id"], "00000000-0000-0000-0000-000000000001")
        self.assertLessEqual(len(result[0]["text"]), 500)
        self.assertTrue(result[0]["target"].startswith("/knowledge/documents/"))
        self.assertNotIn("private-csrf", json.dumps(result))
        self.assertEqual(self.budget.calls, 1)

    def test_behavior_funnel_preserves_subset_warning_and_run_identity(self):
        result = self.invoke("get_published_behavior_metrics", {"view": "funnel", "window": "full"})
        self.behavior_repo.fetch_funnel.assert_called_once_with("behavior-v1-s3854376992136224865", "full")
        self.assertEqual(result[0]["kind"], "behavior")
        self.assertEqual(result[0]["meta"]["dataset_id"], "rees46-multicategory")
        self.assertEqual(result[0]["meta"]["metric_version"], "behavior-v1")
        self.assertEqual(result[0]["meta"]["metric_run_id"], "behavior-v1-s3854376992136224865")
        self.assertEqual(result[0]["meta"]["data_scope"], "g2c-correctness-subset")
        self.assertEqual(result[0]["meta"]["source_event_count"], 1002)
        self.assertIn("not the full", result[0]["meta"]["warnings"][0])
        self.assertEqual(result[0]["rows"][0]["full_conversion_rate"], "0.333333")
        self.assertEqual(result[0]["target"], "/behavior")

    def test_orders_payments_preserves_published_meta_without_detail(self):
        result = self.invoke("get_published_order_metrics", {"view": "payments", "window": "full"})
        self.order_repo.fetch_payments.assert_called_once()
        self.assertEqual(result[0]["kind"], "orders")
        self.assertEqual(result[0]["meta"]["dataset_id"], "olist-brazilian-ecommerce-v2")
        self.assertEqual(result[0]["meta"]["metric_version"], "orders-v1")
        self.assertEqual(result[0]["meta"]["metric_run_id"], f"orders-v1-b{'a' * 64}")
        self.assertEqual(result[0]["meta"]["source_order_count"], 2)
        self.assertEqual(result[0]["meta"]["source_timezone"], "unspecified")
        self.assertIn("币种", result[0]["meta"]["warnings"][0])
        self.assertEqual(result[0]["target"], "/orders")
        self.assertIn("payment_type", result[0]["rows"][0])
        self.assertNotIn("source_snapshots", json.dumps(result))

    def test_behavior_views_dispatch_only_to_the_matching_published_method(self):
        cases = (
            ({"view": "overview"}, "fetch_overview", "/behavior"),
            ({"view": "rankings", "dimension": "brand"}, "fetch_rankings", "/rankings"),
            ({"view": "quality"}, "fetch_quality", "/quality"),
        )
        for values, method, target in cases:
            with self.subTest(view=values["view"]):
                result = self.invoke("get_published_behavior_metrics", values)
                getattr(self.behavior_repo, method).assert_called_once()
                self.assertEqual(result[0]["target"], target)
                self.assertEqual(result[0]["meta"]["metric_run_id"], "behavior-v1-s3854376992136224865")
                self.behavior_repo.reset_mock()

    def test_order_views_dispatch_only_to_the_matching_published_method(self):
        cases = (
            ({"view": "overview"}, "fetch_overview", "/orders"),
            ({"view": "delivery"}, "fetch_delivery", "/fulfillment"),
            ({"view": "rankings", "dimension": "customer_state", "sort_by": "payment_value"}, "fetch_rankings", "/rankings"),
            ({"view": "reviews"}, "fetch_reviews", "/fulfillment"),
            ({"view": "quality"}, "fetch_quality", "/quality"),
        )
        for values, method, target in cases:
            with self.subTest(view=values["view"]):
                result = self.invoke("get_published_order_metrics", values)
                getattr(self.order_repo, method).assert_called_once()
                self.assertEqual(result[0]["target"], target)
                self.assertEqual(result[0]["meta"]["metric_run_id"], f"orders-v1-b{'a' * 64}")
                self.assertNotIn("source_snapshots", json.dumps(result))
                self.order_repo.reset_mock()

    def test_day_series_has_at_most_ten_rows_and_reports_truncation(self):
        from datetime import date

        self.behavior_repo.fetch_overview.return_value = [
            overview_row(window_type="DAY", window_start=date(2019, 10, day), window_end=date(2019, 10, day))
            for day in range(1, 13)
        ]
        result = self.invoke("get_published_behavior_metrics", {"view": "overview", "window": "day"})
        self.assertEqual(len(result[0]["rows"]), 10)
        self.assertEqual(result[0]["meta"]["total_rows"], 12)
        self.assertTrue(result[0]["meta"]["truncated"])
        self.assertEqual(result[0]["rows"][0]["window_start"], "2019-10-03")
        self.assertEqual(result[0]["rows"][-1]["window_start"], "2019-10-12")
        self.assertEqual(result[0]["meta"]["selection"], "latest_10_windows")

    def test_invalid_dates_and_full_date_range_never_reach_service(self):
        invalid = (
            {"view": "overview", "window": "day", "start_date": "2026-99-01", "end_date": "2026-10-01"},
            {"view": "overview", "window": "day", "start_date": "2018-01-01"},
            {"view": "overview", "window": "full", "start_date": "2018-01-01", "end_date": "2018-01-31"},
            {"view": "overview", "window": "month", "start_date": "2018-02-01", "end_date": "2018-01-01"},
        )
        for values in invalid:
            with self.subTest(values=values), self.assertRaises(ValidationError):
                self.tools["get_published_order_metrics"].invoke(values)
        self.order_repo.fetch_overview.assert_not_called()

    def test_ranking_shape_and_limit_are_validated_before_service(self):
        invalid = (
            {"view": "rankings", "dimension": "product", "sort_by": "payment_value"},
            {"view": "rankings", "dimension": "customer_state", "sort_by": "item_value"},
            {"view": "rankings", "dimension": "city"},
            {"view": "rankings", "dimension": "product", "limit": 11},
            {"view": "rankings", "limit": 5},
        )
        for values in invalid:
            with self.subTest(values=values), self.assertRaises(ValidationError):
                self.tools["get_published_order_metrics"].invoke(values)
        self.order_repo.fetch_rankings.assert_not_called()
        with self.assertRaises(ValidationError):
            self.tools["get_published_behavior_metrics"].invoke({
                "view": "rankings", "dimension": "product", "limit": 11,
            })
        self.behavior_repo.fetch_rankings.assert_not_called()

    def test_model_cannot_spoof_identity_or_add_tool_parameters(self):
        for tool_name, values in (
            ("search_knowledge", {"query": "订单", "role": "admin"}),
            ("get_published_behavior_metrics", {"view": "funnel", "user_id": 1}),
            ("get_published_order_metrics", {"view": "overview", "role": "admin"}),
        ):
            with self.subTest(name=tool_name), self.assertRaises(ValidationError):
                self.tools[tool_name].invoke(values)
        self.assertEqual(self.budget.calls, 0)

    def test_budget_stops_sixth_call_and_evidence_ids_are_unique(self):
        for _ in range(5):
            self.invoke("search_knowledge", {"query": "订单"})
        self.assertEqual(self.budget.calls, 5)
        self.assertEqual(len(self.budget.evidence), 5)
        self.assertEqual(len({item.evidence_id for item in self.budget.evidence}), 5)
        with self.assertRaises(RuntimeError):
            self.tools["get_published_order_metrics"].invoke({"view": "overview"})
        self.order_repo.fetch_overview.assert_not_called()

    def test_knowledge_excerpt_limit_applies_across_the_whole_request(self):
        original = self.knowledge.search.return_value.hits[0]
        hits = [replace(original, chunk_id=UUID(int=index)) for index in range(1, 6)]
        self.knowledge.search.return_value = SearchResult(hits=hits, mode="hybrid", elapsed_ms=8)
        first = self.invoke("search_knowledge", {"query": "订单", "limit": 3})
        second = self.invoke("search_knowledge", {"query": "支付", "limit": 5})
        self.assertEqual((len(first), len(second)), (3, 2))
        self.assertEqual(self.knowledge.search.call_args_list[1].kwargs["limit"], 2)
        self.assertEqual(sum(item.kind == "knowledge" for item in self.budget.evidence), 5)

    def test_failed_search_does_not_leave_partial_evidence(self):
        valid = self.knowledge.search.return_value.hits[0]
        invalid = replace(valid, chunk_id=UUID(int=2), text=None)
        self.knowledge.search.return_value = SearchResult(
            hits=[valid, invalid], mode="hybrid", elapsed_ms=8,
        )
        with self.assertRaises(TypeError):
            self.tools["search_knowledge"].invoke({"query": "订单"})
        self.assertEqual(self.budget.evidence, [])

    def test_unpublished_metrics_fail_without_evidence(self):
        self.behavior_repo.fetch_latest_publication.return_value = None
        with self.assertRaises(BehaviorMetricsUnavailableError):
            self.tools["get_published_behavior_metrics"].invoke({"view": "funnel"})
        self.assertEqual(self.budget.evidence, [])
        self.order_repo.fetch_latest_publication.return_value = None
        with self.assertRaises(OrderMetricsUnavailableError):
            self.tools["get_published_order_metrics"].invoke({"view": "payments"})
        self.assertEqual(self.budget.evidence, [])

    def test_viewer_cannot_build_agent_tools(self):
        viewer = Principal(id=18, username="viewer", role="viewer", csrf_token="private")
        with self.assertRaises(PermissionError):
            build_tools(viewer, self.knowledge, self.behavior, self.orders, ToolBudget())


if __name__ == "__main__":
    unittest.main()
