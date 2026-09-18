import json
from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parent.parent
DEFINITIONS = ROOT / "configs" / "metrics" / "behavior-v1.json"
DORIS_SCHEMA = ROOT / "infra" / "compose" / "doris" / "init" / "02_create_behavior_metrics.sql"
TRINO_TEMPLATE = ROOT / "jobs" / "sql" / "18_g2d_behavior_metrics.sql.template"

EXPECTED_METRICS = {
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
EXPECTED_ITEM_KEYS = {
    "metric_name",
    "display_name",
    "formula",
    "numerator",
    "denominator",
    "source_fields",
    "allowed_windows",
    "additive",
    "null_policy",
    "limitations",
    "forbidden_claims",
}
EXPECTED_DORIS_TABLES = (
    "behavior_metric_publications",
    "behavior_overview_metrics",
    "behavior_funnel_metrics",
    "behavior_dimension_metrics",
    "behavior_quality_metrics",
)


class G2dArtifactContractTests(unittest.TestCase):
    def test_definition_catalog_declares_fixed_identity_and_proxy_warning(self):
        """Protect consumers from catalog identity or public metric drift."""
        payload = json.loads(DEFINITIONS.read_text(encoding="utf-8"))
        self.assertEqual("behavior", payload["domain"])
        self.assertEqual("rees46-multicategory", payload["dataset_id"])
        self.assertEqual("behavior-v1", payload["metric_version"])
        by_name = {item["metric_name"]: item for item in payload["definitions"]}
        self.assertEqual(EXPECTED_METRICS, set(by_name))
        self.assertEqual(len(payload["definitions"]), len(by_name))
        for item in payload["definitions"]:
            self.assertEqual(EXPECTED_ITEM_KEYS, set(item))
        self.assertIn("purchase_amount_proxy", by_name)
        self.assertEqual(["DAY", "FULL"], by_name["unique_user_count"]["allowed_windows"])
        self.assertIn("GMV", by_name["purchase_amount_proxy"]["forbidden_claims"])
        self.assertIn("收入", by_name["purchase_amount_proxy"]["forbidden_claims"])

    def test_doris_schema_uses_run_scoped_tables_without_destructive_sql(self):
        """Protect published runs from destructive or non-idempotent schema changes."""
        sql = DORIS_SCHEMA.read_text(encoding="utf-8")
        for table in EXPECTED_DORIS_TABLES:
            self.assertIn(f"CREATE TABLE IF NOT EXISTS {table}", sql)
            self.assertIn(f"DISTRIBUTED BY HASH(metric_run_id) BUCKETS 1", sql)
        self.assertNotRegex(sql.upper(), r"\b(?:DROP|TRUNCATE|DELETE)\b")
        self.assertNotIn("CURRENT_TIMESTAMP", sql.upper())
        self.assertIn("CREATE DATABASE IF NOT EXISTS analytics", sql)
        self.assertEqual(1, len(re.findall(r"(?mi)^USE analytics;$", sql)))
        self.assertIn("metric_run_id", sql)
        self.assertIn("source_snapshot_id", sql)
        self.assertIn("UNIQUE KEY(metric_run_id, window_type, window_start)", sql)
        self.assertIn(
            "UNIQUE KEY(metric_run_id, window_type, window_start, dimension_type, dimension_id)",
            sql,
        )
        self.assertIn("purchase_amount_proxy DECIMAL(38,2)", sql)

    def test_trino_template_has_five_fixed_results_and_no_dynamic_source(self):
        """Protect every result family from current-table reads or caller-controlled SQL."""
        sql = TRINO_TEMPLATE.read_text(encoding="utf-8")
        result_names = re.findall(r"(?m)^-- result:([a-z_]+)$", sql)
        self.assertEqual(
            ["source_identity", "overview", "funnel", "dimension", "quality"], result_names
        )
        self.assertEqual(5, len([part for part in sql.split(";") if part.strip()]))
        self.assertIn("lakehouse.analytics.real_behavior_detail_v1", sql)
        self.assertEqual({"__SNAPSHOT_ID__"}, set(re.findall(r"__[A-Z0-9_]+__", sql)))
        self.assertIn("FOR VERSION AS OF __SNAPSHOT_ID__", sql)
        self.assertIn("source_row_number", sql)
        self.assertIn("purchase_amount_proxy", sql)
        self.assertEqual(5, sql.count("WITH base AS"))
        self.assertEqual(5, sql.count("FOR VERSION AS OF __SNAPSHOT_ID__"))
        self.assertIn("event_ts, source_file, source_row_number, event_id", sql)
        self.assertIn("category_id", sql)
        self.assertIn("category_code", sql)
        self.assertIn("is_unknown", sql)
        self.assertIn("reconciliation_status", sql)
        self.assertIn('"real_behavior_detail_v1$snapshots"', sql)


if __name__ == "__main__":
    unittest.main()
