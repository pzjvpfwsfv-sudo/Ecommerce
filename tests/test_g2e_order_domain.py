from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/verify_g2e_order_domain.ps1"
BUNDLE = "a" * 64
RUN_ID = f"orders-v1-b{BUNDLE}"
REVISION = "1" * 40

SOURCE_TABLES = {
    "orders": "orders_src_v1",
    "order_items": "order_items_src_v1",
    "order_payments": "order_payments_src_v1",
    "order_reviews": "order_reviews_src_v1",
    "customers": "customers_src_v1",
    "products": "products_src_v1",
    "sellers": "sellers_src_v1",
    "geolocation": "geolocation_src_v1",
    "category_translation": "category_translation_src_v1",
}
CURATED_TABLES = [
    "customer_dim_v1",
    "category_dim_v1",
    "product_dim_v1",
    "seller_dim_v1",
    "geolocation_dim_v1",
    "order_fact_v1",
    "order_item_fact_v1",
    "payment_fact_v1",
    "review_fact_v1",
]
FAMILIES = ["overview", "delivery", "payment", "ranking", "review", "quality"]
ENDPOINTS = [
    "/api/v1/orders/publication",
    "/api/v1/orders/overview?window=full",
    "/api/v1/orders/delivery?window=full",
    "/api/v1/orders/payments?window=full",
    "/api/v1/orders/rankings?dimension=category&window=full&sort_by=item_value&limit=20",
    "/api/v1/orders/reviews?window=full",
    "/api/v1/orders/quality",
    "/api/v1/metrics/definitions?domain=orders&version=orders-v1",
]


def _meta(source_snapshots: dict[str, str], curated_snapshots: dict[str, str]) -> dict:
    return {
        "dataset_id": "olist-brazilian-ecommerce-v2",
        "metric_version": "orders-v1",
        "metric_run_id": RUN_ID,
        "source_bundle_sha256": BUNDLE,
        "source_snapshots": source_snapshots,
        "curated_snapshots": curated_snapshots,
        "window_start": "2017-01-01",
        "window_end": "2017-01-31",
        "source_timezone": "unspecified",
        "source_currency": None,
        "source_order_count": 2,
        "calculated_at": "2026-09-22T08:00:00Z",
        "implementation_revision": REVISION,
        "warnings": ["源数据币种尚未核验，不展示货币符号或执行汇率换算。"],
    }


def _overview(window_type: str, start: str, end: str) -> dict:
    return {
        "window_type": window_type,
        "window_start": start,
        "window_end": end,
        "order_count": 2,
        "delivered_order_count": 2,
        "canceled_order_count": 0,
        "unavailable_order_count": 0,
        "status_eligible_order_count": 2,
        "status_excluded_order_count": 0,
        "delivered_rate": "1.000000",
        "canceled_rate": "0.000000",
        "unique_customer_count": 1,
        "repeat_customer_count": 1,
        "repeat_customer_rate": "1.000000",
        "item_row_count": 2,
        "item_value_sum": "30.00",
        "freight_value_sum": "5.00",
        "payment_value_sum": "35.00",
        "items_per_order_avg": "1.000000",
    }


def valid_synthetic_acceptance() -> dict:
    source_snapshots = {
        table: str(101 + index) for index, table in enumerate(SOURCE_TABLES.values())
    }
    curated_snapshots = {
        table: str(201 + index) for index, table in enumerate(CURATED_TABLES)
    }
    files = []
    source_reports = []
    for entity, table in SOURCE_TABLES.items():
        files.append(
            {
                "entity": entity,
                "name": f"{entity}.csv",
                "sha256": "d" * 64,
                "bytes": 100,
                "row_count": 2,
                "header": ["fixture"],
                "normalized": {
                    "name": f"normalized/{entity}.jsonl",
                    "sha256": "b" * 64,
                    "bytes": 200,
                    "row_count": 2,
                    "source_row_id_sequence_sha256": "c" * 64,
                },
            }
        )
        source_reports.append(
            {
                "status": "PASS",
                "dataset_id": "olist-brazilian-ecommerce-v2",
                "entity": entity,
                "source_bundle_sha256": BUNDLE,
                "normalized_sha256": "b" * 64,
                "source_row_id_sequence_sha256": "c" * 64,
                "expected_row_count": 2,
                "target_table": table,
                "source_file": f"{entity}.csv",
                "iceberg": {
                    "row_count": 2,
                    "snapshot_id": source_snapshots[table],
                    "snapshot_committed_at": "2026-09-22 08:00:00.000 UTC",
                },
            }
        )

    family_counts = {
        "overview": 3,
        "delivery": 3,
        "payment": 4,
        "ranking": 5,
        "review": 3,
        "quality": 1,
    }
    family_digests = {
        family: format(index + 1, "x") * 64 for index, family in enumerate(FAMILIES)
    }
    candidates = {
        family: {
            "row_count": family_counts[family],
            "sha256": family_digests[family],
            "file_sha256": "e" * 64,
        }
        for family in FAMILIES
    }
    publication = {
        "metric_run_id": RUN_ID,
        "dataset_id": "olist-brazilian-ecommerce-v2",
        "metric_version": "orders-v1",
        "source_bundle_sha256": BUNDLE,
        "source_snapshots_json": json.dumps(
            source_snapshots, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ),
        "curated_snapshots_json": json.dumps(
            curated_snapshots, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ),
        "implementation_revision": REVISION,
        "source_order_count": 2,
        "window_start": "2017-01-01",
        "window_end": "2017-01-31",
        "calculated_at": "2026-09-22T08:00:00Z",
        "published_at": "2026-09-22T08:01:00Z",
        "status": "PUBLISHED",
    }
    for family in FAMILIES:
        publication[f"{family}_row_count"] = family_counts[family]
        publication[f"{family}_sha256"] = family_digests[family]

    meta = _meta(source_snapshots, curated_snapshots)
    overview = _overview("FULL", "2017-01-01", "2017-01-31")
    delivery = {
        "window_type": "FULL",
        "window_start": "2017-01-01",
        "window_end": "2017-01-31",
        "delivery_eligible_order_count": 2,
        "delivery_excluded_order_count": 0,
        "delivery_days_avg": "3.000000",
        "delivery_days_p50": "3.000000",
        "delivery_days_p90": "4.000000",
        "late_delivery_order_count": 0,
        "late_delivery_eligible_order_count": 2,
        "late_delivery_excluded_order_count": 0,
        "late_delivery_rate": "0.000000",
    }
    payments = [
        {
            "window_type": "FULL",
            "window_start": "2017-01-01",
            "window_end": "2017-01-31",
            "payment_type": "__ALL__",
            "is_all": True,
            "global_order_count": 2,
            "payment_order_count": 2,
            "payment_row_count": 2,
            "installment_order_count": 1,
            "payment_value_sum": "35.00",
        }
    ]
    rankings = [
        {
            "window_type": "FULL",
            "window_start": "2017-01-01",
            "window_end": "2017-01-31",
            "dimension_type": "category",
            "dimension_id": "books",
            "dimension_name": "Books",
            "is_unknown": False,
            "ranking_order_count": 2,
            "ranking_item_row_count": 2,
            "ranking_customer_count": 1,
            "ranking_item_value_sum": "30.00",
            "ranking_freight_value_sum": "5.00",
            "ranking_payment_value_sum": None,
            "ranking_late_delivery_order_count": None,
            "ranking_late_delivery_eligible_order_count": None,
            "ranking_late_delivery_rate": None,
            "payment_value_is_additive": None,
        }
    ]
    reviews = [
        {
            "window_type": "FULL",
            "window_start": "2017-01-01",
            "window_end": "2017-01-31",
            "review_row_count": 2,
            "reviewed_order_count": 2,
            "all_order_count": 2,
            "review_coverage_rate": "1.000000",
            "review_score_avg": "4.000000",
            "low_score_order_count": 0,
            "low_score_rate": "0.000000",
            "multi_review_order_count": 0,
        }
    ]
    quality = {
        "window_type": "FULL",
        "window_start": "2017-01-01",
        "window_end": "2017-01-31",
        "source_row_count": 2,
        "iceberg_row_count": 2,
        "duplicate_key_count": 0,
        "orphan_key_count": 0,
        "invalid_value_count": 0,
        "temporal_anomaly_count": 0,
        "amount_comparable_order_count": 2,
        "amount_reconciled_order_count": 2,
        "amount_mismatch_order_count": 0,
        "amount_reconciliation_rate": "1.000000",
        "payment_item_freight_abs_difference_avg": "0.000000",
        "payment_item_freight_abs_difference_p50": "0.000000",
        "payment_item_freight_abs_difference_p90": "0.000000",
        "raw_row_counts": {entity: 2 for entity in SOURCE_TABLES},
        "normalized_row_counts": {entity: 2 for entity in SOURCE_TABLES},
        "iceberg_row_counts": {table: 2 for table in SOURCE_TABLES.values()},
        "normalized_sha256": {entity: "b" * 64 for entity in SOURCE_TABLES},
        "source_snapshots": source_snapshots,
        "curated_snapshots": curated_snapshots,
        "fact_reconciliations": {
            "order_fact_expected_count": 2,
            "order_fact_row_count": 2,
            "order_item_fact_expected_count": 2,
            "order_item_fact_row_count": 2,
            "payment_fact_expected_count": 2,
            "payment_fact_row_count": 2,
            "review_fact_expected_count": 2,
            "review_fact_row_count": 2,
        },
        "reportable_quality": {
            "unknown_order_status_count": 0,
            "unknown_payment_type_count": 0,
            "multi_review_order_count": 0,
            "lifecycle_order_anomaly_count": 0,
            "payment_item_total_mismatch_count": 0,
        },
        "reconciliation_status": "PASS",
    }
    day = [_overview("DAY", "2017-01-01", "2017-01-01")]
    month = [_overview("MONTH", "2017-01-01", "2017-01-31")]
    responses = {
        "publication": {
            "meta": meta,
            "data": {
                "published_at": "2026-09-22T08:01:00Z",
                **{
                    key: value
                    for family in FAMILIES
                    for key, value in (
                        (f"{family}_row_count", family_counts[family]),
                        (f"{family}_sha256", family_digests[family]),
                    )
                },
                "status": "PUBLISHED",
            },
        },
        "overview": {"meta": meta, "data": [overview]},
        "delivery": {"meta": meta, "data": [delivery]},
        "payments": {"meta": meta, "data": payments},
        "rankings": {"meta": meta, "data": rankings},
        "reviews": {"meta": meta, "data": reviews},
        "quality": {"meta": meta, "data": quality},
        "definitions": {
            "domain": "orders",
            "dataset_id": "olist-brazilian-ecommerce-v2",
            "metric_version": "orders-v1",
            "definitions": [{"metric_name": "order_count"}],
        },
        "day_overview": {"meta": meta, "data": day},
        "month_overview": {"meta": meta, "data": month},
    }
    expected_rows = {
        key: deepcopy(responses[key]["data"])
        for key in (
            "overview",
            "delivery",
            "payments",
            "rankings",
            "reviews",
            "quality",
            "day_overview",
            "month_overview",
        )
    }
    return {
        "schema_version": 1,
        "status": "PASS",
        "dataset_id": "olist-brazilian-ecommerce-v2",
        "source_bundle_sha256": BUNDLE,
        "metric_run_id": RUN_ID,
        "implementation_revision": REVISION,
        "window_start": "2017-01-01",
        "window_end": "2017-01-31",
        "source_order_count": 2,
        "manifest": {
            "schema_version": 1,
            "dataset_id": "olist-brazilian-ecommerce-v2",
            "kaggle_version": "synthetic-v1",
            "source_url": "https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce",
            "acquired_at": "2026-09-22T00:00:00Z",
            "license": {"name": "synthetic", "url": "https://example.test/license"},
            "archive": {"name": "olist.zip", "sha256": "f" * 64, "bytes": 999},
            "source_bundle_sha256": BUNDLE,
            "files": files,
        },
        "source_reports": source_reports,
        "curated": {
            "status": "PASS",
            "dataset_id": "olist-brazilian-ecommerce-v2",
            "source_bundle_sha256": BUNDLE,
            "source_snapshots": source_snapshots,
            "curated_snapshots": curated_snapshots,
            "hard_gate": {"duplicate_order_key_count": 0, "orphan_item_order_count": 0},
            "reportable_quality": quality["reportable_quality"],
            "grain_reconciliation": quality["fact_reconciliations"],
        },
        "refresh": {
            "status": "published",
            "metric_run_id": RUN_ID,
            "source_bundle_sha256": BUNDLE,
            "implementation_revision": REVISION,
            "candidates": candidates,
            "publication": publication,
        },
        "doris": {
            "publication": publication,
            "families": {
                family: {
                    "row_count": family_counts[family],
                    "sha256": family_digests[family],
                }
                for family in FAMILIES
            },
            "api_rows": expected_rows,
        },
        "api": {"endpoints": list(ENDPOINTS), "responses": responses},
        "verification": {
            "trino_snapshot_identity": True,
            "trino_fact_reconciliation": True,
            "doris_candidate_digests": True,
            "api_full_contracts": True,
            "api_day_range": True,
            "api_month_range": True,
            "invalid_requests_422": True,
        },
        "measured": {
            "day_range": {"start_date": "2017-01-01", "end_date": "2017-01-01"},
            "month_range": {"start_date": "2017-01-01", "end_date": "2017-01-31"},
        },
        "verified_at": "2026-09-22T08:02:00Z",
    }


class PowerShellAcceptanceTestCase(unittest.TestCase):
    def setUp(self):
        temp_parent = ROOT / "tmp/graduation/test-temp"
        temp_parent.mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=temp_parent)
        self.addCleanup(self.temp.cleanup)
        self.temp_path = Path(self.temp.name)

    def run_script(self, evidence: dict, body: str = "Assert-G2eAcceptanceEvidence -Evidence $evidence | Out-Null"):
        executable = shutil.which("pwsh") or shutil.which("powershell")
        if executable is None:
            self.skipTest("PowerShell is required for G2-E acceptance coverage")
        evidence_path = self.temp_path / "evidence.json"
        evidence_path.write_text(
            json.dumps(evidence, ensure_ascii=False, separators=(",", ":")), encoding="utf-8"
        )
        command = (
            "$ErrorActionPreference='Stop'; "
            ". './scripts/verify_g2e_order_domain.ps1' -FunctionsOnly; "
            f"$evidence=Get-Content -LiteralPath '{evidence_path}' -Raw -Encoding UTF8 | ConvertFrom-Json; "
            f"{body}; 'PASS'"
        )
        return subprocess.run(
            [executable, "-NoLogo", "-NoProfile", "-NonInteractive", "-Command", command],
            cwd=ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
            check=False,
        )


class G2eOrderDomainContractTests(PowerShellAcceptanceTestCase):
    def test_valid_evidence_and_all_four_public_assertions_pass(self):
        evidence = valid_synthetic_acceptance()
        body = (
            "$identity=Assert-G2eAcceptanceIdentity -Manifest $evidence.manifest "
            "-SourceReports $evidence.source_reports -CuratedReport $evidence.curated "
            "-RefreshReport $evidence.refresh -Publication $evidence.doris.publication; "
            "Assert-G2eApiMeta -Meta $evidence.api.responses.overview.meta -Identity $identity | Out-Null; "
            "Assert-G2eApiRows -Family overview -Actual $evidence.api.responses.overview.data "
            "-Expected $evidence.doris.api_rows.overview | Out-Null; "
            "Assert-G2eAcceptanceEvidence -Evidence $evidence | Out-Null"
        )
        result = self.run_script(evidence, body)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        self.assertIn("PASS", result.stdout)

    def test_identity_digest_and_snapshot_mutations_fail_closed(self):
        cases = {}
        cases["bundle"] = lambda item: item.__setitem__("source_bundle_sha256", "f" * 64)
        cases["run"] = lambda item: item["refresh"].__setitem__("metric_run_id", "orders-v1-b" + "f" * 64)
        cases["source snapshot"] = lambda item: item["source_reports"][0]["iceberg"].__setitem__("snapshot_id", "999")
        cases["curated snapshot"] = lambda item: item["curated"]["curated_snapshots"].__setitem__("order_fact_v1", "999")
        cases["source order count"] = lambda item: item["doris"]["publication"].__setitem__("source_order_count", 3)
        cases["window"] = lambda item: item.__setitem__("window_end", "2017-02-01")
        cases["revision"] = lambda item: item.__setitem__("implementation_revision", "2" * 40)
        cases["row count"] = lambda item: item["doris"]["families"]["overview"].__setitem__("row_count", 4)
        cases["digest"] = lambda item: item["doris"]["families"]["overview"].__setitem__("sha256", "f" * 64)
        for label, mutate in cases.items():
            with self.subTest(label=label):
                evidence = valid_synthetic_acceptance()
                mutate(evidence)
                result = self.run_script(evidence)
                self.assertNotEqual(0, result.returncode, result.stdout)

    def test_api_quality_metadata_endpoint_and_row_mutations_fail_closed(self):
        cases = {}
        cases["quality"] = lambda item: item["api"]["responses"]["quality"]["data"].__setitem__("reconciliation_status", "FAIL")
        cases["timezone"] = lambda item: item["api"]["responses"]["overview"]["meta"].__setitem__("source_timezone", "UTC")
        cases["currency warning"] = lambda item: item["api"]["responses"]["overview"]["meta"].__setitem__("warnings", ["wrong"])
        cases["endpoint"] = lambda item: item["api"]["endpoints"].pop()
        cases["response metadata"] = lambda item: item["api"]["responses"]["delivery"]["meta"].__setitem__("metric_run_id", "orders-v1-b" + "f" * 64)
        cases["response row"] = lambda item: item["api"]["responses"]["overview"]["data"][0].__setitem__("order_count", 3)
        for label, mutate in cases.items():
            with self.subTest(label=label):
                evidence = valid_synthetic_acceptance()
                mutate(evidence)
                result = self.run_script(evidence)
                self.assertNotEqual(0, result.returncode, result.stdout)

    def test_acceptance_path_is_fixed_and_existing_evidence_is_never_overwritten(self):
        evidence = valid_synthetic_acceptance()
        workspace = self.temp_path / "workspace"
        workspace.mkdir()
        expected = workspace / "tmp/graduation/g2e" / BUNDLE / "acceptance.json"
        body = (
            f"$first=Write-G2eAcceptanceEvidence -RepositoryRoot '{workspace}' -Evidence $evidence; "
            "$rejected=$false; try { Write-G2eAcceptanceEvidence "
            f"-RepositoryRoot '{workspace}' -Evidence $evidence | Out-Null }} catch {{ $rejected=$true }}; "
            f"if ($first -cne '{expected}' -or -not $rejected) {{ throw 'path/no-overwrite contract failed' }}"
        )
        result = self.run_script(evidence, body)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        self.assertEqual(evidence, json.loads(expected.read_text(encoding="utf-8")))


class G2eOrderDomainArtifactTests(unittest.TestCase):
    def test_verifier_is_read_only_and_contains_no_local_credentials_or_profile_paths(self):
        text = SCRIPT.read_text(encoding="utf-8")
        self.assertNotRegex(text, r"(?im)^\s*(DROP|DELETE|TRUNCATE|UPDATE|CREATE OR REPLACE)\b")
        self.assertNotRegex(text, r"(?i)C:\\Users\\|AppData|kaggle\.json")
        self.assertNotRegex(text, r"(?i)(password|token|cookie)\s*=\s*['\"][^'\"]+['\"]")
        for name in (
            "Assert-G2eAcceptanceIdentity",
            "Assert-G2eApiMeta",
            "Assert-G2eApiRows",
            "Assert-G2eAcceptanceEvidence",
        ):
            self.assertRegex(text, rf"function\s+{re.escape(name)}\b")
        self.assertIn("tmp/graduation/g2e", text)
        self.assertIn("acceptance.json", text)


if __name__ == "__main__":
    unittest.main()
