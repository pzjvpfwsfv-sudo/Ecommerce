import { HttpResponse, http } from "msw";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, it } from "vitest";

import { Quality } from "./Quality";
import { server } from "../test/handlers";

const orderMeta = {
  dataset_id: "olist-brazilian-ecommerce-v2", metric_version: "orders-v1",
  metric_run_id: "orders-v1-b" + "a".repeat(64), source_bundle_sha256: "b".repeat(64),
  source_snapshots: { orders_src_v1: "1" }, curated_snapshots: { order_fact_v1: "2" },
  window_start: "2016-09-04", window_end: "2018-10-17", source_timezone: "unspecified",
  source_currency: null, source_order_count: 99441,
  calculated_at: "2026-09-20T00:00:00Z", implementation_revision: "c".repeat(40), warnings: ["历史快照"],
};
const behaviorMeta = {
  dataset_id: "rees46-multicategory", metric_version: "behavior-v1",
  metric_run_id: "behavior-v1-b" + "a".repeat(64), source_snapshot_id: "snapshot-1",
  data_scope: "g2c-correctness-subset", source_event_count: 1002,
  window_start: "2020-01-01", window_end: "2020-01-02",
  calculated_at: "2026-09-20T00:00:00Z", warnings: ["当前为正确性子集，不代表完整数据"],
};
const orderQuality = {
  window_type: "FULL", window_start: "2016-09-04", window_end: "2018-10-17",
  source_row_count: 99441, iceberg_row_count: 99441,
  duplicate_key_count: 0, orphan_key_count: 0, invalid_value_count: 0, temporal_anomaly_count: 0,
  amount_comparable_order_count: 90000, amount_reconciled_order_count: 89000,
  amount_mismatch_order_count: 1000, amount_reconciliation_rate: "0.988889",
  raw_row_counts: { orders: 99441, reviews: 100000 },
  normalized_row_counts: { orders: 99441, reviews: 100000 },
  iceberg_row_counts: { order_fact_v1: 99441, order_reviews_src_v1: 100000 },
  normalized_sha256: { orders: "a".repeat(64), reviews: "b".repeat(64) },
  source_snapshots: { orders_src_v1: "1" }, curated_snapshots: { order_fact_v1: "2" },
  fact_reconciliations: { order_fact_expected_count: 99441, order_fact_row_count: 99441 },
  reportable_quality: { duplicate_review_id_count: 789, unknown_order_status_count: 0,
    multi_review_order_count: 24 },
  reconciliation_status: "PASS",
};
const behaviorQuality = {
  window_type: "FULL", window_start: "2020-01-01", window_end: "2020-01-02",
  source_event_count: 1002, clean_event_count: 990, late_event_count: 12,
  clean_event_rate: "0.988024", late_event_rate: "0.011976",
  distinct_event_count: 1000, duplicate_event_count: 2,
  missing_session_count: 7, unknown_category_count: 3, unknown_brand_count: 1,
  invalid_event_type_count: 0, empty_key_id_count: 0, invalid_price_count: 0,
  invalid_derived_date_count: 0, overview_event_count: 990,
  reconciliation_status: "PASS",
};

beforeEach(() => {
  server.use(
    http.get("/api/v1/orders/quality", () => HttpResponse.json({ meta: orderMeta, data: orderQuality })),
    http.get("/api/v1/orders/publication", () => HttpResponse.json({ meta: orderMeta, data: { status: "PUBLISHED", published_at: "2026-09-21T00:00:00Z" } })),
    http.get("/api/v1/behavior/quality", () => HttpResponse.json({ meta: behaviorMeta, data: behaviorQuality })),
    http.get("/api/v1/behavior/publication", () => HttpResponse.json({ meta: behaviorMeta, data: { status: "PUBLISHED", published_at: "2026-09-21T00:00:00Z" } })),
  );
});

it("keeps reportable Olist issues visible even when reconciliation is PASS", async () => {
  render(<Quality />);
  expect(await screen.findByText("PASS")).toBeVisible();
  expect(screen.getByText("重复 review_id")).toBeVisible();
  expect(screen.getByText("789")).toBeVisible();
  expect(screen.getByText(/可报告异常不等于门禁失败/)).toBeVisible();
  expect(screen.getAllByText("原始来源").length).toBeGreaterThan(0);
  expect(screen.getAllByText("规范化").length).toBeGreaterThan(0);
  expect(screen.getByText("入湖 Iceberg")).toBeVisible();
  await userEvent.click(screen.getByText("查看逐表行数"));
  expect(screen.getByText("order_fact_v1")).toBeVisible();
  expect(screen.getByText("order_fact_expected_count")).toBeVisible();
});

it("shows a separate REES46 quality path and its subset warning", async () => {
  render(<Quality />);
  expect(await screen.findByText("重复 review_id")).toBeVisible();
  await userEvent.click(screen.getByRole("button", { name: "REES46 行为域" }));
  expect(await screen.findByText("当前为正确性子集，不代表完整数据")).toBeVisible();
  expect(screen.getByText("1,002")).toBeVisible();
  expect(screen.getAllByText("990").length).toBeGreaterThan(0);
  expect(screen.getByText("清洗 / 迟到")).toBeVisible();
  expect(screen.queryByText("重复 review_id")).not.toBeInTheDocument();
});

it("shows 503 without substituting preview data", async () => {
  server.use(http.get("/api/v1/orders/quality", () => new HttpResponse(null, { status: 503 })));
  render(<Quality />);
  expect(await screen.findByText(/指标暂不可用/)).toBeVisible();
  expect(screen.queryByText("789")).not.toBeInTheDocument();
});

it("rejects quality and publication from different runs", async () => {
  server.use(http.get("/api/v1/orders/publication", () => HttpResponse.json({
    meta: { ...orderMeta, metric_run_id: "orders-v1-b" + "d".repeat(64) },
    data: { status: "PUBLISHED", published_at: "2026-09-21T00:00:00Z" },
  })));
  render(<Quality />);
  expect(await screen.findByText(/数据未能通过校验/)).toBeVisible();
  expect(screen.queryByText("789")).not.toBeInTheDocument();
});
