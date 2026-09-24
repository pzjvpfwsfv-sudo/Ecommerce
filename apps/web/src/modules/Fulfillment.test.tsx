import { HttpResponse, http } from "msw";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, it, vi } from "vitest";

import { Fulfillment } from "./Fulfillment";
import { server } from "../test/handlers";

vi.mock("../components/Chart", () => ({ Chart: ({ label }: { label: string }) => <div role="img" aria-label={label} /> }));

const meta = {
  dataset_id: "olist-brazilian-ecommerce-v2", metric_version: "orders-v1",
  metric_run_id: "orders-v1-b" + "a".repeat(64), source_bundle_sha256: "b".repeat(64),
  source_snapshots: { orders_src_v1: "1" }, curated_snapshots: { order_fact_v1: "2" },
  window_start: "2016-09-04", window_end: "2018-10-17", source_timezone: "unspecified",
  source_currency: null, source_order_count: 99441,
  calculated_at: "2026-09-20T00:00:00Z", implementation_revision: "c".repeat(40), warnings: ["历史快照"],
};
const delivery = [
  { window_type: "MONTH", window_start: "2018-07-01", window_end: "2018-07-31",
    delivery_eligible_order_count: 40, delivery_excluded_order_count: 10,
    delivery_days_avg: "3.000000", delivery_days_p50: "2.000000", delivery_days_p90: "5.000000",
    late_delivery_order_count: 4, late_delivery_eligible_order_count: 40,
    late_delivery_excluded_order_count: 10, late_delivery_rate: "0.100000" },
  { window_type: "MONTH", window_start: "2018-08-01", window_end: "2018-08-31",
    delivery_eligible_order_count: 0, delivery_excluded_order_count: 50,
    delivery_days_avg: null, delivery_days_p50: null, delivery_days_p90: null,
    late_delivery_order_count: 0, late_delivery_eligible_order_count: 0,
    late_delivery_excluded_order_count: 50, late_delivery_rate: null },
];
const reviews = [
  { window_type: "MONTH", window_start: "2018-07-01", window_end: "2018-07-31",
    review_row_count: 42, reviewed_order_count: 40, all_order_count: 50,
    review_coverage_rate: "0.800000", review_score_avg: "4.200000",
    low_score_order_count: 5, low_score_rate: "0.125000", multi_review_order_count: 2 },
  { window_type: "MONTH", window_start: "2018-08-01", window_end: "2018-08-31",
    review_row_count: 0, reviewed_order_count: 0, all_order_count: 50,
    review_coverage_rate: "0.000000", review_score_avg: null,
    low_score_order_count: 0, low_score_rate: null, multi_review_order_count: 0 },
];

beforeEach(() => {
  server.use(
    http.get("/api/v1/orders/delivery", () => HttpResponse.json({ meta, data: delivery })),
    http.get("/api/v1/orders/reviews", () => HttpResponse.json({ meta, data: reviews })),
    http.get("/api/v1/orders/publication", () => HttpResponse.json({ meta, data: { status: "PUBLISHED", published_at: "2026-09-21T00:00:00Z" } })),
  );
});

it("shows ineligible delivery periods as not applicable with excluded counts", async () => {
  render(<Fulfillment />);
  expect(await screen.findByRole("heading", { name: "履约时效" })).toBeVisible();
  expect(screen.getByText(/^合格订单 0 · 排除订单 50$/)).toBeVisible();
  expect(screen.getAllByText("不适用").length).toBeGreaterThan(0);
  expect(screen.queryByText("0.00 天")).not.toBeInTheDocument();
  await userEvent.selectOptions(screen.getByLabelText("业务窗口"), "2018-07-01");
  expect(screen.getByText("2.00 天")).toBeVisible();
  expect(screen.getByText("5.00 天")).toBeVisible();
  expect(screen.getByText(/^合格订单 40 · 排除订单 10$/)).toBeVisible();
});

it("switches to review coverage and low-score views without inventing a rating histogram", async () => {
  render(<Fulfillment />);
  await userEvent.click(await screen.findByRole("tab", { name: "评价" }));
  expect(screen.getByRole("heading", { name: "评价覆盖率" })).toBeVisible();
  expect(screen.getByRole("heading", { name: "低分率" })).toBeVisible();
  expect(screen.getAllByText("不适用").length).toBeGreaterThan(0);
  expect(screen.queryByText(/五星分布/)).not.toBeInTheDocument();
  await userEvent.selectOptions(screen.getByLabelText("业务窗口"), "2018-07-01");
  expect(screen.getByText("4.20")).toBeVisible();
  expect(screen.getByText(/80.00%/)).toBeVisible();
});

it("uses the same date range for delivery and reviews", async () => {
  const seen: string[] = [];
  server.use(
    http.get("/api/v1/orders/delivery", ({ request }) => { seen.push(new URL(request.url).search);
      return HttpResponse.json({ meta, data: delivery }); }),
    http.get("/api/v1/orders/reviews", ({ request }) => { seen.push(new URL(request.url).search);
      return HttpResponse.json({ meta, data: reviews }); }),
  );
  render(<Fulfillment />);
  expect(await screen.findByRole("heading", { name: "履约时效" })).toBeVisible();
  await userEvent.type(screen.getByLabelText("开始日期"), "2018-07-01");
  await userEvent.type(screen.getByLabelText("结束日期"), "2018-08-31");
  await userEvent.click(screen.getByRole("button", { name: "应用日期范围" }));
  expect(await screen.findByText(/2018-07-01 至 2018-08-31/)).toBeVisible();
  expect(seen.filter((query) => query.includes("start_date=2018-07-01") && query.includes("end_date=2018-08-31")).length).toBe(2);
});

it("fails closed when delivery and reviews come from different runs", async () => {
  server.use(http.get("/api/v1/orders/reviews", () => HttpResponse.json({
    meta: { ...meta, metric_run_id: "orders-v1-b" + "d".repeat(64) }, data: reviews,
  })));
  render(<Fulfillment />);
  expect(await screen.findByText(/数据未能通过校验/)).toBeVisible();
  expect(screen.queryByRole("heading", { name: "履约时效" })).not.toBeInTheDocument();
});
