import { HttpResponse, http } from "msw";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, it, vi } from "vitest";

import { Rankings } from "./Rankings";
import { server } from "../test/handlers";

vi.mock("../components/Chart", () => ({ Chart: ({ label }: { label: string }) => <div role="img" aria-label={label} /> }));

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
  source_table: "real_behavior_detail_v1", replay_first_at: null, replay_last_at: null,
  data_scope: "g2c-correctness-subset", source_event_count: 1002,
  window_start: "2020-01-01", window_end: "2020-01-02",
  calculated_at: "2026-09-20T00:00:00Z", warnings: ["当前为正确性子集"],
};
const orderRows = [
  { window_type: "FULL", window_start: "2016-09-04", window_end: "2018-10-17",
    dimension_type: "product", dimension_id: "p-1", dimension_name: "产品甲", is_unknown: false,
    ranking_order_count: 38, ranking_item_row_count: 42, ranking_customer_count: 35,
    ranking_item_value_sum: "3200.00", ranking_freight_value_sum: "450.00", ranking_payment_value_sum: null,
    ranking_late_delivery_order_count: null, ranking_late_delivery_eligible_order_count: null,
    ranking_late_delivery_rate: null, payment_value_is_additive: null },
  { window_type: "FULL", window_start: "2016-09-04", window_end: "2018-10-17",
    dimension_type: "product", dimension_id: "unknown", dimension_name: "unknown", is_unknown: true,
    ranking_order_count: 12, ranking_item_row_count: 12, ranking_customer_count: 12,
    ranking_item_value_sum: "900.00", ranking_freight_value_sum: "100.00", ranking_payment_value_sum: null,
    ranking_late_delivery_order_count: null, ranking_late_delivery_eligible_order_count: null,
    ranking_late_delivery_rate: null, payment_value_is_additive: null },
];

beforeEach(() => {
  server.use(
    http.get("/api/v1/orders/rankings", () => HttpResponse.json({ meta: orderMeta, data: orderRows })),
    http.get("/api/v1/orders/publication", () => HttpResponse.json({ meta: orderMeta, data: { status: "PUBLISHED", published_at: "2026-09-21T00:00:00Z" } })),
    http.get("/api/v1/behavior/rankings", () => HttpResponse.json({ meta: behaviorMeta, data: [{
      window_type: "FULL", window_start: "2020-01-01", window_end: "2020-01-02",
      dimension_type: "product", dimension_id: "bp-1", dimension_name: "浏览商品乙", is_unknown: false,
      view_count: 22, cart_count: 5, purchase_count: 2, unique_user_count: 19,
      purchase_amount_proxy: "89.50",
    }] })),
    http.get("/api/v1/behavior/publication", () => HttpResponse.json({ meta: behaviorMeta, data: { status: "PUBLISHED", published_at: "2026-09-21T00:00:00Z" } })),
  );
});

it("allows only legal product sorts and labels the result as Top N", async () => {
  render(<Rankings />);
  expect(await screen.findByText("产品甲")).toBeVisible();
  expect(screen.getByText(/仅展示 Top 20/)).toBeVisible();
  expect(screen.getByText(/未识别维度/)).toBeVisible();
  expect(screen.queryByRole("option", { name: "支付值" })).not.toBeInTheDocument();
  await userEvent.selectOptions(screen.getByLabelText("维度"), "customer_state");
  expect(await screen.findByRole("option", { name: "支付值" })).toBeInTheDocument();
  expect(screen.queryByRole("option", { name: "商品值" })).not.toBeInTheDocument();
});

it("shows only returned aggregate evidence when a row is selected", async () => {
  render(<Rankings />);
  await userEvent.click(await screen.findByRole("button", { name: /查看 产品甲 聚合详情/ }));
  expect(screen.getByText("p-1")).toBeVisible();
  expect(screen.getByText("3,200.00")).toBeVisible();
  expect(screen.queryByText(/逐笔订单/)).not.toBeInTheDocument();
  expect(screen.queryByText(/R\$/)).not.toBeInTheDocument();
});

it("keeps behavior rankings separate and identifies the correctness subset", async () => {
  let orderRequests = 0;
  let behaviorRequests = 0;
  server.use(
    http.get("/api/v1/orders/rankings", () => { orderRequests += 1; return HttpResponse.json({ meta: orderMeta, data: orderRows }); }),
    http.get("/api/v1/behavior/rankings", () => { behaviorRequests += 1; return HttpResponse.json({ meta: behaviorMeta, data: [{
      window_type: "FULL", window_start: "2020-01-01", window_end: "2020-01-02",
      dimension_type: "product", dimension_id: "bp-1", dimension_name: "浏览商品乙", is_unknown: false,
      view_count: 22, cart_count: 5, purchase_count: 2, unique_user_count: 19, purchase_amount_proxy: "89.50",
    }] }); }),
  );
  render(<Rankings />);
  expect(await screen.findByText("产品甲")).toBeVisible();
  await userEvent.click(screen.getByRole("button", { name: "REES46 行为域" }));
  expect(await screen.findByText("浏览商品乙")).toBeVisible();
  expect(screen.queryByText("产品甲")).not.toBeInTheDocument();
  expect(screen.getByText(/数据范围：正确性子集/)).toBeVisible();
  expect(screen.getByText("当前为正确性子集")).toBeVisible();
  expect(orderRequests).toBe(1);
  expect(behaviorRequests).toBe(1);
});

it("warns that seller-state payment values are not additive", async () => {
  server.use(http.get("/api/v1/orders/rankings", () => HttpResponse.json({ meta: orderMeta, data: [{
    ...orderRows[0], dimension_type: "seller_state", dimension_id: "SP", dimension_name: "SP",
    ranking_payment_value_sum: "4200.00", ranking_item_value_sum: null,
    ranking_freight_value_sum: null, ranking_item_row_count: null,
    ranking_late_delivery_order_count: 3, ranking_late_delivery_eligible_order_count: 30,
    ranking_late_delivery_rate: "0.100000", payment_value_is_additive: false,
  }] })));
  render(<Rankings />);
  await userEvent.selectOptions(screen.getByLabelText("维度"), "seller_state");
  expect(await screen.findByText(/卖家地域支付值不可加总/)).toBeVisible();
});

it("keeps the old sort label on retained results until the new query arrives", async () => {
  let release: (() => void) | undefined;
  server.use(http.get("/api/v1/orders/rankings", ({ request }) => {
    if (new URL(request.url).searchParams.get("sort_by") !== "item_value") {
      return HttpResponse.json({ meta: orderMeta, data: orderRows });
    }
    return new Promise((resolve) => {
      release = () => resolve(HttpResponse.json({ meta: orderMeta, data: orderRows }));
    });
  }));
  render(<Rankings />);
  expect(await screen.findByText("产品甲")).toBeVisible();
  await userEvent.selectOptions(screen.getByLabelText("排序指标"), "item_value");
  expect(screen.getByText(/指标：订单数/)).toBeVisible();
  expect(screen.getByText(/上一次选择/)).toBeVisible();
  release?.();
  expect(await screen.findByText(/指标：商品值/)).toBeVisible();
});
