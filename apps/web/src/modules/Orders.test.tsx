import { HttpResponse, http } from "msw";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, it, vi } from "vitest";

import { Orders } from "./Orders";
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

beforeEach(() => {
  server.use(
    http.get("/api/v1/orders/overview", () => HttpResponse.json({ meta, data: [{
      window_type: "MONTH", window_start: "2018-08-01", window_end: "2018-08-31",
      order_count: 6500, delivered_order_count: 6000, canceled_order_count: 100,
      status_eligible_order_count: 6400, status_excluded_order_count: 100,
      delivered_rate: "0.937500", canceled_rate: "0.015625",
    }] })),
    http.get("/api/v1/orders/payments", () => HttpResponse.json({ meta, data: [
      { window_type: "MONTH", window_start: "2018-08-01", window_end: "2018-08-31",
        payment_type: "all", is_all: true, global_order_count: 6500, payment_order_count: 6500,
        payment_row_count: 6800, installment_order_count: 300, payment_value_sum: "16008872.12" },
      { window_type: "MONTH", window_start: "2018-08-01", window_end: "2018-08-31",
        payment_type: "credit_card", is_all: false, global_order_count: 6500, payment_order_count: 4700,
        payment_row_count: 4800, installment_order_count: 300, payment_value_sum: "13591643.70" },
      { window_type: "MONTH", window_start: "2018-08-01", window_end: "2018-08-31",
        payment_type: "boleto", is_all: false, global_order_count: 6500, payment_order_count: 1800,
        payment_row_count: 2000, installment_order_count: 0, payment_value_sum: "2417228.42" },
    ] })),
    http.get("/api/v1/orders/publication", () => HttpResponse.json({ meta, data: {
      published_at: "2026-09-21T00:00:00Z", status: "PUBLISHED",
    } })),
  );
});

it("switches payment comparison without inventing currency or ranking the all row", async () => {
  render(<Orders />);
  expect((await screen.findAllByText("6,500")).length).toBe(2);
  await userEvent.click(screen.getByRole("button", { name: "支付值" }));
  expect(screen.getByText("16,008,872.12")).toBeVisible();
  expect(screen.getByText("13,591,643.70")).toBeVisible();
  expect(screen.queryByText(/R\$/)).not.toBeInTheDocument();
  expect(screen.queryByRole("heading", { name: /GMV|营收/ })).not.toBeInTheDocument();
  expect(screen.getByText("credit_card")).toBeVisible();
  expect(screen.queryByText("all")).not.toBeInTheDocument();
});

it("refuses to mix overview and payments from different runs", async () => {
  server.use(http.get("/api/v1/orders/payments", () => HttpResponse.json({
    meta: { ...meta, metric_run_id: "orders-v1-b" + "d".repeat(64) }, data: [],
  })));
  render(<Orders />);
  expect(await screen.findByText(/数据未能通过校验/)).toBeVisible();
  expect(screen.queryByText("6,500")).not.toBeInTheDocument();
});
