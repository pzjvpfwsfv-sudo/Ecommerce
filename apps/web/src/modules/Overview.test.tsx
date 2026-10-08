import { HttpResponse, http } from "msw";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, it, vi } from "vitest";

import { Overview } from "./Overview";
import { server } from "../test/handlers";

vi.mock("../components/Chart", () => ({ Chart: ({ label }: { label: string }) => <div role="img" aria-label={label} /> }));

const orderMeta = {
  dataset_id: "olist-brazilian-ecommerce-v2", metric_version: "orders-v1",
  metric_run_id: "orders-v1-b" + "a".repeat(64), source_bundle_sha256: "b".repeat(64),
  source_snapshots: { orders_src_v1: "1" }, curated_snapshots: { order_fact_v1: "2" },
  window_start: "2016-09-04", window_end: "2018-10-17", source_timezone: "unspecified",
  source_currency: null, source_order_count: 99441,
  calculated_at: "2026-09-20T00:00:00Z", implementation_revision: "c".repeat(40),
  warnings: ["历史数据快照"],
};

const behaviorMeta = {
  dataset_id: "rees46-multicategory", metric_version: "behavior-v1",
  metric_run_id: "behavior-v1-s1", source_snapshot_id: "1", window_start: "2019-10-01",
  source_table: "real_behavior_detail_v1", replay_first_at: null, replay_last_at: null,
  window_end: "2019-11-30", calculated_at: "2026-09-20T00:00:00Z",
  data_scope: "g2c-correctness-subset", source_event_count: 1002,
  warnings: ["正确性子集"],
};

beforeEach(() => {
  server.use(
    http.get("/api/v1/orders/overview", ({ request }) => {
      const window = new URL(request.url).searchParams.get("window");
      return HttpResponse.json({ meta: orderMeta, data: [
        { window_type: window?.toUpperCase(), window_start: "2016-09-04", window_end: "2018-10-17",
          order_count: window === "full" ? 99441 : 4220, delivered_order_count: 3900,
          canceled_order_count: 80, status_eligible_order_count: 4200,
          status_excluded_order_count: 20, delivered_rate: "0.928571", canceled_rate: "0.019048",
          unique_customer_count: 4100, repeat_customer_count: 90, repeat_customer_rate: "0.021951",
          item_value_sum: "13591643.70", freight_value_sum: "2250000.00", payment_value_sum: "16008872.12" },
      ] });
    }),
    http.get("/api/v1/orders/publication", () => HttpResponse.json({ meta: orderMeta,
      data: { published_at: "2026-09-21T00:00:00Z", status: "PUBLISHED" } })),
    http.get("/api/v1/behavior/publication", () => HttpResponse.json({ meta: behaviorMeta,
      data: { published_at: "2026-09-21T00:00:00Z", status: "PUBLISHED" } })),
  );
});

it("shows historical Olist context and separate REES46 publication", async () => {
  render(<Overview />);
  expect(await screen.findByText("99,441")).toBeVisible();
  expect(screen.getAllByText(/2016-09-04/).length).toBeGreaterThan(0);
  expect(screen.getByText(/REES46 行为域/)).toBeVisible();
  expect(screen.queryByText(/今日实时订单/)).not.toBeInTheDocument();
});

it("sends legal full query without date parameters", async () => {
  const queries: string[] = [];
  server.use(http.get("/api/v1/orders/overview", ({ request }) => {
    queries.push(request.url);
    return HttpResponse.json({ meta: orderMeta, data: [] });
  }));
  render(<Overview />);
  await userEvent.selectOptions(screen.getByLabelText("订单粒度"), "full");
  expect(await screen.findByText(/当前筛选没有已发布结果/)).toBeVisible();
  expect(queries.some((url) => url.includes("window=full") && !url.includes("start_date") && !url.includes("end_date"))).toBe(true);
});

it("keeps Olist visible when the independent behavior publication fails", async () => {
  server.use(http.get("/api/v1/behavior/publication", () => new HttpResponse(null, { status: 503 })));
  render(<Overview />);
  expect(await screen.findByText("99,441")).toBeVisible();
  expect(await screen.findByText(/行为域发布状态暂不可用/)).toBeVisible();
});
