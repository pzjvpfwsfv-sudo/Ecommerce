import { HttpResponse, http } from "msw";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, it, vi } from "vitest";

import { Behavior } from "./Behavior";
import { server } from "../test/handlers";

vi.mock("../components/Chart", () => ({ Chart: ({ label }: { label: string }) => <div role="img" aria-label={label} /> }));

const meta = {
  dataset_id: "rees46-multicategory", metric_version: "behavior-v1",
  metric_run_id: "behavior-v1-s1", source_snapshot_id: "1", window_start: "2019-10-01",
  source_table: "real_behavior_detail_v1", replay_first_at: null, replay_last_at: null,
  window_end: "2019-11-30", calculated_at: "2026-09-20T00:00:00Z",
  data_scope: "g2c-correctness-subset", source_event_count: 1002,
  warnings: ["当前仅为 1,002 条真实事件的正确性子集，不代表完整交易分布"],
};

beforeEach(() => {
  server.use(
    http.get("/api/v1/behavior/overview", () => HttpResponse.json({ meta, data: [{
      window_type: "FULL", window_start: "2019-10-01", window_end: "2019-11-30",
      event_count: 1002, view_count: 800, cart_count: 100, purchase_count: 20,
      unique_user_count: 80, session_count: 90, product_count: 60, purchase_amount_proxy: null,
    }] })),
    http.get("/api/v1/behavior/funnel", () => HttpResponse.json({ meta, data: [{
      window_type: "FULL", window_start: "2019-10-01", window_end: "2019-11-30",
      missing_session_event_count: 12, view_sessions: 70, view_to_cart_sessions: 30,
      completed_sessions: 10, view_to_cart_rate: "0.428571", cart_to_purchase_rate: null,
      full_conversion_rate: null,
    }] })),
    http.get("/api/v1/behavior/quality", () => HttpResponse.json({ meta, data: {
      window_type: "FULL", window_start: "2019-10-01", window_end: "2019-11-30",
      source_event_count: 1002, clean_event_count: 990, late_event_count: 12,
      missing_session_count: 12, reconciliation_status: "PASS",
    } })),
  );
});

it("shows the correctness subset, missing sessions, and non-applicable rates", async () => {
  render(<Behavior />);
  expect(await screen.findByText(/1,002 条来源事件/)).toBeVisible();
  expect(screen.getAllByText(/正确性子集/).length).toBeGreaterThan(0);
  expect(screen.getByText(/历史回放/)).toBeVisible();
  expect(screen.getByText(/缺失会话/)).toBeVisible();
  expect(screen.getAllByText(/不适用/).length).toBeGreaterThan(0);
  expect(screen.queryByRole("option", { name: "按月" })).not.toBeInTheDocument();
});

it("selects a stage and opens its real metric definition", async () => {
  server.use(http.get("/api/v1/metrics/definitions", () => HttpResponse.json({
    domain: "behavior", dataset_id: meta.dataset_id, metric_version: meta.metric_version,
    definitions: [{ metric_name: "view_sessions", display_name: "浏览会话", formula: "count(session)",
      numerator: "浏览会话", denominator: null, source_fields: ["user_session"],
      allowed_windows: ["FULL"], additive: false, null_policy: "不适用",
      limitations: ["仅当前子集"], forbidden_claims: [] }],
  })));
  render(<Behavior />);
  await userEvent.click(await screen.findByRole("button", { name: /浏览会话/ }));
  expect(await screen.findByRole("dialog", { name: "数据证据" })).toBeVisible();
  expect(await screen.findByText("count(session)")).toBeVisible();
});

it("rejects mismatched quality runs instead of blending counts", async () => {
  server.use(http.get("/api/v1/behavior/quality", () => HttpResponse.json({
    meta: { ...meta, metric_run_id: "behavior-v1-s2" }, data: { missing_session_count: 12 },
  })));
  render(<Behavior />);
  expect(await screen.findByText(/数据未能通过校验/)).toBeVisible();
  expect(screen.queryByText("1,002 条来源事件")).not.toBeInTheDocument();
});
