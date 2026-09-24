import { HttpResponse, http } from "msw";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { expect, it } from "vitest";

import { EvidenceDrawer } from "./EvidenceDrawer";
import type { OrderMeta } from "../lib/types";
import { server } from "../test/handlers";

const meta = {
  dataset_id: "olist-brazilian-ecommerce-v2",
  metric_version: "orders-v1",
  metric_run_id: "orders-v1-b" + "a".repeat(64),
  window_start: "2016-09-04",
  window_end: "2018-10-17",
  calculated_at: "2026-09-20T00:00:00Z",
  warnings: ["历史快照，不是实时订单"],
  source_bundle_sha256: "a".repeat(64),
  source_timezone: "unspecified",
  source_order_count: 99441,
  implementation_revision: "b".repeat(40),
  source_currency: null,
  source_snapshots: { orders_src_v1: "1" },
  curated_snapshots: { order_fact_v1: "2" },
} satisfies OrderMeta;

function Host() {
  const [open, setOpen] = useState(false);
  return <><button onClick={() => setOpen(true)}>查看证据</button>
    <EvidenceDrawer open={open} onClose={() => setOpen(false)} meta={meta} domain="orders" publishedAt="2026-09-21T00:00:00Z" />
  </>;
}

it("shows live identity and definitions, closes on Escape, and restores focus", async () => {
  server.use(http.get("/api/v1/metrics/definitions", () => HttpResponse.json({
    domain: "orders", dataset_id: meta.dataset_id, metric_version: "orders-v1",
    definitions: [{ metric_name: "order_count", display_name: "订单数", formula: "count(*)", numerator: "订单",
      denominator: null, source_fields: ["order_id"], allowed_windows: ["FULL"], additive: true,
      null_policy: "不适用", exclusions: [], limitations: ["历史订单"], forbidden_claims: [] }],
  })));
  render(<Host />);
  const opener = screen.getByRole("button", { name: "查看证据" });
  await userEvent.click(opener);
  const dialog = await screen.findByRole("dialog", { name: "数据证据" });
  expect(dialog).toHaveTextContent(meta.metric_run_id);
  expect(dialog).toHaveTextContent("历史快照，不是实时订单");
  expect(await screen.findByText("订单数")).toBeVisible();
  await userEvent.keyboard("{Escape}");
  expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  expect(opener).toHaveFocus();
});
