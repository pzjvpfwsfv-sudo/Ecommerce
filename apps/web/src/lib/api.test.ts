import { HttpResponse, http } from "msw";
import { describe, expect, it } from "vitest";

import { fetchSameRun, getBehaviorOverview, getOrderOverview } from "./api";
import { formatRate, formatValue } from "./format";
import { ApiError } from "./http";
import type { MetricMeta } from "./types";
import { server } from "../test/handlers";

const meta = (run: string): MetricMeta => ({
  dataset_id: "olist-brazilian-ecommerce-v2",
  metric_version: "orders-v1",
  metric_run_id: run,
  window_start: "2016-09-04",
  window_end: "2018-10-17",
  calculated_at: "2026-09-20T00:00:00Z",
  warnings: [],
});

describe("truthful metric client", () => {
  for (const status of [401, 403, 422, 503]) {
    it(`keeps HTTP ${status} distinct instead of returning fixture numbers`, async () => {
      server.use(http.get("/api/v1/orders/overview", () => new HttpResponse(null, { status })));
      await expect(getOrderOverview({ window: "full" })).rejects.toMatchObject({ status });
    });
  }

  it("rejects invalid JSON and metadata-free success", async () => {
    server.use(http.get("/api/v1/orders/overview", () => HttpResponse.text("not json")));
    await expect(getOrderOverview({ window: "full" })).rejects.toBeInstanceOf(ApiError);
    server.use(http.get("/api/v1/orders/overview", () => HttpResponse.json({ data: [] })));
    await expect(getOrderOverview({ window: "full" })).rejects.toThrow(/meta/);
  });

  it("rejects responses from different runs", async () => {
    const first = Promise.resolve({ meta: meta("run-a"), data: [] });
    const second = Promise.resolve({ meta: meta("run-b"), data: [] });
    await expect(fetchSameRun(first, second)).rejects.toThrow("metric run mismatch");
  });

  it("rejects missing behavior source and cross-table metadata for one run", async () => {
    const behaviorMeta = {
      dataset_id: "rees46-multicategory", metric_version: "behavior-v1",
      metric_run_id: "behavior-v1-s9", source_snapshot_id: "9",
      source_table: "real_behavior_detail_v1", data_scope: "g2c-correctness-subset",
      source_event_count: 1002, window_start: "2019-10-01", window_end: "2019-11-30",
      replay_first_at: null, replay_last_at: null,
      calculated_at: "2026-09-20T00:00:00Z", warnings: [],
    };
    server.use(http.get("/api/v1/behavior/overview", () => HttpResponse.json({
      meta: { ...behaviorMeta, source_table: undefined }, data: [],
    })));
    await expect(getBehaviorOverview({ window: "full" })).rejects.toThrow(/behavior meta/);
    await expect(fetchSameRun(
      Promise.resolve({ meta: behaviorMeta, data: [] }),
      Promise.resolve({ meta: { ...behaviorMeta, source_table: "real_behavior_detail_v1_other" }, data: [] }),
    )).rejects.toThrow("metric run mismatch");
  });

  it("rejects illegal full-range dates and behavior-like month queries", async () => {
    await expect(getOrderOverview({ window: "full", startDate: "2016-09-04" })).rejects.toThrow();
    await expect(getBehaviorOverview({ window: "month" as "day" })).rejects.toThrow("invalid behavior window");
  });

  it("does not invent a denominator or currency", () => {
    expect(formatRate(null)).toBe("不适用");
    expect(formatValue("13591643.70", null)).toBe("13,591,643.70");
    expect(formatValue("13591643.70", "BRL")).toContain("BRL");
  });
});
