import type { EChartsOption } from "echarts";

import { formatCount, formatRate } from "../lib/format";
import type { OrderDeliveryPoint, OrderReviewPoint } from "../lib/types";

export function formatDays(value: string | null): string {
  return value === null ? "不适用" : `${Number(value).toFixed(2)} 天`;
}

function tooltipRow(params: unknown): number {
  const item = Array.isArray(params) ? params[0] : params;
  return (item as { dataIndex?: number })?.dataIndex ?? 0;
}

export function deliveryBandChart(points: OrderDeliveryPoint[]): EChartsOption {
  return {
    color: ["#b48361", "#2b5b4d", "#2d7566"],
    grid: { left: 48, right: 20, top: 34, bottom: 55 },
    legend: { bottom: 0, data: ["平均", "P50", "P90"] },
    tooltip: { trigger: "axis", renderMode: "richText", formatter: (params) => {
      const row = points[tooltipRow(params)];
      return row ? `${row.window_start} 至 ${row.window_end}\n平均 ${formatDays(row.delivery_days_avg)} · P50 ${formatDays(row.delivery_days_p50)} · P90 ${formatDays(row.delivery_days_p90)}\n合格 ${formatCount(row.delivery_eligible_order_count)} 单 · 排除 ${formatCount(row.delivery_excluded_order_count)} 单` : "";
    } },
    xAxis: { type: "category", data: points.map((row) => row.window_start), axisLabel: { hideOverlap: true } },
    yAxis: { type: "value", name: "天", min: 0, splitLine: { lineStyle: { color: "#e7eee8" } } },
    series: [
      { name: "平均", type: "line", smooth: false, data: points.map((row) => row.delivery_days_avg === null ? null : Number(row.delivery_days_avg)) },
      { name: "P50", type: "line", smooth: false, data: points.map((row) => row.delivery_days_p50 === null ? null : Number(row.delivery_days_p50)) },
      { name: "P90", type: "line", smooth: false, data: points.map((row) => row.delivery_days_p90 === null ? null : Number(row.delivery_days_p90)) },
    ],
  };
}

export function lateRateChart(points: OrderDeliveryPoint[]): EChartsOption {
  return {
    color: ["#b48361"],
    grid: { left: 46, right: 12, top: 18, bottom: 38 },
    tooltip: { trigger: "item", renderMode: "richText", formatter: (params) => {
      const row = points[tooltipRow(params)];
      return row ? `${row.window_start} 至 ${row.window_end}\n晚到率 ${formatRate(row.late_delivery_rate, row.late_delivery_order_count, row.late_delivery_eligible_order_count)}\n排除 ${formatCount(row.late_delivery_excluded_order_count)} 单` : "";
    } },
    xAxis: { type: "category", data: points.map((row) => row.window_start), axisLabel: { hideOverlap: true } },
    yAxis: { type: "value", min: 0, max: 100, axisLabel: { formatter: "{value}%" }, splitLine: { lineStyle: { color: "#e7eee8" } } },
    series: [{ type: "bar", barMaxWidth: 18, data: points.map((row) => row.late_delivery_rate === null ? null : Number(row.late_delivery_rate) * 100) }],
  };
}

export function reviewChart(points: OrderReviewPoint[], metric: "score" | "coverage" | "low"): EChartsOption {
  const value = (row: OrderReviewPoint) => {
    const raw = metric === "score" ? row.review_score_avg
      : metric === "coverage" ? row.review_coverage_rate : row.low_score_rate;
    return raw === null ? null : Number(raw) * (metric === "score" ? 1 : 100);
  };
  const label = metric === "score" ? "评分均值" : metric === "coverage" ? "评价覆盖率" : "低分率";
  return {
    color: [metric === "low" ? "#b48361" : "#2d7566"],
    grid: { left: 43, right: 12, top: 17, bottom: 36 },
    tooltip: { trigger: "item", renderMode: "richText", formatter: (params) => {
      const row = points[tooltipRow(params)];
      if (!row) return "";
      const display = metric === "score" ? (row.review_score_avg === null ? "不适用" : Number(row.review_score_avg).toFixed(2))
        : metric === "coverage" ? formatRate(row.review_coverage_rate, row.reviewed_order_count, row.all_order_count)
          : formatRate(row.low_score_rate, row.low_score_order_count, row.reviewed_order_count);
      return `${row.window_start} 至 ${row.window_end}\n${label} ${display}\n已评价 ${formatCount(row.reviewed_order_count)} 单 / 全部 ${formatCount(row.all_order_count)} 单`;
    } },
    xAxis: { type: "category", data: points.map((row) => row.window_start), axisLabel: { hideOverlap: true } },
    yAxis: { type: "value", min: 0, max: metric === "score" ? 5 : 100,
      axisLabel: { formatter: metric === "score" ? "{value}" : "{value}%" }, splitLine: { lineStyle: { color: "#e7eee8" } } },
    series: [{ type: "line", smooth: false, symbolSize: 6, data: points.map(value) }],
  };
}
