import type { EChartsOption } from "echarts";

import { formatCount, formatValue } from "../lib/format";
import type { OrderOverviewPoint, OrderPaymentPoint } from "../lib/types";

export function orderStatusChart(points: OrderOverviewPoint[]): EChartsOption {
  return {
    color: ["#2d7566", "#b48361"],
    grid: { left: 55, right: 58, top: 35, bottom: 50 },
    legend: { bottom: 0, data: ["订单数", "送达率"] },
    tooltip: { trigger: "axis" },
    xAxis: { type: "category", data: points.map((row) => row.window_start), axisLabel: { hideOverlap: true } },
    yAxis: [
      { type: "value", name: "订单数", splitLine: { lineStyle: { color: "#e7eee8" } } },
      { type: "value", name: "送达率 %", min: 0, max: 100, splitLine: { show: false } },
    ],
    series: [
      { name: "订单数", type: "bar", barMaxWidth: 22, data: points.map((row) => row.order_count) },
      { name: "送达率", type: "line", yAxisIndex: 1, smooth: false,
        data: points.map((row) => row.delivered_rate === null ? null : Number(row.delivered_rate) * 100) },
    ],
  };
}

export function paymentChart(rows: OrderPaymentPoint[], mode: "count" | "value", currency: string | null): EChartsOption {
  const metric = (row: OrderPaymentPoint) => mode === "count" ? row.payment_order_count : Number(row.payment_value_sum);
  return {
    color: ["#2b5b4d"],
    grid: { left: 112, right: 26, top: 20, bottom: 35 },
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" },
      valueFormatter: (value) => mode === "count" ? `${formatCount(Number(value))} 单` : formatValue(Number(value).toFixed(2), currency) },
    xAxis: { type: "value", axisLabel: { color: "#64766c" }, splitLine: { lineStyle: { color: "#e7eee8" } } },
    yAxis: { type: "category", data: rows.map((row) => row.payment_type).reverse(), axisLabel: { color: "#1a2e29" } },
    series: [{ type: "bar", barMaxWidth: 19, data: rows.map(metric).reverse() }],
  };
}
