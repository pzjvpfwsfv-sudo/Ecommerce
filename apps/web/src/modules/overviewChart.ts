import type { EChartsOption } from "echarts";

import { formatCount } from "../lib/format";
import type { OrderOverviewPoint } from "../lib/types";

export function overviewChart(points: OrderOverviewPoint[]): EChartsOption {
  return {
    animationDuration: 320,
    color: ["#2d7566"],
    grid: { left: 56, right: 25, top: 26, bottom: 55 },
    tooltip: { trigger: "axis", valueFormatter: (value) => `${formatCount(Number(value))} 单` },
    xAxis: { type: "category", data: points.map((point) => point.window_start),
      axisLabel: { color: "#64766c", hideOverlap: true }, axisLine: { lineStyle: { color: "#d8e0d9" } } },
    yAxis: { type: "value", name: "订单数", nameTextStyle: { color: "#64766c" },
      axisLabel: { color: "#64766c", formatter: (value: number) => formatCount(value) },
      splitLine: { lineStyle: { color: "#e7eee8" } } },
    series: [{ name: "订单数", type: "line", smooth: false, symbolSize: 6, lineStyle: { width: 3 },
      areaStyle: { color: "rgba(45,117,102,.12)" }, data: points.map((point) => point.order_count) }],
  };
}
