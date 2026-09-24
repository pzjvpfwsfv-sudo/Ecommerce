import type { EChartsOption } from "echarts";

import { formatCount } from "../lib/format";
import type { BehaviorOverviewPoint } from "../lib/types";

export function behaviorChart(points: BehaviorOverviewPoint[]): EChartsOption {
  return {
    animationDuration: 300,
    color: ["#2d7566", "#b48361"],
    grid: { left: 50, right: 25, top: 28, bottom: 55 },
    tooltip: { trigger: "axis", valueFormatter: (value) => `${formatCount(Number(value))} 事件` },
    legend: { data: ["浏览事件", "购买事件"], bottom: 0, textStyle: { color: "#64766c" } },
    xAxis: { type: "category", data: points.map((point) => point.window_start), axisLabel: { hideOverlap: true } },
    yAxis: { type: "value", name: "事件数", splitLine: { lineStyle: { color: "#e7eee8" } } },
    series: [
      { name: "浏览事件", type: "line", smooth: false, data: points.map((point) => point.view_count) },
      { name: "购买事件", type: "line", smooth: false, data: points.map((point) => point.purchase_count) },
    ],
  };
}
