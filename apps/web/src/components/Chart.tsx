import { useEffect, useRef, type ReactNode } from "react";
import { BarChart, LineChart } from "echarts/charts";
import { GridComponent, LegendComponent, TooltipComponent } from "echarts/components";
import * as echarts from "echarts/core";
import { CanvasRenderer } from "echarts/renderers";
import type { EChartsOption } from "echarts";

echarts.use([BarChart, LineChart, GridComponent, LegendComponent, TooltipComponent, CanvasRenderer]);

export function Chart({ option, label, height = 310, fallback }: {
  option: EChartsOption;
  label: string;
  height?: number;
  fallback?: ReactNode;
}) {
  const node = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!node.current) return;
    const chart = echarts.init(node.current);
    chart.setOption(option, true);
    const resize = () => chart.resize();
    const observer = typeof ResizeObserver === "undefined" ? null : new ResizeObserver(resize);
    if (observer) observer.observe(node.current);
    else window.addEventListener("resize", resize);
    return () => {
      observer?.disconnect();
      window.removeEventListener("resize", resize);
      chart.dispose();
    };
  }, [option]);

  return <div className="chart-block">
    <div ref={node} role="img" aria-label={label} style={{ height, width: "100%" }} />
    {fallback && <details className="chart-fallback"><summary>查看图表数据表</summary>{fallback}</details>}
  </div>;
}
