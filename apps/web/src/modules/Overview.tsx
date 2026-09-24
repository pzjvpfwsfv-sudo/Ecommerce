import { useState } from "react";

import { Chart } from "../components/Chart";
import { EvidenceDrawer } from "../components/EvidenceDrawer";
import { MetricFrame } from "../components/MetricFrame";
import { fetchSameRun, getOrderOverview, getPublication, type OrderQuery } from "../lib/api";
import { formatCount, formatDateTime, formatRate } from "../lib/format";
import { useMetricQuery } from "../lib/useMetricQuery";
import { overviewChart } from "./overviewChart";

export function Overview() {
  const [window, setWindow] = useState<OrderQuery["window"]>("month");
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [activeRange, setActiveRange] = useState<{ startDate: string; endDate: string } | null>(null);
  const [filterError, setFilterError] = useState("");
  const [evidenceOpen, setEvidenceOpen] = useState(false);
  const query: OrderQuery = { window, ...(window === "full" ? {} : activeRange ?? {}) };
  const key = JSON.stringify(query);
  const core = useMetricQuery((signal) => fetchSameRun(
    getOrderOverview({ ...query, signal }), getPublication("orders", signal),
  ), key);
  const behavior = useMetricQuery((signal) => getPublication("behavior", signal), "behavior-publication");
  const overview = core.data?.[0];
  const publication = core.data?.[1];
  const points = overview?.data ?? [];

  function applyRange() {
    if (Boolean(startDate) !== Boolean(endDate) || (startDate && startDate > endDate)) {
      setFilterError("请同时填写有效的起止日期，且开始日期不能晚于结束日期。");
      return;
    }
    setFilterError("");
    setActiveRange(startDate ? { startDate, endDate } : null);
  }

  return <div className="overview-page">
    <div className="module-intro"><div><span className="eyebrow">ORDER OPERATIONS / HISTORICAL SNAPSHOT</span>
      <p>从历史订单里看运营结构与时间走势，不把回放数据写成“今日实时”。</p></div>
      <div className="filter-bar"><label htmlFor="order-window">订单粒度</label>
        <select id="order-window" value={window} onChange={(event) => {
          setWindow(event.target.value as OrderQuery["window"]);
          if (event.target.value === "full") setActiveRange(null);
        }}>
          <option value="day">按日</option><option value="month">按月</option><option value="full">全量</option>
        </select></div></div>
    {window !== "full" && <div className="date-filter">
      <label>从 <input type="date" value={startDate} onChange={(event) => setStartDate(event.target.value)} /></label>
      <label>至 <input type="date" value={endDate} onChange={(event) => setEndDate(event.target.value)} /></label>
      <button type="button" className="secondary-button" onClick={applyRange}>应用日期范围</button>
      {filterError && <span role="alert">{filterError}</span>}
    </div>}
    <MetricFrame title="Olist 历史订单" meta={overview?.meta} loading={core.loading} error={core.error}
      empty={!!overview && points.length === 0} retry={core.retry} onEvidence={overview ? () => setEvidenceOpen(true) : undefined}>
      {overview && publication && <>
        <div className="overview-hero">
          <div className="overview-lead"><span className="eyebrow">SOURCE ORDER COUNT / 来源订单数</span>
            <strong>{formatCount(overview.meta.source_order_count)}</strong>
            <span>Olist 匿名化历史快照 · {overview.meta.window_start} 至 {overview.meta.window_end}</span></div>
          <div className="overview-pub"><span>指标发布</span><strong>{publication.data.status}</strong>
            <small>{formatDateTime(publication.data.published_at)}</small></div>
        </div>
        <div className="overview-chart-layout"><section className="surface-panel"><div className="panel-heading"><h3>订单量 · {window === "day" ? "按日" : window === "month" ? "按月" : "全量"}</h3>
          <span>按 API 已发布窗口展示</span></div>
          <Chart label="历史订单量时间序列" option={overviewChart(points)} fallback={<table><thead><tr><th>窗口</th><th>订单数</th></tr></thead>
            <tbody>{points.map((point) => <tr key={point.window_start}><td>{point.window_start} 至 {point.window_end}</td><td>{formatCount(point.order_count)}</td></tr>)}</tbody></table>} />
        </section><section className="overview-side"><span className="eyebrow">READING GUIDE / 阅读提示</span>
          <h3>一个来源，一条口径。</h3><p>这里的订单数不与行为域事件数相加。切换粒度只改变已发布的订单时间窗口。</p>
          {points.length === 1 && <p>当前窗口订单数 <strong>{formatCount(points[0].order_count)}</strong>；送达率 {formatRate(points[0].delivered_rate, points[0].delivered_order_count, points[0].status_eligible_order_count)}。</p>}
          <small>run {overview.meta.metric_run_id}</small>
        </section></div>
      </>}
    </MetricFrame>
    <section className="behavior-status" aria-label="REES46 行为域发布状态">
      <div><span className="eyebrow">SEPARATE SOURCE / 独立来源</span><h2>REES46 行为域</h2>
        <p>与 Olist 订单域分开呈现，不合并成跨源转化率。</p></div>
      {behavior.loading && <p role="status">正在读取发布状态…</p>}
      {behavior.error && <p role="alert">行为域发布状态暂不可用。<button className="text-button" onClick={behavior.retry}>重试</button></p>}
      {behavior.data && <div className="behavior-status-detail"><strong>{behavior.data.data.status}</strong>
        <span>{behavior.data.meta.data_scope === "g2c-correctness-subset" ? "正确性子集" : "稳定用户样本"} · {formatCount(behavior.data.meta.source_event_count)} 条来源事件</span>
        <small>{behavior.data.meta.warnings.join("；")}</small></div>}
    </section>
    {overview && <EvidenceDrawer open={evidenceOpen} onClose={() => setEvidenceOpen(false)} meta={overview.meta}
      domain="orders" publishedAt={publication?.data.published_at} />}
  </div>;
}
