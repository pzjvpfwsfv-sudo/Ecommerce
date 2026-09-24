import { useState } from "react";

import { Chart } from "../components/Chart";
import { EvidenceDrawer } from "../components/EvidenceDrawer";
import { MetricFrame } from "../components/MetricFrame";
import { fetchSameRun, getBehaviorFunnel, getBehaviorOverview, getBehaviorQuality, type BehaviorQuery } from "../lib/api";
import { formatCount, formatRate } from "../lib/format";
import { useMetricQuery } from "../lib/useMetricQuery";
import { behaviorChart } from "./behaviorChart";

export function Behavior() {
  const [window, setWindow] = useState<BehaviorQuery["window"]>("full");
  const [selectedDate, setSelectedDate] = useState("");
  const [activeStage, setActiveStage] = useState("view_sessions");
  const [evidenceOpen, setEvidenceOpen] = useState(false);
  const state = useMetricQuery(async (signal) => {
    const [overview, funnel] = await fetchSameRun(
      getBehaviorOverview({ window, signal }), getBehaviorFunnel({ window, signal }),
    );
    const [, quality] = await fetchSameRun(Promise.resolve(overview), getBehaviorQuality(signal));
    return { overview, funnel, quality };
  }, window);
  const overview = state.data?.overview;
  const funnel = state.data?.funnel;
  const quality = state.data?.quality;
  const point = funnel?.data.find((row) => row.window_start === selectedDate) ?? funnel?.data[0];
  const stages = point ? [
    { metric: "view_sessions", label: "浏览会话", count: point.view_sessions, rate: null,
      numerator: undefined, denominator: undefined },
    { metric: "view_to_cart_sessions", label: "加购会话", count: point.view_to_cart_sessions,
      rate: point.view_to_cart_rate, numerator: point.view_to_cart_sessions, denominator: point.view_sessions },
    { metric: "completed_sessions", label: "完成购买会话", count: point.completed_sessions,
      rate: point.cart_to_purchase_rate, numerator: point.completed_sessions, denominator: point.view_to_cart_sessions },
  ] : [];

  return <div className="behavior-page">
    <div className="module-intro"><div><span className="eyebrow">BEHAVIOR / SESSION FUNNEL</span>
      <p>浏览、加购与购买由同一会话链路定义；事件量只用来描述来源规模，不直接充当转化分母。</p></div>
      <div className="filter-bar"><label htmlFor="behavior-window">行为粒度</label>
        <select id="behavior-window" value={window} onChange={(event) => { setWindow(event.target.value as BehaviorQuery["window"]); setSelectedDate(""); }}>
          <option value="full">全量</option><option value="day">按日</option>
        </select></div></div>
    <MetricFrame title="REES46 行为与转化" meta={overview?.meta} loading={state.loading} error={state.error}
      empty={!!overview && (overview.data.length === 0 || funnel?.data.length === 0)} retry={state.retry} onEvidence={overview ? () => setEvidenceOpen(true) : undefined}>
      {overview && funnel && quality && <>
        <div className="subset-banner"><div><span className="eyebrow">DATA SCOPE / 数据范围</span><strong>{overview.meta.data_scope === "g2c-correctness-subset" ? "正确性子集" : "稳定用户 2% 全量样本"}</strong>
          <p>{formatCount(overview.meta.source_event_count)} 条来源事件；按本次发布的数据范围解读，不代表企业实时生产规模。</p></div>
          <small>{overview.meta.warnings.join("；")}</small></div>
        <div className="behavior-grid"><section className="surface-panel behavior-funnel"><div className="panel-heading"><h3>会话转化阶梯</h3><span>仅使用 API 提供的会话计数与比率</span></div>
          {funnel.data.length > 1 && <label className="date-select">查看日期 <select value={point?.window_start} onChange={(event) => setSelectedDate(event.target.value)}>
            {funnel.data.map((row) => <option key={row.window_start} value={row.window_start}>{row.window_start}</option>)}</select></label>}
          <div className="funnel-stages">{stages.map((stage, index) => <button key={stage.metric} type="button"
            className={`funnel-stage funnel-stage--${index}${activeStage === stage.metric ? " funnel-stage--active" : ""}`}
            onClick={() => { setActiveStage(stage.metric); setEvidenceOpen(true); }}>
            <span className="stage-number">0{index + 1}</span><span className="stage-label">{stage.label}</span>
            <strong>{formatCount(stage.count)}</strong><small>{index === 0 ? "会话数" : `阶段转化 ${formatRate(stage.rate, stage.numerator, stage.denominator)}`}</small>
          </button>)}</div>
          <p className="missing-note">当前窗口缺失会话标识事件 {formatCount(point?.missing_session_event_count ?? 0)} 条；全量质量表缺失会话 {formatCount(quality.data.missing_session_count)} 条。</p>
          {point && <p className="full-rate">完整链路转化率 {formatRate(point.full_conversion_rate, point.completed_sessions, point.view_sessions)}</p>}
        </section><section className="surface-panel"><div className="panel-heading"><h3>事件走势</h3><span>不据此推导会话转化</span></div>
          <Chart label="浏览与购买事件时间趋势" option={behaviorChart(overview.data)} fallback={<table><thead><tr><th>窗口</th><th>浏览事件</th><th>购买事件</th></tr></thead>
            <tbody>{overview.data.map((row) => <tr key={row.window_start}><td>{row.window_start}</td><td>{formatCount(row.view_count)}</td><td>{formatCount(row.purchase_count)}</td></tr>)}</tbody></table>} />
          <div className="behavior-quality-strip"><span>全量质量表 · 来源事件 {formatCount(quality.data.source_event_count)}</span>
            <span>清洗后 {formatCount(quality.data.clean_event_count)}</span><span>晚到 {formatCount(quality.data.late_event_count)}</span></div>
        </section></div>
      </>}
    </MetricFrame>
    {overview && <EvidenceDrawer open={evidenceOpen} onClose={() => setEvidenceOpen(false)} meta={overview.meta}
      domain="behavior" highlightMetric={activeStage} />}
  </div>;
}
