import { useState } from "react";

import { Chart } from "../components/Chart";
import { EvidenceDrawer } from "../components/EvidenceDrawer";
import { MetricFrame } from "../components/MetricFrame";
import { fetchSameRun, getOrderDelivery, getOrderReviews, getPublication, type OrderQuery } from "../lib/api";
import { formatCount, formatRate } from "../lib/format";
import { useMetricQuery } from "../lib/useMetricQuery";
import { deliveryBandChart, formatDays, lateRateChart, reviewChart } from "./fulfillmentChart";

export function Fulfillment() {
  const [view, setView] = useState<"delivery" | "reviews">("delivery");
  const [window, setWindow] = useState<OrderQuery["window"]>("month");
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [activeRange, setActiveRange] = useState<{ startDate: string; endDate: string } | null>(null);
  const [filterError, setFilterError] = useState("");
  const [period, setPeriod] = useState("");
  const [evidenceOpen, setEvidenceOpen] = useState(false);
  const query: OrderQuery = { window, ...(window === "full" ? {} : activeRange ?? {}) };
  const key = JSON.stringify(query);
  const state = useMetricQuery(async (signal) => {
    const [delivery, reviews] = await fetchSameRun(
      getOrderDelivery({ ...query, signal }), getOrderReviews({ ...query, signal }),
    );
    const [, publication] = await fetchSameRun(Promise.resolve(delivery), getPublication("orders", signal));
    return { delivery, reviews, publication, key };
  }, key);
  const result = state.data;
  const delivery = result?.delivery;
  const reviews = result?.reviews;
  const deliveryPoints = delivery?.data ?? [];
  const reviewPoints = reviews?.data ?? [];
  const periods = [...new Set([...deliveryPoints.map((row) => row.window_start),
    ...reviewPoints.map((row) => row.window_start)])].sort();
  const activePeriod = periods.includes(period) ? period : periods[periods.length - 1];
  const selectedDelivery = deliveryPoints.find((row) => row.window_start === activePeriod);
  const selectedReview = reviewPoints.find((row) => row.window_start === activePeriod);

  function applyRange() {
    if (Boolean(startDate) !== Boolean(endDate) || (startDate && startDate > endDate)) {
      setFilterError("请同时填写有效的起止日期，且开始日期不能晚于结束日期。");
      return;
    }
    setFilterError("");
    setPeriod("");
    setActiveRange(startDate ? { startDate, endDate } : null);
  }

  return <div className="fulfillment-page">
    <div className="module-intro"><div><span className="eyebrow">FULFILLMENT / REVIEWS</span>
      <p>分开看配送时效与评价结果。所有比率均来自已发布指标，缺少合格样本不补零。</p></div>
      <div className="filter-bar"><label htmlFor="fulfillment-window">订单粒度</label>
        <select id="fulfillment-window" value={window} onChange={(event) => { setWindow(event.target.value as OrderQuery["window"]); setPeriod("");
          if (event.target.value === "full") setActiveRange(null); }}>
          <option value="day">按日</option><option value="month">按月</option><option value="full">全量</option>
        </select></div></div>
    {window !== "full" && <div className="date-filter">
      <label>开始日期 <input aria-label="开始日期" type="date" value={startDate} onChange={(event) => setStartDate(event.target.value)} /></label>
      <label>结束日期 <input aria-label="结束日期" type="date" value={endDate} onChange={(event) => setEndDate(event.target.value)} /></label>
      <button type="button" className="secondary-button" onClick={applyRange}>应用日期范围</button>
      {filterError && <span role="alert">{filterError}</span>}
    </div>}
    {activeRange && window !== "full" && <p className="fulfillment-range">已应用范围：{activeRange.startDate} 至 {activeRange.endDate}</p>}
    <div className="fulfillment-tabs" role="tablist" aria-label="履约评价视角">
      <button type="button" role="tab" aria-selected={view === "delivery"} onClick={() => setView("delivery")}>履约</button>
      <button type="button" role="tab" aria-selected={view === "reviews"} onClick={() => setView("reviews")}>评价</button>
    </div>
    <MetricFrame title="履约与评价" meta={delivery?.meta} loading={state.loading} error={state.error}
      empty={!!result && deliveryPoints.length === 0 && reviewPoints.length === 0} retry={state.retry}
      onEvidence={delivery ? () => setEvidenceOpen(true) : undefined}>
      {result && <>
        {result.key !== key && <p className="notice">正在更新筛选；以下仍是上一次日期条件下的结果。</p>}
        {periods.length > 1 && <label className="date-select">业务窗口 <select aria-label="业务窗口" value={activePeriod} onChange={(event) => setPeriod(event.target.value)}>
          {periods.map((value) => <option key={value} value={value}>{value}</option>)}</select></label>}
        {view === "delivery" ? <div className="fulfillment-delivery" role="tabpanel">
          <div className="fulfillment-head"><div><span className="eyebrow">DELIVERY / DISTRIBUTION</span><h3>履约时效</h3>
            <p>平均值与分位数展示配送时间；P90 表示九成合格订单不超过该天数。</p></div>
            {selectedDelivery && <div className="fulfillment-sample">合格订单 {formatCount(selectedDelivery.delivery_eligible_order_count)} · 排除订单 {formatCount(selectedDelivery.delivery_excluded_order_count)}</div>}</div>
          {selectedDelivery ? <><div className="fulfillment-quantiles">
            <div><span>中位时长 P50</span><strong>{formatDays(selectedDelivery.delivery_days_p50)}</strong></div>
            <div><span>高位时长 P90</span><strong>{formatDays(selectedDelivery.delivery_days_p90)}</strong></div>
            <div><span>平均时长</span><strong>{formatDays(selectedDelivery.delivery_days_avg)}</strong></div>
          </div><div className="fulfillment-chart-grid"><section className="surface-panel"><div className="panel-heading"><h4>配送天数 · 平均 / P50 / P90</h4></div>
            <Chart label="配送天数平均值与分位数趋势" option={deliveryBandChart(deliveryPoints)} fallback={<table><thead><tr><th>窗口</th><th>平均</th><th>P50</th><th>P90</th><th>合格 / 排除</th></tr></thead><tbody>
              {deliveryPoints.map((row) => <tr key={row.window_start}><td>{row.window_start}</td><td>{formatDays(row.delivery_days_avg)}</td><td>{formatDays(row.delivery_days_p50)}</td><td>{formatDays(row.delivery_days_p90)}</td><td>{formatCount(row.delivery_eligible_order_count)} / {formatCount(row.delivery_excluded_order_count)}</td></tr>)}
            </tbody></table>} /></section><section className="surface-panel"><div className="panel-heading"><h4>晚到率</h4></div>
            <div className="fulfillment-late"><strong>{formatRate(selectedDelivery.late_delivery_rate, selectedDelivery.late_delivery_order_count, selectedDelivery.late_delivery_eligible_order_count)}</strong>
              <small>晚到排除订单 {formatCount(selectedDelivery.late_delivery_excluded_order_count)}</small></div>
            <Chart label="晚到率时间趋势" height={225} option={lateRateChart(deliveryPoints)} fallback={<table><thead><tr><th>窗口</th><th>晚到率</th></tr></thead><tbody>
              {deliveryPoints.map((row) => <tr key={row.window_start}><td>{row.window_start}</td><td>{formatRate(row.late_delivery_rate, row.late_delivery_order_count, row.late_delivery_eligible_order_count)}</td></tr>)}
            </tbody></table>} /></section></div></> : <p className="notice">当前业务窗口没有履约结果。</p>}
        </div> : <div className="fulfillment-review" role="tabpanel"><div className="fulfillment-head"><div><span className="eyebrow">REVIEWS / COVERAGE</span><h3>评价概况</h3>
          <p>仅展示后端已有的均分、覆盖率和低分率，不推导星级分布。</p></div></div>
          {selectedReview ? <><div className="review-ledger">
            <div><span>评分均值</span><strong>{selectedReview.review_score_avg === null ? "不适用" : Number(selectedReview.review_score_avg).toFixed(2)}</strong><small>已评价 {formatCount(selectedReview.reviewed_order_count)} 单</small></div>
            <div><span>评价覆盖率</span><strong>{formatRate(selectedReview.review_coverage_rate, selectedReview.reviewed_order_count, selectedReview.all_order_count)}</strong><small>全部 {formatCount(selectedReview.all_order_count)} 单</small></div>
            <div><span>低分率</span><strong>{formatRate(selectedReview.low_score_rate, selectedReview.low_score_order_count, selectedReview.reviewed_order_count)}</strong><small>多次评价订单 {formatCount(selectedReview.multi_review_order_count)}</small></div>
          </div><div className="review-small-multiples">
            {(["score", "coverage", "low"] as const).map((metric) => <section className="surface-panel" key={metric}><h4>{metric === "score" ? "评分均值" : metric === "coverage" ? "评价覆盖率" : "低分率"}</h4>
              <Chart label={`${metric === "score" ? "评分均值" : metric === "coverage" ? "评价覆盖率" : "低分率"}时间趋势`} height={230} option={reviewChart(reviewPoints, metric)}
                fallback={<table><thead><tr><th>窗口</th><th>值</th></tr></thead><tbody>{reviewPoints.map((row) => <tr key={row.window_start}><td>{row.window_start}</td><td>{metric === "score" ? (row.review_score_avg === null ? "不适用" : Number(row.review_score_avg).toFixed(2)) : metric === "coverage" ? formatRate(row.review_coverage_rate, row.reviewed_order_count, row.all_order_count) : formatRate(row.low_score_rate, row.low_score_order_count, row.reviewed_order_count)}</td></tr>)}</tbody></table>} />
            </section>)}
          </div></> : <p className="notice">当前业务窗口没有评价结果。</p>}
        </div>}
      </>}
    </MetricFrame>
    {delivery && <EvidenceDrawer open={evidenceOpen} onClose={() => setEvidenceOpen(false)} meta={delivery.meta}
      domain="orders" publishedAt={result?.publication.data.published_at} />}
  </div>;
}
