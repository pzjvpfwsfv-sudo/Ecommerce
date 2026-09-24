import { useState } from "react";

import { Chart } from "../components/Chart";
import { EvidenceDrawer } from "../components/EvidenceDrawer";
import { MetricFrame } from "../components/MetricFrame";
import { fetchSameRun, getOrderOverview, getOrderPayments, getPublication, type OrderQuery } from "../lib/api";
import { formatCount, formatRate, formatValue } from "../lib/format";
import { useMetricQuery } from "../lib/useMetricQuery";
import { orderStatusChart, paymentChart } from "./ordersChart";

export function Orders() {
  const [window, setWindow] = useState<OrderQuery["window"]>("month");
  const [period, setPeriod] = useState("");
  const [mode, setMode] = useState<"count" | "value">("count");
  const [evidenceOpen, setEvidenceOpen] = useState(false);
  const state = useMetricQuery(async (signal) => {
    const [overview, payments] = await fetchSameRun(
      getOrderOverview({ window, signal }), getOrderPayments({ window, signal }),
    );
    const [, publication] = await fetchSameRun(Promise.resolve(overview), getPublication("orders", signal));
    return { overview, payments, publication };
  }, window);
  const overview = state.data?.overview;
  const payments = state.data?.payments;
  const publication = state.data?.publication;
  const periods = [...new Set(overview?.data.map((row) => row.window_start) ?? [])].sort();
  const activePeriod = periods.includes(period) ? period : periods[periods.length - 1];
  const selectedOverview = overview?.data.find((row) => row.window_start === activePeriod);
  const selectedPayments = payments?.data.filter((row) => row.window_start === activePeriod) ?? [];
  const total = selectedPayments.find((row) => row.is_all);
  const rows = selectedPayments.filter((row) => !row.is_all).sort((a, b) =>
    mode === "count" ? b.payment_order_count - a.payment_order_count : Number(b.payment_value_sum) - Number(a.payment_value_sum));
  const currency = overview?.meta.source_currency ?? null;

  return <div className="orders-page">
    <div className="module-intro"><div><span className="eyebrow">ORDERS / PAYMENTS</span>
      <p>订单状态与支付方式分开比较。支付值不是利润、营收或经审计的 GMV。</p></div>
      <div className="filter-bar"><label htmlFor="payments-window">订单粒度</label>
        <select id="payments-window" value={window} onChange={(event) => { setWindow(event.target.value as OrderQuery["window"]); setPeriod(""); }}>
          <option value="day">按日</option><option value="month">按月</option><option value="full">全量</option>
        </select></div></div>
    <MetricFrame title="订单与支付" meta={overview?.meta} loading={state.loading} error={state.error}
      empty={!!overview && (overview.data.length === 0 || payments?.data.length === 0)} retry={state.retry}
      onEvidence={overview ? () => setEvidenceOpen(true) : undefined}>
      {overview && payments && <>
        <div className="orders-layout"><section className="surface-panel orders-trend"><div className="panel-heading"><h3>订单量与送达率</h3><span>同一历史窗口 · 比率来自后端</span></div>
          <Chart label="订单数和送达率组合趋势" option={orderStatusChart(overview.data)} fallback={<table><thead><tr><th>窗口</th><th>订单数</th><th>送达率</th></tr></thead>
            <tbody>{overview.data.map((row) => <tr key={row.window_start}><td>{row.window_start}</td><td>{formatCount(row.order_count)}</td><td>{formatRate(row.delivered_rate, row.delivered_order_count, row.status_eligible_order_count)}</td></tr>)}</tbody></table>} />
          {selectedOverview && <div className="orders-status-line"><span><strong>{formatCount(selectedOverview.order_count)}</strong> 订单</span>
            <span>送达率 {formatRate(selectedOverview.delivered_rate, selectedOverview.delivered_order_count, selectedOverview.status_eligible_order_count)}</span>
            <span>排除 {formatCount(selectedOverview.status_excluded_order_count)} 单</span></div>}
        </section><section className="surface-panel payments-panel"><div className="panel-heading"><h3>支付方式比较</h3><span>仅展示当前窗口</span></div>
          {periods.length > 1 && <label className="date-select">时间窗口 <select value={activePeriod} onChange={(event) => setPeriod(event.target.value)}>
            {periods.map((value) => <option key={value} value={value}>{value}</option>)}</select></label>}
          <div className="payment-mode" role="group" aria-label="支付比较指标">
            <button type="button" aria-pressed={mode === "count"} onClick={() => setMode("count")}>订单数</button>
            <button type="button" aria-pressed={mode === "value"} onClick={() => setMode("value")}>支付值</button>
          </div>
          {total && <div className="payment-total"><span>{mode === "count" ? "当前窗口支付订单数" : "当前窗口支付值合计"}</span>
            <strong>{mode === "count" ? formatCount(total.payment_order_count) : formatValue(total.payment_value_sum, currency)}</strong></div>}
          {rows.length ? <><Chart label="支付方式横向排名" height={270} option={paymentChart(rows, mode, currency)} />
            <ul className="payment-rows">{rows.map((row) => <li key={row.payment_type}><span>{row.payment_type}</span><strong>{mode === "count" ? formatCount(row.payment_order_count) : formatValue(row.payment_value_sum, currency)}</strong></li>)}</ul>
          </> : <p className="notice">当前窗口没有支付方式数据。</p>}
          <small className="payment-warning">汇总行仅作为总计，不参与支付方式排名；未知币种不显示货币符号。</small>
        </section></div>
      </>}
    </MetricFrame>
    {overview && <EvidenceDrawer open={evidenceOpen} onClose={() => setEvidenceOpen(false)} meta={overview.meta}
      domain="orders" publishedAt={publication?.data.published_at} />}
  </div>;
}
