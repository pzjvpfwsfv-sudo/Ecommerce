import { useState } from "react";

import { Chart } from "../components/Chart";
import { EvidenceDrawer } from "../components/EvidenceDrawer";
import { MetricFrame } from "../components/MetricFrame";
import { fetchSameRun, getBehaviorRankings, getOrderRankings, getPublication } from "../lib/api";
import { formatCount, formatRate, formatValue } from "../lib/format";
import { metricLinkParam } from "../lib/metricNavigation";
import type { BehaviorDimension, BehaviorRankingPoint, BehaviorSort, OrderDimension, OrderRankingPoint, OrderSort } from "../lib/types";
import { useMetricQuery } from "../lib/useMetricQuery";
import { behaviorDimensions, behaviorSortLabels, behaviorValue, orderDimensions, orderSortLabels, orderSorts, orderValue, rankingChart, rankingName } from "./rankingOptions";

type Domain = "orders" | "behavior";
type RankingResult =
  | { domain: "orders"; key: string; sort: OrderSort; limit: number; response: Awaited<ReturnType<typeof getOrderRankings>>; publishedAt: string }
  | { domain: "behavior"; key: string; sort: BehaviorSort; limit: number; response: Awaited<ReturnType<typeof getBehaviorRankings>>; publishedAt: string };

function OrderDetail({ row, currency }: { row: OrderRankingPoint; currency: string | null }) {
  return <dl className="ranking-detail-list">
    <div><dt>维度 ID</dt><dd>{row.dimension_id}</dd></div>
    <div><dt>订单数</dt><dd>{formatCount(row.ranking_order_count)}</dd></div>
    <div><dt>客户数</dt><dd>{formatCount(row.ranking_customer_count)}</dd></div>
    {row.ranking_item_row_count !== null && <div><dt>商品行数</dt><dd>{formatCount(row.ranking_item_row_count)}</dd></div>}
    {row.ranking_item_value_sum !== null && <div><dt>商品值</dt><dd>{formatValue(row.ranking_item_value_sum, currency)}</dd></div>}
    {row.ranking_freight_value_sum !== null && <div><dt>运费值</dt><dd>{formatValue(row.ranking_freight_value_sum, currency)}</dd></div>}
    {row.ranking_payment_value_sum !== null && <div><dt>支付值</dt><dd>{formatValue(row.ranking_payment_value_sum, currency)}</dd></div>}
    {row.ranking_late_delivery_eligible_order_count !== null && <div><dt>晚到率</dt><dd>{formatRate(row.ranking_late_delivery_rate,
      row.ranking_late_delivery_order_count ?? undefined, row.ranking_late_delivery_eligible_order_count)}</dd></div>}
  </dl>;
}

function BehaviorDetail({ row }: { row: BehaviorRankingPoint }) {
  return <dl className="ranking-detail-list">
    <div><dt>维度 ID</dt><dd>{row.dimension_id}</dd></div>
    <div><dt>浏览数</dt><dd>{formatCount(row.view_count)}</dd></div>
    <div><dt>加购数</dt><dd>{formatCount(row.cart_count)}</dd></div>
    <div><dt>购买数</dt><dd>{formatCount(row.purchase_count)}</dd></div>
    <div><dt>用户数</dt><dd>{formatCount(row.unique_user_count)}</dd></div>
    <div><dt>购买金额代理值</dt><dd>{row.purchase_amount_proxy === null ? "不适用" : formatValue(row.purchase_amount_proxy, null)}</dd></div>
  </dl>;
}

export function Rankings() {
  const [domain, setDomain] = useState<Domain>(() =>
    metricLinkParam(globalThis.location.hash, "/rankings", "source", ["orders", "behavior"] as const) ?? "orders");
  const [orderDimension, setOrderDimension] = useState<OrderDimension>("product");
  const [orderSort, setOrderSort] = useState<OrderSort>("order_count");
  const [orderWindow, setOrderWindow] = useState<"day" | "month" | "full">(() =>
    metricLinkParam(globalThis.location.hash, "/rankings", "window", ["day", "month", "full"] as const) ?? "full");
  const [behaviorDimension, setBehaviorDimension] = useState<BehaviorDimension>("product");
  const [behaviorSort, setBehaviorSort] = useState<BehaviorSort>("views");
  const [behaviorWindow, setBehaviorWindow] = useState<"day" | "full">(() =>
    metricLinkParam(globalThis.location.hash, "/rankings", "window", ["day", "full"] as const) ?? "full");
  const [limit, setLimit] = useState(20);
  const [period, setPeriod] = useState("");
  const [selectedId, setSelectedId] = useState("");
  const [evidenceOpen, setEvidenceOpen] = useState(false);
  const queryKey = domain === "orders"
    ? `${domain}:${orderWindow}:${orderDimension}:${orderSort}:${limit}`
    : `${domain}:${behaviorWindow}:${behaviorDimension}:${behaviorSort}:${limit}`;
  const state = useMetricQuery<RankingResult>(async (signal) => {
    if (domain === "orders") {
      const [response, publication] = await fetchSameRun(
        getOrderRankings({ window: orderWindow, dimension: orderDimension, sortBy: orderSort, limit, signal }),
        getPublication("orders", signal),
      );
      return { domain, key: queryKey, sort: orderSort, limit, response, publishedAt: publication.data.published_at };
    }
    const [response, publication] = await fetchSameRun(
      getBehaviorRankings({ window: behaviorWindow, dimension: behaviorDimension, sortBy: behaviorSort, limit, signal }),
      getPublication("behavior", signal),
    );
    return { domain, key: queryKey, sort: behaviorSort, limit, response, publishedAt: publication.data.published_at };
  }, queryKey);
  const result = state.data?.domain === domain ? state.data : undefined;
  const response = result?.response;
  const periods = [...new Set(response?.data.map((row) => row.window_start) ?? [])].sort();
  const activePeriod = periods.includes(period) ? period : periods[periods.length - 1];
  const rows = response?.data.filter((row) => row.window_start === activePeriod) ?? [];
  const isOldSelection = !!result && result.key !== queryKey;
  const orderRows = result?.domain === "orders" ? rows as OrderRankingPoint[] : [];
  const behaviorRows = result?.domain === "behavior" ? rows as BehaviorRankingPoint[] : [];
  const selectedOrder = orderRows.find((row) => row.dimension_id === selectedId);
  const selectedBehavior = behaviorRows.find((row) => row.dimension_id === selectedId);
  const sortLabel = result?.domain === "orders" ? orderSortLabels[result.sort]
    : result?.domain === "behavior" ? behaviorSortLabels[result.sort]
      : domain === "orders" ? orderSortLabels[orderSort] : behaviorSortLabels[behaviorSort];
  const currency = result?.domain === "orders" ? result.response.meta.source_currency : null;
  const selectedSort = result?.sort ?? (domain === "orders" ? orderSort : behaviorSort);
  const displayedLimit = result?.limit ?? limit;

  function clearSelection() { setSelectedId(""); setPeriod(""); }
  return <div className="rankings-page">
    <div className="module-intro"><div><span className="eyebrow">RANKINGS / DIMENSIONS</span>
      <p>只比较同一数据源的聚合排名。Top N 不是全体分布，点击行仅查看已返回的聚合值。</p></div></div>
    <div className="ranking-domain-switch" role="group" aria-label="排行数据源">
      <button type="button" aria-pressed={domain === "orders"} onClick={() => { setDomain("orders"); clearSelection(); }}>Olist 订单域</button>
      <button type="button" aria-pressed={domain === "behavior"} onClick={() => { setDomain("behavior"); clearSelection(); }}>REES46 行为域</button>
    </div>
    <div className="ranking-filters">
      <label>维度 <select aria-label="维度" value={domain === "orders" ? orderDimension : behaviorDimension}
        onChange={(event) => { if (domain === "orders") { setOrderDimension(event.target.value as OrderDimension); setOrderSort("order_count"); }
          else { setBehaviorDimension(event.target.value as BehaviorDimension); setBehaviorSort("views"); } clearSelection(); }}>
        {domain === "orders" ? Object.entries(orderDimensions).map(([value, name]) => <option key={value} value={value}>{name}</option>)
          : Object.entries(behaviorDimensions).map(([value, name]) => <option key={value} value={value}>{name}</option>)}
      </select></label>
      <label>排序指标 <select aria-label="排序指标" value={selectedSort}
        onChange={(event) => { if (domain === "orders") setOrderSort(event.target.value as OrderSort); else setBehaviorSort(event.target.value as BehaviorSort); clearSelection(); }}>
        {domain === "orders" ? orderSorts[orderDimension].map((value) => <option key={value} value={value}>{orderSortLabels[value]}</option>)
          : Object.entries(behaviorSortLabels).map(([value, name]) => <option key={value} value={value}>{name}</option>)}
      </select></label>
      <label>时间粒度 <select aria-label="时间粒度" value={domain === "orders" ? orderWindow : behaviorWindow}
        onChange={(event) => { if (domain === "orders") setOrderWindow(event.target.value as typeof orderWindow);
          else setBehaviorWindow(event.target.value as typeof behaviorWindow); clearSelection(); }}>
        <option value="full">全量</option><option value="day">按日</option>
        {domain === "orders" && <option value="month">按月</option>}
      </select></label>
      <label>返回数量 <select aria-label="返回数量" value={limit} onChange={(event) => { setLimit(Number(event.target.value)); clearSelection(); }}>
        <option value={20}>Top 20</option><option value={50}>Top 50</option><option value={100}>Top 100</option>
      </select></label>
    </div>
    <MetricFrame title={domain === "orders" ? "Olist 聚合排名" : "REES46 聚合排名"}
      meta={response?.meta} loading={state.loading || (!!state.data && !result)} error={state.error}
      empty={!!response && response.data.length === 0} retry={state.retry}
      onEvidence={response ? () => setEvidenceOpen(true) : undefined}>
      {result && <>
        {isOldSelection && <p className="notice" role="status">筛选正在更新；以下仍是上一次选择的已发布结果。</p>}
        {result.domain === "behavior" && <p className="ranking-scope">数据范围：{result.response.meta.data_scope === "g2c-correctness-subset" ? "正确性子集" : "稳定用户 2% 样本"} · {formatCount(result.response.meta.source_event_count)} 条来源事件</p>}
        {response && response.meta.warnings.length > 0 && <ul className="ranking-warnings">
          {response.meta.warnings.map((warning) => <li key={warning}>{warning}</li>)}
        </ul>}
        <div className="ranking-context"><strong>仅展示 Top {displayedLimit} 排名（实际返回 {rows.length} 行）</strong><span>指标：{sortLabel} · 窗口：{activePeriod}</span></div>
        {periods.length > 1 && <label className="date-select">业务窗口 <select value={activePeriod} onChange={(event) => { setPeriod(event.target.value); setSelectedId(""); }}>
          {periods.map((value) => <option key={value} value={value}>{value}</option>)}</select></label>}
        {orderRows.some((row) => row.payment_value_is_additive === false) && <p className="ranking-caveat">卖家地域支付值不可加总；跨地域相加可能重复计算订单。</p>}
        {rows.length ? <div className="ranking-layout"><section className="surface-panel ranking-chart-panel">
          <div className="panel-heading"><h3>前列比较</h3><span>{sortLabel}</span></div>
          <Chart label={`${domain === "orders" ? "Olist" : "REES46"} ${sortLabel} Top ${displayedLimit} 排行`}
            height={Math.max(300, Math.min(680, rows.length * 31 + 75))} option={rankingChart(rows as OrderRankingPoint[] | BehaviorRankingPoint[], selectedSort, currency)}
            fallback={<table><thead><tr><th>维度</th><th>{sortLabel}</th></tr></thead><tbody>{rows.map((row) => <tr key={row.dimension_id}>
              <td>{rankingName(row)}</td><td>{result.domain === "orders" ? orderValue(row as OrderRankingPoint, result.sort, currency) : behaviorValue(row as BehaviorRankingPoint, result.sort)}</td>
            </tr>)}</tbody></table>} />
        </section><section className="surface-panel ranking-table-panel"><div className="panel-heading"><h3>返回行</h3><span>选择一行查看聚合证据</span></div>
          <div className="ranking-table-wrap"><table><thead><tr><th>维度</th><th>{sortLabel}</th><th>详情</th></tr></thead><tbody>
            {rows.map((row) => <tr key={row.dimension_id} className={selectedId === row.dimension_id ? "ranking-row--selected" : undefined}>
              <td>{rankingName(row)}</td><td>{result.domain === "orders" ? orderValue(row as OrderRankingPoint, result.sort, currency) : behaviorValue(row as BehaviorRankingPoint, result.sort)}</td>
              <td><button type="button" className="text-button" onClick={() => setSelectedId(row.dimension_id)} aria-label={`查看 ${rankingName(row)} 聚合详情`}>查看</button></td>
            </tr>)}
          </tbody></table></div>
          {selectedOrder && <div className="ranking-detail"><h4>{rankingName(selectedOrder)} · 聚合详情</h4><OrderDetail row={selectedOrder} currency={currency} /></div>}
          {selectedBehavior && <div className="ranking-detail"><h4>{rankingName(selectedBehavior)} · 聚合详情</h4><BehaviorDetail row={selectedBehavior} /></div>}
        </section></div> : <p className="notice">当前业务窗口没有排行结果。</p>}
      </>}
    </MetricFrame>
    {result && <EvidenceDrawer open={evidenceOpen} onClose={() => setEvidenceOpen(false)} meta={result.response.meta}
      domain={result.domain} publishedAt={result.publishedAt} />}
  </div>;
}
