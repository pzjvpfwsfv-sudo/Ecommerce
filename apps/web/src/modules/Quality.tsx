import { useState } from "react";

import { EvidenceDrawer } from "../components/EvidenceDrawer";
import { MetricFrame } from "../components/MetricFrame";
import { fetchSameRun, getBehaviorQuality, getOrderQuality, getPublication } from "../lib/api";
import { formatCount, formatRate } from "../lib/format";
import type { OrderQuality, PublicationData } from "../lib/types";
import { useMetricQuery } from "../lib/useMetricQuery";

type QualityResult =
  | { domain: "orders"; quality: Awaited<ReturnType<typeof getOrderQuality>>; publication: { data: PublicationData } }
  | { domain: "behavior"; quality: Awaited<ReturnType<typeof getBehaviorQuality>>; publication: { data: PublicationData } };

const reportableNames: Record<string, string> = {
  duplicate_review_id_count: "重复 review_id",
  unknown_order_status_count: "未知订单状态",
  unknown_payment_type_count: "未知支付方式",
  missing_product_category_count: "缺失商品品类",
  missing_category_translation_count: "缺失品类译名",
  multi_review_order_count: "多次评价订单",
  missing_optional_time_count: "缺失可选时间",
  lifecycle_order_anomaly_count: "订单生命周期异常",
  payment_item_total_mismatch_count: "支付与商品/运费不一致",
};

function sumRows(rows: Record<string, number>): number {
  return Object.values(rows).reduce((total, count) => total + count, 0);
}

function RowTable({ title, rows }: { title: string; rows: Record<string, number> }) {
  return <section className="quality-row-table"><h4>{title}</h4><table><thead><tr><th>实体 / 校验项</th><th>行数</th></tr></thead><tbody>
    {Object.entries(rows).sort(([a], [b]) => a.localeCompare(b)).map(([name, count]) => <tr key={name}><td><code>{name}</code></td><td>{formatCount(count)}</td></tr>)}
  </tbody></table></section>;
}

function OrderQualityView({ quality, status }: { quality: OrderQuality; status: string }) {
  const reportable = Object.entries(quality.reportable_quality).sort((a, b) => b[1] - a[1]);
  const nonzero = reportable.filter(([, value]) => value > 0);
  return <div className="quality-order">
    <div className="quality-split-head"><div><span className="eyebrow">OLIST / TRACEABLE PIPELINE</span><h3>订单域质量路径</h3>
      <p>流程中的多表行数合计不是订单总数；逐表对账见下方明细。</p></div>
      <div className="quality-gate"><span>硬门禁 / 对账状态</span><strong>{quality.reconciliation_status}</strong><small>硬门禁异常 {formatCount(quality.duplicate_key_count + quality.orphan_key_count + quality.invalid_value_count + quality.temporal_anomaly_count)}</small></div></div>
    <ol className="quality-path">
      <li><span>01</span><h4>原始来源</h4><strong>{formatCount(sumRows(quality.raw_row_counts))}</strong><small>各表行数之和 · {Object.keys(quality.raw_row_counts).length} 类实体</small></li>
      <li><span>02</span><h4>规范化</h4><strong>{formatCount(sumRows(quality.normalized_row_counts))}</strong><small>各表行数之和 · {Object.keys(quality.normalized_row_counts).length} 类实体</small></li>
      <li><span>03</span><h4>入湖 Iceberg</h4><strong>{formatCount(sumRows(quality.iceberg_row_counts))}</strong><small>各表行数之和 · {Object.keys(quality.iceberg_row_counts).length} 类表</small></li>
      <li><span>04</span><h4>指标发布</h4><strong>{status}</strong><small>run 经过门禁后发布</small></li>
    </ol>
    <div className="quality-issues"><div><span className="eyebrow">REPORTABLE / 仍需说明</span><h3>可报告异常</h3>
      <p>可报告异常不等于门禁失败。门禁 PASS 表示符合当前发布规则，不表示来源完全无问题。</p></div>
      {nonzero.length ? <ul>{reportable.map(([name, count]) => <li key={name} className={count > 0 ? "quality-issue--nonzero" : undefined}>
        <span>{reportableNames[name] ?? name}</span><strong>{formatCount(count)}</strong></li>)}</ul> : <p>当前报告项均为 0。</p>}</div>
    <details className="quality-reconciliation"><summary>查看逐表行数</summary><div className="quality-matrix">
      <RowTable title="原始来源" rows={quality.raw_row_counts} />
      <RowTable title="规范化" rows={quality.normalized_row_counts} />
      <RowTable title="Iceberg" rows={quality.iceberg_row_counts} />
      <RowTable title="事实表对账" rows={quality.fact_reconciliations} />
    </div></details>
  </div>;
}

function BehaviorQualityView({ quality, status, scope, warnings }: {
  quality: Awaited<ReturnType<typeof getBehaviorQuality>>["data"];
  status: string; scope: string; warnings: string[];
}) {
  const issues = [
    ["重复事件", quality.duplicate_event_count], ["缺失会话", quality.missing_session_count],
    ["未知品类", quality.unknown_category_count], ["未知品牌", quality.unknown_brand_count],
    ["无效事件类型", quality.invalid_event_type_count], ["无效价格", quality.invalid_price_count],
    ["无效派生日期", quality.invalid_derived_date_count],
  ] as const;
  return <div className="quality-behavior"><div className="quality-split-head"><div>
    <span className="eyebrow">REES46 / EVENT QUALITY</span><h3>行为域质量路径</h3>
    <p>数据范围：{scope === "g2c-correctness-subset" ? "正确性子集" : "稳定用户 2% 样本"}。不与 Olist 订单行数合并。</p>
    {warnings.map((warning) => <p className="quality-warning" key={warning}>{warning}</p>)}
    </div><div className="quality-gate"><span>行为指标对账</span><strong>{quality.reconciliation_status}</strong><small>事件质量与订单质量分别验收</small></div></div>
    <ol className="quality-path quality-path--behavior">
      <li><span>01</span><h4>来源事件</h4><strong>{formatCount(quality.source_event_count)}</strong><small>当前发布范围</small></li>
      <li><span>02</span><h4>清洗 / 迟到</h4><strong>{formatCount(quality.clean_event_count)}</strong><small>清洗事件 · 迟到 {formatCount(quality.late_event_count)}</small></li>
      <li><span>03</span><h4>总览对账</h4><strong>{formatCount(quality.overview_event_count)}</strong><small>清洗率 {formatRate(quality.clean_event_rate, quality.clean_event_count, quality.source_event_count)}</small></li>
      <li><span>04</span><h4>指标发布</h4><strong>{status}</strong><small>单独的行为域 run</small></li>
    </ol>
    <section className="behavior-anomalies"><div><span className="eyebrow">EVENT DIAGNOSTICS</span><h3>事件质量明细</h3>
      <p>迟到率 {formatRate(quality.late_event_rate, quality.late_event_count, quality.source_event_count)}；未知维度与缺失会话保留原始计数。</p></div>
      <ul>{issues.map(([name, count]) => <li key={name}><span>{name}</span><strong>{formatCount(count)}</strong></li>)}</ul>
    </section>
  </div>;
}

export function Quality() {
  const [domain, setDomain] = useState<"orders" | "behavior">("orders");
  const [evidenceOpen, setEvidenceOpen] = useState(false);
  const state = useMetricQuery<QualityResult>(async (signal) => {
    if (domain === "orders") {
      const [quality, publication] = await fetchSameRun(getOrderQuality(signal), getPublication("orders", signal));
      return { domain, quality, publication };
    }
    const [quality, publication] = await fetchSameRun(getBehaviorQuality(signal), getPublication("behavior", signal));
    return { domain, quality, publication };
  }, domain);
  const result = state.data?.domain === domain ? state.data : undefined;
  return <div className="quality-page">
    <div className="module-intro"><div><span className="eyebrow">DATA QUALITY / AUDIT TRAIL</span>
      <p>让来源、处理、入湖和发布有迹可循；“通过”与“仍有异常”可以同时成立。</p></div></div>
    <div className="ranking-domain-switch" role="group" aria-label="质量数据源">
      <button type="button" aria-pressed={domain === "orders"} onClick={() => { setDomain("orders"); setEvidenceOpen(false); }}>Olist 订单域</button>
      <button type="button" aria-pressed={domain === "behavior"} onClick={() => { setDomain("behavior"); setEvidenceOpen(false); }}>REES46 行为域</button>
    </div>
    <MetricFrame title={domain === "orders" ? "Olist 来源与对账" : "REES46 事件质量"}
      meta={result?.quality.meta} loading={state.loading || (!!state.data && !result)} error={state.error}
      retry={state.retry} onEvidence={result ? () => setEvidenceOpen(true) : undefined}>
      {result?.domain === "orders" && <OrderQualityView quality={result.quality.data} status={result.publication.data.status} />}
      {result?.domain === "behavior" && <BehaviorQualityView quality={result.quality.data} status={result.publication.data.status}
        scope={result.quality.meta.data_scope} warnings={result.quality.meta.warnings} />}
    </MetricFrame>
    {result && <EvidenceDrawer open={evidenceOpen} onClose={() => setEvidenceOpen(false)}
      meta={result.quality.meta} domain={result.domain} publishedAt={result.publication.data.published_at} />}
  </div>;
}
