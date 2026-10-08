import { useEffect, useRef, useState } from "react";

import { getDefinitions } from "../lib/api";
import { formatDateTime } from "../lib/format";
import type { DefinitionsResponse, MetricMeta, OrderMeta, BehaviorMeta } from "../lib/types";

function snapshots(meta: MetricMeta): [string, string][] {
  if (meta.metric_version === "orders-v1") {
    const order = meta as OrderMeta;
    return [...Object.entries(order.source_snapshots ?? {}), ...Object.entries(order.curated_snapshots ?? {})];
  }
  return [["REES46 来源 Snapshot", (meta as BehaviorMeta).source_snapshot_id]];
}

export function EvidenceDrawer({ open, onClose, meta, domain, publishedAt, highlightMetric }: {
  open: boolean;
  onClose: () => void;
  meta: MetricMeta;
  domain: "orders" | "behavior";
  publishedAt?: string;
  highlightMetric?: string;
}) {
  const closeRef = useRef<HTMLButtonElement>(null);
  const [definitions, setDefinitions] = useState<DefinitionsResponse | null>(null);
  const [error, setError] = useState(false);

  useEffect(() => {
    if (!open) return;
    const previousFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    closeRef.current?.focus();
    return () => previousFocus?.focus();
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const controller = new AbortController();
    setDefinitions(null);
    setError(false);
    getDefinitions(domain, controller.signal).then(
      (result) => { if (!controller.signal.aborted) setDefinitions(result); },
      () => { if (!controller.signal.aborted) setError(true); },
    );
    return () => controller.abort();
  }, [open, domain, meta.metric_run_id]);

  if (!open) return null;
  const behavior = meta.metric_version === "behavior-v1" ? meta as BehaviorMeta : null;
  return <div className="drawer-backdrop" onMouseDown={(event) => { if (event.target === event.currentTarget) onClose(); }}>
    <aside className="evidence-drawer" role="dialog" aria-modal="true" aria-label="数据证据"
      onKeyDown={(event) => {
        if (event.key === "Escape") onClose();
        if (event.key === "Tab") {
          const focusables = Array.from(event.currentTarget.querySelectorAll<HTMLElement>("button, a[href], [tabindex]:not([tabindex='-1'])"));
          const first = focusables[0];
          const last = focusables[focusables.length - 1];
          if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus(); }
          else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus(); }
        }
      }}>
      <div className="drawer-header"><div><span className="eyebrow">EVIDENCE / 可追溯性</span><h2>数据证据</h2></div>
        <button type="button" ref={closeRef} className="text-button" aria-label="关闭数据证据" onClick={onClose}>关闭</button></div>
      {behavior && <p className="evidence-replay-note">历史回放 · 展示原始业务发生窗口，不代表当前实时交易</p>}
      <dl className="evidence-facts">
        <div><dt>数据集</dt><dd>{meta.dataset_id}</dd></div>
        <div><dt>指标版本</dt><dd>{meta.metric_version}</dd></div>
        <div><dt>运行 ID</dt><dd className="break-all">{meta.metric_run_id}</dd></div>
        <div><dt>业务窗口</dt><dd>{meta.window_start} 至 {meta.window_end}</dd></div>
        {behavior && <div><dt>来源表</dt><dd className="break-all">{behavior.source_table}</dd></div>}
        {behavior && <div><dt>回放区间</dt><dd>{behavior.replay_first_at && behavior.replay_last_at
          ? `${formatDateTime(behavior.replay_first_at)} 至 ${formatDateTime(behavior.replay_last_at)}`
          : "回放时间未记录（旧发布）"}</dd></div>}
        <div><dt>计算时间</dt><dd>{formatDateTime(meta.calculated_at)}</dd></div>
        <div><dt>发布时间</dt><dd>{publishedAt ? formatDateTime(publishedAt) : "当前响应未提供"}</dd></div>
      </dl>
      <h3>来源 Snapshot</h3>
      <ul className="snapshot-list">{snapshots(meta).map(([name, id]) => <li key={name}><span>{name}</span><code>{id}</code></li>)}</ul>
      <h3>限制与警告</h3>
      {meta.warnings.length ? <ul className="evidence-warnings">{meta.warnings.map((warning) => <li key={warning}>{warning}</li>)}</ul> : <p>当前响应未列出警告。</p>}
      <h3>指标定义</h3>
      {error && <p role="alert">定义接口暂不可用；不会显示缓存的演示定义。</p>}
      {!error && !definitions && <p role="status">正在加载定义…</p>}
      {definitions && <ul className="definition-list">{[...definitions.definitions].sort((a, b) => Number(b.metric_name === highlightMetric) - Number(a.metric_name === highlightMetric)).map((definition) => <li key={definition.metric_name} className={definition.metric_name === highlightMetric ? "definition--highlight" : undefined}>
        <strong>{definition.display_name}</strong><code>{definition.metric_name}</code>
        <p>{definition.formula}</p>
        {definition.limitations.map((limit) => <small key={limit}>{limit}</small>)}
      </li>)}</ul>}
    </aside>
  </div>;
}
