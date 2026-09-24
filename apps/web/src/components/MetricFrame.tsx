import type { ReactNode } from "react";

import { ApiError } from "../lib/http";
import type { MetricMeta } from "../lib/types";

function errorText(error: Error): string {
  if (error instanceof ApiError) {
    if (error.status === 401) return "会话已过期，请重新登录。";
    if (error.status === 403) return "当前账户没有访问权限。";
    if (error.status === 422) return "筛选范围不被接口支持，请调整条件。";
    if (error.status === 503) return "指标暂不可用，请稍后重试。";
  }
  return "数据未能通过校验，请重试或查看接口状态。";
}

export function MetricFrame({ title, meta, loading, error, empty, retry, onEvidence, children }: {
  title: string;
  meta?: MetricMeta;
  loading: boolean;
  error: Error | null;
  empty?: boolean;
  retry: () => void;
  onEvidence?: () => void;
  children: ReactNode;
}) {
  if (loading && !meta) return <section className="metric-state" role="status">正在载入{title}…</section>;
  if (error && !meta) return <section className="metric-state" role="alert"><h3>{errorText(error)}</h3><button onClick={retry}>重试</button></section>;
  if (empty && !loading) return <section className="metric-state"><h3>当前筛选没有已发布结果</h3><p>不会用演示数据补齐空白。</p><button onClick={retry}>重新加载</button></section>;

  return <section className="metric-frame" aria-label={title}>
    <div className="metric-frame-head"><div><span className="eyebrow">PUBLISHED METRICS / 已发布指标</span><h2>{title}</h2></div>
      {onEvidence && <button type="button" className="secondary-button" onClick={onEvidence}>查看数据证据</button>}</div>
    {meta && <p className="metric-identity">{meta.dataset_id} · {meta.metric_version} · {meta.window_start} 至 {meta.window_end}</p>}
    {loading && <p className="notice" role="status">正在更新筛选；以下仍是上一筛选结果。</p>}
    {error && <p className="notice notice--error" role="alert">{errorText(error)}以下保留上一筛选结果。</p>}
    {children}
  </section>;
}
