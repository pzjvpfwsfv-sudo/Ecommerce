import { useEffect, useState, type FormEvent } from "react";
import { Link } from "react-router-dom";

import type { AuthUser } from "../lib/auth";
import {
  askAgent, getAgentReport, listAgentReports, saveAgentReport,
  type AgentAnswer, type AgentEvidence, type AskResult, type MetricValue,
  type ReportListItem, type SavedReport,
} from "../lib/agent";
import { ApiError } from "../lib/http";
import "../styles/agent.css";

const templates = [
  { id: "orders_payments", label: "订单趋势与支付", question: "Olist 订单趋势和支付结构有哪些值得关注的变化？" },
  { id: "delivery_reviews", label: "配送与评价", question: "Olist 配送时效与低评分之间能观察到什么？" },
  { id: "rankings", label: "商品品类排行", question: "已发布的商品和品类排行说明了什么？" },
  { id: "behavior_funnel", label: "行为漏斗", question: "REES46 行为漏斗中哪些环节流失明显？" },
  { id: "definitions_quality", label: "口径与质量", question: "这些指标的口径和数据质量有哪些限制？" },
] as const;

const toolLabels: Record<string, string> = {
  search_knowledge: "检索知识库",
  get_published_behavior_metrics: "读取行为指标",
  get_published_order_metrics: "读取订单指标",
};

const fieldLabels: Record<string, string> = {
  window_type: "窗口", window_start: "开始日期", window_end: "结束日期",
  order_count: "订单数", payment_value_sum: "支付金额汇总",
  view_count: "浏览次数", cart_count: "加购次数", purchase_count: "购买次数",
  view_sessions: "浏览会话", completed_sessions: "完成会话",
  review_score_avg: "平均评分", late_delivery_rate: "延迟配送比例（0–1）",
};

const metricRoutes: Record<string, string> = {
  "/behavior": "行为与转化", "/orders": "订单与支付",
  "/rankings": "商品与品类", "/fulfillment": "履约与评价", "/quality": "数据质量",
};

function valueText(value: MetricValue): string {
  if (value === null) return "不适用";
  if (typeof value === "number") return new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 6 }).format(value);
  if (typeof value === "boolean") return value ? "是" : "否";
  return value;
}

function metaText(evidence: AgentEvidence, key: string): string {
  const value = evidence.meta[key];
  return Array.isArray(value) ? value.join("、") : value == null ? "未提供" : String(value);
}

function metricTarget(evidence: AgentEvidence): string {
  const params = new URLSearchParams();
  const requestedWindow = evidence.meta.requested_window;
  const windows = evidence.kind === "behavior" ? ["day", "full"] : ["day", "month", "full"];
  if (evidence.target !== "/quality" && typeof requestedWindow === "string" && windows.includes(requestedWindow)) {
    params.set("window", requestedWindow);
  }
  if (evidence.target === "/rankings") params.set("source", evidence.kind);
  if (evidence.target === "/fulfillment" && (evidence.meta.view === "delivery" || evidence.meta.view === "reviews")) {
    params.set("view", evidence.meta.view);
  }
  return `${evidence.target}${params.size ? `?${params.toString()}` : ""}`;
}

function requestError(cause: unknown): string {
  if (cause instanceof ApiError && cause.status === 403) return "当前账户没有权限执行此操作。";
  if (cause instanceof ApiError && cause.status === 504) return "分析请求超时，请稍后重试。";
  if (cause instanceof ApiError && cause.status === 422) return "问题格式不符合要求，请检查后重试。";
  return "分析服务暂不可用，请稍后重试。";
}

function KnowledgeCard({ evidence }: { evidence: AgentEvidence }) {
  const target = evidence.target.startsWith("/knowledge/") ? evidence.target : null;
  return <article className="agent-source agent-source--knowledge">
    <div className="agent-source-top"><span>知识引用</span><code>{evidence.evidence_id}</code></div>
    <h4>{metaText(evidence, "section")}</h4>
    <p>{evidence.text}</p>
    <small>{metaText(evidence, "source_label")} · {metaText(evidence, "source_ref")}</small>
    {target && <Link to={target}>查看原文定位 →</Link>}
  </article>;
}

function MetricCard({ evidence }: { evidence: AgentEvidence }) {
  const routeLabel = metricRoutes[evidence.target];
  const warnings = evidence.meta.warnings;
  const fields = Object.keys(evidence.rows[0] ?? {});
  return <article className="agent-source agent-source--metric">
    <div className="agent-source-top"><span>{evidence.kind === "orders" ? "Olist 订单指标" : "REES46 行为指标"}</span><code>{evidence.evidence_id}</code></div>
    <h4>{metaText(evidence, "view")} · {metaText(evidence, "requested_window")}</h4>
    <dl className="agent-provenance">
      <div><dt>数据集</dt><dd>{metaText(evidence, "dataset_id")}</dd></div>
      <div><dt>指标版本</dt><dd>{metaText(evidence, "metric_version")}</dd></div>
      <div><dt>发布 run</dt><dd>{metaText(evidence, "metric_run_id")}</dd></div>
      <div><dt>窗口</dt><dd>{metaText(evidence, "window_start")} 至 {metaText(evidence, "window_end")}</dd></div>
      <div><dt>计算时间</dt><dd>{metaText(evidence, "calculated_at")}</dd></div>
    </dl>
    {evidence.meta.data_scope === "g2c-correctness-subset" &&
      <p className="agent-caution">当前仅为正确性子集，不代表完整行为样本。</p>}
    {Array.isArray(warnings) && warnings.length > 0 && <ul className="agent-warnings">{warnings.map((warning) => <li key={warning}>{warning}</li>)}</ul>}
    {evidence.rows.length > 0 ? <div className="agent-table-scroll"><table>
      <thead><tr>{fields.map((field) => <th key={field}>{fieldLabels[field] ?? field}</th>)}</tr></thead>
      <tbody>{evidence.rows.map((row, index) => <tr key={index}>{fields.map((field) => <td key={field}>{valueText(row[field] ?? null)}</td>)}</tr>)}</tbody>
    </table></div> : <p className="agent-empty-inline">本次没有可展示的聚合行。</p>}
    {routeLabel && <Link to={metricTarget(evidence)}>前往{routeLabel} →</Link>}
  </article>;
}

function AnswerView({ answer, reportStatus }: { answer: AgentAnswer | null; reportStatus?: SavedReport["status"] }) {
  if (reportStatus === "invalidated") return <section className="agent-result-state agent-result-state--invalid">
    <span className="eyebrow">REPORT / INVALIDATED</span><h3>引用已失效</h3>
    <p>知识资料已撤回、删除或不再对当前账户可见。旧回答正文已隐藏，请重新检索。</p>
  </section>;
  if (!answer) return <section className="agent-result-state">
    <span className="eyebrow">READY / 待分析</span><h3>从一个可核对的问题开始。</h3>
    <p>选择建议提问或输入自己的问题。系统不会在选择模板时自动调用模型。</p>
  </section>;

  const statusLabel = answer.status === "answered" ? "已基于证据回答" :
    answer.status === "evidence_only" ? "仅展示证据" : "证据不足，拒绝作答";
  return <div className="agent-result-stack">
    <section className="agent-conclusion" aria-label="分析结论与状态">
      <div className="agent-result-heading"><span className={`agent-status agent-status--${answer.status}`}>{statusLabel}</span>
        {reportStatus === "historical" && <span className="agent-report-status">历史快照 · 原 run 保留</span>}
        {reportStatus === "unverified" && <span className="agent-report-status">当前发布状态待核验</span>}
      </div>
      <h3>{answer.status === "answered" ? "分析观察" : "可核对的证据"}</h3>
      {answer.insights.length > 0 && <ol className="agent-insights">{answer.insights.map((insight, index) => <li key={index}>
        <p>{insight.text}</p><small>引用 {insight.evidence_ids.join(" · ")}</small>
      </li>)}</ol>}
      {answer.fallback_summary && <p className="agent-fallback">{answer.fallback_summary}</p>}
      {answer.status === "refused" && !answer.fallback_summary && <p>缺少可验证证据，本次不作推断。</p>}
      <p className="agent-answer-foot">{answer.status === "answered" ? `模型：${answer.model_name ?? "未提供"}` : "未将降级路径标为模型回答"} · 检索版本：{answer.retrieval_version}</p>
    </section>
    <section className="agent-timeline" aria-label="工具执行记录">
      <div className="agent-panel-heading"><span className="eyebrow">TRACE / 步骤</span><h3>工具步骤</h3></div>
      {answer.trace.length === 0 ? <p className="agent-empty-inline">本次未成功执行工具。</p> :
        <ol aria-label="工具步骤">{answer.trace.map((step, index) => <li key={`${step.name}-${index}`}>
          <span className="agent-step-index">{String(index + 1).padStart(2, "0")}</span>
          <div><strong>{toolLabels[step.name] ?? step.name}</strong><code>{step.name}</code>
            <small>{step.status === "ok" ? "已完成" : "调用失败"} · {Math.round(step.elapsed_ms)} ms · {step.evidence_ids.length} 条证据</small></div>
        </li>)}</ol>}
      <p className="agent-timeline-note">仅显示工具事实，不展示模型内部思考或原始参数。</p>
    </section>
    <section className="agent-evidence" aria-label="可核对证据">
      <div className="agent-panel-heading"><span className="eyebrow">SOURCES / 证据</span><h3>来源与指标卡</h3></div>
      {answer.evidence.length === 0 ? <p className="agent-empty-inline">没有可展示的授权证据。</p> :
        <div className="agent-evidence-grid">{answer.evidence.map((item) => item.kind === "knowledge" ?
          <KnowledgeCard key={item.evidence_id} evidence={item} /> :
          <MetricCard key={item.evidence_id} evidence={item} />)}</div>}
    </section>
  </div>;
}

export function Agent({ user }: { user: AuthUser }) {
  const [question, setQuestion] = useState("");
  const [templateId, setTemplateId] = useState<string | null>(null);
  const [asking, setAsking] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [reportError, setReportError] = useState<string | null>(null);
  const [askResult, setAskResult] = useState<AskResult | null>(null);
  const [reports, setReports] = useState<ReportListItem[]>([]);
  const [selectedReport, setSelectedReport] = useState<SavedReport | null>(null);
  const [savedReportId, setSavedReportId] = useState<string | null>(null);

  useEffect(() => {
    if (user.role === "viewer") return;
    let active = true;
    listAgentReports().then(
      (items) => { if (active) setReports(items); },
      () => { if (active) setReportError("报告列表暂不可用。"); },
    );
    return () => { active = false; };
  }, [user.role]);

  function chooseTemplate(item: typeof templates[number]) {
    setQuestion(item.question);
    setTemplateId(item.id);
    setAskResult(null);
    setSelectedReport(null);
    setSavedReportId(null);
    setError(null);
  }

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!question.trim() || asking) return;
    setAsking(true);
    setError(null);
    setAskResult(null);
    setSelectedReport(null);
    setSavedReportId(null);
    try {
      setAskResult(await askAgent(question.trim(), templateId, user.csrf_token));
    } catch (cause) { setError(requestError(cause)); }
    finally { setAsking(false); }
  }

  async function saveReport() {
    if (!askResult || saving) return;
    setSaving(true);
    setReportError(null);
    try {
      const saved = await saveAgentReport(askResult.answer_id, user.csrf_token);
      setSavedReportId(saved.report_id);
      setReports((current) => [{ report_id: saved.report_id, question, created_at: new Date().toISOString() }, ...current]);
    } catch (cause) { setReportError(requestError(cause)); }
    finally { setSaving(false); }
  }

  async function openReport(id: string) {
    setReportError(null);
    try { setSelectedReport(await getAgentReport(id)); }
    catch (cause) { setReportError(requestError(cause)); }
  }

  const displayedAnswer = selectedReport ? selectedReport.answer : askResult?.answer ?? null;
  return <section className="agent-page" aria-labelledby="agent-title">
    <header className="agent-hero">
      <div><span className="eyebrow">G4 / GROUNDED ANALYSIS</span>
        <h2 id="agent-title">问一个问题，<br />拿得出依据。</h2>
        <p>知识引用和已发布聚合指标分开呈现。模型负责选择工具和组织观点，数字、窗口与来源由后端证据卡提供。</p></div>
      <div className="agent-hero-seal" aria-hidden="true"><span>READ</span><i /><strong>证据优先</strong><small>01 / CONTROLLED AGENT</small></div>
    </header>
    <div className="agent-grid">
      <aside className="agent-controls">
        {user.role === "viewer" ? <div className="agent-access"><h3>分析权限</h3><p>只读用户不能发起 AI 分析。你仍可以浏览已授权的业务页面和知识库。</p></div> : <>
          <section className="agent-compose" aria-label="提问台">
            <span className="eyebrow">ASK / 提问</span><h3>从业务问题进入</h3>
            <div className="agent-templates" role="group" aria-label="建议提问">{templates.map((item) =>
              <button key={item.id} type="button" className={templateId === item.id ? "agent-template agent-template--active" : "agent-template"}
                onClick={() => chooseTemplate(item)}>{item.label}<span aria-hidden="true">↗</span></button>)}</div>
            <form onSubmit={submit}><label htmlFor="agent-question">分析问题</label>
              <textarea id="agent-question" aria-label="分析问题" value={question} maxLength={500} rows={5}
                onChange={(event) => { setQuestion(event.target.value); setTemplateId(null); setAskResult(null); setSelectedReport(null); }}
                placeholder="例如：Olist 订单趋势和支付结构有哪些值得关注的变化？" />
              <div className="agent-compose-foot"><small>选择模板不会自动请求模型。最多 500 字。</small>
                <button type="submit" disabled={asking || !question.trim()}>{asking ? "正在检索与分析…" : "开始分析"}</button></div>
            </form>
            {error && <p className="agent-error" role="alert">{error}</p>}
          </section>
          <section className="agent-reports" aria-label="报告">
            <div className="agent-panel-heading"><span className="eyebrow">ARCHIVE / 报告</span><h3>我的分析报告</h3></div>
            {askResult && !selectedReport && !savedReportId && <button type="button" className="agent-save" onClick={saveReport} disabled={saving}>{saving ? "正在保存…" : "保存报告"}</button>}
            {savedReportId && <p className="agent-report-saved" role="status">报告已保存，点击下方记录可重新核验引用。</p>}
            {reportError && <p className="agent-error" role="alert">{reportError}</p>}
            {reports.length === 0 ? <p className="agent-empty-inline">还没有保存的报告。</p> :
              <ul>{reports.map((item) => <li key={item.report_id}><button type="button" onClick={() => openReport(item.report_id)}>
                <span>{item.question}</span><small>查看报告 →</small></button></li>)}</ul>}
          </section>
        </>}
      </aside>
      <div className="agent-output">
        {selectedReport && <div className="agent-report-head"><span>已保存报告</span><strong>{selectedReport.question}</strong><small>{selectedReport.status === "historical" ? "历史快照" : selectedReport.status === "invalidated" ? "引用已失效" : selectedReport.status === "unverified" ? "当前发布待核验" : "当前报告"}</small></div>}
        <AnswerView answer={displayedAnswer} reportStatus={selectedReport?.status} />
      </div>
    </div>
    <p className="agent-page-note">Olist 订单与 REES46 行为是独立来源，不进行跨平台用户关联或因果推断。历史回放不等于企业生产实时数据。</p>
  </section>;
}
