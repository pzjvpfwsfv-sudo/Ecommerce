import { jsonRequest } from "./http";

export type AgentStatus = "answered" | "evidence_only" | "refused";
export type ReportStatus = "current" | "historical" | "invalidated" | "unverified";
export type EvidenceKind = "knowledge" | "behavior" | "orders";
export type MetricValue = string | number | boolean | null;

export interface AgentEvidence {
  evidence_id: string;
  kind: EvidenceKind;
  meta: Record<string, MetricValue | string[]>;
  text: string | null;
  rows: Record<string, MetricValue>[];
  target: string;
}

export interface AgentAnswer {
  status: AgentStatus;
  insights: { text: string; evidence_ids: string[] }[];
  evidence: AgentEvidence[];
  trace: { name: string; status: "ok" | "error"; elapsed_ms: number; evidence_ids: string[] }[];
  model_name: string | null;
  retrieval_version: string;
  fallback_reason: string | null;
  fallback_summary: string | null;
}

export interface AskResult {
  answer_id: string;
  answer: AgentAnswer;
}

export interface ReportListItem {
  report_id: string;
  question: string;
  created_at: string;
}

export interface SavedReport extends ReportListItem {
  answer_id: string;
  status: ReportStatus;
  answer: AgentAnswer | null;
}

const base = "/api/v1/agent";

export const askAgent = (question: string, templateId: string | null, csrf: string) =>
  jsonRequest<AskResult>(`${base}/ask`, {
    method: "POST", headers: { "X-CSRF-Token": csrf },
    body: JSON.stringify({ question, template_id: templateId }),
  });

export const listAgentReports = () => jsonRequest<ReportListItem[]>(`${base}/reports`);

export const getAgentReport = (id: string) =>
  jsonRequest<SavedReport>(`${base}/reports/${encodeURIComponent(id)}`);

export const saveAgentReport = (answerId: string, csrf: string) =>
  jsonRequest<{ report_id: string }>(`${base}/reports`, {
    method: "POST", headers: { "X-CSRF-Token": csrf },
    body: JSON.stringify({ answer_id: answerId }),
  });
