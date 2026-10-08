import { jsonRequest } from "./http";
import type { Role } from "./auth";

export interface KnowledgeDocument {
  id: string;
  title: string;
  category: string;
  source_type: "project_doc" | "external";
  source_ref: string;
  visibility_roles: Role[];
  published_version_id: string | null;
}

export interface DocumentVersion {
  id: string;
  document_id: string;
  version_no: number;
  status: "draft" | "published" | "withdrawn";
  created_at: string;
  published_at: string | null;
}

export interface DocumentPreview {
  document_id: string;
  version_id: string;
  extracted_text: string;
  chunks: { id: string; ordinal: number; section: string; page: number | null; text: string }[];
}

export interface KnowledgeHit {
  chunk_id: string;
  document_id: string;
  version_id: string;
  section: string;
  page: number | null;
  text: string;
  source_label: string;
  source_ref: string;
  keyword_rank: number | null;
  vector_rank: number | null;
  score: number;
  locator: string;
}

export interface SearchResult {
  hits: KnowledgeHit[];
  mode: "hybrid" | "keyword_only";
  elapsed_ms: number;
}

const base = "/api/v1/knowledge";
export const listKnowledgeDocuments = () => jsonRequest<KnowledgeDocument[]>(`${base}/documents`);
export const listDocumentVersions = (id: string) => jsonRequest<DocumentVersion[]>(`${base}/documents/${id}/versions`);
export const getDocumentPreview = (id: string, version: string) =>
  jsonRequest<DocumentPreview>(`${base}/documents/${id}/versions/${version}`);
export const searchKnowledge = (query: string) =>
  jsonRequest<SearchResult>(`${base}/search?q=${encodeURIComponent(query)}`);
export const uploadKnowledge = (form: FormData, csrf: string) =>
  jsonRequest<DocumentVersion>(`${base}/documents`, {
    method: "POST", headers: { "X-CSRF-Token": csrf }, body: form,
  });
export const importMetricKnowledge = (domain: "orders" | "behavior", csrf: string) =>
  jsonRequest<DocumentVersion>(`${base}/import-metrics`, {
    method: "POST", headers: { "X-CSRF-Token": csrf }, body: JSON.stringify({ domain }),
  });
export const publishKnowledge = (id: string, version: string, csrf: string) =>
  jsonRequest<DocumentVersion>(`${base}/documents/${id}/versions/${version}/publish`, {
    method: "POST", headers: { "X-CSRF-Token": csrf },
  });
export const withdrawKnowledge = (id: string, csrf: string) =>
  jsonRequest<{ status: string }>(`${base}/documents/${id}/withdraw`, {
    method: "POST", headers: { "X-CSRF-Token": csrf },
  });
export const deleteKnowledge = (id: string, csrf: string) =>
  jsonRequest<{ status: string }>(`${base}/documents/${id}`, {
    method: "DELETE", headers: { "X-CSRF-Token": csrf },
  });
