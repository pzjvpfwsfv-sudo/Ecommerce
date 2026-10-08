import { useEffect, useState, type FormEvent } from "react";
import { Link, useLocation, useNavigate, useParams } from "react-router-dom";

import type { AuthUser } from "../lib/auth";
import { ApiError } from "../lib/http";
import {
  deleteKnowledge, getDocumentPreview, importMetricKnowledge, listDocumentVersions,
  listKnowledgeDocuments, publishKnowledge, searchKnowledge, uploadKnowledge,
  withdrawKnowledge,
  type DocumentPreview, type DocumentVersion, type KnowledgeDocument, type SearchResult,
} from "../lib/knowledge";
import "../styles/knowledge.css";

function message(cause: unknown) {
  if (cause instanceof ApiError && cause.status === 422) return "资料格式或来源不符合要求，请检查后重试。";
  if (cause instanceof ApiError && cause.status === 403) return "当前账户没有执行此操作的权限。";
  return "知识库暂不可用，请稍后重试。";
}

export function Knowledge({ user }: { user: AuthUser }) {
  const { documentId, versionId } = useParams();
  const location = useLocation();
  const navigate = useNavigate();
  const [documents, setDocuments] = useState<KnowledgeDocument[]>([]);
  const [versions, setVersions] = useState<DocumentVersion[]>([]);
  const [preview, setPreview] = useState<DocumentPreview | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [result, setResult] = useState<SearchResult | null>(null);
  const [query, setQuery] = useState("");
  const [busy, setBusy] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [draft, setDraft] = useState<DocumentVersion | null>(null);
  const [revisionTarget, setRevisionTarget] = useState<KnowledgeDocument | null>(null);
  const [sourceType, setSourceType] = useState<"external" | "project_doc">("external");
  const [refresh, setRefresh] = useState(0);
  const activeId = documentId ?? selectedId;
  const activeDocument = documents.find((item) => item.id === activeId);

  useEffect(() => {
    let active = true;
    listKnowledgeDocuments().then(
      (items) => { if (active) { setDocuments(items); setLoading(false); setError(null); } },
      (cause) => { if (active) { setLoading(false); setError(message(cause)); } },
    );
    return () => { active = false; };
  }, [refresh]);

  useEffect(() => {
    if (!documentId || !versionId) return;
    let active = true;
    Promise.all([listDocumentVersions(documentId), getDocumentPreview(documentId, versionId)]).then(
      ([items, body]) => {
        if (active) { setSelectedId(documentId); setVersions(items); setPreview(body); setError(null); }
      },
      (cause) => { if (active) { setPreview(null); setError(message(cause)); } },
    );
    return () => { active = false; };
  }, [documentId, versionId, refresh]);

  useEffect(() => {
    if (preview && location.hash.startsWith("#chunk-")) {
      document.getElementById(location.hash.slice(1))?.scrollIntoView?.({ block: "center" });
    }
  }, [preview, location.hash]);

  async function openDocument(item: KnowledgeDocument) {
    setError(null);
    setSelectedId(item.id);
    try {
      const items = await listDocumentVersions(item.id);
      setVersions(items);
      const chosen = item.published_version_id ?? items[0]?.id;
      if (chosen) navigate(`/knowledge/documents/${item.id}/versions/${chosen}`);
      else { navigate("/knowledge"); setPreview(null); }
    } catch (cause) { setError(message(cause)); }
  }

  async function submitSearch(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!query.trim()) return;
    setBusy(true);
    setError(null);
    try { setResult(await searchKnowledge(query.trim())); }
    catch (cause) { setResult(null); setError(message(cause)); }
    finally { setBusy(false); }
  }

  async function submitUpload(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    const file = (event.currentTarget.elements.namedItem("file") as HTMLInputElement).files?.[0];
    if (!file || file.size === 0) {
      setError("请选择非空的 MD、TXT 或文本 PDF 文件。");
      return;
    }
    form.set("file", file, file.name);
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      const created = await uploadKnowledge(form, user.csrf_token);
      setDraft(created);
      setRevisionTarget(null);
      setSourceType("external");
      setNotice("草稿已创建。确认内容后再发布，旧版在此之前保持可见。");
      setRefresh((value) => value + 1);
    } catch (cause) { setError(message(cause)); }
    finally { setBusy(false); }
  }

  async function importMetrics(domain: "orders" | "behavior") {
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      const created = await importMetricKnowledge(domain, user.csrf_token);
      setDraft(created);
      setNotice("草稿已创建。确认内容后再发布，旧版在此之前保持可见。");
      setRefresh((value) => value + 1);
    } catch (cause) { setError(message(cause)); }
    finally { setBusy(false); }
  }

  async function publish(version: DocumentVersion) {
    setBusy(true);
    setError(null);
    try {
      await publishKnowledge(version.document_id, version.id, user.csrf_token);
      setDraft(null);
      setNotice("版本已发布，可被授权账户检索。");
      setRefresh((value) => value + 1);
      navigate(`/knowledge/documents/${version.document_id}/versions/${version.id}`);
    } catch (cause) { setError(message(cause)); }
    finally { setBusy(false); }
  }

  async function remove(kind: "withdraw" | "delete") {
    if (!activeId || !window.confirm(kind === "delete" ? "永久删除该资料及所有原文和向量？" : "撤回该资料，停止检索和引用？")) return;
    setBusy(true);
    setError(null);
    try {
      if (kind === "delete") await deleteKnowledge(activeId, user.csrf_token);
      else await withdrawKnowledge(activeId, user.csrf_token);
      setSelectedId(null);
      setPreview(null);
      setVersions([]);
      setNotice(kind === "delete" ? "资料已删除，旧引用将显示失效。" : "资料已撤回，旧引用将显示失效。");
      navigate("/knowledge");
      setRefresh((value) => value + 1);
    } catch (cause) { setError(message(cause)); }
    finally { setBusy(false); }
  }

  return <section className="knowledge-page" aria-labelledby="knowledge-title">
    <header className="knowledge-hero">
      <div>
        <span className="eyebrow">G4 / RETRIEVAL LIBRARY</span>
        <h2 id="knowledge-title">让答案回到原文。</h2>
        <p>项目指标定义与运行手册独立于交易明细存储。仅检索已发布、当前账户可见的版本。</p>
      </div>
      <div className="knowledge-hero-mark" aria-hidden="true"><span>01</span><strong>源</strong><i /><span>02</span><strong>证</strong></div>
    </header>

    {error && <p className="notice notice--error" role="alert">{error}</p>}
    {notice && <p className="notice" role="status">{notice}</p>}

    <form className="knowledge-search" onSubmit={submitSearch} role="search">
      <label htmlFor="knowledge-query">检索项目知识</label>
      <div className="knowledge-search-row">
        <input id="knowledge-query" type="search" aria-label="检索知识库" value={query}
          onChange={(event) => setQuery(event.target.value)} placeholder="例如：metric_run_id 的口径如何追溯？" maxLength={500} />
        <button type="submit" disabled={busy || !query.trim()}>检索</button>
      </div>
      <small>只搜索知识资料，不把 Olist 与 REES46 明细混作同一用户。</small>
    </form>

    {result && <section className="knowledge-results" aria-label="检索结果">
      <div className="knowledge-section-heading">
        <div><span className="eyebrow">EVIDENCE / 证据</span><h3>检索命中</h3></div>
        <span className="knowledge-mode">{result.mode === "hybrid" ? "融合检索" : "仅关键词检索"} · {result.hits.length} 条</span>
      </div>
      {result.hits.length === 0 ? <p className="knowledge-empty">没有找到可见的相关资料。请换个关键词，或检查资料是否已发布。</p> :
        <ol className="knowledge-hit-list">{result.hits.map((hit, index) => <li key={hit.chunk_id}>
          <span className="knowledge-hit-index">{String(index + 1).padStart(2, "0")}</span>
          <div><div className="knowledge-hit-meta"><strong>{hit.section}</strong><span>{hit.source_label}{hit.page ? ` · 第 ${hit.page} 页` : ""}</span></div>
            <p>{hit.text}</p><small>{hit.source_ref}</small></div>
          <Link to={hit.locator}>查看原文定位 →</Link>
        </li>)}</ol>}
    </section>}

    <div className="knowledge-layout">
      <section className="knowledge-catalog" aria-label="资料目录">
        <div className="knowledge-section-heading"><div><span className="eyebrow">SOURCES / 资料</span><h3>知识目录</h3></div><span>{documents.length} 份</span></div>
        {loading ? <p className="knowledge-empty" role="status">正在读取资料目录…</p> : documents.length === 0 ?
          <p className="knowledge-empty">还没有已发布资料。管理员可导入真实项目资料并发布。</p> :
          <ul className="knowledge-doc-list">{documents.map((item) => <li key={item.id}>
            <button type="button" className={activeId === item.id ? "knowledge-doc knowledge-doc--active" : "knowledge-doc"} onClick={() => openDocument(item)}>
              <span className="knowledge-doc-type">{item.source_type === "project_doc" ? "项目文档" : "外部来源"} / {item.category}</span>
              <strong>{item.title}</strong><small>{item.source_ref}</small>
              <span className="knowledge-doc-status">{item.published_version_id ? "已发布" : "待发布 / 已撤回"}</span>
            </button>
          </li>)}</ul>}
      </section>

      <section className="knowledge-reader" aria-label="原文与版本">
        <div className="knowledge-section-heading"><div><span className="eyebrow">SOURCE TRACE / 溯源</span><h3>{activeDocument?.title ?? "原文与版本"}</h3></div></div>
        {!activeId ? <p className="knowledge-empty">选择左侧资料，或从检索结果进入可定位的原文。</p> : <>
          <div className="knowledge-version-list" aria-label="版本时间线">{versions.map((item) => <div key={item.id} className="knowledge-version">
            <Link to={`/knowledge/documents/${item.document_id}/versions/${item.id}`} aria-current={versionId === item.id ? "page" : undefined}>v{item.version_no} · {item.status === "published" ? "已发布" : item.status === "draft" ? "草稿" : "已撤回"}</Link>
            {user.role === "admin" && item.status === "draft" && <button type="button" disabled={busy} onClick={() => publish(item)}>发布此版</button>}
          </div>)}</div>
          {preview ? <div className="knowledge-text">
            <span className="knowledge-text-label">EXTRACTED TEXT / 原文文本</span>
            <pre>{preview.extracted_text}</pre>
            <h4>分块定位</h4>
            {preview.chunks.map((item) => <article id={`chunk-${item.id}`} key={item.id} className="knowledge-chunk">
              <small>{item.section}{item.page ? ` · 第 ${item.page} 页` : ""}</small><p>{item.text}</p>
            </article>)}
          </div> : <p className="knowledge-empty">该版本不可预览，或尚未选择版本。</p>}
          {user.role === "admin" && <div className="knowledge-danger">
            <button type="button" disabled={busy} onClick={() => remove("withdraw")}>撤回资料</button>
            <button type="button" disabled={busy} onClick={() => remove("delete")}>永久删除</button>
          </div>}
        </>}
      </section>
    </div>

    {user.role === "admin" && <section className="knowledge-admin" aria-label="资料管理">
      <div><span className="eyebrow">ADMIN / CONTENT CONTROL</span><h3>导入真实资料</h3><p>文件先成为草稿，核对来源与内容后再发布。</p>
        <div className="knowledge-import"><button type="button" disabled={busy} onClick={() => importMetrics("orders")}>导入指标口径 · 订单</button><button type="button" disabled={busy} onClick={() => importMetrics("behavior")}>导入指标口径 · 行为</button></div>
        {activeDocument && (activeDocument.source_type === "external" || /\.(md|txt|pdf)$/i.test(activeDocument.source_ref)) && <button type="button" className="knowledge-revise" onClick={() => {
          setRevisionTarget(activeDocument);
          setSourceType(activeDocument.source_type);
          setDraft(null);
        }}>为此资料上传修订版</button>}
        {revisionTarget && <button type="button" className="knowledge-revise" onClick={() => {
          setRevisionTarget(null);
          setSourceType("external");
        }}>取消修订，改为新资料</button>}
      </div>
      <form key={revisionTarget?.id ?? "new"} onSubmit={submitUpload} className="knowledge-upload">
        {revisionTarget && <input type="hidden" name="document_id" value={revisionTarget.id} />}
        <label>资料标题<input name="title" required maxLength={160} defaultValue={revisionTarget?.title ?? ""} readOnly={!!revisionTarget} /></label>
        <label>分类<input name="category" required defaultValue={revisionTarget?.category ?? "项目资料"} readOnly={!!revisionTarget} /></label>
        <label>来源类型<select name="source_type" value={sourceType} disabled={!!revisionTarget} onChange={(event) => setSourceType(event.target.value as "external" | "project_doc")}><option value="external">外部 HTTPS 来源</option><option value="project_doc">仓库白名单原文</option></select></label>
        {revisionTarget && <input type="hidden" name="source_type" value={revisionTarget.source_type} />}
        <label>{sourceType === "external" ? "来源链接" : "项目相对路径"}<input name="source_ref" required defaultValue={revisionTarget?.source_ref ?? ""} readOnly={!!revisionTarget} placeholder={sourceType === "external" ? "https://..." : "docs/graduation/..."} /></label>
        <label>可见角色<select name="visibility_roles" defaultValue={revisionTarget?.visibility_roles.join(",") ?? "admin,analyst,viewer"} disabled={!!revisionTarget}><option value="admin,analyst,viewer">管理员、分析员、只读用户</option><option value="admin,analyst">管理员、分析员</option><option value="admin">仅管理员</option></select></label>
        {revisionTarget && <input type="hidden" name="visibility_roles" value={revisionTarget.visibility_roles.join(",")} />}
        <label>资料文件<input name="file" type="file" accept=".md,.txt,.pdf" /></label>
        <button type="submit" disabled={busy}>{revisionTarget ? "上传修订草稿" : "上传草稿"}</button>
      </form>
      {draft && <div className="knowledge-draft"><strong>草稿已创建 · v{draft.version_no}</strong><span>请确认来源和正文，再开放给其他角色。</span><button type="button" disabled={busy} onClick={() => publish(draft)}>发布草稿</button></div>}
    </section>}
  </section>;
}
