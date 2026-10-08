import { HttpResponse, http } from "msw";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, beforeEach, expect, it, vi } from "vitest";

import { Knowledge } from "./Knowledge";
import { server } from "../test/handlers";

const id = "11111111-1111-1111-1111-111111111111";
const version = "22222222-2222-2222-2222-222222222222";
const chunk = "33333333-3333-3333-3333-333333333333";
const admin = { id: 1, username: "owner", role: "admin", csrf_token: "csrf-test" } as const;
const viewer = { ...admin, role: "viewer" } as const;
const knowledgeDoc = {
  id, title: "真实订单口径", category: "指标定义", source_type: "project_doc",
  source_ref: "configs/metrics/orders-v1.json", visibility_roles: ["admin", "analyst", "viewer"],
  published_version_id: version,
};

function view(user: typeof admin | typeof viewer = viewer) {
  return render(<MemoryRouter initialEntries={["/knowledge"]}><Routes>
    <Route path="/knowledge" element={<Knowledge user={user} />} />
    <Route path="/knowledge/documents/:documentId/versions/:versionId" element={<Knowledge user={user} />} />
  </Routes></MemoryRouter>);
}

beforeEach(() => {
  server.use(
    http.get("/api/v1/knowledge/documents", () => HttpResponse.json([knowledgeDoc])),
    http.get(`/api/v1/knowledge/documents/${id}/versions`, () => HttpResponse.json([
      { id: version, document_id: id, version_no: 1, status: "published", created_at: "2026-10-09T00:00:00Z", published_at: "2026-10-09T00:00:00Z" },
    ])),
    http.get(`/api/v1/knowledge/documents/${id}/versions/${version}`, () => HttpResponse.json({
      document_id: id, version_id: version, extracted_text: "原始 <script>alert(1)</script>",
      chunks: [{ id: chunk, ordinal: 0, section: "口径", page: null, text: "metric_run_id 口径" }],
    })),
    http.get("/api/v1/knowledge/search", () => HttpResponse.json({
      hits: [{ chunk_id: chunk, document_id: id, version_id: version, section: "口径", page: null,
        text: "metric_run_id 口径", source_label: "项目文档", source_ref: knowledgeDoc.source_ref,
        keyword_rank: 1, vector_rank: 1, score: .032, locator: `/knowledge/documents/${id}/versions/${version}#chunk-${chunk}` }],
      mode: "hybrid", elapsed_ms: 24,
    })),
  );
});

afterEach(() => vi.restoreAllMocks());

it("shows real catalog and navigable evidence without executing source text", async () => {
  view();
  expect(await screen.findByText("真实订单口径")).toBeVisible();
  expect(screen.queryByRole("button", { name: "导入指标口径" })).not.toBeInTheDocument();
  await userEvent.type(screen.getByRole("searchbox", { name: "检索知识库" }), "metric_run_id");
  await userEvent.click(screen.getByRole("button", { name: "检索" }));
  expect(await screen.findByText(/融合检索/)).toBeVisible();
  await userEvent.click(screen.getByRole("link", { name: /查看原文定位/ }));
  expect(await screen.findByText("原始 <script>alert(1)</script>")).toBeVisible();
  expect(document.querySelector("script")).toBeNull();
  expect(screen.getAllByText("metric_run_id 口径").length).toBeGreaterThan(0);
});

it("sends multipart upload without JSON content type and publishes with CSRF", async () => {
  let multipart = false;
  let csrf = "";
  const originalFetch = globalThis.fetch;
  vi.spyOn(globalThis, "fetch").mockImplementation((input, init) => {
    if (input === "/api/v1/knowledge/documents" && init?.method === "POST") {
      const body = init?.body as FormData;
      multipart = body instanceof FormData && !new Headers(init?.headers).has("Content-Type");
      expect(body.get("title")).toBe("答辩资料");
      return Promise.resolve(Response.json({ id: version, document_id: id, version_no: 2, status: "draft" }, { status: 201 }));
    }
    if (input === `/api/v1/knowledge/documents/${id}/versions/${version}/publish`) {
      csrf = new Headers(init?.headers).get("X-CSRF-Token") ?? "";
      return Promise.resolve(Response.json({ id: version, document_id: id, version_no: 2, status: "published" }));
    }
    return originalFetch(input, init);
  });
  view(admin);
  await screen.findByText("真实订单口径");
  await userEvent.type(screen.getByLabelText("资料标题"), "答辩资料");
  await userEvent.type(screen.getByLabelText("来源链接"), "https://example.org/report");
  await userEvent.upload(screen.getByLabelText("资料文件"), new File(["# 口径\n真实正文"], "guide.md", { type: "text/markdown" }));
  await userEvent.click(screen.getByRole("button", { name: "上传草稿" }));
  expect(await screen.findByRole("status")).toHaveTextContent(/草稿已创建/);
  expect(multipart).toBe(true);
  await userEvent.click(screen.getByRole("button", { name: "发布草稿" }));
  await waitFor(() => expect(csrf).toBe("csrf-test"));
});

it("shows empty and service error states rather than invented results", async () => {
  server.use(http.get("/api/v1/knowledge/documents", () => HttpResponse.json([])));
  const result = view();
  expect(await screen.findByText(/还没有已发布资料/)).toBeVisible();
  result.unmount();
  server.use(http.get("/api/v1/knowledge/documents", () => new HttpResponse(null, { status: 503 })));
  view();
  expect(await screen.findByRole("alert")).toHaveTextContent(/知识库暂不可用/);
});

it("prefills source identity when an admin prepares a revised version", async () => {
  server.use(http.get("/api/v1/knowledge/documents", () => HttpResponse.json([{
    ...knowledgeDoc, source_type: "external", source_ref: "https://example.org/guide",
  }])));
  view(admin);
  await userEvent.click(await screen.findByRole("button", { name: /真实订单口径/ }));
  await userEvent.click(await screen.findByRole("button", { name: "为此资料上传修订版" }));
  expect(screen.getByLabelText("资料标题")).toHaveValue("真实订单口径");
  expect(screen.getByLabelText("来源链接")).toHaveValue("https://example.org/guide");
  expect(document.querySelector('input[name="document_id"]')).toHaveValue(id);
});
