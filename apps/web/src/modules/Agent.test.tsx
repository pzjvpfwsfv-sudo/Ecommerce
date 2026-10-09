import { HttpResponse, http } from "msw";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, expect, it } from "vitest";

import { Agent } from "./Agent";
import { server } from "../test/handlers";

const analyst = { id: 7, username: "analyst", role: "analyst", csrf_token: "csrf-test" } as const;
const viewer = { ...analyst, role: "viewer" } as const;
const answerId = "11111111-1111-1111-1111-111111111111";
const reportId = "22222222-2222-2222-2222-222222222222";
const answer = {
  status: "evidence_only", insights: [], model_name: null, retrieval_version: "g4-a-hybrid-v1",
  fallback_reason: "model_off", fallback_summary: "未调用模型。以下仅展示已授权证据。",
  trace: [{ name: "search_knowledge", status: "ok", elapsed_ms: 12, evidence_ids: ["ev-knowledge"] },
    { name: "get_published_order_metrics", status: "ok", elapsed_ms: 30, evidence_ids: ["ev-orders"] }],
  evidence: [
    { evidence_id: "ev-knowledge", kind: "knowledge", text: "订单数只来自已发布指标。",
      meta: { document_id: "doc-1", version_id: "version-1", chunk_id: "chunk-1", section: "订单数", source_label: "项目文档", source_ref: "configs/metrics/orders-v1.json" },
      rows: [], target: "/knowledge/documents/doc-1/versions/version-1#chunk-chunk-1" },
    { evidence_id: "ev-orders", kind: "orders", text: null,
      meta: { dataset_id: "olist-brazilian-ecommerce", metric_version: "orders-v1", metric_run_id: "orders-v1-real-run", view: "overview",
        requested_window: "full", window_start: "2016-09-04", window_end: "2018-10-17", calculated_at: "2026-10-09T00:00:00Z",
        warnings: ["历史订单快照，不是实时交易"], source_order_count: 99441 },
      rows: [{ window_type: "FULL", order_count: 99441, payment_value_sum: "16008872.12" }], target: "/orders" },
  ],
};

function view(user: typeof analyst | typeof viewer = analyst) {
  return render(<MemoryRouter><Agent user={user} /></MemoryRouter>);
}

beforeEach(() => {
  server.use(http.get("/api/v1/agent/reports", () => HttpResponse.json([])));
});

it("fills five templates without calling a model and clears template identity after edits", async () => {
  let payload: Record<string, unknown> | null = null;
  let csrf = "";
  server.use(http.post("/api/v1/agent/ask", async ({ request }) => {
    payload = await request.json() as Record<string, unknown>;
    csrf = request.headers.get("X-CSRF-Token") ?? "";
    return HttpResponse.json({ answer_id: answerId, answer });
  }));
  view();
  const templates = screen.getByRole("group", { name: "建议提问" });
  expect(within(templates).getAllByRole("button")).toHaveLength(5);
  await userEvent.click(within(templates).getByRole("button", { name: /订单趋势与支付/ }));
  const question = screen.getByRole("textbox", { name: "分析问题" });
  expect(question).toHaveValue("Olist 订单趋势和支付结构有哪些值得关注的变化？");
  expect(payload).toBeNull();
  await userEvent.type(question, "请核对口径");
  await userEvent.click(screen.getByRole("button", { name: "开始分析" }));
  await waitFor(() => expect(payload).not.toBeNull());
  expect(payload).toMatchObject({ template_id: null });
  expect(csrf).toBe("csrf-test");
});

it("shows backend evidence, provenance, steps and downgrade without fabricated metrics", async () => {
  let payload: Record<string, unknown> | null = null;
  server.use(http.post("/api/v1/agent/ask", async ({ request }) => {
    payload = await request.json() as Record<string, unknown>;
    return HttpResponse.json({ answer_id: answerId, answer });
  }));
  view();
  await userEvent.click(screen.getByRole("button", { name: /订单趋势与支付/ }));
  await userEvent.click(screen.getByRole("button", { name: "开始分析" }));
  expect(await screen.findByText(/未调用模型。以下仅展示已授权证据/)).toBeVisible();
  expect(payload).toMatchObject({ template_id: "orders_payments" });
  expect(screen.getByText("仅展示证据")).toBeVisible();
  expect(screen.getByText("orders-v1-real-run")).toBeVisible();
  expect(screen.getByText("99,441")).toBeVisible();
  expect(screen.getByText("历史订单快照，不是实时交易")).toBeVisible();
  expect(screen.getByText("订单数只来自已发布指标。")).toBeVisible();
  expect(screen.getByRole("link", { name: /查看原文/ })).toHaveAttribute("href", "/knowledge/documents/doc-1/versions/version-1#chunk-chunk-1");
  expect(screen.getByRole("link", { name: /前往订单与支付/ })).toHaveAttribute("href", "/orders?window=full");
  expect(screen.getByRole("list", { name: "工具步骤" })).toHaveTextContent("search_knowledge");
  expect(screen.queryByText(/隐藏思考/)).not.toBeInTheDocument();
});

it("distinguishes refusal, permission failure, timeout and a withdrawn report", async () => {
  let mode: "refused" | "forbidden" | "timeout" = "refused";
  server.use(
    http.post("/api/v1/agent/ask", () => mode === "forbidden" ? new HttpResponse(null, { status: 403 })
      : mode === "timeout" ? new HttpResponse(null, { status: 504 })
        : HttpResponse.json({ answer_id: answerId, answer: { ...answer, status: "refused", evidence: [], trace: [], fallback_summary: "没有可用的授权证据。" } })),
    http.post("/api/v1/agent/reports", () => HttpResponse.json({ report_id: reportId }, { status: 201 })),
    http.get(`/api/v1/agent/reports/${reportId}`, () => HttpResponse.json({
      report_id: reportId, question: "订单口径？", status: "invalidated", answer: null,
    })),
  );
  view();
  const question = screen.getByRole("textbox", { name: "分析问题" });
  await userEvent.type(question, "订单口径？");
  await userEvent.click(screen.getByRole("button", { name: "开始分析" }));
  expect(await screen.findByText("没有可用的授权证据。")).toBeVisible();
  expect(screen.getByText("证据不足，拒绝作答")).toBeVisible();
  mode = "forbidden";
  await userEvent.click(screen.getByRole("button", { name: "开始分析" }));
  expect(await screen.findByRole("alert")).toHaveTextContent(/没有权限/);
  mode = "timeout";
  await userEvent.click(screen.getByRole("button", { name: "开始分析" }));
  expect(await screen.findByRole("alert")).toHaveTextContent(/超时/);

  mode = "refused";
  await userEvent.click(screen.getByRole("button", { name: "开始分析" }));
  expect(await screen.findByRole("button", { name: "保存报告" })).toBeVisible();
  await userEvent.click(screen.getByRole("button", { name: "保存报告" }));
  await userEvent.click(await screen.findByRole("button", { name: /订单口径？/ }));
  expect(await screen.findByRole("heading", { name: "引用已失效" })).toBeVisible();
  expect(screen.queryByText("没有可用的授权证据。")).not.toBeInTheDocument();
});

it("keeps viewer out of agent requests", () => {
  view(viewer);
  expect(screen.getByText(/只读用户不能发起 AI 分析/)).toBeVisible();
  expect(screen.queryByRole("button", { name: "开始分析" })).not.toBeInTheDocument();
});
