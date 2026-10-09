import { expect, test } from "@playwright/test";

const answerId = "11111111-1111-1111-1111-111111111111";
const reportId = "22222222-2222-2222-2222-222222222222";
const documentId = "33333333-3333-3333-3333-333333333333";
const versionId = "44444444-4444-4444-4444-444444444444";
const chunkId = "55555555-5555-5555-5555-555555555555";
const locator = `/knowledge/documents/${documentId}/versions/${versionId}#chunk-${chunkId}`;

const answer = {
  status: "evidence_only", insights: [], model_name: null, retrieval_version: "g4-a-hybrid-v1",
  fallback_reason: "model_off", fallback_summary: "模型未启用，仅展示可核对的证据。",
  trace: [
    { name: "search_knowledge", status: "ok", elapsed_ms: 12, evidence_ids: ["ev-knowledge"] },
    { name: "get_published_order_metrics", status: "ok", elapsed_ms: 30, evidence_ids: ["ev-orders"] },
  ],
  evidence: [
    { evidence_id: "ev-knowledge", kind: "knowledge", text: "订单数来自已发布指标。",
      meta: { section: "订单数", source_label: "项目文档", source_ref: "configs/metrics/orders-v1.json" },
      rows: [], target: locator },
    { evidence_id: "ev-orders", kind: "orders", text: null,
      meta: { dataset_id: "olist-brazilian-ecommerce", metric_version: "orders-v1",
        metric_run_id: "orders-v1-real-run", view: "overview", requested_window: "full",
        window_start: "2016-09-04", window_end: "2018-10-17", calculated_at: "2026-10-09T00:00:00Z",
        warnings: ["历史订单快照，不是实时交易"] },
      rows: [{ window_type: "FULL", order_count: 99441 }], target: "/orders" },
  ],
};

for (const [label, width, height] of [["desktop", 1440, 900], ["mobile", 390, 844]] as const) {
  test.describe(label, () => {
    test.use({ viewport: { width, height } });

    test("asks, inspects evidence, and hides a withdrawn report without overflow", async ({ page }, testInfo) => {
      await page.emulateMedia({ reducedMotion: "reduce" });
      let asks = 0;
      await page.route("**/api/v1/auth/me", (route) => route.fulfill({ json: {
        id: 7, username: "analyst", role: "analyst", csrf_token: "csrf-test",
      } }));
      await page.route("**/api/v1/agent/reports", (route) => route.request().method() === "POST"
        ? route.fulfill({ status: 201, json: { report_id: reportId } })
        : route.fulfill({ json: [] }));
      await page.route("**/api/v1/agent/ask", async (route) => {
        asks += 1;
        expect(route.request().headers()["x-csrf-token"]).toBe("csrf-test");
        expect(route.request().postDataJSON().template_id).toBe("orders_payments");
        await route.fulfill({ json: { answer_id: answerId, answer } });
      });
      await page.route(`**/api/v1/agent/reports/${reportId}`, (route) => route.fulfill({ json: {
        report_id: reportId, question: "Olist 订单趋势和支付结构有哪些值得关注的变化？",
        status: "invalidated", answer: null,
      } }));

      await page.goto("#/agent");
      await expect(page.getByRole("heading", { name: /问一个问题/ })).toBeVisible();
      await page.getByRole("button", { name: /订单趋势与支付/ }).click();
      expect(asks).toBe(0);
      await expect(page.getByRole("textbox", { name: "分析问题" })).toHaveValue(/Olist 订单趋势/);
      await page.getByRole("button", { name: "开始分析" }).click();
      await expect(page.getByText("模型未启用，仅展示可核对的证据。")).toBeVisible();
      await expect(page.getByText("99,441")).toBeVisible();
      await expect(page.getByText("orders-v1-real-run")).toBeVisible();
      await expect(page.getByText("历史订单快照，不是实时交易")).toBeVisible();
      await expect(page.getByRole("list", { name: "工具步骤" })).toContainText("search_knowledge");
      await expect(page.getByRole("link", { name: /查看原文定位/ })).toHaveAttribute("href", `#${locator}`);
      await expect(page.getByRole("link", { name: /前往订单与支付/ })).toHaveAttribute("href", "#/orders?window=full");
      expect(asks).toBe(1);
      expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width + 1);
      await page.screenshot({ path: testInfo.outputPath(`${label}-agent-answer.png`), fullPage: true });

      await page.getByRole("button", { name: "保存报告" }).click();
      await page.getByRole("button", { name: /Olist 订单趋势/ }).click();
      await expect(page.getByRole("heading", { name: "引用已失效" })).toBeVisible();
      await expect(page.getByText("99,441")).toHaveCount(0);
      await expect(page.getByText("订单数来自已发布指标。")).toHaveCount(0);
      expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width + 1);
      await page.screenshot({ path: testInfo.outputPath(`${label}-agent-invalidated.png`), fullPage: true });

      await page.goto("#/orders?window=full");
      await expect(page.getByLabel("订单粒度")).toHaveValue("full");
    });
  });
}
