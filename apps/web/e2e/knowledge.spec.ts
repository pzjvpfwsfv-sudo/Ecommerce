import { expect, test } from "@playwright/test";

const documentId = "11111111-1111-1111-1111-111111111111";
const versionId = "22222222-2222-2222-2222-222222222222";
const chunkId = "33333333-3333-3333-3333-333333333333";

for (const [label, width, height] of [["desktop", 1440, 900], ["mobile", 390, 844]] as const) {
  test.describe(label, () => {
    test.use({ viewport: { width, height } });

    test("opens a knowledge citation without overflow or HTML execution", async ({ page }, testInfo) => {
      await page.emulateMedia({ reducedMotion: "reduce" });
      await page.route("**/api/v1/auth/me", (route) => route.fulfill({ json: {
        id: 1, username: "reader", role: "viewer", csrf_token: "test-only",
      } }));
      await page.route("**/api/v1/knowledge/documents", (route) => route.fulfill({ json: [{
        id: documentId, title: "真实订单口径", category: "指标定义", source_type: "project_doc",
        source_ref: "configs/metrics/orders-v1.json", visibility_roles: ["viewer"],
        published_version_id: versionId,
      }] }));
      await page.route("**/api/v1/knowledge/search?*", (route) => route.fulfill({ json: {
        mode: "keyword_only", elapsed_ms: 12, hits: [{
          chunk_id: chunkId, document_id: documentId, version_id: versionId,
          section: "订单数", page: null, text: "order_count 来自真实 Olist 订单",
          source_label: "项目文档", source_ref: "configs/metrics/orders-v1.json",
          keyword_rank: 1, vector_rank: null, score: .016,
          locator: `/knowledge/documents/${documentId}/versions/${versionId}#chunk-${chunkId}`,
        }],
      } }));
      await page.route(`**/api/v1/knowledge/documents/${documentId}/versions`, (route) => route.fulfill({ json: [{
        id: versionId, document_id: documentId, version_no: 1, status: "published",
        created_at: "2026-10-09T00:00:00Z", published_at: "2026-10-09T00:00:00Z",
      }] }));
      await page.route(`**/api/v1/knowledge/documents/${documentId}/versions/${versionId}`, (route) => route.fulfill({ json: {
        document_id: documentId, version_id: versionId,
        extracted_text: "原文 <script>window.injected=true</script>",
        chunks: [{ id: chunkId, ordinal: 0, section: "订单数", page: null, text: "order_count 来自真实 Olist 订单" }],
      } }));

      await page.goto("#/knowledge");
      if (label === "mobile") {
        await page.getByRole("button", { name: "展开导航" }).click();
        await expect(page.getByRole("navigation", { name: "主导航" })).toBeVisible();
        await page.getByRole("button", { name: "展开导航" }).click();
      }
      await expect(page.getByRole("heading", { name: "让答案回到原文。" })).toBeVisible();
      await page.getByRole("searchbox", { name: "检索知识库" }).fill("order_count");
      await page.getByRole("button", { name: "检索", exact: true }).click();
      await expect(page.getByText("仅关键词检索 · 1 条")).toBeVisible();
      await page.getByRole("link", { name: /查看原文定位/ }).click();
      await expect(page.getByText("原文 <script>window.injected=true</script>")).toBeVisible();
      expect(await page.evaluate(() => (window as Window & { injected?: boolean }).injected)).not.toBe(true);
      expect(await page.locator(".knowledge-text script").count()).toBe(0);
      expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width + 1);
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.screenshot({ path: testInfo.outputPath(`${label}-knowledge.png`), fullPage: true });
    });
  });
}
