import { expect, test } from "@playwright/test";

const username = process.env.G3_E2E_USERNAME;
const password = process.env.G3_E2E_PASSWORD;

for (const [label, width, height] of [["desktop", 1440, 900], ["mobile", 390, 844]] as const) {
  test.describe(label, () => {
    test.use({ viewport: { width, height } });

    test("shows a usable login page without horizontal overflow", async ({ page }, testInfo) => {
      await page.emulateMedia({ reducedMotion: "reduce" });
      await page.goto("");
      await expect(page.getByRole("heading", { name: /登录/ })).toBeVisible();
      expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width + 1);
      await page.screenshot({ path: testInfo.outputPath(`${label}-login.png`), fullPage: true });
    });

    test("uses published metrics from login through evidence, quality and logout", async ({ page }, testInfo) => {
      test.skip(!username || !password, "G3_E2E_USERNAME/G3_E2E_PASSWORD are required for real published-API verification");
      await page.emulateMedia({ reducedMotion: "reduce" });
      await page.goto("");
      await page.getByLabel("用户名").fill(username!);
      await page.getByLabel("密码").fill(password!);
      await page.getByRole("button", { name: "登录" }).click();
      if (label === "desktop") await expect(page.getByRole("navigation", { name: "主导航" })).toBeVisible();
      else await expect(page.getByRole("button", { name: "展开导航" })).toBeVisible();
      await expect(page.getByRole("heading", { name: "Olist 历史订单" })).toBeVisible();
      await expect(page.locator(".metric-identity")).toContainText("olist-brazilian-ecommerce-v2");
      const fullResponse = page.waitForResponse((response) => {
        const url = new URL(response.url());
        return url.pathname === "/api/v1/orders/overview" && url.searchParams.get("window") === "full";
      });
      await page.getByLabel("订单粒度").selectOption("full");
      expect((await fullResponse).ok()).toBe(true);
      await expect(page.locator(".metric-identity")).toContainText("orders-v1");
      await expect(page.locator(".overview-side")).toContainText("当前窗口订单数");
      await page.getByRole("button", { name: "查看数据证据" }).click();
      const dialog = page.getByRole("dialog", { name: "数据证据" });
      await expect(dialog).toBeVisible();
      await expect(dialog).toContainText(/orders-v1-[a-f0-9]+/);
      await page.keyboard.press("Escape");
      await expect(dialog).not.toBeVisible();
      if (label === "mobile") await page.getByRole("button", { name: "展开导航" }).click();
      await page.getByRole("link", { name: /数据质量/ }).click();
      await expect(page.getByRole("heading", { name: "Olist 来源与对账" })).toBeVisible();
      await expect(page.getByText("PASS", { exact: true })).toBeVisible();
      expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width + 1);
      await page.screenshot({ path: testInfo.outputPath(`${label}-quality.png`), fullPage: true });
      await page.getByRole("button", { name: "退出" }).click();
      await expect(page.getByRole("heading", { name: /登录/ })).toBeVisible();
    });
  });
}
