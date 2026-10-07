import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  timeout: 45_000,
  expect: { timeout: 15_000 },
  retries: 0,
  workers: 1,
  outputDir: "D:/EcommerceDev/temp/g3-playwright",
  reporter: "list",
  use: {
    baseURL: process.env.G3_WEB_URL ?? "http://127.0.0.1:8000/app/",
    channel: "msedge",
    browserName: "chromium",
    headless: true,
    trace: "retain-on-failure",
  },
});
