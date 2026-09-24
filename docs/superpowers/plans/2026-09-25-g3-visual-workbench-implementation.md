# G3 Visual Workbench Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a responsive, premium green analytics workbench with six distinct, interactive modules backed only by published Olist and REES46 metrics.

**Architecture:** A Vite/React app under `apps/web` uses one shared shell, auth session, typed domain clients, and evidence drawer, while each business module owns its chart composition and interactions. Vite proxies API calls in development; a production build is served from the existing FastAPI origin. Complete `2026-09-25-g3-identity-access-implementation.md` first.

**Tech Stack:** Node 24, React, TypeScript, Vite, ECharts, Vitest, Testing Library, Playwright using an installed system browser, FastAPI static file serving.

**Spec:** `docs/superpowers/specs/2026-09-25-graduation-g3-visual-workbench-design.md`

## Global Constraints

- Use the approved palette: ink `#1a2e29`, deep `#1d3c33`, forest `#2b5b4d`, jade `#2d7566`, mint `#e7eee8`, paper `#f2f2ed`, sheet `#fdfdfa`, line `#d8e0d9`, sparse copper `#b48361`. Do not revert to fluorescent green.
- Shared shell, filter grammar, feedback, and evidence are reusable; the six modules must not be one repeated KPI-card/line-chart template.
- Olist `orders-v1` is a historical snapshot with `day/month/full` queries; REES46 `behavior-v1` currently has a 1,002-event correctness subset and `day/full` queries. Keep datasets, versions, runs, and identities separate.
- Never label purchase price proxy as revenue or GMV; never label Olist item, freight, or payment value as profit or audited GMV. When `source_currency` is null, display no currency symbol.
- Never inject mock data into production, turn null into zero, hide subset warnings, mix results from different runs, or call Top N a complete distribution.
- AI/RAG, knowledge base, reports, replay control, and per-order detail queries are G4 or later; G3 navigation must not pretend they work.
- Browser auth is same-origin via HttpOnly cookie. Unauthenticated requests redirect to login; 403 and 503 show distinct, useful states.
- Test visual and keyboard interactions on desktop and narrow mobile width. On this Windows host use PowerShell and keep downloads/cache off C when possible.

## Review Focus

1. A slow older filter response must not replace a newer selection or mix its meta/run; Task 2 tests cancellation and Task 9 checks the browser flow.
2. Null rates, absent currency, and a tiny behavior subset must show honest labels and warnings rather than zero, money symbols, or scale claims; Tasks 2 and 4 test this.
3. An API 401, 403, 422, 503, empty result, or invalid JSON must not fall back to preview numbers; Tasks 1-2 and Task 9 test these states.
4. A ranking dimension cannot use an unsupported sort (for example payment value on product), and Top N must remain labeled; Task 6 tests this.
5. Evidence drawer, navigation, tooltips, and filters must work by keyboard and remain usable on a narrow viewport or reduced-motion setting; Tasks 1-2 and Task 9 test this.

## File Map

- `apps/web/package.json`, lockfile, `index.html`, `tsconfig.json`, `vite.config.ts`: repeatable build and same-origin dev proxy.
- `apps/web/src/main.tsx`, `App.tsx`, `styles/tokens.css`, `styles/global.css`: app entry, routes, approved visual language.
- `apps/web/src/lib/http.ts`, `auth.ts`, `api.ts`, `types.ts`, `format.ts`, `useMetricQuery.ts`, `apps/web/src/test/setup.ts`, `apps/web/src/test/handlers.ts`: auth, typed metric fetching, truth-preserving formatting, cancellation, and test-only HTTP fixtures.
- `apps/web/src/components/Shell.tsx`, `LoginPage.tsx`, `AccountPage.tsx`, `MetricFrame.tsx`, `Chart.tsx`, `EvidenceDrawer.tsx`: shared but limited infrastructure.
- `apps/web/src/modules/Overview.tsx`, `Behavior.tsx`, `Orders.tsx`, `Rankings.tsx`, `Fulfillment.tsx`, `Quality.tsx`: modules with distinct compositions.
- Colocated `*.test.tsx` and `*.test.ts` files: unit/component contracts; `apps/web/e2e/workbench.spec.ts`: real-browser flow.
- `services/api/app/main.py`, `services/api/app/config.py`, `infra/docker-compose.yml`, `docs/graduation/g3-visual-workbench-runbook.md`: production static serving and evidence.

---

### Task 1: App Shell, Login, And Account

**Files:**
- Create: `apps/web/package.json`, `apps/web/package-lock.json`, `apps/web/index.html`, `apps/web/tsconfig.json`, `apps/web/vite.config.ts`, `apps/web/src/main.tsx`, `apps/web/src/App.tsx`, `apps/web/src/styles/tokens.css`, `apps/web/src/styles/global.css`, `apps/web/src/lib/http.ts`, `apps/web/src/lib/auth.ts`, `apps/web/src/test/setup.ts`, `apps/web/src/test/handlers.ts`, `apps/web/src/components/Shell.tsx`, `apps/web/src/components/LoginPage.tsx`, `apps/web/src/components/AccountPage.tsx`, `apps/web/src/components/Shell.test.tsx`

**Interfaces:**
- Consumes: Auth plan `POST /api/v1/auth/login`, `GET /api/v1/auth/me`, `POST /api/v1/auth/logout`, `GET/POST /api/v1/auth/users`.
- Produces: `jsonRequest<T>(path: string, init?: RequestInit): Promise<T>`, `AuthUser`, `AuthSession`, `HashRouter` paths `/overview`, `/behavior`, `/orders`, `/rankings`, `/fulfillment`, `/quality`, `/account`.

- [ ] **Step 1: Write failing shell tests for unauthenticated login, admin-only user creation, active navigation, explicit G4-unavailable labels, focus-visible controls, and compact mobile navigation.**

~~~tsx
it("shows login instead of metrics when /me returns 401", async () => {
  server.use(http.get("/api/v1/auth/me", () => new HttpResponse(null, { status: 401 })));
  render(<App />);
  expect(await screen.findByRole("heading", { name: /登录/ })).toBeVisible();
  expect(screen.queryByText("99,441")).not.toBeInTheDocument();
});
~~~

- [ ] **Step 2: Run `npm test -- --run src/components/Shell.test.tsx` from `apps/web`; expect missing package/app failure.**
- [ ] **Step 3: Scaffold with `npm install --save-exact react react-dom react-router-dom echarts` and `npm install --save-dev --save-exact typescript vite @vitejs/plugin-react vitest @types/react @types/react-dom @testing-library/react @testing-library/user-event @testing-library/jest-dom jsdom msw`; commit the generated lockfile. Set scripts to `test: vitest`, `typecheck: tsc --noEmit`, `build: tsc --noEmit && vite build`. Configure Vitest `environment: "jsdom"`, `setupFiles: ["./src/test/setup.ts"]`, the MSW server in `src/test/handlers.ts`, Vite `base: "/app/"`, and API proxies for `/api`, `/analysis`, `/metrics`. Implement auth calls, routes, responsive shell, account/logout, and palette tokens. Use a purposeful Chinese sans family for body and a restrained serif heading; keep the sidebar grid subtle, motion limited to page/chart changes, and label AI/RAG as G4 unavailable. Do not add demo metric constants.**

~~~css
:root {
  --ink: #1a2e29; --deep: #1d3c33; --forest: #2b5b4d;
  --jade: #2d7566; --mint: #e7eee8; --paper: #f2f2ed;
  --sheet: #fdfdfa; --line: #d8e0d9; --copper: #b48361;
}
~~~

- [ ] **Step 4: Run `npm test -- --run src/components/Shell.test.tsx`, `npm run typecheck`, and `npm run build`; expect pass. Verify login POST includes credentials only in request body, not URL or logs.**
- [ ] **Step 5: Commit with `git commit -m "feat: scaffold G3 authenticated workbench"`.**

### Task 2: Typed Metric Client, States, And Evidence

**Files:**
- Create: `apps/web/src/lib/api.ts`, `apps/web/src/lib/types.ts`, `apps/web/src/lib/format.ts`, `apps/web/src/lib/useMetricQuery.ts`, `apps/web/src/lib/api.test.ts`, `apps/web/src/components/MetricFrame.tsx`, `apps/web/src/components/Chart.tsx`, `apps/web/src/components/EvidenceDrawer.tsx`, `apps/web/src/components/EvidenceDrawer.test.tsx`

**Interfaces:**
- Produces: `OrderQuery = { window: "day" | "month" | "full"; startDate?: string; endDate?: string; signal?: AbortSignal }` and `BehaviorQuery = { window: "day" | "full"; signal?: AbortSignal }`. Implement `getOrderOverview(query: OrderQuery)`, `getOrderDelivery(query: OrderQuery)`, `getOrderPayments(query: OrderQuery)`, `getOrderReviews(query: OrderQuery)`, `getOrderRankings(query: OrderRankingQuery)`, `getOrderQuality(signal?: AbortSignal)`, and corresponding behavior functions. `OrderRankingQuery` extends `OrderQuery` with `dimension`, `sortBy`, and `limit`; `BehaviorRankingQuery` extends `BehaviorQuery` with its own domain-valid `dimension`, `sortBy`, and `limit`. `getPublication(domain: "orders" | "behavior")` and `getDefinitions(domain: "orders" | "behavior")` return domain-specific responses.
- Produces: `fetchSameRun<T, U>(first, second): Promise<[T, U]>`, which rejects mismatched `dataset_id`, `metric_version`, or `metric_run_id` within one domain. `useMetricQuery(loader: (signal: AbortSignal) => Promise<T>, key: string)` aborts stale requests and atomically replaces data/meta.
- Produces: `formatRate(value: string | null, numerator?: number, denominator?: number): string` and `formatValue(value: string, currency: string | null): string`.

- [ ] **Step 1: Write failing tests for 401/403/422/503/invalid JSON, two responses with different run IDs, late old response, null rate, no currency, and drawer Escape/focus return.**

~~~ts
it("rejects two order responses from different runs", async () => {
  const first = Promise.resolve({ meta: orderMeta("run-a"), data: [] });
  const second = Promise.resolve({ meta: orderMeta("run-b"), data: [] });
  await expect(fetchSameRun(first, second)).rejects.toThrow("metric run mismatch");
});
it("does not invent a currency", () => {
  expect(formatValue("13591643.70", null)).toBe("13,591,643.70");
  expect(formatRate(null)).toBe("不适用");
});
~~~

- [ ] **Step 2: Run `npm test -- --run src/lib/api.test.ts src/components/EvidenceDrawer.test.tsx`; expect import failure.**
- [ ] **Step 3: Implement fetch wrapper with `credentials: "same-origin"`, response JSON checks, exact allowed query keys, and runtime meta guards; use one AbortController per filter request. The evidence drawer loads definitions for the current domain/version and shows the response's dataset, run, windows, calculated/published time, warning, and snapshots.**

~~~ts
export async function fetchSameRun<T extends { meta: MetricMeta }, U extends { meta: MetricMeta }>(
  first: Promise<T>, second: Promise<U>,
): Promise<[T, U]> {
  const result = await Promise.all([first, second] as const);
  const [a, b] = result;
  if (a.meta.dataset_id !== b.meta.dataset_id ||
      a.meta.metric_version !== b.meta.metric_version ||
      a.meta.metric_run_id !== b.meta.metric_run_id) {
    throw new Error("metric run mismatch");
  }
  return result;
}
~~~

- [ ] **Step 4: Run focused tests, `npm run typecheck`, and `npm run build`; expect pass. Exercise keyboard open, Escape close, and focus restoration in the drawer test.**
- [ ] **Step 5: Commit with `git commit -m "feat: add truthful metric client and evidence states"`.**

### Task 3: Order Operations Overview

**Files:**
- Create: `apps/web/src/modules/Overview.tsx`, `apps/web/src/modules/overviewChart.ts`, `apps/web/src/modules/Overview.test.tsx`
- Modify: `apps/web/src/App.tsx`

**Interfaces:**
- Consumes: Task 2 `getOrderOverview`, `getPublication("orders")`, `MetricFrame`, `Chart`, `EvidenceDrawer`.
- Produces: order-only summary with DAY/MONTH/FULL time views; REES46 publication is a separate status panel, not a merged count.

- [ ] **Step 1: Write failing test asserting real historical window, API number, empty state, and separate behavior publication label.**

~~~tsx
render(<Overview />);
expect(await screen.findByText("99,441")).toBeVisible();
expect(screen.getByText(/2016-09-04/)).toBeVisible();
expect(screen.getByText(/REES46 行为域/)).toBeVisible();
expect(screen.queryByText(/今日实时订单/)).not.toBeInTheDocument();
~~~

- [ ] **Step 2: Run `npm test -- --run src/modules/Overview.test.tsx`; expect missing module failure.**
- [ ] **Step 3: Implement the approved editorial overview layout: key order facts, order-volume time series, publication strip, evidence action, and legal date filter. FULL uses no date parameters. Chart tooltip reports exact count/window, not fabricated growth.**

~~~ts
const option = {
  xAxis: { type: "category", data: points.map((p) => p.window_start) },
  yAxis: { type: "value", name: "订单数" },
  series: [{ type: "line", smooth: false, data: points.map((p) => p.order_count) }],
};
~~~

- [ ] **Step 4: Run focused tests, typecheck, and build; expect pass.**
- [ ] **Step 5: Commit with `git commit -m "feat: add G3 order operations overview"`.**

### Task 4: Behavior And Conversion

**Files:**
- Create: `apps/web/src/modules/Behavior.tsx`, `apps/web/src/modules/behaviorChart.ts`, `apps/web/src/modules/Behavior.test.tsx`
- Modify: `apps/web/src/App.tsx`

**Interfaces:**
- Consumes: `getBehaviorOverview/Funnel/Quality` and behavior `meta.data_scope`; only `day/full` filters.
- Produces: ordered session ladder `view_sessions -> view_to_cart_sessions -> completed_sessions`, with missing-session count and rate denominators.

- [ ] **Step 1: Write failing tests that show `g2c-correctness-subset` and the 1,002-event caveat, preserve null rates, and reject a month filter.**

~~~tsx
render(<Behavior />);
expect(await screen.findByText(/正确性子集/)).toBeVisible();
expect(screen.getByText(/缺失会话/)).toBeVisible();
expect(screen.queryByRole("option", { name: "按月" })).not.toBeInTheDocument();
~~~

- [ ] **Step 2: Run `npm test -- --run src/modules/Behavior.test.tsx`; expect missing module failure.**
- [ ] **Step 3: Implement a three-stage stepped funnel beside event trend, each stage showing actual session count and denominator. Stage click highlights the related metric definition; it never divides raw event counts to invent conversion.**

~~~ts
const stages = [
  { label: "浏览会话", count: point.view_sessions },
  { label: "加购会话", count: point.view_to_cart_sessions },
  { label: "完成购买会话", count: point.completed_sessions },
];
~~~

- [ ] **Step 4: Run focused tests, typecheck, and build; expect pass.**
- [ ] **Step 5: Commit with `git commit -m "feat: show honest behavior conversion funnel"`.**

### Task 5: Orders And Payments

**Files:**
- Create: `apps/web/src/modules/Orders.tsx`, `apps/web/src/modules/ordersChart.ts`, `apps/web/src/modules/Orders.test.tsx`
- Modify: `apps/web/src/App.tsx`

**Interfaces:**
- Consumes: `getOrderOverview` and `getOrderPayments` for one matching run.
- Produces: order count/status-rate combination and payment-type horizontal ranking; toggles only between API-provided `payment_order_count` and `payment_value_sum`.

- [ ] **Step 1: Write failing tests for run mismatch, payment toggle, and currency-null display.**

~~~tsx
render(<Orders />);
await user.click(await screen.findByRole("button", { name: "支付值" }));
expect(screen.getByText("16,008,872.12")).toBeVisible();
expect(screen.queryByText(/R\$/)).not.toBeInTheDocument();
~~~

- [ ] **Step 2: Run `npm test -- --run src/modules/Orders.test.tsx`; expect missing module failure.**
- [ ] **Step 3: Implement a mixed order-count/status trend and a separate sorted horizontal payment comparison. Payment values remain strings until formatting; no implied payment share or financial-revenue claim.**

~~~ts
const paymentRows = [...response.data]
  .filter((row) => !row.is_all)
  .sort((a, b) => Number(b.payment_value_sum) - Number(a.payment_value_sum));
~~~

- [ ] **Step 4: Run focused tests, typecheck, and build; expect pass.**
- [ ] **Step 5: Commit with `git commit -m "feat: add order and payment analysis"`.**

### Task 6: Product, Category, And Regional Rankings

**Files:**
- Create: `apps/web/src/modules/Rankings.tsx`, `apps/web/src/modules/rankingOptions.ts`, `apps/web/src/modules/Rankings.test.tsx`
- Modify: `apps/web/src/App.tsx`

**Interfaces:**
- Consumes: `getOrderRankings` or `getBehaviorRankings`, never both in one metric series.
- Produces: valid dimension/sort matrix and Top N panel, row-selected aggregate detail, unknown-dimension label.

- [ ] **Step 1: Write failing tests for product/payment-value sort rejection, domain isolation, unknown category, Top N label, and selected-row detail.**

~~~tsx
render(<Rankings />);
await user.selectOptions(screen.getByLabelText("维度"), "product");
expect(screen.queryByRole("option", { name: "支付值" })).not.toBeInTheDocument();
expect(screen.getByText(/仅展示 Top 20/)).toBeVisible();
~~~

- [ ] **Step 2: Run `npm test -- --run src/modules/Rankings.test.tsx`; expect missing module failure.**
- [ ] **Step 3: Encode order sort matrix exactly: product/category/seller allow `order_count`, `item_value`, `freight_value`; customer_state/seller_state allow `order_count`, `payment_value`, `late_rate`. Use bars plus a sortable table; selecting a row opens only its returned aggregate values, not a fictional time series. Mark seller-state payment values as nonadditive when `payment_value_is_additive` is false.**

~~~ts
const orderSorts = {
  product: ["order_count", "item_value", "freight_value"],
  category: ["order_count", "item_value", "freight_value"],
  seller: ["order_count", "item_value", "freight_value"],
  customer_state: ["order_count", "payment_value", "late_rate"],
  seller_state: ["order_count", "payment_value", "late_rate"],
} as const;
~~~

- [ ] **Step 4: Run focused tests, typecheck, and build; expect pass.**
- [ ] **Step 5: Commit with `git commit -m "feat: add source-aware ranking exploration"`.**

### Task 7: Fulfillment And Reviews

**Files:**
- Create: `apps/web/src/modules/Fulfillment.tsx`, `apps/web/src/modules/fulfillmentChart.ts`, `apps/web/src/modules/Fulfillment.test.tsx`
- Modify: `apps/web/src/App.tsx`

**Interfaces:**
- Consumes: `getOrderDelivery` and `getOrderReviews` for the same run and date filter.
- Produces: delivery mean/P50/P90 and late-rate view; separate review average/coverage/low-score view, not a made-up star histogram.

- [ ] **Step 1: Write failing tests for switch between delivery and review, null eligible periods, excluded counts, and no rating histogram.**

~~~tsx
render(<Fulfillment />);
await user.click(await screen.findByRole("tab", { name: "评价" }));
expect(screen.getByText(/评价覆盖率/)).toBeVisible();
expect(screen.queryByText(/五星分布/)).not.toBeInTheDocument();
~~~

- [ ] **Step 2: Run `npm test -- --run src/modules/Fulfillment.test.tsx`; expect missing module failure.**
- [ ] **Step 3: Implement P50/P90 band and late-rate chart for delivery; review uses separate small multiple time series. Tooltip shows eligible/excluded denominator and source window, null as not applicable.**

~~~ts
const deliveryBand = points.map((p) => ({
  date: p.window_start,
  p50: p.delivery_days_p50 === null ? null : Number(p.delivery_days_p50),
  p90: p.delivery_days_p90 === null ? null : Number(p.delivery_days_p90),
}));
~~~

- [ ] **Step 4: Run focused tests, typecheck, and build; expect pass.**
- [ ] **Step 5: Commit with `git commit -m "feat: add fulfillment and review views"`.**

### Task 8: Data Quality And Audit Flow

**Files:**
- Create: `apps/web/src/modules/Quality.tsx`, `apps/web/src/modules/Quality.test.tsx`
- Modify: `apps/web/src/App.tsx`

**Interfaces:**
- Consumes: order/behavior `quality` and `publication`; never merges their row counts.
- Produces: an Olist four-stage provenance path using source/normalized/Iceberg row maps and publication; a separate REES46 quality path using source/clean/late/overview counts; reconciliation matrix, reportable issues, run/Snapshot evidence.

- [ ] **Step 1: Write failing tests showing reportable Olist issues even when reconciliation is PASS, distinct behavior quality warning, and a 503 state without backup fixture data.**

~~~tsx
render(<Quality />);
expect(await screen.findByText("PASS")).toBeVisible();
expect(screen.getByText(/重复 review_id/)).toBeVisible();
expect(screen.getByText("789")).toBeVisible();
~~~

- [ ] **Step 2: Run `npm test -- --run src/modules/Quality.test.tsx`; expect missing module failure.**
- [ ] **Step 3: Implement the Olist source -> normalized -> Iceberg -> published process using row-count maps, then a separate anomaly list. Implement REES46 as source -> clean/late -> overview -> published using its different API shape. Distinguish hard-gate failure from nonzero reportable source issues in copy and visual semantics.**

~~~ts
const sumRows = (rows: Record<string, number>) =>
  Object.values(rows).reduce((total, count) => total + count, 0);
const stages = [
  { label: "原始来源", detail: sumRows(quality.raw_row_counts).toLocaleString() },
  { label: "规范化", detail: sumRows(quality.normalized_row_counts).toLocaleString() },
  { label: "入湖", detail: sumRows(quality.iceberg_row_counts).toLocaleString() },
  { label: "指标发布", detail: publication.data.status },
];
~~~

- [ ] **Step 4: Run focused tests, typecheck, and build; expect pass.**
- [ ] **Step 5: Commit with `git commit -m "feat: show traceable data quality"`.**

### Task 9: Same-Origin Build, Accessibility, And Real Verification

**Files:**
- Create: `apps/web/playwright.config.ts`, `apps/web/e2e/workbench.spec.ts`, `tests/test_g3_web_static.py`, `docs/graduation/g3-visual-workbench-runbook.md`
- Modify: `services/api/app/config.py`, `services/api/app/main.py`, `infra/docker-compose.yml`, `apps/web/package.json`

**Interfaces:**
- Consumes: Tasks 1-8 build output `apps/web/dist`, auth cookie, all metric APIs.
- Produces: `http://localhost:8000/app/` served by FastAPI from `WEB_DIST_DIR` after `npm run build`. Vite uses `/app/` base and same-origin API proxy; HashRouter avoids SPA deep-link fallback.

- [ ] **Step 1: Write failing FastAPI test that an explicit, missing `WEB_DIST_DIR` fails startup, valid built directory serves `/app/`, and metric API routes still work. Add browser tests for login -> overview -> filter -> evidence -> quality -> logout at desktop and 390px mobile width.**

~~~python
with tempfile.TemporaryDirectory() as root:
    Path(root, "index.html").write_text("<h1>G3</h1>", encoding="utf-8")
    app = create_app(
        settings=ApiSettings(web_dist_dir=Path(root)),
        auth_service=make_fake_auth_service(),
    )
    self.assertEqual(TestClient(app).get("/app/").status_code, 200)
~~~

- [ ] **Step 2: Run `python -m unittest tests.test_g3_web_static -v`; expect the missing settings/mount test to fail.**
- [ ] **Step 3: Add optional `WEB_DIST_DIR` setting (empty environment value maps to None) that fails if configured but missing; mount `StaticFiles` at `/app` only when set. Build before starting Compose API and mount `../apps/web/dist:/web:ro`; leave `WEB_DIST_DIR` empty in `infra/.env.example` so older API-only flows keep working, and set `WEB_DIST_DIR=/web` in the local deployment environment after the build. Install `@playwright/test` as a dev dependency and use the installed Edge channel `msedge`, avoiding a browser download to C. Respect `prefers-reduced-motion` and keyboard focus in all modules.**

~~~python
if settings.web_dist_dir is not None:
    index = settings.web_dist_dir / "index.html"
    if not index.is_file():
        raise RuntimeError("WEB_DIST_DIR must contain index.html")
    app.mount("/app", StaticFiles(directory=settings.web_dist_dir, html=True), name="web")
~~~

- [ ] **Step 4: Run `npm test -- --run`, `npm run typecheck`, `npm run build`, `python -m unittest discover -s tests -q`, and `docker compose --env-file infra/.env.example -f infra/docker-compose.yml config --quiet`. With services available, run Playwright against the real published APIs using an admin/analyst test account and record actual dataset/run IDs; otherwise state that browser-dynamic verification remains pending. Review screenshots at desktop and mobile and fix visual defects before claiming completion.**
- [ ] **Step 5: Record verified commands and known limits in the runbook; commit with `git commit -m "docs: verify G3 visual workbench"`.**
