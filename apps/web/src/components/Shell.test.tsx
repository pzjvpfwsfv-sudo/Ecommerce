import { HttpResponse, http } from "msw";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import App from "../App";
import { server } from "../test/handlers";

vi.mock("../modules/Overview", () => ({ Overview: () => <div>总览模块</div> }));

const admin = { id: 1, username: "owner", role: "admin", csrf_token: "csrf-test" };
const viewer = { ...admin, username: "reader", role: "viewer" };

beforeEach(() => {
  window.location.hash = "#/overview";
});

describe("workbench shell", () => {
  it("shows login rather than metrics when the session is missing", async () => {
    server.use(http.get("/api/v1/auth/me", () => new HttpResponse(null, { status: 401 })));
    render(<App />);
    expect(await screen.findByRole("heading", { name: /登录/ })).toBeVisible();
    expect(screen.queryByText("99,441")).not.toBeInTheDocument();
  });

  it("navigates authenticated modules and exposes analyst AI work", async () => {
    server.use(http.get("/api/v1/auth/me", () => HttpResponse.json(admin)));
    render(<App />);
    expect(await screen.findByRole("navigation", { name: "主导航" })).toBeVisible();
    const orders = screen.getByRole("link", { name: /订单与支付/ });
    await userEvent.click(orders);
    expect(orders).toHaveAttribute("aria-current", "page");
    expect(screen.getByRole("link", { name: /AI 指标分析/ })).toHaveAttribute("href", "#/agent");
  });

  it("makes account creation available only to administrators", async () => {
    window.location.hash = "#/account";
    server.use(
      http.get("/api/v1/auth/me", () => HttpResponse.json(admin)),
      http.get("/api/v1/auth/users", () => HttpResponse.json([
        { id: 1, username: "owner", role: "admin" },
      ])),
    );
    const view = render(<App />);
    expect(await screen.findByRole("button", { name: "创建账户" })).toBeVisible();
    view.unmount();
    server.use(http.get("/api/v1/auth/me", () => HttpResponse.json(viewer)));
    render(<App />);
    expect(await screen.findByText(/只读账户/)).toBeVisible();
    expect(screen.queryByRole("button", { name: "创建账户" })).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: /AI 指标分析/ })).not.toBeInTheDocument();
  });

  it("exposes a collapsible navigation control on narrow screens", async () => {
    server.use(http.get("/api/v1/auth/me", () => HttpResponse.json(admin)));
    render(<App />);
    const toggle = await screen.findByRole("button", { name: "展开导航" });
    expect(toggle).toHaveAttribute("aria-expanded", "false");
    await userEvent.click(toggle);
    expect(toggle).toHaveAttribute("aria-expanded", "true");
  });

  it("sends login credentials only in a POST body", async () => {
    let observedUrl = "";
    let observedBody: unknown;
    server.use(
      http.post("/api/v1/auth/login", async ({ request }) => {
        observedUrl = request.url;
        observedBody = await request.json();
        return HttpResponse.json(admin);
      }),
    );
    render(<App />);
    await userEvent.type(await screen.findByLabelText("用户名"), "owner");
    await userEvent.type(screen.getByLabelText("密码"), "secret-password");
    await userEvent.click(screen.getByRole("button", { name: "登录" }));
    expect(await screen.findByRole("navigation", { name: "主导航" })).toBeVisible();
    expect(observedUrl).not.toContain("secret-password");
    expect(observedBody).toEqual({ username: "owner", password: "secret-password" });
  });

  it("sends an admin's user creation with CSRF", async () => {
    window.location.hash = "#/account";
    let csrf = "";
    server.use(
      http.get("/api/v1/auth/me", () => HttpResponse.json(admin)),
      http.get("/api/v1/auth/users", () => HttpResponse.json([])),
      http.post("/api/v1/auth/users", ({ request }) => {
        csrf = request.headers.get("X-CSRF-Token") ?? "";
        return HttpResponse.json({ id: 2, username: "reader", role: "viewer" }, { status: 201 });
      }),
    );
    render(<App />);
    await userEvent.type(await screen.findByLabelText("用户名"), "reader");
    await userEvent.type(screen.getByLabelText(/初始密码/), "reader-password");
    await userEvent.click(screen.getByRole("button", { name: "创建账户" }));
    await waitFor(() => expect(csrf).toBe("csrf-test"));
    expect(await screen.findByText("账户已创建。")).toBeVisible();
  });
});
