import { useState, type ReactNode } from "react";
import { NavLink, useLocation } from "react-router-dom";

import type { AuthUser } from "../lib/auth";

const navigation = [
  ["/overview", "运营总览", "01"],
  ["/behavior", "行为与转化", "02"],
  ["/orders", "订单与支付", "03"],
  ["/rankings", "商品与品类", "04"],
  ["/fulfillment", "履约与评价", "05"],
  ["/quality", "数据质量", "06"],
  ["/knowledge", "知识库检索", "07"],
  ["/agent", "AI 指标分析", "08"],
] as const;

const roleNames = { admin: "管理员", analyst: "分析员", viewer: "只读用户" };

export function Shell({ user, onLogout, children }: {
  user: AuthUser;
  onLogout: () => Promise<void>;
  children: ReactNode;
}) {
  const [mobileOpen, setMobileOpen] = useState(false);
  const location = useLocation();
  const title = location.pathname.startsWith("/knowledge/") ? "知识库检索"
    : navigation.find(([path]) => path === location.pathname)?.[1] ?? "账户";

  return (
    <div className="workbench">
      <aside className="sidebar">
        <div className="brand">
          <span className="brand-mark" aria-hidden="true">A</span>
          <div><strong>ATLAS</strong><small>ECOMMERCE INTELLIGENCE</small></div>
        </div>
        <button
          type="button"
          className="mobile-nav-toggle"
          aria-label="展开导航"
          aria-expanded={mobileOpen}
          aria-controls="main-navigation"
          onClick={() => setMobileOpen((open) => !open)}
        >{mobileOpen ? "收起菜单" : "浏览模块"}</button>
        <nav id="main-navigation" aria-label="主导航" className={mobileOpen ? "nav nav--open" : "nav"}>
          <span className="nav-caption">WORKSPACE / 工作区</span>
          {navigation.filter(([path]) => path !== "/agent" || user.role !== "viewer").map(([path, label, number]) => (
            <NavLink
              key={path}
              to={path}
              className={({ isActive }) => `nav-link${isActive ? " nav-link--active" : ""}`}
              onClick={() => setMobileOpen(false)}
            ><span aria-hidden="true">{number}</span>{label}</NavLink>
          ))}
        </nav>
        <div className="sidebar-foot">
          <span className="status-dot" aria-hidden="true" />
          Olist + REES46 · 独立来源
        </div>
      </aside>
      <div className="main-column">
        <header className="topbar">
          <div><span className="eyebrow">ATLAS / WORKBENCH</span><h1>{title}</h1></div>
          <div className="topbar-actions">
            <NavLink to="/account" className="account-link" onClick={() => setMobileOpen(false)}>
              <span className="account-avatar" aria-hidden="true">{user.username.slice(0, 1).toUpperCase()}</span>
              <span><strong>{user.username}</strong><small>{roleNames[user.role]}</small></span>
            </NavLink>
            <button type="button" className="text-button" onClick={onLogout}>退出</button>
          </div>
        </header>
        <main id="main-content" className="page-content">{children}</main>
      </div>
    </div>
  );
}
