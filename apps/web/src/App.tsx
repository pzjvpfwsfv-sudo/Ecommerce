import { useEffect, useState } from "react";
import { HashRouter, Navigate, Route, Routes } from "react-router-dom";

import { AccountPage } from "./components/AccountPage";
import { LoginPage } from "./components/LoginPage";
import { Shell } from "./components/Shell";
import { AuthSession, getSession, logoutUser } from "./lib/auth";
import { ApiError } from "./lib/http";
import { Overview } from "./modules/Overview";
import { Behavior } from "./modules/Behavior";
import { Orders } from "./modules/Orders";

const moduleNames: Record<string, string> = {
  overview: "运营总览",
  behavior: "行为与转化",
  orders: "订单与支付",
  rankings: "商品与品类",
  fulfillment: "履约与评价",
  quality: "数据质量",
};

function ModulePending({ name }: { name: string }) {
  return (
    <section className="pending-module" aria-labelledby="pending-title">
      <span className="eyebrow">G3 / DATA WORKBENCH</span>
      <h2 id="pending-title">{name}</h2>
      <p>该模块正在接入已发布指标，当前不展示演示数值。</p>
    </section>
  );
}

export default function App() {
  const [session, setSession] = useState<AuthSession | null>();
  const [error, setError] = useState<string | null>(null);
  const [refresh, setRefresh] = useState(0);

  useEffect(() => {
    let active = true;
    getSession().then(
      (user) => { if (active) { setSession(user); setError(null); } },
      (cause: unknown) => {
        if (!active) return;
        if (cause instanceof ApiError && cause.status === 401) setSession(null);
        else setError("身份服务暂不可用，请稍后重试。");
      },
    );
    return () => { active = false; };
  }, [refresh]);

  useEffect(() => {
    const expire = () => setSession(null);
    window.addEventListener("g3:unauthorized", expire);
    return () => window.removeEventListener("g3:unauthorized", expire);
  }, []);

  async function handleLogout() {
    if (!session) return;
    try {
      await logoutUser(session.csrf_token);
      setSession(null);
      setError(null);
    } catch {
      setError("退出失败，请检查服务状态后重试。");
    }
  }

  if (error && session === undefined) {
    return (
      <main className="auth-stage">
        <div className="auth-card" role="alert">
          <span className="eyebrow">SERVICE STATUS</span>
          <h1>暂时无法连接身份服务</h1>
          <p>{error}</p>
          <button type="button" onClick={() => setRefresh((value) => value + 1)}>重新尝试</button>
        </div>
      </main>
    );
  }
  if (session === undefined) return <main className="auth-stage" role="status">正在验证会话…</main>;
  if (session === null) return <LoginPage onLogin={(user) => { setSession(user); setError(null); }} />;

  return (
    <HashRouter>
      <Shell user={session} onLogout={handleLogout}>
        {error && <p className="notice notice--error" role="alert">{error}</p>}
        <Routes>
          <Route path="/" element={<Navigate to="/overview" replace />} />
          {Object.entries(moduleNames).map(([path, name]) => (
            <Route key={path} path={`/${path}`} element={path === "overview" ? <Overview /> : path === "behavior" ? <Behavior /> : path === "orders" ? <Orders /> : <ModulePending name={name} />} />
          ))}
          <Route path="/account" element={<AccountPage user={session} />} />
          <Route path="*" element={<Navigate to="/overview" replace />} />
        </Routes>
      </Shell>
    </HashRouter>
  );
}
