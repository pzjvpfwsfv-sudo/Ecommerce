import { useState, type FormEvent } from "react";

import { loginUser, type AuthSession } from "../lib/auth";
import { ApiError } from "../lib/http";

export function LoginPage({ onLogin }: { onLogin: (session: AuthSession) => void }) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setBusy(true);
    setError("");
    try {
      onLogin(await loginUser(username, password));
    } catch (cause) {
      setError(cause instanceof ApiError && cause.status === 401
        ? "账号或密码不正确。" : "登录服务暂不可用，请稍后重试。");
    } finally {
      setBusy(false);
    }
  }

  return (
    <main className="auth-stage">
      <section className="login-intro">
        <span className="brand-mark brand-mark--large" aria-hidden="true">A</span>
        <span className="eyebrow">ATLAS / TRUSTED COMMERCE DATA</span>
        <h1>让每一个结论<br />都有数据来处。</h1>
        <p>历史订单与行为事件分域分析。先确认身份，再进入有口径、有证据的工作台。</p>
        <div className="login-ledger"><span>OLIST / ORDERS-V1</span><span>REES46 / BEHAVIOR-V1</span></div>
      </section>
      <section className="auth-card" aria-labelledby="login-heading">
        <span className="eyebrow">SECURE ACCESS / 安全访问</span>
        <h2 id="login-heading">登录工作台</h2>
        <p>使用管理员分配的账户继续。</p>
        <form onSubmit={submit}>
          <label htmlFor="username">用户名</label>
          <input id="username" autoComplete="username" value={username} onChange={(event) => setUsername(event.target.value)} required />
          <label htmlFor="password">密码</label>
          <input id="password" type="password" autoComplete="current-password" value={password} onChange={(event) => setPassword(event.target.value)} required />
          {error && <p role="alert" className="form-error">{error}</p>}
          <button className="primary-button" type="submit" disabled={busy}>{busy ? "验证中…" : "登录"}</button>
        </form>
        <small>会话由服务器管理；浏览器不保存口令或令牌。</small>
      </section>
    </main>
  );
}
