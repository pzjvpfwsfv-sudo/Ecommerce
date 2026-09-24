import { useEffect, useState, type FormEvent } from "react";

import { createUser, listUsers, type AuthUser, type PublicUser, type Role } from "../lib/auth";
import { ApiError } from "../lib/http";

const roleLabels: Record<Role, string> = { admin: "管理员", analyst: "分析员", viewer: "只读用户" };

export function AccountPage({ user }: { user: AuthUser }) {
  const [users, setUsers] = useState<PublicUser[]>([]);
  const [status, setStatus] = useState("");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [role, setRole] = useState<Role>("viewer");

  useEffect(() => {
    if (user.role !== "admin") return;
    let active = true;
    listUsers().then(
      (result) => { if (active) setUsers(result); },
      () => { if (active) setStatus("账户列表暂不可用。"); },
    );
    return () => { active = false; };
  }, [user.role]);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setStatus("");
    try {
      const created = await createUser({ username, password, role }, user.csrf_token);
      setUsers((current) => [...current, created]);
      setUsername("");
      setPassword("");
      setStatus("账户已创建。");
    } catch (cause) {
      setStatus(cause instanceof ApiError && cause.status === 409
        ? "用户名已存在。" : "创建失败，请检查输入和服务状态。");
    }
  }

  return (
    <section className="account-page">
      <div className="section-heading"><span className="eyebrow">ACCESS / 身份边界</span><h2>账户与权限</h2>
        <p>权限在 API 服务端执行，隐藏操作按钮不能代替鉴权。</p></div>
      <div className="account-panel">
        <div className="account-identity"><span className="account-avatar account-avatar--large" aria-hidden="true">{user.username.slice(0, 1).toUpperCase()}</span>
          <div><strong>{user.username}</strong><span>{roleLabels[user.role]}</span></div></div>
        {user.role === "viewer" && <p className="notice">只读账户可查看已发布指标，不能管理用户或调用分析功能。</p>}
        {user.role === "analyst" && <p className="notice">分析员可查看指标和调用受控分析，不能管理用户。</p>}
      </div>
      {user.role === "admin" && <div className="account-admin-grid">
        <section className="account-panel"><h3>现有账户</h3><ul className="user-list">
          {users.map((entry) => <li key={entry.id}><span>{entry.username}</span><small>{roleLabels[entry.role]}</small></li>)}
        </ul></section>
        <section className="account-panel"><h3>创建账户</h3><form onSubmit={submit}>
          <label htmlFor="new-username">用户名</label>
          <input id="new-username" value={username} onChange={(event) => setUsername(event.target.value)} required />
          <label htmlFor="new-password">初始密码（至少 12 位）</label>
          <input id="new-password" type="password" minLength={12} value={password} onChange={(event) => setPassword(event.target.value)} required />
          <label htmlFor="new-role">权限角色</label>
          <select id="new-role" value={role} onChange={(event) => setRole(event.target.value as Role)}>
            <option value="viewer">只读用户</option><option value="analyst">分析员</option><option value="admin">管理员</option>
          </select>
          <button type="submit" className="primary-button">创建账户</button>
        </form></section>
      </div>}
      {status && <p className="notice" role="status">{status}</p>}
    </section>
  );
}
