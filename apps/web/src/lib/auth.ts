import { jsonRequest } from "./http";

export type Role = "admin" | "analyst" | "viewer";

export interface AuthUser {
  id: number;
  username: string;
  role: Role;
  csrf_token: string;
}

export type AuthSession = AuthUser;
export type PublicUser = Pick<AuthUser, "id" | "username" | "role">;

export const getSession = () => jsonRequest<AuthSession>("/api/v1/auth/me");

export const loginUser = (username: string, password: string) =>
  jsonRequest<AuthSession>("/api/v1/auth/login", {
    method: "POST",
    body: JSON.stringify({ username, password }),
  });

export const logoutUser = (csrfToken: string) =>
  jsonRequest<{ status: string }>("/api/v1/auth/logout", {
    method: "POST",
    headers: { "X-CSRF-Token": csrfToken },
  });

export const listUsers = () => jsonRequest<PublicUser[]>("/api/v1/auth/users");

export const createUser = (
  body: { username: string; password: string; role: Role },
  csrfToken: string,
) =>
  jsonRequest<PublicUser>("/api/v1/auth/users", {
    method: "POST",
    headers: { "X-CSRF-Token": csrfToken },
    body: JSON.stringify(body),
  });
