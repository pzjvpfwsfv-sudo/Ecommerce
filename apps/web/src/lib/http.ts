export class ApiError extends Error {
  constructor(public readonly status: number, message: string) {
    super(message);
    this.name = "ApiError";
  }
}

export async function jsonRequest<T>(path: string, init: RequestInit = {}): Promise<T> {
  if (!path.startsWith("/") || new URL(path, window.location.origin).origin !== window.location.origin) {
    throw new Error("API path must be same-origin");
  }
  const headers = new Headers(init.headers);
  if (!headers.has("Accept")) headers.set("Accept", "application/json");
  if (init.body && !(init.body instanceof FormData) && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  const response = await fetch(path, {
    ...init,
    credentials: "same-origin",
    headers,
  });
  if (!response.ok) {
    if (response.status === 401 && path !== "/api/v1/auth/me") {
      window.dispatchEvent(new Event("g3:unauthorized"));
    }
    throw new ApiError(response.status, `HTTP ${response.status}`);
  }
  try {
    return (await response.json()) as T;
  } catch {
    throw new ApiError(502, "API 返回了无效 JSON");
  }
}
