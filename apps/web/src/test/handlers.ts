import { HttpResponse, http } from "msw";
import { setupServer } from "msw/node";

export const server = setupServer(
  http.get("/api/v1/auth/me", () => new HttpResponse(null, { status: 401 })),
);
