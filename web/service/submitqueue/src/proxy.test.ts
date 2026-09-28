import { afterEach, describe, expect, it, vi } from "vitest";

import { BASIC_AUTH_USERNAME } from "./server/auth";
import { proxy } from "./proxy";

const token = "demo-token-with-at-least-thirty-two-characters";

function request(authorization?: string) {
  return {
    headers: new Headers(
      authorization ? { authorization } : undefined,
    ),
  } as Parameters<typeof proxy>[0];
}

function basic(username: string, password: string): string {
  return `Basic ${Buffer.from(`${username}:${password}`).toString("base64")}`;
}

describe("proxy", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("challenges an anonymous browser", () => {
    vi.stubEnv("SUBMITQUEUE_WEB_TOKEN", token);
    const response = proxy(request());

    expect(response?.status).toBe(401);
    expect(response?.headers.get("www-authenticate")).toMatch(/^Basic /u);
    expect(response?.headers.get("cache-control")).toBe("no-store");
  });

  it("allows valid demo credentials", () => {
    vi.stubEnv("SUBMITQUEUE_WEB_TOKEN", token);
    expect(proxy(request(basic(BASIC_AUTH_USERNAME, token)))).toBeUndefined();
  });

  it("challenges an incorrect password", () => {
    vi.stubEnv("SUBMITQUEUE_WEB_TOKEN", token);
    expect(
      proxy(request(basic(BASIC_AUTH_USERNAME, "incorrect-password-value-000000")))
        ?.status,
    ).toBe(401);
  });
});
