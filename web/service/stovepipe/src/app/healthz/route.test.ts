import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { GET } from "./route";

const { ping } = vi.hoisted(() => ({ ping: vi.fn() }));
vi.mock("@connectrpc/connect", () => ({ createClient: () => ({ ping }) }));

beforeEach(() => {
  vi.stubEnv("STOVEPIPE_URL", "http://localhost:8080");
  vi.stubEnv("STOVEPIPE_ALLOW_PLAINTEXT", "true");
  vi.stubEnv("STOVEPIPE_WEB_QUEUES", '[{"name":"repo/main","projects":["example"]}]');
  vi.stubEnv("STOVEPIPE_WEB_TOKEN", "test");
  ping.mockReset().mockResolvedValue({});
});
afterEach(() => vi.unstubAllEnvs());

describe("readiness", () => {
  it("is ready only when configuration and the backend are available", async () => {
    const response = await GET();
    expect(response.status).toBe(200);
    expect(response.headers.get("cache-control")).toBe("no-store");
    ping.mockRejectedValueOnce(new Error("unavailable"));
    expect((await GET()).status).toBe(503);
  });

  it.each([undefined, ""])("rejects missing or empty auth configuration (%s)", async token => {
    vi.stubEnv("STOVEPIPE_WEB_TOKEN", token);
    expect((await GET()).status).toBe(503);
    expect(ping).not.toHaveBeenCalled();
  });
});
