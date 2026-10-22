import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { GET } from "./route";

const { handle } = vi.hoisted(() => ({ handle: vi.fn() }));
vi.mock("../../../server/host", () => ({ resolveHost: () => ({ handle }), onLog: vi.fn() }));
const authorization = `Basic ${Buffer.from("test:test").toString("base64")}`;
function request(path: string, authorized = true) {
  return new Request(`http://localhost/api/queue-requests?${new URLSearchParams({ path, page: "opaque+/=" })}`, {
    headers: authorized ? { authorization } : {},
  });
}
beforeEach(() => {
  vi.stubEnv("STOVEPIPE_WEB_TOKEN", "test");
  handle.mockReset().mockResolvedValue({ kind: "render", model: { kind: "queue", requests: { ok: true, data: { requests: [], nextHref: null } } } });
});
afterEach(() => vi.unstubAllEnvs());

it("requires authentication before loading a continuation", async () => {
  const response = await GET(request("/repo%2Fmain", false));
  expect(response.status).toBe(401);
  expect(response.headers.get("www-authenticate")).toContain("Basic");
  expect(handle).not.toHaveBeenCalled();
});
it("uses the authorized page loader, preserves the opaque cursor, and disables caching", async () => {
  const response = await GET(request("/repo%2Fmain"));
  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toEqual({ requests: [], nextHref: null });
  expect(handle).toHaveBeenCalledWith(expect.objectContaining({ path: "/repo%2Fmain", search: new URLSearchParams({ page: "opaque+/=" }), signal: expect.any(AbortSignal) }));
});
it("does not load request-detail routes through the queue endpoint", async () => {
  expect((await GET(request("/repo%2Fmain/request/1"))).status).toBe(400);
  expect(handle).not.toHaveBeenCalled();
});
it.each([
  [{ kind: "not-found" }, 404],
  [{ kind: "forbidden" }, 403],
  [{ kind: "render", model: { kind: "queue", requests: { ok: false, error: "unavailable" } } }, 503],
])("preserves failed page loads (%j)", async (result, status) => {
  handle.mockResolvedValueOnce(result);
  expect((await GET(request("/repo%2Fmain"))).status).toBe(status);
});
