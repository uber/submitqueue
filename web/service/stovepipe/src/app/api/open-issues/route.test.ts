import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { GET } from "./route";

const { handle } = vi.hoisted(() => ({ handle: vi.fn() }));
vi.mock("../../../server/host", () => ({ resolveHost: () => ({ handle }), onLog: vi.fn() }));
const authorization = `Basic ${Buffer.from("test:test").toString("base64")}`;
function request(path: string, authorized = true) {
  return new Request(`http://localhost/api/open-issues?${new URLSearchParams({ path, page: "opaque+/=" })}`, {
    headers: authorized ? { authorization } : {},
  });
}
beforeEach(() => {
  vi.stubEnv("STOVEPIPE_WEB_TOKEN", "test");
  handle.mockReset().mockResolvedValue({ kind: "render", model: { kind: "issues", issues: { ok: true, data: { issues: [], nextHref: null } } } });
});
afterEach(() => vi.unstubAllEnvs());

it("authenticates before resolving the issue page", async () => {
  const response = await GET(request("/repo%2Fmain", false));
  expect(response.status).toBe(401);
  expect(handle).not.toHaveBeenCalled();
});
it("loads only the issues view, preserves the cursor, and disables caching", async () => {
  const response = await GET(request("/repo%2Fmain"));
  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toEqual({ issues: [], nextHref: null });
  expect(handle).toHaveBeenCalledWith(expect.objectContaining({
    path: "/repo%2Fmain", search: new URLSearchParams({ view: "issues", page: "opaque+/=" }), signal: expect.any(AbortSignal),
  }));
});
it("rejects request-detail paths", async () => {
  expect((await GET(request("/repo%2Fmain/request/7"))).status).toBe(400);
  expect(handle).not.toHaveBeenCalled();
});
it.each([
  [{ kind: "not-found" }, 404],
  [{ kind: "forbidden" }, 403],
  [{ kind: "render", model: { kind: "issues", issues: { ok: false, error: "unavailable" } } }, 503],
  [{ kind: "render", model: { kind: "queue" } }, 400],
])("preserves page failure status (%j)", async (result, status) => {
  handle.mockResolvedValueOnce(result);
  expect((await GET(request("/repo%2Fmain"))).status).toBe(status);
});
