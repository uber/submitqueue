import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { GET } from "./route";

const { queueRequests } = vi.hoisted(() => ({ queueRequests: vi.fn() }));
vi.mock("../../../../../server/host", () => ({ resolveReferenceWeb: () => ({ queueRequests }) }));
vi.mock("../../../../../server/observability", () => ({ resolveReferenceLogCallback: () => vi.fn() }));
const token = "demo-token-with-at-least-thirty-two-characters";
const authorization = `Basic ${Buffer.from(`test:${token}`).toString("base64")}`;
function request(target: string, authorized = true) {
  return new Request(`http://localhost${target}`, { headers: authorized ? { authorization } : {} });
}
beforeEach(() => {
  vi.stubEnv("SUBMITQUEUE_WEB_TOKEN", token);
  queueRequests.mockReset().mockResolvedValue({ kind: "ok", page: { requests: [], nextHref: null } });
});
afterEach(() => vi.unstubAllEnvs());

it("requires authentication before reading", async () => {
  const response = await GET(request("/api/queue/demo-queue/requests", false));
  expect(response.status).toBe(401);
  expect(response.headers.get("www-authenticate")).toContain("Basic");
  expect(queueRequests).not.toHaveBeenCalled();
});

it("reads the decoded queue with its statuses and opaque cursor, uncached", async () => {
  const response = await GET(request(`/api/queue/uber-code%2Fsqv2-dummy/requests?status=error&status=landed&page=${encodeURIComponent("opaque+/=")}`));
  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toEqual({ requests: [], nextHref: null });
  expect(queueRequests).toHaveBeenCalledWith(expect.objectContaining({
    queue: "uber-code/sqv2-dummy", search: { page: "opaque+/=", status: ["error", "landed"] }, signal: expect.any(AbortSignal),
  }));
});

it("rejects a malformed queue segment without reading", async () => {
  expect((await GET(request("/api/queue/demo%E0%A4%A/requests"))).status).toBe(400);
  expect(queueRequests).not.toHaveBeenCalled();
});

it.each([
  [{ kind: "not-found" }, 404],
  [{ kind: "forbidden" }, 403],
  [{ kind: "invalid" }, 400],
  [{ kind: "unavailable", error: { kind: "transient" } }, 503],
])("maps %j to its status", async (result, status) => {
  queueRequests.mockResolvedValueOnce(result);
  expect((await GET(request("/api/queue/demo-queue/requests"))).status).toBe(status);
});
