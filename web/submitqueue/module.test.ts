import { Code, ConnectError } from "@connectrpc/connect";
import { describe, expect, it, vi } from "vitest";
import { createSubmitQueueWeb, type SubmitQueueWebOptions, type SubmitQueueWebRequest } from "./module.js";
import { createFakeGatewayReader, gatewayHistoryFixture, gatewayRequestFixture } from "./extension/gateway/mock/index.js";
import type { SubmitQueueGateway } from "./extension/gateway/index.js";
import { WebPaths } from "./core/paths.js";
import { REQUEST_WINDOW_MS } from "./core/window.js";

const now = 1_700_000_000_000;
const paths = new WebPaths({ basePath: "/sq" });
const sha = "0123456789abcdef0123456789abcdef01234567";
const gitUri = `git://git.example.com/demo/refs%2Fheads%2Fmain/${sha}`;
const changePath = "/sq/queue/demo-queue/change/git/git.example.com/demo/refs%2Fheads%2Fmain";
const githubPath = "/sq/queue/demo-queue/change/github/github.com/uber/submitqueue/pull/123";

function newWeb(options: Partial<SubmitQueueWebOptions> = {}) {
  const web = createSubmitQueueWeb({ gateway: createFakeGatewayReader(), paths, ...options });
  return (path: string, search: Record<string, string | string[]> = {}, extra: Pick<SubmitQueueWebRequest, "principal" | "requestId" | "onLog"> = {}) =>
    web.handle({ path, search, ...extra });
}

function nextHref(result: Awaited<ReturnType<ReturnType<typeof newWeb>>>): string {
  if (result.kind !== "render" || result.model.kind !== "queue" || !result.model.result.ok) {
    throw new Error("Expected a queue page");
  }
  return result.model.result.data.nextHref!;
}

describe("createSubmitQueueWeb", () => {
  it("loads configured queues and emits a serializable page without transport/framework setup", async () => {
    const get = newWeb({ gateway: createFakeGatewayReader({ queues: [{ name: "empty-queue" }, { name: "demo-queue" }] }) });
    const result = await get("/sq");
    expect(result).toMatchObject({
      kind: "render",
      model: { kind: "queues", title: "Queues", basePath: "/sq", result: { ok: true, data: [{ name: "empty-queue" }, { name: "demo-queue" }] } },
    });
    expect(() => JSON.stringify(result)).not.toThrow();
  });

  it.each([
    ["outside the mount", "/elsewhere"],
    ["an unknown page", "/sq/queue/demo-queue/unknown/x"],
    ["malformed percent-encoding", "/sq/queue/%E0%A4%A"],
  ])("reports not-found for %s", async (_name, path) => {
    expect(await newWeb()(path)).toEqual({ kind: "not-found" });
  });

  it("starts at the newest request however old, links the raw gateway token, and resumes it without resending bounds", async () => {
    let clock = now;
    const gateway = createFakeGatewayReader({
      nextPageToken: "opaque/gateway-token", window: { fromMs: 0n, toMs: BigInt(now) },
    });
    const list = vi.spyOn(gateway, "list");
    const get = newWeb({ gateway, now: () => clock });
    const first = await get("/sq/queue/demo-queue");
    expect(first).toMatchObject({
      kind: "render", model: { kind: "queue", paged: false, latestHref: "/sq/queue/demo-queue", refresh: { terminal: false } },
    });
    expect(list).toHaveBeenLastCalledWith(expect.objectContaining({
      receivedLowerBound: { case: "receivedAtOrAfterMs", value: 0n },
      receivedUpperBound: { case: "receivedBeforeMs", value: BigInt(now) },
    }));
    const href = nextHref(first);
    expect(href).toBe("/sq/queue/demo-queue?page=opaque%2Fgateway-token");
    clock += 10_000;
    const page = new URL(href, "http://host.invalid").searchParams.get("page")!;
    expect(await get("/sq/queue/demo-queue", { page })).toMatchObject({
      kind: "render", model: { kind: "queue", paged: true, refresh: { terminal: true, refreshHref: "/sq/queue/demo-queue" } },
    });
    expect(list).toHaveBeenLastCalledWith({ queue: "demo-queue", pageSize: 50, pageToken: "opaque/gateway-token" });
    await get("/sq/queue/demo-queue");
    expect(list).toHaveBeenLastCalledWith(expect.objectContaining({
      receivedUpperBound: { case: "receivedBeforeMs", value: BigInt(clock) },
    }));
  });

  it("restricts the list to sorted statuses, keeps them on continuation links, and canonicalizes the URL", async () => {
    const gateway = createFakeGatewayReader({ nextPageToken: "next" });
    const list = vi.spyOn(gateway, "list");
    const get = newWeb({ gateway, now: () => now });
    expect(await get("/sq/queue/demo-queue", { status: ["landed", "error", "landed"] }))
      .toEqual({ kind: "redirect", href: "/sq/queue/demo-queue?status=error&status=landed" });
    const first = await get("/sq/queue/demo-queue", { status: ["error", "landed"] });
    expect(first).toMatchObject({
      kind: "render", model: { kind: "queue", statuses: ["error", "landed"], latestHref: "/sq/queue/demo-queue?status=error&status=landed" },
    });
    expect(list).toHaveBeenLastCalledWith(expect.objectContaining({ statuses: ["error", "landed"], pageToken: "" }));
    expect(nextHref(first)).toBe("/sq/queue/demo-queue?status=error&status=landed&page=next");
    await get("/sq/queue/demo-queue", { status: ["error", "landed"], page: "next" });
    expect(list).toHaveBeenLastCalledWith({ queue: "demo-queue", pageSize: 50, pageToken: "next", statuses: ["error", "landed"] });
    expect(await get("/sq/queue/demo-queue", { status: "" })).toEqual({ kind: "redirect", href: "/sq/queue/demo-queue" });
  });

  it.each([
    { from: "1", to: "2" },
    { page: ["multiple", "tokens"] },
    { page: "" },
  ])("normalizes stale or invalid queue inputs to a live URL: %j", async search => {
    expect(await newWeb()("/sq/queue/demo-queue", search)).toEqual({ kind: "redirect", href: "/sq/queue/demo-queue" });
  });

  it.each([
    ["queue", "/sq/queue/demo-queue"],
    ["change", changePath],
  ])("restarts a %s page from the live window when the gateway rejects its token", async (_name, path) => {
    const gateway = createFakeGatewayReader({ listError: new ConnectError("bad token", Code.InvalidArgument) });
    expect(await newWeb({ gateway })(path, { page: "tampered" })).toEqual({ kind: "redirect", href: path });
  });

  it("keeps request IDs opaque and provides complete history navigation and refresh policy", async () => {
    const sqid = "legacy/id%2Fvalue";
    const gateway = createFakeGatewayReader({
      summary: gatewayRequestFixture({ sqid, status: "landed" }),
      history: [gatewayHistoryFixture({ status: "landed" })],
    });
    const summary = vi.spyOn(gateway, "getRequestSummaryByID");
    const result = await newWeb({ gateway })("/sq/queue/demo-queue/request/legacy/id%252Fvalue", { view: "history" });
    expect(summary).toHaveBeenCalledWith({ sqid, queue: "demo-queue" });
    expect(result).toMatchObject({
      kind: "render", model: {
        kind: "request", view: "history", title: `${sqid} · History`,
        historyHref: "/sq/queue/demo-queue/request/legacy/id%252Fvalue?view=history",
        refresh: { terminal: true }, result: { ok: true, data: { request: { sqid } } },
      },
    });
  });

  it("returns explicit not-found decisions without exceptions for absent resources", async () => {
    const get = newWeb();
    expect(await get("/sq/queue/unknown")).toEqual({ kind: "not-found" });
    expect(await get("/sq/queue/demo-queue/change/unknown")).toEqual({ kind: "not-found" });
  });

  it("keeps catalog failures visible rather than declaring a configured queue missing", async () => {
    const gateway = createFakeGatewayReader({ queuesError: new ConnectError("upstream failure", Code.Unavailable) });
    const list = vi.spyOn(gateway, "list");
    expect(await newWeb({ gateway })("/sq/queue/demo-queue")).toMatchObject({
      kind: "render", model: { result: { ok: false, error: { kind: "transient" } }, refresh: { terminal: false } },
    });
    expect(list).not.toHaveBeenCalled();
  });

  it.each([
    ["a logical GitHub change, which scans the receipt window", githubPath, true],
    ["a Git ref, which scans the receipt window", changePath, true],
    ["a pinned GitHub version, which is one exact lookup", `${githubPath}/${sha}`, false],
  ])("refresh policy for %s (terminal: %s)", async (_name, path, terminal) => {
    expect(await newWeb()(path)).toMatchObject({ kind: "render", model: { refresh: { terminal } } });
  });

  it("loads pinned reviews through exact-URI lookup and preserves base-path change links", async () => {
    const uri = `github://github.com/uber/submitqueue/pull/123/${sha}`;
    const gateway = createFakeGatewayReader({ changeRequests: [gatewayRequestFixture({ changeUris: [uri] })] });
    const read = vi.spyOn(gateway, "getRequestSummaryByChangeURI");
    const list = vi.spyOn(gateway, "list");
    const result = await newWeb({ gateway })(`${githubPath}/${sha}`);
    expect(read).toHaveBeenCalledWith({ queue: "demo-queue", changeUri: uri });
    expect(list).not.toHaveBeenCalled();
    expect(result).toMatchObject({
      kind: "render", model: { result: { ok: true, data: { window: null, submissions: [{ versionHref: `${githubPath}/${sha}` }] } } },
    });
  });

  it("continues a change scan from the raw gateway token and shows the token's window", async () => {
    const gateway = createFakeGatewayReader({ requests: [gatewayRequestFixture({ changeUris: [gitUri] })] });
    const list = vi.spyOn(gateway, "list").mockImplementation(async input => {
      const page = input.pageToken ? Number(input.pageToken.slice(5)) : 0;
      return {
        requests: [gatewayRequestFixture({ sqid: String(page + 100), changeUris: [gitUri] })], nextPageToken: page < 10 ? `page-${page + 1}` : "",
        receivedAtOrAfterMs: BigInt(now - REQUEST_WINDOW_MS), receivedBeforeMs: BigInt(now),
      };
    });
    const get = newWeb({ gateway, now: () => now });
    const first = await get(changePath);
    if (first.kind !== "render" || first.model.kind !== "change" || !first.model.result.ok) {
      throw new Error("Expected a change page");
    }
    const page = new URL(first.model.result.data.pagination!.nextHref!, "http://host.invalid").searchParams.get("page")!;
    expect(page).toBe("page-10");
    expect(first.model.result.data.submissions[0]?.versionHref).toBe(`${changePath}/${sha}`);
    expect(list).toHaveBeenCalledTimes(10);
    expect(await newWeb({ gateway, now: () => now + 10_000 })(changePath, { page })).toMatchObject({
      kind: "render",
      model: {
        refresh: { terminal: true, refreshHref: changePath },
        result: { ok: true, data: { window: { fromMs: now - REQUEST_WINDOW_MS, toMs: now }, pagination: { nextHref: null } } },
      },
    });
    expect(list).toHaveBeenLastCalledWith({ queue: "demo-queue", pageSize: 50, pageToken: "page-10" });
  });

  it("resolves the gateway per request, so authorization-dependent visibility is never cached", async () => {
    let gateway: SubmitQueueGateway = createFakeGatewayReader({ queues: [{ name: "first" }], requests: [] });
    const resolve = vi.fn(() => gateway);
    const get = newWeb({ gateway: resolve });
    expect(await get("/sq/queue/first", {}, { principal: "viewer" })).toMatchObject({ kind: "render", model: { result: { ok: true } } });
    expect(resolve).toHaveBeenCalledWith(expect.objectContaining({ principal: "viewer" }));
    gateway = createFakeGatewayReader({ queues: [{ name: "second" }], requests: [] });
    expect(await get("/sq/queue/first")).toEqual({ kind: "not-found" });
    expect(await get("/sq/queue/second")).toMatchObject({ kind: "render", model: { result: { ok: true } } });
  });

  it("reports forbidden without reading the gateway when authorization fails", async () => {
    const gateway = createFakeGatewayReader();
    const listQueues = vi.spyOn(gateway, "listQueues");
    const authorize = vi.fn(() => false);
    expect(await newWeb({ gateway, authorize })("/sq/queue/demo-queue", {}, { principal: "viewer" })).toEqual({ kind: "forbidden" });
    expect(authorize).toHaveBeenCalledWith("viewer", { kind: "queue", queue: "demo-queue", search: {} });
    expect(listQueues).not.toHaveBeenCalled();
  });

  it("logs upstream failures with the request ID while showing a sanitized error", async () => {
    const cause = new ConnectError("upstream failure", Code.Unavailable);
    const onLog = vi.fn();
    const get = newWeb({ gateway: createFakeGatewayReader({ queuesError: cause }) });
    expect(await get("/sq", {}, { requestId: "request-7", onLog })).toMatchObject({
      kind: "render", model: { result: { ok: false, error: { kind: "transient" } } },
    });
    expect(onLog).toHaveBeenCalledWith("warn", expect.any(String), expect.objectContaining({ requestId: "request-7", operation: "queues", error: cause, code: "unavailable" }));
  });

  it("keeps overlapping loads bound to their own host logging callbacks", async () => {
    let release!: () => void;
    const ready = new Promise<void>(resolve => { release = resolve; });
    const cause = new ConnectError("private upstream detail", Code.Unavailable);
    const gateway = createFakeGatewayReader();
    vi.spyOn(gateway, "listQueues").mockImplementation(async () => {
      await ready;
      throw cause;
    });
    const first = vi.fn();
    const second = vi.fn();
    const get = newWeb({ gateway });
    const pending = [
      get("/sq", {}, { requestId: "first", onLog: first }),
      get("/sq", {}, { requestId: "second", onLog: second }),
    ];
    release();
    const results = await Promise.all(pending);

    expect(first).toHaveBeenCalledOnce();
    expect(second).toHaveBeenCalledOnce();
    expect(first).toHaveBeenCalledWith("warn", expect.any(String), expect.objectContaining({ requestId: "first", error: cause }));
    expect(second).toHaveBeenCalledWith("warn", expect.any(String), expect.objectContaining({ requestId: "second", error: cause }));
    expect(JSON.stringify(results)).not.toContain("private upstream detail");
  });

  it("reports authorization decisions through the current request's callback", async () => {
    const onLog = vi.fn();
    const get = newWeb({ authorize: () => false });
    expect(await get("/sq", {}, { requestId: "denied", onLog })).toEqual({ kind: "forbidden" });
    expect(onLog).toHaveBeenCalledWith("info", "Web page request was not authorized", { requestId: "denied", path: "/sq" });
  });

  it("logs and rethrows an unexpected failure", async () => {
    const failure = new Error("bug");
    const onLog = vi.fn();
    const get = newWeb({ gateway: () => { throw failure; } });
    await expect(get("/sq", {}, { onLog })).rejects.toBe(failure);
    expect(onLog).toHaveBeenCalledWith("error", "Web page load failed", expect.objectContaining({ error: failure }));
  });

  it("caches successful catalogs per key until they expire", async () => {
    let clock = now;
    const gateway = createFakeGatewayReader();
    const listQueues = vi.spyOn(gateway, "listQueues");
    const get = newWeb({ gateway, now: () => clock, queuesCache: { ttlMs: 1_000, key: context => String(context.principal) } });
    await get("/sq", {}, { principal: "a" });
    await get("/sq", {}, { principal: "a" });
    expect(listQueues).toHaveBeenCalledTimes(1);
    await get("/sq", {}, { principal: "b" });
    expect(listQueues).toHaveBeenCalledTimes(2);
    clock += 1_001;
    await get("/sq", {}, { principal: "a" });
    expect(listQueues).toHaveBeenCalledTimes(3);
  });

  it("does not cache a failed catalog", async () => {
    const gateway = createFakeGatewayReader({ queuesError: new ConnectError("down", Code.Unavailable) });
    const listQueues = vi.spyOn(gateway, "listQueues");
    const get = newWeb({ gateway, queuesCache: { ttlMs: 60_000, key: () => "all" } });
    await get("/sq");
    await get("/sq");
    expect(listQueues).toHaveBeenCalledTimes(2);
  });
});
