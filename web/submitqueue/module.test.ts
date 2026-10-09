import { Code, ConnectError } from "@connectrpc/connect";
import type { Meter } from "@opentelemetry/api";
import type { Logger } from "pino";
import { describe, expect, it, vi } from "vitest";
import { createSubmitQueueWeb, type SubmitQueueWebOptions } from "./module.js";
import { createFakeGatewayReader, gatewayHistoryFixture, gatewayRequestFixture } from "./extension/gateway/mock/index.js";
import type { SubmitQueueGateway } from "./extension/gateway/index.js";
import { newHmacCursorCodec } from "./extension/cursor/hmac/index.js";
import { WebPaths } from "./core/paths.js";
import { REQUEST_WINDOW_MS } from "./core/window.js";

const now = 1_700_000_000_000;
const cursors = newHmacCursorCodec("host-owned-test-secret");
const paths = new WebPaths({ basePath: "/sq" });
const sha = "0123456789abcdef0123456789abcdef01234567";
const gitUri = `git://git.example.com/demo/refs%2Fheads%2Fmain/${sha}`;
const changePath = "/sq/demo-queue/change/git/git.example.com/demo/refs%2Fheads%2Fmain";
const githubPath = "/sq/demo-queue/change/github/github.com/uber/submitqueue/pull/123";

function newWeb(options: Partial<SubmitQueueWebOptions> = {}) {
  const web = createSubmitQueueWeb({ gateway: createFakeGatewayReader(), cursors, paths, ...options });
  return (path: string, search: Record<string, string | string[]> = {}, extra: { principal?: unknown; requestId?: string } = {}) =>
    web.handle({ path, search, ...extra });
}

function recordingMeter() {
  const record = vi.fn();
  return { meter: { createHistogram: () => ({ record }) } as unknown as Meter, record };
}

function recordingLogger() {
  const warn = vi.fn();
  const error = vi.fn();
  const child = vi.fn((): Logger => logger);
  const logger = { child, debug: vi.fn(), info: vi.fn(), warn, error } as unknown as Logger;
  return { logger, child, warn, error };
}

function nextPageToken(result: Awaited<ReturnType<ReturnType<typeof newWeb>>>): string {
  if (result.kind !== "render" || result.model.kind !== "queue" || !result.model.result.ok) {
    throw new Error("Expected a queue page");
  }
  return result.model.result.data.nextPageToken!;
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
    ["an unknown page", "/sq/demo-queue/unknown/x"],
    ["malformed percent-encoding", "/sq/%E0%A4%A"],
  ])("reports not-found for %s", async (_name, path) => {
    expect(await newWeb()(path)).toEqual({ kind: "not-found" });
  });

  it("loads live pages and resumes signed snapshot cursors without moving their bounds", async () => {
    let clock = now;
    const gateway = createFakeGatewayReader({ nextPageToken: "opaque/gateway-token" });
    const list = vi.spyOn(gateway, "list");
    const get = newWeb({ gateway, now: () => clock });
    const first = await get("/sq/demo-queue");
    expect(first).toMatchObject({
      kind: "render", model: { kind: "queue", paged: false, latestHref: "/sq/demo-queue", refresh: { terminal: false } },
    });
    const page = nextPageToken(first);
    clock += 10_000;
    expect(await get("/sq/demo-queue", { page })).toMatchObject({
      kind: "render", model: {
        kind: "queue", paged: true, window: { fromMs: now - REQUEST_WINDOW_MS, toMs: now },
        refresh: { terminal: true, refreshHref: "/sq/demo-queue" },
      },
    });
    expect(list).toHaveBeenLastCalledWith(expect.objectContaining({
      pageToken: "opaque/gateway-token", receivedAtOrAfterMs: BigInt(now - REQUEST_WINDOW_MS), receivedBeforeMs: BigInt(now),
    }));
    expect(await get("/sq/demo-queue")).toMatchObject({ kind: "render", model: { window: { toMs: clock } } });
  });

  it.each([
    { from: "1", to: "2" },
    { page: ["multiple", "cursors"] },
    { page: "tampered" },
  ])("normalizes stale or invalid queue inputs to a live URL: %j", async search => {
    expect(await newWeb()("/sq/demo-queue", search)).toEqual({ kind: "redirect", href: "/sq/demo-queue" });
  });

  it("keeps request IDs opaque and provides complete history navigation and refresh policy", async () => {
    const sqid = "legacy/id%2Fvalue";
    const gateway = createFakeGatewayReader({
      summary: gatewayRequestFixture({ sqid, status: "landed" }),
      history: [gatewayHistoryFixture({ status: "landed" })],
    });
    const summary = vi.spyOn(gateway, "getRequestSummaryByID");
    const result = await newWeb({ gateway })("/sq/demo-queue/request/legacy/id%252Fvalue", { view: "history" });
    expect(summary).toHaveBeenCalledWith({ sqid, queue: "demo-queue" });
    expect(result).toMatchObject({
      kind: "render", model: {
        kind: "request", view: "history", title: `${sqid} · History`,
        historyHref: "/sq/demo-queue/request/legacy/id%252Fvalue?view=history",
        refresh: { terminal: true }, result: { ok: true, data: { request: { sqid } } },
      },
    });
  });

  it("accepts asynchronous cursor infrastructure without putting promises in page URLs", async () => {
    const get = newWeb({
      gateway: createFakeGatewayReader({ nextPageToken: "gateway-continuation" }),
      cursors: {
        encode: async (...args) => cursors.encode(...args),
        decode: async (...args) => cursors.decode(...args),
      },
      now: () => now,
    });
    const page = nextPageToken(await get("/sq/demo-queue"));
    expect(typeof page).toBe("string");
    expect(await get("/sq/demo-queue", { page })).toMatchObject({ kind: "render", model: { paged: true, window: { toMs: now } } });
  });

  it("returns explicit not-found decisions without exceptions for absent resources", async () => {
    const get = newWeb();
    expect(await get("/sq/unknown")).toEqual({ kind: "not-found" });
    expect(await get("/sq/demo-queue/change/unknown")).toEqual({ kind: "not-found" });
  });

  it("keeps catalog failures visible rather than declaring a configured queue missing", async () => {
    const gateway = createFakeGatewayReader({ queuesError: new ConnectError("upstream failure", Code.Unavailable) });
    const list = vi.spyOn(gateway, "list");
    expect(await newWeb({ gateway })("/sq/demo-queue")).toMatchObject({
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

  it("scopes change continuation to queue, reference, and version while preserving the snapshot", async () => {
    const gateway = createFakeGatewayReader({ requests: [gatewayRequestFixture({ changeUris: [gitUri] })] });
    const list = vi.spyOn(gateway, "list").mockImplementation(async input => {
      const page = input.pageToken ? Number(input.pageToken.slice(5)) : 0;
      return { requests: [gatewayRequestFixture({ sqid: String(page + 100), changeUris: [gitUri] })], nextPageToken: page < 10 ? `page-${page + 1}` : "" };
    });
    const get = newWeb({ gateway, now: () => now });
    const first = await get(changePath);
    if (first.kind !== "render" || first.model.kind !== "change" || !first.model.result.ok) {
      throw new Error("Expected a change page");
    }
    const page = new URL(first.model.result.data.pagination!.nextHref!, "http://host.invalid").searchParams.get("page")!;
    expect(first.model.result.data.submissions[0]?.versionHref).toBe(`${changePath}/${sha}`);
    expect(await get(`${changePath}/${sha}`, { page })).toEqual({ kind: "redirect", href: `${changePath}/${sha}` });
    expect(list).toHaveBeenCalledTimes(10);
    expect(await get(changePath, { page })).toMatchObject({
      kind: "render",
      model: { refresh: { terminal: true, refreshHref: changePath }, result: { ok: true, data: { pagination: { nextHref: null } } } },
    });
    expect(list).toHaveBeenLastCalledWith(expect.objectContaining({
      pageToken: "page-10", receivedAtOrAfterMs: BigInt(now - REQUEST_WINDOW_MS), receivedBeforeMs: BigInt(now),
    }));
  });

  it("refuses a queue-page cursor on a change page", async () => {
    const get = newWeb({
      gateway: createFakeGatewayReader({ nextPageToken: "gateway-continuation", requests: [gatewayRequestFixture({ changeUris: [gitUri] })] }),
      now: () => now,
    });
    const queuePageHref = paths.requests("demo-queue", { pageToken: nextPageToken(await get("/sq/demo-queue")) });
    const page = new URL(queuePageHref, "https://host.invalid").searchParams.get("page")!;
    expect(await get(changePath, { page })).toEqual({ kind: "redirect", href: changePath });
  });

  it("resolves the gateway per request, so authorization-dependent visibility is never cached", async () => {
    let gateway: SubmitQueueGateway = createFakeGatewayReader({ queues: [{ name: "first" }], requests: [] });
    const resolve = vi.fn(() => gateway);
    const get = newWeb({ gateway: resolve });
    expect(await get("/sq/first", {}, { principal: "viewer" })).toMatchObject({ kind: "render", model: { result: { ok: true } } });
    expect(resolve).toHaveBeenCalledWith(expect.objectContaining({ principal: "viewer" }));
    gateway = createFakeGatewayReader({ queues: [{ name: "second" }], requests: [] });
    expect(await get("/sq/first")).toEqual({ kind: "not-found" });
    expect(await get("/sq/second")).toMatchObject({ kind: "render", model: { result: { ok: true } } });
  });

  it("reports forbidden without reading the gateway when authorization fails", async () => {
    const gateway = createFakeGatewayReader();
    const listQueues = vi.spyOn(gateway, "listQueues");
    const authorize = vi.fn(() => false);
    expect(await newWeb({ gateway, authorize })("/sq/demo-queue", {}, { principal: "viewer" })).toEqual({ kind: "forbidden" });
    expect(authorize).toHaveBeenCalledWith("viewer", { kind: "queue", queue: "demo-queue", search: {} });
    expect(listQueues).not.toHaveBeenCalled();
  });

  it("logs upstream failures with the request ID while showing a sanitized error", async () => {
    const cause = new ConnectError("upstream failure", Code.Unavailable);
    const { logger, child, warn } = recordingLogger();
    const get = newWeb({ gateway: createFakeGatewayReader({ queuesError: cause }), logger });
    expect(await get("/sq", {}, { requestId: "request-7" })).toMatchObject({
      kind: "render", model: { result: { ok: false, error: { kind: "transient" } } },
    });
    expect(child).toHaveBeenCalledWith({ requestId: "request-7" });
    expect(warn).toHaveBeenCalledWith(expect.objectContaining({ operation: "queues", err: cause, code: "unavailable" }), expect.any(String));
  });

  it("records each page load's outcome", async () => {
    const { meter, record } = recordingMeter();
    await newWeb({ meter })("/sq/demo-queue", { page: "tampered" });
    expect(record).toHaveBeenCalledWith(expect.any(Number), { page: "queue", outcome: "redirect" });
  });

  it("logs, records, and rethrows an unexpected failure", async () => {
    const failure = new Error("bug");
    const { logger, error } = recordingLogger();
    const { meter, record } = recordingMeter();
    const get = newWeb({ gateway: () => { throw failure; }, logger, meter });
    await expect(get("/sq")).rejects.toBe(failure);
    expect(error).toHaveBeenCalledWith(expect.objectContaining({ err: failure }), "Web page load failed");
    expect(record).toHaveBeenCalledWith(expect.any(Number), { page: "queues", outcome: "exception" });
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
