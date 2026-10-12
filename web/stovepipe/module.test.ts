import { describe, expect, it, vi } from "vitest";
import { createStovepipeWeb } from "./module.js";
import { StovepipePaths } from "./core/paths.js";
import type {
  StovepipeService,
  RequestSummary,
} from "./extension/service/index.js";

const summary: RequestSummary = {
  requestId: "9007199254740993",
  queue: "repo/main",
  changeUri: "git://repo/main/sha?x=1&file=src%2Fmain#fragment",
  baseUri: "git://repo/main/base",
  requestState: "future_state",
  stateUpdatedAtMs: 9007199254740993n,
  acceptedAtMs: 100n,
  outcomeReason: "future_reason",
};
function createPageTestContext() {
  const service: StovepipeService = {
    list: vi
      .fn()
      .mockResolvedValue({ requests: [], nextPageToken: "older+/page" }),
    getRequestHistoryByID: vi.fn().mockResolvedValue({
      requestSummary: summary,
      events: [
        {
          eventId: "e1",
          timestampMs: 100n,
          occurrence: { case: "event", value: "future_event" },
          outcomeReason: "event_reason",
        },
      ],
    }),
  };
  const paths = new StovepipePaths("/stovepipe");
  const app = createStovepipeWeb({
    service,
    paths,
    queues: [{ name: "repo/main" }],
  });
  return { service, paths, app };
}
describe("Stovepipe page loading", () => {
  it("keeps overlapping loads bound to their own host logging callbacks", async () => {
    const { app, service, paths } = createPageTestContext();
    let release!: () => void;
    const ready = new Promise<void>((resolve) => {
      release = resolve;
    });
    const error = new Error("private upstream detail");
    vi.mocked(service.list).mockImplementation(async () => {
      await ready;
      throw error;
    });
    const first = vi.fn();
    const second = vi.fn();
    const path = paths.queue("repo/main");
    const pending = [
      app.handle({
        path, search: new URLSearchParams(),
        requestId: "first", onLog: first,
      }),
      app.handle({
        path, search: new URLSearchParams(),
        requestId: "second", onLog: second,
      }),
    ];
    release();
    const results = await Promise.all(pending);

    expect(first).toHaveBeenCalledOnce();
    expect(second).toHaveBeenCalledOnce();
    expect(first).toHaveBeenCalledWith("warn", "Stovepipe page section failed", {
      requestId: "first", path, section: "queue requests", error,
    });
    expect(second).toHaveBeenCalledWith("warn", "Stovepipe page section failed", {
      requestId: "second", path, section: "queue requests", error,
    });
    expect(JSON.stringify(results)).not.toContain("private upstream detail");
  });

  it("retains the page result when log delivery throws", async () => {
    const { app, service, paths } = createPageTestContext();
    vi.mocked(service.list).mockRejectedValue(new Error("private upstream detail"));
    const result = await app.handle({
      path: paths.queue("repo/main"), search: new URLSearchParams(),
      onLog: () => {
        throw new Error("logging unavailable");
      },
    });
    expect(result).toMatchObject({ kind: "render", model: { requests: { ok: false } } });
    expect(JSON.stringify(result)).not.toContain("private upstream detail");
  });

  it("reports and rethrows unexpected page failures without replacing the cause", async () => {
    const { service, paths } = createPageTestContext();
    const error = new Error("invalid host configuration");
    const onLog = vi.fn(() => {
      throw new Error("logging unavailable");
    });
    const app = createStovepipeWeb({
      service: () => { throw error; },
      paths,
      queues: [{ name: "repo/main" }],
    });
    const path = paths.queue("repo/main");
    await expect(app.handle({
      path, search: new URLSearchParams(), requestId: "failed", onLog,
    })).rejects.toBe(error);
    expect(onLog).toHaveBeenCalledWith("error", "Web page load failed", {
      requestId: "failed", path, error,
    });
  });
  it("uses backend queue cursors and links each request by its ID", async () => {
    const { service, app, paths } = createPageTestContext();
    vi.mocked(service.list).mockResolvedValue({
      requests: [summary, { ...summary, requestId: "7" }],
      nextPageToken: "older+/page",
    });
    const result = await app.handle({
      path: paths.queue("repo/main"),
      search: new URLSearchParams({ page: "opaque+/cursor" }),
    });
    expect(service.list).toHaveBeenCalledWith(
      { queue: "repo/main", pageSize: 50, pageToken: "opaque+/cursor" },
      {},
    );
    if (
      result.kind !== "render" ||
      result.model.kind !== "queue" ||
      !result.model.requests.ok
    )
      throw new Error("No queue page");
    expect(result.model.requests.data.nextHref).toBe(
      paths.queue("repo/main", "older+/page"),
    );
    expect(
      result.model.requests.data.requests.map((request) => request.href),
    ).toEqual([
      paths.request("repo/main", summary.requestId),
      paths.request("repo/main", "7"),
    ]);
    expect(service.getRequestHistoryByID).not.toHaveBeenCalled();
  });
  it("loads the current summary and events by ID without reconstructing state from history", async () => {
    const { app, service, paths } = createPageTestContext();
    const signal = new AbortController().signal;
    const result = await app.handle({
      path: paths.request("repo/main", summary.requestId),
      search: new URLSearchParams(),
      signal,
    });
    expect(service.getRequestHistoryByID).toHaveBeenCalledWith(
      { queue: "repo/main", requestId: summary.requestId },
      { signal },
    );
    expect(result).toMatchObject({
      kind: "render",
      model: {
        kind: "request",
        requestId: summary.requestId,
        history: {
          ok: true,
          data: {
            request: {
              id: summary.requestId,
              changeUri: summary.changeUri,
              baseUri: summary.baseUri,
              state: "future_state",
              updatedAtMs: "9007199254740993",
              acceptedAtMs: "100",
              outcomeReason: "future_reason",
            },
            events: [{ label: "future_event", outcomeReason: "event_reason" }],
          },
        },
      },
    });
    expect(service.list).not.toHaveBeenCalled();
    expect(() => JSON.stringify(result)).not.toThrow();
  });
  it("retains the current summary when there are no retained events", async () => {
    const { app, service, paths } = createPageTestContext();
    vi.mocked(service.getRequestHistoryByID).mockResolvedValue({
      requestSummary: summary,
      events: [],
    });
    const result = await app.handle({
      path: paths.request("repo/main", summary.requestId),
      search: new URLSearchParams(),
    });
    expect(result).toMatchObject({
      kind: "render",
      model: {
        history: {
          ok: true,
          data: { request: { state: "future_state" }, events: [] },
        },
      },
    });
  });
  it.each([
    { name: "missing summary", requestSummary: undefined },
    { name: "another request", requestSummary: { ...summary, requestId: "7" } },
    { name: "another queue", requestSummary: { ...summary, queue: "other" } },
  ])("rejects history with $name", async ({ requestSummary }) => {
    const { app, service, paths } = createPageTestContext();
    vi.mocked(service.getRequestHistoryByID).mockResolvedValue({
      requestSummary,
      events: [],
    });
    const result = await app.handle({
      path: paths.request("repo/main", summary.requestId),
      search: new URLSearchParams(),
    });
    expect(result).toMatchObject({
      kind: "render",
      model: { history: { ok: false } },
    });
  });
  it("renders a section error if history is unavailable", async () => {
    const { app, service, paths } = createPageTestContext();
    vi.mocked(service.getRequestHistoryByID).mockRejectedValue(
      new Error("unavailable"),
    );
    const result = await app.handle({
      path: paths.request("repo/main", summary.requestId),
      search: new URLSearchParams(),
    });
    expect(result).toMatchObject({
      kind: "render",
      model: { requestId: summary.requestId, history: { ok: false } },
    });
  });
  it("blocks unauthorized loads before calling the service", async () => {
    const { service, paths } = createPageTestContext();
    const app = createStovepipeWeb({
      service,
      paths,
      queues: [{ name: "repo/main" }],
      authorize: () => false,
    });
    const result = await app.handle({
      path: paths.request("repo/main", summary.requestId),
      search: new URLSearchParams(),
    });
    expect(result).toEqual({ kind: "forbidden" });
    expect(service.getRequestHistoryByID).not.toHaveBeenCalled();
  });
  it.each([
    "/other",
    "/stovepipe/%",
    "/stovepipe/unknown",
    "/stovepipe/repo%2Fmain/change/uri",
    "/stovepipe/repo%2Fmain/change/uri/projects.json",
    "/stovepipe/repo%2Fmain/request/",
    "/stovepipe/repo%2Fmain/request/7/extra",
  ])("rejects invalid or removed route %s", async (path) => {
    const { app, service } = createPageTestContext();
    expect(
      await app.handle({ path, search: new URLSearchParams() }),
    ).toEqual({ kind: "not-found" });
    expect(service.list).not.toHaveBeenCalled();
    expect(service.getRequestHistoryByID).not.toHaveBeenCalled();
  });
});
