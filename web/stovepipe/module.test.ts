import { describe, expect, it, vi } from "vitest";
import { createStovepipeWeb } from "./module.js";
import { StovepipePaths } from "./core/paths.js";
import type {
  StovepipeService,
  RequestSummary,
  ProjectStatus,
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
  projectCount: 51,
  brokenProjectCount: 2,
};
function createPageTestContext() {
  const service: StovepipeService = {
    listOpenIssues: vi.fn().mockResolvedValue({ issues: [], nextPageToken: "" }),
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
    getProjectStatusByID: vi.fn().mockResolvedValue({
      queue: summary.queue,
      requestId: summary.requestId,
      updatedAtMs: 9007199254740993n,
      repositoryResult: { case: "repositoryBreakageDegree", value: 0 },
      projectResultsComplete: false,
      projects: [
        { project: "green", result: { case: "breakageDegree", value: 0 } },
        { project: "broken", result: { case: "breakageDegree", value: 0.25 } },
        { project: "unknown", result: { case: undefined } },
      ],
      nextPageToken: "next+/=",
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
  it("loads only open issues for the queue tab and preserves identities and opaque cursors", async () => {
    const { app, service, paths } = createPageTestContext();
    vi.mocked(service.listOpenIssues).mockResolvedValue({
      issues: [{ taskId: "build/test", unitId: "", requestId: summary.requestId, changedAtMs: 9007199254740993n }],
      nextPageToken: "next+/=",
    });
    const controller = new AbortController();
    const result = await app.handle({
      path: paths.queue("repo/main"), search: new URLSearchParams({ view: "issues", page: "opaque+/=" }),
      signal: controller.signal,
    });
    expect(service.listOpenIssues).toHaveBeenCalledWith(
      { queue: "repo/main", pageSize: 50, pageToken: "opaque+/=" }, { signal: controller.signal },
    );
    expect(service.list).not.toHaveBeenCalled();
    expect(service.getRequestHistoryByID).not.toHaveBeenCalled();
    expect(result).toMatchObject({
      kind: "render",
      model: {
        kind: "issues", latestHref: paths.openIssues("repo/main"), showLatestLink: true,
        issues: { ok: true, data: {
          issues: [{
            taskId: "build/test", unitId: "", firstSeenAtMs: "9007199254740993",
            requestId: summary.requestId, requestHref: paths.request("repo/main", summary.requestId),
          }],
          nextHref: paths.openIssues("repo/main", "next+/="),
        } },
      },
    });
  });

  it("does not call issue storage for an unknown queue or denied access", async () => {
    const { app, service, paths } = createPageTestContext();
    expect(await app.handle({ path: paths.queue("unknown"), search: new URLSearchParams({ view: "issues" }) })).toEqual({ kind: "not-found" });
    const denied = createStovepipeWeb({ service, paths, queues: [{ name: "repo/main" }], authorize: () => false });
    expect(await denied.handle({ path: paths.queue("repo/main"), search: new URLSearchParams({ view: "issues" }) })).toEqual({ kind: "forbidden" });
    expect(service.listOpenIssues).not.toHaveBeenCalled();
  });

  it("contains issue-service failures in the page section without exposing backend diagnostics", async () => {
    const { app, service, paths } = createPageTestContext();
    vi.mocked(service.listOpenIssues).mockRejectedValue(new Error("private issue-service detail"));
    const result = await app.handle({ path: paths.queue("repo/main"), search: new URLSearchParams({ view: "issues" }) });
    expect(result).toMatchObject({ kind: "render", model: { kind: "issues", issues: { ok: false } } });
    expect(JSON.stringify(result)).not.toContain("private issue-service detail");
  });

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
    expect(service.getProjectStatusByID).not.toHaveBeenCalled();
    expect(result.model.requests.data.requests).toEqual([
      expect.objectContaining({ projectCount: 51, brokenProjectCount: 2 }),
      expect.objectContaining({ projectCount: 51, brokenProjectCount: 2 }),
    ]);
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
    expect(service.getProjectStatusByID).toHaveBeenCalledWith(
      { queue: summary.queue, requestId: summary.requestId, projects: [], pageSize: 50, pageToken: "" },
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
              projectCount: 51,
              brokenProjectCount: 2,
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
    expect(service.getProjectStatusByID).not.toHaveBeenCalled();
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
    expect(service.getProjectStatusByID).not.toHaveBeenCalled();
  });

  it("loads recorded projects by exact request ID and preserves opaque cursors and int64 precision", async () => {
    const { app, service, paths } = createPageTestContext();
    const result = await app.handle({
      path: paths.request(summary.queue, summary.requestId),
      search: new URLSearchParams({ projects: "opaque+/=" }),
    });
    expect(service.getProjectStatusByID).toHaveBeenCalledWith({
      queue: summary.queue, requestId: summary.requestId,
      projects: [], pageSize: 50, pageToken: "opaque+/=",
    }, {});
    expect(result).toMatchObject({ kind: "render", model: { projectStatus: {
      ok: true,
      data: {
        updatedAtMs: "9007199254740993",
        repositoryBreakageDegree: 0,
        complete: false,
        projects: [
          { project: "green", breakageDegree: 0 },
          { project: "broken", breakageDegree: 0.25 },
          { project: "unknown", breakageDegree: null },
        ],
        firstHref: paths.request(summary.queue, summary.requestId),
        nextHref: paths.request(summary.queue, summary.requestId, "next+/="),
      },
    } } });
    expect(() => JSON.stringify(result)).not.toThrow();
  });

  it.each([25, 100, 200])("requests %s project results and preserves the size in pagination links", async (pageSize) => {
    const { app, service, paths } = createPageTestContext();
    const result = await app.handle({
      path: paths.request(summary.queue, summary.requestId),
      search: new URLSearchParams({ projects: "opaque+/=", projectPageSize: String(pageSize) }),
    });
    expect(service.getProjectStatusByID).toHaveBeenCalledWith({
      queue: summary.queue, requestId: summary.requestId,
      projects: [], pageSize, pageToken: "opaque+/=",
    }, {});
    expect(result).toMatchObject({ kind: "render", model: { projectStatus: {
      ok: true, data: {
        pageSize,
        firstHref: paths.request(summary.queue, summary.requestId) + `?projectPageSize=${pageSize}`,
        nextHref: paths.request(summary.queue, summary.requestId) + `?projects=next%2B%2F%3D&projectPageSize=${pageSize}`,
      },
    } } });
  });

  it.each(["", "0", "-1", "201", "25.5", "invalid"])("uses 50 project results for an invalid page size %j", async (projectPageSize) => {
    const { app, service, paths } = createPageTestContext();
    await app.handle({
      path: paths.request(summary.queue, summary.requestId),
      search: new URLSearchParams({ projectPageSize }),
    });
    expect(service.getProjectStatusByID).toHaveBeenCalledWith({
      queue: summary.queue, requestId: summary.requestId,
      projects: [], pageSize: 50, pageToken: "",
    }, {});
  });

  it.each([
    { queue: "other" },
    { requestId: "other" },
    { projects: [{ project: "invalid", result: { case: "breakageDegree", value: NaN } }] },
    { repositoryResult: { case: "repositoryBreakageDegree", value: 1.1 } },
  ] satisfies Partial<ProjectStatus>[])("rejects mismatched or malformed project status %j", async (overrides) => {
    const { app, service, paths } = createPageTestContext();
    const original = await service.getProjectStatusByID({
      queue: summary.queue, requestId: summary.requestId, projects: [], pageSize: 50, pageToken: "",
    });
    vi.mocked(service.getProjectStatusByID).mockResolvedValue({
      ...original, ...overrides,
    });
    const result = await app.handle({
      path: paths.request(summary.queue, summary.requestId), search: new URLSearchParams(),
    });
    expect(result).toMatchObject({
      kind: "render", model: { history: { ok: true }, projectStatus: { ok: false } },
    });
  });

  it("loads history independently when the project read fails without leaking diagnostics", async () => {
    const { app, service, paths } = createPageTestContext();
    vi.mocked(service.getProjectStatusByID).mockRejectedValue(new Error("private diagnostic"));
    const result = await app.handle({
      path: paths.request(summary.queue, summary.requestId), search: new URLSearchParams(),
    });
    expect(result).toMatchObject({
      kind: "render", model: { history: { ok: true }, projectStatus: { ok: false } },
    });
    expect(JSON.stringify(result)).not.toContain("private diagnostic");
  });

  it("keeps unknown repository results and empty complete pages distinct from green", async () => {
    const { app, service, paths } = createPageTestContext();
    vi.mocked(service.getProjectStatusByID).mockResolvedValue({
      queue: summary.queue, requestId: summary.requestId, updatedAtMs: 0n,
      repositoryResult: { case: undefined }, projectResultsComplete: true,
      projects: [], nextPageToken: "",
    });
    const result = await app.handle({
      path: paths.request(summary.queue, summary.requestId), search: new URLSearchParams(),
    });
    expect(result).toMatchObject({ kind: "render", model: { projectStatus: {
      ok: true, data: {
        repositoryBreakageDegree: null, complete: true, projects: [],
        firstHref: null, nextHref: null,
      },
    } } });
  });
});
