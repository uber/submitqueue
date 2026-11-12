import { describe, expect, it, vi } from "vitest";
import { createStovepipeWeb, type StovepipeWebOptions } from "./module.js";
import { StovepipePaths } from "./core/paths.js";
import type {
  StovepipeService,
  RequestSummary,
  ProjectStatus,
  HistoryEvent,
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
function createPageTestContext(options: Pick<StovepipeWebOptions, "resolveBuildUrl"> = {}) {
  const service: StovepipeService = {
    listRequestProjectsByIssue: vi.fn().mockResolvedValue({ attributions: [], attributionComplete: true, nextPageToken: "" }),
    getIssuesByRequestID: vi.fn().mockResolvedValue({ issues: [], status: "complete", nextPageToken: "" }),
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
      requestState: "processing",
      updatedAtMs: 9007199254740993n,
      repositoryResult: { case: "repositoryBreakageDegree", value: 0 },
      projectResultsComplete: false,
      projects: [
        { project: "green", projectType: "service", result: { case: "breakageDegree", value: 0 } },
        { project: "broken", projectType: "future-type", result: { case: "breakageDegree", value: 0.25 } },
        { project: "unknown", projectType: "", result: { case: undefined } },
      ],
      nextPageToken: "next+/=",
    }),
  };
  const paths = new StovepipePaths("/stovepipe");
  const app = createStovepipeWeb({
    service,
    paths,
    queues: [{ name: "repo/main" }],
    ...options,
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
    ...(["requestState", "buildTriggered", "buildFinished", "buildAnalysisAbandoned"] as const).map((eventCase) => ({
      name: eventCase,
      data: { payload: { case: eventCase, value: { buildId: "runner/build/7?attempt=2" } } },
      details: [{ label: "Build ID", value: "runner/build/7?attempt=2" }],
    })),
    { name: "green validation", data: { payload: { case: "validationFactRecorded", value: { factDegree: 0 } } }, details: [{ label: "Breakage degree", value: "0" }] },
    { name: "partial breakage", data: { payload: { case: "validationFactRecorded", value: { factDegree: 0.25 } } }, details: [{ label: "Breakage degree", value: "0.25" }] },
    { name: "no project facts", data: { payload: { case: "projectFactsRecorded", value: { projectFactCount: 0n } } }, details: [{ label: "Project facts", value: "0" }] },
    { name: "large project count", data: { payload: { case: "projectFactsRecorded", value: { projectFactCount: 9007199254740993n } } }, details: [{ label: "Project facts", value: "9007199254740993" }] },
    { name: "missing metadata", data: undefined, details: [] },
    { name: "unknown metadata", data: { payload: { case: undefined } }, details: [] },
    { name: "empty build ID", data: { payload: { case: "buildTriggered", value: { buildId: "" } } }, details: [] },
  ] satisfies { name: string; data: HistoryEvent["data"]; details: { label: string; value: string }[] }[])("exposes serializable history details for $name", async ({ data, details }) => {
    const { app, service, paths } = createPageTestContext();
    vi.mocked(service.getRequestHistoryByID).mockResolvedValue({
      requestSummary: summary,
      events: [{ eventId: "e1", timestampMs: 100n, occurrence: { case: "event", value: "retained_event" }, outcomeReason: "", data }],
    });
    const result = await app.handle({
      path: paths.request(summary.queue, summary.requestId), search: new URLSearchParams(),
    });
    expect(result).toMatchObject({ kind: "render", model: { history: { ok: true, data: { events: [{ details }] } } } });
    expect(() => JSON.stringify(result)).not.toThrow();
  });
  it.each(["requestState", "buildTriggered", "buildFinished", "buildAnalysisAbandoned"] as const)("resolves %s build links with the opaque ID and queue", async (payloadCase) => {
    const href = "https://builds.example.test/pipeline/builds/7?attempt=2";
    const resolveBuildUrl = vi.fn(() => href);
    const { app, service, paths } = createPageTestContext({ resolveBuildUrl });
    vi.mocked(service.getRequestHistoryByID).mockResolvedValue({
      requestSummary: summary,
      events: [{
        eventId: "e1", timestampMs: 100n, occurrence: { case: "event", value: "retained_event" }, outcomeReason: "",
        data: { payload: { case: payloadCase, value: { buildId: "runner/build/7?attempt=2" } } },
      }],
    });
    const result = await app.handle({ path: paths.request(summary.queue, summary.requestId), search: new URLSearchParams() });
    expect(resolveBuildUrl).toHaveBeenCalledExactlyOnceWith({ queue: summary.queue, buildId: "runner/build/7?attempt=2" });
    expect(result).toMatchObject({ kind: "render", model: { history: { ok: true, data: { events: [{
      details: [{ label: "Build ID", value: "runner/build/7?attempt=2", href }],
    }] } } } });
    expect(() => JSON.stringify(result)).not.toThrow();
  });
  it.each([undefined, "", "not a URL", "/build/7", "//builds.example.test/7", "javascript:alert(1)", "data:text/html,build"])("leaves build IDs as text when the resolver returns %j", async (href) => {
    const { app, service, paths } = createPageTestContext({ resolveBuildUrl: () => href });
    vi.mocked(service.getRequestHistoryByID).mockResolvedValue({
      requestSummary: summary,
      events: [{
        eventId: "e1", timestampMs: 100n, occurrence: { case: "event", value: "build_triggered" }, outcomeReason: "",
        data: { payload: { case: "buildTriggered", value: { buildId: "runner/build/7" } } },
      }],
    });
    const result = await app.handle({ path: paths.request(summary.queue, summary.requestId), search: new URLSearchParams() });
    if (result.kind !== "render" || result.model.kind !== "request" || !result.model.history.ok)
      throw new Error("No history page");
    expect(result.model.history.data.events[0]?.details).toEqual([{ label: "Build ID", value: "runner/build/7" }]);
  });
  it("does not resolve links for metadata without a build ID", async () => {
    const resolveBuildUrl = vi.fn(() => "https://builds.example.test/7");
    const { app, service, paths } = createPageTestContext({ resolveBuildUrl });
    const metadata: HistoryEvent["data"][] = [
      undefined,
      { payload: { case: "validationFactRecorded", value: { factDegree: 0 } } },
      { payload: { case: "buildTriggered", value: { buildId: "" } } },
    ];
    vi.mocked(service.getRequestHistoryByID).mockResolvedValue({
      requestSummary: summary,
      events: metadata.map((data, index): HistoryEvent => ({
        eventId: String(index), timestampMs: 100n, occurrence: { case: "event", value: "retained_event" }, outcomeReason: "", data,
      })),
    });
    const result = await app.handle({ path: paths.request(summary.queue, summary.requestId), search: new URLSearchParams() });
    expect(result).toMatchObject({ kind: "render", model: { history: { ok: true } } });
    expect(resolveBuildUrl).not.toHaveBeenCalled();
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
        requestState: "processing",
        updatedAtMs: "9007199254740993",
        repositoryBreakageDegree: 0,
        complete: false,
        projects: [
          { project: "green", projectType: "service", breakageDegree: 0 },
          { project: "broken", projectType: "future-type", breakageDegree: 0.25 },
          { project: "unknown", projectType: "", breakageDegree: null },
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
    { projects: [{ project: "invalid", projectType: "", result: { case: "breakageDegree", value: NaN } }] },
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
      queue: summary.queue, requestId: summary.requestId, requestState: "succeeded", updatedAtMs: 0n,
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

it("loads request issues only for their tab and preserves opaque continuation tokens", async () => {
  const { app, service, paths } = createPageTestContext();
  const signal = new AbortController().signal;
  vi.mocked(service.getIssuesByRequestID).mockResolvedValue({
    issues: [{ taskId: "build/test", unitId: "//pkg:a", kind: "new", buildId: "runner/7", firstSeenBuildId: "runner/7" }],
    status: "in_progress", nextPageToken: "next+/=",
  });
  await app.handle({ path: paths.request("repo/main", summary.requestId), search: new URLSearchParams() });
  expect(service.getIssuesByRequestID).not.toHaveBeenCalled();
  const result = await app.handle({ path: paths.request("repo/main", summary.requestId),
    search: new URLSearchParams({ view: "issues", issues: "opaque+/=" }), signal });
  expect(service.getIssuesByRequestID).toHaveBeenCalledWith(
    { queue: "repo/main", requestId: summary.requestId, pageSize: 50, pageToken: "opaque+/=" }, { signal },
  );
  expect(result).toMatchObject({ kind: "render", model: { kind: "request", requestIssues: { ok: true, data: {
    status: "in_progress", firstHref: paths.requestIssues("repo/main", summary.requestId),
    nextHref: paths.requestIssues("repo/main", summary.requestId, "next+/="),
    issues: [{ taskId: "build/test", unitId: "//pkg:a", kind: "new" }],
  } } } });
});

it("keeps request history and project results when the request issues read fails", async () => {
  const { app, service, paths } = createPageTestContext();
  vi.mocked(service.getIssuesByRequestID).mockRejectedValue(new Error("backend details"));
  const result = await app.handle({ path: paths.request("repo/main", summary.requestId), search: new URLSearchParams({ view: "issues" }) });
  expect(result).toMatchObject({ kind: "render", model: {
    history: { ok: true }, projectStatus: { ok: true },
    requestIssues: { ok: false, error: expect.not.stringContaining("backend details") },
  } });
});

it("reads projects only for a selected unit issue and preserves both pagination cursors", async () => {
  const { app, service, paths } = createPageTestContext();
  const path = paths.request("repo/main", summary.requestId);
  await app.handle({ path, search: new URLSearchParams({ view: "issues" }) });
  await app.handle({ path, search: new URLSearchParams({ view: "issues", issueTask: "test", issueUnit: "" }) });
  expect(service.listRequestProjectsByIssue).not.toHaveBeenCalled();
  vi.mocked(service.listRequestProjectsByIssue).mockResolvedValue({
    attributionComplete: true, nextPageToken: "next+/=",
    attributions: [{ taskId: "build/test", unitId: "//pkg:a?x#y", project: "project/one" }],
  });
  const signal = new AbortController().signal;
  const result = await app.handle({ path, signal, search: new URLSearchParams({
    view: "issues", issues: "issue+/=", issueTask: "build/test", issueUnit: "//pkg:a?x#y", affectedProjects: "projects+/=",
  }) });
  expect(service.listRequestProjectsByIssue).toHaveBeenCalledWith({ queue: "repo/main", requestId: summary.requestId,
    taskId: "build/test", unitId: "//pkg:a?x#y", pageSize: 50, pageToken: "projects+/=" }, { signal });
  expect(result).toMatchObject({ kind: "render", model: { issueProjects: { closeHref: paths.requestIssues("repo/main", summary.requestId, "issue+/="), result: { ok: true, data: {
    complete: true, projects: ["project/one"],
    firstHref: paths.issueProjects("repo/main", summary.requestId, "build/test", "//pkg:a?x#y", "issue+/="),
    nextHref: paths.issueProjects("repo/main", summary.requestId, "build/test", "//pkg:a?x#y", "issue+/=", "next+/="),
  } } } } });
});

it("rejects attribution for another issue without hiding the request issues", async () => {
  const { app, service, paths } = createPageTestContext();
  vi.mocked(service.listRequestProjectsByIssue).mockResolvedValue({
    attributionComplete: true, nextPageToken: "", attributions: [{ taskId: "wrong", unitId: "unit", project: "p" }],
  });
  const result = await app.handle({ path: paths.request("repo/main", summary.requestId), search: new URLSearchParams({ view: "issues", issueTask: "test", issueUnit: "unit" }) });
  expect(result).toMatchObject({ kind: "render", model: { requestIssues: { ok: true }, issueProjects: { result: { ok: false } } } });
});
