import { describe, expect, it, vi } from "vitest";
import { createStovepipeWeb } from "./module.js";
import { StovepipePaths } from "./core/paths.js";
import type { StovepipeService, ProjectStatus } from "./extension/service/index.js";

const changeUri = "git://repo/main/sha?x=1&file=src%2Fmain#fragment";
const status: ProjectStatus = { requestId: "7", queue: "repo/main", changeUri, baseUri: "", requestState: "future_state", updatedAtMs: 9007199254740993n, repositoryResult: { case: "repositoryBreakageDegree", value: 0 }, projectResultsComplete: false, projects: [{ project: "green", result: { case: "breakageDegree", value: 0 } }], nextPageToken: "" };
const allProjects = [...status.projects, { project: "pending", result: { case: undefined } }];
function setup(projects: string[] = ["green", "pending"]) {
  const service: StovepipeService = { list: vi.fn().mockResolvedValue({ requests: [], nextPageToken: "older+/page" }), getProjectStatusByURI: vi.fn().mockResolvedValue(status), getRequestHistoryByID: vi.fn().mockResolvedValue({ events: [{ eventId: "e1", timestampMs: 100n, occurrence: { case: "event", value: "future_event" }, outcomeReason: "future_reason" }] }) };
  const paths = new StovepipePaths("/stovepipe");
  const app = createStovepipeWeb({ service, paths, queues: [{ name: "repo/main", projects }] });
  return { service, paths, app };
}
describe("Stovepipe page loading", () => {
  it("uses backend queue cursors and links each request to its exact change URI", async () => {
    const { service, app, paths } = setup();
    vi.mocked(service.list).mockResolvedValue({ requests: [{ ...status, stateUpdatedAtMs: 1n, acceptedAtMs: 1n, outcomeReason: "" }], nextPageToken: "older+/page" });
    const result = await app.handle({ path: paths.queue("repo/main"), search: new URLSearchParams({ page: "opaque+/cursor" }) });
    expect(service.list).toHaveBeenCalledWith({ queue: "repo/main", pageSize: 50, pageToken: "opaque+/cursor" }, {});
    if (result.kind !== "render" || result.model.kind !== "queue" || !result.model.requests.ok) throw new Error("No queue page");
    expect(result.model.requests.data.nextHref).toBe(paths.queue("repo/main", "older+/page"));
    expect(result.model.requests.data.requests[0]?.href).toBe(paths.change("repo/main", changeUri));
  });
  it("reads the exact URI, uses the returned ID for history and preserves green versus missing", async () => {
    const { app, service, paths } = setup();
    const result = await app.handle({ path: paths.change("repo/main", changeUri), search: new URLSearchParams() });
    expect(service.getProjectStatusByURI).toHaveBeenCalledWith({ queue: "repo/main", changeUri, projects: ["green", "pending"], pageSize: 10, pageToken: "" }, {});
    expect(service.getRequestHistoryByID).toHaveBeenCalledWith({ queue: "repo/main", requestId: "7" }, {});
    if (result.kind !== "render" || result.model.kind !== "request") throw new Error("No request page");
    expect(result.model).toMatchObject({ changeUri, requestId: "7", status: { ok: true, data: { repositoryDegree: 0, request: { updatedAtMs: "9007199254740993", state: "future_state" }, projects: [{ name: "green", degree: 0 }, { name: "pending", degree: null }] } }, history: { ok: true, data: [{ label: "future_event", outcomeReason: "future_reason" }] } });
    expect(() => JSON.stringify(result)).not.toThrow();
  });
  it("requests only ten configured projects per page and shows the final missing result", async () => {
    const projects = Array.from({ length: 11 }, (_, index) => `project-${index + 1}`);
    const { app, service, paths } = setup(projects);
    vi.mocked(service.getProjectStatusByURI).mockResolvedValue({ ...status, projects: [] });
    const first = await app.handle({ path: paths.change("repo/main", changeUri), search: new URLSearchParams() });
    if (first.kind !== "render" || first.model.kind !== "request" || !first.model.status.ok) throw new Error("No status");
    expect(first.model.status.data.projects).toHaveLength(10);
    expect(first.model.status.data.nextHref).toBe(paths.change("repo/main", changeUri, "2"));
    const last = await app.handle({ path: paths.change("repo/main", changeUri), search: new URLSearchParams({ projectsPage: "2" }) });
    expect(service.getProjectStatusByURI).toHaveBeenLastCalledWith({ queue: "repo/main", changeUri, projects: ["project-11"], pageSize: 10, pageToken: "" }, {});
    if (last.kind !== "render" || last.model.kind !== "request" || !last.model.status.ok) throw new Error("No status");
    expect(last.model.status.data).toMatchObject({ projects: [{ name: "project-11", degree: null }], nextHref: null, firstHref: paths.change("repo/main", changeUri), rawHref: paths.projectJSON("repo/main", changeUri) });
  });
  it("skips history when status fails and retains status when history fails", async () => {
    for (const failedMethod of ["getProjectStatusByURI", "getRequestHistoryByID"] as const) {
      const { app, service, paths } = setup();
      vi.mocked(service[failedMethod]).mockRejectedValue(new Error("service unavailable"));
      const result = await app.handle({ path: paths.change("repo/main", changeUri), search: new URLSearchParams() });
      if (result.kind !== "render" || result.model.kind !== "request") throw new Error("No request page");
      expect(result.model.status.ok).toBe(failedMethod !== "getProjectStatusByURI");
      expect(result.model.history.ok).toBe(false);
      if (failedMethod === "getProjectStatusByURI") expect(service.getRequestHistoryByID).not.toHaveBeenCalled();
    }
  });
  it("collects all raw project pages, includes missing projects and skips history", async () => {
    const { app, service, paths } = setup();
    vi.mocked(service.getProjectStatusByURI)
      .mockResolvedValueOnce({ ...status, nextPageToken: "next" })
      .mockResolvedValueOnce({ ...status, projects: [], nextPageToken: "" });
    const result = await app.handle({ path: paths.projectJSON("repo/main", changeUri), search: new URLSearchParams({ projectsPage: "displayed-page" }) });
    expect(result).toEqual({ kind: "project-json", data: allProjects });
    expect(service.getProjectStatusByURI).toHaveBeenNthCalledWith(1, { queue: "repo/main", changeUri, projects: ["green", "pending"], pageSize: 0, pageToken: "" }, {});
    expect(service.getProjectStatusByURI).toHaveBeenNthCalledWith(2, { queue: "repo/main", changeUri, projects: ["green", "pending"], pageSize: 0, pageToken: "next" }, {});
    expect(service.getRequestHistoryByID).not.toHaveBeenCalled();
  });
  it("rejects incomplete exports when a later page fails, repeats a cursor or selects another request", async () => {
    for (const failure of ["rpc", "cursor", "identity", "uri"] as const) {
      const { app, service, paths } = setup();
      const next = { ...status, nextPageToken: "next" };
      const read = vi.mocked(service.getProjectStatusByURI).mockResolvedValueOnce(next);
      if (failure === "rpc") read.mockRejectedValueOnce(new Error("service unavailable"));
      else read.mockResolvedValueOnce(failure === "identity" ? { ...status, requestId: "8" } : failure === "uri" ? { ...status, changeUri: "other" } : next);
      const result = await app.handle({ path: paths.projectJSON("repo/main", changeUri), search: new URLSearchParams() });
      expect(result.kind).toBe("unavailable");
      expect(service.getProjectStatusByURI).toHaveBeenCalledTimes(2);
    }
  });
  it("rejects invalid project pages before reading the service", async () => {
    const { app, service, paths } = setup();
    for (const projectsPage of ["0", "-1", "2", "1.5", "Infinity", "01"]) {
      const result = await app.handle({ path: paths.change("repo/main", changeUri), search: new URLSearchParams({ projectsPage }) });
      if (result.kind !== "render" || result.model.kind !== "request") throw new Error("No request page");
      expect(result.model.status.ok).toBe(false);
    }
    expect(service.getProjectStatusByURI).not.toHaveBeenCalled();
    expect(service.getRequestHistoryByID).not.toHaveBeenCalled();
  });
  it("blocks unauthorized loads before calling the service", async () => {
    const { service, paths } = setup();
    const app = createStovepipeWeb({ service, paths, queues: [{ name: "repo/main", projects: ["green"] }], authorize: () => false });
    expect(await app.handle({ path: paths.change("repo/main", changeUri), search: new URLSearchParams() })).toEqual({ kind: "forbidden" });
    expect(service.getProjectStatusByURI).not.toHaveBeenCalled();
  });
  it("rejects invalid routes and leaves request routes reserved", async () => {
    const { app, service } = setup();
    for (const path of ["/other", "/stovepipe/%", "/stovepipe/unknown", "/stovepipe/repo%2Fmain/request/7", "/stovepipe/repo%2Fmain/change/uri/extra"]) expect(await app.handle({ path, search: new URLSearchParams() })).toEqual({ kind: "not-found" });
    expect(service.list).not.toHaveBeenCalled();
    expect(service.getProjectStatusByURI).not.toHaveBeenCalled();
  });
});
