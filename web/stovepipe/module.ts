import { createLogCallback, type LogCallback } from "@submitqueue/web-platform/logging";
import { parseStovepipeRoute } from "./core/route.js";
import type { StovepipeService } from "./extension/service/index.js";
import type { Queue, StovepipePageModel } from "./entity/index.js";
import { parseProjectPageSize, StovepipePaths } from "./core/paths.js";
import { readSection } from "./controller/page.js";
import { loadQueueRequests } from "./controller/queue.js";
import { loadOpenIssues, loadRequestIssues, loadIssueProjects } from "./controller/issues.js";
import { loadRequestHistory, type BuildUrlResolver } from "./controller/request.js";
import { loadProjectStatus, loadProjectIssues } from "./controller/projects.js";

export interface StovepipeWebRequest {
  /** Percent-encoded URL pathname, including the mount path. */
  path: string;
  search: URLSearchParams;
  principal?: unknown;
  signal?: AbortSignal;
  /** Correlation ID for logs; generated when absent. */
  requestId?: string;
  /** Host callback bound to this request's logging context; absent means silent. */
  onLog?: LogCallback;
}
export type StovepipeWebResult =
  | { kind: "render"; model: StovepipePageModel }
  | { kind: "not-found" }
  | { kind: "forbidden" }
  | { kind: "redirect"; href: string };
export interface StovepipeWebOptions {
  service: StovepipeService | ((request: StovepipeWebRequest) => StovepipeService);
  queues: readonly Queue[];
  paths?: StovepipePaths;
  authorize?(principal: unknown, queue: string): boolean | Promise<boolean>;
  /** Maps opaque build IDs to absolute HTTP(S) URLs; undefined leaves the ID as text. */
  resolveBuildUrl?: BuildUrlResolver;
}
export function createStovepipeWeb(options: StovepipeWebOptions) {
  const paths = options.paths ?? new StovepipePaths();
  return {
    async handle(request: StovepipeWebRequest): Promise<StovepipeWebResult> {
      const onLog = createLogCallback(request.onLog, {
        requestId: request.requestId ?? globalThis.crypto.randomUUID(),
        path: request.path,
      });
      try {
        if (request.path === (paths.basePath || "/")) {
          const first = options.queues[0];
          return first
            ? { kind: "redirect", href: paths.queue(first.name) }
            : { kind: "not-found" };
        }
        const route = parseStovepipeRoute(request.path, paths, options.queues);
        if (!route) return { kind: "not-found" };
        const { queue, requestId } = route;
        const issuesView = !requestId && request.search.get("view") === "issues";
        if (options.authorize && !(await options.authorize(request.principal, queue.name)))
          return { kind: "forbidden" };
        const service =
          typeof options.service === "function" ? options.service(request) : options.service;
        const common = {
          key: `${request.path}?${request.search}`,
          queue: queue.name,
          queues: options.queues.map((queue) => ({
            name: queue.name,
            href: issuesView ? paths.openIssues(queue.name) : paths.queue(queue.name),
          })),
          queueHref: paths.queue(queue.name),
          latestHref: issuesView ? paths.openIssues(queue.name) : paths.queue(queue.name),
          showLatestLink: Boolean(!requestId && request.search.get("page")),
          loadedAtMs: Date.now(),
        };
        if (issuesView) {
          const issues = await readSection(
            () => loadOpenIssues(service, paths, queue.name, request.search.get("page") ?? "", request.signal),
            onLog,
            "open issues",
          );
          return { kind: "render", model: { ...common, kind: "issues", issues } };
        }
        if (!requestId) {
          const requests = await readSection(
            () =>
              loadQueueRequests(
                service,
                paths,
                queue.name,
                request.search.get("page") ?? "",
                request.signal
              ),
            onLog,
            "queue requests"
          );
          return { kind: "render", model: { ...common, kind: "queue", requests } };
        }
        const requestIssuesView = request.search.get("view") === "issues";
        const issuesPage = request.search.get("issues") ?? "";
        const issueTask = request.search.get("issueTask") ?? "";
        const issueUnit = request.search.get("issueUnit") ?? "";
        const projectsPage = request.search.get("affectedProjects") ?? "";
        const selectedIssue = requestIssuesView && issueTask && issueUnit ? {
          taskId: issueTask, unitId: issueUnit,
          key: JSON.stringify([issueTask, issueUnit, projectsPage]),
          closeHref: paths.requestIssues(queue.name, requestId, issuesPage),
        } : null;
        const projectsCursor = request.search.get("projects") ?? "";
        const projectPageSize = parseProjectPageSize(request.search.get("projectPageSize"));
        const project = request.search.get("project") ?? "";
        const projectIssuesPage = request.search.get("projectIssues") ?? "";
        const projectResultsView = !["issues", "history", "fix-attempts"].includes(request.search.get("view") ?? "");
        const selectedProject = projectResultsView && project ? {
          project, key: JSON.stringify([project, projectIssuesPage]),
          closeHref: paths.request(queue.name, requestId, projectsCursor, projectPageSize),
        } : null;
        const [history, projectStatus, requestIssues, issueProjectsResult, projectIssuesResult] = await Promise.all([
          readSection(
            () => loadRequestHistory(service, paths, queue.name, requestId, options.resolveBuildUrl, request.signal),
            onLog,
            "request history",
          ),
          readSection(
            () => loadProjectStatus(
              service, paths, queue.name, requestId,
              projectsCursor, projectPageSize, request.signal,
            ),
            onLog,
            "project results",
          ),
          requestIssuesView ? readSection(
            () => loadRequestIssues(service, paths, queue.name, requestId, issuesPage, request.signal),
            onLog,
            "request issues",
          ) : Promise.resolve(null),
          selectedIssue ? readSection(
            () => loadIssueProjects(service, paths, queue.name, requestId, issueTask, issueUnit,
              issuesPage, projectsPage, request.signal),
            onLog, "affected projects",
          ) : Promise.resolve(null),
          selectedProject ? readSection(
            () => loadProjectIssues(service, paths, queue.name, requestId, project,
              projectsCursor, projectPageSize, projectIssuesPage, request.signal),
            onLog, "project issues",
          ) : Promise.resolve(null),
        ]);
        const issueProjects = selectedIssue && issueProjectsResult ? { ...selectedIssue, result: issueProjectsResult } : null;
        const projectIssues = selectedProject && projectIssuesResult ? { ...selectedProject, result: projectIssuesResult } : null;
        return {
          kind: "render",
          model: { ...common, kind: "request", requestId, history, projectStatus, requestIssues, issueProjects, projectIssues },
        };
      } catch (error) {
        onLog("error", "Web page load failed", { error });
        throw error;
      }
    },
  };
}
