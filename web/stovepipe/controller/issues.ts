import type { StovepipeService } from "../extension/service/index.js";
import type { StovepipePaths } from "../core/paths.js";
import type { OpenIssuesModel, RequestIssuesModel, IssueProjectsModel } from "../entity/index.js";

export async function loadOpenIssues(
  service: StovepipeService,
  paths: StovepipePaths,
  queue: string,
  pageToken: string,
  signal?: AbortSignal,
): Promise<OpenIssuesModel> {
  const result = await service.listOpenIssues(
    { queue, pageSize: 50, pageToken },
    signal ? { signal } : {},
  );
  return {
    issues: result.issues.map(issue => ({
      taskId: issue.taskId,
      unitId: issue.unitId,
      firstSeenAtMs: issue.changedAtMs.toString(),
      requestId: issue.requestId,
      requestHref: paths.request(queue, issue.requestId),
    })),
    nextHref: result.nextPageToken ? paths.openIssues(queue, result.nextPageToken) : null,
  };
}

export async function loadRequestIssues(
  service: StovepipeService,
  paths: StovepipePaths,
  queue: string,
  requestId: string,
  pageToken: string,
  signal?: AbortSignal,
): Promise<RequestIssuesModel> {
  const result = await service.getIssuesByRequestID(
    { queue, requestId, pageSize: 50, pageToken }, signal ? { signal } : {},
  );
  return {
    issues: result.issues.map(issue => ({
      ...issue,
      projectsHref: issue.unitId && (issue.kind === "new" || issue.kind === "existing")
        ? paths.issueProjects(queue, requestId, issue.taskId, issue.unitId, pageToken) : null,
    })),
    status: result.status,
    firstHref: pageToken ? paths.requestIssues(queue, requestId) : null,
    nextHref: result.nextPageToken ? paths.requestIssues(queue, requestId, result.nextPageToken) : null,
  };
}

export async function loadIssueProjects(
  service: StovepipeService,
  paths: StovepipePaths,
  queue: string,
  requestId: string,
  taskId: string,
  unitId: string,
  issuesPage: string,
  pageToken: string,
  signal?: AbortSignal,
): Promise<IssueProjectsModel> {
  const result = await service.listRequestProjectsByIssue(
    { queue, requestId, taskId, unitId, pageSize: 50, pageToken }, signal ? { signal } : {},
  );
  if (result.attributions.some(entry => entry.taskId !== taskId || entry.unitId !== unitId || !entry.project))
    throw new Error("Project attribution response does not identify the selected issue");
  return {
    projects: result.attributions.map(entry => entry.project), complete: result.attributionComplete,
    firstHref: pageToken ? paths.issueProjects(queue, requestId, taskId, unitId, issuesPage) : null,
    nextHref: result.nextPageToken ? paths.issueProjects(queue, requestId, taskId, unitId, issuesPage, result.nextPageToken) : null,
  };
}
