import type { StovepipeService } from "../extension/service/index.js";
import type { StovepipePaths } from "../core/paths.js";
import type { OpenIssuesModel, RequestIssuesModel } from "../entity/index.js";

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
    issues: result.issues,
    status: result.status,
    firstHref: pageToken ? paths.requestIssues(queue, requestId) : null,
    nextHref: result.nextPageToken ? paths.requestIssues(queue, requestId, result.nextPageToken) : null,
  };
}
