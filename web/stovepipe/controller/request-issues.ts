import type { RequestIssuesModel } from "../entity/index.js";
import type { StovepipeService } from "../extension/service/index.js";
import { parseProjectPageSize, type StovepipePaths } from "../core/paths.js";

export async function loadRequestIssues(
  service: StovepipeService,
  paths: StovepipePaths,
  queue: string,
  requestId: string,
  search: URLSearchParams,
  signal?: AbortSignal,
): Promise<RequestIssuesModel> {
  const pageToken = search.get("issues") ?? "";
  const result = await service.listIssuesByRequestID(
    { queue, requestId, pageSize: 50, pageToken },
    signal ? { signal } : {},
  );
  if (result.queue !== queue || result.requestId !== requestId)
    throw new Error("Issue response does not identify the selected request");
  const href = (page: string) => paths.requestIssues(
    queue, requestId, page, search.get("projects") ?? "",
    parseProjectPageSize(search.get("projectPageSize")),
  );
  return {
    issues: result.issues,
    builds: result.builds,
    winningBuildId: result.winningBuildId,
    firstHref: pageToken ? href("") : null,
    nextHref: result.nextPageToken ? href(result.nextPageToken) : null,
  };
}
