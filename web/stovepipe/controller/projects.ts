import type { ProjectStatusModel } from "../entity/index.js";
import type { StovepipeService } from "../extension/service/index.js";
import { projectPageSizes, type StovepipePaths } from "../core/paths.js";

function breakageDegree(value: number): number {
  if (!Number.isFinite(value) || value < 0 || value > 1)
    throw new Error("Invalid breakage degree");
  return value;
}

export async function loadProjectStatus(
  service: StovepipeService,
  paths: StovepipePaths,
  queue: string,
  requestId: string,
  pageToken: string,
  pageSize: number,
  signal?: AbortSignal,
): Promise<ProjectStatusModel> {
  const result = await service.getProjectStatusByID(
    { queue, requestId, projects: [], pageSize, pageToken },
    signal ? { signal } : {},
  );
  if (result.queue !== queue || result.requestId !== requestId)
    throw new Error("Project response does not identify the selected request");
  return {
    updatedAtMs: result.updatedAtMs.toString(),
    repositoryBreakageDegree: result.repositoryResult.case === "repositoryBreakageDegree"
      ? breakageDegree(result.repositoryResult.value) : null,
    complete: result.projectResultsComplete,
    pageSize,
    pageSizeChoices: projectPageSizes.map(value => ({
      value, href: paths.request(queue, requestId, "", value),
    })),
    projects: result.projects.map((entry) => ({
      project: entry.project,
      breakageDegree: entry.result.case === "breakageDegree"
        ? breakageDegree(entry.result.value) : null,
    })),
    firstHref: pageToken ? paths.request(queue, requestId, "", pageSize) : null,
    nextHref: result.nextPageToken
      ? paths.request(queue, requestId, result.nextPageToken, pageSize)
      : null,
  };
}
