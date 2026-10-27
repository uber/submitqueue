import type { ProjectStatusModel } from "../entity/index.js";
import type { StovepipeService } from "../extension/service/index.js";
import type { StovepipePaths } from "../core/paths.js";

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
  signal?: AbortSignal,
): Promise<ProjectStatusModel> {
  const result = await service.getProjectStatusByID(
    { queue, requestId, projects: [], pageSize: 50, pageToken },
    signal ? { signal } : {},
  );
  if (result.queue !== queue || result.requestId !== requestId)
    throw new Error("Project response does not identify the selected request");
  return {
    updatedAtMs: result.updatedAtMs.toString(),
    repositoryBreakageDegree: result.repositoryResult.case === "repositoryBreakageDegree"
      ? breakageDegree(result.repositoryResult.value) : null,
    complete: result.projectResultsComplete,
    projects: result.projects.map((entry) => ({
      project: entry.project,
      projectType: entry.projectType,
      breakageDegree: entry.result.case === "breakageDegree"
        ? breakageDegree(entry.result.value) : null,
    })),
    firstHref: pageToken ? paths.request(queue, requestId) : null,
    nextHref: result.nextPageToken
      ? paths.request(queue, requestId, result.nextPageToken)
      : null,
  };
}
