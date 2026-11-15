import type { ProjectStatusModel, ProjectIssuesModel } from "../entity/index.js";
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
    requestState: result.requestState,
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
      projectType: entry.projectType,
      issuesHref: paths.projectIssues(queue, requestId, entry.project, pageToken, pageSize),
      breakageDegree: entry.result.case === "breakageDegree"
        ? breakageDegree(entry.result.value) : null,
    })),
    firstHref: pageToken ? paths.request(queue, requestId, "", pageSize) : null,
    nextHref: result.nextPageToken
      ? paths.request(queue, requestId, result.nextPageToken, pageSize)
      : null,
  };
}

export async function loadProjectIssues(
  service: StovepipeService,
  paths: StovepipePaths,
  queue: string,
  requestId: string,
  project: string,
  projectsPage: string,
  projectPageSize: number,
  pageToken: string,
  signal?: AbortSignal,
): Promise<ProjectIssuesModel> {
  const result = await service.listRequestIssuesByProject(
    { queue, requestId, project, pageSize: 50, pageToken }, signal ? { signal } : {},
  );
  if (result.attributions.some(entry => entry.project !== project || !entry.taskId || !entry.unitId))
    throw new Error("Issue attribution response does not identify the selected project");
  return {
    issues: result.attributions.map(entry => ({ taskId: entry.taskId, unitId: entry.unitId })),
    complete: result.attributionComplete,
    firstHref: pageToken ? paths.projectIssues(queue, requestId, project, projectsPage, projectPageSize) : null,
    nextHref: result.nextPageToken ? paths.projectIssues(queue, requestId, project, projectsPage, projectPageSize, result.nextPageToken) : null,
  };
}
