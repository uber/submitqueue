import type { ProjectStatusModel, HistoryEventModel } from "../entity/index.js";
import type { StovepipeService, ProjectStatus } from "../extension/service/index.js";
import type { StovepipePaths } from "../core/paths.js";
import { mapRequestSummary } from "./mapping.js";

const projectsPerPage = 10;

export async function loadProjectStatus(service: StovepipeService, paths: StovepipePaths, queue: string, changeUri: string, projects: readonly string[], projectsPage: string, signal?: AbortSignal): Promise<ProjectStatusModel> {
  const page = projectsPage === "" ? 1 : Number(projectsPage);
  if ((projectsPage !== "" && !/^[1-9][0-9]*$/u.test(projectsPage)) || !Number.isSafeInteger(page) || page > Math.max(1, Math.ceil(projects.length / projectsPerPage))) throw new Error("Invalid project page");
  const start = (page - 1) * projectsPerPage;
  const selectedProjects = projects.slice(start, start + projectsPerPage);
  const result = await service.getProjectStatusByURI({ queue, changeUri, projects: [...selectedProjects], pageSize: projectsPerPage, pageToken: "" }, signal ? { signal } : {});
  validateProjectStatusIdentity(result, queue, changeUri);
  const results = includeMissingProjects(selectedProjects, result.projects);
  return { request: mapRequestSummary({ ...result, stateUpdatedAtMs: result.updatedAtMs, acceptedAtMs: 0n, outcomeReason: "" }, paths),
    repositoryDegree: result.repositoryResult.case === "repositoryBreakageDegree" ? result.repositoryResult.value : null,
    complete: result.projectResultsComplete,
    projects: results.map(project => ({ name: project.project, degree: project.result.case === "breakageDegree" ? project.result.value : null })),
    nextHref: start + projectsPerPage < projects.length ? paths.change(queue, changeUri, String(page + 1)) : null,
    firstHref: page > 1 ? paths.change(queue, changeUri) : null,
    rawHref: paths.projectJSON(queue, changeUri) };
}

export async function readAllProjectResults(service: StovepipeService, queue: string, changeUri: string, projects: readonly string[], signal?: AbortSignal): Promise<ProjectStatus["projects"]> {
  const results: ProjectStatus["projects"] = [];
  const seenTokens = new Set<string>();
  let requestId: string | undefined;
  let pageToken = "";
  do {
    const result = await service.getProjectStatusByURI({ queue, changeUri, projects: [...projects], pageSize: 0, pageToken }, signal ? { signal } : {});
    validateProjectStatusIdentity(result, queue, changeUri);
    if (requestId !== undefined && result.requestId !== requestId) throw new Error("Project pagination selects another request");
    requestId = result.requestId;
    results.push(...result.projects);
    pageToken = result.nextPageToken;
    if (pageToken && seenTokens.has(pageToken)) throw new Error("Project pagination did not advance");
    seenTokens.add(pageToken);
  } while (pageToken);
  return includeMissingProjects(projects, results);
}

function validateProjectStatusIdentity(result: ProjectStatus, queue: string, changeUri: string) {
  if (!result.requestId || result.changeUri !== changeUri || result.queue !== queue) throw new Error("Status response selects another change");
}

function includeMissingProjects(projects: readonly string[], results: ProjectStatus["projects"]): ProjectStatus["projects"] {
  const recorded = new Map(results.map(result => [result.project, result]));
  return projects.map(project => recorded.get(project) ?? { project, result: { case: undefined } });
}

export async function loadRequestHistory(service: StovepipeService, queue: string, requestId: string, signal?: AbortSignal): Promise<HistoryEventModel[]> {
  const result = await service.getRequestHistoryByID({ queue, requestId }, signal ? { signal } : {});
  return result.events.map(event => ({ id: event.eventId, timestampMs: event.timestampMs.toString(),
    kind: event.occurrence.case ?? "unknown", label: event.occurrence.value ?? "Unknown event", outcomeReason: event.outcomeReason }));
}
