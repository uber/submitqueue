import { encodePathSegment } from "@submitqueue/web-platform/path-segment";

export const projectPageSizes = [25, 50, 100, 200] as const;

export function parseProjectPageSize(value: string | null): number {
  return projectPageSizes.find(size => String(size) === value) ?? 50;
}

export class StovepipePaths {
  readonly basePath: string;
  constructor(basePath = "") {
    this.basePath = basePath.replace(/\/$/u, "");
  }
  queue(queue: string, page = ""): string {
    const path = `${this.basePath}/${encodePathSegment(queue)}`;
    return page ? `${path}?${new URLSearchParams({ page })}` : path;
  }
  request(queue: string, requestId: string, projects = "", projectPageSize = 50): string {
    const path = `${this.queue(queue)}/request/${encodePathSegment(requestId)}`;
    const search = new URLSearchParams();
    if (projects) search.set("projects", projects);
    if (projectPageSize !== 50) search.set("projectPageSize", String(projectPageSize));
    return search.size ? `${path}?${search}` : path;
  }
}
