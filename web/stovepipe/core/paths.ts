import { encodePathSegment } from "./path-segment.js";

export class StovepipePaths {
  readonly basePath: string;
  constructor(basePath = "") { this.basePath = basePath.replace(/\/$/u, ""); }
  queue(queue: string, page = ""): string {
    const path = `${this.basePath}/${encodePathSegment(queue)}`;
    return page ? `${path}?${new URLSearchParams({ page })}` : path;
  }
  change(queue: string, changeUri: string, projectsPage = ""): string {
    const path = `${this.queue(queue)}/change/${encodePathSegment(changeUri)}`;
    return projectsPage ? `${path}?${new URLSearchParams({ projectsPage })}` : path;
  }
  projectJSON(queue: string, changeUri: string): string {
    return `${this.change(queue, changeUri)}/projects.json`;
  }
}
