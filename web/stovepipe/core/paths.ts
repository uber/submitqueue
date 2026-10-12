import { encodePathSegment } from "@submitqueue/web-platform/path-segment";

export class StovepipePaths {
  readonly basePath: string;
  constructor(basePath = "") {
    this.basePath = basePath.replace(/\/$/u, "");
  }
  queue(queue: string, page = ""): string {
    const path = `${this.basePath}/${encodePathSegment(queue)}`;
    return page ? `${path}?${new URLSearchParams({ page })}` : path;
  }
  request(queue: string, requestId: string): string {
    return `${this.queue(queue)}/request/${encodePathSegment(requestId)}`;
  }
}
