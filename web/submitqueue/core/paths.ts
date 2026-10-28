import { encodePathSegment } from "@submitqueue/web-platform/path-segment";

export interface RequestsPathOptions {
  pageToken?: string | null;
  /** Statuses a queue list is restricted to, as repeated `status` parameters. */
  statuses?: readonly string[];
}

export interface WebPathsOptions {
  basePath?: string;
}

export interface RequestPathOptions extends RequestsPathOptions {
  view?: "summary" | "history";
}


function encodePath(value: string): string {
  return value.split("/").map(encodePathSegment).join("/");
}

export class WebPaths {
  readonly basePath: string;

  constructor(options: WebPathsOptions = {}) {
    const requestedPath = options.basePath ?? "";
    this.basePath = requestedPath.endsWith("/")
      ? requestedPath.slice(0, -1)
      : requestedPath;
  }

  requests(queue: string, options: RequestsPathOptions = {}): string {
    const parameters = new URLSearchParams();
    for (const status of options.statuses ?? []) {
      parameters.append("status", status);
    }
    if (options.pageToken) {
      parameters.set("page", options.pageToken);
    }
    const query = parameters.toString();
    const queuePath = `${this.basePath}/queue/${encodePathSegment(queue)}`;
    return query === "" ? queuePath : `${queuePath}?${query}`;
  }

  directory(): string {
    return this.basePath || "/";
  }

  request(queue: string, sqid: string, options: RequestPathOptions = {}): string {
    const query = new URL(this.requests(queue, options), "http://paths.invalid").searchParams;
    if (options.view === "history") {
      query.set("view", "history");
    }
    const suffix = query.toString();
    return `${this.requests(queue)}/request/${encodePath(sqid)}${suffix ? `?${suffix}` : ""}`;
  }
}
