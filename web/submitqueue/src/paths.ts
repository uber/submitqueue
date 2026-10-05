export interface RequestsPathOptions {
  pageToken?: string | null;
}

export interface WebPathsOptions {
  basePath?: string;
}

export interface RequestPathOptions extends RequestsPathOptions {
  view?: "summary" | "history";
}

function encodePathSegment(value: string): string {
  // A visible prefix prevents browser normalization of dot-only opaque values.
  return value === "." || value === ".." || value.startsWith("~")
    ? `~${encodeURIComponent(value)}`
    : encodeURIComponent(value);
}

export function decodePathSegment(value: string): string {
  return value.startsWith("~") ? value.slice(1) : value;
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
    if (options.pageToken) {
      parameters.set("page", options.pageToken);
    }
    const query = parameters.toString();
    const queuePath = `${this.basePath}/${encodePathSegment(queue)}`;
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
