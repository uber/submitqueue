import type { WebSearchParams } from "../core/route.js";
import type { LoadResult } from "../entity/index.js";
import type { ChangeDetailModel } from "../entity/index.js";
import type { GatewayChangeReader, GatewayReader, GatewayRequestSummary } from "../extension/gateway/index.js";
import { changeHref, parseChangePath, parseChangeUri, type ChangeReference } from "../core/change.js";
import { WebPaths } from "../core/paths.js";
import { defaultRequestWindow, type RequestPageStart, type RequestWindow } from "../core/window.js";
import { INTERNAL_ERROR, INVALID_INPUT } from "./error.js";
import { callGateway, type GatewayLoadOptions } from "./gateway-call.js";
import { mapRequestSummary, mapResponseWindow } from "./mapping.js";
import { gatewayListInput } from "./queue.js";
import { notFound, redirect, refreshModel, render, type PageDependencies, type SubmitQueueWebResult } from "./page.js";

const MAX_CHANGE_SCAN_PAGES = 10;

export interface GatewayChangeSubmission {
  request: GatewayRequestSummary;
  version: string;
  versionHref: string;
}

export interface ChangeSubmissionPage {
  submissions: GatewayChangeSubmission[];
  nextPageToken: string | null;
  /** Receipt window the gateway scanned, or `null` for an exact lookup. */
  window: RequestWindow | null;
}

export interface GatewayChangeSubmissionPage {
  submissions: readonly GatewayChangeSubmission[];
  pagination?: ChangeDetailModel["pagination"];
  /** Replaces the context's window once the gateway reports the one it scanned. */
  window?: RequestWindow | null;
}

/**
 * Finds the submissions of a change: by exact URI when the reference pins a
 * reviewable version, otherwise by scanning at most
 * {@link MAX_CHANGE_SCAN_PAGES} queue pages from `start`.
 */
export async function readChangeSubmissions(
  client: GatewayReader & GatewayChangeReader,
  queue: string,
  reference: ChangeReference,
  start: RequestPageStart,
  paths = new WebPaths(),
): Promise<ChangeSubmissionPage> {
  const submissions: GatewayChangeSubmission[] = [];
  const appendSubmissions = (requests: readonly GatewayRequestSummary[]) => {
    for (const request of requests) {
      const match = request.changeUris.map(parseChangeUri).find(value =>
        value !== null && value.logicalPath === reference.logicalPath &&
        (reference.version === null || value.version === reference.version));
      if (match?.version) {
        submissions.push({
          request, version: match.version,
          versionHref: changeHref(queue, match, true, paths),
        });
      }
    }
  };
  if (reference.uri !== null && reference.scheme !== "git") {
    const response = await client.getRequestSummaryByChangeURI({ queue, changeUri: reference.uri });
    appendSubmissions(response.requests);
    return { submissions, nextPageToken: null, window: null };
  }
  let pageStart = start;
  let window: RequestWindow | null = null;
  const seenTokens = new Set<string>("pageToken" in start ? [start.pageToken] : []);
  for (let page = 0; page < MAX_CHANGE_SCAN_PAGES; page += 1) {
    const response = await client.list(gatewayListInput(queue, pageStart));
    window ??= mapResponseWindow(response);
    if (window === null) {
      throw new Error("The gateway returned an invalid receipt window");
    }
    appendSubmissions(response.requests);
    if (!response.nextPageToken) {
      return { submissions, nextPageToken: null, window };
    }
    if (seenTokens.has(response.nextPageToken)) {
      // A gateway contract violation; continuing would present duplicate history.
      throw new Error("The gateway repeated a queue page token");
    }
    pageStart = { pageToken: response.nextPageToken };
    seenTokens.add(response.nextPageToken);
  }
  return { submissions, nextPageToken: "pageToken" in pageStart ? pageStart.pageToken : null, window };
}

export async function loadChangeSubmissions(
  read: () => Promise<readonly GatewayChangeSubmission[] | GatewayChangeSubmissionPage>,
  context: Omit<ChangeDetailModel, "submissions">,
  options: GatewayLoadOptions = {},
): Promise<LoadResult<ChangeDetailModel>> {
  if (!context.queue) {
    return { ok: false, error: INVALID_INPUT };
  }
  const call = await callGateway(options, "change", { queue: context.queue }, read);
  if (!call.ok) {
    return call;
  }
  const response = call.value;
  const page: GatewayChangeSubmissionPage = "submissions" in response ? response : { submissions: response };
  const submissions: ChangeDetailModel["submissions"] = [];
  for (const item of page.submissions) {
    const request = mapRequestSummary(item.request);
    if (!request) {
      return { ok: false, error: INTERNAL_ERROR };
    }
    submissions.push({ request, version: item.version, versionHref: item.versionHref });
  }
  return {
    ok: true,
    data: {
      ...context, submissions,
      ...(page.window !== undefined ? { window: page.window } : {}),
      ...(page.pagination ? { pagination: page.pagination } : {}),
    },
  };
}

const PROVIDER_NAMES: Readonly<Record<ChangeReference["scheme"], string>> = {
  github: "GitHub",
  phab: "Phabricator",
  git: "Git",
};

export async function loadChangePage(
  deps: PageDependencies,
  queue: string,
  referencePath: readonly string[],
  search: WebSearchParams,
): Promise<SubmitQueueWebResult> {
  const reference = parseChangePath(referencePath);
  if (reference === null) {
    return notFound;
  }
  const latestHref = changeHref(queue, reference, reference.version !== null, deps.paths);
  const scansQueue = reference.version === null || reference.scheme === "git";
  if (search.from !== undefined || search.to !== undefined ||
    (search.page !== undefined && (typeof search.page !== "string" || search.page === "" || !scansQueue))) {
    return redirect(latestHref);
  }
  const pageToken = search.page;
  const liveWindow = defaultRequestWindow(deps.now());
  const result = deps.catalog.ok ? await loadChangeSubmissions(async () => {
    const page = await readChangeSubmissions(
      deps.gateway, queue, reference, pageToken === undefined ? { window: liveWindow } : { pageToken }, deps.paths);
    return {
      submissions: page.submissions,
      window: page.window,
      pagination: page.nextPageToken || pageToken !== undefined ? {
        nextHref: page.nextPageToken ? `${latestHref}?page=${encodeURIComponent(page.nextPageToken)}` : null,
        latestHref: pageToken !== undefined ? latestHref : null,
      } : undefined,
    };
  }, {
    queue, provider: PROVIDER_NAMES[reference.scheme],
    host: reference.host, repository: reference.repository, review: reference.review, pinnedVersion: reference.version,
    logicalHref: changeHref(queue, reference, false, deps.paths),
    window: scansQueue ? liveWindow : null,
  }, deps.gatewayOptions) : deps.catalog;
  // The gateway rejects a stale or foreign token; restart from the live scan rather than show an error.
  if (pageToken !== undefined && !result.ok && result.error === INVALID_INPUT) {
    return redirect(latestHref);
  }
  const refresh = refreshModel(result, pageToken !== undefined ? latestHref : null);
  return render({
    kind: "change",
    key: `${queue}:${reference.logicalPath}:${reference.version ?? "all"}:${pageToken ?? "live"}`,
    title: reference.review, basePath: deps.paths.basePath, result,
    // Each render of a scanning page reads up to MAX_CHANGE_SCAN_PAGES queue pages, too costly to poll.
    refresh: scansQueue ? { ...refresh, terminal: true } : refresh,
  });
}
