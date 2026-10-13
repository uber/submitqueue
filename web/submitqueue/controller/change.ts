import type { WebSearchParams } from "../core/route.js";
import type { LoadResult } from "../entity/index.js";
import type { ChangeDetailModel } from "../entity/index.js";
import type { GatewayChangeReader, GatewayReader, GatewayRequestSummary } from "../extension/gateway/index.js";
import { changeHref, parseChangePath, parseChangeUri, type ChangeReference } from "../core/change.js";
import { WebPaths } from "../core/paths.js";
import { decodeWindowCursor, defaultRequestWindow, encodeWindowCursor, REQUEST_PAGE_SIZE, type RequestWindow } from "../core/window.js";
import { INTERNAL_ERROR, INVALID_INPUT } from "./error.js";
import { callGateway, type GatewayLoadOptions } from "./gateway-call.js";
import { mapRequestSummary } from "./mapping.js";
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
}

export interface GatewayChangeSubmissionPage {
  submissions: readonly GatewayChangeSubmission[];
  pagination?: ChangeDetailModel["pagination"];
}

/**
 * Finds the submissions of a change: by exact URI when the reference pins a
 * reviewable version, otherwise by scanning at most
 * {@link MAX_CHANGE_SCAN_PAGES} queue pages of the receipt window.
 */
export async function readChangeSubmissions(
  client: GatewayReader & GatewayChangeReader,
  queue: string,
  reference: ChangeReference,
  window: RequestWindow,
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
    return { submissions, nextPageToken: null };
  }
  let pageToken = window.pageToken ?? "";
  const seenTokens = new Set<string>(pageToken ? [pageToken] : []);
  for (let page = 0; page < MAX_CHANGE_SCAN_PAGES; page += 1) {
    const response = await client.list({
      queue,
      receivedAtOrAfterMs: BigInt(window.fromMs),
      receivedBeforeMs: BigInt(window.toMs),
      pageSize: REQUEST_PAGE_SIZE,
      pageToken,
    });
    appendSubmissions(response.requests);
    if (!response.nextPageToken) {
      return { submissions, nextPageToken: null };
    }
    if (seenTokens.has(response.nextPageToken)) {
      // A gateway contract violation; continuing would present duplicate history.
      throw new Error("The gateway repeated a queue page token");
    }
    pageToken = response.nextPageToken;
    seenTokens.add(pageToken);
  }
  return { submissions, nextPageToken: pageToken };
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
  return { ok: true, data: { ...context, submissions, ...(page.pagination ? { pagination: page.pagination } : {}) } };
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
    (search.page !== undefined && (typeof search.page !== "string" || !scansQueue))) {
    return redirect(latestHref);
  }
  const scope = `submitqueue:change:${JSON.stringify([queue, reference.logicalPath, reference.version])}`;
  const pageWindow = typeof search.page === "string" ? await decodeWindowCursor(deps.cursors, scope, search.page) : undefined;
  if (search.page !== undefined && pageWindow === undefined) {
    return redirect(latestHref);
  }
  const window = pageWindow ?? defaultRequestWindow(deps.now());
  const result = deps.catalog.ok ? await loadChangeSubmissions(async () => {
    const page = await readChangeSubmissions(deps.gateway, queue, reference, window, deps.paths);
    return {
      submissions: page.submissions,
      pagination: page.nextPageToken || pageWindow ? {
        nextHref: page.nextPageToken
          ? `${latestHref}?page=${await encodeWindowCursor(deps.cursors, scope, window, page.nextPageToken)}` : null,
        latestHref: pageWindow ? latestHref : null,
      } : undefined,
    };
  }, {
    queue, provider: PROVIDER_NAMES[reference.scheme],
    host: reference.host, repository: reference.repository, review: reference.review, pinnedVersion: reference.version,
    logicalHref: changeHref(queue, reference, false, deps.paths),
    window: scansQueue ? { fromMs: window.fromMs, toMs: window.toMs } : null,
  }, deps.gatewayOptions) : deps.catalog;
  const refresh = refreshModel(result, pageWindow ? latestHref : null);
  return render({
    kind: "change",
    key: `${queue}:${reference.logicalPath}:${reference.version ?? "all"}:${typeof search.page === "string" ? search.page : "live"}`,
    title: reference.review, basePath: deps.paths.basePath, result,
    // Each render of a scanning page reads up to MAX_CHANGE_SCAN_PAGES queue pages, too costly to poll.
    refresh: scansQueue ? { ...refresh, terminal: true } : refresh,
  });
}
