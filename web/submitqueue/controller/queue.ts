import type { WebSearchParams } from "../core/route.js";
import type { LoadResult } from "../entity/index.js";
import type { RequestListModel, RequestSummaryModel } from "../entity/index.js";
import type { GatewayReader } from "../extension/gateway/index.js";
import { requestChangeLabels, requestChangeLinks } from "../core/change.js";
import { decodeWindowCursor, defaultRequestWindow, encodeWindowCursor, REQUEST_PAGE_SIZE } from "../core/window.js";
import { INTERNAL_ERROR, INVALID_INPUT } from "./error.js";
import { callGateway, type GatewayLoadOptions } from "./gateway-call.js";
import { mapRequestSummary } from "./mapping.js";
import { redirect, refreshModel, render, type PageDependencies, type SubmitQueueWebResult } from "./page.js";

export interface LoadRequestListInput {
  queue: string;
  receivedAtOrAfterMs: number;
  receivedBeforeMs: number;
  pageSize?: number;
  pageToken?: string;
}

function validListInput(input: LoadRequestListInput): boolean {
  const pageSize = input.pageSize ?? REQUEST_PAGE_SIZE;
  return (
    input.queue !== "" &&
    Number.isSafeInteger(input.receivedAtOrAfterMs) &&
    Number.isSafeInteger(input.receivedBeforeMs) &&
    input.receivedAtOrAfterMs < input.receivedBeforeMs &&
    Number.isInteger(pageSize) &&
    pageSize > 0
  );
}

export async function loadRequestList(
  gateway: GatewayReader,
  input: LoadRequestListInput,
  options: GatewayLoadOptions = {},
): Promise<LoadResult<RequestListModel>> {
  if (!validListInput(input)) {
    return { ok: false, error: INVALID_INPUT };
  }
  const call = await callGateway(options, "list", { queue: input.queue }, () => gateway.list({
    queue: input.queue,
    receivedAtOrAfterMs: BigInt(input.receivedAtOrAfterMs),
    receivedBeforeMs: BigInt(input.receivedBeforeMs),
    pageSize: input.pageSize ?? REQUEST_PAGE_SIZE,
    pageToken: input.pageToken ?? "",
  }));
  if (!call.ok) {
    return call;
  }
  const response = call.value;
  const requests: RequestSummaryModel[] = [];
  for (const value of response.requests) {
    const request = mapRequestSummary(value);
    if (request === null) {
      return { ok: false, error: INTERNAL_ERROR };
    }
    requests.push(request);
  }
  return {
    ok: true,
    data: {
      queue: input.queue,
      receivedAtOrAfterMs: input.receivedAtOrAfterMs,
      receivedBeforeMs: input.receivedBeforeMs,
      requests,
      nextPageToken: response.nextPageToken === "" ? null : response.nextPageToken,
    },
  };
}

// Namespaced so a codec shared with other pages or modules never accepts this page's cursors elsewhere.
function queueCursorScope(queue: string): string {
  return `submitqueue:queue:${queue}`;
}

export async function loadQueuePage(
  deps: PageDependencies,
  queue: string,
  search: WebSearchParams,
): Promise<SubmitQueueWebResult> {
  const latestHref = deps.paths.requests(queue);
  if (search.from !== undefined || search.to !== undefined ||
    (search.page !== undefined && typeof search.page !== "string")) {
    return redirect(latestHref);
  }
  const pageWindow = typeof search.page === "string" ? await decodeWindowCursor(deps.cursors, queueCursorScope(queue), search.page) : undefined;
  if (search.page !== undefined && pageWindow === undefined) {
    return redirect(latestHref);
  }
  const window = pageWindow ?? defaultRequestWindow(deps.now());
  const loaded = deps.catalog.ok ? await loadRequestList(deps.gateway, {
    queue, receivedAtOrAfterMs: window.fromMs, receivedBeforeMs: window.toMs,
    pageSize: REQUEST_PAGE_SIZE, ...(window.pageToken ? { pageToken: window.pageToken } : {}),
  }, deps.gatewayOptions) : deps.catalog;
  const result = loaded.ok && loaded.data.nextPageToken ? {
    ...loaded,
    data: { ...loaded.data, nextPageToken: await encodeWindowCursor(deps.cursors, queueCursorScope(queue), window, loaded.data.nextPageToken) },
  } : loaded;
  return render({
    kind: "queue", key: `${queue}:${typeof search.page === "string" ? search.page : "live"}`, title: queue,
    basePath: deps.paths.basePath, queue, window: { fromMs: window.fromMs, toMs: window.toMs },
    paged: pageWindow !== undefined, latestHref, result, refresh: refreshModel(result, pageWindow ? latestHref : null),
    changeLinks: result.ok ? requestChangeLinks(queue, result.data.requests, deps.paths) : {},
    changeLabels: result.ok ? requestChangeLabels(result.data.requests) : {},
  });
}
