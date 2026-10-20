import type { WebSearchParams } from "../core/route.js";
import type { LoadResult } from "../entity/index.js";
import type { RequestListModel, RequestSummaryModel } from "../entity/index.js";
import type { GatewayReader } from "../extension/gateway/index.js";
import { requestChangeLabels, requestChangeLinks } from "../core/change.js";
import { defaultRequestWindow, REQUEST_PAGE_SIZE, type RequestPageStart } from "../core/window.js";
import { INTERNAL_ERROR, INVALID_INPUT } from "./error.js";
import { callGateway, type GatewayLoadOptions } from "./gateway-call.js";
import { mapRequestSummary, mapResponseWindow } from "./mapping.js";
import { redirect, refreshModel, render, type PageDependencies, type SubmitQueueWebResult } from "./page.js";

export type LoadRequestListInput = RequestPageStart & {
  queue: string;
  pageSize?: number;
};

function validListInput(input: LoadRequestListInput): boolean {
  const pageSize = input.pageSize ?? REQUEST_PAGE_SIZE;
  const validStart = "pageToken" in input
    ? input.pageToken !== ""
    : Number.isSafeInteger(input.window.fromMs) && Number.isSafeInteger(input.window.toMs) && input.window.fromMs < input.window.toMs;
  return input.queue !== "" && validStart && Number.isInteger(pageSize) && pageSize > 0;
}

/** The gateway `list` input for one page; bounds are sent only when no token carries them. */
export function gatewayListInput(queue: string, start: RequestPageStart, pageSize = REQUEST_PAGE_SIZE): Parameters<GatewayReader["list"]>[0] {
  if ("pageToken" in start) {
    return { queue, pageSize, pageToken: start.pageToken };
  }
  return {
    queue, pageSize, pageToken: "",
    receivedLowerBound: { case: "receivedAtOrAfterMs", value: BigInt(start.window.fromMs) },
    receivedUpperBound: { case: "receivedBeforeMs", value: BigInt(start.window.toMs) },
  };
}

export async function loadRequestList(
  gateway: GatewayReader,
  input: LoadRequestListInput,
  options: GatewayLoadOptions = {},
): Promise<LoadResult<RequestListModel>> {
  if (!validListInput(input)) {
    return { ok: false, error: INVALID_INPUT };
  }
  const call = await callGateway(options, "list", { queue: input.queue }, () =>
    gateway.list(gatewayListInput(input.queue, input, input.pageSize)));
  if (!call.ok) {
    return call;
  }
  const response = call.value;
  const window = mapResponseWindow(response);
  if (window === null) {
    return { ok: false, error: INTERNAL_ERROR };
  }
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
      receivedAtOrAfterMs: window.fromMs,
      receivedBeforeMs: window.toMs,
      requests,
      nextPageToken: response.nextPageToken === "" ? null : response.nextPageToken,
    },
  };
}

export async function loadQueuePage(
  deps: PageDependencies,
  queue: string,
  search: WebSearchParams,
): Promise<SubmitQueueWebResult> {
  const latestHref = deps.paths.requests(queue);
  if (search.from !== undefined || search.to !== undefined ||
    (search.page !== undefined && (typeof search.page !== "string" || search.page === ""))) {
    return redirect(latestHref);
  }
  const pageToken = search.page;
  const liveWindow = defaultRequestWindow(deps.now());
  const result = deps.catalog.ok ? await loadRequestList(deps.gateway, {
    queue, pageSize: REQUEST_PAGE_SIZE, ...(pageToken === undefined ? { window: liveWindow } : { pageToken }),
  }, deps.gatewayOptions) : deps.catalog;
  // The gateway rejects a stale or foreign token; restart from the live window rather than show an error.
  if (pageToken !== undefined && !result.ok && result.error === INVALID_INPUT) {
    return redirect(latestHref);
  }
  const window = result.ok ? { fromMs: result.data.receivedAtOrAfterMs, toMs: result.data.receivedBeforeMs } : liveWindow;
  return render({
    kind: "queue", key: `${queue}:${pageToken ?? "live"}`, title: queue,
    basePath: deps.paths.basePath, queue, window,
    paged: pageToken !== undefined, latestHref, result, refresh: refreshModel(result, pageToken !== undefined ? latestHref : null),
    changeLinks: result.ok ? requestChangeLinks(queue, result.data.requests, deps.paths) : {},
    changeLabels: result.ok ? requestChangeLabels(result.data.requests) : {},
  });
}
