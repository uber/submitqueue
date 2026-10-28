import type { WebSearchParams } from "../core/route.js";
import type { LoadResult } from "../entity/index.js";
import type { QueueRequestsModel, RequestListModel, RequestSummaryModel } from "../entity/index.js";
import type { GatewayReader } from "../extension/gateway/index.js";
import { allReceiptsWindow, REQUEST_PAGE_SIZE, type RequestPageStart } from "../core/window.js";
import { INTERNAL_ERROR, INVALID_INPUT } from "./error.js";
import { callGateway, type GatewayLoadOptions } from "./gateway-call.js";
import { mapRequestSummary, mapResponseWindow } from "./mapping.js";
import { redirect, refreshModel, render, type PageDependencies, type SubmitQueueWebResult } from "./page.js";

export type LoadRequestListInput = RequestPageStart & {
  queue: string;
  pageSize?: number;
  /** Current statuses to include; empty or absent includes every status. */
  statuses?: readonly string[];
};

function validListInput(input: LoadRequestListInput): boolean {
  const pageSize = input.pageSize ?? REQUEST_PAGE_SIZE;
  const validStart = "pageToken" in input
    ? input.pageToken !== ""
    : Number.isSafeInteger(input.window.fromMs) && Number.isSafeInteger(input.window.toMs) && input.window.fromMs < input.window.toMs;
  return input.queue !== "" && validStart && Number.isInteger(pageSize) && pageSize > 0;
}

/**
 * The gateway `list` input for one page; bounds are sent only when no token carries them. Statuses are sent with a
 * token too, so the gateway rejects a URL whose filter disagrees with its token.
 */
export function gatewayListInput(
  queue: string, start: RequestPageStart, pageSize = REQUEST_PAGE_SIZE, statuses: readonly string[] = [],
): Parameters<GatewayReader["list"]>[0] {
  const filter = statuses.length > 0 ? { statuses: [...statuses] } : {};
  if ("pageToken" in start) {
    return { queue, pageSize, pageToken: start.pageToken, ...filter };
  }
  return {
    queue, pageSize, pageToken: "", ...filter,
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
    gateway.list(gatewayListInput(input.queue, input, input.pageSize, input.statuses)));
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
  const requested = search.status === undefined ? [] : typeof search.status === "string" ? [search.status] : search.status;
  const statuses = [...new Set(requested)].sort();
  const latestHref = deps.paths.requests(queue, { statuses });
  if (statuses.includes("") || search.from !== undefined || search.to !== undefined ||
    (search.page !== undefined && (typeof search.page !== "string" || search.page === ""))) {
    return redirect(deps.paths.requests(queue, { statuses: statuses.filter(status => status !== "") }));
  }
  if (statuses.join("\u0000") !== requested.join("\u0000")) {
    return redirect(deps.paths.requests(queue, { statuses, pageToken: typeof search.page === "string" ? search.page : null }));
  }
  const pageToken = search.page;
  const loaded = deps.catalog.ok ? await loadRequestList(deps.gateway, {
    queue, pageSize: REQUEST_PAGE_SIZE, statuses,
    ...(pageToken === undefined ? { window: allReceiptsWindow(deps.now()) } : { pageToken }),
  }, deps.gatewayOptions) : deps.catalog;
  // The gateway rejects a stale or foreign token; restart from the newest requests rather than show an error.
  if (pageToken !== undefined && !loaded.ok && loaded.error === INVALID_INPUT) {
    return redirect(latestHref);
  }
  const result: LoadResult<QueueRequestsModel> = loaded.ok ? {
    ok: true,
    data: {
      requests: loaded.data.requests,
      nextHref: loaded.data.nextPageToken === null ? null : deps.paths.requests(queue, { statuses, pageToken: loaded.data.nextPageToken }),
    },
  } : loaded;
  return render({
    kind: "queue", key: `${queue}:${statuses.join(",")}:${pageToken ?? "live"}`, title: queue,
    basePath: deps.paths.basePath, queue, statuses,
    paged: pageToken !== undefined, latestHref, result, refresh: refreshModel(result, pageToken !== undefined ? latestHref : null),
  });
}
