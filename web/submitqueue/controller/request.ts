import type { WebSearchParams } from "../core/route.js";
import type { LoadResult } from "../entity/index.js";
import type { HistoryEventModel, RequestDetailModel } from "../entity/index.js";
import type { GatewayReader } from "../extension/gateway/index.js";
import { requestChangeLabels, requestChangeLinks } from "../core/change.js";
import { isTerminalStatus } from "../core/status.js";
import { INTERNAL_ERROR, INVALID_INPUT } from "./error.js";
import { callGateway, type GatewayLoadOptions } from "./gateway-call.js";
import { mapHistoryEvent, mapRequestSummary } from "./mapping.js";
import { notFound, redirect, refreshModel, render, type PageDependencies, type SubmitQueueWebResult } from "./page.js";

export interface LoadRequestDetailInput {
  queue: string;
  sqid: string;
}

export function requestDetailIsComplete(model: RequestDetailModel): boolean {
  if (model.historyError !== null || !isTerminalStatus(model.request.status)) {
    return false;
  }
  return model.history.some(
    (event) => event.type === "status" && event.status === model.request.status,
  );
}

export interface RequestDetailRefreshState {
  terminal: boolean;
  transientFailureCount: number;
}

/**
 * Stops polling once a terminal summary is matched by loaded history, or when
 * a terminal summary pairs with a non-retryable history error that polling
 * cannot fix.
 */
export function requestDetailRefreshState(model: RequestDetailModel): RequestDetailRefreshState {
  const historyError = model.historyError;
  return {
    terminal:
      requestDetailIsComplete(model) ||
      (isTerminalStatus(model.request.status) && historyError !== null && !historyError.retryable),
    transientFailureCount: historyError?.retryable ? 1 : 0,
  };
}

export async function loadRequestDetail(
  gateway: GatewayReader,
  input: LoadRequestDetailInput,
  options: GatewayLoadOptions = {},
): Promise<LoadResult<RequestDetailModel>> {
  if (input.queue === "" || input.sqid === "") {
    return { ok: false, error: INVALID_INPUT };
  }
  const [summaryResult, historyResult] = await Promise.all([
    callGateway(options, "summary", input, () => gateway.getRequestSummaryByID(input)),
    callGateway(options, "history", input, () => gateway.getRequestHistoryByID(input)),
  ]);
  if (!summaryResult.ok) {
    return summaryResult;
  }
  if (summaryResult.value.request === undefined) {
    return { ok: false, error: INTERNAL_ERROR };
  }
  const request = mapRequestSummary(summaryResult.value.request);
  if (request === null) {
    return { ok: false, error: INTERNAL_ERROR };
  }
  if (!historyResult.ok) {
    return { ok: true, data: { request, history: [], historyError: historyResult.error } };
  }
  const history: HistoryEventModel[] = [];
  for (const value of historyResult.value.events) {
    const event = mapHistoryEvent(value);
    if (event === null) {
      return { ok: true, data: { request, history: [], historyError: INTERNAL_ERROR } };
    }
    history.push(event);
  }
  return { ok: true, data: { request, history, historyError: null } };
}

export async function loadRequestPage(
  deps: PageDependencies,
  queue: string,
  sqid: string,
  search: WebSearchParams,
): Promise<SubmitQueueWebResult> {
  const view = search.view === "history" ? "history" : "summary";
  if (search.from !== undefined || search.to !== undefined || search.page !== undefined) {
    return redirect(deps.paths.request(queue, sqid, { view }));
  }
  const result = deps.catalog.ok ? await loadRequestDetail(deps.gateway, { queue, sqid }, deps.gatewayOptions) : deps.catalog;
  if (!result.ok && result.error.kind === "not-found") {
    return notFound;
  }
  return render({
    kind: "request", key: `${queue}:${sqid}`, title: view === "history" ? `${sqid} · History` : sqid,
    basePath: deps.paths.basePath, view, result,
    backHref: deps.paths.requests(queue), summaryHref: deps.paths.request(queue, sqid),
    historyHref: deps.paths.request(queue, sqid, { view: "history" }),
    changeLinks: result.ok ? requestChangeLinks(queue, [result.data.request], deps.paths) : {},
    changeLabels: result.ok ? requestChangeLabels([result.data.request]) : {},
    refresh: result.ok ? { ...requestDetailRefreshState(result.data), refreshHref: null } : refreshModel(result),
  });
}
