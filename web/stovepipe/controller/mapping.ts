import type { RequestSummaryModel } from "../entity/index.js";
import type { RequestSummary } from "../extension/service/index.js";
import type { StovepipePaths } from "../core/paths.js";

export function mapRequestSummary(request: RequestSummary, paths: StovepipePaths): RequestSummaryModel {
  return { id: request.requestId, href: paths.change(request.queue, request.changeUri), changeUri: request.changeUri,
    baseUri: request.baseUri, state: request.requestState, updatedAtMs: request.stateUpdatedAtMs.toString(),
    acceptedAtMs: request.acceptedAtMs.toString(), outcomeReason: request.outcomeReason };
}
