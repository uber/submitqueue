import type { RequestHistoryModel } from "../entity/index.js";
import type { StovepipeService } from "../extension/service/index.js";
import type { StovepipePaths } from "../core/paths.js";
import { mapRequestSummary } from "./mapping.js";

export async function loadRequestHistory(
  service: StovepipeService,
  paths: StovepipePaths,
  queue: string,
  requestId: string,
  signal?: AbortSignal,
): Promise<RequestHistoryModel> {
  const result = await service.getRequestHistoryByID(
    { queue, requestId },
    signal ? { signal } : {},
  );
  const summary = result.requestSummary;
  if (!summary || summary.requestId !== requestId || summary.queue !== queue)
    throw new Error("History response does not identify the selected request");
  return {
    request: mapRequestSummary(summary, paths),
    events: result.events.map((event) => ({
      id: event.eventId,
      timestampMs: event.timestampMs.toString(),
      kind: event.occurrence.case ?? "unknown",
      label: event.occurrence.value ?? "Unknown event",
      outcomeReason: event.outcomeReason,
    })),
  };
}
