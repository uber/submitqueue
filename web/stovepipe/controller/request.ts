import type { HistoryEventModel, RequestHistoryModel } from "../entity/index.js";
import type { HistoryEvent, StovepipeService } from "../extension/service/index.js";
import type { StovepipePaths } from "../core/paths.js";
import { mapRequestSummary } from "./mapping.js";

export type BuildUrlResolver = (input: { queue: string; buildId: string }) => string | undefined;

function mapHistoryDetails(
  event: HistoryEvent,
  queue: string,
  resolveBuildUrl?: BuildUrlResolver,
): HistoryEventModel["details"] {
  const payload = event.data?.payload;
  switch (payload?.case) {
    case "requestState":
    case "buildTriggered":
    case "buildFinished":
    case "buildAnalysisAbandoned": {
      const buildId = payload.value.buildId;
      if (!buildId) return [];
      const href = resolveBuildUrl?.({ queue, buildId });
      const protocol = href && URL.canParse(href) ? new URL(href).protocol : "";
      return [{
        label: "Build ID", value: buildId,
        ...(href && (protocol === "https:" || protocol === "http:") ? { href } : {}),
      }];
    }
    case "validationFactRecorded":
      return [{ label: "Breakage degree", value: String(payload.value.factDegree) }];
    case "projectFactsRecorded":
      return [{ label: "Project facts", value: payload.value.projectFactCount.toString() }];
    default:
      return [];
  }
}

export async function loadRequestHistory(
  service: StovepipeService,
  paths: StovepipePaths,
  queue: string,
  requestId: string,
  resolveBuildUrl?: BuildUrlResolver,
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
      details: mapHistoryDetails(event, queue, resolveBuildUrl),
    })),
  };
}
