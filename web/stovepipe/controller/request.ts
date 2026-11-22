import type { HistoryEventModel, RequestHistoryModel } from "../entity/index.js";
import type { HistoryEvent, StovepipeService } from "../extension/service/index.js";
import type { StovepipePaths } from "../core/paths.js";
import { mapRequestSummary } from "./mapping.js";

export type BuildUrlResolver = (input: { queue: string; buildId: string }) => string | undefined;

function mapHistoryBuildDetails(
  buildId: string,
  queue: string,
  resolveBuildUrl?: BuildUrlResolver,
): HistoryEventModel["details"] {
  if (!buildId) return [];
  const href = resolveBuildUrl?.({ queue, buildId });
  const protocol = href && URL.canParse(href) ? new URL(href).protocol : "";
  return [{
    label: "Build ID", value: buildId,
    ...(href && (protocol === "https:" || protocol === "http:") ? { href } : {}),
  }];
}

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
    case "buildAnalysisAbandoned":
    case "issueTrackingAbandoned":
      return mapHistoryBuildDetails(payload.value.buildId, queue, resolveBuildUrl);
    case "validationFactRecorded":
      return [{ label: "Breakage degree", value: String(payload.value.factDegree) }];
    case "projectFactsRecorded":
      return [
        { label: "Project facts", value: payload.value.projectFactCount.toString() },
        { label: "Broken project facts", value: payload.value.brokenProjectFactCount.toString() },
      ];
    case "buildAnalysisCompleted":
      return [
        ...mapHistoryBuildDetails(payload.value.buildId, queue, resolveBuildUrl),
        { label: "Issues", value: payload.value.issueCount.toString() },
      ];
    case "issuesReconciled": {
      const details = mapHistoryBuildDetails(payload.value.buildId, queue, resolveBuildUrl);
      if (payload.value.closureMode)
        details.push({ label: "Closure mode", value: payload.value.closureMode });
      return details;
    }
    case "fixSubmitted":
    case "fixSubmissionSkipped":
    case "fixSubmissionAbandoned":
      return [
        ...mapHistoryBuildDetails(payload.value.buildId, queue, resolveBuildUrl),
        ...[
          { label: "Task ID", value: payload.value.taskId },
          { label: "Unit ID", value: payload.value.unitId },
          { label: "Executor ID", value: payload.value.executorId },
          { label: "Fix ID", value: payload.value.fixId },
          { label: "Reason", value: payload.value.reason },
        ].filter((detail) => detail.value !== ""),
      ];
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
