import { safeInt64ToNumber } from "../core/int64.js";
import type { HistoryEventModel, RequestSummaryModel } from "../entity/index.js";
import type { GatewayHistoryEvent, GatewayRequestSummary } from "../extension/gateway/index.js";

export function mapRequestSummary(value: GatewayRequestSummary): RequestSummaryModel | null {
  const receivedAtMs = safeInt64ToNumber(value.receivedAtMs);
  if (receivedAtMs === null) {
    return null;
  }
  return {
    sqid: value.sqid,
    queue: value.queue,
    changeUris: [...value.changeUris],
    receivedAtMs,
    status: value.status,
    lastError: value.lastError === "" ? null : value.lastError,
    metadata: { ...value.metadata },
  };
}

export function mapHistoryEvent(value: GatewayHistoryEvent): HistoryEventModel | null {
  const timestampMs = safeInt64ToNumber(value.timestampMs);
  if (timestampMs === null) {
    return null;
  }
  const type = value.type === "status" || value.type === "event" ? value.type : "unknown";
  return {
    timestampMs,
    type,
    status: value.status === "" ? null : value.status,
    event: value.event === "" ? null : value.event,
    lastError: value.lastError === "" ? null : value.lastError,
    metadata: { ...value.metadata },
  };
}
