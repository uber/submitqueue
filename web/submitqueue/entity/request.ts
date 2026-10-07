import type { WebError } from "./result.js";

/** One SubmitQueue request as shown in lists and detail pages. */
export interface RequestSummaryModel {
  /** Queue-scoped request ID; opaque, may contain `/`. */
  sqid: string;
  /** Queue the request was submitted to. */
  queue: string;
  /** Change URIs submitted together, in submission order. */
  changeUris: string[];
  /** Receipt time, Unix epoch milliseconds. */
  receivedAtMs: number;
  /** Current lifecycle status; unknown values are kept verbatim. */
  status: string;
  /** Most recent failure message, or `null` when none was recorded. */
  lastError: string | null;
  /** Uninterpreted key/value metadata. */
  metadata: Record<string, string>;
}

/** Whether a history entry records a lifecycle status, an occurrence, or something unrecognized. */
export type HistoryEventType = "status" | "event" | "unknown";

/** One entry of a request's append-only history. */
export interface HistoryEventModel {
  /** Time the entry was recorded, Unix epoch milliseconds. */
  timestampMs: number;
  /** Entry kind. */
  type: HistoryEventType;
  /** Lifecycle status for `status` entries, otherwise `null`. */
  status: string | null;
  /** Occurrence name for `event` entries, otherwise `null`. */
  event: string | null;
  /** Failure message recorded with the entry, or `null`. */
  lastError: string | null;
  /** Uninterpreted key/value metadata. */
  metadata: Record<string, string>;
}

/** One page of requests received by a queue within a half-open window. */
export interface RequestListModel {
  /** Queue the requests belong to. */
  queue: string;
  /** Inclusive window start, Unix epoch milliseconds. */
  receivedAtOrAfterMs: number;
  /** Exclusive window end, Unix epoch milliseconds. */
  receivedBeforeMs: number;
  /** Requests on this page, newest received first. */
  requests: RequestSummaryModel[];
  /** Opaque cursor for the next page within the same window, or `null` on the last page. */
  nextPageToken: string | null;
}

/** A request with its retained history. */
export interface RequestDetailModel {
  /** The request's current summary. */
  request: RequestSummaryModel;
  /** History in recording order; empty when `historyError` is set. */
  history: HistoryEventModel[];
  /** Why history could not be loaded, or `null` when it loaded. */
  historyError: WebError | null;
}
