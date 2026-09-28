export type StatusTone = "neutral" | "progress" | "success" | "danger" | "warning";

export interface StatusDisplay {
  label: string;
  tone: StatusTone;
  terminal: boolean;
}

export interface RequestSummaryModel {
  sqid: string;
  queue: string;
  changeUris: string[];
  receivedAtMs: number;
  status: string;
  lastError: string | null;
  metadata: Record<string, string>;
}

export type HistoryEventType = "status" | "event" | "unknown";

export interface HistoryEventModel {
  timestampMs: number;
  type: HistoryEventType;
  status: string | null;
  event: string | null;
  lastError: string | null;
  metadata: Record<string, string>;
}

export interface RequestListModel {
  queue: string;
  receivedAtOrAfterMs: number;
  receivedBeforeMs: number;
  requests: RequestSummaryModel[];
  nextPageToken: string | null;
}

export interface RequestDetailModel {
  request: RequestSummaryModel;
  history: HistoryEventModel[];
  historyError: WebError | null;
}

export type WebErrorKind = "user" | "not-found" | "transient" | "internal";

export interface WebError {
  kind: WebErrorKind;
  title: string;
  message: string;
  retryable: boolean;
}

export type LoadResult<T> =
  | { ok: true; data: T }
  | { ok: false; error: WebError };
