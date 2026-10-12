export interface Queue {
  /** Queue name, preserved verbatim in the UI. */
  name: string;
}
export type ReadResult<T> =
  | { ok: true; data: T }
  | { ok: false; error: string };
export interface RequestSummaryModel {
  /** Request identifier within its queue. */
  id: string;
  /** In-app URL of this request. */
  href: string;
  /** Exact URI of the ingested commit. */
  changeUri: string;
  /** Baseline URI, empty when none is selected. */
  baseUri: string;
  /** Backend lifecycle label, including future values. */
  state: string;
  /** Unix milliseconds as a decimal string, preserving wire int64 precision. */
  updatedAtMs: string;
  /** Original acceptance time in Unix milliseconds; zero means unknown. */
  acceptedAtMs: string;
  /** Stable backend outcome reason, empty when absent. */
  outcomeReason: string;
}
export interface HistoryEventModel {
  /** Stable event identifier within the request. */
  id: string;
  /** Occurrence time in Unix milliseconds. */
  timestampMs: string;
  /** Occurrence type, independent of the lifecycle label. */
  kind: "requestState" | "event" | "unknown";
  /** Backend state or event label, shown verbatim. */
  label: string;
  /** Backend reason associated with this occurrence. */
  outcomeReason: string;
}
export interface RequestHistoryModel {
  /** Current materialized lifecycle summary returned with the history. */
  request: RequestSummaryModel;
  /** Retained events ordered by occurrence time and stable event identity. */
  events: HistoryEventModel[];
}
interface PageBase {
  /** Route and cursor identity; independent of refresh time. */
  key: string;
  /** Selected queue name. */
  queue: string;
  /** Configured queue names and their list URLs. */
  queues: { name: string; href: string }[];
  /** Selected queue's latest request list. */
  queueHref: string;
  /** This view without pagination. */
  latestHref: string;
  /** Whether this view has a pagination continuation. */
  showLatestLink: boolean;
  /** Page load time in Unix milliseconds. */
  loadedAtMs: number;
}
export type StovepipePageModel = PageBase &
  (
    | {
        kind: "queue";
        requests: ReadResult<{
          requests: RequestSummaryModel[];
          nextHref: string | null;
        }>;
      }
    | {
        kind: "request";
        requestId: string;
        history: ReadResult<RequestHistoryModel>;
      }
  );
