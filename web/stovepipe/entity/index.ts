export interface Queue {
  /** Queue name, preserved verbatim in the UI. */
  name: string;
  /** Nonempty consumer-selected project IDs. */
  projects: readonly string[];
}
export type ReadResult<T> = { ok: true; data: T } | { ok: false; error: string };
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
export interface ProjectStatusModel {
  /** Identity and lifecycle of the selected request. */
  request: RequestSummaryModel;
  /** Whole-repository breakage degree; null means no recorded result. */
  repositoryDegree: number | null;
  /** Backend's durable project-completion indicator. */
  complete: boolean;
  /** Selected projects on this page; null degree means no recorded result. */
  projects: { name: string; degree: number | null }[];
  /** Continuation URL; null on the final project page. */
  nextHref: string | null;
  /** First project page; null when already on it. */
  firstHref: string | null;
  /** Complete projects-only JSON, independent of the displayed page. */
  rawHref: string;
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
export type StovepipePageModel = PageBase & (
  { kind: "queue"; requests: ReadResult<{ requests: RequestSummaryModel[]; nextHref: string | null }> } |
  { kind: "request"; changeUri: string; requestId: string | null; status: ReadResult<ProjectStatusModel>; history: ReadResult<HistoryEventModel[]> }
);
