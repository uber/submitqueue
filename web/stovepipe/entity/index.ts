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
  /** Total recorded project results; zero means no ratio to display. */
  projectCount: number;
  /** Recorded project results with nonzero breakage. */
  brokenProjectCount: number;
}
export interface QueueRequestsModel {
  /** Requests on this page, newest accepted first. */
  requests: RequestSummaryModel[];
  /** Opaque continuation URL; null on the final page. */
  nextHref: string | null;
}
export interface OpenIssueModel {
  /** Logical validation task, preserved verbatim. */
  taskId: string;
  /** Validation unit; empty identifies a task-wide failure. */
  unitId: string;
  /** Opening time in Unix milliseconds as a decimal string. */
  firstSeenAtMs: string;
  /** Request that established this opening. */
  requestId: string;
  /** In-app URL of the opening request. */
  requestHref: string;
}
export interface OpenIssuesModel {
  /** Open issues on this page, in backend task/unit order. */
  issues: OpenIssueModel[];
  /** Opaque continuation URL; null when no further row was observed. */
  nextHref: string | null;
}
export interface RequestIssuesModel {
  /** Accepted issue updates on this page, not the queue's open-issue set. */
  issues: { taskId: string; unitId: string; kind: string; buildId: string; firstSeenBuildId: string; projectsHref: string | null }[];
  /** Backend reconciliation status; completion does not imply a green build. */
  status: string;
  /** First issue page for this request; null when already on it. */
  firstHref: string | null;
  /** Opaque continuation URL; null on the final page. */
  nextHref: string | null;
}
export interface IssueProjectsModel {
  /** Projects affected by the selected issue on this page. */
  projects: string[];
  /** False means attribution is not available, not that no projects are affected. */
  complete: boolean;
  /** Return to this issue's first project page; null on that page. */
  firstHref: string | null;
  /** Opaque continuation URL; null on the final page. */
  nextHref: string | null;
}
export interface IssueProjectsSelection {
  /** Selected validation task. */
  taskId: string;
  /** Selected failed unit. */
  unitId: string;
  /** Issue and continuation identity used to scope retained results. */
  key: string;
  /** Collapse the selected issue's project list even when its read fails. */
  closeHref: string;
  /** Independent association read, so failures leave issue results visible. */
  result: ReadResult<IssueProjectsModel>;
}
export interface ProjectIssuesModel {
  /** Failed units attributed to the selected project on this page. */
  issues: { taskId: string; unitId: string }[];
  /** False means attribution is unavailable, not that the project has no issues. */
  complete: boolean;
  /** First association page for this project; null when already on it. */
  firstHref: string | null;
  /** Opaque association continuation URL; null on the final page. */
  nextHref: string | null;
}
export interface ProjectIssuesSelection {
  /** Project selected within the displayed request. */
  project: string;
  /** Project and continuation identity used to scope retained results. */
  key: string;
  /** Collapse the issue list without changing the project result page. */
  closeHref: string;
  /** Independent association read, so failures leave project statuses visible. */
  result: ReadResult<ProjectIssuesModel>;
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
  /** Labeled occurrence metadata with optional absolute HTTP(S) links; empty when unavailable. */
  details: { label: string; value: string; href?: string }[];
}
export interface RequestHistoryModel {
  /** Current materialized lifecycle summary returned with the history. */
  request: RequestSummaryModel;
  /** Retained events ordered by occurrence time and stable event identity. */
  events: HistoryEventModel[];
}
export interface ProjectStatusModel {
  /** Lifecycle state from the same response as recording completion. */
  requestState: string;
  /** Newest durable result or lifecycle record in Unix milliseconds. */
  updatedAtMs: string;
  /** Whole-repository breakage on [0, 1]; null means no recorded result. */
  repositoryBreakageDegree: number | null;
  /** Whether production of relevant project results has finished, independent of pagination. */
  complete: boolean;
  /** Maximum project results requested per page. */
  pageSize: number;
  /** Supported page sizes and their first-page URLs for this request. */
  pageSizeChoices: { value: number; href: string }[];
  /** Recorded projects on this page; null breakage means no result. */
  projects: { project: string; projectType: string; breakageDegree: number | null; issuesHref: string }[];
  /** First project page for this request; null on the first page. */
  firstHref: string | null;
  /** Opaque project continuation URL; null on the final page. */
  nextHref: string | null;
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
        requests: ReadResult<QueueRequestsModel>;
      }
    | {
        kind: "issues";
        issues: ReadResult<OpenIssuesModel>;
      }
    | {
        kind: "request";
        requestId: string;
        history: ReadResult<RequestHistoryModel>;
        projectStatus: ReadResult<ProjectStatusModel>;
        requestIssues: ReadResult<RequestIssuesModel> | null;
        issueProjects: IssueProjectsSelection | null;
        projectIssues: ProjectIssuesSelection | null;
      }
  );
