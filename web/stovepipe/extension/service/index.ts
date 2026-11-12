export interface RequestSummary {
  requestId: string;
  queue: string;
  changeUri: string;
  baseUri: string;
  requestState: string;
  stateUpdatedAtMs: bigint;
  acceptedAtMs: bigint;
  outcomeReason: string;
  projectCount: number;
  brokenProjectCount: number;
}

export interface HistoryEvent {
  eventId: string;
  timestampMs: bigint;
  occurrence:
    | { case: "requestState" | "event"; value: string }
    | { case: undefined; value?: undefined };
  outcomeReason: string;
  data?: {
    payload:
      | { case: "requestState" | "buildTriggered" | "buildFinished" | "buildAnalysisAbandoned"; value: { buildId: string } }
      | { case: "validationFactRecorded"; value: { factDegree: number } }
      | { case: "projectFactsRecorded"; value: { projectFactCount: bigint } }
      | { case: undefined; value?: undefined };
  } | undefined;
}

export interface ProjectStatus {
  queue: string;
  requestId: string;
  requestState: string;
  updatedAtMs: bigint;
  repositoryResult:
    | { case: "repositoryBreakageDegree"; value: number }
    | { case: undefined; value?: undefined };
  projectResultsComplete: boolean;
  projects: {
    project: string;
    projectType: string;
    result:
      | { case: "breakageDegree"; value: number }
      | { case: undefined; value?: undefined };
  }[];
  nextPageToken: string;
}

export interface OpenIssueSummary {
  taskId: string;
  unitId: string;
  requestId: string;
  changedAtMs: bigint;
}

export interface RequestIssueSummary {
  taskId: string;
  unitId: string;
  kind: string;
  buildId: string;
  firstSeenBuildId: string;
}

/** Hosts supply a transport client; the library has no generated-binding dependency. */
export interface StovepipeService {
  getIssuesByRequestID(
    request: { queue: string; requestId: string; pageSize: number; pageToken: string },
    options?: { signal?: AbortSignal },
  ): Promise<{ issues: RequestIssueSummary[]; status: string; nextPageToken: string }>;
  listOpenIssues(
    request: { queue: string; pageSize: number; pageToken: string },
    options?: { signal?: AbortSignal },
  ): Promise<{ issues: OpenIssueSummary[]; nextPageToken: string }>;
  list(
    request: { queue: string; pageSize: number; pageToken: string },
    options?: { signal?: AbortSignal },
  ): Promise<{ requests: RequestSummary[]; nextPageToken: string }>;
  getRequestHistoryByID(
    request: { queue: string; requestId: string },
    options?: { signal?: AbortSignal },
  ): Promise<{
    requestSummary?: RequestSummary | undefined;
    events: HistoryEvent[];
  }>;
  getProjectStatusByID(
    request: {
      queue: string;
      requestId: string;
      projects: string[];
      pageSize: number;
      pageToken: string;
    },
    options?: { signal?: AbortSignal },
  ): Promise<ProjectStatus>;
}
