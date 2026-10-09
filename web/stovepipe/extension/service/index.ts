export interface RequestSummary {
  requestId: string;
  queue: string;
  changeUri: string;
  baseUri: string;
  requestState: string;
  stateUpdatedAtMs: bigint;
  acceptedAtMs: bigint;
  outcomeReason: string;
}

export interface ProjectStatus {
  requestId: string;
  queue: string;
  changeUri: string;
  baseUri: string;
  requestState: string;
  updatedAtMs: bigint;
  repositoryResult: { case: "repositoryBreakageDegree"; value: number } | { case: undefined; value?: undefined };
  projectResultsComplete: boolean;
  projects: { project: string; result: { case: "breakageDegree"; value: number } | { case: undefined; value?: undefined } }[];
  nextPageToken: string;
}

export interface HistoryEvent {
  eventId: string;
  timestampMs: bigint;
  occurrence: { case: "requestState" | "event"; value: string } | { case: undefined; value?: undefined };
  outcomeReason: string;
}

/** Hosts supply a transport client; the library has no generated-binding dependency. */
export interface StovepipeService {
  list(request: { queue: string; pageSize: number; pageToken: string }, options?: { signal?: AbortSignal }): Promise<{ requests: RequestSummary[]; nextPageToken: string }>;
  getProjectStatusByURI(request: { queue: string; changeUri: string; projects: string[]; pageSize: number; pageToken: string }, options?: { signal?: AbortSignal }): Promise<ProjectStatus>;
  getRequestHistoryByID(request: { queue: string; requestId: string }, options?: { signal?: AbortSignal }): Promise<{ events: HistoryEvent[] }>;
}
