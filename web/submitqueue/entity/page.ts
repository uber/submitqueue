import type { LoadResult } from "./result.js";
import type { ChangeDetailModel } from "./change.js";
import type { QueueModel } from "./queue.js";
import type { RequestDetailModel, RequestListModel } from "./request.js";

/** How the client keeps a rendered page current. */
export interface WebRefreshModel {
  /** Whether polling has stopped because the page can no longer change, or is too costly to poll. */
  terminal: boolean;
  /** Consecutive retryable failures behind this page; drives polling backoff. */
  transientFailureCount: number;
  /** Page a refresh moves to instead of reloading this one, or `null` to reload in place. */
  refreshHref: string | null;
}

/** Fields every serializable page model carries; `WebApp` relies on exactly these. */
export interface WebPageModel {
  /** Identity of the rendered resource; changes whenever client state must reset. */
  key: string;
  /** Document title for the page, without any host-specific suffix. */
  title: string;
  /** Refresh policy for the page. */
  refresh: WebRefreshModel;
}

/** Fields shared by every SubmitQueue page model. */
interface SubmitQueuePagePresentation extends WebPageModel {
  /** Path prefix of every in-app link; empty when mounted at the root. */
  basePath: string;
}

/** The queue directory. */
export interface QueueDirectoryPageModel extends SubmitQueuePagePresentation {
  /** Page discriminator. */
  kind: "queues";
  /** Configured queues, or why they could not be listed. */
  result: LoadResult<QueueModel[]>;
}

/** Requests one queue received in a 24-hour window. */
export interface QueueRequestsPageModel extends SubmitQueuePagePresentation {
  /** Page discriminator. */
  kind: "queue";
  /** Queue name. */
  queue: string;
  /** Half-open receipt window shown, Unix epoch milliseconds. */
  window: { fromMs: number; toMs: number };
  /** Whether this is an older snapshot page rather than the live first page. */
  paged: boolean;
  /** In-app link to the live first page. */
  latestHref: string;
  /** The page of requests, or why it could not be loaded. */
  result: LoadResult<RequestListModel>;
  /** In-app links for change URIs that have a change page, keyed by URI. */
  changeLinks: Record<string, string>;
  /** Display labels for change URIs, keyed by URI; unlabeled URIs show verbatim. */
  changeLabels: Record<string, string>;
}

/** One request's summary or history. */
export interface RequestPageModel extends SubmitQueuePagePresentation {
  /** Page discriminator. */
  kind: "request";
  /** Selected tab. */
  view: "summary" | "history";
  /** The request and its history, or why it could not be loaded. */
  result: LoadResult<RequestDetailModel>;
  /** In-app link back to the request's queue. */
  backHref: string;
  /** In-app link to the summary tab. */
  summaryHref: string;
  /** In-app link to the history tab. */
  historyHref: string;
  /** In-app links for change URIs that have a change page, keyed by URI. */
  changeLinks: Record<string, string>;
  /** Display labels for change URIs, keyed by URI. */
  changeLabels: Record<string, string>;
}

/** Submissions of one change. */
export interface ChangePageModel extends SubmitQueuePagePresentation {
  /** Page discriminator. */
  kind: "change";
  /** The change's submissions, or why they could not be loaded. */
  result: LoadResult<ChangeDetailModel>;
}

/** Any serializable SubmitQueue page. */
export type SubmitQueuePageModel =
  | QueueDirectoryPageModel
  | QueueRequestsPageModel
  | RequestPageModel
  | ChangePageModel;
