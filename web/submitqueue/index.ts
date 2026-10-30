"use client";

// Client-safe entry: the app, page frames, navigation, and page-model types. Loading lives in "./server".
export {
  SubmitQueueApp, SubmitQueuePage, SubmitQueueShell, SubmitQueueStatePage,
  type SubmitQueuePageProps, type SubmitQueueShellProps,
} from "./view/index.js";
export {
  RequestPagesProvider, WebNavigationProvider, type RequestPageLoader, type WebLinkProps, type WebNavigation,
} from "@submitqueue/web-platform";
export type {
  ChangeDetailModel, ChangePageModel, ChangeSubmissionModel, HistoryEventModel, HistoryEventType, LoadResult,
  QueueDirectoryPageModel, QueueModel, QueueRequestsModel, QueueRequestsPageModel, RequestDetailModel, RequestListModel,
  RequestPageModel, RequestSummaryModel, StatusDisplay, StatusTone, SubmitQueuePageModel, WebError, WebErrorKind,
  WebPageModel, WebRefreshModel,
} from "./entity/index.js";
