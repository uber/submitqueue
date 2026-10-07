"use client";

// Client-safe entry: the app, page frames, navigation, and page-model types. Loading lives in "./server".
export {
  SubmitQueueApp, SubmitQueuePage, SubmitQueueShell, SubmitQueueStatePage,
  type SubmitQueuePageProps, type SubmitQueueShellProps,
} from "./view/index.js";
export { WebNavigationProvider, type WebLinkProps, type WebNavigation } from "./view/navigation.js";
export type {
  ChangeDetailModel, ChangePageModel, ChangeSubmissionModel, HistoryEventModel, HistoryEventType, LoadResult,
  QueueDirectoryPageModel, QueueModel, QueueRequestsPageModel, RequestDetailModel, RequestListModel,
  RequestPageModel, RequestSummaryModel, StatusDisplay, StatusTone, SubmitQueuePageModel, WebError, WebErrorKind,
  WebPageModel, WebRefreshModel,
} from "./entity/index.js";
