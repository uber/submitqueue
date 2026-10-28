// Server entry: the page handler and the contracts a host implements for it.
export {
  createSubmitQueueWeb,
  type QueueCatalogCache, type QueueRequestsResult, type SubmitQueueQueueRequestsRequest, type SubmitQueueRequestContext,
  type SubmitQueueWeb, type SubmitQueueWebOptions, type SubmitQueueWebRequest, type SubmitQueueWebResult,
} from "./module.js";
export * from "./extension/gateway/index.js";
export { grpcStatusErrorCode } from "./extension/gateway/grpcstatus/index.js";
export type * from "./entity/index.js";
export { decodeResourceSegment as decodeQueuePathSegment, type SubmitQueueWebRoute, type WebSearchParams } from "./core/route.js";
export { WebPaths, type RequestPathOptions, type RequestsPathOptions, type WebPathsOptions } from "./core/paths.js";
