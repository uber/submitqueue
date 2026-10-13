import type { LoadResult, QueueModel, SubmitQueuePageModel, WebRefreshModel } from "../entity/index.js";
import type { SubmitQueueGateway } from "../extension/gateway/index.js";
import type { WebPaths } from "../core/paths.js";
import type { CursorCodec } from "../extension/cursor/index.js";
import type { GatewayLoadOptions } from "./gateway-call.js";

/** Outcome of one page request; only `render` carries a page. */
export type SubmitQueueWebResult =
  | { kind: "render"; model: SubmitQueuePageModel }
  | { kind: "redirect"; href: string }
  | { kind: "not-found" }
  | { kind: "forbidden" };

/** Dependencies of one page load, resolved for the current request. */
export interface PageDependencies {
  gateway: SubmitQueueGateway;
  cursors: CursorCodec;
  paths: WebPaths;
  now(): number;
  gatewayOptions: GatewayLoadOptions;
  /** Queues the viewer may see; a failure is shown instead of the page's own data. */
  catalog: LoadResult<QueueModel[]>;
}

export function refreshModel<T>(result: LoadResult<T>, refreshHref: string | null = null): WebRefreshModel {
  return {
    terminal: refreshHref !== null || (!result.ok && !result.error.retryable),
    transientFailureCount: !result.ok && result.error.retryable ? 1 : 0,
    refreshHref,
  };
}

export const render = (model: SubmitQueuePageModel): SubmitQueueWebResult => ({ kind: "render", model });
export const redirect = (href: string): SubmitQueueWebResult => ({ kind: "redirect", href });
export const notFound: SubmitQueueWebResult = { kind: "not-found" };
