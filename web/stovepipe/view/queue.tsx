"use client";

import { PagedRequests } from "@submitqueue/web-platform";
import type { QueueRequestsModel, RequestSummaryModel } from "../entity/index.js";
import { RequestList } from "./pages.js";

const requestId = (request: RequestSummaryModel) => request.id;

export function QueueRequests({ initial, routeKey }: { initial: QueueRequestsModel; routeKey: string }) {
  return <PagedRequests initial={initial} routeKey={routeKey} requestKey={requestId}>
    {requests => <RequestList requests={requests} />}
  </PagedRequests>;
}
