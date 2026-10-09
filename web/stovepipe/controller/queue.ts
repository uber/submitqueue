import type { StovepipeService } from "../extension/service/index.js";
import type { StovepipePaths } from "../core/paths.js";
import { mapRequestSummary } from "./mapping.js";

export async function loadQueueRequests(service: StovepipeService, paths: StovepipePaths, queue: string, pageToken: string, signal?: AbortSignal) {
  const result = await service.list({ queue, pageSize: 50, pageToken }, signal ? { signal } : {});
  return {
    requests: result.requests.map(request => mapRequestSummary(request, paths)),
    nextHref: result.nextPageToken ? paths.queue(queue, result.nextPageToken) : null,
  };
}
