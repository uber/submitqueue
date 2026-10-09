import type { LoadResult } from "../entity/index.js";
import type { QueueModel } from "../entity/index.js";
import type { GatewayQueueReader } from "../extension/gateway/index.js";
import { INTERNAL_ERROR } from "./error.js";
import { callGateway, type GatewayLoadOptions } from "./gateway-call.js";
import { refreshModel, render, type PageDependencies, type SubmitQueueWebResult } from "./page.js";

export async function loadQueueDirectory(
  gateway: GatewayQueueReader,
  options: GatewayLoadOptions = {},
): Promise<LoadResult<QueueModel[]>> {
  const call = await callGateway(options, "queues", { queue: "" }, () => gateway.listQueues({}));
  if (!call.ok) {
    return call;
  }
  const response = call.value;
  const names = new Set<string>();
  const queues: QueueModel[] = [];
  for (const queue of response.queues) {
    if (!queue.name || names.has(queue.name)) {
      return { ok: false, error: INTERNAL_ERROR };
    }
    names.add(queue.name);
    queues.push({ name: queue.name, description: "" });
  }
  return { ok: true, data: queues };
}

export function loadDirectoryPage(deps: PageDependencies): SubmitQueueWebResult {
  return render({
    kind: "queues", key: "queues", title: "Queues", basePath: deps.paths.basePath,
    result: deps.catalog, refresh: refreshModel(deps.catalog),
  });
}
