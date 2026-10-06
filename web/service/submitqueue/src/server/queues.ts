import "server-only";

import type { LoadResult } from "@submitqueue/web-submitqueue";
import { loadQueueDirectory } from "@submitqueue/web-submitqueue/server";
import { gatewayDiagnostics } from "./diagnostics";
import { resolveGatewayClient } from "./gateway";

export async function loadConfiguredQueue(queue: string): Promise<LoadResult<boolean>> {
  const directory = await loadQueueDirectory(resolveGatewayClient(), { diagnostics: gatewayDiagnostics });
  return directory.ok
    ? { ok: true, data: directory.data.some(value => value.name === queue) }
    : directory;
}
