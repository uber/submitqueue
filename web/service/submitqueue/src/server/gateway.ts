import "server-only";

import { createClient, type Client } from "@connectrpc/connect";
import { SubmitQueueGateway } from "@submitqueue/api/submitqueue/gateway";
import { newConnectTransport } from "./transport";
import { GATEWAY_DEADLINE_MS, GATEWAY_HEALTH_DEADLINE_MS, loadGatewayConfiguration } from "./config";

let client: Client<typeof SubmitQueueGateway> | undefined;

/** The process-wide gateway client; the demo's visibility does not depend on the viewer. */
export function resolveReferenceGateway(): Client<typeof SubmitQueueGateway> {
  client ??= createClient(SubmitQueueGateway, newConnectTransport({
    ...loadGatewayConfiguration(),
    defaultTimeoutMs: GATEWAY_DEADLINE_MS,
  }));
  return client;
}

export async function pingReferenceGateway(): Promise<void> {
  await resolveReferenceGateway().ping(
    { message: "submitqueue-web-readiness" },
    { timeoutMs: GATEWAY_HEALTH_DEADLINE_MS },
  );
}
