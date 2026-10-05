import "server-only";

import { createClient, type Client } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { SubmitQueueGateway } from "@submitqueue/api/submitqueue/gateway";
import type {
  GatewayReader,
  GatewayResolver,
} from "@submitqueue/web-submitqueue/server";
import { gatewayReader } from "@submitqueue/web-submitqueue/server";

import {
  GATEWAY_DEADLINE_MS,
  GATEWAY_HEALTH_DEADLINE_MS,
  loadGatewayConfiguration,
} from "./config";

let client: Client<typeof SubmitQueueGateway> | undefined;
let reader: GatewayReader | undefined;

export function resolveGatewayClient(): Client<typeof SubmitQueueGateway> {
  if (client) {
    return client;
  }
  const configuration = loadGatewayConfiguration();
  const transport = createGrpcTransport({
    baseUrl: configuration.baseUrl,
    defaultTimeoutMs: GATEWAY_DEADLINE_MS,
  });
  client = createClient(SubmitQueueGateway, transport);
  return client;
}

export const resolveDemoGateway: GatewayResolver = () => {
  reader ??= gatewayReader(resolveGatewayClient());
  return reader;
};

export async function pingDemoGateway(): Promise<void> {
  await resolveGatewayClient().ping(
    { message: "submitqueue-web-readiness" },
    { timeoutMs: GATEWAY_HEALTH_DEADLINE_MS },
  );
}
