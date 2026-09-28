import "server-only";

import { createClient } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { SubmitQueueGateway } from "@submitqueue/api/submitqueue/gateway";
import type {
  GatewayReader,
  GatewayResolver,
} from "@submitqueue/web-submitqueue/server";
import { gatewayReader } from "@submitqueue/web-submitqueue/server";

import { GATEWAY_DEADLINE_MS, loadGatewayConfiguration } from "./config";

let reader: GatewayReader | undefined;

function createGatewayReader(): GatewayReader {
  const configuration = loadGatewayConfiguration();
  const transport = createGrpcTransport({
    baseUrl: configuration.baseUrl,
    defaultTimeoutMs: GATEWAY_DEADLINE_MS,
  });
  return gatewayReader(createClient(SubmitQueueGateway, transport));
}

export const resolveDemoGateway: GatewayResolver = () => {
  reader ??= createGatewayReader();
  return reader;
};
