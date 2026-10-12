import "server-only";
import { createClient } from "@connectrpc/connect";
import { Stovepipe } from "@submitqueue/api/stovepipe";
import { createStovepipeWeb } from "@submitqueue/web-stovepipe/server";
import { pino } from "pino";
import { loadConfiguration } from "./config";
import { newConnectTransport } from "@submitqueue/web-service-shared/transport";
export const logger = pino({
  name: "stovepipe-web",
  level: process.env.LOG_LEVEL ?? "info",
});

let host: ReturnType<typeof createStovepipeWeb> | undefined;
export function resolveHost() {
  if (!host) {
    const configuration = loadConfiguration();
    const service = createClient(
      Stovepipe,
      newConnectTransport({ ...configuration, defaultTimeoutMs: 5_000 }),
    );
    host = createStovepipeWeb({
      service,
      queues: configuration.queues,
      logger,
    });
  }
  return host;
}
