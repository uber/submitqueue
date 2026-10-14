import "server-only";
import { createClient } from "@connectrpc/connect";
import { metrics, trace } from "@opentelemetry/api";
import { Stovepipe } from "@submitqueue/api/stovepipe";
import { createStovepipeWeb } from "@submitqueue/web-stovepipe/server";
import { pino } from "pino";
import { createPinoLogCallback } from "@submitqueue/web-service-shared/logging";
import { traceConnectRequests } from "@submitqueue/web-service-shared/instrumentation";
import { instrumentStovepipeWeb } from "./instrumentation";
import { loadConfiguration } from "./config";
import { newConnectTransport } from "@submitqueue/web-service-shared/transport";
const logger = pino({
  name: "stovepipe-web",
  level: process.env.LOG_LEVEL ?? "info",
});
export const onLog = createPinoLogCallback(logger);

let host: ReturnType<typeof createStovepipeWeb> | undefined;
export function resolveHost() {
  if (!host) {
    const configuration = loadConfiguration();
    const service = createClient(
      Stovepipe,
      newConnectTransport({
        ...configuration,
        defaultTimeoutMs: 5_000,
        interceptors: [traceConnectRequests(trace.getTracer("stovepipe-web"))],
      }),
    );
    host = instrumentStovepipeWeb(createStovepipeWeb({
      service,
      queues: configuration.queues,
    }), metrics.getMeter("stovepipe-web"), trace.getTracer("stovepipe-web"));
  }
  return host;
}
