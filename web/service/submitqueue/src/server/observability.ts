import "server-only";

import { metrics, type Meter } from "@opentelemetry/api";
import { PrometheusExporter } from "@opentelemetry/exporter-prometheus";
import { MeterProvider } from "@opentelemetry/sdk-metrics";
import { pino, type Logger } from "pino";
import { loadMetricsPort } from "./config";

let logger: Logger | undefined;

/** The process-wide reference logger: JSON lines on stdout at `LOG_LEVEL` (default `info`). */
export function resolveReferenceLogger(): Logger {
  logger ??= pino({ name: "submitqueue-web", level: process.env.LOG_LEVEL ?? "info" });
  return logger;
}

let meter: Meter | undefined;

/**
 * The process-wide reference meter. The first call registers a
 * Prometheus-exporting provider when `SUBMITQUEUE_WEB_METRICS_PORT` is set;
 * otherwise metrics stay on the OpenTelemetry API's no-op default.
 */
export function resolveReferenceMeter(): Meter {
  if (meter) {
    return meter;
  }
  const port = loadMetricsPort();
  if (port !== null) {
    metrics.setGlobalMeterProvider(new MeterProvider({ readers: [new PrometheusExporter({ port })] }));
  }
  meter = metrics.getMeter("submitqueue-web");
  return meter;
}
