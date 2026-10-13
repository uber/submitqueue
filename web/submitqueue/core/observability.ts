import { metrics, trace, type Attributes, type Meter, type Tracer } from "@opentelemetry/api";
import { pino, type Logger } from "pino";

/** Discards every entry; the default when a host supplies no logger. */
export const silentLogger: Logger = pino({ level: "silent" });

/** Instrumentation scope for meters and tracers the web packages create by default. */
export const INSTRUMENTATION_SCOPE = "submitqueue-web";

/** Latency histogram bucket boundaries in milliseconds, for gateway reads and page loads. */
export const LATENCY_BUCKETS_MS = [5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000];

export function defaultMeter(): Meter {
  return metrics.getMeter(INSTRUMENTATION_SCOPE);
}

export function defaultTracer(): Tracer {
  return trace.getTracer(INSTRUMENTATION_SCOPE);
}

export interface Operation {
  /** Records the operation's latency and outcome; call exactly once. */
  end(outcome: string, attributes?: Attributes): void;
}

/**
 * Starts timing one operation. `end` records `<name>.duration` (milliseconds)
 * tagged with the start attributes plus `outcome`, the counterpart of the Go
 * services' `metrics.Begin`.
 */
export function beginOperation(meter: Meter, name: string, attributes: Attributes = {}): Operation {
  const startedAt = performance.now();
  const duration = meter.createHistogram(`${name}.duration`, {
    unit: "ms",
    advice: { explicitBucketBoundaries: LATENCY_BUCKETS_MS },
  });
  return {
    end(outcome, extra = {}) {
      duration.record(Math.max(0, performance.now() - startedAt), { ...attributes, ...extra, outcome });
    },
  };
}
