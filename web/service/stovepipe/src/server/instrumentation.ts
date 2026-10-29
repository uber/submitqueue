import { SpanStatusCode, type Meter, type Tracer } from "@opentelemetry/api";
import type { createStovepipeWeb } from "@submitqueue/web-stovepipe/server";
import { LATENCY_BUCKETS_MS, observeTelemetry, withSpan } from "@submitqueue/web-service-shared/instrumentation";

export function instrumentStovepipeWeb(
  web: ReturnType<typeof createStovepipeWeb>,
  meter: Meter,
  tracer: Tracer,
): ReturnType<typeof createStovepipeWeb> {
  return {
    handle: request => withSpan(tracer, "stovepipe.web.page_load", async span => {
      const started = performance.now();
      let outcome: string = "exception";
      try {
        const result = await web.handle(request);
        outcome = result.kind;
        if (result.kind === "render") {
          const sections = result.model.kind === "queue"
            ? [result.model.requests] : result.model.kind === "issues"
              ? [result.model.issues] : [result.model.history, result.model.projectStatus];
          if (sections.some((section) => !section.ok)) {
            outcome = "error";
            observeTelemetry(() => span?.setStatus({ code: SpanStatusCode.ERROR }));
          }
        }
        return result;
      } finally {
        observeTelemetry(() => span?.setAttribute("web.outcome", outcome));
        observeTelemetry(() => meter.createCounter("stovepipe.web.page_loads").add(1, { outcome }));
        observeTelemetry(() => meter.createHistogram("stovepipe.web.page_load_duration", {
          unit: "ms",
          advice: { explicitBucketBoundaries: LATENCY_BUCKETS_MS },
        }).record(Math.max(0, performance.now() - started), { outcome }));
      }
    }),
  };
}
