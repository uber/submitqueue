import { SpanStatusCode, type Meter, type Tracer } from "@opentelemetry/api";
import type { Interceptor } from "@connectrpc/connect";
import { grpcStatusErrorCode, type SubmitQueueWeb } from "@submitqueue/web-submitqueue/server";
import { LATENCY_BUCKETS_MS, observeTelemetry, withSpan } from "@submitqueue/web-service-shared/instrumentation";

export function instrumentSubmitQueueWeb(web: SubmitQueueWeb, meter: Meter, tracer: Tracer): SubmitQueueWeb {
  return {
    handle: request => withSpan(tracer, "web.page_load", async span => {
      const started = performance.now();
      let outcome: string = "exception";
      let page = "unknown";
      try {
        const result = await web.handle(request);
        outcome = result.kind;
        if (result.kind === "render") {
          page = result.model.kind;
          if (!result.model.result.ok) {
            outcome = result.model.result.error.kind;
            observeTelemetry(() => span?.setStatus({ code: SpanStatusCode.ERROR }));
          }
        }
        return result;
      } finally {
        observeTelemetry(() => span?.setAttribute("web.page", page));
        observeTelemetry(() => span?.setAttribute("web.outcome", outcome));
        observeTelemetry(() => meter.createHistogram("web.page_load.duration", {
          unit: "ms",
          advice: { explicitBucketBoundaries: LATENCY_BUCKETS_MS },
        }).record(Math.max(0, performance.now() - started), { page, outcome }));
      }
    }),
    queueRequests: request => withSpan(tracer, "web.queue_requests", async span => {
      const started = performance.now();
      let outcome: string = "exception";
      try {
        const result = await web.queueRequests(request);
        outcome = result.kind === "unavailable" ? result.error.kind : result.kind;
        if (result.kind === "unavailable") {
          observeTelemetry(() => span?.setStatus({ code: SpanStatusCode.ERROR }));
        }
        return result;
      } finally {
        observeTelemetry(() => span?.setAttribute("web.outcome", outcome));
        observeTelemetry(() => meter.createHistogram("web.queue_requests.duration", {
          unit: "ms",
          advice: { explicitBucketBoundaries: LATENCY_BUCKETS_MS },
        }).record(Math.max(0, performance.now() - started), { outcome }));
      }
    }),
  };
}

const gatewayOperations: Readonly<Record<string, string>> = {
  List: "list",
  ListQueues: "queues",
  GetRequestSummaryByID: "summary",
  GetRequestHistoryByID: "history",
  GetRequestHistoryByChangeURI: "change",
};

export function measureGatewayRequests(meter: Meter): Interceptor {
  return next => async request => {
    const operation = gatewayOperations[request.method.name];
    if (!operation) return next(request);
    const started = performance.now();
    let outcome = "success";
    try {
      return await next(request);
    } catch (error) {
      outcome = grpcStatusErrorCode(error);
      throw error;
    } finally {
      observeTelemetry(() => meter.createHistogram("submitqueue_web.gateway.duration", {
        unit: "ms",
        advice: { explicitBucketBoundaries: LATENCY_BUCKETS_MS },
      }).record(Math.max(0, performance.now() - started), { operation, outcome }));
    }
  };
}
