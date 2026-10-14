import { context, propagation, trace, SpanKind, SpanStatusCode, type Span, type SpanOptions, type Tracer } from "@opentelemetry/api";
import type { Interceptor } from "@connectrpc/connect";

export const LATENCY_BUCKETS_MS = [5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000];

export function observeTelemetry(deliver: () => void): void {
  try {
    deliver();
  } catch {
    // A telemetry failure must not change the business result.
  }
}

/** Uses the host's provider and active context; never repeats work on telemetry failure. */
export async function withSpan<T>(
  tracer: Tracer,
  name: string,
  execute: (span: Span | undefined) => Promise<T>,
  options: SpanOptions = {},
): Promise<T> {
  let span: Span | undefined;
  observeTelemetry(() => { span = tracer.startSpan(name, options); });
  let execution: Promise<T> | undefined;
  const run = () => execution ??= Promise.resolve().then(() => execute(span));
  try {
    const activeSpan = span;
    if (activeSpan) {
      observeTelemetry(() => { context.with(trace.setSpan(context.active(), activeSpan), run); });
    }
    return await run();
  } catch (error) {
    observeTelemetry(() => span?.recordException(error instanceof Error ? error : String(error)));
    observeTelemetry(() => span?.setStatus({ code: SpanStatusCode.ERROR }));
    throw error;
  } finally {
    observeTelemetry(() => span?.end());
  }
}

export function traceConnectRequests(tracer: Tracer): Interceptor {
  return next => request => withSpan(tracer, `rpc.${request.service.typeName}.${request.method.name}`, async () => {
    observeTelemetry(() => propagation.inject(context.active(), request.header, {
      set: (headers, key, value) => headers.set(key, value),
    }));
    return next(request);
  }, {
    kind: SpanKind.CLIENT,
    attributes: {
      "rpc.system": "grpc",
      "rpc.service": request.service.typeName,
      "rpc.method": request.method.name,
    },
  });
}
