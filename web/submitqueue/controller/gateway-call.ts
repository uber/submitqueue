import { SpanStatusCode, type Meter, type Span, type Tracer } from "@opentelemetry/api";
import type { WebError } from "../entity/index.js";
import { beginOperation, defaultMeter, defaultTracer, silentLogger } from "../core/observability.js";
import type { Logger } from "pino";
import type { GatewayErrorClassifier, GatewayErrorCode } from "../extension/gateway/index.js";
import { grpcStatusErrorCode } from "../extension/gateway/grpcstatus/index.js";
import { webErrorForGatewayCode } from "./error.js";

export type GatewayOperation = "list" | "summary" | "history" | "change" | "queues";

export interface GatewayLoadOptions {
  /** Maps transport errors to codes; defaults to {@link grpcStatusErrorCode}. */
  classifyError?: GatewayErrorClassifier;
  /** Receives one warning per failed call, with the raw cause. */
  logger?: Logger;
  meter?: Meter;
  tracer?: Tracer;
}

export type GatewayCall<T> = { ok: true; value: T } | { ok: false; error: WebError };

function classifyGatewayError(classify: GatewayErrorClassifier, cause: unknown): GatewayErrorCode {
  try {
    return classify(cause);
  } catch {
    // A faulty host classifier must not break the never-rejects contract below.
    return "internal";
  }
}

/**
 * Performs one gateway read inside a span and a `submitqueue_web.gateway`
 * duration metric. A failure is classified, logged with its raw cause, and
 * returned as a viewer-safe error; this never rejects.
 */
export async function callGateway<T>(
  options: GatewayLoadOptions,
  operation: GatewayOperation,
  target: { queue: string; sqid?: string },
  call: () => Promise<T>,
): Promise<GatewayCall<T>> {
  const timing = beginOperation(options.meter ?? defaultMeter(), "submitqueue_web.gateway", { operation });
  const tracer = options.tracer ?? defaultTracer();
  return tracer.startActiveSpan(`submitqueue_web.gateway.${operation}`, async (span: Span): Promise<GatewayCall<T>> => {
    try {
      const value = await call();
      timing.end("success");
      return { ok: true, value };
    } catch (cause) {
      const code = classifyGatewayError(options.classifyError ?? grpcStatusErrorCode, cause);
      timing.end(code);
      span.setStatus({ code: SpanStatusCode.ERROR, message: code });
      (options.logger ?? silentLogger).warn({ operation, ...target, code, err: cause }, "SubmitQueue gateway call failed");
      return { ok: false, error: webErrorForGatewayCode(code) };
    } finally {
      span.end();
    }
  });
}
