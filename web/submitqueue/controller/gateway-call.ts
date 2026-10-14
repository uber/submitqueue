import type { WebError } from "../entity/index.js";
import { createLogCallback, type LogCallback } from "@submitqueue/web-platform/logging";
import type { GatewayErrorClassifier, GatewayErrorCode } from "../extension/gateway/index.js";
import { grpcStatusErrorCode } from "../extension/gateway/grpcstatus/index.js";
import { webErrorForGatewayCode } from "./error.js";

export type GatewayOperation = "list" | "summary" | "history" | "change" | "queues";

export interface GatewayLoadOptions {
  /** Maps transport errors to codes; defaults to {@link grpcStatusErrorCode}. */
  classifyError?: GatewayErrorClassifier;
  /** Receives one warning per failed call, with the raw cause. */
  onLog?: LogCallback;
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
 * Performs one gateway read. A failure is classified, logged with its raw cause, and
 * returned as a viewer-safe error; this never rejects.
 */
export async function callGateway<T>(
  options: GatewayLoadOptions,
  operation: GatewayOperation,
  target: { queue: string; sqid?: string },
  call: () => Promise<T>,
): Promise<GatewayCall<T>> {
  try {
    return { ok: true, value: await call() };
  } catch (cause) {
    const code = classifyGatewayError(options.classifyError ?? grpcStatusErrorCode, cause);
    createLogCallback(options.onLog)("warn", "SubmitQueue gateway call failed", { operation, ...target, code, error: cause });
    return { ok: false, error: webErrorForGatewayCode(code) };
  }
}
