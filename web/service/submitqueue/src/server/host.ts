import "server-only";

import { newHmacCursorCodec } from "@submitqueue/web-submitqueue/extension/cursor/hmac";
import { createSubmitQueueWeb, type SubmitQueueWeb } from "@submitqueue/web-submitqueue/server";
import { loadCursorSecret } from "./config";
import { resolveReferenceGateway } from "./gateway";
import { instrumentSubmitQueueWeb } from "./instrumentation";
import { referenceTracer, resolveReferenceMeter } from "./observability";

let web: SubmitQueueWeb | undefined;

/**
 * The OSS reference composition: SubmitQueue mounted at the root, read
 * through the Connect gateway client, with pino logs and opt-in Prometheus
 * metrics. Frameworks adapt this composition; they do not build their own.
 */
export function resolveReferenceWeb(): SubmitQueueWeb {
  web ??= instrumentSubmitQueueWeb(createSubmitQueueWeb({
    gateway: resolveReferenceGateway,
    cursors: newHmacCursorCodec(loadCursorSecret()),
  }), resolveReferenceMeter(), referenceTracer);
  return web;
}
