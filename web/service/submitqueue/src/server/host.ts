import "server-only";

import { createSubmitQueueWeb, type SubmitQueueWeb } from "@submitqueue/web-submitqueue/server";
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
  }), resolveReferenceMeter(), referenceTracer);
  return web;
}
