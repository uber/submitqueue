import "server-only";

import { newHmacCursorCodec } from "@submitqueue/web-submitqueue/extension/cursor/hmac";
import { createSubmitQueueWeb, type SubmitQueueWeb } from "@submitqueue/web-submitqueue/server";
import { loadCursorSecret } from "./config";
import { resolveReferenceGateway } from "./gateway";
import { resolveReferenceLogger, resolveReferenceMeter } from "./observability";

let web: SubmitQueueWeb | undefined;

/**
 * The OSS reference composition: SubmitQueue mounted at the root, read
 * through the Connect gateway client, with pino logs and opt-in Prometheus
 * metrics. Frameworks adapt this composition; they do not build their own.
 */
export function resolveReferenceWeb(): SubmitQueueWeb {
  web ??= createSubmitQueueWeb({
    gateway: resolveReferenceGateway,
    cursors: newHmacCursorCodec(loadCursorSecret()),
    logger: resolveReferenceLogger(),
    meter: resolveReferenceMeter(),
  });
  return web;
}
