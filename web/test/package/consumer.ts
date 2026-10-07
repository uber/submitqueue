// Typechecks a host written only against the package's public entry points.
import { SubmitQueueApp, SubmitQueueShell, WebNavigationProvider } from "@submitqueue/web-submitqueue";
import { createSubmitQueueWeb, type SubmitQueueGateway } from "@submitqueue/web-submitqueue/server";
import { createFakeGatewayReader } from "@submitqueue/web-submitqueue/extension/gateway/mock";
import { newHmacCursorCodec } from "@submitqueue/web-submitqueue/extension/cursor/hmac";

const gateway: SubmitQueueGateway = createFakeGatewayReader();
const web = createSubmitQueueWeb({ gateway, cursors: newHmacCursorCodec("consumer-owned-secret") });

export async function render() {
  const result = await web.handle({ path: "/demo-queue", search: {} });
  if (result.kind !== "render") {
    return null;
  }
  return [WebNavigationProvider, SubmitQueueShell, SubmitQueueApp, result.model] as const;
}
