// Typechecks a host written only against the package's public entry points.
import { WebNavigationProvider } from "@submitqueue/web-platform";
import { createLogCallback } from "@submitqueue/web-platform/logging";
import { encodePathSegment } from "@submitqueue/web-platform/path-segment";
import { StovepipeApp } from "@submitqueue/web-stovepipe";
import { createStovepipeWeb, StovepipePaths, type StovepipeService } from "@submitqueue/web-stovepipe/server";
import { SubmitQueueApp, SubmitQueueShell } from "@submitqueue/web-submitqueue";
import { createSubmitQueueWeb, type SubmitQueueGateway } from "@submitqueue/web-submitqueue/server";
import { createFakeGatewayReader } from "@submitqueue/web-submitqueue/extension/gateway/mock";

const gateway: SubmitQueueGateway = createFakeGatewayReader();
const web = createSubmitQueueWeb({ gateway });

export async function render() {
  const result = await web.handle({
    path: "/queue/demo-queue", search: {},
    onLog: (level, message, metadata) => { console[level](message, metadata); },
  });
  if (result.kind !== "render") {
    return null;
  }
  return [WebNavigationProvider, SubmitQueueShell, SubmitQueueApp, result.model] as const;
}

export async function renderStovepipe(service: StovepipeService) {
  const web = createStovepipeWeb({
    service,
    queues: [{ name: "demo-queue" }],
    paths: new StovepipePaths("/queues"),
  });
  const result = await web.handle({
    path: "/queues/" + encodePathSegment("demo-queue"),
    search: new URLSearchParams(),
    onLog: createLogCallback(),
  });
  return result.kind === "render"
    ? [WebNavigationProvider, StovepipeApp, result.model] as const
    : null;
}
