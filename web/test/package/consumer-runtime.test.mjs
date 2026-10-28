import { test } from "vitest";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { BaseProvider, LightTheme } from "baseui";
import { Server } from "styletron-engine-atomic";
import { Provider } from "styletron-react";
import { SubmitQueueApp } from "@submitqueue/web-submitqueue";
import { createSubmitQueueWeb, WebPaths } from "@submitqueue/web-submitqueue/server";
import { createFakeGatewayReader } from "@submitqueue/web-submitqueue/extension/gateway/mock";

test("renders a packaged application with its host providers", async () => {
  const web = createSubmitQueueWeb({
    gateway: createFakeGatewayReader(),
    paths: new WebPaths({ basePath: "/queues" }),
  });
  const result = await web.handle({ path: "/queues/queue/demo-queue", search: {} });
  assert.equal(result.kind, "render");
  assert.equal(result.model.result.ok, true);
  assert.doesNotThrow(() => JSON.stringify(result.model));
  const engine = new Server();
  const html = renderToStaticMarkup(createElement(Provider, { value: engine },
    createElement(BaseProvider, { theme: LightTheme },
      createElement(SubmitQueueApp, { model: result.model }))));
  assert.ok(engine.getCss().length > 0);
  assert.ok(html.includes('href="/queues/queue/demo-queue/request/demo-queue/1"'));
  assert.ok(html.includes('aria-label="Queue requests"'));
  const stylesheet = await readFile(new URL(import.meta.resolve("@submitqueue/web-submitqueue/styles.css")), "utf8");
  assert.ok(stylesheet.includes("@submitqueue/web-platform/styles.css"));
  const commonStyles = await readFile(new URL(import.meta.resolve("@submitqueue/web-platform/styles.css")), "utf8");
  assert.ok(commonStyles.trim().length > 0);
});
