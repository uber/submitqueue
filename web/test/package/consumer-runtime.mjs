import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { SubmitQueueApp, SubmitQueueShell } from "@submitqueue/web-submitqueue";
import { createSubmitQueueWeb, WebPaths } from "@submitqueue/web-submitqueue/server";
import { newHmacCursorCodec } from "@submitqueue/web-submitqueue/extension/cursor/hmac";
import { createFakeGatewayReader } from "@submitqueue/web-submitqueue/extension/gateway/mock";

const web = createSubmitQueueWeb({
  gateway: createFakeGatewayReader(),
  cursors: newHmacCursorCodec("ordinary-server-signing-secret"),
  paths: new WebPaths({ basePath: "/queues" }),
});
const result = await web.handle({ path: "/queues/demo-queue", search: {} });
assert.equal(result.kind, "render");
assert.equal(result.model.result.ok, true);
assert.doesNotThrow(() => JSON.stringify(result.model));
const html = renderToStaticMarkup(createElement(SubmitQueueShell, { homeHref: "/queues" },
  createElement(SubmitQueueApp, { model: result.model })));
assert.ok(html.includes('href="/queues/demo-queue/request/demo-queue/1"'));
assert.ok(html.includes('aria-label="Queue requests"'));
const stylesheet = await readFile(new URL(import.meta.resolve("@submitqueue/web-submitqueue/styles.css")), "utf8");
assert.ok(stylesheet.includes(".sq-app.site-frame"));
assert.ok(stylesheet.includes(".sq-app .sq-history"));
console.log("An ordinary Node/React host loaded and rendered the application with its packaged stylesheet.");
