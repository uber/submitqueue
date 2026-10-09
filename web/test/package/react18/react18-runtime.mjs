import assert from "node:assert/strict";
import { JSDOM } from "jsdom";
import React, { createElement } from "react";
import { renderToString } from "react-dom/server";
import { SubmitQueueApp, SubmitQueueShell } from "@submitqueue/web-submitqueue";
import { createSubmitQueueWeb } from "@submitqueue/web-submitqueue/server";
import { newHmacCursorCodec } from "@submitqueue/web-submitqueue/extension/cursor/hmac";
import { createFakeGatewayReader } from "@submitqueue/web-submitqueue/extension/gateway/mock";

assert.match(React.version, /^18\./u);

const web = createSubmitQueueWeb({
  gateway: createFakeGatewayReader(),
  cursors: newHmacCursorCodec("react-18-consumer-secret"),
});
const routes = [
  { path: "/", search: {} },
  { path: "/demo-queue", search: {} },
  { path: "/demo-queue/request/demo-queue/1", search: {} },
  { path: "/demo-queue/request/demo-queue/1", search: { view: "history" } },
];
const pages = [];
for (const route of routes) {
  const result = await web.handle(route);
  assert.equal(result.kind, "render", `${JSON.stringify(route)} did not render`);
  const page = createElement(SubmitQueueShell, null, createElement(SubmitQueueApp, { model: result.model }));
  pages.push({ route, page, html: renderToString(page) });
}

const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "https://submitqueue.test/" });
for (const name of ["window", "document", "navigator"]) {
  Object.defineProperty(globalThis, name, { configurable: true, value: name === "window" ? dom.window : dom.window[name] });
}
const errors = [];
const reportError = console.error;
console.error = (...values) => errors.push(values.map(String).join(" "));
const { hydrateRoot } = await import("react-dom/client");
// act flushes hydration before unmounting; unmounting a root that is still
// waiting to hydrate is itself reported as a hydration failure.
const { act } = React;
globalThis.IS_REACT_ACT_ENVIRONMENT = true;
for (const { route, page, html } of pages) {
  const container = document.createElement("div");
  container.innerHTML = html;
  document.body.append(container);
  let root;
  await act(async () => {
    root = hydrateRoot(container, page);
  });
  await act(async () => root.unmount());
  container.remove();
  assert.deepEqual(errors, [], `${JSON.stringify(route)} failed to hydrate on React ${React.version}`);
}
console.error = reportError;
console.log(`React ${React.version} rendered and hydrated ${pages.length} packaged pages.`);
