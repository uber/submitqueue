import { BaseProvider, LightTheme, DarkTheme } from "baseui";
import { Client, Server } from "styletron-engine-atomic";
import { Provider } from "styletron-react";
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
for (const theme of [LightTheme, DarkTheme]) {
  for (const route of routes) {
    const result = await web.handle(route);
    assert.equal(result.kind, "render", `${JSON.stringify(route)} did not render`);
    const content = createElement(BaseProvider, { theme }, createElement(SubmitQueueShell, null, createElement(SubmitQueueApp, { model: result.model })));
    const engine = new Server({ prefix: "sq" });
    const html = renderToString(createElement(Provider, { value: engine }, content));
    assert.ok(engine.getCss().length > 0, "server rendering must emit Base Web styles");
    pages.push({ route, content, html, styles: engine.getStylesheetsHtml("sq-styletron") });
  }
}

const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "https://submitqueue.test/" });
for (const name of ["window", "document", "navigator"]) {
  Object.defineProperty(globalThis, name, { configurable: true, value: name === "window" ? dom.window : dom.window[name] });
}
const errors = [];
const reportError = console.error;
console.error = (...values) => {
  // Base Web's Popover still declares defaultProps; React 18 warns before hydration.
  if (String(values[0]).includes("Support for defaultProps will be removed") && values[1] === "Popover") {
    reportError(...values);
    return;
  }
  errors.push(values.map(String).join(" "));
};
const { hydrateRoot } = await import("react-dom/client");
// act flushes hydration before unmounting; unmounting a root that is still
// waiting to hydrate is itself reported as a hydration failure.
const { act } = React;
globalThis.IS_REACT_ACT_ENVIRONMENT = true;
for (const { route, content, html, styles } of pages) {
  document.head.innerHTML = styles;
  const engine = new Client({ prefix: "sq", hydrate: document.querySelectorAll("style.sq-styletron") });
  const page = createElement(Provider, { value: engine }, content);
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
