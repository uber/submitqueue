import { test } from "vitest";
import assert from "node:assert/strict";
import { JSDOM } from "jsdom";
import React, { createElement } from "react";
import { renderToString } from "react-dom/server";
import { BaseProvider, createTheme, DarkTheme, LightTheme } from "baseui";
import { Client, Server } from "styletron-engine-atomic";
import { Provider } from "styletron-react";
import { SubmitQueueApp } from "@submitqueue/web-submitqueue";
import { createSubmitQueueWeb } from "@submitqueue/web-submitqueue/server";
import { createFakeGatewayReader } from "@submitqueue/web-submitqueue/extension/gateway/mock";

import { StovepipeApp } from "@submitqueue/web-stovepipe";
import { createStovepipeWeb } from "@submitqueue/web-stovepipe/server";

test("renders and hydrates packaged pages with React 18 and host themes", async () => {
  assert.match(React.version, /^18\./u);

  const web = createSubmitQueueWeb({
    gateway: createFakeGatewayReader(),
  });
  const routes = [
    { path: "/", search: {} },
    { path: "/queue/demo-queue", search: {} },
    { path: "/queue/demo-queue/request/demo-queue/1", search: {} },
    { path: "/queue/demo-queue/request/demo-queue/1", search: { view: "history" } },
  ];
  const pages = [];
  const themes = [LightTheme, DarkTheme, createTheme({ colors: {
    backgroundPrimary: "#102030", contentPrimary: "#ffffff",
    buttonPrimaryFill: "#123456", buttonPrimaryText: "#ffffff",
  } })];
  function withProviders(page, engine, theme) {
    return createElement(Provider, { value: engine }, createElement(BaseProvider, { theme }, page));
  }
  function addPage(route, page) {
    for (const theme of themes) {
      const engine = new Server();
      const html = renderToString(withProviders(page, engine, theme));
      assert.ok(engine.getCss().length > 0);
      pages.push({ route, page, theme, html, stylesheets: engine.getStylesheets() });
    }
  }
  for (const route of routes) {
    const result = await web.handle(route);
    assert.equal(result.kind, "render", `${JSON.stringify(route)} did not render`);
    const page = createElement(SubmitQueueApp, { model: result.model });
    addPage(route, page);
  }

  const stovepipe = createStovepipeWeb({ queues: [{ name: "repo/main" }], service: {
    list: async () => ({ requests: [], nextPageToken: "" }),
    getProjectStatusByID: async () => ({
      queue: "repo/main", requestId: "1", updatedAtMs: 100n,
      repositoryResult: { case: "repositoryBreakageDegree", value: 0.25 },
      projectResultsComplete: true, nextPageToken: "",
      projects: [{ project: "example-project", result: { case: "breakageDegree", value: 0 } }],
    }),
    getRequestHistoryByID: async () => ({ requestSummary: { requestId: "1", queue: "repo/main", changeUri: "git://repo/main/sha", baseUri: "", requestState: "accepted", stateUpdatedAtMs: 100n, acceptedAtMs: 100n, outcomeReason: "", projectCount: 0, brokenProjectCount: 0 }, events: [] }),
  } });
  for (const href of ["/repo%2Fmain", "/repo%2Fmain/request/1", "/repo%2Fmain/request/1?view=history"]) {
    const url = new URL(href, "https://submitqueue.test");
    const result = await stovepipe.handle({ path: url.pathname, search: url.searchParams });
    assert.equal(result.kind, "render");
    const page = createElement(StovepipeApp, { model: result.model });
    addPage(href, page);
  }

  const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "https://submitqueue.test/" });
  for (const name of ["window", "document", "navigator"]) {
    Object.defineProperty(globalThis, name, { configurable: true, value: name === "window" ? dom.window : dom.window[name] });
  }
  const errors = [];
  const reportError = console.error;
  console.error = (...values) => {
    const message = values.map(String).join(" ");
    // BaseUI's Popover still uses defaultProps; React 18 warns but hydrates correctly.
    if (message.includes("Popover") && message.includes("Support for defaultProps will be removed from function components")) {
      reportError(...values);
    } else {
      errors.push(message);
    }
  };
  const { hydrateRoot } = await import("react-dom/client");
  // act flushes hydration before unmounting; unmounting a root that is still
  // waiting to hydrate is itself reported as a hydration failure.
  const { act } = React;
  globalThis.IS_REACT_ACT_ENVIRONMENT = true;
  for (const { route, page, theme, html, stylesheets } of pages) {
    const styles = stylesheets.map(({ css, attrs }) => {
      const style = document.createElement("style");
      for (const [name, value] of Object.entries(attrs)) style.setAttribute(name, value);
      style.textContent = css;
      document.head.append(style);
      return style;
    });
    const engine = new Client({ hydrate: styles });
    const container = document.createElement("div");
    container.innerHTML = html;
    document.body.append(container);
    let root;
    await act(async () => {
      root = hydrateRoot(container, withProviders(page, engine, theme));
    });
    const expected = document.createElement("div");
    expected.style.backgroundColor = theme.colors.backgroundPrimary;
    document.body.append(expected);
    assert.equal(dom.window.getComputedStyle(container.querySelector(".sq-app")).backgroundColor,
      dom.window.getComputedStyle(expected).backgroundColor, "the page must inherit its host's background");
    expected.style.color = theme.colors.contentPrimary;
    const selectedTimeZone = container.querySelector('[role="option"][aria-selected="true"]');
    assert.ok(selectedTimeZone);
    assert.equal(dom.window.getComputedStyle(selectedTimeZone).color,
      dom.window.getComputedStyle(expected).color, "the selected time zone must inherit its host's text color");
    expected.remove();
    if (route === "/repo%2Fmain/request/1") {
      assert.match(container.textContent, /example-project/u);
      assert.match(container.textContent, /Repository resultPartial Breakage/u);
      assert.match(container.textContent, /example-projectGreen/u);
      assert.doesNotMatch(container.textContent, /%/u);
    }
    await act(async () => root.unmount());
    container.remove();
    for (const style of styles) style.remove();
    assert.deepEqual(errors, [], `${JSON.stringify(route)} failed to hydrate on React ${React.version}`);
  }
  console.error = reportError;
  console.log(`React ${React.version} rendered and hydrated ${pages.length} packaged pages.`);
});
