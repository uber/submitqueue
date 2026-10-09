// @vitest-environment jsdom

import { afterEach, expect, it } from "vitest";
import { Server } from "styletron-engine-atomic";
import { hydrateReferenceStyles } from "./styles.js";

afterEach(() => { document.head.innerHTML = ""; });

it("hydrates every streamed rule and reuses its server class", () => {
  const server = new Server({ prefix: "sq" });
  const first = { color: "red" };
  const second = { backgroundColor: "blue" };
  const firstClass = server.renderStyle(first);
  const initial = server.getStylesheets()[0]!;
  const initialLength = initial.css.length;
  document.head.innerHTML = `<style data-submitqueue-styletron>${initial.css}</style>`;
  const secondClass = server.renderStyle(second);
  const delta = document.createElement("style");
  delta.dataset.submitqueueStyletron = "";
  delta.textContent = server.getStylesheets()[0]!.css.slice(initialLength);
  document.head.append(delta);

  const client = hydrateReferenceStyles();
  expect(client.renderStyle(first)).toBe(firstClass);
  expect(client.renderStyle(second)).toBe(secondClass);
  const nextClass = client.renderStyle({ paddingTop: "8px" });
  expect(nextClass).not.toBe(firstClass);
  expect(nextClass).not.toBe(secondClass);
  expect([...document.styleSheets].flatMap(sheet => [...sheet.cssRules]).some(rule => rule.cssText.includes("padding-top: 8px"))).toBe(true);
});
