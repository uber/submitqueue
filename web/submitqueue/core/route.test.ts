import { describe, expect, it } from "vitest";

import { WebPaths } from "./paths.js";
import { parseRoute } from "./route.js";

function segments(href: string): string[] {
  const { pathname } = new URL(href, "https://submitqueue.test");
  return pathname === "/" ? [] : pathname.slice(1).split("/");
}

describe("parseRoute", () => {
  const paths = new WebPaths();

  it.each([
    ["the directory", paths.directory(), { kind: "queues" }],
    ["a queue", paths.requests("demo-queue"), { kind: "queue", queue: "demo-queue", search: {} }],
    ["a dot-only queue", paths.requests(".."), { kind: "queue", queue: "..", search: {} }],
    ["a slash-containing sqid", paths.request("demo-queue", "demo-queue/42"),
      { kind: "request", queue: "demo-queue", sqid: "demo-queue/42", search: {} }],
    ["an escaped sqid", paths.request("demo-queue", "opaque/id?query#~."),
      { kind: "request", queue: "demo-queue", sqid: "opaque/id?query#~.", search: {} }],
    ["a change", "/demo-queue/change/git/git.example.com/demo/refs%2Fheads%2Fmain",
      { kind: "change", queue: "demo-queue", reference: ["git", "git.example.com", "demo", "refs/heads/main"], search: {} }],
  ])("round-trips %s produced by WebPaths", (_name, href, route) => {
    expect(parseRoute(segments(href))).toEqual(route);
  });

  it("carries search parameters to the page", () => {
    expect(parseRoute(["demo-queue"], { page: "cursor" })).toEqual({ kind: "queue", queue: "demo-queue", search: { page: "cursor" } });
  });

  it.each([
    ["an unknown resource", ["demo-queue", "unknown", "1"]],
    ["a request without an ID", ["demo-queue", "request"]],
    ["an empty segment", ["demo-queue", "", "1"]],
    ["malformed percent-encoding", ["demo%E0%A4%A"]],
  ])("does not route %s", (_name, value) => {
    expect(parseRoute(value)).toBeNull();
  });
});
