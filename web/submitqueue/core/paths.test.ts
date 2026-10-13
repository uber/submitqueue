import { describe, expect, it } from "vitest";
import { decodePathSegment, WebPaths } from "./paths";

describe("WebPaths", () => {
  it("keeps the queue and complete sqid readable in the request path", () => {
    const paths = new WebPaths();

    expect(paths.request("demo-queue", "demo-queue/42")).toBe(
      "/demo-queue/request/demo-queue/42",
    );
  });

  it("uses only the host's opaque cursor while paging", () => {
    const paths = new WebPaths({ basePath: "/submitqueue/" });
    const href = paths.requests("demo queue", {
      pageToken: "opaque+/=",
    });

    expect(href).toBe(
      "/submitqueue/demo%20queue?page=opaque%2B%2F%3D",
    );
  });

  it("URL-escapes reserved characters without interpreting the sqid", () => {
    const paths = new WebPaths();

    expect(paths.request("demo-queue", "opaque/id?query#fragment")).toBe(
      "/demo-queue/request/opaque/id%3Fquery%23fragment",
    );
  });

  it.each([".", "..", "~.", "~value"])("round-trips %s without URL dot normalization", (id) => {
    const paths = new WebPaths();
    const url = new URL(paths.request(id, `${id}/42`), "https://example.test");
    const [queue, resource, ...sqid] = url.pathname.slice(1).split("/").map(decodeURIComponent);
    expect(resource).toBe("request");
    expect(decodePathSegment(queue!)).toBe(id);
    expect(sqid.map(decodePathSegment).join("/")).toBe(`${id}/42`);
  });

  it("makes the history view shareable without consuming part of an opaque ID", () => {
    expect(new WebPaths().request("demo-queue", "42", { view: "history" })).toBe("/demo-queue/request/42?view=history");
  });
});
