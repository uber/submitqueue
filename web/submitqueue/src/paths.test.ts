import { describe, expect, it } from "vitest";
import { decodeSqidSegment, encodeSqidSegment, WebPaths } from "./paths";

describe("WebPaths", () => {
  it("round-trips a slash-containing sqid in one path segment", () => {
    const paths = new WebPaths();
    const href = paths.request("demo-queue/42");
    const segment = href.split("/").at(-1);

    expect(segment).toBeDefined();
    expect(segment).not.toContain("/");
    expect(paths.decodeRequest(segment!)).toBe("demo-queue/42");
    expect(encodeSqidSegment("demo-queue/42")).not.toMatch(/=|\//u);
  });

  it("rejects malformed segments without throwing", () => {
    expect(decodeSqidSegment("not+base64")).toBeUndefined();
    expect(decodeSqidSegment("_w")).toBeUndefined();
  });

  it("preserves fixed list bounds while paging", () => {
    const paths = new WebPaths({ requestsPath: "/submitqueue/requests/" });
    const href = paths.requests({ fromMs: 10, toMs: 20, pageToken: "opaque+/=" });

    expect(href).toBe("/submitqueue/requests?from=10&to=20&page=opaque%2B%2F%3D");
  });
});
