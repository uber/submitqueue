import { describe, expect, it } from "vitest";

import { newHmacCursorCodec } from "./index.js";

describe("newHmacCursorCodec", () => {
  const codec = newHmacCursorCodec("test-secret");

  it("round-trips a payload within its scope", () => {
    const cursor = codec.encode("demo-queue", "v1\n100\n200\ntoken/with\nnewline") as string;
    expect(cursor).toMatch(/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/u);
    expect(codec.decode("demo-queue", cursor)).toBe("v1\n100\n200\ntoken/with\nnewline");
  });

  it.each([
    ["another scope", codec, "other-queue", (cursor: string) => cursor],
    ["a modified payload", codec, "demo-queue", (cursor: string) => `A${cursor}`],
    ["another secret", newHmacCursorCodec("other-secret"), "demo-queue", (cursor: string) => cursor],
    ["malformed input", codec, "demo-queue", () => "not a cursor"],
  ])("rejects %s", (_name, decoder, scope, alter) => {
    expect(decoder.decode(scope, alter(codec.encode("demo-queue", "payload") as string))).toBeUndefined();
  });

  it("refuses an empty secret", () => {
    expect(() => newHmacCursorCodec("")).toThrow();
  });
});
