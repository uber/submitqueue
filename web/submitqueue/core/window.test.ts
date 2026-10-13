import { newHmacCursorCodec } from "../extension/cursor/hmac/index.js";
import { describe, expect, it } from "vitest";
import { decodeWindowCursor, defaultRequestWindow, encodeWindowCursor, REQUEST_WINDOW_MS } from "./window.js";

const codec = newHmacCursorCodec("test-pagination-secret");
const queue = "demo-queue";
const window = defaultRequestWindow(REQUEST_WINDOW_MS + 100);

describe("live request window and snapshot pagination", () => {
  it("advances the default window each time it is refreshed", () => {
    expect(defaultRequestWindow(REQUEST_WINDOW_MS + 100)).toEqual({ fromMs: 100, toMs: REQUEST_WINDOW_MS + 100 });
    expect(defaultRequestWindow(REQUEST_WINDOW_MS + 200)).toEqual({ fromMs: 200, toMs: REQUEST_WINDOW_MS + 200 });
  });

  it("round-trips stable gateway paging bounds inside one opaque cursor", async () => {
    const cursor = await encodeWindowCursor(codec, queue, window, "opaque/token+=\nvalue");
    expect(await decodeWindowCursor(codec, queue, cursor)).toEqual({ ...window, pageToken: "opaque/token+=\nvalue" });
  });

  it("rejects altered, cross-queue, or differently signed cursors", async () => {
    const cursor = await encodeWindowCursor(codec, queue, window, "next");
    expect(await decodeWindowCursor(codec, "other-queue", cursor)).toBeUndefined();
    expect(await decodeWindowCursor(codec, queue, `${cursor}x`)).toBeUndefined();
    expect(await decodeWindowCursor(newHmacCursorCodec("other-secret"), queue, cursor)).toBeUndefined();
  });

  it.each(["", "not-a-cursor", ".invalid", "a".repeat(16_385)])("rejects malformed cursor", async (cursor) => {
    expect(await decodeWindowCursor(codec, queue, cursor)).toBeUndefined();
  });

  it.each([
    ["a missing token", window, ""],
    ["inverted bounds", { fromMs: 200, toMs: 100 }, "next"],
  ])("rejects a signed cursor with %s", async (_name, bounds, token) => {
    expect(await decodeWindowCursor(codec, queue, await encodeWindowCursor(codec, queue, bounds, token))).toBeUndefined();
  });

  it("rejects a signed payload from another cursor format", async () => {
    expect(await decodeWindowCursor(codec, queue, await codec.encode(queue, "v0\n1\n2\nnext"))).toBeUndefined();
  });
});
