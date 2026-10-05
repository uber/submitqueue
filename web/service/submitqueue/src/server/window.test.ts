import { describe, expect, it } from "vitest";
import { defaultRequestWindow, decodeRequestPage, encodeRequestPage, REQUEST_WINDOW_MS } from "./window";

const secret = "test-pagination-secret";
const queue = "demo-queue";
const window = defaultRequestWindow(REQUEST_WINDOW_MS + 100);

describe("live request window and snapshot pagination", () => {
  it("advances the default window each time it is refreshed", () => {
    expect(defaultRequestWindow(REQUEST_WINDOW_MS + 100)).toEqual({ fromMs: 100, toMs: REQUEST_WINDOW_MS + 100 });
    expect(defaultRequestWindow(REQUEST_WINDOW_MS + 200)).toEqual({ fromMs: 200, toMs: REQUEST_WINDOW_MS + 200 });
  });

  it("round-trips stable gateway paging bounds inside one opaque cursor", () => {
    const cursor = encodeRequestPage(queue, window, "opaque/token+=\nvalue", secret);
    expect(decodeRequestPage(queue, cursor, secret)).toEqual({ ...window, pageToken: "opaque/token+=\nvalue" });
  });

  it("rejects altered, cross-queue, or differently signed cursors", () => {
    const cursor = encodeRequestPage(queue, window, "next", secret);
    expect(decodeRequestPage("other-queue", cursor, secret)).toBeUndefined();
    expect(decodeRequestPage(queue, `${cursor}x`, secret)).toBeUndefined();
    expect(decodeRequestPage(queue, cursor, "other-secret")).toBeUndefined();
  });

  it.each(["", "not-a-cursor", ".invalid", "a".repeat(16_385)])("rejects malformed cursor", (cursor) => {
    expect(decodeRequestPage(queue, cursor, secret)).toBeUndefined();
  });

  it("rejects signed cursors with missing tokens or invalid snapshot bounds", () => {
    expect(decodeRequestPage(queue, encodeRequestPage(queue, window, "", secret), secret)).toBeUndefined();
    expect(decodeRequestPage(queue, encodeRequestPage(queue, { fromMs: 200, toMs: 100 }, "next", secret), secret)).toBeUndefined();
  });
});
