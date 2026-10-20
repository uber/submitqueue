import { describe, expect, it } from "vitest";
import { defaultRequestWindow, REQUEST_WINDOW_MS } from "./window.js";

describe("live request window", () => {
  it("advances the default window each time it is refreshed", () => {
    expect(defaultRequestWindow(REQUEST_WINDOW_MS + 100)).toEqual({ fromMs: 100, toMs: REQUEST_WINDOW_MS + 100 });
    expect(defaultRequestWindow(REQUEST_WINDOW_MS + 200)).toEqual({ fromMs: 200, toMs: REQUEST_WINDOW_MS + 200 });
  });
});
