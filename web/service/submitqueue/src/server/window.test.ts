import { describe, expect, it } from "vitest";

import {
  defaultRequestWindow,
  parseRequestWindow,
  REQUEST_WINDOW_MS,
  requestWindowSearch,
} from "./window";

describe("request window", () => {
  it("creates a fixed trailing 24-hour interval", () => {
    expect(defaultRequestWindow(REQUEST_WINDOW_MS + 100)).toEqual({
      fromMs: 100,
      toMs: REQUEST_WINDOW_MS + 100,
    });
  });

  it("parses stable bounds and an opaque page token", () => {
    expect(
      parseRequestWindow({ from: "100", to: "200", page: "token/+=" }),
    ).toEqual({ fromMs: 100, toMs: 200, pageToken: "token/+=" });
  });

  it.each([
    ["missing bounds", {}],
    ["duplicate bounds", { from: ["1", "2"], to: "3" }],
    ["negative bound", { from: "-1", to: "3" }],
    ["fractional bound", { from: "1.5", to: "3" }],
    ["unsafe bound", { from: "1", to: "9007199254740992" }],
    ["empty interval", { from: "3", to: "3" }],
    ["reversed interval", { from: "4", to: "3" }],
  ])("rejects %s", (_name, search) => {
    expect(parseRequestWindow(search)).toBeUndefined();
  });

  it("serializes bounds and preserves the page token", () => {
    expect(
      requestWindowSearch({ fromMs: 100, toMs: 200, pageToken: "a/b+c" }).toString(),
    ).toBe("from=100&to=200&page=a%2Fb%2Bc");
  });
});
