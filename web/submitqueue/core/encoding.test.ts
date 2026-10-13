import { describe, expect, it } from "vitest";

import { safeInt64ToNumber } from "./int64.js";
import { decodePathSegment, encodePathSegment } from "./path-segment.js";

describe("safeInt64ToNumber", () => {
  it.each([
    ["safe bigint", 1_700_000_000_000n, 1_700_000_000_000],
    ["safe decimal string", "1700000000000", 1_700_000_000_000],
    ["safe number", 42, 42],
    ["Long-style decimal object", { toString: () => "1700000000000" }, 1_700_000_000_000],
    ["object without a decimal value", {}, null],
    ["bigint beyond the safe range", BigInt(Number.MAX_SAFE_INTEGER) + 1n, null],
    ["non-integer number", 1.5, null],
    ["non-numeric string", "not-an-integer", null],
  ])("converts a %s", (_name, value, expected) => {
    expect(safeInt64ToNumber(value)).toBe(expected);
  });
});

describe("path segment codec", () => {
  it.each([
    ["ordinary value", "demo-queue", "demo-queue"],
    ["reserved characters", "a/b?c", "a%2Fb%3Fc"],
    ["single dot", ".", "~."],
    ["double dot", "..", "~.."],
    ["leading tilde", "~value", "~~value"],
  ])("round-trips an %s", (_name, value, encoded) => {
    expect(encodePathSegment(value)).toBe(encoded);
    expect(decodePathSegment(decodeURIComponent(encoded))).toBe(value);
  });
});
