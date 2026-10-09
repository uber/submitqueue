import { Code, ConnectError } from "@connectrpc/connect";
import { describe, expect, it } from "vitest";

import { grpcStatusErrorCode } from "./index.js";

describe("grpcStatusErrorCode", () => {
  it.each([
    [Code.InvalidArgument, "invalid-argument"],
    [Code.NotFound, "not-found"],
    [Code.ResourceExhausted, "resource-exhausted"],
    [Code.Unavailable, "unavailable"],
    [Code.DeadlineExceeded, "unavailable"],
    [Code.PermissionDenied, "internal"],
  ] as const)("classifies Connect code %s", (code, expected) => {
    expect(grpcStatusErrorCode(new ConnectError("detail", code))).toBe(expected);
  });

  it.each([
    ["a grpc-js style status object", Object.assign(new Error("unavailable"), { code: 14 }), "unavailable"],
    ["a Node system error", Object.assign(new Error("refused"), { code: "ECONNREFUSED" }), "internal"],
    ["an HTTP-like status", { code: 404 }, "internal"],
    ["a plain error", new Error("boom"), "internal"],
    ["a non-object", "boom", "internal"],
  ])("classifies %s", (_name, error, expected) => {
    expect(grpcStatusErrorCode(error)).toBe(expected);
  });
});
