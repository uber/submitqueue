import { describe, expect, it } from "vitest";

import {
  BASIC_AUTH_USERNAME,
  isAuthorized,
  loadAuthConfiguration,
  MINIMUM_TOKEN_LENGTH,
} from "./auth";

const token = "a".repeat(MINIMUM_TOKEN_LENGTH);

function basic(username: string, password: string): string {
  return `Basic ${Buffer.from(`${username}:${password}`).toString("base64")}`;
}

describe("loadAuthConfiguration", () => {
  it("loads a sufficiently long token", () => {
    expect(loadAuthConfiguration({ SUBMITQUEUE_WEB_TOKEN: token })).toEqual({
      token,
    });
  });

  it.each([undefined, "short"])("rejects a missing or short token", (value) => {
    expect(() =>
      loadAuthConfiguration({ SUBMITQUEUE_WEB_TOKEN: value }),
    ).toThrow(/at least 32 characters/u);
  });
});

describe("isAuthorized", () => {
  it("accepts the configured credentials", () => {
    expect(
      isAuthorized(basic(BASIC_AUTH_USERNAME, token), { token }),
    ).toBe(true);
  });

  it.each([
    ["missing header", null],
    ["wrong scheme", `Bearer ${token}`],
    ["malformed base64", "Basic !!!"],
    ["missing separator", `Basic ${Buffer.from("submitqueue").toString("base64")}`],
    ["wrong user", basic("operator", token)],
    ["wrong password", basic(BASIC_AUTH_USERNAME, "b".repeat(32))],
  ])("rejects %s", (_name, authorization) => {
    expect(isAuthorized(authorization, { token })).toBe(false);
  });

  it("allows colons in the password", () => {
    const password = `${token}:suffix`;
    expect(
      isAuthorized(basic(BASIC_AUTH_USERNAME, password), { token: password }),
    ).toBe(true);
  });
});
