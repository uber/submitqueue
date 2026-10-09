import { describe, expect, it } from "vitest";

import {
  BASIC_AUTH_USERNAME,
  isAuthorized,
  loadAuthConfiguration,
} from "./auth";

const token = "test";

function basic(username: string, password: string): string {
  return `Basic ${Buffer.from(`${username}:${password}`).toString("base64")}`;
}

describe("loadAuthConfiguration", () => {
  it("loads the local demo password", () => {
    expect(loadAuthConfiguration({ STOVEPIPE_WEB_TOKEN: token })).toEqual({
      token,
    });
  });

  it.each([undefined, ""])("rejects a missing or empty token", (value) => {
    expect(() =>
      loadAuthConfiguration({ STOVEPIPE_WEB_TOKEN: value }),
    ).toThrow(Error);
  });
});

describe("isAuthorized", () => {
  it("accepts the configured credentials", () => {
    expect(
      isAuthorized(basic("test", "test"), { token }),
    ).toBe(true);
  });

  it.each([
    ["missing header", null],
    ["wrong scheme", `Bearer ${token}`],
    ["malformed base64", "Basic !!!"],
    ["missing separator", `Basic ${Buffer.from("submitqueue").toString("base64")}`],
    ["wrong user", basic("operator", token)],
    ["previous demo user", basic("submitqueue", token)],
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
