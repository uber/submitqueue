import { describe, expect, it } from "vitest";
import { isAuthorized, authenticationChallenge } from "./auth.js";
const username = "test";
const token = "test";
function basic(username: string, password: string): string {
  return `Basic ${Buffer.from(`${username}:${password}`).toString("base64")}`;
}
describe("isAuthorized", () => {
  it("accepts the configured credentials", () => {
    expect(
      isAuthorized(basic("test", "test"), { token, username }),
    ).toBe(true);
  });

  it.each([
    ["missing header", null],
    ["wrong scheme", `Bearer ${token}`],
    ["malformed base64", "Basic !!!"],
    ["missing separator", `Basic ${Buffer.from("submitqueue").toString("base64")}`],
    ["wrong user", basic("operator", token)],
    ["previous demo user", basic("submitqueue", token)],
    ["wrong password", basic(username, "b".repeat(32))],
  ])("rejects %s", (_name, authorization) => {
    expect(isAuthorized(authorization, { token, username })).toBe(false);
  });

  it("allows colons in the password", () => {
    const password = `${token}:suffix`;
    expect(
      isAuthorized(basic(username, password), { token: password, username }),
    ).toBe(true);
  });
});

it("responds with a noncacheable challenge for the supplied realm", () => {
  const response = authenticationChallenge("Demo");
  expect(response.status).toBe(401);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(response.headers.get("www-authenticate")).toBe(
    'Basic realm="Demo", charset="UTF-8"',
  );
});
