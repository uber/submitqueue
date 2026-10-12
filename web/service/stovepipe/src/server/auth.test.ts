import { describe, expect, it } from "vitest";

import { authenticationChallenge, loadAuthConfiguration } from "./auth";

const token = "test";

describe("loadAuthConfiguration", () => {
  it("loads the local demo password", () => {
    expect(loadAuthConfiguration({ STOVEPIPE_WEB_TOKEN: token })).toEqual({
      token,
    });
  });

  it.each([undefined, ""])("rejects a missing or empty token", (value) => {
    expect(() => loadAuthConfiguration({ STOVEPIPE_WEB_TOKEN: value })).toThrow(
      Error,
    );
  });
});

it("uses the host-specific authentication realm", () => {
  expect(authenticationChallenge().headers.get("www-authenticate")).toBe(
    'Basic realm="Stovepipe demo", charset="UTF-8"',
  );
});
