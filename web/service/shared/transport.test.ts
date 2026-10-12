import { describe, expect, it } from "vitest";

import { parseServiceBaseUrl } from "./transport.js";

describe("parseServiceBaseUrl", () => {
  it.each([
    ["an https origin", "https://gateway.example:8443/", false, "https://gateway.example:8443"],
    ["an allowed plaintext origin", "http://gateway:8081", true, "http://gateway:8081"],
  ])("accepts %s", (_name, raw, allowPlaintext, origin) => {
    expect(parseServiceBaseUrl(raw, allowPlaintext)).toBe(origin);
  });

  it.each([
    ["a relative URL", "gateway:8081"],
    ["a path", "https://gateway.example/api"],
    ["credentials", "https://user:secret@gateway.example"],
    ["plaintext without opting in", "http://gateway:8081"],
    ["another protocol", "ftp://gateway.example"],
  ])("rejects %s", (_name, raw) => {
    expect(() => parseServiceBaseUrl(raw)).toThrow();
  });
});
