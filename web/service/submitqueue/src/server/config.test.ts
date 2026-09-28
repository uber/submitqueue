import { describe, expect, it } from "vitest";

import { loadGatewayConfiguration } from "./config";

describe("loadGatewayConfiguration", () => {
  it("uses TLS by default", () => {
    expect(
      loadGatewayConfiguration({
        SUBMITQUEUE_GATEWAY_URL: "https://gateway.example:8081",
      }),
    ).toEqual({
      baseUrl: "https://gateway.example:8081",
      allowPlaintext: false,
    });
  });

  it("allows h2c only when explicitly enabled", () => {
    expect(
      loadGatewayConfiguration({
        SUBMITQUEUE_GATEWAY_ALLOW_PLAINTEXT: "true",
        SUBMITQUEUE_GATEWAY_URL: "http://gateway-service:8081",
      }),
    ).toEqual({
      baseUrl: "http://gateway-service:8081",
      allowPlaintext: true,
    });
  });

  it.each([
    ["missing URL", {}],
    ["relative URL", { SUBMITQUEUE_GATEWAY_URL: "gateway:8081" }],
    ["implicit plaintext", { SUBMITQUEUE_GATEWAY_URL: "http://gateway:8081" }],
    ["unsupported scheme", { SUBMITQUEUE_GATEWAY_URL: "ftp://gateway:8081" }],
    ["path", { SUBMITQUEUE_GATEWAY_URL: "https://gateway:8081/grpc" }],
    ["query", { SUBMITQUEUE_GATEWAY_URL: "https://gateway:8081?x=1" }],
    ["credentials", { SUBMITQUEUE_GATEWAY_URL: "https://user:pass@gateway:8081" }],
  ])("rejects %s", (_name, environment) => {
    expect(() => loadGatewayConfiguration(environment)).toThrow();
  });
});
