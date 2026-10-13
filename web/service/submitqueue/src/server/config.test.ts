import { describe, expect, it } from "vitest";

import { loadCursorSecret, loadGatewayConfiguration, loadMetricsPort } from "./config";

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

describe("loadCursorSecret", () => {
  it("requires its own secret", () => {
    expect(loadCursorSecret({ SUBMITQUEUE_WEB_CURSOR_SECRET: "cursor-secret" })).toBe("cursor-secret");
    expect(() => loadCursorSecret({ SUBMITQUEUE_WEB_TOKEN: "viewer-password" })).toThrow();
  });
});

describe("loadMetricsPort", () => {
  it.each([
    ["unset", {}, null],
    ["a port", { SUBMITQUEUE_WEB_METRICS_PORT: "9464" }, 9464],
  ])("reads %s", (_name, environment, port) => {
    expect(loadMetricsPort(environment)).toBe(port);
  });

  it.each(["0", "65536", "http"])("rejects %s", value => {
    expect(() => loadMetricsPort({ SUBMITQUEUE_WEB_METRICS_PORT: value })).toThrow();
  });
});
