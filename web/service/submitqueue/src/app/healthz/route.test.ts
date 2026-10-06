import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const { pingDemoGateway } = vi.hoisted(() => ({
  pingDemoGateway: vi.fn(),
}));

vi.mock("../../server/gateway", () => ({ pingDemoGateway }));

import { GET } from "./route";

describe("GET /healthz", () => {
  beforeEach(() => {
    pingDemoGateway.mockResolvedValue(undefined);
  });

  afterEach(() => {
    pingDemoGateway.mockReset();
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
  });

  it("is ready when required configuration is valid", async () => {
    vi.stubEnv("SUBMITQUEUE_WEB_TOKEN", "a".repeat(32));
    vi.stubEnv("SUBMITQUEUE_GATEWAY_URL", "https://gateway.example:8080");
    const response = await GET();

    expect(response.status).toBe(200);
    expect(response.headers.get("cache-control")).toBe("no-store");
    await expect(response.json()).resolves.toEqual({ status: "ok" });
  });

  it.each([
    ["an empty token", "", "https://gateway.example:8080"],
    ["a missing gateway", "a".repeat(32), ""],
  ])("is unavailable with %s", async (_name, token, gatewayURL) => {
    vi.stubEnv("SUBMITQUEUE_WEB_TOKEN", token);
    vi.stubEnv("SUBMITQUEUE_GATEWAY_URL", gatewayURL);
    const response = await GET();

    expect(response.status).toBe(503);
    expect(response.headers.get("cache-control")).toBe("no-store");
    await expect(response.json()).resolves.toEqual({ status: "unavailable" });
  });

  it("logs one actionable diagnostic for a repeated configuration failure", async () => {
    vi.stubEnv("SUBMITQUEUE_WEB_TOKEN", "a".repeat(32));
    vi.stubEnv("SUBMITQUEUE_GATEWAY_URL", "https://gateway.example:8080");
    await GET();

    const error = vi.spyOn(console, "error").mockImplementation(() => undefined);
    vi.stubEnv("SUBMITQUEUE_WEB_TOKEN", "");
    await GET();
    await GET();

    expect(error).toHaveBeenCalledOnce();
    expect(error).toHaveBeenCalledWith(
      "SubmitQueue web configuration is invalid",
      { message: "SUBMITQUEUE_WEB_TOKEN must be set" },
    );
  });

  it("is unavailable and logs once while the gateway is unreachable", async () => {
    vi.stubEnv("SUBMITQUEUE_WEB_TOKEN", "a".repeat(32));
    vi.stubEnv("SUBMITQUEUE_GATEWAY_URL", "https://gateway.example:8080");
    const cause = new Error("connection refused");
    pingDemoGateway.mockRejectedValue(cause);
    const error = vi.spyOn(console, "error").mockImplementation(() => undefined);

    const first = await GET();
    const second = await GET();

    expect(first.status).toBe(503);
    expect(second.status).toBe(503);
    expect(error).toHaveBeenCalledOnce();
    expect(error).toHaveBeenCalledWith(
      "SubmitQueue web gateway readiness check failed",
      { cause },
    );
  });
});
