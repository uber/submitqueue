import { describe, expect, it, vi } from "vitest";
import { createFakeGatewayReader } from "@submitqueue/web-submitqueue/testing";
import { Code, ConnectError } from "@connectrpc/connect";

const resolver = vi.hoisted(() => ({ resolveGatewayClient: vi.fn() }));
vi.mock("./gateway", () => resolver);

import { loadConfiguredQueue } from "./queues";

describe("gateway-backed host queue validation", () => {
  it("accepts configured queues other than the former demo-only queue", async () => {
    resolver.resolveGatewayClient.mockReturnValue(createFakeGatewayReader({
      queues: [{ name: "empty-queue" }],
    }));
    expect(await loadConfiguredQueue("empty-queue")).toEqual({ ok: true, data: true });
    expect(await loadConfiguredQueue("unknown")).toEqual({ ok: true, data: false });
  });

  it("does not treat a failed queue discovery as a missing queue", async () => {
    resolver.resolveGatewayClient.mockReturnValue(createFakeGatewayReader({
      queuesError: new ConnectError("unavailable", Code.Unavailable),
    }));
    expect(await loadConfiguredQueue("demo-queue")).toMatchObject({
      ok: false, error: { kind: "transient", retryable: true },
    });
  });
});
