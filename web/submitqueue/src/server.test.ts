import { Code, ConnectError } from "@connectrpc/connect";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { createFakeGatewayReader, gatewayHistoryFixture, gatewayRequestFixture } from "./testing";

vi.mock("next/server", () => ({ connection: vi.fn(async () => undefined) }));

import {
  classifyGatewayError,
  loadRequestDetail,
  loadRequestList,
  safeInt64ToNumber,
} from "./server";

describe("server presentation mapping", () => {
  beforeEach(() => vi.clearAllMocks());

  it("converts int64 only within JavaScript's safe range", () => {
    expect(safeInt64ToNumber(1_700_000_000_000n)).toBe(1_700_000_000_000);
    expect(safeInt64ToNumber(BigInt(Number.MAX_SAFE_INTEGER) + 1n)).toBeNull();
    expect(safeInt64ToNumber("not-an-integer")).toBeNull();
  });

  it("maps a request page while preserving its bounds", async () => {
    const reader = createFakeGatewayReader({ nextPageToken: "next" });
    const result = await loadRequestList(() => reader, {
      queue: "demo-queue",
      receivedAtOrAfterMs: 100,
      receivedBeforeMs: 200,
      pageToken: "current",
    });

    expect(result).toMatchObject({
      ok: true,
      data: {
        queue: "demo-queue",
        receivedAtOrAfterMs: 100,
        receivedBeforeMs: 200,
        nextPageToken: "next",
        requests: [{ sqid: "demo-queue/1", receivedAtMs: 1_700_000_000_000 }],
      },
    });
  });

  it("returns an explicit invalid-input result before resolving a client", async () => {
    const resolver = vi.fn(() => createFakeGatewayReader());
    const result = await loadRequestList(resolver, {
      queue: "demo-queue",
      receivedAtOrAfterMs: 200,
      receivedBeforeMs: 100,
    });

    expect(result).toMatchObject({ ok: false, error: { kind: "user" } });
    expect(resolver).not.toHaveBeenCalled();
  });

  it("keeps the summary available when history has a transient failure", async () => {
    const reader = createFakeGatewayReader({
      summary: gatewayRequestFixture({ status: "landed" }),
      historyError: new ConnectError("private upstream text", Code.Unavailable),
    });
    const result = await loadRequestDetail(() => reader, {
      queue: "demo-queue",
      sqid: "demo-queue/1",
    });

    expect(result).toMatchObject({
      ok: true,
      data: {
        request: { status: "landed" },
        history: [],
        historyError: { kind: "transient", retryable: true },
      },
    });
    expect(JSON.stringify(result)).not.toContain("private upstream text");
  });

  it("maps chronological status and occurrence history", async () => {
    const reader = createFakeGatewayReader({
      history: [
        gatewayHistoryFixture({ timestampMs: 1n, status: "started" }),
        gatewayHistoryFixture({
          timestampMs: 2n,
          type: "event",
          status: "",
          event: "building",
          metadata: { build_url: "https://build.example/1" },
        }),
      ],
    });
    const result = await loadRequestDetail(() => reader, {
      queue: "demo-queue",
      sqid: "demo-queue/1",
    });

    expect(result).toMatchObject({
      ok: true,
      data: {
        history: [
          { timestampMs: 1, type: "status", status: "started" },
          { timestampMs: 2, type: "event", event: "building" },
        ],
        historyError: null,
      },
    });
  });
});

describe("gateway error classification", () => {
  it.each([
    [Code.InvalidArgument, "user", false],
    [Code.NotFound, "not-found", false],
    [Code.ResourceExhausted, "user", false],
    [Code.Unavailable, "transient", true],
    [Code.DeadlineExceeded, "transient", true],
    [Code.Internal, "internal", false],
  ] as const)("classifies code %s", (code, kind, retryable) => {
    const classified = classifyGatewayError(new ConnectError("raw gateway detail", code));

    expect(classified).toMatchObject({ kind, retryable });
    expect(classified.message).not.toContain("raw gateway detail");
  });

  it("does not expose arbitrary errors", () => {
    expect(classifyGatewayError(new Error("database password"))).toMatchObject({
      kind: "internal",
      retryable: false,
    });
  });
});
