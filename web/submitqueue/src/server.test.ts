import { Code, ConnectError } from "@connectrpc/connect";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { createFakeGatewayReader, gatewayHistoryFixture, gatewayRequestFixture } from "./testing";

import {
  classifyGatewayError,
  loadChangeSubmissions,
  loadRequestDetail,
  loadRequestList,
  requestDetailIsComplete,
  requestDetailRefreshState,
  safeInt64ToNumber,
} from "./server";

describe("server presentation mapping", () => {
  beforeEach(() => vi.clearAllMocks());

  it.each([false, true])("maps change submissions with optional host pagination (paged: %s)", async paged => {
    const request = gatewayRequestFixture();
    const submissions = [{
      request, version: "sha", versionHref: "/change/sha",
    }];
    const pagination = { nextHref: "/change?page=next", latestHref: null };
    const result = await loadChangeSubmissions(async () => paged ? { submissions, pagination } : submissions, {
      queue: "demo-queue", provider: "Git", host: "git.example.com",
      repository: "demo", review: "refs/heads/main", pinnedVersion: null, window: { fromMs: 100, toMs: 200 },
    });
    expect(result).toMatchObject({
      ok: true, data: { submissions: [{ request: { sqid: request.sqid }, version: "sha" }] },
    });
    if (result.ok) {
      expect(result.data.pagination).toEqual(paged ? pagination : undefined);
    }
  });

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

  it("leaves list, summary, and history deadlines to the host transport", async () => {
    const fake = createFakeGatewayReader();
    const list = vi.fn(fake.list);
    const getRequestSummaryByID = vi.fn(fake.getRequestSummaryByID);
    const getRequestHistoryByID = vi.fn(fake.getRequestHistoryByID);
    const reader = { list, getRequestSummaryByID, getRequestHistoryByID };

    await loadRequestList(() => reader, {
      queue: "demo-queue",
      receivedAtOrAfterMs: 100,
      receivedBeforeMs: 200,
    });
    await loadRequestDetail(() => reader, {
      queue: "demo-queue",
      sqid: "demo-queue/1",
    });

    expect(list.mock.calls[0]).toHaveLength(1);
    expect(getRequestSummaryByID.mock.calls[0]).toHaveLength(1);
    expect(getRequestHistoryByID.mock.calls[0]).toHaveLength(1);
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
    const onGatewayError = vi.fn();
    const historyCause = new ConnectError("private upstream text", Code.Unavailable);
    const reader = createFakeGatewayReader({
      summary: gatewayRequestFixture({ status: "landed" }),
      historyError: historyCause,
    });
    const result = await loadRequestDetail(
      () => reader,
      {
        queue: "demo-queue",
        sqid: "demo-queue/1",
      },
      { diagnostics: { onGatewayError } },
    );

    expect(result).toMatchObject({
      ok: true,
      data: {
        request: { status: "landed" },
        history: [],
        historyError: { kind: "transient", retryable: true },
      },
    });
    expect(onGatewayError).toHaveBeenCalledWith({
      operation: "history",
      queue: "demo-queue",
      sqid: "demo-queue/1",
      connectCode: Code.Unavailable,
      cause: historyCause,
    });
    expect(JSON.stringify(result)).not.toContain("private upstream text");
    if (result.ok) {
      expect(requestDetailIsComplete(result.data)).toBe(false);
      expect(requestDetailRefreshState(result.data)).toEqual({
        terminal: false,
        transientFailureCount: 1,
      });
    }
  });

  it("stops a terminal summary when history cannot converge", async () => {
    const reader = createFakeGatewayReader({
      summary: gatewayRequestFixture({ status: "landed" }),
      historyError: new ConnectError("invalid history", Code.PermissionDenied),
    });
    const result = await loadRequestDetail(() => reader, {
      queue: "demo-queue",
      sqid: "demo-queue/1",
    });

    expect(result.ok && requestDetailRefreshState(result.data)).toEqual({
      terminal: true,
      transientFailureCount: 0,
    });
  });

  it("waits for history to contain the terminal summary status", async () => {
    const summary = gatewayRequestFixture({ status: "landed" });
    const incomplete = await loadRequestDetail(
      () =>
        createFakeGatewayReader({
          summary,
          history: [gatewayHistoryFixture({ status: "landing" })],
        }),
      { queue: "demo-queue", sqid: "demo-queue/1" },
    );
    const complete = await loadRequestDetail(
      () =>
        createFakeGatewayReader({
          summary,
          history: [gatewayHistoryFixture({ status: "landed" })],
        }),
      { queue: "demo-queue", sqid: "demo-queue/1" },
    );

    expect(incomplete.ok && requestDetailIsComplete(incomplete.data)).toBe(false);
    expect(complete.ok && requestDetailIsComplete(complete.data)).toBe(true);
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

  it("emits one structured diagnostic without exposing the cause", async () => {
    const cause = new ConnectError("private upstream detail", Code.Unavailable);
    const onGatewayError = vi.fn();
    const result = await loadRequestList(
      () => createFakeGatewayReader({ listError: cause }),
      {
        queue: "demo-queue",
        receivedAtOrAfterMs: 100,
        receivedBeforeMs: 200,
      },
      { diagnostics: { onGatewayError } },
    );

    expect(onGatewayError).toHaveBeenCalledOnce();
    expect(onGatewayError).toHaveBeenCalledWith({
      operation: "list",
      queue: "demo-queue",
      sqid: null,
      connectCode: Code.Unavailable,
      cause,
    });
    expect(JSON.stringify(result)).not.toContain("private upstream detail");
  });
});
