import { Code, ConnectError } from "@connectrpc/connect";
import type { Logger } from "pino";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { createFakeGatewayReader, gatewayHistoryFixture, gatewayRequestFixture } from "../extension/gateway/mock/index.js";

import { loadChangeSubmissions } from "./change.js";
import { loadQueueDirectory } from "./directory.js";
import { grpcStatusErrorCode } from "../extension/gateway/grpcstatus/index.js";
import { webErrorForGatewayCode } from "./error.js";

const classifyGatewayError = (error: unknown) => webErrorForGatewayCode(grpcStatusErrorCode(error));
import { loadRequestList } from "./queue.js";
import { loadRequestDetail, requestDetailIsComplete, requestDetailRefreshState } from "./request.js";

const FAILURE_MESSAGE = "SubmitQueue gateway call failed";

function recordingLogger() {
  const warn = vi.fn();
  const logger = { child: () => logger, debug: vi.fn(), info: vi.fn(), warn, error: vi.fn() } as unknown as Logger;
  return { logger, warn };
}

describe("queue discovery mapping", () => {
  it("lists gateway-configured queues, including ones without requests", async () => {
    const reader = createFakeGatewayReader({ queues: [{ name: "demo-queue" }, { name: "empty-queue" }] });
    const result = await loadQueueDirectory(reader);
    expect(result).toEqual({
      ok: true, data: [{ name: "demo-queue", description: "" }, { name: "empty-queue", description: "" }],
    });
  });

  it("returns an empty directory when the gateway has no configured queues", async () => {
    expect(await loadQueueDirectory(createFakeGatewayReader({ queues: [] }))).toEqual({ ok: true, data: [] });
  });

  it("classifies discovery failures without leaking the gateway message", async () => {
    const { logger, warn } = recordingLogger();
    const cause = new ConnectError("private upstream detail", Code.Unavailable);
    const result = await loadQueueDirectory(
      createFakeGatewayReader({ queuesError: cause }),
      { logger },
    );
    expect(result).toMatchObject({ ok: false, error: { kind: "transient", retryable: true } });
    expect(JSON.stringify(result)).not.toContain("private upstream detail");
    expect(warn).toHaveBeenCalledWith(expect.objectContaining({ operation: "queues", err: cause }), FAILURE_MESSAGE);
  });

  it.each([{ queues: [{ name: "" }] }, { queues: [{ name: "same" }, { name: "same" }] }])("rejects malformed discovery data", async ({ queues }) => {
    const result = await loadQueueDirectory(createFakeGatewayReader({ queues }));
    expect(result).toMatchObject({ ok: false, error: { kind: "internal" } });
  });
});

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

  it("maps a request page while preserving its bounds", async () => {
    const reader = createFakeGatewayReader({ nextPageToken: "next" });
    const result = await loadRequestList(reader, {
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

    await loadRequestList(reader, {
      queue: "demo-queue",
      receivedAtOrAfterMs: 100,
      receivedBeforeMs: 200,
    });
    await loadRequestDetail(reader, {
      queue: "demo-queue",
      sqid: "demo-queue/1",
    });

    expect(list.mock.calls[0]).toHaveLength(1);
    expect(getRequestSummaryByID.mock.calls[0]).toHaveLength(1);
    expect(getRequestHistoryByID.mock.calls[0]).toHaveLength(1);
  });

  it("returns an explicit invalid-input result before calling the gateway", async () => {
    const gateway = createFakeGatewayReader();
    const list = vi.spyOn(gateway, "list");
    const result = await loadRequestList(gateway, {
      queue: "demo-queue",
      receivedAtOrAfterMs: 200,
      receivedBeforeMs: 100,
    });

    expect(result).toMatchObject({ ok: false, error: { kind: "user" } });
    expect(list).not.toHaveBeenCalled();
  });

  it("keeps the summary available when history has a transient failure", async () => {
    const { logger, warn } = recordingLogger();
    const historyCause = new ConnectError("private upstream text", Code.Unavailable);
    const reader = createFakeGatewayReader({
      summary: gatewayRequestFixture({ status: "landed" }),
      historyError: historyCause,
    });
    const result = await loadRequestDetail(
      reader,
      {
        queue: "demo-queue",
        sqid: "demo-queue/1",
      },
      { logger },
    );

    expect(result).toMatchObject({
      ok: true,
      data: {
        request: { status: "landed" },
        history: [],
        historyError: { kind: "transient", retryable: true },
      },
    });
    expect(warn).toHaveBeenCalledWith({
      operation: "history",
      queue: "demo-queue",
      sqid: "demo-queue/1",
      code: "unavailable",
      err: historyCause,
    }, FAILURE_MESSAGE);
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
    const result = await loadRequestDetail(reader, {
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
      createFakeGatewayReader({
        summary,
        history: [gatewayHistoryFixture({ status: "landing" })],
      }),
      { queue: "demo-queue", sqid: "demo-queue/1" },
    );
    const complete = await loadRequestDetail(
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
    const result = await loadRequestDetail(reader, {
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

  it("uses a host classifier for transports without gRPC status codes", async () => {
    const { logger, warn } = recordingLogger();
    const result = await loadRequestList(
      createFakeGatewayReader({ listError: new Error("custom transport: upstream timed out") }),
      { queue: "demo-queue", receivedAtOrAfterMs: 100, receivedBeforeMs: 200 },
      { classifyError: () => "unavailable", logger },
    );
    expect(result).toMatchObject({ ok: false, error: { kind: "transient", retryable: true } });
    expect(warn).toHaveBeenCalledWith(expect.objectContaining({ code: "unavailable" }), FAILURE_MESSAGE);
  });

  it("treats a throwing host classifier as an internal failure", async () => {
    const { logger, warn } = recordingLogger();
    const result = await loadRequestList(
      createFakeGatewayReader({ listError: new Error("upstream") }),
      { queue: "demo-queue", receivedAtOrAfterMs: 100, receivedBeforeMs: 200 },
      { classifyError: () => { throw new Error("classifier bug"); }, logger },
    );
    expect(result).toMatchObject({ ok: false, error: { kind: "internal", retryable: false } });
    expect(warn).toHaveBeenCalledWith(expect.objectContaining({ code: "internal" }), FAILURE_MESSAGE);
  });

  it("does not expose arbitrary errors", () => {
    expect(classifyGatewayError(new Error("database password"))).toMatchObject({
      kind: "internal",
      retryable: false,
    });
  });

  it("logs one structured failure without exposing the cause", async () => {
    const cause = new ConnectError("private upstream detail", Code.Unavailable);
    const { logger, warn } = recordingLogger();
    const result = await loadRequestList(
      createFakeGatewayReader({ listError: cause }),
      {
        queue: "demo-queue",
        receivedAtOrAfterMs: 100,
        receivedBeforeMs: 200,
      },
      { logger },
    );

    expect(warn).toHaveBeenCalledOnce();
    expect(warn).toHaveBeenCalledWith({ operation: "list", queue: "demo-queue", code: "unavailable", err: cause }, FAILURE_MESSAGE);
    expect(JSON.stringify(result)).not.toContain("private upstream detail");
  });
});
