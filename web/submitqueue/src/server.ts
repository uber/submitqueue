import { Code, ConnectError, type Client } from "@connectrpc/connect";
import { SubmitQueueGateway } from "@submitqueue/api/submitqueue/gateway";
import { connection } from "next/server";
import type {
  HistoryEventModel,
  LoadResult,
  RequestDetailModel,
  RequestListModel,
  RequestSummaryModel,
  WebError,
} from "./models.js";

export { decodeSqidSegment, encodeSqidSegment, WebPaths } from "./paths.js";
export { isTerminalStatus, statusDisplay } from "./status.js";

type Int64Value = bigint | number | string;
const GATEWAY_TIMEOUT_MS = 5_000;

export interface GatewayCallOptions {
  timeoutMs?: number;
}

export interface GatewayRequestSummary {
  sqid: string;
  queue: string;
  changeUris: readonly string[];
  receivedAtMs: Int64Value;
  status: string;
  lastError: string;
  metadata: Readonly<Record<string, string>>;
}

export interface GatewayHistoryEvent {
  timestampMs: Int64Value;
  status: string;
  lastError: string;
  metadata: Readonly<Record<string, string>>;
  type: string;
  event: string;
}

export interface GatewayReader {
  list(
    input: {
      queue: string;
      receivedAtOrAfterMs: bigint;
      receivedBeforeMs: bigint;
      pageSize: number;
      pageToken: string;
    },
    options?: GatewayCallOptions,
  ): Promise<{
    requests: readonly GatewayRequestSummary[];
    nextPageToken: string;
  }>;
  getRequestSummaryByID(
    input: { sqid: string; queue: string },
    options?: GatewayCallOptions,
  ): Promise<{
    request?: GatewayRequestSummary | undefined;
  }>;
  getRequestHistoryByID(
    input: { sqid: string; queue: string },
    options?: GatewayCallOptions,
  ): Promise<{
    events: readonly GatewayHistoryEvent[];
  }>;
}

export type GatewayResolver = (queue: string) => GatewayReader;

export type SubmitQueueGatewayClient = Client<typeof SubmitQueueGateway>;

export function gatewayReader(client: SubmitQueueGatewayClient): GatewayReader {
  return client;
}

export interface LoadRequestListInput {
  queue: string;
  receivedAtOrAfterMs: number;
  receivedBeforeMs: number;
  pageSize?: number;
  pageToken?: string;
}

export interface LoadRequestDetailInput {
  queue: string;
  sqid: string;
}

const INVALID_INPUT: WebError = {
  kind: "user",
  title: "Invalid request",
  message: "The request parameters are invalid.",
  retryable: false,
};

const INTERNAL_ERROR: WebError = {
  kind: "internal",
  title: "Something went wrong",
  message: "SubmitQueue could not load this information.",
  retryable: false,
};

export function safeInt64ToNumber(value: Int64Value): number | null {
  let converted: bigint;
  if (typeof value === "bigint") {
    converted = value;
  } else if (typeof value === "number") {
    return Number.isSafeInteger(value) ? value : null;
  } else {
    try {
      converted = BigInt(value);
    } catch {
      return null;
    }
  }
  if (converted > BigInt(Number.MAX_SAFE_INTEGER) || converted < BigInt(Number.MIN_SAFE_INTEGER)) {
    return null;
  }
  return Number(converted);
}

export function classifyGatewayError(error: unknown): WebError {
  if (!(error instanceof ConnectError)) {
    return INTERNAL_ERROR;
  }
  switch (error.code) {
    case Code.InvalidArgument:
      return INVALID_INPUT;
    case Code.NotFound:
      return {
        kind: "not-found",
        title: "Request not found",
        message: "The requested SubmitQueue request was not found.",
        retryable: false,
      };
    case Code.ResourceExhausted:
      return {
        kind: "user",
        title: "Request limit reached",
        message: "The requested result set is too large. Narrow the time window and try again.",
        retryable: false,
      };
    case Code.Unavailable:
    case Code.DeadlineExceeded:
      return {
        kind: "transient",
        title: "SubmitQueue is unavailable",
        message: "SubmitQueue could not be reached. Try again shortly.",
        retryable: true,
      };
    default:
      return INTERNAL_ERROR;
  }
}

function mapSummary(value: GatewayRequestSummary): RequestSummaryModel | null {
  const receivedAtMs = safeInt64ToNumber(value.receivedAtMs);
  if (receivedAtMs === null) {
    return null;
  }
  return {
    sqid: value.sqid,
    queue: value.queue,
    changeUris: [...value.changeUris],
    receivedAtMs,
    status: value.status,
    lastError: value.lastError === "" ? null : value.lastError,
    metadata: { ...value.metadata },
  };
}

function mapHistoryEvent(value: GatewayHistoryEvent): HistoryEventModel | null {
  const timestampMs = safeInt64ToNumber(value.timestampMs);
  if (timestampMs === null) {
    return null;
  }
  const type = value.type === "status" || value.type === "event" ? value.type : "unknown";
  return {
    timestampMs,
    type,
    status: value.status === "" ? null : value.status,
    event: value.event === "" ? null : value.event,
    lastError: value.lastError === "" ? null : value.lastError,
    metadata: { ...value.metadata },
  };
}

function validListInput(input: LoadRequestListInput): boolean {
  const pageSize = input.pageSize ?? 50;
  return (
    input.queue !== "" &&
    Number.isSafeInteger(input.receivedAtOrAfterMs) &&
    Number.isSafeInteger(input.receivedBeforeMs) &&
    input.receivedAtOrAfterMs < input.receivedBeforeMs &&
    Number.isInteger(pageSize) &&
    pageSize > 0
  );
}

export async function loadRequestList(
  resolveGateway: GatewayResolver,
  input: LoadRequestListInput,
): Promise<LoadResult<RequestListModel>> {
  if (!validListInput(input)) {
    return { ok: false, error: INVALID_INPUT };
  }
  try {
    await connection();
    const response = await resolveGateway(input.queue).list(
      {
        queue: input.queue,
        receivedAtOrAfterMs: BigInt(input.receivedAtOrAfterMs),
        receivedBeforeMs: BigInt(input.receivedBeforeMs),
        pageSize: input.pageSize ?? 50,
        pageToken: input.pageToken ?? "",
      },
      { timeoutMs: GATEWAY_TIMEOUT_MS },
    );
    const requests: RequestSummaryModel[] = [];
    for (const value of response.requests) {
      const request = mapSummary(value);
      if (request === null) {
        return { ok: false, error: INTERNAL_ERROR };
      }
      requests.push(request);
    }
    return {
      ok: true,
      data: {
        queue: input.queue,
        receivedAtOrAfterMs: input.receivedAtOrAfterMs,
        receivedBeforeMs: input.receivedBeforeMs,
        requests,
        nextPageToken: response.nextPageToken === "" ? null : response.nextPageToken,
      },
    };
  } catch (error) {
    return { ok: false, error: classifyGatewayError(error) };
  }
}

export async function loadRequestDetail(
  resolveGateway: GatewayResolver,
  input: LoadRequestDetailInput,
): Promise<LoadResult<RequestDetailModel>> {
  if (input.queue === "" || input.sqid === "") {
    return { ok: false, error: INVALID_INPUT };
  }
  try {
    await connection();
    const gateway = resolveGateway(input.queue);
    const [summaryResult, historyResult] = await Promise.allSettled([
      gateway.getRequestSummaryByID(input, { timeoutMs: GATEWAY_TIMEOUT_MS }),
      gateway.getRequestHistoryByID(input, { timeoutMs: GATEWAY_TIMEOUT_MS }),
    ]);
    if (summaryResult.status === "rejected") {
      return { ok: false, error: classifyGatewayError(summaryResult.reason) };
    }
    if (summaryResult.value.request === undefined) {
      return { ok: false, error: INTERNAL_ERROR };
    }
    const request = mapSummary(summaryResult.value.request);
    if (request === null) {
      return { ok: false, error: INTERNAL_ERROR };
    }
    if (historyResult.status === "rejected") {
      return {
        ok: true,
        data: {
          request,
          history: [],
          historyError: classifyGatewayError(historyResult.reason),
        },
      };
    }
    const history: HistoryEventModel[] = [];
    for (const value of historyResult.value.events) {
      const event = mapHistoryEvent(value);
      if (event === null) {
        return {
          ok: true,
          data: { request, history: [], historyError: INTERNAL_ERROR },
        };
      }
      history.push(event);
    }
    return { ok: true, data: { request, history, historyError: null } };
  } catch (error) {
    return { ok: false, error: classifyGatewayError(error) };
  }
}
