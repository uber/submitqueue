import type {
  GatewayHistoryEvent,
  GatewayReader,
  GatewayRequestSummary,
} from "./server.js";

const DEFAULT_TIMESTAMP = 1_700_000_000_000n;

export function gatewayRequestFixture(
  overrides: Partial<GatewayRequestSummary> = {},
): GatewayRequestSummary {
  return {
    sqid: "demo-queue/1",
    queue: "demo-queue",
    changeUris: ["github://github.com/uber/submitqueue/pull/1/0123456789012345678901234567890123456789"],
    receivedAtMs: DEFAULT_TIMESTAMP,
    status: "speculating",
    lastError: "",
    metadata: {},
    ...overrides,
  };
}

export function gatewayHistoryFixture(
  overrides: Partial<GatewayHistoryEvent> = {},
): GatewayHistoryEvent {
  return {
    timestampMs: DEFAULT_TIMESTAMP,
    status: "started",
    lastError: "",
    metadata: {},
    type: "status",
    event: "",
    ...overrides,
  };
}

export interface FakeGatewayReaderOptions {
  requests?: readonly GatewayRequestSummary[];
  nextPageToken?: string;
  summary?: GatewayRequestSummary;
  history?: readonly GatewayHistoryEvent[];
  listError?: unknown;
  summaryError?: unknown;
  historyError?: unknown;
}

export function createFakeGatewayReader(options: FakeGatewayReaderOptions = {}): GatewayReader {
  const summary = options.summary ?? options.requests?.[0] ?? gatewayRequestFixture();
  return {
    async list() {
      if (options.listError !== undefined) {
        throw options.listError;
      }
      return {
        requests: options.requests ?? [summary],
        nextPageToken: options.nextPageToken ?? "",
      };
    },
    async getRequestSummaryByID() {
      if (options.summaryError !== undefined) {
        throw options.summaryError;
      }
      return { request: summary };
    },
    async getRequestHistoryByID() {
      if (options.historyError !== undefined) {
        throw options.historyError;
      }
      return { events: options.history ?? [gatewayHistoryFixture()] };
    },
  };
}

export type { GatewayHistoryEvent, GatewayReader, GatewayRequestSummary } from "./server.js";
