import type { Int64Value } from "../../core/int64.js";

/**
 * Structural contract for the SubmitQueue gateway reads the web UI needs. A
 * Protobuf-ES / Connect client satisfies it directly; any other transport
 * (protobufjs, another RPC stack, a test fake) satisfies it through a thin adapter.
 * Requests use Protobuf-ES types (`bigint` for int64), so adapters convert
 * them; responses accept any common int64 representation.
 */

/** Per-call options; deadlines normally belong to the host's transport. */
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

export interface GatewayQueueReader {
  listQueues(
    input: Record<string, never>,
    options?: GatewayCallOptions,
  ): Promise<{ queues: readonly { name: string }[] }>;
}

export interface GatewayChangeReader {
  getRequestSummaryByChangeURI(
    input: { queue: string; changeUri: string },
    options?: GatewayCallOptions,
  ): Promise<{ requests: readonly GatewayRequestSummary[] }>;
}

/** Every gateway read the SubmitQueue UI performs. */
export type SubmitQueueGateway = GatewayReader & GatewayQueueReader & GatewayChangeReader;

/** Transport-neutral outcome of a failed gateway call. */
export type GatewayErrorCode = "invalid-argument" | "not-found" | "resource-exhausted" | "unavailable" | "internal";

/** Maps a rejected gateway call to a {@link GatewayErrorCode}; must not throw. */
export type GatewayErrorClassifier = (error: unknown) => GatewayErrorCode;
