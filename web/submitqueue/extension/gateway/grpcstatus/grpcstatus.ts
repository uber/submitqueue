import type { GatewayErrorCode } from "../gateway.js";

// gRPC status codes, shared by Connect's `Code` enum and @grpc/grpc-js.
const GRPC_INVALID_ARGUMENT = 3;
const GRPC_DEADLINE_EXCEEDED = 4;
const GRPC_NOT_FOUND = 5;
const GRPC_RESOURCE_EXHAUSTED = 8;
const GRPC_UNAVAILABLE = 14;
const GRPC_MAX_CODE = 16;

/**
 * Classifies any error that carries a numeric gRPC status `code`, which
 * covers Connect and @grpc/grpc-js without importing either. Anything else
 * is `internal`.
 */
export function grpcStatusErrorCode(error: unknown): GatewayErrorCode {
  const code = typeof error === "object" && error !== null ? (error as { code?: unknown }).code : undefined;
  if (typeof code !== "number" || !Number.isInteger(code) || code < 0 || code > GRPC_MAX_CODE) {
    return "internal";
  }
  switch (code) {
    case GRPC_INVALID_ARGUMENT:
      return "invalid-argument";
    case GRPC_NOT_FOUND:
      return "not-found";
    case GRPC_RESOURCE_EXHAUSTED:
      return "resource-exhausted";
    case GRPC_UNAVAILABLE:
    case GRPC_DEADLINE_EXCEEDED:
      return "unavailable";
    default:
      return "internal";
  }
}
