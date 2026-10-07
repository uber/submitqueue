import "server-only";

import type { Interceptor, Transport } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";

export interface ConnectTransportConfig {
  /** Gateway origin, `https://host[:port]`; `http://` only with `allowPlaintext`. */
  baseUrl: string;
  /** Permits plaintext HTTP/2 (h2c), for local stacks only. */
  allowPlaintext?: boolean;
  /** Deadline applied to calls that set none. */
  defaultTimeoutMs?: number;
  interceptors?: readonly Interceptor[];
}

/**
 * Validates a gateway URL and returns its origin. Throws on invalid
 * configuration, which is a startup failure rather than a per-request outcome.
 */
export function parseGatewayBaseUrl(raw: string, allowPlaintext = false): string {
  if (!URL.canParse(raw)) {
    throw new Error("The gateway URL must be a valid absolute URL");
  }
  const url = new URL(raw);
  if (url.pathname !== "/" || url.search || url.hash) {
    throw new Error("The gateway URL must not contain a path, query, or fragment");
  }
  if (url.username || url.password) {
    throw new Error("The gateway URL must not contain credentials");
  }
  if (url.protocol === "http:" && !allowPlaintext) {
    throw new Error("Plaintext gateway connections must be explicitly allowed");
  }
  if (url.protocol !== "https:" && url.protocol !== "http:") {
    throw new Error("The gateway URL must use https, or explicitly allowed http");
  }
  return url.origin;
}

/** A gRPC transport for Node; TLS unless plaintext is explicitly allowed. */
export function newConnectTransport(config: ConnectTransportConfig): Transport {
  return createGrpcTransport({
    baseUrl: parseGatewayBaseUrl(config.baseUrl, config.allowPlaintext),
    interceptors: [...(config.interceptors ?? [])],
    ...(config.defaultTimeoutMs === undefined ? {} : { defaultTimeoutMs: config.defaultTimeoutMs }),
  });
}
