import "server-only";

import { parseGatewayBaseUrl } from "./transport";

export const GATEWAY_DEADLINE_MS = 5_000;
export const GATEWAY_HEALTH_DEADLINE_MS = 1_000;

type Environment = Readonly<Record<string, string | undefined>>;

export type GatewayConfiguration = Readonly<{
  baseUrl: string;
  allowPlaintext: boolean;
}>;

export function loadGatewayConfiguration(environment: Environment = process.env): GatewayConfiguration {
  const rawBaseUrl = environment.SUBMITQUEUE_GATEWAY_URL;
  if (!rawBaseUrl) {
    throw new Error("SUBMITQUEUE_GATEWAY_URL is required");
  }
  const allowPlaintext = environment.SUBMITQUEUE_GATEWAY_ALLOW_PLAINTEXT === "true";
  return { baseUrl: parseGatewayBaseUrl(rawBaseUrl, allowPlaintext), allowPlaintext };
}

/** The key that signs pagination cursors; independent of any viewer credential so either can rotate alone. */
export function loadCursorSecret(environment: Environment = process.env): string {
  const secret = environment.SUBMITQUEUE_WEB_CURSOR_SECRET ?? "";
  if (secret.length === 0) {
    throw new Error("SUBMITQUEUE_WEB_CURSOR_SECRET must be set");
  }
  return secret;
}

/** Port for the Prometheus metrics endpoint, or `null` when metrics export is disabled. */
export function loadMetricsPort(environment: Environment = process.env): number | null {
  const raw = environment.SUBMITQUEUE_WEB_METRICS_PORT;
  if (!raw) {
    return null;
  }
  const port = Number(raw);
  if (!Number.isInteger(port) || port <= 0 || port > 65_535) {
    throw new Error("SUBMITQUEUE_WEB_METRICS_PORT must be a TCP port");
  }
  return port;
}
