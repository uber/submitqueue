import "server-only";

export const GATEWAY_DEADLINE_MS = 5_000;
export const GATEWAY_HEALTH_DEADLINE_MS = 1_000;

export type GatewayConfiguration = Readonly<{
  baseUrl: string;
  allowPlaintext: boolean;
}>;

export function loadGatewayConfiguration(
  environment: Readonly<Record<string, string | undefined>> = process.env,
): GatewayConfiguration {
  const rawBaseUrl = environment.SUBMITQUEUE_GATEWAY_URL;
  if (!rawBaseUrl) {
    throw new Error("SUBMITQUEUE_GATEWAY_URL is required");
  }

  if (!URL.canParse(rawBaseUrl)) {
    throw new Error("SUBMITQUEUE_GATEWAY_URL must be a valid absolute URL");
  }
  const gatewayUrl = new URL(rawBaseUrl);

  if (gatewayUrl.pathname !== "/" || gatewayUrl.search || gatewayUrl.hash) {
    throw new Error("SUBMITQUEUE_GATEWAY_URL must not contain a path, query, or fragment");
  }
  if (gatewayUrl.username || gatewayUrl.password) {
    throw new Error("SUBMITQUEUE_GATEWAY_URL must not contain credentials");
  }

  const allowPlaintext = environment.SUBMITQUEUE_GATEWAY_ALLOW_PLAINTEXT === "true";
  if (gatewayUrl.protocol === "http:" && !allowPlaintext) {
    throw new Error(
      "Plaintext gateway connections require SUBMITQUEUE_GATEWAY_ALLOW_PLAINTEXT=true",
    );
  }
  if (gatewayUrl.protocol !== "https:" && gatewayUrl.protocol !== "http:") {
    throw new Error("SUBMITQUEUE_GATEWAY_URL must use https, or explicitly enabled http");
  }

  return {
    baseUrl: gatewayUrl.origin,
    allowPlaintext,
  };
}
