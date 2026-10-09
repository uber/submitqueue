import "server-only";

import { createHash, timingSafeEqual } from "node:crypto";

export const BASIC_AUTH_USERNAME = "test";
export const BASIC_AUTH_REALM = "Stovepipe demo";

export type AuthConfiguration = Readonly<{
  token: string;
}>;

export function loadAuthConfiguration(
  environment: Readonly<Record<string, string | undefined>> = process.env,
): AuthConfiguration {
  const token = environment.STOVEPIPE_WEB_TOKEN ?? "";
  if (token.length === 0) {
    throw new Error("STOVEPIPE_WEB_TOKEN must be set");
  }
  return { token };
}

function decodeBasicCredentials(
  authorization: string | null,
): Readonly<{ username: string; password: string }> | undefined {
  if (authorization === null) {
    return undefined;
  }

  const [scheme, encoded, ...extra] = authorization.trim().split(/\s+/u);
  if (scheme?.toLowerCase() !== "basic" || !encoded || extra.length !== 0) {
    return undefined;
  }
  if (
    !/^[A-Za-z0-9+/]+={0,2}$/u.test(encoded) ||
    encoded.length % 4 === 1
  ) {
    return undefined;
  }

  const decoded = Buffer.from(encoded, "base64").toString("utf8");

  const separator = decoded.indexOf(":");
  if (separator < 0) {
    return undefined;
  }
  return {
    username: decoded.slice(0, separator),
    password: decoded.slice(separator + 1),
  };
}

function digest(value: string): Buffer {
  return createHash("sha256").update(value, "utf8").digest();
}

function securelyEqual(actual: string, expected: string): boolean {
  return timingSafeEqual(digest(actual), digest(expected));
}

export function isAuthorized(
  authorization: string | null,
  configuration: AuthConfiguration,
): boolean {
  const credentials = decodeBasicCredentials(authorization);
  if (!credentials) {
    return false;
  }
  return (
    securelyEqual(credentials.username, BASIC_AUTH_USERNAME) &&
    securelyEqual(credentials.password, configuration.token)
  );
}

export function authenticationChallenge(): Response {
  return new Response("Authentication required", {
    status: 401,
    headers: {
      "Cache-Control": "no-store",
      "Content-Type": "text/plain; charset=utf-8",
      "WWW-Authenticate": `Basic realm="${BASIC_AUTH_REALM}", charset="UTF-8"`,
    },
  });
}
