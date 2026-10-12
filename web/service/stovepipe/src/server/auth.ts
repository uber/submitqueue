import "server-only";

import {
  isAuthorized as checkCredentials,
  authenticationChallenge as challenge,
} from "@submitqueue/web-service-shared/auth";

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

export function isAuthorized(
  authorization: string | null,
  configuration: AuthConfiguration,
): boolean {
  return checkCredentials(authorization, {
    ...configuration,
    username: BASIC_AUTH_USERNAME,
  });
}

export function authenticationChallenge(): Response {
  return challenge(BASIC_AUTH_REALM);
}
