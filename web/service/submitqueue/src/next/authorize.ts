import "server-only";

import { headers } from "next/headers";
import { unauthorized } from "next/navigation";

import { isAuthorized, loadAuthConfiguration } from "../server/auth";

export async function requireAuthorization(): Promise<void> {
  const requestHeaders = await headers();
  if (
    !isAuthorized(
      requestHeaders.get("authorization"),
      loadAuthConfiguration(),
    )
  ) {
    unauthorized();
  }
}
