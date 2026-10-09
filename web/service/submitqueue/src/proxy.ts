import { NextResponse, type NextRequest } from "next/server";

import { authenticationChallenge, isAuthorized, loadAuthConfiguration } from "./server/auth";

// Pages repeat the check themselves; this only challenges anonymous browsers early.
export function proxy(request: NextRequest): Response {
  return isAuthorized(request.headers.get("authorization"), loadAuthConfiguration())
    ? NextResponse.next()
    : authenticationChallenge();
}

export const config = {
  matcher: [
    "/((?!healthz$|_next/static|_next/image|favicon.ico|robots.txt).*)",
  ],
};
