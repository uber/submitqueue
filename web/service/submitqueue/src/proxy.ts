import { NextResponse, type NextRequest } from "next/server";

import {
  authenticationChallenge,
  isAuthorized,
  loadAuthConfiguration,
} from "./server/auth";

export function proxy(request: NextRequest): Response {
  const configuration = loadAuthConfiguration();
  if (!isAuthorized(request.headers.get("authorization"), configuration)) {
    return authenticationChallenge();
  }
  return NextResponse.next();
}

export const config = {
  matcher: [
    "/((?!healthz$|_next/static|_next/image|favicon.ico|robots.txt).*)",
  ],
};
