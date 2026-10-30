import { decodeQueuePathSegment, type WebSearchParams } from "@submitqueue/web-submitqueue/server";
import { authenticationChallenge, isAuthorized, loadAuthConfiguration } from "../../../../../server/auth";
import { resolveReferenceWeb } from "../../../../../server/host";
import { resolveReferenceLogCallback } from "../../../../../server/observability";

export const dynamic = "force-dynamic";

const statusCodes = { "not-found": 404, forbidden: 403, invalid: 400, unavailable: 503 } as const;

/** `GET /api/queue/<queue>/requests?status=…&page=…`: the queue page's requests as JSON. */
export async function GET(request: Request): Promise<Response> {
  if (!isAuthorized(request.headers.get("authorization"), loadAuthConfiguration())) return authenticationChallenge();
  const url = new URL(request.url);
  // The raw segment, not Next's params, so an encoded "/" in a queue name is decoded exactly once.
  const queue = decodeQueuePathSegment(url.pathname.split("/")[3] ?? "");
  if (queue === null) return new Response(null, { status: 400 });
  const page = url.searchParams.get("page");
  const statuses = url.searchParams.getAll("status");
  const search: WebSearchParams = {
    ...(page === null ? {} : { page }),
    ...(statuses.length > 0 ? { status: statuses } : {}),
  };
  const result = await resolveReferenceWeb().queueRequests({
    queue, search, signal: request.signal, onLog: resolveReferenceLogCallback(),
  });
  if (result.kind !== "ok") return new Response(null, { status: statusCodes[result.kind] });
  return Response.json(result.page, { headers: { "cache-control": "no-store" } });
}
