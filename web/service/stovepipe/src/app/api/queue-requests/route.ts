import { authenticationChallenge, isAuthorized, loadAuthConfiguration } from "../../../server/auth";
import { onLog, resolveHost } from "../../../server/host";

export const dynamic = "force-dynamic";

export async function GET(request: Request): Promise<Response> {
  if (!isAuthorized(request.headers.get("authorization"), loadAuthConfiguration())) return authenticationChallenge();
  const url = new URL(request.url);
  const path = url.searchParams.get("path");
  if (!path || !/^\/[^/]+$/u.test(path)) return new Response(null, { status: 400 });
  const result = await resolveHost().handle({
    path, search: new URLSearchParams({ page: url.searchParams.get("page") ?? "" }), signal: request.signal, onLog,
  });
  if (result.kind !== "render") return new Response(null, { status: result.kind === "forbidden" ? 403 : 404 });
  if (result.model.kind !== "queue") return new Response(null, { status: 400 });
  if (!result.model.requests.ok) return new Response(null, { status: 503 });
  return Response.json(result.model.requests.data, { headers: { "cache-control": "no-store" } });
}
