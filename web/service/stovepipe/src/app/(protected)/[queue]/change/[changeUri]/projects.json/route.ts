import { create, toJson } from "@bufbuild/protobuf";
import { ProjectValidationSchema } from "@submitqueue/api/stovepipe";
import { resolveHost } from "../../../../../../server/host";
import { authenticationChallenge, isAuthorized, loadAuthConfiguration } from "../../../../../../server/auth";

export const dynamic = "force-dynamic";
export async function GET(request: Request): Promise<Response> {
  if (!isAuthorized(request.headers.get("authorization"), loadAuthConfiguration())) return authenticationChallenge();
  const url = new URL(request.url);
  const result = await resolveHost().handle({ path: url.pathname, search: url.searchParams, signal: request.signal });
  const headers = { "Cache-Control": "private, no-store" };
  if (result.kind === "project-json") {
    return Response.json(result.data.map(project => toJson(ProjectValidationSchema, create(ProjectValidationSchema, project))), { headers });
  }
  if (result.kind === "unavailable") return Response.json({ error: result.error }, { status: 502, headers });
  return Response.json({ error: result.kind === "forbidden" ? "Forbidden" : "Not found" }, { status: result.kind === "forbidden" ? 403 : 404, headers });
}
