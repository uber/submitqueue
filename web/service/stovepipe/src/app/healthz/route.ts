import { createClient } from "@connectrpc/connect";
import { Stovepipe } from "@submitqueue/api/stovepipe";
import { loadConfiguration } from "../../server/config";
import { loadAuthConfiguration } from "../../server/auth";
import { newConnectTransport } from "../../server/transport";
export const dynamic = "force-dynamic";
export async function GET() {
  try {
    loadAuthConfiguration();
    const client = createClient(Stovepipe, newConnectTransport({ ...loadConfiguration(), defaultTimeoutMs: 1_000 }));
    await client.ping({ message: "stovepipe-web-readiness" });
    return Response.json({ status: "ok" }, { headers: { "Cache-Control": "no-store" } });
  } catch {
    return Response.json({ status: "unavailable" }, { status: 503, headers: { "Cache-Control": "no-store" } });
  }
}
