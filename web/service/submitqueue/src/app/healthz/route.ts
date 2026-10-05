import { loadAuthConfiguration } from "../../server/auth";
import { loadGatewayConfiguration } from "../../server/config";
import { pingDemoGateway } from "../../server/gateway";

export const dynamic = "force-dynamic";

let lastConfigurationFailure: string | null = null;
let gatewayFailureReported = false;

function reportConfigurationFailure(cause: unknown): void {
  const message =
    cause instanceof Error ? cause.message : "Unknown configuration failure";
  if (message === lastConfigurationFailure) {
    return;
  }
  lastConfigurationFailure = message;
  console.error("SubmitQueue web configuration is invalid", { message });
}

export async function GET(): Promise<Response> {
  try {
    loadAuthConfiguration();
    loadGatewayConfiguration();
    lastConfigurationFailure = null;
  } catch (cause) {
    reportConfigurationFailure(cause);
    return Response.json(
      { status: "unavailable" },
      { status: 503, headers: { "Cache-Control": "no-store" } },
    );
  }

  try {
    await pingDemoGateway();
    gatewayFailureReported = false;
  } catch (cause) {
    if (!gatewayFailureReported) {
      gatewayFailureReported = true;
      console.error("SubmitQueue web gateway readiness check failed", { cause });
    }
    return Response.json(
      { status: "unavailable" },
      { status: 503, headers: { "Cache-Control": "no-store" } },
    );
  }
  return Response.json(
    { status: "ok" },
    { headers: { "Cache-Control": "no-store" } },
  );
}
