import { createNextHealthRoute } from "@submitqueue/web-service-shared/health";
import { loadAuthConfiguration } from "../../server/auth";
import { loadCursorSecret, loadGatewayConfiguration } from "../../server/config";
import { pingReferenceGateway } from "../../server/gateway";
import { resolveReferenceLogCallback } from "../../server/observability";

export const dynamic = "force-dynamic";

export const GET = createNextHealthRoute({
  configuration: () => {
    loadAuthConfiguration();
    loadCursorSecret();
    loadGatewayConfiguration();
  },
  gateway: pingReferenceGateway,
}, resolveReferenceLogCallback());
