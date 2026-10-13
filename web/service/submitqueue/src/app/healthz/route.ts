import { createNextHealthRoute } from "../../next/health";
import { loadAuthConfiguration } from "../../server/auth";
import { loadCursorSecret, loadGatewayConfiguration } from "../../server/config";
import { pingReferenceGateway } from "../../server/gateway";
import { resolveReferenceLogger } from "../../server/observability";

export const dynamic = "force-dynamic";

export const GET = createNextHealthRoute({
  configuration: () => {
    loadAuthConfiguration();
    loadCursorSecret();
    loadGatewayConfiguration();
  },
  gateway: pingReferenceGateway,
}, resolveReferenceLogger());
