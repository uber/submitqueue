import "server-only";

import type {
  GatewayDiagnostic,
  GatewayDiagnostics,
} from "@submitqueue/web-submitqueue/server";

function reportGatewayError(diagnostic: GatewayDiagnostic): void {
  console.error("SubmitQueue gateway request failed", diagnostic);
}

export const gatewayDiagnostics: GatewayDiagnostics = {
  onGatewayError: reportGatewayError,
};
