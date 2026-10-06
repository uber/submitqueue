import { connection } from "next/server";
import { ErrorState, QueueDirectory } from "@submitqueue/web-submitqueue";
import { loadQueueDirectory } from "@submitqueue/web-submitqueue/server";

import { resolveGatewayClient } from "../../server/gateway";
import { gatewayDiagnostics } from "../../server/diagnostics";
import { NextRefresh } from "../../components/next-refresh";
import { requireAuthorization } from "../../server/request-auth";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export default async function HomePage() {
  await connection();
  await requireAuthorization();
  const result = await loadQueueDirectory(resolveGatewayClient(), { diagnostics: gatewayDiagnostics });
  return <main className="shell">
    {result.ok ? <QueueDirectory queues={result.data} /> : <ErrorState error={result.error} />}
    <div className="page-actions"><NextRefresh
      terminal={!result.ok && !result.error.retryable}
      transientFailureCount={!result.ok && result.error.retryable ? 1 : 0}
    /></div>
  </main>;
}
