import { ErrorState } from "@submitqueue/web-submitqueue";
import { decodePathSegment, loadChangeSubmissions } from "@submitqueue/web-submitqueue/server";
import { connection } from "next/server";
import { notFound, redirect } from "next/navigation";
import { NextRefresh } from "../../../../../components/next-refresh";
import { ChangeView } from "../../../../../components/change-view";
import { DEMO_QUEUE } from "../../../../../server/config";
import { parseChangePath, changeHref } from "../../../../../server/change";
import { readChangeSubmissions } from "../../../../../server/change-reader";
import { resolveGatewayClient } from "../../../../../server/gateway";
import { requireAuthorization } from "../../../../../server/request-auth";
import { gatewayDiagnostics } from "../../../../../server/diagnostics";
import { defaultRequestWindow } from "../../../../../server/window";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export default async function ChangePage({ params, searchParams }: {
  params: Promise<{ queue: string; reference: string[] }>;
  searchParams: Promise<{ from?: string | string[]; to?: string | string[] }>;
}) {
  await connection();
  await requireAuthorization();
  const path = await params;
  const queue = decodePathSegment(decodeURIComponent(path.queue));
  const reference = parseChangePath(path.reference.map(decodeURIComponent));
  if (queue !== DEMO_QUEUE || reference === null) {
    notFound();
  }
  const search = await searchParams;
  if (search.from !== undefined || search.to !== undefined) {
    redirect(changeHref(queue, reference, reference.version !== null));
  }
  const requestWindow = defaultRequestWindow();
  const result = await loadChangeSubmissions(
    () => readChangeSubmissions(resolveGatewayClient(), queue, reference, requestWindow),
    {
      queue, provider: reference.scheme === "github" ? "GitHub" : reference.scheme === "phab" ? "Phabricator" : "Git",
      host: reference.host, repository: reference.repository, review: reference.review,
      pinnedVersion: reference.version,
      logicalHref: changeHref(queue, reference),
      window: reference.version === null || reference.scheme === "git" ? { fromMs: requestWindow.fromMs, toMs: requestWindow.toMs } : null,
    },
    { diagnostics: gatewayDiagnostics },
  );
  return <main className="shell">
    {result.ok ? <ChangeView model={result.data} /> : <ErrorState error={result.error} />}
    <div className="page-actions"><NextRefresh
      terminal={!result.ok && !result.error.retryable}
      transientFailureCount={!result.ok && result.error.retryable ? 1 : 0}
    /></div>
  </main>;
}
