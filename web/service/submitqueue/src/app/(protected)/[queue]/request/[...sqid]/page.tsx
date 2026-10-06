import {
  RequestDetailView,
} from "@submitqueue/web-submitqueue";
import {
  loadRequestDetail,
  requestDetailRefreshState,
  WebPaths,
  decodePathSegment,
} from "@submitqueue/web-submitqueue/server";
import type { Metadata } from "next";
import { notFound, redirect } from "next/navigation";
import { connection } from "next/server";

import { loadConfiguredQueue } from "../../../../../server/queues";
import { gatewayDiagnostics } from "../../../../../server/diagnostics";
import { resolveDemoGateway } from "../../../../../server/gateway";
import { requireAuthorization } from "../../../../../server/request-auth";
import { NextRefresh } from "../../../../../components/next-refresh";
import { requestChangeLabels, requestChangeLinks } from "../../../../../server/change";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export const metadata: Metadata = {
  title: "Request status",
};

const paths = new WebPaths();

export default async function RequestPage({
  params,
  searchParams,
}: Readonly<{
  params: Promise<{ queue: string; sqid: string[] }>;
  searchParams: Promise<{ view?: string | string[]; from?: string | string[]; to?: string | string[]; page?: string | string[] }>;
}>) {
  await connection();
  await requireAuthorization();

  const { queue: queuePath, sqid: sqidPath } = await params;
  const queue = decodePathSegment(decodeURIComponent(queuePath));
  if (sqidPath.length === 0) {
    notFound();
  }
  const configured = await loadConfiguredQueue(queue);
  if (configured.ok && !configured.data) {
    notFound();
  }
  const sqid = sqidPath.map(value => decodePathSegment(decodeURIComponent(value))).join("/");
  const search = await searchParams;
  const view = search.view === "history" ? "history" : "summary";
  if (search.from !== undefined || search.to !== undefined || search.page !== undefined) {
    redirect(paths.request(queue, sqid, { view }));
  }

  const result = configured.ok ? await loadRequestDetail(
    resolveDemoGateway,
    {
      queue,
      sqid,
    },
    { diagnostics: gatewayDiagnostics },
  ) : configured;

  if (!result.ok && result.error.kind === "not-found") {
    notFound();
  }

  const refreshState = result.ok ? requestDetailRefreshState(result.data) : {
    terminal: !result.error.retryable,
    transientFailureCount: result.error.retryable ? 1 : 0,
  };

  return (
    <main className="shell">
      <RequestDetailView
        key={`${queue}:${sqid}`}
        result={result} view={view}
        backHref={paths.requests(queue)}
        summaryHref={paths.request(queue, sqid)}
        historyHref={paths.request(queue, sqid, { view: "history" })}
        changeLinks={result.ok ? requestChangeLinks(queue, [result.data.request]) : {}}
        changeLabels={result.ok ? requestChangeLabels([result.data.request]) : {}}
        controls={<NextRefresh {...refreshState} />}
      />
    </main>
  );
}
