import {
  AutoRefresh,
  ErrorState,
  RequestList,
} from "@submitqueue/web-submitqueue";
import { loadRequestList } from "@submitqueue/web-submitqueue/server";
import type { Metadata } from "next";
import { redirect } from "next/navigation";
import { connection } from "next/server";

import { DEMO_QUEUE } from "../../../server/config";
import { resolveDemoGateway } from "../../../server/gateway";
import { requireAuthorization } from "../../../server/request-auth";
import {
  defaultRequestWindow,
  parseRequestWindow,
  REQUEST_PAGE_SIZE,
  requestWindowSearch,
  type RequestSearchParams,
} from "../../../server/window";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export const metadata: Metadata = {
  title: "Requests",
};

export default async function RequestsPage({
  searchParams,
}: Readonly<{
  searchParams: Promise<RequestSearchParams>;
}>) {
  await connection();
  await requireAuthorization();

  const requestWindow = parseRequestWindow(await searchParams);
  if (!requestWindow) {
    const canonical = requestWindowSearch(defaultRequestWindow());
    redirect(`/requests?${canonical.toString()}`);
  }

  const result = await loadRequestList(resolveDemoGateway, {
    queue: DEMO_QUEUE,
    receivedAtOrAfterMs: requestWindow.fromMs,
    receivedBeforeMs: requestWindow.toMs,
    pageSize: REQUEST_PAGE_SIZE,
    pageToken: requestWindow.pageToken,
  });

  return (
    <main className="shell">
      <div className="page-heading">
        <div>
          <p className="eyebrow">Queue activity</p>
          <h1>Requests</h1>
          <p>
            Requests received in the fixed 24-hour demo window, newest first.
          </p>
        </div>
        <AutoRefresh />
      </div>

      {result.ok ? (
        <section className="panel" aria-label="Queue requests">
          <RequestList model={result.data} requestsBasePath="/requests" />
        </section>
      ) : (
        <section className="state-card">
          <ErrorState error={result.error} />
          <AutoRefresh
            terminal={!result.error.retryable}
            transientFailureCount={result.error.retryable ? 1 : 0}
          />
        </section>
      )}
    </main>
  );
}
