import {
  RequestListView,
  Timestamp,
} from "@submitqueue/web-submitqueue";
import {
  loadRequestList,
  WebPaths,
  decodePathSegment,
} from "@submitqueue/web-submitqueue/server";
import type { Metadata } from "next";
import Link from "next/link";
import { notFound, redirect } from "next/navigation";
import { connection } from "next/server";

import { DEMO_QUEUE } from "../../../server/config";
import { gatewayDiagnostics } from "../../../server/diagnostics";
import { resolveDemoGateway } from "../../../server/gateway";
import { requireAuthorization } from "../../../server/request-auth";
import { NextRefresh } from "../../../components/next-refresh";
import { requestChangeLabels, requestChangeLinks } from "../../../server/change";
import { loadAuthConfiguration } from "../../../server/auth";
import {
  defaultRequestWindow,
  decodeRequestPage,
  encodeRequestPage,
  REQUEST_PAGE_SIZE,
  type RequestSearchParams,
} from "../../../server/window";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export const metadata: Metadata = {
  title: "Requests",
};

const paths = new WebPaths();

export default async function QueueRequestsPage({
  params,
  searchParams,
}: Readonly<{
  params: Promise<{ queue: string }>;
  searchParams: Promise<RequestSearchParams>;
}>) {
  await connection();
  await requireAuthorization();

  const queue = decodePathSegment(decodeURIComponent((await params).queue));
  if (queue !== DEMO_QUEUE) {
    notFound();
  }

  const search = await searchParams;
  if (search.from !== undefined || search.to !== undefined ||
    (search.page !== undefined && typeof search.page !== "string")) {
    redirect(paths.requests(queue));
  }
  const secret = loadAuthConfiguration().token;
  const pageWindow = typeof search.page === "string" ? decodeRequestPage(queue, search.page, secret) : undefined;
  if (search.page !== undefined && pageWindow === undefined) {
    redirect(paths.requests(queue));
  }
  const requestWindow = pageWindow ?? defaultRequestWindow();

  const loaded = await loadRequestList(
    resolveDemoGateway,
    {
      queue,
      receivedAtOrAfterMs: requestWindow.fromMs,
      receivedBeforeMs: requestWindow.toMs,
      pageSize: REQUEST_PAGE_SIZE,
      pageToken: requestWindow.pageToken,
    },
    { diagnostics: gatewayDiagnostics },
  );
  const result = loaded.ok && loaded.data.nextPageToken ? {
    ...loaded,
    data: { ...loaded.data, nextPageToken: encodeRequestPage(queue, requestWindow, loaded.data.nextPageToken, secret) },
  } : loaded;

  return (
    <main className="shell">
      <div className="page-heading">
        <div>
          <p className="eyebrow">Queue activity</p>
          <h1>{queue}</h1>
          <p>{pageWindow ? "Older requests · snapshot" : "Last 24 hours · updates on refresh"}: <Timestamp value={requestWindow.fromMs} /> — <Timestamp value={requestWindow.toMs} /></p>
        </div>
        <div className="page-actions">
          <Link href={paths.requests(queue)}>Latest 24 hours</Link>
          <NextRefresh
            terminal={pageWindow !== undefined || (!result.ok && !result.error.retryable)}
            transientFailureCount={!result.ok && result.error.retryable ? 1 : 0}
            {...(pageWindow ? { refreshHref: paths.requests(queue) } : {})}
          />
        </div>
      </div>

      <section className="panel" aria-label="Queue requests">
        <RequestListView
          key={`${queue}:${search.page ?? "live"}`}
          result={result} changeLinks={result.ok ? requestChangeLinks(queue, result.data.requests) : {}}
          changeLabels={result.ok ? requestChangeLabels(result.data.requests) : {}}
        />
      </section>
    </main>
  );
}
