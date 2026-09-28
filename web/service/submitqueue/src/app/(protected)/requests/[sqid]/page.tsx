import {
  AutoRefresh,
  ErrorState,
  RequestDetail,
} from "@submitqueue/web-submitqueue";
import {
  isTerminalStatus,
  loadRequestDetail,
  WebPaths,
} from "@submitqueue/web-submitqueue/server";
import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { connection } from "next/server";

import { DEMO_QUEUE } from "../../../../server/config";
import { resolveDemoGateway } from "../../../../server/gateway";
import { requireAuthorization } from "../../../../server/request-auth";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export const metadata: Metadata = {
  title: "Request status",
};

const paths = new WebPaths();

export default async function RequestPage({
  params,
}: Readonly<{
  params: Promise<{ sqid: string }>;
}>) {
  await connection();
  await requireAuthorization();

  const sqid = paths.decodeRequest((await params).sqid);
  if (!sqid) {
    notFound();
  }

  const result = await loadRequestDetail(resolveDemoGateway, {
    queue: DEMO_QUEUE,
    sqid,
  });

  if (!result.ok) {
    if (result.error.kind === "not-found") {
      notFound();
    }
    return (
      <main className="shell">
        <div className="page-heading">
          <div>
            <p className="eyebrow">Request status</p>
            <h1>Unable to load request</h1>
          </div>
        </div>
        <section className="state-card">
          <ErrorState error={result.error} />
          <AutoRefresh
            terminal={!result.error.retryable}
            transientFailureCount={result.error.retryable ? 1 : 0}
          />
        </section>
      </main>
    );
  }

  return (
    <main className="shell">
      <div className="page-heading">
        <div>
          <p className="eyebrow">
            <Link href="/requests">Requests</Link> / Detail
          </p>
          <h1>Request status</h1>
          <p className="request-identifier">{result.data.request.sqid}</p>
        </div>
        <AutoRefresh
          terminal={isTerminalStatus(result.data.request.status)}
        />
      </div>
      <RequestDetail model={result.data} />
    </main>
  );
}
