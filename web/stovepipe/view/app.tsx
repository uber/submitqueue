"use client";

import { Timestamp } from "@submitqueue/web-platform";
import { RefreshControls } from "./refresh.js";
import type { StovepipePageModel } from "../entity/index.js";
import { Link, Section } from "./components.js";
import { RequestList, RequestDetails, History } from "./pages.js";

function Page({ model }: { model: StovepipePageModel }) {
  return (
    <div className="sq-app site-frame">
      <header className="site-header">
        <Link className="brand" href={model.queueHref}>
          <span className="brand-mark">SP</span>Stovepipe
        </Link>
      </header>
      <main className="shell">
        <nav aria-label="Queues" className="sq-breadcrumb">
          {model.queues.map((queue) => (
            <Link
              key={queue.name}
              href={queue.href}
              aria-current={queue.name === model.queue ? "page" : undefined}
            >
              {queue.name}
            </Link>
          ))}
        </nav>
        <div className="page-heading">
          <div>
            {model.kind === "request" && (
              <p>
                <Link href={model.queueHref}>← Queue requests</Link>
              </p>
            )}
            <h1>
              {model.kind === "queue"
                ? model.queue
                : `Request ${model.requestId}`}
            </h1>
            <p>
              {model.kind === "queue"
                ? "Requests ordered by acceptance time, newest first."
                : model.queue}
            </p>
          </div>
          <RefreshControls />
        </div>
        {model.kind === "queue" && model.showLatestLink && (
          <p>
            <Link href={model.latestHref}>Latest page</Link>
          </p>
        )}
        {model.kind === "queue" ? (
          <Section result={model.requests}>
            {(data) => <RequestList {...data} />}
          </Section>
        ) : (
          <Section result={model.history}>
            {({ request, events }) => (
              <div className="sq-stacked-sections">
                <section aria-label="Request status">
                  <h2>Request status</h2>
                  <RequestDetails request={request} />
                </section>
                <section aria-label="History">
                  <h2>History</h2>
                  <History events={events} />
                </section>
              </div>
            )}
          </Section>
        )}
      </main>
      <footer className="site-footer">
        Loaded <Timestamp value={model.loadedAtMs} />
      </footer>
    </div>
  );
}
export function StovepipeApp({ model }: { model: StovepipePageModel }) {
  return <Page key={model.key} model={model} />;
}
