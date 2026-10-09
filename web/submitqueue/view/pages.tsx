"use client";

import { HeadingSmall } from "baseui/typography";

import { WebRefreshControl } from "./refresh-control.js";
import { WebShell, type WebShellProps } from "./shell.js";
import { Timestamp } from "./timestamp.js";
import { WebLink } from "./web-app.js";
import { ChangeSubmissions, LastSuccessfulData, QueueDirectory, RequestDetailView, RequestListView } from "./components.js";
import type { SubmitQueuePageModel } from "../entity/index.js";

export type SubmitQueueShellProps = Omit<WebShellProps, "brandMark" | "homeLabel">;

export function SubmitQueueShell({ brand = "SubmitQueue", ...props }: SubmitQueueShellProps) {
  return <WebShell brand={brand} brandMark="SQ" homeLabel="SubmitQueue queues" {...props} />;
}

export interface SubmitQueuePageProps {
  model: SubmitQueuePageModel;
}

/** Renders one page model; refresh and version controls appear only inside a `WebApp` such as `SubmitQueueApp`. */
export function SubmitQueuePage({ model }: SubmitQueuePageProps) {
  const refreshControl = <WebRefreshControl refresh={model.refresh} />;
  if (model.kind === "queue") {
    return <main className="shell">
      <div className="page-heading">
        <div>
          <p className="eyebrow">Queue activity</p><HeadingSmall as="h1" marginTop="0" marginBottom="scale400">{model.queue}</HeadingSmall>
          <p>{model.paged ? "Older requests · snapshot" : "Last 24 hours · updates on refresh"}: <Timestamp value={model.window.fromMs} /> — <Timestamp value={model.window.toMs} /></p>
        </div>
        <div className="page-actions"><WebLink href={model.latestHref}>Latest 24 hours</WebLink>{refreshControl}</div>
      </div>
      <section className="panel" aria-label="Queue requests">
        <RequestListView key={model.key} result={model.result} basePath={model.basePath}
          changeLinks={model.changeLinks} changeLabels={model.changeLabels} />
      </section>
    </main>;
  }
  if (model.kind === "request") {
    return <main className="shell">
      <RequestDetailView key={model.key} result={model.result} view={model.view} basePath={model.basePath}
        backHref={model.backHref} summaryHref={model.summaryHref} historyHref={model.historyHref}
        changeLinks={model.changeLinks} changeLabels={model.changeLabels} controls={refreshControl} />
    </main>;
  }
  return <main className="shell">
    {model.kind === "queues"
      ? <LastSuccessfulData key={model.key} result={model.result}>{queues => <QueueDirectory queues={queues} basePath={model.basePath} />}</LastSuccessfulData>
      : <LastSuccessfulData key={model.key} result={model.result}>{change => <ChangeSubmissions model={change} basePath={model.basePath} />}</LastSuccessfulData>}
    <div className="page-actions">{refreshControl}</div>
  </main>;
}

export function SubmitQueueStatePage({ kind, homeHref = "/", message }: {
  kind: "not-found" | "unauthorized"; homeHref?: string; message?: string;
}) {
  const notFound = kind === "not-found";
  return <SubmitQueueShell homeHref={homeHref}>
    <main className="shell centered-state">
      <section className="state-card" aria-labelledby="state-title">
        <p className="eyebrow">{notFound ? "Request lookup" : "SubmitQueue"}</p>
        <HeadingSmall as="h1" marginTop="0" marginBottom="scale400" id="state-title">{notFound ? "Request not found" : "Authentication required"}</HeadingSmall>
        <p>{message ?? (notFound ? "The queue or request may not exist, or its retained data may have expired." : "Use the credentials configured for this host.")}</p>
        {notFound ? <a className="button-link" href={homeHref}>Return to queues</a> : null}
      </section>
    </main>
  </SubmitQueueShell>;
}
