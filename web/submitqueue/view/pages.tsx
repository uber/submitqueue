"use client";

import { WebRefreshControl } from "./refresh-control.js";
import { WebShell, type WebShellProps } from "./shell.js";
import { Timestamp } from "@submitqueue/web-platform";
import { Block } from "baseui/block";
import { HeadingLarge, ParagraphSmall, ParagraphXSmall } from "baseui/typography";
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
  return <Block as="main" paddingTop="scale600" paddingBottom="scale600">
    {model.kind === "queue" ? <>
      <Block display="flex" justifyContent="space-between" alignItems="flex-start" flexWrap gridGap="scale600" marginBottom="scale600">
        <div>
          <ParagraphXSmall color="contentSecondary" marginTop="0">Queue activity</ParagraphXSmall><HeadingLarge as="h1" marginTop="0" marginBottom="scale300">{model.queue}</HeadingLarge>
          <ParagraphXSmall color="contentSecondary">{model.paged ? "Older requests · snapshot" : "Last 24 hours · updates on refresh"}: <Timestamp value={model.window.fromMs} /> — <Timestamp value={model.window.toMs} /></ParagraphXSmall>
        </div>
        <Block display="flex" alignItems="center" flexWrap gridGap="scale500"><WebLink href={model.latestHref}>Latest 24 hours</WebLink>{refreshControl}</Block>
      </Block>
      <section className="panel" aria-label="Queue requests">
        <RequestListView key={model.key} result={model.result} basePath={model.basePath}
          changeLinks={model.changeLinks} changeLabels={model.changeLabels} />
      </section>
    </> : model.kind === "request" ?
      <RequestDetailView key={model.key} result={model.result} view={model.view} basePath={model.basePath}
        backHref={model.backHref} summaryHref={model.summaryHref} historyHref={model.historyHref}
        changeLinks={model.changeLinks} changeLabels={model.changeLabels} controls={refreshControl} />
      : <>
      {model.kind === "queues"
        ? <LastSuccessfulData key={model.key} result={model.result}>{queues => <QueueDirectory queues={queues} basePath={model.basePath} />}</LastSuccessfulData>
        : <LastSuccessfulData key={model.key} result={model.result}>{change => <ChangeSubmissions model={change} basePath={model.basePath} />}</LastSuccessfulData>}
      <Block marginTop="scale500">{refreshControl}</Block>
    </>}
  </Block>;
}

export function SubmitQueueStatePage({ kind, homeHref = "/", message }: {
  kind: "not-found" | "unauthorized"; homeHref?: string; message?: string;
}) {
  const notFound = kind === "not-found";
  return <SubmitQueueShell homeHref={homeHref}>
    <Block as="main" display="grid" placeItems="center" paddingTop="scale600" paddingBottom="scale600">
      <section aria-labelledby="state-title">
        <ParagraphXSmall color="contentSecondary">{notFound ? "Request lookup" : "SubmitQueue"}</ParagraphXSmall>
        <HeadingLarge as="h1" marginTop="0" marginBottom="scale300" id="state-title">{notFound ? "Request not found" : "Authentication required"}</HeadingLarge>
        <ParagraphSmall>{message ?? (notFound ? "The queue or request may not exist, or its retained data may have expired." : "Use the credentials configured for this host.")}</ParagraphSmall>
        {notFound ? <WebLink href={homeHref}>Return to queues</WebLink> : null}
      </section>
    </Block>
  </SubmitQueueShell>;
}
