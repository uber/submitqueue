"use client";

import { WebShell, type WebShellProps } from "./shell.js";
import { Block } from "baseui/block";
import { HeadingMedium, ParagraphSmall, ParagraphXSmall } from "baseui/typography";
import { WebLink } from "./web-app.js";
import { ChangeSubmissions, LastSuccessfulData, QueueBreadcrumb, QueueDirectory, QueueRequests, QueueStatusFilter, RequestDetailView } from "./components.js";
import type { SubmitQueuePageModel } from "../entity/index.js";

export type SubmitQueueShellProps = Omit<WebShellProps, "brandMark" | "homeLabel">;

export function SubmitQueueShell({ brand = "SubmitQueue", ...props }: SubmitQueueShellProps) {
  return <WebShell brand={brand} brandMark="SQ" homeLabel="SubmitQueue queues" {...props} />;
}

export interface SubmitQueuePageProps {
  model: SubmitQueuePageModel;
}

/** Renders one page model; version controls appear only inside a `WebApp` such as `SubmitQueueApp`. */
export function SubmitQueuePage({ model }: SubmitQueuePageProps) {
  return <Block as="main" paddingTop="28px" paddingBottom="48px">
    {model.kind === "queue" ? <>
      <QueueBreadcrumb queue={model.queue} basePath={model.basePath} />
      <Block display="flex" justifyContent="space-between" alignItems="flex-end" flexWrap gridGap="scale600" marginBottom="scale600">
        <div>
          <HeadingMedium as="h1" marginTop="0" marginBottom="scale300">{model.queue}</HeadingMedium>
          <ParagraphSmall margin="0" color="contentSecondary">Requests ordered by receipt time, newest first.</ParagraphSmall>
        </div>
        <QueueStatusFilter queue={model.queue} statuses={model.statuses} basePath={model.basePath} />
      </Block>
      {model.paged ? <ParagraphSmall><WebLink href={model.latestHref}>Latest requests</WebLink></ParagraphSmall> : null}
      <section className="panel" aria-label="Queue requests">
        <QueueRequests key={model.key} queue={model.queue} result={model.result} routeKey={model.key} basePath={model.basePath}
          filtered={model.statuses.length > 0} />
      </section>
    </> : model.kind === "request" ?
      <RequestDetailView key={model.key} result={model.result} view={model.view} basePath={model.basePath}
        backHref={model.backHref} summaryHref={model.summaryHref} historyHref={model.historyHref}
        changeLinks={model.changeLinks} changeLabels={model.changeLabels} />
      : <>
      {model.kind === "queues"
        ? <LastSuccessfulData key={model.key} result={model.result}>{queues => <QueueDirectory queues={queues} basePath={model.basePath} />}</LastSuccessfulData>
        : <LastSuccessfulData key={model.key} result={model.result}>{change => <ChangeSubmissions model={change} basePath={model.basePath} />}</LastSuccessfulData>}
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
        <HeadingMedium as="h1" marginTop="0" marginBottom="scale300" id="state-title">{notFound ? "Request not found" : "Authentication required"}</HeadingMedium>
        <ParagraphSmall>{message ?? (notFound ? "The queue or request may not exist, or its retained data may have expired." : "Use the credentials configured for this host.")}</ParagraphSmall>
        {notFound ? <WebLink href={homeHref}>Return to queues</WebLink> : null}
      </section>
    </Block>
  </SubmitQueueShell>;
}
