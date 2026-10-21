"use client";

import { Fact, Timestamp } from "@submitqueue/web-platform";
import { Block } from "baseui/block/index.js";
import { Tag } from "baseui/tag/index.js";
import { HeadingLarge, HeadingSmall, LabelMedium, ParagraphSmall } from "baseui/typography/index.js";
import { RefreshControls } from "./refresh.js";
import type { StovepipePageModel } from "../entity/index.js";
import { Link, Section } from "./components.js";
import { RequestList, RequestDetails, History } from "./pages.js";
import { ProjectResults, RecordedResult } from "./projects.js";

function Page({ model }: { model: StovepipePageModel }) {
  return (
    <Block className="sq-app" backgroundColor="backgroundPrimary" color="contentPrimary" font="ParagraphSmall"
      minHeight="100vh" display="grid" gridTemplateColumns="minmax(0, 1fr)" gridTemplateRows="auto 1fr auto"
      paddingLeft="scale600" paddingRight="scale600">
      <Block as="header" paddingTop="scale300" paddingBottom="scale300">
        <Block as={Link} display="inline-flex" alignItems="center" gridGap="scale300" href={model.queueHref}>
          <Tag closeable={false} kind="neutral" size="small">SP</Tag><LabelMedium as="span">Stovepipe</LabelMedium>
        </Block>
      </Block>
      <Block as="main" paddingTop="scale600" paddingBottom="scale600">
        <Block as="nav" aria-label="Queues" display="flex" alignItems="center" flexWrap gridGap="scale500" marginBottom="scale600">
          {model.queues.map((queue) => (
            <Link
              key={queue.name}
              href={queue.href}
              aria-current={queue.name === model.queue ? "page" : undefined}
            >
              {queue.name}
            </Link>
          ))}
        </Block>
        <Block display="flex" justifyContent="space-between" alignItems="flex-start" flexWrap gridGap="scale600" marginBottom="scale600">
          <div>
            {model.kind === "request" && (
              <ParagraphSmall>
                <Link href={model.queueHref}>← Queue requests</Link>
              </ParagraphSmall>
            )}
            <HeadingLarge marginTop="0" marginBottom="scale300" as="h1">
              {model.kind === "queue"
                ? model.queue
                : `Request ${model.requestId}`}
            </HeadingLarge>
            <ParagraphSmall marginTop="0">
              {model.kind === "queue"
                ? "Requests ordered by acceptance time, newest first."
                : model.queue}
            </ParagraphSmall>
          </div>
          <RefreshControls />
        </Block>
        {model.kind === "queue" && model.showLatestLink && (
          <ParagraphSmall>
            <Link href={model.latestHref}>Latest page</Link>
          </ParagraphSmall>
        )}
        {model.kind === "queue" ? (
          <Section result={model.requests}>
            {(data) => data && <RequestList {...data} />}
          </Section>
        ) : (
          <Section result={model.history}>
            {(history) => (
              <Section result={model.projectStatus}>
                {(projects) => (
                  <Block display="grid" gridTemplateColumns="minmax(0, 1fr)" gridGap="scale600">
                    <section aria-label="Request status">
                      <HeadingSmall as="h2" marginTop="0">Request status</HeadingSmall>
                      {history && <RequestDetails request={history.request} />}
                      {projects && (
                        <Block as="dl" margin="0">
                          <Fact label="Repository result">
                            <RecordedResult degree={projects.repositoryBreakageDegree} />
                          </Fact>
                        </Block>
                      )}
                    </section>
                    <section aria-label="History">
                      <HeadingSmall as="h2" marginTop="0">History</HeadingSmall>
                      {history && <History events={history.events} />}
                    </section>
                    <section aria-label="Project results">
                      <HeadingSmall as="h2" marginTop="0">Project results</HeadingSmall>
                      {projects && <ProjectResults model={projects} />}
                    </section>
                  </Block>
                )}
              </Section>
            )}
          </Section>
        )}
      </Block>
      <Block as="footer" paddingTop="scale300" paddingBottom="scale300" font="ParagraphXSmall" color="contentSecondary">
        Loaded <Timestamp value={model.loadedAtMs} />
      </Block>
    </Block>
  );
}
export function StovepipeApp({ model }: { model: StovepipePageModel }) {
  return <Page key={model.key} model={model} />;
}
