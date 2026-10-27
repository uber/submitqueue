"use client";

import { Timestamp, TimeZoneProvider, TimeZoneToggle } from "@submitqueue/web-platform";
import { styled } from "baseui";
import { Block } from "baseui/block";
import { Tag } from "baseui/tag";
import { HeadingLarge, HeadingMedium, HeadingSmall, LabelMedium, ParagraphSmall } from "baseui/typography";
import { RefreshControls } from "./refresh.js";
import type { StovepipePageModel } from "../entity/index.js";
import { Link, Section } from "./components.js";
import { RequestDetails, History } from "./pages.js";
import { QueueRequests } from "./queue.js";
import { ProjectResults, RecordedResult } from "./projects.js";

const RequestViewLink = styled<typeof Link, { $active: boolean }>(Link, ({ $theme, $active }) => ({
  display: "inline-block",
  paddingTop: $theme.sizing.scale400,
  paddingBottom: $theme.sizing.scale400,
  textDecoration: "none",
  borderBottom: `2px solid ${$active ? $theme.colors.contentPrimary : "transparent"}`,
}));

const RequestViewNavigation = styled("nav", ({ $theme }) => ({
  display: "flex",
  gap: $theme.sizing.scale700,
  borderBottom: `1px solid ${$theme.colors.borderOpaque}`,
}));

function requestViews(routeKey: string) {
  const [path, ...query] = routeKey.split("?");
  const search = new URLSearchParams(query.join("?"));
  const historySelected = search.get("view") === "history";
  search.delete("view");
  const projectsHref = path + (search.size ? `?${search}` : "");
  search.set("view", "history");
  return { historySelected, projectsHref, historyHref: `${path}?${search}` };
}

function Page({ model }: { model: StovepipePageModel }) {
  const views = requestViews(model.key);
  return (
    <Block className={model.kind === "request" ? "sq-app sq-stovepipe-request" : "sq-app"} backgroundColor="backgroundPrimary" color="contentPrimary" font="ParagraphSmall"
      minHeight="100vh" display="grid" gridTemplateColumns="minmax(0, 1fr)" gridTemplateRows="auto 1fr auto"
      paddingLeft="scale600" paddingRight="scale600">
      <Block as="header" display="flex" alignItems="center" justifyContent="space-between" flexWrap gridGap="scale600"
        paddingTop="scale300" paddingBottom="scale300">
        <Block as={Link} display="inline-flex" alignItems="center" gridGap="scale300" href={model.queueHref}>
          <Tag closeable={false} kind="neutral" size="small">SP</Tag><LabelMedium as="span">Stovepipe</LabelMedium>
        </Block>
        <Block display="flex" alignItems="center" flexWrap gridGap="scale600">
          <TimeZoneToggle /><RefreshControls />
        </Block>
      </Block>
      <Block as="main" paddingTop="scale400" paddingBottom="scale600">
        <Block as="nav" aria-label="Queues" display="flex" alignItems="center" flexWrap gridGap="scale500" marginBottom="scale400">
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
        <Block display="flex" justifyContent="space-between" alignItems="flex-start" flexWrap gridGap="scale400" marginBottom="scale400">
          <div>
            {model.kind === "request" && (
              <ParagraphSmall marginTop="0" marginBottom="scale300">
                <Link href={model.queueHref}>← Queue requests</Link> / {model.queue}
              </ParagraphSmall>
            )}
            {model.kind === "queue" ? <>
              <HeadingLarge marginTop="0" marginBottom="scale300" as="h1">{model.queue}</HeadingLarge>
              <ParagraphSmall marginTop="0">Requests ordered by acceptance time, newest first.</ParagraphSmall>
            </> : <HeadingMedium marginTop="0" marginBottom="0" as="h1">Request {model.requestId}</HeadingMedium>}
          </div>
        </Block>
        {model.kind === "queue" && model.showLatestLink && (
          <ParagraphSmall>
            <Link href={model.latestHref}>Latest page</Link>
          </ParagraphSmall>
        )}
        {model.kind === "queue" ? (
          <Section result={model.requests}>
            {(data) => data && <QueueRequests initial={data} routeKey={model.key} />}
          </Section>
        ) : (
          <Section result={model.history}>
            {(history) => (
              <Section cacheKey={views.projectsHref} result={model.projectStatus}>
                {(projects) => (
                  <Block display="grid" gridTemplateColumns="minmax(0, 1fr)" gridGap="scale400">
                    <section aria-label="Request status">
                      <RequestDetails request={history?.request ?? null} repositoryResult={projects && <RecordedResult degree={projects.repositoryBreakageDegree} />} />
                    </section>
                    <RequestViewNavigation aria-label="Request views">
                      <RequestViewLink $active={!views.historySelected} href={views.projectsHref} scroll={false} aria-current={!views.historySelected ? "page" : undefined}>Project results</RequestViewLink>
                      <RequestViewLink $active={views.historySelected} href={views.historyHref} scroll={false} aria-current={views.historySelected ? "page" : undefined}>History</RequestViewLink>
                    </RequestViewNavigation>
                    <section aria-label="History" hidden={!views.historySelected}>
                      <HeadingSmall as="h2" marginTop="0" marginBottom="scale400">History</HeadingSmall>
                      {history && <History events={history.events} />}
                    </section>
                    <section aria-label="Project results" hidden={views.historySelected}>
                      <HeadingSmall as="h2" marginTop="0" marginBottom="scale400">Project results</HeadingSmall>
                      {projects && <ProjectResults model={projects} pageKey={views.projectsHref} />}
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
  const pageKey = model.kind === "request" ? model.key.split("?")[0] : model.key;
  return <TimeZoneProvider><Page key={pageKey} model={model} /></TimeZoneProvider>;
}
