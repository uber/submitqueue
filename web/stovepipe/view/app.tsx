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
  flexWrap: "wrap",
  columnGap: $theme.sizing.scale700,
  borderBottom: `1px solid ${$theme.colors.borderOpaque}`,
}));

const requestViewTabs = [
  { id: "projects", label: "Project Results" },
  { id: "history", label: "History" },
  { id: "issues", label: "Build and Test Issues" },
  { id: "fix-attempts", label: "Fix Attempts" },
] as const;

function requestViews(routeKey: string) {
  const [path, ...query] = routeKey.split("?");
  const search = new URLSearchParams(query.join("?"));
  const selected = requestViewTabs.find(tab => tab.id === search.get("view"))?.id ?? "projects";
  search.delete("view");
  const projectsHref = path + (search.size ? `?${search}` : "");
  const tabs = requestViewTabs.map(tab => {
    const params = new URLSearchParams(search);
    if (tab.id !== "projects") params.set("view", tab.id);
    return { ...tab, selected: tab.id === selected, href: path + (params.size ? `?${params}` : "") };
  });
  return { projectsHref, tabs };
}

function Page({ model }: { model: StovepipePageModel }) {
  const views = requestViews(model.key);
  return (
    <Block className={model.kind === "request" ? "sq-app sq-stovepipe-request" : "sq-app"} backgroundColor="backgroundPrimary" color="contentPrimary" font="ParagraphSmall"
      minHeight="100vh" height={model.kind === "request" ? "100dvh" : "auto"} display="grid" gridTemplateColumns="minmax(0, 1fr)" gridTemplateRows={model.kind === "request" ? "auto minmax(min-content, 1fr) auto" : "auto 1fr auto"}
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
                  <Block className="sq-stovepipe-request-content" display="flex" flexDirection="column" gridGap="scale400">
                    <section aria-label="Request status">
                      <RequestDetails request={history?.request ?? null} repositoryResult={projects && <RecordedResult degree={projects.repositoryBreakageDegree} />} />
                    </section>
                    <RequestViewNavigation aria-label="Request views">
                      {views.tabs.map(tab => (
                        <RequestViewLink key={tab.id} $active={tab.selected} href={tab.href} scroll={false} aria-current={tab.selected ? "page" : undefined}>{tab.label}</RequestViewLink>
                      ))}
                    </RequestViewNavigation>
                    {views.tabs.map(tab => (
                      <section key={tab.id} className="sq-stovepipe-request-view" aria-label={tab.label} hidden={!tab.selected}>
                        <HeadingSmall as="h2" marginTop="0" marginBottom="scale400">{tab.label}</HeadingSmall>
                        {tab.id === "projects" ? projects && <ProjectResults model={projects} pageKey={views.projectsHref} />
                          : tab.id === "history" ? history && <History events={history.events} />
                            : <ParagraphSmall color="contentSecondary">Coming soon</ParagraphSmall>}
                      </section>
                    ))}
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
