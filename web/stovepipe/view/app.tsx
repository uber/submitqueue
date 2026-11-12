"use client";

import { AppBrand, AppHeader, Timestamp, TimeZoneProvider } from "@submitqueue/web-platform";
import { styled } from "baseui";
import { Block } from "baseui/block";
import { HeadingLarge, HeadingMedium, HeadingSmall, ParagraphSmall } from "baseui/typography";
import { RefreshControls } from "./refresh.js";
import type { StovepipePageModel } from "../entity/index.js";
import { Link, Section } from "./components.js";
import { RequestDetails, History } from "./pages.js";
import { QueueRequests } from "./queue.js";
import { OpenIssues, RequestIssues } from "./issues.js";
import { ProjectResults, RecordedResult } from "./projects.js";
import { StovepipeLogo } from "./stovepipe-logo.js";

const ViewLink = styled<typeof Link, { $active: boolean }>(Link, ({ $theme, $active }) => ({
  display: "inline-block",
  paddingTop: $theme.sizing.scale400,
  paddingBottom: $theme.sizing.scale400,
  textDecoration: "none",
  borderBottom: `2px solid ${$active ? $theme.colors.contentPrimary : "transparent"}`,
}));

const ViewNavigation = styled("nav", ({ $theme }) => ({
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
  const projectPage = new URLSearchParams();
  for (const key of ["projects", "projectPageSize"]) {
    const value = search.get(key);
    if (value) projectPage.set(key, value);
  }
  const projectsHref = path + (projectPage.size ? `?${projectPage}` : "");
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
      <AppHeader>
        <AppBrand href={model.queueHref} homeLabel="Stovepipe queue" mark={<StovepipeLogo />}>Stovepipe</AppBrand>
        <Block display="flex" alignItems="center" flexWrap gridGap="scale600">
          <RefreshControls {...(model.kind === "issues" && model.showLatestLink ? { href: model.latestHref } : {})} />
        </Block>
      </AppHeader>
      <Block as="main" paddingTop="scale400" paddingBottom={model.kind === "request" ? "scale200" : "scale600"}>
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
            {model.kind !== "request" ? <>
              <HeadingLarge marginTop="0" marginBottom="scale300" as="h1">{model.queue}</HeadingLarge>
              <ParagraphSmall marginTop="0">{model.kind === "issues" ? "Open issues grouped by task." : "Requests ordered by acceptance time, newest first."}</ParagraphSmall>
            </> : <HeadingMedium marginTop="0" marginBottom="0" as="h1">Request {model.requestId}</HeadingMedium>}
          </div>
        </Block>
        {model.kind !== "request" && <ViewNavigation aria-label="Queue views">
          <ViewLink $active={model.kind === "queue"} href={model.queueHref} aria-current={model.kind === "queue" ? "page" : undefined}>Requests</ViewLink>
          <ViewLink $active={model.kind === "issues"} href={`${model.queueHref}?view=issues`} aria-current={model.kind === "issues" ? "page" : undefined}>Open issues</ViewLink>
        </ViewNavigation>}
        {model.kind !== "request" && model.showLatestLink && (
          <ParagraphSmall>
            <Link href={model.latestHref}>{model.kind === "issues" ? "First page" : "Latest page"}</Link>
          </ParagraphSmall>
        )}
        {model.kind === "queue" ? (
          <Section result={model.requests}>
            {(data) => data && <QueueRequests initial={data} routeKey={model.key} />}
          </Section>
        ) : model.kind === "issues" ? (
          <Block marginTop="scale600">
            <Section cacheKey={model.latestHref} result={model.issues}>
              {data => data && <OpenIssues initial={data} />}
            </Section>
          </Block>
        ) : (
          <Section result={model.history}>
            {(history) => (
              <Section cacheKey={views.projectsHref} result={model.projectStatus}>
                {(projects) => (
                  <Block className="sq-stovepipe-request-content" display="flex" flexDirection="column" gridGap="scale400">
                    <section aria-label="Request status">
                      <RequestDetails request={history?.request ?? null} repositoryResult={projects && <RecordedResult degree={projects.repositoryBreakageDegree} />} />
                    </section>
                    <ViewNavigation aria-label="Request views">
                      {views.tabs.map(tab => (
                        <ViewLink key={tab.id} $active={tab.selected} href={tab.href} scroll={false} aria-current={tab.selected ? "page" : undefined}>{tab.label}</ViewLink>
                      ))}
                    </ViewNavigation>
                    {views.tabs.map(tab => (
                      <section key={tab.id} className="sq-stovepipe-request-view" aria-label={tab.label} hidden={!tab.selected}>
                        {(tab.id !== "projects" || !projects) && <HeadingSmall as="h2" marginTop="0" marginBottom="scale400">{tab.label}</HeadingSmall>}
                        {tab.id === "projects" ? projects && <ProjectResults model={projects} pageKey={views.projectsHref} issues={model.projectIssues} />
                          : tab.id === "issues" ? model.requestIssues && <Section cacheKey={model.key} result={model.requestIssues}>
                            {issues => issues && <RequestIssues model={issues} projects={model.issueProjects} />}
                          </Section>
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
      <Block as="footer" paddingTop={model.kind === "request" ? "0" : "scale300"} paddingBottom={model.kind === "request" ? "scale200" : "scale300"} font="ParagraphXSmall" color="contentSecondary">
        Loaded <Timestamp value={model.loadedAtMs} />
      </Block>
    </Block>
  );
}
export function StovepipeApp({ model }: { model: StovepipePageModel }) {
  const pageKey = model.kind === "issues" ? model.latestHref : model.kind === "request" ? model.key.split("?")[0] : model.key;
  return <TimeZoneProvider><Page key={pageKey} model={model} /></TimeZoneProvider>;
}
