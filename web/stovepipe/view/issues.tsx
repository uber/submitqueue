"use client";

import { Fragment, createContext, useContext, useEffect, useState, type ReactNode } from "react";
import { appendUniqueItems, Timestamp, usePageLoader } from "@submitqueue/web-platform";
import { Block } from "baseui/block";
import { Button, SIZE } from "baseui/button";
import { StyledTableBodyCell, StyledTableBodyRow } from "baseui/table-semantic";
import { Tag } from "baseui/tag";
import { HeadingSmall, ParagraphSmall } from "baseui/typography";
import type { OpenIssueModel, OpenIssuesModel, RequestIssuesModel, IssueProjectsSelection, IssueHistoryModel } from "../entity/index.js";
import { Link, Table, Section } from "./components.js";

export type OpenIssuePageLoader = (href: string, signal: AbortSignal) => Promise<OpenIssuesModel>;

const OpenIssuePagesContext = createContext<OpenIssuePageLoader | null>(null);

export function OpenIssuePagesProvider({ loadPage, children }: { loadPage: OpenIssuePageLoader; children: ReactNode }) {
  return <OpenIssuePagesContext.Provider value={loadPage}>{children}</OpenIssuePagesContext.Provider>;
}

const issueKey = (issue: OpenIssueModel) => JSON.stringify([issue.taskId, issue.unitId]);

export function OpenIssues({ initial }: { initial: OpenIssuesModel }) {
  const loadPage = useContext(OpenIssuePagesContext);
  const [snapshot, setSnapshot] = useState({ initial, latest: initial, ...initial, loadedHrefs: [] as string[] });
  const [hydrated, setHydrated] = useState(false);

  if (snapshot.latest !== initial) {
    setSnapshot(snapshot.loadedHrefs.length
      ? { ...snapshot, latest: initial }
      : { initial, latest: initial, ...initial, loadedHrefs: [] });
  }

  const { loading, error, loadNextPage, cancelPageLoad } = usePageLoader({
    loadPage,
    nextHref: snapshot.nextHref,
    loadedHrefs: snapshot.loadedHrefs,
    resetKey: snapshot.initial,
    onPage: (page, href) => setSnapshot(current => current.nextHref !== href ? current : {
      ...current,
      issues: appendUniqueItems(current.issues, page.issues, issueKey),
      nextHref: page.nextHref,
      loadedHrefs: [...current.loadedHrefs, href],
    }),
  });

  useEffect(() => {
    setHydrated(true);
  }, []);

  const tasks = new Map<string, OpenIssueModel[]>();
  for (const issue of snapshot.issues) {
    const group = tasks.get(issue.taskId) ?? [];
    group.push(issue);
    tasks.set(issue.taskId, group);
  }
  return (
    <section aria-label="Open issues" className="sq-stovepipe-open-issues">
      {snapshot.initial !== snapshot.latest && <Block marginBottom="scale400">
        <Button size={SIZE.compact} onClick={() => {
          cancelPageLoad();
          setSnapshot({ initial, latest: initial, ...initial, loadedHrefs: [] });
        }}>Show latest issues</Button>
      </Block>}
      {!snapshot.issues.length && <ParagraphSmall color="contentSecondary">
        {snapshot.nextHref ? "No matching issues in this batch. More results are available." : "No tracked open issues."}
      </ParagraphSmall>}
      {[...tasks].map(([task, issues]) => <Block as="section" key={task} aria-label={`Task ${task}`} marginBottom="scale600">
        <HeadingSmall as="h2" marginTop="0" marginBottom="scale300">{task}</HeadingSmall>
        <Table label={`Issues for ${task}`} headers={["Unit", "First seen", "Opening request", "History"]}>
          {issues.map(issue => <StyledTableBodyRow key={issueKey(issue)}>
            <StyledTableBodyCell>{issue.unitId
              ? <Block as="code" font="MonoParagraphXSmall">{issue.unitId}</Block>
              : <Tag closeable={false} kind="neutral" size="small">Task-wide failure</Tag>}</StyledTableBodyCell>
            <StyledTableBodyCell>{issue.firstSeenAtMs === "0" ? "Unknown" : <Timestamp value={issue.firstSeenAtMs} />}</StyledTableBodyCell>
            <StyledTableBodyCell><Link href={issue.requestHref}>{issue.requestId}</Link></StyledTableBodyCell>
            <StyledTableBodyCell><Link href={issue.historyHref}>View history</Link></StyledTableBodyCell>
          </StyledTableBodyRow>)}
        </Table>
      </Block>)}
      {error && <ParagraphSmall role="alert" color="contentNegative">Could not load more issues. Displayed issues are unchanged.</ParagraphSmall>}
      <Block display="flex" alignItems="center" justifyContent="space-between" flexWrap gridGap="scale400" paddingTop="scale400">
        <ParagraphSmall role="status" margin="0" color="contentSecondary">
          {loading ? "Loading more issues…" : `${snapshot.issues.length} ${snapshot.issues.length === 1 ? "issue" : "issues"} loaded`}
        </ParagraphSmall>
        {snapshot.nextHref && (loadPage && hydrated
          ? <Button size={SIZE.compact} isLoading={loading} disabled={loading} onClick={() => void loadNextPage()}>{error ? "Retry loading issues" : "Load more"}</Button>
          : <Link href={snapshot.nextHref}>Load more</Link>)}
      </Block>
    </section>
  );
}

export function RequestIssues({ model, projects }: { model: RequestIssuesModel; projects?: IssueProjectsSelection | null }) {
  const status = model.status === "complete" ? "Issue recording complete"
    : model.status === "in_progress" ? "Issue recording in progress. Results may change; return to the first page after recording completes."
      : model.status === "incomplete" ? "Issue recording was abandoned. These results may be incomplete."
        : model.status === "unavailable" ? "Issue results are not available. Recording may not have started, or this request predates retained coverage."
          : "Issue recording status is unknown.";
  const groups = [
    { kind: "new", label: "New Issues" },
    { kind: "existing", label: "Existing Issues" },
    { kind: "closed", label: "Resolved Issues" },
  ];
  if (model.issues.some(issue => !groups.some(group => group.kind === issue.kind)))
    groups.push({ kind: "unknown", label: "Other Issues" });
  return <Block className="sq-stovepipe-request-issues">
    <ParagraphSmall role="status" marginTop="0">{status}</ParagraphSmall>
    <ParagraphSmall color="contentSecondary">Project attribution is available for reported unit-level issues, not task-wide or resolved updates.</ParagraphSmall>
    {groups.map(group => {
      const issues = model.issues.filter(issue => group.kind === "unknown"
        ? !["new", "existing", "closed"].includes(issue.kind) : issue.kind === group.kind);
      return <Block as="section" key={group.kind} aria-label={group.label} marginBottom="scale500">
        <HeadingSmall as="h3" marginTop="0" marginBottom="scale300">{group.label}</HeadingSmall>
        {issues.length ? <Table label={group.label} headers={["Task", "Unit", "First seen build", "Observed build", "Affected projects", "History"]}>
          {issues.map(issue => {
            const selected = projects?.taskId === issue.taskId && projects.unitId === issue.unitId ? projects : null;
            return <Fragment key={JSON.stringify([issue.taskId, issue.unitId])}>
              <StyledTableBodyRow>
                <StyledTableBodyCell>{issue.taskId}</StyledTableBodyCell>
                <StyledTableBodyCell>{issue.unitId ? <Block as="code" font="MonoParagraphXSmall">{issue.unitId}</Block>
                  : <Tag closeable={false} size="small" kind="neutral">Task-wide failure</Tag>}</StyledTableBodyCell>
                <StyledTableBodyCell>{issue.firstSeenBuildId || "Unknown"}</StyledTableBodyCell>
                <StyledTableBodyCell>{issue.buildId || "Unknown"}</StyledTableBodyCell>
                <StyledTableBodyCell>{issue.projectsHref ? <Link href={selected ? selected.closeHref : issue.projectsHref} scroll={false} aria-expanded={Boolean(selected)}>{selected ? "Hide projects" : "Show projects"}</Link> : "Not available"}</StyledTableBodyCell>
                <StyledTableBodyCell><Link href={issue.historyHref}>View history</Link></StyledTableBodyCell>
              </StyledTableBodyRow>
              {selected && <StyledTableBodyRow><StyledTableBodyCell colSpan={6}>
                <Section cacheKey={selected.key} result={selected.result}>{page => page && <section aria-label={`Affected projects for ${issue.taskId} ${issue.unitId}`}>
                  {!page.complete ? <ParagraphSmall>Project attribution is not available yet or did not complete.</ParagraphSmall>
                    : page.projects.length ? <ul>{page.projects.map(project => <li key={project}>{project}</li>)}</ul>
                      : <ParagraphSmall>No affected projects recorded for this issue.</ParagraphSmall>}
                  <Block as="nav" aria-label="Affected project pages" display="flex" flexWrap gridGap="scale400">
                    {page.firstHref && <Link href={page.firstHref} scroll={false}>First affected projects</Link>}
                    {page.nextHref && <Link href={page.nextHref} scroll={false}>More affected projects →</Link>}
                  </Block>
                </section>}</Section>
              </StyledTableBodyCell></StyledTableBodyRow>}
            </Fragment>;
          })}
        </Table> : <ParagraphSmall color="contentSecondary">No {group.label.toLowerCase()} on this page.</ParagraphSmall>}
      </Block>;
    })}
    <Block as="nav" aria-label="Request issue pages" display="flex" flexWrap gridGap="scale500" paddingBottom="scale400">
      {model.firstHref && <Link href={model.firstHref} scroll={false}>First issues</Link>}
      {model.nextHref && <Link href={model.nextHref} scroll={false}>Next issues →</Link>}
    </Block>
  </Block>;
}

export function IssueHistory({ model }: { model: IssueHistoryModel }) {
  return <section aria-label="Issue history" className="sq-stovepipe-issue-history">
    <HeadingSmall as="h2" marginTop="0">Issue history</HeadingSmall>
    <Block as="dl" margin="0" display="grid" gridTemplateColumns="auto minmax(0, 1fr)" gridGap="scale300">
      <dt>Task</dt><Block as="dd" margin="0" $style={{ overflowWrap: "anywhere" }}>{model.taskId}</Block>
      <dt>Unit</dt><Block as="dd" margin="0" $style={{ overflowWrap: "anywhere" }}>{model.unitId
        ? <code>{model.unitId}</code> : <Tag closeable={false} kind="neutral" size="small">Task-wide failure</Tag>}</Block>
    </Block>
    <ParagraphSmall color="contentSecondary">Retained events across requests, oldest recorded timestamp first. Timestamp ties use build ID and event order, not request chronology. Open events include reopenings.</ParagraphSmall>
    <ParagraphSmall color="contentSecondary">Older history may be incomplete. Late events can appear on earlier pages; return to the first page to refresh coverage.</ParagraphSmall>
    {model.events.length ? <Table label="Issue history events" headers={["Time", "Event", "Build", "Opening build", "Request"]}>
      {model.events.map(event => <StyledTableBodyRow key={JSON.stringify([event.buildId, event.event])}>
        <StyledTableBodyCell><Timestamp value={event.timestampMs} /></StyledTableBodyCell>
        <StyledTableBodyCell>{event.event === "open" ? "Opened / reopened" : event.event === "reported" ? "Reported again" : event.event === "closed" ? "Closed" : event.event}</StyledTableBodyCell>
        <StyledTableBodyCell>{event.buildId}</StyledTableBodyCell>
        <StyledTableBodyCell>{event.firstSeenBuildId || "Unknown"}</StyledTableBodyCell>
        <StyledTableBodyCell>{event.requestHref
          ? <Link href={event.requestHref}>{event.requestId}</Link> : "Not retained"}</StyledTableBodyCell>
      </StyledTableBodyRow>)}
    </Table> : <ParagraphSmall>No retained issue events on this page. This does not establish that the issue never existed.</ParagraphSmall>}
    <Block as="nav" aria-label="Issue history pages" display="flex" flexWrap gridGap="scale500" paddingTop="scale400">
      {model.firstHref && <Link href={model.firstHref}>First events</Link>}
      {model.nextHref && <Link href={model.nextHref}>Next events →</Link>}
    </Block>
  </section>;
}
