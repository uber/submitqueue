"use client";

import { createContext, useContext, useEffect, useState, type ReactNode } from "react";
import { appendUniqueItems, Timestamp, usePageLoader } from "@submitqueue/web-platform";
import { Block } from "baseui/block";
import { Button, SIZE } from "baseui/button";
import { StyledTableBodyCell, StyledTableBodyRow } from "baseui/table-semantic";
import { Tag } from "baseui/tag";
import { HeadingSmall, ParagraphSmall } from "baseui/typography";
import type { OpenIssueModel, OpenIssuesModel, RequestIssuesModel } from "../entity/index.js";
import { Link, Table } from "./components.js";

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
        <Table label={`Issues for ${task}`} headers={["Unit", "First seen", "Opening request"]}>
          {issues.map(issue => <StyledTableBodyRow key={issueKey(issue)}>
            <StyledTableBodyCell>{issue.unitId
              ? <Block as="code" font="MonoParagraphXSmall">{issue.unitId}</Block>
              : <Tag closeable={false} kind="neutral" size="small">Task-wide failure</Tag>}</StyledTableBodyCell>
            <StyledTableBodyCell>{issue.firstSeenAtMs === "0" ? "Unknown" : <Timestamp value={issue.firstSeenAtMs} />}</StyledTableBodyCell>
            <StyledTableBodyCell><Link href={issue.requestHref}>{issue.requestId}</Link></StyledTableBodyCell>
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

export function RequestIssues({ model }: { model: RequestIssuesModel }) {
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
    {groups.map(group => {
      const issues = model.issues.filter(issue => group.kind === "unknown"
        ? !["new", "existing", "closed"].includes(issue.kind) : issue.kind === group.kind);
      return <Block as="section" key={group.kind} aria-label={group.label} marginBottom="scale500">
        <HeadingSmall as="h3" marginTop="0" marginBottom="scale300">{group.label}</HeadingSmall>
        {issues.length ? <Table label={group.label} headers={["Task", "Unit", "First seen build", "Observed build"]}>
          {issues.map(issue => <StyledTableBodyRow key={JSON.stringify([issue.taskId, issue.unitId])}>
            <StyledTableBodyCell>{issue.taskId}</StyledTableBodyCell>
            <StyledTableBodyCell>{issue.unitId ? <Block as="code" font="MonoParagraphXSmall">{issue.unitId}</Block>
              : <Tag closeable={false} size="small" kind="neutral">Task-wide failure</Tag>}</StyledTableBodyCell>
            <StyledTableBodyCell>{issue.firstSeenBuildId || "Unknown"}</StyledTableBodyCell>
            <StyledTableBodyCell>{issue.buildId || "Unknown"}</StyledTableBodyCell>
          </StyledTableBodyRow>)}
        </Table> : <ParagraphSmall color="contentSecondary">No {group.label.toLowerCase()} on this page.</ParagraphSmall>}
      </Block>;
    })}
    <Block as="nav" aria-label="Request issue pages" display="flex" flexWrap gridGap="scale500" paddingBottom="scale400">
      {model.firstHref && <Link href={model.firstHref} scroll={false}>First issues</Link>}
      {model.nextHref && <Link href={model.nextHref} scroll={false}>Next issues →</Link>}
    </Block>
  </Block>;
}
