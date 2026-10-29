"use client";

import { createContext, useContext, useEffect, useState, type ReactNode } from "react";
import { appendUniqueItems, Timestamp, usePageLoader } from "@submitqueue/web-platform";
import { Block } from "baseui/block";
import { Button, SIZE } from "baseui/button";
import { StyledTableBodyCell, StyledTableBodyRow } from "baseui/table-semantic";
import { Tag } from "baseui/tag";
import { HeadingSmall, ParagraphSmall } from "baseui/typography";
import type { OpenIssueModel, OpenIssuesModel } from "../entity/index.js";
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
