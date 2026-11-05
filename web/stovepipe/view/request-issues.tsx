"use client";

import { Block } from "baseui/block";
import { StyledTableBodyCell, StyledTableBodyRow } from "baseui/table-semantic";
import { Tag } from "baseui/tag";
import { ParagraphSmall } from "baseui/typography";
import type { RequestIssuesModel } from "../entity/index.js";
import { Link, Table } from "./components.js";

function analysisLabel(status: string): string {
  if (status === "completed") return "Analysis completed";
  if (status === "abandoned") return "Analysis abandoned; observations may be incomplete";
  return status || "No analysis completion recorded";
}

export function RequestIssues({ model, pageKey }: { model: RequestIssuesModel; pageKey: string }) {
  function renderIssuePagination() {
    return <Block display="flex" alignItems="center" justifyContent="space-between" flexWrap gridGap="scale400">
      <ParagraphSmall color="contentSecondary">{model.issues.length} observations on this page</ParagraphSmall>
      <Block display="flex" gridGap="scale500">
        {model.firstHref && <Link href={model.firstHref} scroll={false}>First issues</Link>}
        {model.nextHref && <Link href={model.nextHref} scroll={false}>Next issues →</Link>}
      </Block>
    </Block>;
  }
  return <>
    <ParagraphSmall color="contentSecondary">Observed failures across this request’s build attempts. These observations remain after an issue recovers.</ParagraphSmall>
    {model.builds.map(build => <ParagraphSmall key={build.buildId} marginTop="0">
      Build <code>{build.buildId}</code>{build.buildId === model.winningBuildId && " (request outcome)"}: {analysisLabel(build.analysisStatus)}
    </ParagraphSmall>)}
    {!model.builds.length && <ParagraphSmall>No builds recorded for this request.</ParagraphSmall>}
    {renderIssuePagination()}
    <Table key={pageKey} label="Observed build and test issues" headers={["Build", "Task", "Unit", "Repair ID"]} fill>
      {model.issues.map(issue => <StyledTableBodyRow key={JSON.stringify([issue.buildId, issue.taskId, issue.unitId])}>
        <StyledTableBodyCell><code>{issue.buildId}</code></StyledTableBodyCell>
        <StyledTableBodyCell>{issue.taskId}</StyledTableBodyCell>
        <StyledTableBodyCell>{issue.unitId ? <code>{issue.unitId}</code> : <Tag closeable={false} kind="neutral" size="small">Task-wide failure</Tag>}</StyledTableBodyCell>
        <StyledTableBodyCell>{issue.fixId || "None recorded"}</StyledTableBodyCell>
      </StyledTableBodyRow>)}
    </Table>
    {!model.issues.length && <ParagraphSmall>No issue observations recorded on this page. This does not establish build success or complete issue coverage.</ParagraphSmall>}
    {renderIssuePagination()}
  </>;
}
