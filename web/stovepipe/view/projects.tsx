"use client";

import { Timestamp } from "@submitqueue/web-platform";
import { Block } from "baseui/block";
import { StyledTableBodyCell, StyledTableBodyRow } from "baseui/table-semantic";
import { Tag } from "baseui/tag";
import { ParagraphSmall } from "baseui/typography";
import type { ProjectStatusModel } from "../entity/index.js";
import { Link, Table } from "./components.js";

export function RecordedResult({ degree }: { degree: number | null }) {
  return (
    <Tag closeable={false} size="small" kind={
      degree === null ? "neutral" : degree === 0 ? "positive" : "negative"
    }>
      {degree === null ? "No result recorded" : degree === 0 ? "Green" : degree === 1 ? "Breakage" : "Partial Breakage"}
    </Tag>
  );
}

export function ProjectResults({ model }: { model: ProjectStatusModel }) {
  return (
    <>
      <ProjectPagination model={model} position="top" />
      <Table label="Recorded project results" headers={["Project", "Recorded result"]}>
        {model.projects.map((entry) => (
          <StyledTableBodyRow key={entry.project}>
            <StyledTableBodyCell>{entry.project}</StyledTableBodyCell>
            <StyledTableBodyCell><RecordedResult degree={entry.breakageDegree} /></StyledTableBodyCell>
          </StyledTableBodyRow>
        ))}
      </Table>
      {!model.projects.length && <ParagraphSmall>No project results recorded on this page.</ParagraphSmall>}
      <ParagraphSmall color="contentSecondary">{model.complete
        ? "Project result recording is complete."
        : "Project result recording is still in progress; more results may arrive."}</ParagraphSmall>
      <ProjectPagination model={model} position="bottom" />
      <ParagraphSmall color="contentSecondary">Results updated: {model.updatedAtMs === "0"
        ? "Unknown" : <Timestamp value={model.updatedAtMs} />}</ParagraphSmall>
    </>
  );
}

function ProjectPagination({ model, position }: { model: ProjectStatusModel; position: "top" | "bottom" }) {
  return <Block as="nav" aria-label={`Project pages (${position})`} display="flex" alignItems="center" justifyContent="space-between" flexWrap gridGap="scale400" marginBottom="scale300">
    <ParagraphSmall margin="0" color="contentSecondary">{model.projects.length} projects on this page{model.nextHref ? " · More results available" : " · Last page"}</ParagraphSmall>
    <Block display="flex" flexWrap gridGap="scale500">
      {model.firstHref && <Link href={model.firstHref}>First projects</Link>}
      {model.nextHref && <Link href={model.nextHref}>Next projects →</Link>}
    </Block>
  </Block>;
}
