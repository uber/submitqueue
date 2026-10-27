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
      <Table label="Recorded project results" headers={["Project", "Project type", "Recorded result"]}>
        {model.projects.map((entry) => (
          <StyledTableBodyRow key={entry.project}>
            <StyledTableBodyCell>{entry.project}</StyledTableBodyCell>
            <StyledTableBodyCell>{entry.projectType || "Unknown"}</StyledTableBodyCell>
            <StyledTableBodyCell><RecordedResult degree={entry.breakageDegree} /></StyledTableBodyCell>
          </StyledTableBodyRow>
        ))}
      </Table>
      {!model.projects.length && <ParagraphSmall>No project results recorded on this page.</ParagraphSmall>}
      <ParagraphSmall color="contentSecondary">{model.complete
        ? "Project result recording is complete."
        : "Project result recording is still in progress; more results may arrive."}</ParagraphSmall>
      {(model.firstHref || model.nextHref) && (
        <Block as="nav" aria-label="Project pages" display="flex" flexWrap gridGap="scale500">
          {model.firstHref && <Link href={model.firstHref}>First projects</Link>}
          {model.nextHref && <Link href={model.nextHref}>Next projects →</Link>}
        </Block>
      )}
      <ParagraphSmall color="contentSecondary">Results updated: {model.updatedAtMs === "0"
        ? "Unknown" : <Timestamp value={model.updatedAtMs} />}</ParagraphSmall>
    </>
  );
}
