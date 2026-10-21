"use client";

import { Timestamp } from "@submitqueue/web-platform";
import { Block } from "baseui/block/index.js";
import { StyledTableBodyCell, StyledTableBodyRow } from "baseui/table-semantic/index.js";
import { Tag } from "baseui/tag/index.js";
import { ParagraphSmall } from "baseui/typography/index.js";
import type { ProjectStatusModel } from "../entity/index.js";
import { Link, Table } from "./components.js";

function Result({ degree }: { degree: number | null }) {
  return (
    <Tag closeable={false} size="small" kind={
      degree === null ? "neutral" : degree === 0 ? "positive" : "negative"
    }>
      {degree === null ? "No result recorded" : (
        <>{degree === 0 ? "Green" : "Breakage"} ({
          (degree * 100).toLocaleString("en-US", { maximumSignificantDigits: 6 })
        }%)</>
      )}
    </Tag>
  );
}

export function ProjectResults({ model }: { model: ProjectStatusModel }) {
  return (
    <>
      <ParagraphSmall color="contentSecondary">
        Breakage ranges from 0% (green) to 100%; missing results are not green.
      </ParagraphSmall>
      <Table label="Recorded project results" headers={["Scope", "Recorded result"]}>
        <StyledTableBodyRow>
          <StyledTableBodyCell $as="th" scope="row">Repository</StyledTableBodyCell>
          <StyledTableBodyCell><Result degree={model.repositoryBreakageDegree} /></StyledTableBodyCell>
        </StyledTableBodyRow>
        {model.projects.map((entry) => (
          <StyledTableBodyRow key={entry.project}>
            <StyledTableBodyCell>{entry.project}</StyledTableBodyCell>
            <StyledTableBodyCell><Result degree={entry.breakageDegree} /></StyledTableBodyCell>
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
