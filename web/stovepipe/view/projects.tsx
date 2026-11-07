"use client";

import { selectOverrides, Timestamp, useWebNavigation } from "@submitqueue/web-platform";
import { Block } from "baseui/block";
import { Select } from "baseui/select";
import { StyledTableBodyCell, StyledTableBodyRow } from "baseui/table-semantic";
import { Tag } from "baseui/tag";
import { StatefulTooltip } from "baseui/tooltip";
import { HeadingSmall, ParagraphSmall } from "baseui/typography";
import { useTransition } from "react";
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

function projectRecordingStatus(model: ProjectStatusModel): {
  label: string; explanation: string; kind: "blue" | "neutral";
} {
  if (model.complete) return {
    label: "Recording finished", kind: "neutral",
    explanation: "Project result recording has finished. See the recorded results for the outcome.",
  };
  switch (model.requestState) {
    case "accepted": return {
      label: "Waiting to start", kind: "blue",
      explanation: "This request is waiting to start validation.",
    };
    case "processing": return {
      label: "Waiting for builds", kind: "blue",
      explanation: "Waiting for builds to finish before recording project results.",
    };
    case "succeeded":
    case "failed": return {
      label: "Results pending", kind: "blue",
      explanation: "Validation has ended. Project results are pending; recording may be queued or in progress.",
    };
    case "cancelled": return {
      label: "Cancelled", kind: "neutral",
      explanation: "Validation was cancelled. No project results are expected.",
    };
    case "superseded": return {
      label: "Superseded", kind: "neutral",
      explanation: "A newer request replaced this request. No project results are expected.",
    };
    default: return {
      label: "Status unknown", kind: "neutral",
      explanation: "Project recording status is unavailable for this request state.",
    };
  }
}

export function ProjectResults({ model, pageKey }: { model: ProjectStatusModel; pageKey: string }) {
  const navigation = useWebNavigation();
  const [pending, startTransition] = useTransition();
  const recordingStatus = projectRecordingStatus(model);
  const repositoryOnlyBreakage = model.complete && model.repositoryBreakageDegree === 1
    && !model.projects.length && !model.firstHref && !model.nextHref;
  const options = model.pageSizeChoices.map(choice => ({
    id: choice.value, label: String(choice.value), href: choice.href,
  }));
  function renderPagination() {
    const id = "project-page-size";
    return <Block as="nav" aria-label="Project pages (bottom)" display="flex" alignItems="center" justifyContent="space-between" flexWrap gridGap="scale400" marginTop="scale300">
      <ParagraphSmall margin="0" color="contentSecondary">{model.projects.length} projects on this page{model.nextHref ? " · More results available" : " · Last page"}</ParagraphSmall>
      <Block display="flex" alignItems="center" flexWrap gridGap="scale500">
        <Block display="flex" alignItems="center" gridGap="scale300">
          <label htmlFor={id}>Projects per page</label>
          <Block width="90px">
            <Select id={id} aria-label="Projects per page" size="compact" clearable={false}
              options={options} value={options.filter(option => option.id === model.pageSize)}
              overrides={selectOverrides} isLoading={pending} disabled={pending}
              onChange={({ value }) => {
                const option = value[0];
                if (!option || option.id === model.pageSize || typeof option.href !== "string") return;
                const href = option.href;
                if (navigation.push) startTransition(() => navigation.push?.(href, { scroll: false }));
                else window.location.assign(href);
              }} />
          </Block>
        </Block>
        {model.firstHref && <Link href={model.firstHref} scroll={false}>First projects</Link>}
        {model.nextHref && <Link href={model.nextHref} scroll={false}>Next projects →</Link>}
      </Block>
    </Block>;
  }
  return (
    <>
      <Block display="flex" alignItems="center" flexWrap gridGap="scale400" marginBottom="scale400">
        <HeadingSmall as="h2" margin="0">Project Results</HeadingSmall>
        <StatefulTooltip content={recordingStatus.explanation} accessibilityType="tooltip">
          <span tabIndex={0} aria-label="Project result recording">
            <Tag closeable={false} size="small" noMargin contentMaxWidth={null} kind={recordingStatus.kind}>
              {recordingStatus.label}
            </Tag>
          </span>
        </StatefulTooltip>
        <ParagraphSmall margin="0" color="contentSecondary">Results updated: {model.updatedAtMs === "0"
          ? "Unknown" : <Timestamp value={model.updatedAtMs} />}</ParagraphSmall>
      </Block>
      {repositoryOnlyBreakage ? (
        <Block role="region" aria-label="Recorded project results" display="flex" flexDirection="column" alignItems="center" justifyContent="center"
          flex="1 1 0px" minHeight="12rem" padding="scale600" $style={{ textAlign: "center" }}>
          <ParagraphSmall marginTop="0" marginBottom="scale300">Repository-wide breakage recorded</ParagraphSmall>
          <ParagraphSmall margin="0" color="contentSecondary">This request is fully broken at the repository level. No individual project results were recorded.</ParagraphSmall>
        </Block>
      ) : <Table key={pageKey} label="Recorded project results" headers={["Project", "Project type", "Recorded result"]} fill>
        {model.projects.map((entry) => (
          <StyledTableBodyRow key={entry.project}>
            <StyledTableBodyCell>{entry.project}</StyledTableBodyCell>
            <StyledTableBodyCell>{entry.projectType || "Unknown"}</StyledTableBodyCell>
            <StyledTableBodyCell><RecordedResult degree={entry.breakageDegree} /></StyledTableBodyCell>
          </StyledTableBodyRow>
        ))}
      </Table>}
      {!model.projects.length && !repositoryOnlyBreakage && <ParagraphSmall>No project results recorded on this page.</ParagraphSmall>}
      {renderPagination()}
    </>
  );
}
