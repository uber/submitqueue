"use client";

import { selectOverrides, Timestamp, useWebNavigation } from "@submitqueue/web-platform";
import { Block } from "baseui/block";
import { Select } from "baseui/select";
import { StyledTableBodyCell, StyledTableBodyRow } from "baseui/table-semantic";
import { Tag } from "baseui/tag";
import { ParagraphSmall } from "baseui/typography";
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

export function ProjectResults({ model, pageKey }: { model: ProjectStatusModel; pageKey: string }) {
  const navigation = useWebNavigation();
  const [pending, startTransition] = useTransition();
  const options = model.pageSizeChoices.map(choice => ({
    id: choice.value, label: String(choice.value), href: choice.href,
  }));
  function renderPagination(position: "top" | "bottom") {
    const id = `project-page-size-${position}`;
    return <Block as="nav" aria-label={`Project pages (${position})`} display="flex" alignItems="center" justifyContent="space-between" flexWrap gridGap="scale400" marginBottom="scale300">
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
      {renderPagination("top")}
      <Table key={pageKey} label="Recorded project results" headers={["Project", "Recorded result"]} fill>
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
      {renderPagination("bottom")}
      <ParagraphSmall color="contentSecondary">Results updated: {model.updatedAtMs === "0"
        ? "Unknown" : <Timestamp value={model.updatedAtMs} />}</ParagraphSmall>
    </>
  );
}
