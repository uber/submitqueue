"use client";

import { ErrorState } from "./error-state.js";
import { useLastSuccessfulData } from "./last-successful-data.js";
import { Fact, Table, Timestamp } from "@submitqueue/web-platform";
import { styled } from "baseui";
import { Block } from "baseui/block";
import { Button, SIZE } from "baseui/button";
import { Input } from "baseui/input";
import { KIND, Notification } from "baseui/notification";
import { Select, StyledInput } from "baseui/select";
import { Tag } from "baseui/tag";
import { StyledTableBodyCell, StyledTableBodyRow } from "baseui/table-semantic";
import { HeadingLarge, HeadingSmall, LabelXSmall, ParagraphSmall, ParagraphXSmall } from "baseui/typography";
import { useWebApp, WebLink } from "./web-app.js";
import { forwardRef, useState, type ComponentPropsWithoutRef, type ReactNode } from "react";
import type {
  ChangeDetailModel, HistoryEventModel, LoadResult, QueueModel, RequestDetailModel, RequestListModel, RequestSummaryModel,
} from "../entity/index.js";
import { WebPaths } from "../core/paths.js";
import { statusDisplay } from "../core/status.js";

// Closing removes the options; suppress the old ID until the menu reports a new one.
const SelectInput = forwardRef<HTMLInputElement, ComponentPropsWithoutRef<typeof StyledInput>>((props, ref) => {
  const activeDescendant = props["aria-activedescendant"];
  const [closedActiveDescendant, setClosedActiveDescendant] = useState(activeDescendant);
  if (!props.$isOpen && closedActiveDescendant !== activeDescendant) setClosedActiveDescendant(activeDescendant);
  return <StyledInput {...props} ref={ref}
    aria-activedescendant={props.$isOpen && activeDescendant !== closedActiveDescendant ? activeDescendant : undefined} />;
});

const selectOverrides = {
  Input: { component: SelectInput },
  // The enclosing option owns selection semantics; its label is a generic div.
  OptionContent: { props: { "aria-selected": undefined, "aria-readonly": undefined } },
};

const MetadataRow = styled("div", ({ $theme }) => ({
  display: "grid",
  gridTemplateColumns: "minmax(0, 1fr)",
  gap: $theme.sizing.scale200,
  paddingTop: $theme.sizing.scale300,
  paddingBottom: $theme.sizing.scale300,
  [$theme.mediaQuery.medium]: {
    gridTemplateColumns: "minmax(100px, .35fr) minmax(0, 1fr)",
    gap: $theme.sizing.scale600,
  },
}));

const RequestViewLink = styled<typeof WebLink, { $active: boolean }>(WebLink, ({ $theme, $active }) => ({
  display: "inline-block",
  paddingTop: $theme.sizing.scale400,
  paddingBottom: $theme.sizing.scale400,
  textDecoration: "none",
  borderBottom: `2px solid ${$active ? $theme.colors.contentPrimary : "transparent"}`,
}));

function Metadata({ values }: { values: Record<string, string> }) {
  return <Block as="dl" marginTop="scale500" marginBottom="0">{Object.entries(values).sort(([a], [b]) => a.localeCompare(b)).map(([key, value]) =>
    <MetadataRow key={key}><LabelXSmall as="dt" color="contentSecondary">{key}</LabelXSmall><ParagraphSmall as="dd" margin="0">{value}</ParagraphSmall></MetadataRow>
  )}</Block>;
}

export function RequestStatus({ status }: { status: string }) {
  const display = statusDisplay(status);
  const kind = ({ neutral: "neutral", success: "positive", danger: "negative", warning: "warning", progress: "accent" } as const)[display.tone];
  return <span className="sq-status" data-status={status || "unknown"} data-tone={display.tone}><Tag closeable={false} kind={kind} size="small">{display.label}</Tag></span>;
}

function RequestChanges({ request, links = {}, labels = {} }: {
  request: RequestSummaryModel; links?: Record<string, string>; labels?: Record<string, string>;
}) {
  return <Block as="ul" paddingLeft="scale600" margin="0" aria-label="Changes">{request.changeUris.map(uri =>
    <Block as="li" key={uri} font="MonoParagraphXSmall">{links[uri] ? <WebLink href={links[uri]}>{labels[uri] ?? uri}</WebLink> : <Block as="code" font="MonoParagraphXSmall">{labels[uri] ?? uri}</Block>}</Block>
  )}</Block>;
}

export function QueueDirectory({ queues, basePath }: { queues: readonly QueueModel[]; basePath?: string }) {
  const paths = new WebPaths(basePath === undefined ? {} : { basePath });
  return <section className="sq-directory">
    <HeadingLarge as="h1" marginTop="0" marginBottom="scale300">Queues</HeadingLarge><ParagraphXSmall color="contentSecondary">Available submit queues</ParagraphXSmall>
    {queues.length === 0 ? <ParagraphSmall>No queues are configured.</ParagraphSmall> :
      <ul>{queues.map(queue => <li key={queue.name}>
        <HeadingSmall as="h2"><WebLink href={paths.requests(queue.name)}>{queue.name}</WebLink></HeadingSmall>{queue.description ? <ParagraphSmall color="contentSecondary">{queue.description}</ParagraphSmall> : null}
      </li>)}</ul>}
  </section>;
}

export interface RequestListProps {
  model: RequestListModel;
  basePath?: string;
  empty?: ReactNode;
  changeLinks?: Record<string, string>;
  changeLabels?: Record<string, string>;
}

export function RequestList({ model, basePath, empty, changeLinks, changeLabels }: RequestListProps) {
  const [search, setSearch] = useState("");
  const paths = new WebPaths(basePath === undefined ? {} : { basePath });
  const requests = model.requests.filter(request =>
    [request.sqid, request.status, ...request.changeUris.map(uri => changeLabels?.[uri] ?? uri)]
      .some(value => value.toLowerCase().includes(search.toLowerCase()))
  );
  return <section className="sq-request-list">
    <Block display="flex" justifyContent="space-between" alignItems="center" flexWrap gridGap="scale600" marginBottom="scale600">
      <Block as="label" width="320px" maxWidth="100%" font="LabelXSmall" color="contentSecondary">Find in displayed requests<Input type="search" size="compact" value={search} onChange={event => setSearch(event.currentTarget.value)} /></Block>
      <LabelXSmall as="span" color="contentSecondary">Displayed page</LabelXSmall>
    </Block>
    {model.requests.length === 0 ? <Block paddingTop="scale600" paddingBottom="scale600" color="contentSecondary">{empty ?? "No requests were received in this window."}</Block> :
      requests.length === 0 ? <ParagraphSmall color="contentSecondary">No displayed requests match.</ParagraphSmall> :
        <Table label={`Requests in ${model.queue}`} headers={["Request ID", "Status", "Changes", "Received"]}>
          {requests.map(request => <StyledTableBodyRow className="sq-request-list__item" key={request.sqid}>
            <StyledTableBodyCell><WebLink href={paths.request(model.queue, request.sqid)}>{request.sqid}</WebLink></StyledTableBodyCell>
            <StyledTableBodyCell><RequestStatus status={request.status} />{request.lastError ? <ParagraphXSmall color="contentNegative">{request.lastError}</ParagraphXSmall> : null}</StyledTableBodyCell>
            <StyledTableBodyCell><RequestChanges request={request} {...(changeLinks ? { links: changeLinks } : {})} {...(changeLabels ? { labels: changeLabels } : {})} /></StyledTableBodyCell>
            <StyledTableBodyCell><Timestamp value={request.receivedAtMs} /></StyledTableBodyCell>
          </StyledTableBodyRow>)}
        </Table>}
    <Block display="flex" justifyContent="space-between" alignItems="center" flexWrap gridGap="scale600" marginTop="scale600" font="ParagraphXSmall" color="contentSecondary"><span aria-live="polite">{requests.length} displayed requests · newest received first</span>
      {model.nextPageToken ? <nav aria-label="Request pages"><WebLink href={paths.requests(model.queue, { pageToken: model.nextPageToken })}>Next page</WebLink></nav> : null}
    </Block>
  </section>;
}

function HistoryValue({ item }: { item: HistoryEventModel }) {
  if (item.type === "status" && item.status !== null) {
    return <RequestStatus status={item.status} />;
  }
  return <Tag closeable={false} kind="accent" size="small">{item.type === "event" && item.event !== null ? statusDisplay(item.event).label : "Unknown event"}</Tag>;
}

export function RequestHistory({ events }: { events: HistoryEventModel[] }) {
  const [filter, setFilter] = useState("all");
  const [menuMountNode, setMenuMountNode] = useState<HTMLElement | null>(null);
  const filters = [{ id: "all", label: "All events" }, { id: "status", label: "Lifecycle" }, { id: "event", label: "Occurrence events" }];
  const shown = events.filter(event => filter === "all" || event.type === filter);
  return <section ref={setMenuMountNode}>
    <Block display="flex" justifyContent="space-between" alignItems="center" flexWrap gridGap="scale600" marginBottom="scale600"><HeadingSmall as="h2" marginTop="0">Request history</HeadingSmall><Block width="320px" maxWidth="100%" font="LabelXSmall" color="contentSecondary">Show<Select aria-label="Show events" size="compact" clearable={false} overrides={selectOverrides} {...(menuMountNode ? { mountNode: menuMountNode } : {})}
      options={filters} value={filters.filter(option => option.id === filter)}
      onChange={({ value }) => setFilter(String(value[0]?.id ?? "all"))} /></Block></Block>
    {events.length === 0 ? <ParagraphSmall color="contentSecondary">No history has been retained for this request.</ParagraphSmall> :
      <Block as="ol" padding="0" overrides={{ Block: { style: { listStyleType: "none" } } }} role="list" aria-label="Request history">{shown.map((item, index) =>
        <Block as="li" className="sq-history__item" key={`${item.timestampMs}-${index}`} display="grid"
          gridTemplateColumns={["minmax(0, 1fr)", "minmax(0, 1fr)", "auto minmax(0, 1fr) auto"]}
          gridGap="scale600" paddingTop="scale600" paddingBottom="scale600"
          overrides={{ Block: { style: ({ $theme }) => ({ borderBottom: `1px solid ${$theme.colors.borderOpaque}` }) } }}>
          <Timestamp value={item.timestampMs} />
          <details><summary><HistoryValue item={item} /></summary>
            {Object.keys(item.metadata).length ? <Metadata values={item.metadata} /> : <ParagraphXSmall color="contentSecondary">No additional metadata.</ParagraphXSmall>}
          </details>
          <LabelXSmall as="span" color="contentSecondary">{item.type === "status" ? "Lifecycle" : item.type === "event" ? "Occurrence" : "Unknown"}</LabelXSmall>
          {item.lastError ? <Block as={ParagraphXSmall} gridColumn={["1", "1", "2"]} color="contentNegative" role="alert">{item.lastError}</Block> : null}
        </Block>
      )}</Block>}
    <ParagraphXSmall color="contentSecondary" aria-live="polite">{shown.length} displayed events · oldest first</ParagraphXSmall>
  </section>;
}

/** Shows the result's data, keeping the last successful data visible below the error while a failure is retryable. */
export function LastSuccessfulData<T>({ result, fallback = null, children }: {
  result: LoadResult<T>;
  /** Rendered when there is no data to show. */
  fallback?: ReactNode;
  children: (data: T) => ReactNode;
}) {
  const data = useLastSuccessfulData(result);
  return <>
    {!result.ok ? <><ErrorState compact={data !== undefined} error={result.error} />{data !== undefined ? <ParagraphXSmall color="contentSecondary">Showing the last successful snapshot; updates are unavailable.</ParagraphXSmall> : null}</> : null}
    {data !== undefined ? children(data) : fallback}
  </>;
}

export function RequestListView({ result, ...props }: Omit<RequestListProps, "model"> & {
  result: LoadResult<RequestListModel>;
}) {
  return <LastSuccessfulData result={result}>{data => <RequestList model={data} {...props} />}</LastSuccessfulData>;
}

function CopyControls({ id }: { id: string }) {
  const [message, setMessage] = useState("");
  const copyValue = async (value: string, label: string) => {
    try {
      await navigator.clipboard.writeText(value);
      setMessage(`${label} copied`);
    } catch {
      setMessage("Copy unavailable. Select the ID or copy the browser address.");
    }
  };
  return <Block display="flex" alignItems="center" flexWrap gridGap="scale500">
    <Button size={SIZE.compact} onClick={() => void copyValue(id, "ID")}>Copy ID</Button>
    <Button size={SIZE.compact} onClick={() => void copyValue(window.location.href, "Link")}>Copy link</Button>
    <Block as="span" role="status" width="100%" display={message ? "block" : "none"} font="ParagraphXSmall" color="contentSecondary">{message}</Block>
  </Block>;
}

export interface RequestDetailProps {
  model: RequestDetailModel;
  view?: "summary" | "history";
  basePath?: string;
  backHref?: string;
  summaryHref?: string;
  historyHref?: string;
  changeLinks?: Record<string, string>;
  changeLabels?: Record<string, string>;
  controls?: ReactNode;
}

export function RequestDetail({ model, view = "summary", basePath, backHref, summaryHref, historyHref, changeLinks, changeLabels, controls }: RequestDetailProps) {
  const { request } = model;
  const paths = new WebPaths(basePath === undefined ? {} : { basePath });
  const latest = model.history.at(-1);
  return <article className="sq-request-detail">
    <Block as="nav" display="flex" alignItems="center" flexWrap gridGap="scale500" marginBottom="scale600" aria-label="Breadcrumb"><WebLink href={paths.directory()}>Queues</WebLink><span>›</span><WebLink href={backHref ?? paths.requests(request.queue)}>{request.queue}</WebLink><span>› Request</span></Block>
    <Block as="header" display="flex" justifyContent="space-between" alignItems="flex-start" flexWrap gridGap="scale600" marginBottom="scale600"><div><HeadingLarge as="h1" marginTop="0" marginBottom="scale300">{request.sqid}</HeadingLarge><RequestStatus status={request.status} /></div><Block display="flex" alignItems="flex-start" flexWrap gridGap="scale500"><CopyControls id={request.sqid} />{controls}</Block></Block>
    {request.lastError ? <Notification kind={KIND.negative} overrides={{ Body: { props: { role: "alert" }, style: { width: "auto" } } }}>{request.lastError}</Notification> : null}
    <Block as="nav" display="flex" alignItems="center" flexWrap gridGap="scale700" marginBottom="scale600" aria-label="Request views"
      overrides={{ Block: { style: ({ $theme }) => ({ borderBottom: `1px solid ${$theme.colors.borderOpaque}` }) } }}>
      <RequestViewLink $active={view === "summary"} aria-current={view === "summary" ? "page" : undefined} href={summaryHref ?? paths.request(request.queue, request.sqid)}>
        <Block as="span" font={view === "summary" ? "LabelMedium" : "ParagraphMedium"}>Summary</Block>
      </RequestViewLink>
      <RequestViewLink $active={view === "history"} aria-current={view === "history" ? "page" : undefined} href={historyHref ?? paths.request(request.queue, request.sqid, { view: "history" })}>
        <Block as="span" font={view === "history" ? "LabelMedium" : "ParagraphMedium"}>History{model.historyError ? "" : ` (${model.history.length})`}</Block>
      </RequestViewLink>
    </Block>
    {view === "history" ? model.historyError ? <ErrorState error={model.historyError} /> : <RequestHistory events={model.history} /> :
      <Block display="grid" gridTemplateColumns={["minmax(0, 1fr)", "minmax(0, 1fr)", "repeat(2, minmax(0, 1fr))"]} gridGap="scale700"><Block as="dl" margin="0" aria-label="Request details">
        <Fact label="Queue">{request.queue}</Fact><Fact label="Request ID">{request.sqid}</Fact>
        <Fact label="Received"><Timestamp value={request.receivedAtMs} /></Fact>
        <Fact label="Last recorded event">{model.historyError ? "History unavailable" : latest ? <><HistoryValue item={latest} /> <Timestamp value={latest.timestampMs} /></> : "No retained events"}</Fact>
        <Fact label="Last error">{request.lastError ?? "None reported"}</Fact>
      </Block><div><Block as="section" padding="scale600" backgroundColor="backgroundSecondary" marginBottom="scale600"><HeadingSmall as="h2" marginTop="0">Changes</HeadingSmall><RequestChanges request={request} {...(changeLinks ? { links: changeLinks } : {})} {...(changeLabels ? { labels: changeLabels } : {})} /></Block>
        <Block as="details" padding="scale600" backgroundColor="backgroundSecondary"><summary>Request metadata</summary>{Object.keys(request.metadata).length ? <Metadata values={request.metadata} /> : <ParagraphXSmall color="contentSecondary">No additional metadata.</ParagraphXSmall>}</Block>
        {model.historyError ? <ErrorState compact error={model.historyError} /> : null}
      </div></Block>}
    {view === "summary" && model.historyError === null ?
      <Block as="ol" display="flex" flexWrap gridGap="scale400" padding="0" marginTop="scale700" font="ParagraphXSmall" color="contentSecondary"
        overrides={{ Block: { style: { listStyleType: "none" } } }} role="list" aria-label="Observed lifecycle transitions">
        {model.history.filter(event => event.type === "status" && event.status !== null).map((event, index) =>
          <Block as="li" display="flex" alignItems="center" gridGap="scale400" key={`${event.timestampMs}-${index}`}>
            {index > 0 ? <span aria-hidden="true">›</span> : null}{statusDisplay(event.status!).label}
          </Block>
        )}
      </Block> : null}
  </article>;
}

export function RequestDetailView({ result, ...props }: Omit<RequestDetailProps, "model"> & {
  result: LoadResult<RequestDetailModel>;
}) {
  return <LastSuccessfulData result={result} fallback={props.controls}>{data => <RequestDetail model={data} {...props} />}</LastSuccessfulData>;
}

export interface ChangeSubmissionsProps {
  model: ChangeDetailModel;
  basePath?: string;
}

export function ChangeSubmissions({ model, basePath }: ChangeSubmissionsProps) {
  const app = useWebApp();
  const [menuMountNode, setMenuMountNode] = useState<HTMLElement | null>(null);
  const paths = new WebPaths(basePath === undefined ? {} : { basePath });
  const versions = [...new Map(model.submissions.map(item => [item.versionHref, item.version])).entries()];
  const selectedVersion = model.pinnedVersion === null ? model.logicalHref :
    model.submissions.find(item => item.version === model.pinnedVersion)?.versionHref;
  const versionOptions = [{ id: model.logicalHref ?? "", label: "All submitted versions" }, ...versions.map(([href, version]) => ({ id: href, label: version }))];
  return <section className="sq-change-detail" ref={setMenuMountNode}>
    <Block as="nav" display="flex" alignItems="center" flexWrap gridGap="scale500" marginBottom="scale600" aria-label="Breadcrumb"><WebLink href={paths.directory()}>Queues</WebLink><span>›</span><WebLink href={paths.requests(model.queue)}>{model.queue}</WebLink><span>› Change</span></Block>
    <HeadingLarge as="h1" marginTop="0" marginBottom="scale300">{model.review}</HeadingLarge><ParagraphXSmall color="contentSecondary">{model.pinnedVersion ? `Submitted version: ${model.pinnedVersion}` : "Submissions across versions"}</ParagraphXSmall>
    {model.pagination ? <ParagraphXSmall color="contentSecondary" role="status">
      Showing matches from this page of queue requests only.
      {model.pagination.nextHref ? " More queue requests remain to be searched." : " The history scan is complete for this receipt window."}
    </ParagraphXSmall> : null}
    <Block as="dl" display="flex" flexWrap gridGap="scale600" padding="scale600" backgroundColor="backgroundSecondary" marginTop="scale600" marginBottom="scale600"><div><LabelXSmall as="dt" color="contentSecondary">Provider</LabelXSmall><ParagraphSmall as="dd" marginLeft="0">{model.provider}</ParagraphSmall></div><div><LabelXSmall as="dt" color="contentSecondary">Host</LabelXSmall><ParagraphSmall as="dd" marginLeft="0">{model.host}</ParagraphSmall></div><div><LabelXSmall as="dt" color="contentSecondary">Repository</LabelXSmall><ParagraphSmall as="dd" marginLeft="0">{model.repository || "—"}</ParagraphSmall></div><div><LabelXSmall as="dt" color="contentSecondary">Queue</LabelXSmall><ParagraphSmall as="dd" marginLeft="0">{model.queue}</ParagraphSmall></div></Block>
    <Block display="flex" justifyContent="space-between" alignItems="center" flexWrap gridGap="scale600" marginBottom="scale600"><HeadingSmall as="h2" marginTop="0">Submission history</HeadingSmall>
      {model.logicalHref && app && versions.length > 0 ?
        <Block width="320px" maxWidth="100%" font="LabelXSmall" color="contentSecondary">Submitted version<Select aria-label="Filter submissions by version" size="compact" clearable={false} overrides={selectOverrides} {...(menuMountNode ? { mountNode: menuMountNode } : {})}
          options={versionOptions} value={versionOptions.filter(option => option.id === selectedVersion)}
          onChange={({ value }) => { if (value[0]?.id) void app.navigate(String(value[0].id)); }} /></Block> :
        model.pinnedVersion !== null && model.logicalHref ? <WebLink href={model.logicalHref}>All submitted versions</WebLink> : null}
    </Block>
    {model.window ? <ParagraphXSmall color="contentSecondary">Receipt window: <Timestamp value={model.window.fromMs} /> — <Timestamp value={model.window.toMs} />. This is not unlimited retained history.</ParagraphXSmall> : <ParagraphXSmall color="contentSecondary">Retained submissions for this exact version</ParagraphXSmall>}
    {model.submissions.length === 0 ? <ParagraphSmall color="contentSecondary">{model.pagination ? "No submissions were found on this page." : "No submissions were found in this scope."}</ParagraphSmall> :
      <Table label="Change submissions" headers={["Request", "Submitted version", "Received", "Status"]}>
        {model.submissions.map(item => <StyledTableBodyRow key={item.request.sqid}><StyledTableBodyCell><WebLink href={paths.request(model.queue, item.request.sqid)}>{item.request.sqid}</WebLink></StyledTableBodyCell>
          <StyledTableBodyCell><WebLink aria-label={item.version} title={item.version} href={item.versionHref}>{item.version.length > 20 ? `${item.version.slice(0, 8)}…${item.version.slice(-4)}` : item.version}</WebLink></StyledTableBodyCell><StyledTableBodyCell><Timestamp value={item.request.receivedAtMs} /></StyledTableBodyCell>
          <StyledTableBodyCell><RequestStatus status={item.request.status} />{item.request.lastError ? <ParagraphXSmall color="contentNegative">{item.request.lastError}</ParagraphXSmall> : null}</StyledTableBodyCell></StyledTableBodyRow>)}</Table>}
    <ParagraphXSmall color="contentSecondary">{model.submissions.length} requests{model.pagination ? " on this page" : ""} · newest received first · repeated submissions remain separate</ParagraphXSmall>
    {model.pagination ? <Block as="nav" display="flex" alignItems="center" flexWrap gridGap="scale500" marginTop="scale500" aria-label="Change history pagination">
      {model.pagination.nextHref ? <WebLink href={model.pagination.nextHref}>Continue history scan</WebLink> : null}
      {model.pagination.latestHref ? <WebLink href={model.pagination.latestHref}>Latest history</WebLink> : null}
    </Block> : null}
  </section>;
}
