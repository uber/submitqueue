"use client";

import { ErrorState } from "./error-state.js";
import { useLastSuccessfulData } from "./last-successful-data.js";
import { Fact, PagedRequests, selectOverrides, Table, Timestamp, useWebNavigation } from "@submitqueue/web-platform";
import { styled, withStyle } from "baseui";
import { Block } from "baseui/block";
import { Button, KIND as BUTTON_KIND, SIZE } from "baseui/button";
import { KIND, Notification } from "baseui/notification";
import { Select } from "baseui/select";
import { Tag } from "baseui/tag";
import { StyledTableBodyCell, StyledTableBodyRow } from "baseui/table-semantic";
import { HeadingMedium, HeadingXSmall, LabelXSmall, ParagraphSmall, ParagraphXSmall } from "baseui/typography";
import { AccentLink, useWebApp, WebLink } from "./web-app.js";
import { useState, type ReactNode } from "react";
import type {
  ChangeDetailModel, HistoryEventModel, LoadResult, QueueModel, QueueRequestsModel, RequestDetailModel, RequestSummaryModel,
} from "../entity/index.js";
import { requestChangeLabels, requestChangeLinks } from "../core/change.js";
import { WebPaths } from "../core/paths.js";
import { knownRequestStatuses, statusDisplay } from "../core/status.js";

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
  ":hover": { textDecoration: "none" },
}));

const ChangeLink = styled(WebLink, ({ $theme }) => ({ ...$theme.typography.MonoParagraphXSmall }));

/** Secondary text for timestamps and column labels, smaller than the values beside it. */
function Secondary({ children }: { children: ReactNode }) {
  return <Block as="span" font="ParagraphXSmall" color="contentSecondary">{children}</Block>;
}

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
  return <Block as="ul" padding="0" margin="0" aria-label="Changes" overrides={{ Block: { style: { listStyleType: "none" } } }}>{request.changeUris.map(uri =>
    <Block as="li" key={uri} font="MonoParagraphXSmall">{links[uri] ? <ChangeLink href={links[uri]}>{labels[uri] ?? uri}</ChangeLink> : <Block as="code" font="MonoParagraphXSmall">{labels[uri] ?? uri}</Block>}</Block>
  )}</Block>;
}

const QueueGrid = styled("ul", ({ $theme }) => ({
  display: "grid",
  gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))",
  gap: $theme.sizing.scale600,
  listStyleType: "none",
  padding: 0,
  margin: 0,
}));

const QueueCard = styled("li", ({ $theme }) => ({
  position: "relative",
  display: "flex",
  flexDirection: "column",
  gap: $theme.sizing.scale300,
  padding: $theme.sizing.scale700,
  border: `1px solid ${$theme.colors.borderOpaque}`,
  borderRadius: "8px",
  transitionProperty: "border-color, box-shadow",
  transitionDuration: $theme.animation.timing200,
  ":hover": { borderColor: $theme.colors.borderSelected, boxShadow: $theme.lighting.shadow400 },
  ":focus-within": { borderColor: $theme.colors.borderSelected, boxShadow: $theme.lighting.shadow400 },
}));

// Stretched over its card so the whole card is clickable while the queue name stays the one link and tab stop.
const QueueCardLink = withStyle(AccentLink, ({ $theme }) => ({
  color: $theme.colors.contentPrimary,
  ":visited": { color: $theme.colors.contentPrimary },
  ":hover": { color: $theme.colors.contentPrimary, textDecoration: "none" },
  "::after": { content: '""', position: "absolute", inset: 0, borderRadius: "8px" },
}));

export function QueueDirectory({ queues, basePath }: { queues: readonly QueueModel[]; basePath?: string }) {
  const { Link } = useWebNavigation();
  const paths = new WebPaths(basePath === undefined ? {} : { basePath });
  return <section className="sq-directory">
    <Block display="flex" justifyContent="space-between" alignItems="flex-end" flexWrap gridGap="scale600" marginBottom="scale800">
      <div>
        <HeadingMedium as="h1" marginTop="0" marginBottom="scale300">Queues</HeadingMedium>
        <ParagraphSmall margin="0" color="contentSecondary">Choose a queue to see its requests, newest first.</ParagraphSmall>
      </div>
      {queues.length > 0 ? <LabelXSmall as="span" color="contentSecondary">{queues.length === 1 ? "1 queue" : `${queues.length} queues`}</LabelXSmall> : null}
    </Block>
    {queues.length === 0 ? <ParagraphSmall color="contentSecondary">No queues are configured.</ParagraphSmall> :
      <QueueGrid>{queues.map(queue => <QueueCard key={queue.name}>
        <HeadingXSmall as="h2" margin="0"><QueueCardLink $as={Link ?? "a"} href={paths.requests(queue.name)}>{queue.name}</QueueCardLink></HeadingXSmall>
        {queue.description ? <ParagraphSmall margin="0" color="contentSecondary">{queue.description}</ParagraphSmall> : null}
        <Block as="span" aria-hidden="true" marginTop="auto" paddingTop="scale300" font="LabelXSmall" color="accent500">View requests →</Block>
      </QueueCard>)}</QueueGrid>}
  </section>;
}

/** Trail from the queue directory to a queue, and on to `page` within it when given. */
export function QueueBreadcrumb({ queue, page, queueHref, basePath }: {
  queue: string; page?: string; queueHref?: string; basePath?: string;
}) {
  const paths = new WebPaths(basePath === undefined ? {} : { basePath });
  return <Block as="nav" display="flex" alignItems="center" flexWrap gridGap="scale500" marginBottom="scale600" aria-label="Breadcrumb">
    <WebLink href={paths.directory()}>Queues</WebLink><span>›</span>
    {page ? <><WebLink href={queueHref ?? paths.requests(queue)}>{queue}</WebLink><span>› {page}</span></> : <span aria-current="page">{queue}</span>}
  </Block>;
}

export interface RequestListProps {
  queue: string;
  requests: RequestSummaryModel[];
  basePath?: string;
  /** Whether no older requests remain to load; an empty list says so only once complete. */
  complete?: boolean;
  /** Whether the list is restricted to some statuses. */
  filtered?: boolean;
}

export function RequestList({ queue, requests, basePath, complete = true, filtered = false }: RequestListProps) {
  const paths = new WebPaths(basePath === undefined ? {} : { basePath });
  const changeLinks = requestChangeLinks(queue, requests, paths);
  const changeLabels = requestChangeLabels(requests);
  if (requests.length === 0) {
    return complete ? <ParagraphSmall color="contentSecondary">
      {filtered ? "No requests on this queue have the selected statuses." : "No requests have been received on this queue."}
    </ParagraphSmall> : null;
  }
  return <Table label={`Requests in ${queue}`} headers={["Request ID", "Status", "Changes", "Received", "Updated"].map(header => <Secondary key={header}>{header}</Secondary>)}>
    {requests.map(request => <StyledTableBodyRow className="sq-request-list__item" key={request.sqid}>
      <StyledTableBodyCell><WebLink href={paths.request(queue, request.sqid)}>{request.sqid}</WebLink></StyledTableBodyCell>
      <StyledTableBodyCell><RequestStatus status={request.status} />{request.lastError ? <ParagraphXSmall color="contentNegative">{request.lastError}</ParagraphXSmall> : null}</StyledTableBodyCell>
      <StyledTableBodyCell><RequestChanges request={request} links={changeLinks} labels={changeLabels} /></StyledTableBodyCell>
      <StyledTableBodyCell><Secondary><Timestamp value={request.receivedAtMs} /></Secondary></StyledTableBodyCell>
      <StyledTableBodyCell><Secondary>{request.updatedAtMs === null ? "—" : <Timestamp value={request.updatedAtMs} />}</Secondary></StyledTableBodyCell>
    </StyledTableBodyRow>)}
  </Table>;
}

const requestSqid = (request: RequestSummaryModel) => request.sqid;

/** A queue's requests from `result`, loading older pages through the enclosing `RequestPagesProvider`. */
export function QueueRequests({ queue, result, routeKey, basePath, filtered = false }: {
  queue: string;
  result: LoadResult<QueueRequestsModel>;
  /** Identity of the list for retained rows and scroll position. */
  routeKey: string;
  basePath?: string;
  filtered?: boolean;
}) {
  return <LastSuccessfulData result={result}>{data =>
    <PagedRequests initial={data} routeKey={routeKey} requestKey={requestSqid}>
      {(requests, complete) => <RequestList queue={queue} requests={requests} complete={complete} filtered={filtered}
        {...(basePath === undefined ? {} : { basePath })} />}
    </PagedRequests>}
  </LastSuccessfulData>;
}

const filterableStatuses = knownRequestStatuses.filter(status => status !== "accepting");

/**
 * Restricts a queue list to selected statuses by navigating to its filtered URL; renders nothing outside a `WebApp`.
 * The menu renders in BaseUI's layer rather than inline: the request table's sticky header would otherwise cover it.
 */
export function QueueStatusFilter({ queue, statuses, basePath }: { queue: string; statuses: readonly string[]; basePath?: string }) {
  const app = useWebApp();
  if (!app) {
    return null;
  }
  const paths = new WebPaths(basePath === undefined ? {} : { basePath });
  const options = [...new Set([...filterableStatuses, ...statuses])].map(status => ({ id: status, label: statusDisplay(status).label }));
  return <Block width="360px" maxWidth="100%" font="LabelXSmall" color="contentSecondary">Statuses
    <Select aria-label="Filter requests by status" size="compact" multi placeholder="All statuses" overrides={selectOverrides}
      options={options} value={options.filter(option => statuses.includes(option.id))}
      onChange={({ value }) => void app.navigate(paths.requests(queue, { statuses: value.map(option => String(option.id)).sort() }))} />
  </Block>;
}

function HistoryValue({ item }: { item: HistoryEventModel }) {
  if (item.type === "status" && item.status !== null) {
    return <RequestStatus status={item.status} />;
  }
  return <Tag closeable={false} kind="accent" size="small">{item.type === "event" && item.event !== null ? statusDisplay(item.event).label : "Unknown event"}</Tag>;
}

/** Compact duration such as `45s`, `3m 05s`, `2h 10m`, or `4d 3h`; negative spans read as zero. */
function formatElapsed(ms: number): string {
  const seconds = Math.max(0, Math.floor(ms / 1_000));
  const [days, hours, minutes] = [Math.floor(seconds / 86_400), Math.floor(seconds / 3_600) % 24, Math.floor(seconds / 60) % 60];
  if (days > 0) return `${days}d ${hours}h`;
  if (hours > 0) return `${hours}h ${minutes}m`;
  if (minutes > 0) return `${minutes}m ${String(seconds % 60).padStart(2, "0")}s`;
  return `${seconds}s`;
}

export function RequestHistory({ events }: { events: HistoryEventModel[] }) {
  const [filter, setFilter] = useState("all");
  const [menuMountNode, setMenuMountNode] = useState<HTMLElement | null>(null);
  const filters = [{ id: "all", label: "All events" }, { id: "status", label: "Lifecycle" }, { id: "event", label: "Occurrence events" }];
  const shown = events.filter(event => filter === "all" || event.type === filter);
  return <section ref={setMenuMountNode}>
    <Block display="flex" justifyContent="space-between" alignItems="center" flexWrap gridGap="scale600" marginBottom="scale600"><HeadingXSmall as="h2" marginTop="0">Request history</HeadingXSmall><Block width="320px" maxWidth="100%" font="LabelXSmall" color="contentSecondary">Show<Select aria-label="Show events" size="compact" clearable={false} overrides={selectOverrides} {...(menuMountNode ? { mountNode: menuMountNode } : {})}
      options={filters} value={filters.filter(option => option.id === filter)}
      onChange={({ value }) => setFilter(String(value[0]?.id ?? "all"))} /></Block></Block>
    {events.length === 0 ? <ParagraphSmall color="contentSecondary">No history has been retained for this request.</ParagraphSmall> :
      <Block as="ol" padding="0" overrides={{ Block: { style: { listStyleType: "none" } } }} role="list" aria-label="Request history">{shown.map((item, index) =>
        <Block as="li" className="sq-history__item" key={`${item.timestampMs}-${index}`} display="grid"
          gridTemplateColumns={["minmax(0, 1fr)", "minmax(0, 1fr)", "220px minmax(0, 1fr)"]}
          gridGap="scale300" paddingTop="scale500" paddingBottom="scale500"
          overrides={{ Block: { style: ({ $theme }) => ({ borderBottom: `1px solid ${$theme.colors.borderOpaque}` }) } }}>
          <div>
            <Secondary><Timestamp value={item.timestampMs} /></Secondary>
            {index > 0 ? <ParagraphXSmall margin="0" color="contentSecondary">+{formatElapsed(item.timestampMs - shown[index - 1]!.timestampMs)}</ParagraphXSmall> : null}
          </div>
          <div>
            <details><summary><HistoryValue item={item} /> <LabelXSmall as="span" color="contentSecondary">{item.type === "status" ? "Lifecycle" : item.type === "event" ? "Occurrence" : "Unknown"}</LabelXSmall></summary>
              {Object.keys(item.metadata).length ? <Metadata values={item.metadata} /> : <ParagraphXSmall color="contentSecondary">No additional metadata.</ParagraphXSmall>}
            </details>
            {item.lastError ? <ParagraphXSmall marginBottom="0" color="contentNegative" role="alert">{item.lastError}</ParagraphXSmall> : null}
          </div>
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
    <Button kind={BUTTON_KIND.secondary} size={SIZE.compact} onClick={() => void copyValue(id, "ID")}>Copy ID</Button>
    <Button kind={BUTTON_KIND.secondary} size={SIZE.compact} onClick={() => void copyValue(window.location.href, "Link")}>Copy link</Button>
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
  return <Block as="article" className="sq-request-detail">
    <QueueBreadcrumb queue={request.queue} page="Request" {...(basePath === undefined ? {} : { basePath })} {...(backHref ? { queueHref: backHref } : {})} />
    <Block as="header" display="flex" justifyContent="space-between" alignItems="flex-start" flexWrap gridGap="scale600" marginBottom="scale600"><div><HeadingMedium as="h1" marginTop="0" marginBottom="scale300">{request.sqid}</HeadingMedium><RequestStatus status={request.status} /></div><Block display="flex" alignItems="flex-start" flexWrap gridGap="scale500"><CopyControls id={request.sqid} />{controls}</Block></Block>
    {request.lastError ? <Notification kind={KIND.negative} overrides={{ Body: { props: { role: "alert" }, style: { width: "auto" } } }}>{request.lastError}</Notification> : null}
    <Block as="nav" display="flex" alignItems="center" flexWrap gridGap="scale700" marginBottom="scale600" aria-label="Request views"
      overrides={{ Block: { style: ({ $theme }) => ({ borderBottom: `1px solid ${$theme.colors.borderOpaque}` }) } }}>
      <RequestViewLink $active={view === "summary"} aria-current={view === "summary" ? "page" : undefined} href={summaryHref ?? paths.request(request.queue, request.sqid)}>
        <Block as="span" font={view === "summary" ? "LabelMedium" : "ParagraphMedium"} color={view === "summary" ? "contentPrimary" : "contentSecondary"}>Summary</Block>
      </RequestViewLink>
      <RequestViewLink $active={view === "history"} aria-current={view === "history" ? "page" : undefined} href={historyHref ?? paths.request(request.queue, request.sqid, { view: "history" })}>
        <Block as="span" font={view === "history" ? "LabelMedium" : "ParagraphMedium"} color={view === "history" ? "contentPrimary" : "contentSecondary"}>History{model.historyError ? "" : ` (${model.history.length})`}</Block>
      </RequestViewLink>
    </Block>
    {view === "history" ? model.historyError ? <ErrorState error={model.historyError} /> : <RequestHistory events={model.history} /> :
      <Block display="grid" gridTemplateColumns={["minmax(0, 1fr)", "minmax(0, 1fr)", "repeat(2, minmax(0, 1fr))"]} gridGap="scale700"><Block as="dl" margin="0" aria-label="Request details">
        <Fact label="Queue">{request.queue}</Fact><Fact label="Request ID">{request.sqid}</Fact>
        <Fact label="Received"><Timestamp value={request.receivedAtMs} /></Fact>
        <Fact label="Updated">{request.updatedAtMs === null ? "Not reported" : <Timestamp value={request.updatedAtMs} />}</Fact>
        <Fact label="Last recorded event">{model.historyError ? "History unavailable" : latest ? <><HistoryValue item={latest} /> <Timestamp value={latest.timestampMs} /></> : "No retained events"}</Fact>
        <Fact label="Last error">{request.lastError ?? "None reported"}</Fact>
      </Block><div><Block as="section" padding="scale600" backgroundColor="backgroundSecondary" marginBottom="scale600"><HeadingXSmall as="h2" marginTop="0">Changes</HeadingXSmall><RequestChanges request={request} {...(changeLinks ? { links: changeLinks } : {})} {...(changeLabels ? { labels: changeLabels } : {})} /></Block>
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
  </Block>;
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
    <QueueBreadcrumb queue={model.queue} page="Change" {...(basePath === undefined ? {} : { basePath })} />
    <HeadingMedium as="h1" marginTop="0" marginBottom="scale300">{model.review}</HeadingMedium><ParagraphXSmall color="contentSecondary">{model.pinnedVersion ? `Submitted version: ${model.pinnedVersion}` : "Submissions across versions"}</ParagraphXSmall>
    {model.pagination ? <ParagraphXSmall color="contentSecondary" role="status">
      Showing matches from this page of queue requests only.
      {model.pagination.nextHref ? " More queue requests remain to be searched." : " The history scan is complete for this receipt window."}
    </ParagraphXSmall> : null}
    <Block as="dl" display="flex" flexWrap gridGap="scale600" padding="scale600" backgroundColor="backgroundSecondary" marginTop="scale600" marginBottom="scale600"><div><LabelXSmall as="dt" color="contentSecondary">Provider</LabelXSmall><ParagraphSmall as="dd" marginLeft="0">{model.provider}</ParagraphSmall></div><div><LabelXSmall as="dt" color="contentSecondary">Host</LabelXSmall><ParagraphSmall as="dd" marginLeft="0">{model.host}</ParagraphSmall></div><div><LabelXSmall as="dt" color="contentSecondary">Repository</LabelXSmall><ParagraphSmall as="dd" marginLeft="0">{model.repository || "—"}</ParagraphSmall></div><div><LabelXSmall as="dt" color="contentSecondary">Queue</LabelXSmall><ParagraphSmall as="dd" marginLeft="0">{model.queue}</ParagraphSmall></div></Block>
    <Block display="flex" justifyContent="space-between" alignItems="center" flexWrap gridGap="scale600" marginBottom="scale600"><HeadingXSmall as="h2" marginTop="0">Submission history</HeadingXSmall>
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
