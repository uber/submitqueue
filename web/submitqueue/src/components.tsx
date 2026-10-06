"use client";

import { useState, type ReactNode } from "react";
import type {
  ChangeDetailModel, HistoryEventModel, QueueModel, RequestDetailModel,
  RequestListModel, RequestSummaryModel, WebError, LoadResult,
} from "./models.js";
import { WebPaths } from "./paths.js";
import { statusDisplay } from "./status.js";

const timestampFormatter = new Intl.DateTimeFormat("en-US", {
  dateStyle: "medium", timeStyle: "long", timeZone: "UTC",
});

export function Timestamp({ value }: { value: number }) {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? <time>{value}</time> :
    <time dateTime={date.toISOString()}>{timestampFormatter.format(date)}</time>;
}

function Metadata({ values }: { values: Record<string, string> }) {
  return <dl className="sq-metadata">{Object.entries(values).sort(([a], [b]) => a.localeCompare(b)).map(([key, value]) =>
    <div className="sq-metadata__entry" key={key}><dt>{key}</dt><dd>{value}</dd></div>
  )}</dl>;
}

export function RequestStatus({ status }: { status: string }) {
  const display = statusDisplay(status);
  return <span className="sq-status" data-status={status || "unknown"} data-tone={display.tone}>{display.label}</span>;
}

function RequestChanges({ request, links = {}, labels = {} }: {
  request: RequestSummaryModel; links?: Record<string, string>; labels?: Record<string, string>;
}) {
  return <ul className="sq-changes" aria-label="Changes">{request.changeUris.map(uri =>
    <li key={uri}>{links[uri] ? <a href={links[uri]}>{labels[uri] ?? uri}</a> : <code>{labels[uri] ?? uri}</code>}</li>
  )}</ul>;
}

export function QueueDirectory({ queues, basePath }: { queues: readonly QueueModel[]; basePath?: string }) {
  const paths = new WebPaths(basePath === undefined ? {} : { basePath });
  return <section className="sq-directory">
    <h1>Queues</h1><p className="sq-secondary">Queues configured by this host</p>
    {queues.length === 0 ? <p>No queues are configured.</p> :
      <ul>{queues.map(queue => <li key={queue.name}>
        <h2><a href={paths.requests(queue.name)}>{queue.name}</a></h2><p>{queue.description}</p>
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
    <div className="sq-toolbar">
      <label>Find in displayed requests<input type="search" value={search} onChange={event => setSearch(event.target.value)} /></label>
      <span className="sq-secondary">Displayed page · UTC</span>
    </div>
    {model.requests.length === 0 ? <div className="sq-empty">{empty ?? "No requests were received in this window."}</div> :
      requests.length === 0 ? <p className="sq-empty">No displayed requests match.</p> :
        <div className="sq-table-scroll"><table aria-label={`Requests in ${model.queue}`}>
          <thead><tr><th>Request ID</th><th>Status</th><th>Changes</th><th>Received · UTC</th></tr></thead>
          <tbody>{requests.map(request => <tr className="sq-request-list__item" key={request.sqid}>
            <td><a href={paths.request(model.queue, request.sqid)}>{request.sqid}</a></td>
            <td><RequestStatus status={request.status} />{request.lastError ? <p className="sq-row-error">{request.lastError}</p> : null}</td>
            <td><RequestChanges request={request} {...(changeLinks ? { links: changeLinks } : {})} {...(changeLabels ? { labels: changeLabels } : {})} /></td>
            <td><Timestamp value={request.receivedAtMs} /></td>
          </tr>)}</tbody>
        </table></div>}
    <div className="sq-table-footer"><span aria-live="polite">{requests.length} displayed requests · newest received first</span>
      {model.nextPageToken ? <nav aria-label="Request pages"><a href={paths.requests(model.queue, { pageToken: model.nextPageToken })}>Next page</a></nav> : null}
    </div>
  </section>;
}

function HistoryValue({ item }: { item: HistoryEventModel }) {
  if (item.type === "status" && item.status !== null) {
    return <RequestStatus status={item.status} />;
  }
  return <span className="sq-event">{item.type === "event" && item.event !== null ? statusDisplay(item.event).label : "Unknown event"}</span>;
}

export function RequestHistory({ events }: { events: HistoryEventModel[] }) {
  const [filter, setFilter] = useState("all");
  const shown = events.filter(event => filter === "all" || event.type === filter);
  return <section>
    <div className="sq-toolbar"><h2>Request history</h2><label>Show<select value={filter} onChange={event => setFilter(event.target.value)}>
      <option value="all">All events</option><option value="status">Lifecycle</option><option value="event">Occurrence events</option>
    </select></label></div>
    {events.length === 0 ? <p className="sq-empty">No history has been retained for this request.</p> :
      <ol className="sq-history" aria-label="Request history">{shown.map((item, index) =>
        <li className="sq-history__item" key={`${item.timestampMs}-${index}`}>
          <Timestamp value={item.timestampMs} />
          <details><summary><HistoryValue item={item} /></summary>
            {Object.keys(item.metadata).length ? <Metadata values={item.metadata} /> : <p className="sq-secondary">No additional metadata.</p>}
          </details>
          <span className="sq-secondary">{item.type === "status" ? "Lifecycle" : item.type === "event" ? "Occurrence" : "Unknown"}</span>
          {item.lastError ? <p className="sq-row-error" role="alert">{item.lastError}</p> : null}
        </li>
      )}</ol>}
    <p className="sq-secondary" aria-live="polite">{shown.length} displayed events · oldest first · UTC</p>
  </section>;
}

export function ErrorState({ error, compact = false }: { error: WebError; compact?: boolean }) {
  return <section className="sq-error" data-error-kind={error.kind} role={compact ? "status" : "alert"}>
    <h2>{error.title}</h2><p>{error.message}</p>
  </section>;
}

function useLastSuccessfulData<T>(result: LoadResult<T>): T | undefined {
  const [snapshot, setSnapshot] = useState<{ result: LoadResult<T>; data: T | undefined }>(() => ({
    result, data: result.ok ? result.data : undefined,
  }));
  if (snapshot.result !== result) {
    setSnapshot({
      result,
      data: result.ok ? result.data : result.error.retryable ? snapshot.data : undefined,
    });
  }
  return result.ok ? result.data : result.error.retryable ? snapshot.data : undefined;
}

export function RequestListView({ result, ...props }: Omit<RequestListProps, "model"> & {
  result: LoadResult<RequestListModel>;
}) {
  const data = useLastSuccessfulData(result);
  return <>
    {!result.ok ? <><ErrorState compact={data !== undefined} error={result.error} />{data ? <p className="sq-secondary">Showing the last successful snapshot; updates are unavailable.</p> : null}</> : null}
    {data ? <RequestList model={data} {...props} /> : null}
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
  return <div className="sq-copy">
    <button type="button" onClick={() => void copyValue(id, "ID")}>Copy ID</button>
    <button type="button" onClick={() => void copyValue(window.location.href, "Link")}>Copy link</button>
    <span role="status">{message}</span>
  </div>;
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
    <nav className="sq-breadcrumb" aria-label="Breadcrumb"><a href={paths.directory()}>Queues</a><span>›</span><a href={backHref ?? paths.requests(request.queue)}>{request.queue}</a><span>› Request</span></nav>
    <header className="sq-detail-heading"><div><h1>{request.sqid}</h1><RequestStatus status={request.status} /></div><div className="sq-detail-actions"><CopyControls id={request.sqid} />{controls}</div></header>
    {request.lastError ? <p className="sq-failure-banner" role="alert">{request.lastError}</p> : null}
    <nav className="sq-tabs" aria-label="Request views">
      <a aria-current={view === "summary" ? "page" : undefined} href={summaryHref ?? paths.request(request.queue, request.sqid)}>Summary</a>
      <a aria-current={view === "history" ? "page" : undefined} href={historyHref ?? paths.request(request.queue, request.sqid, { view: "history" })}>History{model.historyError ? "" : ` (${model.history.length})`}</a>
    </nav>
    {view === "history" ? model.historyError ? <ErrorState error={model.historyError} /> : <RequestHistory events={model.history} /> :
      <div className="sq-detail-columns"><dl className="sq-facts">
        <div><dt>Queue</dt><dd>{request.queue}</dd></div><div><dt>Request ID</dt><dd>{request.sqid}</dd></div>
        <div><dt>Received · UTC</dt><dd><Timestamp value={request.receivedAtMs} /></dd></div>
        <div><dt>Last recorded event</dt><dd>{model.historyError ? "History unavailable" : latest ? <><HistoryValue item={latest} /> <Timestamp value={latest.timestampMs} /></> : "No retained events"}</dd></div>
        <div><dt>Last error</dt><dd>{request.lastError ?? "None reported"}</dd></div>
      </dl><div><section className="sq-soft-panel"><h2>Changes</h2><RequestChanges request={request} {...(changeLinks ? { links: changeLinks } : {})} {...(changeLabels ? { labels: changeLabels } : {})} /></section>
        <details className="sq-soft-panel"><summary>Request metadata</summary>{Object.keys(request.metadata).length ? <Metadata values={request.metadata} /> : <p className="sq-secondary">No additional metadata.</p>}</details>
        {model.historyError ? <ErrorState compact error={model.historyError} /> : null}
      </div></div>}
    {view === "summary" && model.historyError === null ?
      <ol className="sq-stage" aria-label="Observed lifecycle transitions">
        {model.history.filter(event => event.type === "status" && event.status !== null).map((event, index) =>
          <li key={`${event.timestampMs}-${index}`}>{statusDisplay(event.status!).label}</li>
        )}
      </ol> : null}
  </article>;
}

export function RequestDetailView({ result, ...props }: Omit<RequestDetailProps, "model"> & {
  result: LoadResult<RequestDetailModel>;
}) {
  const data = useLastSuccessfulData(result);
  return <>
    {!result.ok ? <><ErrorState compact={data !== undefined} error={result.error} />{data ? <p className="sq-secondary">Showing the last successful snapshot; updates are unavailable.</p> : null}</> : null}
    {data ? <RequestDetail model={data} {...props} /> : props.controls}
  </>;
}

export interface ChangeSubmissionsProps {
  model: ChangeDetailModel;
  basePath?: string;
  onVersionChange?: (href: string) => void;
}

export function ChangeSubmissions({ model, basePath, onVersionChange }: ChangeSubmissionsProps) {
  const paths = new WebPaths(basePath === undefined ? {} : { basePath });
  const versions = [...new Map(model.submissions.map(item => [item.versionHref, item.version])).entries()];
  const selectedVersion = model.pinnedVersion === null ? model.logicalHref :
    model.submissions.find(item => item.version === model.pinnedVersion)?.versionHref;
  return <section className="sq-change-detail">
    <nav className="sq-breadcrumb" aria-label="Breadcrumb"><a href={paths.directory()}>Queues</a><span>›</span><a href={paths.requests(model.queue)}>{model.queue}</a><span>› Change</span></nav>
    <h1>{model.review}</h1><p className="sq-secondary">{model.pinnedVersion ? `Submitted version: ${model.pinnedVersion}` : "Submissions across versions"}</p>
    {model.pagination ? <p className="sq-secondary" role="status">
      Showing matches from this page of queue requests only.
      {model.pagination.nextHref ? " More queue requests remain to be searched." : " The history scan is complete for this receipt window."}
    </p> : null}
    <dl className="sq-change-identity"><div><dt>Provider</dt><dd>{model.provider}</dd></div><div><dt>Host</dt><dd>{model.host}</dd></div><div><dt>Repository</dt><dd>{model.repository || "—"}</dd></div><div><dt>Queue</dt><dd>{model.queue}</dd></div></dl>
    <div className="sq-toolbar"><h2>Submission history</h2>
      {model.logicalHref && onVersionChange && versions.length > 0 ?
        <label>Submitted version<select aria-label="Filter submissions by version"
          value={selectedVersion} onChange={event => onVersionChange(event.target.value)}>
          <option value={model.logicalHref}>All submitted versions</option>
          {versions.map(([href, version]) => <option key={href} value={href}>{version}</option>)}
        </select></label> :
        model.pinnedVersion !== null && model.logicalHref ? <a href={model.logicalHref}>All submitted versions</a> : null}
    </div>
    {model.window ? <p className="sq-secondary">Receipt window: <Timestamp value={model.window.fromMs} /> — <Timestamp value={model.window.toMs} />. This is not unlimited retained history.</p> : <p className="sq-secondary">Retained submissions for this exact version</p>}
    {model.submissions.length === 0 ? <p className="sq-empty">{model.pagination ? "No submissions were found on this page." : "No submissions were found in this scope."}</p> :
      <div className="sq-table-scroll"><table aria-label="Change submissions"><thead><tr><th>Request</th><th>Submitted version</th><th>Received · UTC</th><th>Status</th></tr></thead>
        <tbody>{model.submissions.map(item => <tr key={item.request.sqid}><td><a href={paths.request(model.queue, item.request.sqid)}>{item.request.sqid}</a></td>
          <td><a className="sq-version" aria-label={item.version} title={item.version} href={item.versionHref}>{item.version.length > 20 ? `${item.version.slice(0, 8)}…${item.version.slice(-4)}` : item.version}</a></td><td><Timestamp value={item.request.receivedAtMs} /></td>
          <td><RequestStatus status={item.request.status} />{item.request.lastError ? <p className="sq-row-error">{item.request.lastError}</p> : null}</td></tr>)}</tbody></table></div>}
    <p className="sq-secondary">{model.submissions.length} requests{model.pagination ? " on this page" : ""} · newest received first · repeated submissions remain separate</p>
    {model.pagination ? <nav className="page-actions" aria-label="Change history pagination">
      {model.pagination.nextHref ? <a href={model.pagination.nextHref}>Continue history scan</a> : null}
      {model.pagination.latestHref ? <a href={model.pagination.latestHref}>Latest history</a> : null}
    </nav> : null}
  </section>;
}
