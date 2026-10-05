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
