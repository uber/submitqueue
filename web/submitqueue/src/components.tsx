"use client";

import type { ReactNode } from "react";
import type {
  HistoryEventModel,
  RequestDetailModel,
  RequestListModel,
  RequestSummaryModel,
  WebError,
} from "./models.js";
import { WebPaths } from "./paths.js";
import { statusDisplay } from "./status.js";

function Timestamp({ value }: { value: number }) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return <time>{value}</time>;
  }
  return <time dateTime={date.toISOString()}>{date.toLocaleString()}</time>;
}

function Metadata({ values }: { values: Record<string, string> }) {
  const entries = Object.entries(values).sort(([left], [right]) => left.localeCompare(right));
  if (entries.length === 0) {
    return null;
  }
  return (
    <dl className="sq-metadata">
      {entries.map(([key, value]) => (
        <div className="sq-metadata__entry" key={key}>
          <dt>{key}</dt>
          <dd>{value}</dd>
        </div>
      ))}
    </dl>
  );
}

export function RequestStatus({ status }: { status: string }) {
  const display = statusDisplay(status);
  return (
    <span className="sq-status" data-status={status || "unknown"} data-tone={display.tone}>
      {display.label}
    </span>
  );
}

function RequestChanges({ request }: { request: RequestSummaryModel }) {
  return (
    <ul className="sq-changes" aria-label="Changes">
      {request.changeUris.map((uri) => (
        <li key={uri}>
          <code>{uri}</code>
        </li>
      ))}
    </ul>
  );
}

export interface RequestListProps {
  model: RequestListModel;
  requestsBasePath?: string;
  empty?: ReactNode;
}

export function RequestList({ model, requestsBasePath, empty }: RequestListProps) {
  if (model.requests.length === 0) {
    return <div className="sq-empty">{empty ?? "No requests were received in this window."}</div>;
  }
  const paths = new WebPaths(
    requestsBasePath === undefined ? {} : { requestsPath: requestsBasePath },
  );
  return (
    <>
      <ol className="sq-request-list" aria-label={`Requests in ${model.queue}`}>
        {model.requests.map((request) => (
          <li className="sq-request-list__item" key={request.sqid}>
            <article>
              <header>
                <a href={paths.request(request.sqid)}>{request.sqid}</a>
                <RequestStatus status={request.status} />
              </header>
              <RequestChanges request={request} />
              <p>
                Received <Timestamp value={request.receivedAtMs} />
              </p>
              {request.lastError ? <p role="alert">{request.lastError}</p> : null}
            </article>
          </li>
        ))}
      </ol>
      {model.nextPageToken ? (
        <nav aria-label="Request pages">
          <a
            href={paths.requests({
              fromMs: model.receivedAtOrAfterMs,
              toMs: model.receivedBeforeMs,
              pageToken: model.nextPageToken,
            })}
          >
            Next page
          </a>
        </nav>
      ) : null}
    </>
  );
}

function HistoryValue({ item }: { item: HistoryEventModel }) {
  if (item.type === "status" && item.status !== null) {
    return <RequestStatus status={item.status} />;
  }
  if (item.type === "event" && item.event !== null) {
    return <span className="sq-event">{statusDisplay(item.event).label}</span>;
  }
  return <span className="sq-event">Unknown event</span>;
}

export function RequestHistory({ events }: { events: HistoryEventModel[] }) {
  if (events.length === 0) {
    return <p className="sq-empty">No history has been retained for this request.</p>;
  }
  return (
    <ol className="sq-history" aria-label="Request history">
      {events.map((item, index) => (
        <li className="sq-history__item" key={`${item.timestampMs}-${index}`}>
          <Timestamp value={item.timestampMs} />
          <HistoryValue item={item} />
          {item.lastError ? <p role="alert">{item.lastError}</p> : null}
          <Metadata values={item.metadata} />
        </li>
      ))}
    </ol>
  );
}

export function ErrorState({ error, compact = false }: { error: WebError; compact?: boolean }) {
  return (
    <section className="sq-error" data-error-kind={error.kind} role={compact ? "status" : "alert"}>
      <h2>{error.title}</h2>
      <p>{error.message}</p>
    </section>
  );
}

export interface RequestDetailProps {
  model: RequestDetailModel;
}

export function RequestDetail({ model }: RequestDetailProps) {
  const { request } = model;
  return (
    <article className="sq-request-detail">
      <header>
        <p>{request.queue}</p>
        <h1>{request.sqid}</h1>
        <RequestStatus status={request.status} />
        <p>
          Received <Timestamp value={request.receivedAtMs} />
        </p>
      </header>
      <section aria-labelledby="sq-changes-heading">
        <h2 id="sq-changes-heading">Changes</h2>
        <RequestChanges request={request} />
      </section>
      {request.lastError ? (
        <section aria-labelledby="sq-last-error-heading">
          <h2 id="sq-last-error-heading">Last error</h2>
          <p role="alert">{request.lastError}</p>
        </section>
      ) : null}
      <Metadata values={request.metadata} />
      <section aria-labelledby="sq-history-heading">
        <h2 id="sq-history-heading">History</h2>
        {model.historyError ? <ErrorState compact error={model.historyError} /> : null}
        <RequestHistory events={model.history} />
      </section>
    </article>
  );
}
