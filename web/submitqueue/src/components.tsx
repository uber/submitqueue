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

export function ErrorState({ error, compact = false }: { error: WebError; compact?: boolean }) {
  return <section className="sq-error" data-error-kind={error.kind} role={compact ? "status" : "alert"}>
    <h2>{error.title}</h2><p>{error.message}</p>
  </section>;
}
