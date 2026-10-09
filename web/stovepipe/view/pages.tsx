"use client";

import { Timestamp } from "./timestamp.js";
import type { RequestSummaryModel, ProjectStatusModel, HistoryEventModel } from "../entity/index.js";
import { Degree, Link, Table } from "./components.js";

export function RequestList({ requests, nextHref }: { requests: RequestSummaryModel[]; nextHref: string | null }) {
  if (!requests.length) return <p className="sq-empty">No retained requests on this queue.</p>;
  return <><Table label="Requests" headers={["Request", "Change", "State", "Accepted", "Updated", "Outcome"]}>
    {requests.map(request => <tr key={request.id}>
      <td>{request.id}</td>
      <td><code>{request.changeUri ? <Link href={request.href}>{request.changeUri}</Link> : "Unknown URI"}</code></td>
      <td><span className="sq-status" data-tone="neutral">{request.state || "Unknown"}</span></td>
      <td><Timestamp value={request.acceptedAtMs} /></td>
      <td><Timestamp value={request.updatedAtMs} /></td>
      <td>{request.outcomeReason || "—"}</td>
    </tr>)}
  </Table>{nextHref && <p><Link href={nextHref}>Older requests →</Link></p>}</>;
}
export function StatusDetails({ status }: { status: ProjectStatusModel }) {
  const request = status.request;
  return <>
    <dl className="sq-facts">
      <div><dt>State</dt><dd><span className="sq-status" data-tone="neutral">{request.state || "Unknown"}</span></dd></div>
      <div><dt>Change URI</dt><dd><code>{request.changeUri || "Unknown URI"}</code></dd></div>
      <div><dt>Base URI</dt><dd><code>{request.baseUri || "No baseline selected"}</code></dd></div>
      <div><dt>Updated</dt><dd><Timestamp value={request.updatedAtMs} /></dd></div>
      <div><dt>Repository result</dt><dd><Degree value={status.repositoryDegree} /></dd></div>
      <div><dt>Project results complete</dt><dd>{status.complete ? "Yes" : "No"}</dd></div>
    </dl>
  </>;
}
export function ProjectResults({ status }: { status: ProjectStatusModel }) {
  return <>
    <p className="sq-secondary">Results for this queue's configured projects. A missing result does not mean green.</p>
    <p><a href={status.rawHref} target="_blank" rel="noopener">Raw project JSON</a></p>
    {status.projects.length ? <Table label="Project results" headers={["Project", "Result"]}>{status.projects.map(project => <tr key={project.name}><td>{project.name}</td><td><Degree value={project.degree} /></td></tr>)}</Table> : <p>No project results recorded on this page.</p>}
    <div className="sq-table-footer">{status.firstHref && <Link href={status.firstHref}>First project page</Link>}{status.nextHref && <Link href={status.nextHref}>Next 10 projects →</Link>}</div>
  </>;
}
export function History({ events }: { events: HistoryEventModel[] }) {
  return events.length ? <Table label="Request history" headers={["Time", "Type", "State or event", "Outcome"]}>
    {events.map(event => <tr key={event.id}><td><Timestamp value={event.timestampMs} /></td><td>{event.kind === "requestState" ? "State" : event.kind === "event" ? "Event" : "Unknown"}</td><td>{event.label}</td><td>{event.outcomeReason || "—"}</td></tr>)}
  </Table> : <p>No retained history yet.</p>;
}
