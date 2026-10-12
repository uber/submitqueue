"use client";

import { Timestamp } from "@submitqueue/web-platform";
import type {
  RequestSummaryModel,
  HistoryEventModel,
} from "../entity/index.js";
import { Link, Table } from "./components.js";

function RequestTimestamp({ value }: { value: string }) {
  const milliseconds = Number(value);
  if (milliseconds <= 0 || Number.isNaN(new Date(milliseconds).getTime()))
    return <span>Unknown</span>;
  return <Timestamp value={value} />;
}

export function RequestList({
  requests,
  nextHref,
}: {
  requests: RequestSummaryModel[];
  nextHref: string | null;
}) {
  if (!requests.length)
    return <p className="sq-empty">No retained requests on this queue.</p>;
  return (
    <>
      <Table
        label="Requests"
        headers={[
          "Request",
          "Change",
          "State",
          "Accepted",
          "Updated",
          "Outcome",
        ]}
      >
        {requests.map((request) => (
          <tr key={request.id}>
            <td>
              <Link href={request.href}>{request.id}</Link>
            </td>
            <td>
              <code>{request.changeUri || "Unknown URI"}</code>
            </td>
            <td>
              <span className="sq-status" data-tone="neutral">
                {request.state || "Unknown"}
              </span>
            </td>
            <td>
              <RequestTimestamp value={request.acceptedAtMs} />
            </td>
            <td>
              <RequestTimestamp value={request.updatedAtMs} />
            </td>
            <td>{request.outcomeReason || "—"}</td>
          </tr>
        ))}
      </Table>
      {nextHref && (
        <p>
          <Link href={nextHref}>Older requests →</Link>
        </p>
      )}
    </>
  );
}
export function RequestDetails({ request }: { request: RequestSummaryModel }) {
  return (
    <>
      <dl className="sq-facts">
        <div>
          <dt>State</dt>
          <dd>
            <span className="sq-status" data-tone="neutral">
              {request.state || "Unknown"}
            </span>
          </dd>
        </div>
        <div>
          <dt>Change URI</dt>
          <dd>
            <code>{request.changeUri || "Unknown URI"}</code>
          </dd>
        </div>
        <div>
          <dt>Base URI</dt>
          <dd>
            <code>{request.baseUri || "No baseline selected"}</code>
          </dd>
        </div>
        <div>
          <dt>Accepted</dt>
          <dd>
            <RequestTimestamp value={request.acceptedAtMs} />
          </dd>
        </div>
        <div>
          <dt>Updated</dt>
          <dd>
            <RequestTimestamp value={request.updatedAtMs} />
          </dd>
        </div>
        <div>
          <dt>Outcome</dt>
          <dd>{request.outcomeReason || "—"}</dd>
        </div>
      </dl>
    </>
  );
}
export function History({ events }: { events: HistoryEventModel[] }) {
  return events.length ? (
    <Table
      label="Request history"
      headers={["Time", "Type", "State or event", "Outcome"]}
    >
      {events.map((event) => (
        <tr key={event.id}>
          <td>
            <RequestTimestamp value={event.timestampMs} />
          </td>
          <td>
            {event.kind === "requestState"
              ? "State"
              : event.kind === "event"
                ? "Event"
                : "Unknown"}
          </td>
          <td>{event.label}</td>
          <td>{event.outcomeReason || "—"}</td>
        </tr>
      ))}
    </Table>
  ) : (
    <p>No retained history yet.</p>
  );
}
