"use client";

import { Fact, Timestamp } from "@submitqueue/web-platform";
import { Block } from "baseui/block";
import { StyledTableBodyCell, StyledTableBodyRow } from "baseui/table-semantic";
import { Tag } from "baseui/tag";
import { ParagraphSmall } from "baseui/typography";
import type { ReactNode } from "react";
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
}: {
  requests: RequestSummaryModel[];
}) {
  if (!requests.length)
    return <ParagraphSmall color="contentSecondary">No retained requests on this queue.</ParagraphSmall>;
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
          <StyledTableBodyRow key={request.id}>
            <StyledTableBodyCell>
              <Link href={request.href}>{request.id}</Link>
            </StyledTableBodyCell>
            <StyledTableBodyCell>
              <Block as="code" font="MonoParagraphXSmall">{request.changeUri || "Unknown URI"}</Block>
            </StyledTableBodyCell>
            <StyledTableBodyCell>
              <Tag closeable={false} kind="neutral" size="small">
                {request.state || "Unknown"}
              </Tag>
            </StyledTableBodyCell>
            <StyledTableBodyCell>
              <RequestTimestamp value={request.acceptedAtMs} />
            </StyledTableBodyCell>
            <StyledTableBodyCell>
              <RequestTimestamp value={request.updatedAtMs} />
            </StyledTableBodyCell>
            <StyledTableBodyCell>{request.outcomeReason || "—"}</StyledTableBodyCell>
          </StyledTableBodyRow>
        ))}
      </Table>
    </>
  );
}
export function RequestDetails({ request, repositoryResult }: { request: RequestSummaryModel | null; repositoryResult: ReactNode }) {
  return (
    <>
      <Block as="dl" margin="0" display="grid" gridTemplateColumns={["minmax(0, 1fr)", "repeat(2, minmax(0, 1fr))", "repeat(3, minmax(0, 1fr))", "repeat(5, minmax(0, 1fr))"]} gridGap="scale400">
        {request && <Fact label="State" compact>
          <Tag closeable={false} kind="neutral" size="small">
            {request.state || "Unknown"}
          </Tag>
        </Fact>}
        {repositoryResult && <Fact label="Repository result" compact>{repositoryResult}</Fact>}
        {request && <>
          <Fact label="Accepted" compact><RequestTimestamp value={request.acceptedAtMs} /></Fact>
          <Fact label="Updated" compact><RequestTimestamp value={request.updatedAtMs} /></Fact>
          <Fact label="Outcome" compact>{request.outcomeReason || "—"}</Fact>
        </>}
      </Block>
      {request && <Block as="dl" marginTop="scale300" marginBottom="scale400" display="grid" gridTemplateColumns={["minmax(0, 1fr)", "repeat(2, minmax(0, 1fr))"]} gridGap="scale400">
        <Fact label="Change URI" compact>
          <Block as="code" font="MonoParagraphXSmall">{request.changeUri || "Unknown URI"}</Block>
        </Fact>
        <Fact label="Base URI" compact>
          <Block as="code" font="MonoParagraphXSmall">{request.baseUri || "No baseline selected"}</Block>
        </Fact>
      </Block>}
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
        <StyledTableBodyRow key={event.id}>
          <StyledTableBodyCell>
            <RequestTimestamp value={event.timestampMs} />
          </StyledTableBodyCell>
          <StyledTableBodyCell>
            {event.kind === "requestState"
              ? "State"
              : event.kind === "event"
                ? "Event"
                : "Unknown"}
          </StyledTableBodyCell>
          <StyledTableBodyCell>{event.label}</StyledTableBodyCell>
          <StyledTableBodyCell>{event.outcomeReason || "—"}</StyledTableBodyCell>
        </StyledTableBodyRow>
      ))}
    </Table>
  ) : (
    <ParagraphSmall color="contentSecondary">No retained history yet.</ParagraphSmall>
  );
}
