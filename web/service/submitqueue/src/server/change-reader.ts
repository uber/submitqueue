import "server-only";

import { Code, ConnectError } from "@connectrpc/connect";
import type { GatewayChangeSubmission, GatewayRequestSummary, SubmitQueueGatewayClient } from "@submitqueue/web-submitqueue/server";
import { changeHref, parseChangeUri, type ChangeReference } from "./change";
import { REQUEST_PAGE_SIZE, type RequestWindow } from "./window";

const MAX_CHANGE_SCAN_PAGES = 10;

export interface ChangeSubmissionPage {
  submissions: GatewayChangeSubmission[];
  nextPageToken: string | null;
}

export async function readChangeSubmissions(
  client: SubmitQueueGatewayClient,
  queue: string,
  reference: ChangeReference,
  window: RequestWindow,
): Promise<ChangeSubmissionPage> {
  const submissions: GatewayChangeSubmission[] = [];
  const appendSubmissions = (requests: readonly GatewayRequestSummary[]) => {
    for (const request of requests) {
      const match = request.changeUris.map(parseChangeUri).find(value =>
        value !== null && value.logicalPath === reference.logicalPath &&
        (reference.version === null || value.version === reference.version));
      if (match?.version) {
        submissions.push({
          request, version: match.version,
          versionHref: changeHref(queue, match, true),
        });
      }
    }
  };
  if (reference.uri !== null && reference.scheme !== "git") {
    const response = await client.getRequestSummaryByChangeURI({ queue, changeUri: reference.uri });
    appendSubmissions(response.requests);
    return { submissions, nextPageToken: null };
  }
  let pageToken = window.pageToken ?? "";
  const seenTokens = new Set<string>(pageToken ? [pageToken] : []);
  for (let page = 0; page < MAX_CHANGE_SCAN_PAGES; page += 1) {
    const response = await client.list({
      queue,
      receivedAtOrAfterMs: BigInt(window.fromMs),
      receivedBeforeMs: BigInt(window.toMs),
      pageSize: REQUEST_PAGE_SIZE,
      pageToken,
    });
    appendSubmissions(response.requests);
    if (!response.nextPageToken) {
      return { submissions, nextPageToken: null };
    }
    if (seenTokens.has(response.nextPageToken)) {
      throw new ConnectError("Repeated queue page token", Code.Internal);
    }
    pageToken = response.nextPageToken;
    seenTokens.add(pageToken);
  }
  return { submissions, nextPageToken: pageToken };
}
