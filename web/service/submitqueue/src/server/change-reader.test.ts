import { Code, ConnectError } from "@connectrpc/connect";
import type { SubmitQueueGatewayClient } from "@submitqueue/web-submitqueue/server";
import { describe, expect, it, vi } from "vitest";
import { parseChangeUri, parseChangePath } from "./change";
import { readChangeSubmissions } from "./change-reader";

const uri = "github://github.com/uber/submitqueue/pull/123/0123456789abcdef0123456789abcdef01234567";
const request = (sqid: string) => ({ sqid, changeUris: [uri] });
const window = { fromMs: 100, toMs: 200 };

describe("demo bounded change reader", () => {
  it("loads all queue pages and keeps repeated submissions distinct", async () => {
    const list = vi.fn()
      .mockResolvedValueOnce({ requests: [request("42")], nextPageToken: "next" })
      .mockResolvedValueOnce({ requests: [request("38")], nextPageToken: "" });
    const client = { list } as unknown as SubmitQueueGatewayClient;
    const reference = parseChangePath(["github", "github.com", "uber", "submitqueue", "pull", "123"])!;
    const result = await readChangeSubmissions(client, "demo-queue", reference, window);
    expect(result.submissions.map(value => value.request.sqid)).toEqual(["42", "38"]);
    expect(result.nextPageToken).toBeNull();
    expect(list).toHaveBeenLastCalledWith(expect.objectContaining({ pageToken: "next", receivedAtOrAfterMs: 100n, receivedBeforeMs: 200n }));
  });

  it("uses exact-URI lookup rather than the receipt window for a pinned version", async () => {
    const getRequestSummaryByChangeURI = vi.fn().mockResolvedValue({ requests: [request("42")] });
    const list = vi.fn();
    const client = { list, getRequestSummaryByChangeURI } as unknown as SubmitQueueGatewayClient;
    await readChangeSubmissions(client, "demo-queue", parseChangeUri(uri)!, window);
    expect(getRequestSummaryByChangeURI).toHaveBeenCalledWith({ queue: "demo-queue", changeUri: uri });
    expect(list).not.toHaveBeenCalled();
  });

  it("finds demo git changes by clean identity while retaining raw file hints in backend data", async () => {
    const uri = "git://git.example.com/demo/refs%2Fheads%2Fmain/0123456789abcdef0123456789abcdef01234567?sq-files=demo%2F00%2Ffile.txt";
    const list = vi.fn().mockResolvedValue({ requests: [{ sqid: "42", changeUris: [uri] }], nextPageToken: "" });
    const getRequestSummaryByChangeURI = vi.fn();
    const client = { list, getRequestSummaryByChangeURI } as unknown as SubmitQueueGatewayClient;
    const result = await readChangeSubmissions(client, "demo-queue", parseChangeUri(uri)!, window);
    expect(result.submissions[0]?.request.sqid).toBe("42");
    expect(result.submissions[0]?.versionHref).not.toContain("?");
    expect(getRequestSummaryByChangeURI).not.toHaveBeenCalled();
  });

  it.each([true, false])("continues a bounded scan without losing older matches (first page has matches: %s)", async (hasMatches) => {
    const list = vi.fn().mockImplementation(async ({ pageToken }: { pageToken: string }) => {
      const page = pageToken ? Number(pageToken.slice(5)) : 0;
      return {
        requests: page === 10 ? [request("38")] : page === 0 && hasMatches ? [request("42")] : [],
        nextPageToken: page < 10 ? `page-${page + 1}` : "",
      };
    });
    const client = { list } as unknown as SubmitQueueGatewayClient;
    const reference = parseChangePath(["github", "github.com", "uber", "submitqueue", "pull", "123"])!;
    const first = await readChangeSubmissions(client, "demo-queue", reference, window);
    expect(first.submissions.map(value => value.request.sqid)).toEqual(hasMatches ? ["42"] : []);
    expect(first.nextPageToken).toBe("page-10");
    expect(list).toHaveBeenCalledTimes(10);
    const second = await readChangeSubmissions(client, "demo-queue", reference, { ...window, pageToken: first.nextPageToken! });
    expect(second.submissions.map(value => value.request.sqid)).toEqual(["38"]);
    expect(second.nextPageToken).toBeNull();
    expect(list).toHaveBeenLastCalledWith(expect.objectContaining({
      pageToken: "page-10", receivedAtOrAfterMs: 100n, receivedBeforeMs: 200n,
    }));
  });

  it("rejects a cursor that loops back to the starting page", async () => {
    const client = {
      list: vi.fn().mockResolvedValue({ requests: [], nextPageToken: "start" }),
    } as unknown as SubmitQueueGatewayClient;
    const reference = parseChangePath(["github", "github.com", "uber", "submitqueue", "pull", "123"])!;
    await expect(readChangeSubmissions(client, "demo-queue", reference, { ...window, pageToken: "start" }))
      .rejects.toMatchObject({ code: Code.Internal });
  });

  it("fails a looping scan instead of presenting duplicate history", async () => {
    const client = {
      list: vi.fn().mockResolvedValue({ requests: [request("42")], nextPageToken: "repeated" }),
    } as unknown as SubmitQueueGatewayClient;
    const reference = parseChangePath(["github", "github.com", "uber", "submitqueue", "pull", "123"])!;
    await expect(readChangeSubmissions(client, "demo-queue", reference, window)).rejects.toMatchObject({
      code: Code.Internal,
    } satisfies Partial<ConnectError>);
  });
});
