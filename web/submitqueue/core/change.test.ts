import { describe, expect, it } from "vitest";
import { changeHref, parseChangePath, parseChangeUri, requestChangeLabels, requestChangeLinks } from "./change";
import type { RequestSummaryModel } from "../entity/index.js";

const sha = "0123456789abcdef0123456789abcdef01234567";

describe("readable change identity", () => {
  it.each([
    [`github://github.com/uber/submitqueue/pull/123/${sha}`, `/demo-queue/change/github/github.com/uber/submitqueue/pull/123/${sha}`],
    ["phab://phabricator.example.com/D12345/67890", "/demo-queue/change/phab/phabricator.example.com/D12345/67890"],
    [`git://git.example.com/uber/demo/refs%2Fheads%2Fmain/${sha}`, `/demo-queue/change/git/git.example.com/uber/demo/refs%2Fheads%2Fmain/${sha}`],
  ])("preserves authority and path from %s", (uri, href) => {
    const reference = parseChangeUri(uri);
    expect(reference).not.toBeNull();
    expect(changeHref("demo-queue", reference!, true)).toBe(href);
    expect(parseChangePath(href.split("/").slice(3).map(decodeURIComponent))?.uri).toBe(uri);
  });

  it("distinguishes logical review identity from its submitted version", () => {
    const logical = parseChangePath(["github", "github.com", "uber", "submitqueue", "pull", "123"]);
    expect(logical?.uri).toBeNull();
    expect(logical?.version).toBeNull();
    expect(parseChangeUri(`github://github.com/uber/submitqueue/pull/123/${sha}`)?.logicalPath).toBe(logical?.logicalPath);
  });

  it.each([
    ["phab", "other.example", "D01", "2"],
    ["github", "github.com", "uber", "..", "pull", "1", sha],
    ["github", "GitHub.com", "uber", "repo", "pull", "1", sha],
    ["github", "github.com", "uber", "repo", "pull", "1", "abc"],
  ])("rejects malformed or noncanonical route %j", (...path) => {
    expect(parseChangePath(path)).toBeNull();
  });

  it.each(["bad%zz", "%FF", "%75ber"])("rejects malformed or noncanonical URI segment %s", (segment) => {
    expect(parseChangeUri(`github://github.com/${segment}/submitqueue/pull/123/${sha}`)).toBeNull();
  });

  it.each([
    [`git://git.example.com/demo/refs%2Fheads%2Fmain/${sha}`, "?sq-files=demo%2F00%2Ffile.txt", "/demo-queue/change/git/git.example.com/demo/refs%2Fheads%2Fmain"],
    [`github://github.com/uber/submitqueue/pull/123/${sha}`, "?sq-fake=build-fail&attempt=2", "/demo-queue/change/github/github.com/uber/submitqueue/pull/123"],
  ])("hides URI query markers in labels/links without changing the submitted URI: %s", (clean, query, expectedHref) => {
    const raw = `${clean}${query}`;
    const request: RequestSummaryModel = {
      sqid: "42", queue: "demo-queue", changeUris: [raw], receivedAtMs: 1,
      status: "landed", lastError: null, metadata: {},
    };
    expect(requestChangeLabels([request])[raw]).toBe(clean);
    expect(requestChangeLinks("demo-queue", [request])[raw]).toBe(expectedHref);
    expect(request.changeUris).toEqual([raw]);
  });

  it("does not relabel a URI that is already canonical", () => {
    const uri = `github://github.com/uber/submitqueue/pull/123/${sha}`;
    const request: RequestSummaryModel = {
      sqid: "42", queue: "demo-queue", changeUris: [uri], receivedAtMs: 1,
      status: "landed", lastError: null, metadata: {},
    };
    expect(requestChangeLabels([request])).toEqual({});
  });
});
