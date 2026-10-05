import { describe, expect, it } from "vitest";
import { changeHref, parseChangePath, parseChangeUri, requestChangeLabels, requestChangeLinks } from "./change";
import type { RequestSummaryModel } from "@submitqueue/web-submitqueue";

const sha = "0123456789abcdef0123456789abcdef01234567";

describe("host change route adapter", () => {
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

  it("hides fake file markers in labels/links without changing the submitted URI", () => {
    const clean = `git://git.example.com/demo/refs%2Fheads%2Fmain/${sha}`;
    const raw = `${clean}?sq-files=demo%2F00%2Ffile.txt`;
    const request: RequestSummaryModel = {
      sqid: "42", queue: "demo-queue", changeUris: [raw], receivedAtMs: 1,
      status: "landed", lastError: null, metadata: {},
    };
    expect(requestChangeLabels([request])[raw]).toBe(clean);
    const href = requestChangeLinks("demo-queue", [request])[raw]!;
    expect(href).toBe("/demo-queue/change/git/git.example.com/demo/refs%2Fheads%2Fmain");
    expect(href).not.toContain("sq-files");
    expect(request.changeUris).toEqual([raw]);
  });
});
