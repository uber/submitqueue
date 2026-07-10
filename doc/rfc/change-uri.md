# Change URIs

A change URI is the system-wide identity of a code change — a Pull Request, a Phabricator Diff, or a git ref/commit. It is minted by the client at submission, validated at the gateway, and flows as an opaque string through the shared `Change` wire contract (`api/base/change`), cross-domain queue payloads, and storage, where it is a primary-key column and correlation key. A change URI must therefore be globally unambiguous on its own: interpretable without knowing which queue carried it or how any backend is wired.

## Shape

Every change URI is an RFC 3986 URI of the form `scheme://{host[:port]}/{path}`, with a uniform division of labor:

- **scheme** — the provider *model*: how to parse the path and which extension family (change provider, merge checker, pusher) can act on it. One scheme per model — deployment flavors of the same model (github.com vs. GitHub Enterprise) do **not** get their own schemes, because the flavor is derivable from the host and two spellings for one instance would break identity.
- **authority** — the provider *instance*: the `host[:port]` the change lives on. Mandatory.
- **path** — the change within that instance, pinned to an exact code state (head SHA or diff ID), so staleness is detectable by comparing the pin against the provider's current state.

## Formats

| Provider | Format | Example |
|---|---|---|
| GitHub PR | `github://{host[:port]}/{org}/{repo}/pull/{pr}/{head_sha}` | `github://github.uberinternal.com/uber/submitqueue/pull/123/c3a4…89ab` |
| Phabricator Diff | `phab://{host[:port]}/D{revision}/{diff}` | `phab://phabricator.example.com/D12345/67890` |
| git ref/commit | `git://{host[:port]}/{repo}/{ref}/{sha}` | `git://git.example.com:9418/uber/mono/refs%2Fheads%2Fmain/c3a4…89ab` |

Path rules per provider:

- **GitHub** — `{org}` may be a nested path (`uber/frontend`); the literal `pull` segment separates it from the PR number, mirroring the real PR URL layout so URIs are built by substitution, not reshaping. `{head_sha}` is the PR's head commit at submission time.
- **Phabricator** — `D{revision}` is the logical review (stable across updates); `{diff}` is the uploaded patch version that pins the exact code state, analogous to GitHub's head SHA. Both are positive integers without leading zeros.
- **git** — `{repo}` is the repository path on the remote and may contain slashes; `{ref}` is a fully-qualified git ref (`refs/heads/main`, `refs/tags/v1.0`), percent-encoded so it occupies a single path segment; `{sha}` is a commit that ref has pointed to.

## Canonical form

URIs are compared as opaque strings everywhere (primary keys, claim lookups, staleness checks), so exactly one spelling per change is valid. Parsers **validate the canonical form and reject everything else — they never normalize**, because normalization applied at one entry point and skipped at another lets two spellings of one change into the system.

- **Host** — required, non-empty, lowercase (DNS is case-insensitive, so case variants would alias one instance into many identities). Uppercase is rejected, not folded.
- **Port** — optional, digits only, verbatim when present. Custom schemes have no registered default port, so there is nothing to strip; omit it unless the backend listens on a non-standard one.
- **Commit SHAs** — the full 40-character lowercase hex form. Abbreviated or uppercase SHAs are rejected, not expanded or folded.
- **All other path segments** — verbatim. Org, repo, and ref segments live in namespaces that are case-sensitive (git refs, repository paths on a git remote) or provider-canonical (GitHub resolves org/repo case-insensitively, but each repo has one canonical casing and uppercase is legal — the parser cannot know which). Folding their case would silently point the identity at a different resource; canonical casing here is the provider's to enforce, at the point where the provider is consulted.
- **Round-trip** — parsing a valid URI and re-serializing the parsed form yields the input byte-for-byte.

Parsing is delegated to `net/url`, which handles `host:port` splitting, bracketed IPv6 hosts, and percent-encoding correctly.

## Rejected alternatives

- **Host out-of-band in queue config.** Conflates identity with routing: the meaning of a stored primary-key value must not depend on deployment wiring, and the shared contract must be interpretable by every domain that imports it.
- **Per-flavor schemes** (`ghe://`, `ghes://`). Redundant with the authority, and an open-ended enum baked into parsers and routing — a new instance should be configuration, not code. The flavors share one PR model and one API surface; what does differ per instance (API base path, version skew) is wiring config on the client for that host, never identifier grammar.
- **The provider's web URL as identity** (`https://github.com/uber/repo/pull/123`). Human-facing URLs don't uniformly pin the code state, vary with provider UI cosmetics, and hand our identity grammar to a third party. Custom schemes keep the grammar strict and ours.
