# Host in Change ID URI

Embed the target host (domain or domain:port) in every change ID URI so the identity of a change is self-contained — no out-of-band configuration required to know which server it lives on.

## Problem

Change ID URIs currently encode everything about a change except *where it lives*. A GitHub URI carries the scheme, org, repo, PR number, and head SHA, but not the hostname of the GitHub instance that hosts it:

```
github://uber/submitqueue/pull/123/c3a4d5e...   ← no host
```

The host is resolved out-of-band: the `BaseURLTransport` in `platform/http` carries the API base URL, and the change provider factory is configured per queue with a hostname. This works when there is exactly one instance per scheme, but breaks as soon as a second instance of the same type appears — which is a realistic scenario:

- **Multiple GHES instances.** Large organizations commonly run more than one GitHub Enterprise Server: one per business unit, one per compliance boundary, or a staging instance alongside production. Two PRs with the same org/repo/number on different GHES hosts are *different changes*, but their URIs are identical under the old format.
- **Phabricator migrations.** Teams migrating between Phabricator instances need both old and new revision URIs to coexist in the same queue without ambiguity.
- **Multi-tenant deployments.** A single SubmitQueue deployment serving queues across different GitHub instances must distinguish changes by host, not by separate deployments.

Without the host in the URI, the system has no way to tell these apart at the identity level. The workaround — configuring a single host per scheme in the provider factory — is fragile and does not compose.

The `git://` scheme already solved this: its URI includes a `Remote` field (the host) as part of the identity. GitHub and Phabricator URIs should follow the same pattern.

## Decision

Add the host as the first path segment after the scheme separator in all change ID URIs.

### GitHub / GHE / GHES

Old format:

```
{scheme}://{org}/{repo}/pull/{pr}/{sha}
```

New format:

```
{scheme}://{host}/{org}/{repo}/pull/{pr}/{sha}
```

Examples:

```
github://github.com/uber/submitqueue/pull/123/c3a4d5e6f7890123456789abcdef0123456789ab
ghe://ghe.example.com/corp/service/pull/42/deadbeefdeadbeefdeadbeefdeadbeefdeadbeef
ghes://ghes.corp.net:8443/org/repo/pull/7/0123456789abcdef0123456789abcdef01234567
```

The `ChangeID` struct gains a `Host` field. The parser requires at least 6 path segments (was 5): host, org, repo, `pull`, PR number, SHA. The host must be non-empty; ports are allowed (`host:port`). The `String()` serializer includes the host as the first segment after `://`.

### Phabricator

Old format:

```
phab://D{revision_id}/{diff_id}
```

New format:

```
phab://{host}/D{revision_id}/{diff_id}
```

Examples:

```
phab://phabricator.example.com/D12345/67890
phab://phab.corp.net:8443/D42/99
```

The `ChangeID` struct gains a `Host` field. The parser requires exactly 3 path segments (was 2): host, D-prefixed revision, diff ID. The host must be non-empty.

### Validation

Stacked changes (multiple URIs in a single `Change`) must share the same host. The `validateChangeConsistency` function in the GitHub change provider already checks scheme, org, and repo consistency; it now also checks host consistency. A mismatch produces a clear error:

```
stacked changes must be from same host: expected github.com, got ghes.corp.net for PR #42
```

### Wire format

The proto documentation in `api/base/change/proto/change.proto` is updated to reflect the new templates. The proto message itself (`Change`) is a simple `repeated string uris` — it carries opaque URI strings, so the wire format does not change. This is a URI-level identity change, not a proto schema change.

## Scope

### What changes

- `platform/base/change/github/change_id.go` — `ChangeID` struct, `ParseChangeID`, `String()`
- `platform/base/change/phabricator/change_id.go` — `ChangeID` struct, `ParseChangeID`, `String()`
- `platform/base/change/change.go` — doc comments with URI templates
- `api/base/change/proto/change.proto` — doc comments with URI templates
- `submitqueue/extension/changeprovider/github/validate.go` — host consistency check
- All test files with hardcoded URI strings (~40 files)

### What does not change

- The `git://` scheme — it already has a `Remote` field serving the same purpose.
- The proto wire format — `Change.uris` is `repeated string`, unaffected.
- The `BaseURLTransport` — it continues to work as before; the host in the URI is for identity, not for HTTP routing. The transport's base URL is the API endpoint (e.g. `https://api.github.com`), which may differ from the host in the URI (e.g. `github.com`). These are complementary, not redundant.
- The routing provider (`routing/provider.go`) — it dispatches by scheme, not by host. A host-aware routing layer (dispatching to different API clients per host) is a future concern outside this RFC's scope.

## Migration

This is a breaking change to the URI format. All stored URIs (in queues, databases, logs) will use the old format until re-created.

- **New URIs** are generated with the host by the gateway at submission time — the caller supplies the host as part of the URI.
- **Existing URIs** in flight will fail to parse with the new parser. Since SubmitQueue processes requests to completion quickly (minutes, not days), a deploy that drains in-flight requests before upgrading the parser is sufficient. No backfill migration is needed.
- **Persisted URIs** in change records or historical data are immutable. If historical queries must work across the format boundary, the reader should fall back to the old parser on failure. This is a bounded compatibility window, not a permanent dual-parse requirement.

## Alternatives considered

### Host as a query parameter

```
github://uber/repo/pull/123/sha?host=ghes.corp.net
```

This keeps the path structure unchanged but makes the host optional and easy to omit. The host is part of the change's identity — it belongs in the primary path, not in metadata.

### Separate scheme per host

```
ghes-corp://org/repo/pull/123/sha
ghes-staging://org/repo/pull/123/sha
```

This encodes the host in the scheme, but schemes are a finite enumerated set in the routing provider. Adding a scheme per host couples deployment topology to the URI grammar and requires code changes for every new instance.

### Keep host out-of-band, add a host field to the ChangeID struct only

Callers would populate `ChangeID.Host` from configuration rather than parsing it from the URI. This splits identity between the URI (partial) and the struct (full), making the URI non-self-describing. Any system that only sees the URI string — logs, dashboards, queue payloads — cannot determine which host a change belongs to.
