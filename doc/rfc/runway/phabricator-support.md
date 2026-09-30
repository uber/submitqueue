# Phabricator Support in Runway

## Summary

Runway rejects `phab://` change URIs. This RFC adds support so Phabricator-based repositories can use SubmitQueue end-to-end. The change applies to both Runway operations (merge-conflict-check and merge) since they share one code path.

## Background

The `phab://` URI scheme, the `ChangeProvider` (Conduit), and the routing provider already work. The gap is in Runway's git merger — the only component that resolves a change URI into a git commit.

A GitHub URI embeds the commit SHA: `github://{host}/{org}/{repo}/pull/{pr}/{head_sha}`. A Phabricator URI does not: `phab://{host}/D{revision}/{diff_id}`. The DiffID pins the code state, but the commit only exists at a staging ref (`refs/tags/phabricator/diff/{diff_id}`) that Phabricator pushes when staging areas are enabled. The SHA is unknown until fetched.

## Proposal

### 1. `ObjectResolver` interface

Object resolution (fetching commits, resolving refs to SHAs, checking staleness) is currently embedded in the git merger. This RFC extracts it into a pluggable interface so the same orchestration logic works with any VCS backend — the OSS git implementation, or an alternative implementation that a deployment substitutes.

```
runway/extension/merger/
├── merger.go           — Merger interface (Merge, CheckMergeability)
├── object/
│   └── resolver.go     — ObjectResolver interface + shared orchestration
├── git/
│   ├── git_merger.go   — Merger impl
│   ├── changeref.go    — resolveChange (URI → changeRef)
│   └── objects.go      — gitResolver (implements ObjectResolver via git CLI)
├── fake/
├── noop/
└── mock/
```

The `ObjectResolver` interface:

```go
// package object

type ObjectResolver interface {
    HasCommit(ctx context.Context, sha string) bool
    FetchBySHA(ctx context.Context, sha string) error
    FetchByRef(ctx context.Context, ref string) error
    ResolveHead(ctx context.Context) (string, error)
    IsRemoteReachable(ctx context.Context) error
    LSRemote(ctx context.Context, ref string) (string, error)
}
```

Shared orchestration lives in the same package and operates on `ObjectResolver`:

```go
func EnsureObject(ctx context.Context, r ObjectResolver, ref changeRef) (changeRef, error)
func EnsureObjects(ctx context.Context, r ObjectResolver, refs []changeRef) ([]changeRef, error)
func EnsureStepObjects(ctx context.Context, r ObjectResolver, steps []resolvedStep) ([]resolvedStep, error)
func CheckStale(ctx context.Context, r ObjectResolver, refs []changeRef) error
```

The OSS git merger provides a `gitResolver` in `merger/git/` that implements `ObjectResolver` by shelling out to the git CLI. Alternative deployments provide their own implementation. Both plug into the same shared orchestration.

### 2. `phab://` case in `resolveChange`

Each `Merger` implementation has its own URI-to-change resolution. The OSS git merger adds a `phab` case to `resolveChange`:

```go
case "phab":
    cid, _ := entityphab.ParseChangeID(uri)
    return changeRef{
        Provider: scheme,
        Ref:      fmt.Sprintf(m.phabStagingRefFormat, cid.DiffID),
        Label:    cid.Revision(), // "D12345"
    }, nil
```

Alternative deployments need an equivalent change in their own resolution logic. The parsing is ~15 lines and uses the shared `entityphab.ParseChangeID` from `platform/base/change/phabricator/`. If consolidation is desired later, `resolveChange` and `changeRef` can be moved to the `object/` package as exported shared code.

### 3. SHA resolution in `EnsureObject`

When SHA is empty, fetch the staging ref and resolve the commit. Both empty is a terminal error:

```go
if ref.SHA == "" && ref.Ref == "" { return error: no SHA and no ref }
if ref.SHA == "" { fetch ref.Ref → resolve SHA via ResolveHead → return ref }
// ... existing by-SHA flow unchanged ...
```

`EnsureObject` returns the `changeRef` with SHA set. `EnsureStepObjects` returns updated steps so the resolved SHAs propagate to all downstream paths (`checkStale`, `applySteps`, `promote`). `changeRef` stays a value type — resolution returns new values rather than mutating shared state.

### 4. Configurable staging ref format

`Params.PhabStagingRefFormat` defaults to `refs/tags/phabricator/diff/%d`. Validated at startup to contain a `%d` placeholder.

```yaml
defaults:
  merger:
    type: git
    phabStagingRefFormat: "refs/tags/phabricator/diff/%d"  # default, omit if stock
```

## Scope, limitations, and prerequisites

**Prerequisites:**
- **Staging areas must be enabled** on the Phabricator instance. Without staging, the ref doesn't exist and the fetch fails terminally (`ErrInvalidRequest`). The staging ref format is configurable via `phabStagingRefFormat` in the YAML config for non-standard layouts.

**Limitations:**
- **No merge-time staleness detection.** For GitHub, `CheckStale` catches force-pushes between validate and merge via `LSRemote`. For Phab, each `arc diff` creates a new diff ID with its own immutable staging ref, so this check always passes. Staleness detection must happen upstream at the validate stage via Conduit. If the validate-to-merge window proves problematic, the long-term fix is a pluggable staleness check.
- **Merge strategy.** Phabricator workflows produce linear git history (one commit per change, no merge commits). Queues serving Phab repos should use SQUASH_REBASE or REBASE to match this convention. The MERGE strategy works but produces non-linear history which is atypical for Phab history.