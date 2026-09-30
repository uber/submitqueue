# Phabricator Support in Runway

## Summary

Runway rejects `phab://` change URIs. This RFC adds support so Phabricator-based repositories can use SubmitQueue end-to-end. The change applies to both Runway operations (merge-conflict-check and merge) since they share one code path.

## Background

The `phab://` URI scheme, the `ChangeProvider` (Conduit), and the routing provider already work. The gap is in Runway's git merger — the only component that resolves a change URI into a git commit.

A GitHub URI embeds the commit SHA: `github://{host}/{org}/{repo}/pull/{pr}/{head_sha}`. A Phabricator URI does not: `phab://{host}/D{revision}/{diff_id}`. The DiffID pins the code state, but the commit only exists at a staging ref (`refs/tags/phabricator/diff/{diff_id}`) that Phabricator pushes when staging areas are enabled. The SHA is unknown until fetched.

## Proposal

### 1. `phab://` case in `resolveChange`

`resolveChange` becomes a method on `gitMerger` so the phab case can read the configured staging ref format:

```go
case "phab":
    cid, _ := entityphab.ParseChangeID(uri)
    return changeRef{
        Provider: scheme,
        Ref:      fmt.Sprintf(m.phabStagingRefFormat, cid.DiffID),
        Label:    cid.Revision(), // "D12345"
    }, nil
```

### 2. SHA resolution in `ensureObject`

When SHA is empty, fetch the staging ref and resolve the commit. Both empty is a terminal error.

```go
// ensureObject — new dispatch at the top:
if ref.SHA == "" && ref.Ref == "" { return error: no SHA and no ref }
if ref.SHA == "" { return resolveObjectFromRef(ref) }
// ... existing by-SHA flow unchanged ...

// resolveObjectFromRef:
git fetch <remote> <ref.Ref>
ref.SHA = git rev-parse FETCH_HEAD
return ref
```

### 3. Value-returning signatures

`changeRef` is a value type. The merger copies refs into a flattened slice for `ensureObjects`/`checkStale`, but `applySteps` reads the originals — a SHA resolved on the copy never reaches the apply paths.

```go
// Before — ensureObjects returns nothing, resolved SHAs lost:
ensureObjects(refs []changeRef) error

// After — resolution returns values, caller reassigns:
ensureObject(ref changeRef) (changeRef, error)
ensureObjects(refs []changeRef) ([]changeRef, error)
ensureStepObjects(steps []resolvedStep) ([]resolvedStep, error)  // new

// applyTransforming:
steps, _ = m.ensureStepObjects(ctx, steps)   // reassign with resolved SHAs
m.checkStale(ctx, stepChangeRefs(steps))     // sees resolved SHAs
// tryApply uses the resolved steps
```

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
- **No merge-time staleness detection.** For GitHub, `checkStale` catches force-pushes between validate and merge via `ls-remote`. For Phab, each `arc diff` creates a new diff ID with its own immutable staging ref, so this check always passes. Staleness detection must happen upstream at the validate stage via Conduit. If the validate-to-merge window proves problematic, the long-term fix is a pluggable staleness check.
- **Merge strategy.** Phabricator workflows produce linear git history (one commit per change, no merge commits). Queues serving Phab repos should use SQUASH_REBASE or REBASE to match this convention. The MERGE strategy works but produces non-linear history which is atypical for Phab history.
