# Phabricator Support in Runway

## Summary

Runway rejects `phab://` change URIs. This RFC adds support so Phabricator-based repositories can use SubmitQueue end-to-end. The change applies to both Runway operations (merge-conflict-check and merge) since they share one code path.

## Background

The `phab://` URI scheme, the `ChangeProvider` (Conduit), and the routing provider already work. The gap is in Runway's merger — the only component that resolves a change URI into a git commit.

A GitHub URI embeds the commit SHA: `github://{host}/{org}/{repo}/pull/{pr}/{head_sha}`. A Phabricator URI does not: `phab://{host}/D{revision}/{diff_id}`. The DiffID pins the code state, but the commit only exists at a staging ref (`refs/tags/phabricator/diff/{diff_id}`) that Phabricator pushes when staging areas are enabled. The SHA is unknown until fetched.

## Proposal

### 1. `gitworkspace.Workspace` extension

Git operations (fetch, merge, push, rev-parse) are currently embedded in each merger implementation. This RFC extracts them into a shared `platform/extension/gitworkspace/` contract so the same merger logic works with any git backend.

```go
// platform/extension/gitworkspace/

type Workspace interface {
    Exec(commands []Command) ([]Output, error)
    Close() error
}

type Factory interface {
    For(ctx context.Context, repo string) (Workspace, error)
}
```

`Command` carries an `Alias`, `Bin`, `Args`, and optional `Stdin`. `Output` carries the matching `Alias`, `ExitCode`, `Stdout`, and `Stderr`. Commands within one `Exec` call run in order; a non-zero exit code skips the remaining commands in that batch. State persists across `Exec` calls on the same `Workspace`.

Two implementations:

- **OSS** (`platform/extension/gitworkspace/local/`): executes commands as local subprocesses against a git clone, with skip-on-failure semantics.
- **Alternative deployments**: wrap their backend's exec API in the same contract — a thin type-translation adapter.

### 2. `merger.CommitMessageResolver`

Squash and merge commits need a commit message and authorship. The source differs by deployment: the OSS merger builds a synthetic message from the step ID and change label and reads authorship from the git object; other deployments may resolve richer metadata from their code review platform.

```go
// runway/extension/merger/

type CommitMessageResolver interface {
    Resolve(ctx context.Context, uri string) (CommitMessage, error)
}

type CommitMessage struct {
    Message     string
    AuthorName  string
    AuthorEmail string
}
```

The merger takes a `CommitMessageResolver` as a constructor dependency alongside the `gitworkspace.Factory`.

### 3. Unified merger

With `Workspace` and `CommitMessageResolver` injected, the merger becomes a single implementation that builds command batches, sends them to the workspace, and evaluates results. The current OSS git merger and any alternative deployment merger collapse into one codebase. The merger constructor takes both dependencies:

```go
func NewMerger(ws gitworkspace.Factory, cmr merger.CommitMessageResolver, ...) merger.Merger
```

Strategy dispatch (SQUASH_REBASE, REBASE, MERGE, PROMOTE), conflict detection, staleness checking, and result building all live in this single merger — no per-backend duplication.

### 4. `phab://` case in `resolveChange`

The unified merger's URI-to-change resolution adds a `phab` case:

```go
case "phab":
    cid, _ := entityphab.ParseChangeID(uri)
    return changeRef{
        Provider: scheme,
        Ref:      fmt.Sprintf(m.phabStagingRefFormat, cid.DiffID),
        Label:    cid.Revision(), // "D12345"
    }, nil
```

### 5. SHA resolution in `ensureObject`

When SHA is empty, fetch the staging ref and resolve the commit. Both empty is a terminal error:

```go
if ref.SHA == "" && ref.Ref == "" { return error: no SHA and no ref }
if ref.SHA == "" { fetch ref.Ref → resolve SHA → return ref }
// existing by-SHA flow unchanged
```

`ensureObject` returns the `changeRef` with SHA set. `changeRef` stays a value type — resolution returns new values rather than mutating shared state.

### 6. Configurable staging ref format

`PhabStagingRefFormat` defaults to `refs/tags/phabricator/diff/%d`. Validated at startup to contain a `%d` placeholder. Configurable via YAML for non-standard Phabricator layouts.

## Scope, limitations, and prerequisites

**Prerequisites:**
- **Staging areas must be enabled** on the Phabricator instance. Without staging, the ref doesn't exist and the fetch fails terminally (`ErrInvalidRequest`). The staging ref format is configurable via `phabStagingRefFormat` for non-standard layouts.

**Limitations:**
- **No merge-time staleness detection.** For GitHub, `checkStale` catches force-pushes between validate and merge via `ls-remote`. For Phab, each `arc diff` creates a new diff ID with its own immutable staging ref, so this check always passes. Staleness detection must happen upstream at the validate stage via Conduit. If the validate-to-merge window proves problematic, the long-term fix is a pluggable staleness check.
- **Merge strategy.** Phabricator workflows produce linear git history (one commit per change, no merge commits). Queues serving Phab repos should use SQUASH_REBASE or REBASE to match this convention. The MERGE strategy works but produces non-linear history which is atypical for Phab history.
