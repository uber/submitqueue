# SourceControl

Vendor-agnostic interface through which Stovepipe talks to a version control system. It is the **sole owner of URI semantics**: a URI is an opaque, VCS-agnostic locator of a commit. The `git://` scheme used by the reference backend is just one encoding — a Mercurial or Perforce backend mints its own behind the same contract. Nothing outside an implementation parses a URI; it is a token you hand back to ask questions about a ref.

A `SourceControl` is **bound to a single queue** (a repo+ref) when its `Factory` constructs it from a `Config`, so the behavioral methods take no queue argument. Per the repository's extension rules, this package holds the `SourceControl` interface, its `Config`, and the `Factory` *interface* only — concrete `Factory` implementations and the per-queue routing that picks a backend for a `Config.QueueName` live in the wiring layer.

## Behavior

- **Latest** resolves the queue's ref to the URI of its latest commit — the commit a new validation `Request` is minted against during `ingest`.
- **IsAncestor** answers whether one URI is an ancestor of another. The `process` stage uses it to choose a build strategy: if the queue's last-green URI is no longer an ancestor of the latest commit, history was rewritten and a full build is required rather than an incremental one.
- **TrunkPosition** resolves an opaque URI on the queue's ordered trunk to a positive ordinal and a lineage token. Fast-forwards preserve existing ordinals and lineage; an incompatible rewrite starts a never-reused lineage, including for the retained common prefix. Commits outside that ordered trunk return `ErrNotFound`.
- **History** returns a bounded, newest-first page of commit URIs on the ref, using the shared generic `page.Page[string]` (`platform/base/page`). It is paginated with an opaque cursor: callers pass an empty cursor for the newest page and the page's `NextCursor` to walk further back, stopping when it is empty. Pagination keeps a remote backend cheap; callers join the URIs against the request store to render the greenness of each commit.
- **PromotionRef** returns the current commit URI of the queue's promotion ref.
- **Promote** atomically updates the configured promotion ref only when its current URI matches the expected URI. A conflict returns false without changing the ref, so the caller can reload and recheck ownership before retrying. Disabled promotion remains a no-op.

## Errors

Implementations return plain errors and use the package sentinel `ErrNotFound` (with the `IsNotFound` / `WrapNotFound` helpers) when a queue, ref, or URI cannot be resolved. They do not classify errors as user- or infra-caused — the calling controller does that.

`ErrNotFound` from `Promote` specifically means the commit is no longer on the queue's ref, which is a fact about the branch rather than a transient failure: the `record` stage counts and skips it instead of retrying, because no redelivery can put the commit back.

## Implementations

- **fake** — an in-memory backend seeded with a queue's ref history (newest first), with shared promotion state per instance, for examples and tests.

To add a backend, create `sourcecontrol/{backend}/`, implement the `SourceControl` interface, and return it from a `New(...)` constructor.
