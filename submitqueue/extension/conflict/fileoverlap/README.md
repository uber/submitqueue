# fileoverlap

`fileoverlap` is a `conflict.Analyzer` that reports a conflict between two batches when they change one or more of the same files.

It is the first analyzer to exercise the capability the [extension contract](../../../../doc/rfc/submitqueue/extension-contract.md) unblocks: it is handed only batch identity (the candidate batch and the in-flight batches) and resolves each batch's changed files itself through an injected `changeset.Resolver`, rather than depending on a controller to pre-compute them. This is why the `conflict.Analyzer` contract takes identity and resolves internally — a file-overlap analyzer could not be written against a controller-resolved, identity-only batch.

## Behavior

The files a batch changes are drawn from each change's provider-supplied details. The candidate batch conflicts with an in-flight batch when their changed-file sets intersect; each such in-flight batch is reported once, preserving the in-flight order. A shared file is the concrete notion of *target overlap*, so conflicts are classified as `ConflictTypeTargetOverlap`. A batch that changes no files conflicts with nothing, and an empty in-flight list yields no conflicts. A failure to resolve a batch's changes is returned as a (retryable) error.

File-path intersection is a deliberately simple notion of overlap. A richer one (build targets, ownership boundaries) would be a separate analyzer rather than a change to this one.
