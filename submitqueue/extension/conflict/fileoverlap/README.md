# fileoverlap

`fileoverlap` is a `conflict.Analyzer` that reports a conflict between two batches when they change one or more of the same files.

## Behavior

The files a batch changes are drawn from each change's provider-supplied details. The candidate batch conflicts with an in-flight batch when their changed-file sets intersect; each such in-flight batch is reported once, preserving the in-flight order. A shared file is the concrete notion of *target overlap*, so conflicts are classified as `ConflictTypeTargetOverlap`. A batch that changes no files conflicts with nothing, and an empty in-flight list yields no conflicts. A failure to resolve a batch's changes is returned as a (retryable) error.

File-path intersection is a deliberately simple notion of overlap. A richer one (build targets, ownership boundaries) would be a separate analyzer rather than a change to this one.
