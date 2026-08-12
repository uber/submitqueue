# pathoverlap

`pathoverlap` is a `conflict.Analyzer` that reports a conflict between two batches when the paths they change share a key.

## Behavior

The files a batch changes are drawn from each change's provider-supplied details, and each path is projected onto a key by the `PathKey` chosen at construction. The candidate batch conflicts with an in-flight batch when their key sets intersect; each such in-flight batch is reported once, preserving the in-flight order. A shared path key is the concrete notion of *target overlap*, so conflicts are classified as `ConflictTypeTargetOverlap`. A batch that changes no files conflicts with nothing, and an empty in-flight list yields no conflicts. A failure to resolve a batch's changes is returned as a (retryable) error.

## Granularity

Two projections ship with the package, selected per queue in the wiring layer:

- **`ByFile`** keys on the whole path, so only batches touching the same file conflict.
- **`ByDirectory`** keys on the path's immediate parent directory, so batches touching sibling files conflict too. Paths at the repository root share the key `.`, which serializes batches that touch any two root-level files.

`ByDirectory` is strictly coarser: every file overlap is also a directory overlap. It trades parallelism for protection against semantic conflicts between neighbouring files — edits that break each other without touching the same file. Which trade is right is a per-queue judgement about how tightly coupled a directory's contents are, so the choice is a construction parameter rather than a property of the analyzer.

Path-key intersection is a deliberately simple notion of overlap. A richer one that needs inputs beyond the changed paths — build targets, ownership boundaries — would be a separate analyzer rather than another `PathKey`.
