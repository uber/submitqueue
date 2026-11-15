# Tango conflict analyzer

A `conflict.Analyzer` that decides whether two batches conflict by comparing the build targets each one affects, as reported by a Tango build graph. Two batches conflict when their target sets share an ID, which catches a change that touches code another batch's targets depend on, even when the changed files differ. It reports `ConflictTypeTargetOverlap`.

## Signatures

A batch's signature is the sorted set of its affected targets, each as a 64-bit ID (the first 8 bytes of the SHA-256 of the target label) with its distance from the nearest directly changed source. It is created once, when the batch is first analyzed, and never recomputed: asking Tango again later would use a newer base and could give a different answer. A signature is durable in the signature store, and a byte-bounded in-memory cache in front of it can be dropped at any time at the cost of store reads.

## Behaviour

- Only the batch being analyzed is ever sent to Tango, and only when it has no recorded signature. Redelivery finds the record the earlier attempt created and does not call Tango again.
- The new batch is signed even when nothing is in flight, so later batches find its record.
- An in-flight batch is read from the cache, then the store, and never from Tango. A missing record or a store error fails the analysis (`ErrNoSignature` for the missing record), because a signature computed later could differ from the one the batch was admitted with.
- The analyzer fails closed on any error.
- A queue must have no in-flight batches when it is switched to this analyzer, because batches admitted earlier have no signature.

## Metrics

The analyzer records its metrics under a `tango_conflict` sub-scope of `Params.MetricsScope`, and the client under a `tango_client` sub-scope of `tangoclient.Config.MetricsScope`. A nil scope discards them. Every analyzer metric carries a `queue` tag. Operations follow `platform/metrics`: a `start` counter and a `finish` histogram tagged `result` as `success`, `error` or `cancel`.

- `analyze`: the whole analysis. Its `in_flight` histogram records how many in-flight batches each analysis compared, and `conflicts.total` counts the conflicts reported, tagged `type`.
- `base_revision`: resolving the base revision, which runs only for a batch with no recorded signature.
- `signature.outcomes`: tagged `outcome` as `created`, `reused` when the batch already had a record, or `race_lost` when another attempt created the record first and its record was used. `signature.targets` is a histogram of the number of targets in each new signature, with buckets that reach a million.
- `cache.hit` and `cache.miss`: lookups of in-flight signatures. A new batch's own lookup is not counted. A persistently low hit rate means the cache is too small.
- `no_signature.total`: an in-flight batch had no recorded signature. It fails the analysis, so alert on any non-zero rate. It is expected only when a queue is switched to this analyzer with batches already in flight.
- `changed_targets` on the client, tagged `remote`: the whole Tango call. Alert on a sustained error rate or a rising latency, since every new batch waits on it and a failure fails the analysis.

## Limits

Each signature is computed against trunk at its own analysis time, without the other in-flight batches applied. If one batch adds a dependency on a target that another batch changes, no overlap is reported, the same kind of miss that path overlap has. Adding the destination of each new dependency edge to the signature would close it.

## Injected pieces

The analyzer owns no connection. A deployment supplies the Tango client adapter, which collects the response and resolves each target's label, and a `baserevision.Resolver` that returns the commit SHA of the target branch tip a new batch is analyzed against. The `baserevision` extension holds the contract and implementations that read the branch tip from a git workspace or from the GitHub API. The changes of a batch come from the changeset resolver, the signature store is bound to one queue, and one signature cache can be shared by every queue's analyzer. The MySQL implementations of the signature and dictionary stores are in `mysql/`.

## Reference orchestrator

A queue selects this analyzer with `analyzer: {type: tango, tango: {remote, strategy}}`. `remote` must equal the repository remote Tango is configured with, and `strategy` is required and is `unset`, `shell` or `native`. The top-level `tango` block sets `address`, the Tango gRPC endpoint, and `signatureCacheBytes`, the size of the one signature cache every queue shares. Two limits apply:

- The queue must use the git change provider, because the base revision is read from its clone. GitHub-backed queues are not supported until a base revision can be read from the GitHub API.
- Tango's native strategy resolves changes only from canonical `github://` URIs, while the git change provider emits `git://` URIs, so `native` does not work on a git-provider queue.
