# Tango conflict analyzer

A `conflict.Analyzer` that decides whether two batches conflict by comparing the build targets each one affects, as reported by a Tango build graph. Two batches conflict when their target sets share an ID, which catches a change that touches code another batch's targets depend on, even when the changed files differ. It reports `ConflictTypeTargetOverlap`.

## Signatures

A batch's signature is the sorted set of its affected targets, each as a 64-bit ID (the first 8 bytes of the SHA-256 of the target label) with its distance from the nearest directly changed source. It is created once, when the batch is first analyzed, and never recomputed: asking Tango again later would use a newer base and could give a different answer. A signature is durable in the signature store, and a byte-bounded in-memory cache in front of it can be dropped at any time at the cost of store reads.

## Behaviour

- Only the batch being analyzed is ever sent to Tango, and only when it has no recorded signature. Redelivery finds the record the earlier attempt created and does not call Tango again.
- The new batch is signed even when nothing is in flight, so later batches find its record.
- After the attempt that created a signature succeeds, it records each target's label in the dictionary in the background, off the request path, so an ID can be read back as a label. This is best-effort: a failed write is logged and skipped, because conflicts are decided by ID alone and a later batch that touches the label writes it again. On a hash collision the first label recorded stays.
- An in-flight batch is read from the cache, then the store, and never from Tango. A missing record or a store error fails the analysis (`ErrNoSignature` for the missing record), because a signature computed later could differ from the one the batch was admitted with.
- The analyzer fails closed on any error.
- A queue must have no in-flight batches when it is switched to this analyzer, because batches admitted earlier have no signature.

## Limits

Each signature is computed against trunk at its own analysis time, without the other in-flight batches applied. If one batch adds a dependency on a target that another batch changes, no overlap is reported, the same kind of miss that path overlap has. Adding the destination of each new dependency edge to the signature would close it.

## Injected pieces

The analyzer owns no connection. A deployment supplies the Tango client adapter, which collects the response and resolves each target's label, and a function that returns the commit SHA of the target branch tip a new batch is analyzed against. The changes of a batch come from the changeset resolver, the signature store is bound to one queue, and one signature cache can be shared by every queue's analyzer. The MySQL implementations of the signature and dictionary stores are in `mysql/`.
