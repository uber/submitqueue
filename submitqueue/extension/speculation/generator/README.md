# Speculation Generator

The candidate-stream composition point inside the standard Speculator. A Generator turns one snapshot of a queue's batches into a pull-based stream of candidate speculation paths — each an `entity.CandidatePath`: a head in `BatchStateSpeculating`, one assumption per dependency in queue order, and the transient ranking score the stream is ordered by.

This is not a controller-facing extension. The speculate controller depends only on the Speculator contract; the default Speculator opens a Generator's stream and hands it to an Allocator, and an alternate Speculator need not use or expose this seam. See the [Speculation RFC](../../../../doc/rfc/submitqueue/speculation.md) for the composition and the [Best-First Speculation Generator RFC](../../../../doc/rfc/submitqueue/speculation-generator.md) for the default ranking policy.

The stream is lazy: beyond what ranking requires up front, a Generator computes only what the consumer pulls, so the path space — up to two branches per undecided dependency per head — is never materialized. Candidates never repeat and never contradict a resolved fact: a dependency that already succeeded is never assumed to fail, and one that already failed or was cancelled is never assumed to succeed. The batches slice is a caller-owned snapshot the Generator may retain but never mutates; a queue that has moved on is a new snapshot and a new stream. Both `Open` and `Next` abort on a cancelled context.

## Implementations

- [bestfirst](bestfirst/README.md) — the default: prices every path by the probability that all of its assumptions hold, and yields candidates lazily in exactly that order.
