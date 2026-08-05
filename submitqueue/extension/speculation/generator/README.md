# generator

The `generator` package defines the pull-based candidate stream consumed by the default speculation allocator. A generator receives one queue snapshot and yields coherent paths for heads in `BatchStateSpeculating`, ordered according to implementation-owned ranking scores.

Every yielded path contains exactly one assumption for every dependency of its head. Terminal dependency outcomes are fixed facts: succeeded dependencies are assumed to succeed, while failed and cancelled dependencies are assumed to fail. A generator never emits an assumption that contradicts those facts.

The iterator reports exhaustion through its boolean result rather than an error. Both generation and iteration honor context cancellation. Iterators own mutable traversal state and are not safe for concurrent use.

`bestfirst` is the default implementation. It treats unresolved dependency outcomes as independent, obtains their success probabilities from an injected scorer, and lazily merges per-head streams in descending assignment-probability order.
