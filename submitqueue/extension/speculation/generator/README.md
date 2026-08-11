# generator

The `generator` package is a piece the `standard` `Speculator` is built from: a `Generator` produces the queue's candidate paths as one ordered stream across all heads. It is **not** controller-facing — the speculate controller only knows the `Speculator` contract, and a different `Speculator` need not split its work this way. So there is no `Config` or `Factory` here; a `Generator` is chosen when the `standard` `Speculator` is constructed.

`Generate` starts the stream over the queue's live batches and returns an `Iterator`. Beyond any ranking work required up front, the generator computes only the candidates the caller pulls. A cancelled or expired context ends the stream with its error. The snapshot must include every batch a head's direct dependencies reference, carry unique non-empty IDs, and give no head an empty, duplicate, or self dependency. Those are the caller's preconditions: a generator may assume them and is not required to detect a breach, so a malformed snapshot yields undefined candidates rather than an error.

Candidates never repeat and never contradict a known fact. Beyond that, the order is the `Generator`'s own: it yields candidates in whatever ranking it implements, and each carries the score it ranked by — higher first, on a scale the generator defines. Consumers take the iterator in the order given and do not interpret the score. Scores mean something only within the run and are never stored.

The generator offers every path in the space, including paths whose builds already ran. Suppressing finished paths is the `Allocator`'s job, since that is the piece reconciling candidates against the stored path sets.
