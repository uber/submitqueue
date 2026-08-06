# bestfirst

`bestfirst` implements `generator.Generator` by ranking candidate paths by the probability that all their dependency assumptions hold. It returns one path per pull across all speculating heads without enumerating every combination up front.

The [best-first speculation path generation RFC](../../../../../doc/rfc/submitqueue/speculation-generator-best-first.md) defines the terminology, algorithm, correctness argument, worked example, and alternatives considered. This README records only the package's operational behavior.

## Behavior

- `Generate` validates the snapshot, scores each unique unresolved direct dependency once, fixes assumptions for resolved dependencies, calculates each head's best score, and seeds the global heap with every eligible head's best-path candidate. Each head's remaining paths wait in that head's own lazy stream, whose flips are worked out only when the head is first handed out.
- `Next` removes the highest-ranked candidate, advances only that head's stream, constructs that candidate's complete path, and returns it. Pulling long enough returns every path exactly once in non-increasing score order.
- Ranking scores are sums of log probabilities, avoiding underflow while preserving probability order. They are meaningful only within the run that produced them.
- Exact ties prefer fewer flips; head ID then decides between heads (the cross-head heap holds one candidate per head), and taken flip indexes decide within a head.
- The snapshot must contain every batch a head's direct dependencies reference; a snapshot missing one — or carrying empty or duplicate batch IDs, or a head with an empty, duplicate, or self dependency — is malformed input and errors instead of opening a stream. Any defaulting for a batch that is hard to score belongs to the scorer, not the generator.

The behavior is covered by `bestfirst_test.go`.
