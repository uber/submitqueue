# changeset

`changeset` resolves batch identity into the changes a batch contains. It is the single place the orchestrator walks batch → requests → changes, consolidating the resolution the build, merge, and score controllers each performed privately.

## Why it exists

A `Batch` is a thin reference entity: it carries the IDs of the requests it contains, not their changes. Decision and action extensions (the scorer, build runner, pusher, and future detail-aware conflict analyzers) are handed that identity and resolve the granular content themselves through an injected `Resolver`, rather than depending on a controller to pre-resolve and pass the data in. The resolver depends only on the two resolution-target stores — the request store (to walk a batch's contained requests) and the change store (to attach provider details) — and nothing else.

## Two fidelities

The resolver offers the same walk at two levels of detail, and both preserve batch boundaries — neither flattens across batches, so a caller that wants a flat list flattens the result itself:

- The raw view returns each batch's contained changes as URIs only, one group per input batch, in input order. It performs no change-store read. The build stage uses it for base and head inputs; the merge stage uses it for the pusher.
- The detailed view returns a single batch's normalized, batch-level changes: one entry per claimed URI, each carrying the provider details recorded in the change store, aggregated across every request in the batch. Because the change store returns rows for every request that ever claimed a URI, the resolver selects the row owned by the requesting request. The score stage uses it, as will any analyzer that needs changed-file or line-count facts.

## Testing

A programmable in-memory fake lives in `fake/`: seed per-batch results and inject errors without a real store. A generated mock lives in `mock/` for tests that assert on exact call expectations. Extensions that take a `Resolver` can be exercised against either.
