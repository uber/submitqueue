# changeset

`changeset` resolves batch identity into the changes a batch contains. Current consumers include build-runner and scorer implementations and the path-overlap conflict analyzer. The merge controller loads member requests directly because its Runway payload preserves one ordered merge step per request.

## Why it exists

A `Batch` is a thin reference entity: it carries the IDs of the requests it contains, not their changes. Decision and action extensions such as scorers, build runners, and conflict analyzers are handed that identity and resolve the granular content themselves through an injected `Resolver`, rather than depending on a controller to pre-resolve and pass the data in. The resolver uses the queue-scoped storage aggregate to reach the request store (to walk a batch's contained requests) and the change store (to attach provider details).

## Two fidelities

Both methods operate on one batch per call:

- The raw view returns a batch's contained changes as URIs only, in request order. It performs no change-store read. Build-runner implementations use it to construct base and head inputs.
- The detailed view returns a single batch's normalized, batch-level changes: one entry per claimed URI, each carrying the provider details recorded in the change store, aggregated across every request in the batch. Because the change store returns rows for every request that ever claimed a URI, the resolver selects the row owned by the requesting request. Scorers and detail-aware conflict analyzers use this view.

## Testing

A programmable in-memory fake lives in `fake/`: seed per-batch results and inject errors without a real store. A generated mock lives in `mock/` for tests that assert on exact call expectations. Extensions that take a `Resolver` can be exercised against either.
