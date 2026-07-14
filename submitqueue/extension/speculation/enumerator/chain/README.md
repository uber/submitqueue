# Chain Enumerator

Given a batch and its active dependency batches in arrival order, the chain `enumerator.Enumerator` produces exactly one candidate path: the batch built directly on top of the full chain of those dependencies, in the order given. It never branches, so a batch's tree only ever contains this single path. An empty dependency list still yields one path, whose base is empty — the batch built directly on the target branch.

This is deliberately the least interesting enumerator: it does not weigh which dependencies to include or exclude, does not consider dropping a shaky predecessor, and does not produce a "build alone" alternative alongside the chained one. It is the working, deterministic single-chain baseline — a richer enumerator (for example, one that also offers a build-alone fallback path per dependency) can replace or supplement it without changing the contract.

As with any `enumerator.Enumerator`, the returned path carries structure only — a Base/Head split. The controller assigns its identity, stamps its status, and has the scorer fill its score.
