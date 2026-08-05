# Best-First Generator

The default speculation path Generator. Every candidate path is a bet: building it pays off only if each of its dependency assumptions matches how that dependency actually resolves. This implementation prices each path by the probability of exactly that, and yields candidates in strictly non-increasing price order.

## Pricing

A path's price is the product, over its dependencies, of the probability that the assumed outcome happens. Each dependency's success probability comes from its state in the snapshot: `Succeeded` is a fact priced 1 and `Failed`/`Cancelled` are facts priced 0, so the contradicting branch is never generated; `Merging` (build passed, hand-off in flight) and `Cancelling` (halted, resurrected only by a lost cancel race) are not facts but are priced as modal certainties 1 and 0, suppressing the near-worthless opposite branch — if the long shot lands anyway, the controller refutes the affected paths and the next run re-plans against the new facts; `Created` and `Speculating` dependencies ask the injected scorer, once per batch per run. Scorer probabilities of exactly 0 or 1 pin the assumption the same way facts do.

Two properties fall out of pure price ordering. Sure work streams first: a head whose dependencies are all decided has exactly one coherent path, priced 1, so real builds always outrank bets. And hedging is automatic: a dependency near probability 0.5 puts both of its branches adjacent in the stream, so a consumer with budget covers the head against either outcome.

## Lazy enumeration

Per head, the modal path takes every undecided dependency at its more likely outcome; every other path is that template with some subset of dependencies flipped, its price scaled by the flip ratios. Subsets are enumerated add/shift-style over flips sorted by descending ratio — each subset generated exactly once, each child priced at or below its parent — and a single max-heap merges all heads. The result is an exact global best-first stream produced in O(log n) per pull, materializing only what is pulled.

Ties are deterministic: equal prices break by fewer flips (closer to the modal path), then by insertion order, and heads are seeded in queue order.

## Coherence

Dependencies are normalized into canonical queue order — ascending batch counter — before paths are built, because `entity.SpeculationPath` hashes the ordered dependencies into the path ID, and IDs must come out identical run after run for the queue to recognize paths it already built. Duplicate dependency entries collapse. A head whose snapshot is incomplete — a dependency missing, itself listed as its own dependency, or a dependency in a state the model does not cover — yields nothing this run and is re-planned when a later run sees a complete snapshot.

See the [Best-First Speculation Generator RFC](../../../../../doc/rfc/submitqueue/speculation-generator.md) for the model, a worked example, and the design trade-offs.
