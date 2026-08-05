# Best-First Speculation Generator

Design of `bestfirst`, the default Generator inside the standard Speculator (see [Speculation](speculation.md)). The Generator's job: given one snapshot of a queue's batches, stream candidate speculation paths in the order most likely to pay off, lazily, so the Allocator can spend the build budget off the top of the stream.

## Model: every path is a bet

A candidate path is a head in `BatchStateSpeculating` plus one assumption — *succeeds* or *fails* — per dependency. Building a path pays off only if every assumption matches how that dependency actually resolves; a build on a broken assumption is refuted by the controller and its CI time is wasted. So the natural ranking of candidates is the probability that the whole bet holds:

```
price(path) = ∏ over dependencies d:   P(d succeeds)   if the path assumes d succeeds
                                       1 − P(d succeeds) if the path assumes d fails
```

Dependency probabilities come from the snapshot state, with the injected `scorer.Scorer` filling in the genuinely undecided ones:

| dependency state | P(succeeds) | kind | effect |
|---|---|---|---|
| Succeeded | 1 | fact | pinned *succeeds*; the *fails* branch is never generated |
| Failed, Cancelled | 0 | fact | pinned *fails*; the *succeeds* branch is never generated |
| Merging | 1 | modal bet | build already passed and hand-off is in flight; opposite branch suppressed |
| Cancelling | 0 | modal bet | halted, only a lost cancel race resurrects it; opposite branch suppressed |
| Created, Speculating | scorer | bet | both branches generated, priced p and 1−p |
| anything else / missing | — | incoherent | the head yields nothing this run (see below) |

Facts honor the Generator contract that no candidate contradicts a resolved outcome. The two modal bets are a pricing policy, not facts: if the long shot lands anyway — a Merging batch whose merge fails, a Cancelling batch that merges because the cancel lost the race — the controller refutes the affected paths and the next run re-plans against the new facts, exactly as with any lost bet. Scorer probabilities of exactly 0 or 1 pin the same way facts do. Scores are fetched at `Open`, at most once per batch per run.

Two properties fall out of pure price ordering, with no special cases:

- **Sure work first.** A head whose dependencies are all decided has exactly one coherent path, priced 1, so real builds always outrank speculation. In a queue with no useful scores (everything near 0.5), deep speculation prices as 0.5^k and sinks — the stream degrades gracefully toward plain dependency-order building.
- **Hedging is automatic.** A dependency near 0.5 puts both of its branches adjacent in the stream. The branches are mutually exclusive, so funding both covers the head against either outcome — the classic small-change-behind-a-big-one bypass builds on the same mechanism (cover the whole outcome space, merge early), and here it simply falls out of the budget reaching that deep.

## Lazy best-first enumeration

A head with k undecided dependencies has 2^k coherent paths; they are never materialized. Per head:

1. The **modal path** takes every undecided dependency at its more likely outcome. Its price is `P₀ = ∏ q_d` where `q_d = max(p_d, 1−p_d)` (ties at 0.5 go to *succeeds*); pinned dependencies contribute certainty and no factor.
2. Every other path is the modal path with a subset S of dependencies flipped: `price(S) = P₀ · ∏_{d∈S} r_d` with flip ratio `r_d = (1−q_d)/q_d ∈ (0,1]`.
3. Flips are sorted by descending ratio (cheapest flip first; queue order on ties) and subsets are enumerated through the **add/shift tree**: the children of a subset whose largest flip index is m are S∪{m+1} (*add*) and S\{m}∪{m+1} (*shift*). Every subset is generated exactly once, and each child's price is at most its parent's.

A single max-heap merges all heads: seed it with every head's modal path, then pop the best entry, materialize it, and push its at most two children. Since children never outrank parents, popped prices are globally non-increasing — the stream is exactly best-first, at O(log n) per pull, with memory proportional to what was pulled. Ties break deterministically: fewer flips first (closer to the modal path), then insertion order; heads are seeded in queue order, so equally priced sure paths stream oldest first.

Example enumeration for one head with undecided dependencies D (q=0.7, r≈0.43) and C (q=0.8, r=0.25), pinned prefix omitted, P₀ = 0.56:

```
∅ ──────────── price 0.56     {C:succeeds, D:succeeds}    (modal)
└─ {D} ──────── price 0.24     {C:succeeds, D:fails}       (add:   ×0.43)
   ├─ {D,C} ─── price 0.06     {C:fails,    D:fails}       (add:   ×0.25)
   └─ {C} ───── price 0.14     {C:fails,    D:succeeds}    (shift: ×0.25/0.43)
```

The tree only guarantees children never beat parents; the heap decides actual emission order (here ∅, {D}, {C}, {D,C}).

## Run scope: the snapshot replaces the bookkeeping

The abstract version of this algorithm needs machinery for a long-lived iterator: result caching, refutation cascades, entry invalidation when a dependency resolves mid-stream. None of that lives here, because the speculate controller reruns from scratch on every dirty signal: each run reads a fresh snapshot, opens a fresh stream, and persists only what it funds. Resolution "cascades" happen for free — the next snapshot simply pins the resolved dependency, every surviving path's price is implicitly reconditioned, and contradicted paths stop being generated at all. Duplicate-work suppression is the Allocator's half: it matches candidates to existing path sets by path ID, keeps in-flight paths in their slots, and skips terminal ones.

That division makes path identity the load-bearing contract. `entity.SpeculationPath` hashes the head and the *ordered* dependency assumptions, so the Generator must emit dependencies in a canonical order or the same logical path would get a new ID every run and be rebuilt. The Generator normalizes dependencies into queue order — ascending batch counter from the documented `<queue>/batch/<counter>` ID format, with a total deterministic fallback for unparsable IDs — and collapses duplicate entries. Given equal snapshots, the whole stream is deterministic regardless of input slice order.

A head whose snapshot cannot support coherent candidates — a dependency missing from the snapshot, listed as its own dependency, or in a state outside the table above — yields nothing this run. Skipping is safe because runs are cheap and self-healing: the head is re-planned the moment a complete snapshot arrives, and proposing nothing is always a valid Generator output.

## Worked example

Queue `q`, five batches, all Speculating. Edges point dependency → dependent; scorer probabilities in parentheses.

```
        b1 (0.9)          b2 (0.5)           ← no dependencies
            \             /      \
             v           v        v
              b3 (0.8)             b4 (0.7)
                     \            /
                      v          v
                   b5 (deps: b1, b2, b3, b4)
```

### Run 1: everything undecided

Seeds, per head: b1 and b2 price 1 (no dependencies); b4 prices 0.5 (one coin-flip dependency); b3 prices 0.9×0.5 = 0.45; b5 prices 0.9×0.5×0.8×0.7 = 0.252. The stream begins:

| # | head | assumptions (S=succeeds, F=fails) | price | note |
|---|------|-----------------------------------|-------|------|
| 1 | b1 | — | 1.00 | sure build |
| 2 | b2 | — | 1.00 | sure build |
| 3 | b4 | b2:S | 0.50 | best bet |
| 4 | b4 | b2:F | 0.50 | its hedge — b4 now covered either way |
| 5 | b3 | b1:S b2:S | 0.45 | |
| 6 | b3 | b1:S b2:F | 0.45 | coin-flip hedge again |
| 7 | b5 | b1:S b2:S b3:S b4:S | 0.252 | |
| 8 | b5 | b1:S b2:F b3:S b4:S | 0.252 | |
| 9 | b5 | b1:S b2:S b3:S b4:F | 0.108 | flips get deeper |
| 10 | b5 | b1:S b2:F b3:S b4:F | 0.108 | |

An Allocator with budget 6 funds rows 1–6: both sure builds, and both branches of b4 and b3 across the b2 coin flip. The stream continues lazily (24 coherent paths total) only if pulled.

### Run 2: b1 succeeded, b2 failed

The next dirty signals bring a snapshot where b1 is Succeeded and b2 is Failed. No entries are repriced and nothing is cancelled *by the Generator* — the controller has already refuted the paths that assumed b2 succeeds, and the fresh stream simply prices the new facts in:

| # | head | assumptions | price | note |
|---|------|-------------|-------|------|
| 1 | b3 | b1:S b2:F | 1.00 | was row 6 of run 1; same path ID, so the Allocator keeps its slot |
| 2 | b4 | b2:F | 1.00 | was the row-4 hedge; now the sure build — already in flight if funded in run 1 |
| 3 | b5 | b1:S b2:F b3:S b4:S | 0.56 | conditioned from 0.252: the b1 and b2 factors became certainty |
| 4 | b5 | b1:S b2:F b3:S b4:F | 0.24 | |
| 5 | b5 | b1:S b2:F b3:F b4:S | 0.14 | |
| 6 | b5 | b1:S b2:F b3:F b4:F | 0.06 | |

b5's space collapsed from 16 paths to 4 — paths contradicting the b1/b2 facts are never generated — and every surviving path kept its ID, so nothing already built is rebuilt. This is the whole reconciliation story: pin, reprice, re-rank, all implicit in re-opening the stream.

## Design notes and rejected alternatives

- **Score the path's own head too?** No: the head's probability of passing does not change whether the *bet on its dependencies* pays off, and a passed build is informative even for a head that will fail. Head-quality weighting belongs to ranking policy evolution (below), not the payoff model.
- **A ranking-score floor (don't propose paths below price X)?** Rejected by the Speculation RFC: budget is the only rationing lever. A cheap hedge is worth a slot that would otherwise idle; the Allocator decides that, not the Generator.
- **Cache scorer results across runs?** The scorer contract already places caching behind the scorer's own interface; the Generator memoizes per run only, keeping runs stateless and reproducible.
- **Metrics/logging in the Generator?** Omitted: `Open` is pure CPU over one snapshot plus scorer calls, and both neighbors (scorer implementations, the speculate controller) already instrument their halves.

## Future refinements

- **Relaxation.** The path model supports *ignored* assumptions (see Conflict relaxation in the Speculation RFC); this Generator does not emit them yet. A relaxation policy slots in as a pre-pass that drops weak dependencies from the flip universe and marks them ignored on the template.
- **Unblocking weight.** Pure price ordering is myopic about information value: resolving a batch that many heads depend on collapses more of the space. A weight like 1 + λ·(unresolved dependents) multiplied into a head's prices would bias the stream toward unblocking without changing the machinery.
- **Sharper priors.** Passed or failed builds of a head's *other* paths are evidence about its dependencies-independent quality; a scorer (or a wrapper) reading recent path outcomes could sharpen probabilities between runs without touching the Generator.
- **Sensitivity pruning.** If conflict analysis can certify that a head's build outcome is independent of one dependency's content, that dependency needs no branch at all — each certified dependency halves a head's path space. This is the strongest practical lever for large closures and composes with relaxation.
