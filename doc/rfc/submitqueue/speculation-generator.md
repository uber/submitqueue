# Speculation Path Generation

## Summary

The merge queue can build a batch early by assuming whether each batch it conflicts with will succeed or fail. These assumptions create many possible build paths, but CI can run only a few. The default generator returns the most likely paths first and stops when the build budget is full.

It does that by starting from each batch's single most likely path and generating every other path as a deviation from it, most probable next, across all batches at once. Paths come out in score order, and only the paths actually requested are ever created. In the architecture of [speculation.md](speculation.md), this is the Generator inside the default Speculator. This RFC describes that design and the alternatives considered and rejected.

## The problem

A batch with n undecided dependencies has one possible path per combination of outcomes — 2ⁿ in all. A batch with ten has 1,024 possible futures, and CI can afford perhaps two or three of them. The rest of the system asks the generator for candidates one at a time, best first, and stops asking once the build budget is full.

So the generator must return paths in order of how likely they are to be useful, never return the same path twice, never return a path that contradicts a known outcome, and do work proportional to the number of paths requested — not to 2ⁿ.

## The queue in this doc

One worked example runs through the whole document. Four batches are in flight — A, B, and C all conflict with each other, and D stands alone:

```
main ◀── A ◀── B        B conflicts with A
         ▲     ▲
         └─ C ─┘        C conflicts with both A and B
    D                   D conflicts with nothing
```

This is the **DAG**: the queue's batches and the dependency **edges** between them — a direct conflict makes the earlier batch a **dependency** of the later one. Each batch stores exactly its own dependencies, fixed when the batch is created: A and D store none, B stores `[A]`, C stores `[A, B]`. Dependencies are direct conflicts only — nothing inherited: a batch that conflicted only with B would carry no assumption about A. A could still delay that batch's merge, but only through B, and that is the merge stage's business ([speculation.md](speculation.md)), not the generator's.

A pluggable scorer estimates how likely each batch's build is to succeed. Only batches that appear in someone's dependency list are ever asked about — A and B here; nothing depends on C or D, so their probabilities never matter:

| dependency | P(succeeds) |
| --- | --- |
| A | 0.9 |
| B | 0.8 |

## The algorithm

A **path** is one assumption — *succeeds* or *fails* — per dependency of the batch being evaluated. (speculation.md calls that batch the path's *head*.) Its **score** is the probability that every assumption comes true, by plain multiplication: start at 1, multiply by p for each *succeeds*, by 1−p for each *fails*. Drawn as a tree, one branching level per dependency in queue order, every leaf is one complete path and its score is the product collected on the way down:

```
 batch A — no dependencies     batch B — dependencies [A]      batch C — dependencies [A, B]

      start 1.0                     start 1.0                           start 1.0
                                ┌───────┴───────┐                 ┌──────────┴──────────┐
   already a leaf:           A✓ ×0.9         A✗ ×0.1          A✓ ×0.9               A✗ ×0.1
   one path, assume             │               │                 │                     │
   nothing, score 1            0.9             0.1               0.9                   0.1
                              leaf            leaf         ┌──────┴──────┐       ┌──────┴──────┐
 batch D — same as A:                                   B✓ ×0.8      B✗ ×0.2  B✓ ×0.8      B✗ ×0.2
 one path, score 1                                          │            │        │            │
                                                          0.72          0.18     0.08         0.02
```

A has no dependencies, so there is nothing to assume: one path, `[]`, score 1 — and D the same. B has two paths, C has four. A path's identity is its batch plus its assumptions, and the stored dependency list is fixed at creation, so a path's identity never changes across runs. Nobody builds these trees up front — the algorithm hands out their leaves in score order without drawing the whole picture.

**A run sets up three things, each computed exactly once:**

1. **One probability per unique dependency.** The scorer is asked once per dependency however many lists it appears in — A is in both B's and C's lists but costs one call.
2. **Each batch's most likely path and its score, the batch's best score.** Every dependency has a **preferred** assumption — the more probable of succeeds and fails (at exactly 0.5, succeeds) — and an **unpreferred** one. The most likely path takes every dependency's preferred assumption — note that is not "everything succeeds": a dependency that will probably fail is assumed to fail. C's most likely path is `[A✓ B✓]` and its best score is 0.9 × 0.8 = 0.72. This is the one cost that cannot be deferred — ranking batches against each other needs every batch's best score before the first request.
3. **Each dependency's flip cost.** A **flip** turns one assumption to its unpreferred side, and always costs the same fixed ratio — unpreferred over preferred, at most 1 — no matter what else is flipped: flipping B multiplies a score by 0.2/0.8 = 0.25, flipping A by 0.1/0.9 ≈ 0.111. Each batch's flips sort cheapest-first, lazily — a batch nobody asks variants of never sorts them. Any path's score is then the batch's best score times the ratio of each flip it takes: `[A✗ B✓]` is 0.72 × 0.111 = 0.08.

**Then every request repeats two steps** against one list of paths ordered by score — a priority queue — that starts holding just each batch's most likely path:

1. Take the highest-scoring path off the list and return it.
2. Before returning it, put its follow-ups on the list — at most two moves: **add** the next-cheapest flip on top of the path's flips, and — only if the path already has flips — **trade** its newest flip for the next-cheapest one.

Every flip combination is produced by exactly one move from exactly one path, so nothing ever repeats. Walk the two moves on C — full assumptions at every step, flips sorting B (×0.25) then A (×0.111):

```
[A✓ B✓]  0.72        the most likely path — no flips yet
└── [A✓ B✗]  0.18    add the cheapest flip: B✓ becomes B✗
    ├── [A✗ B✗]  0.02    add the next flip on top: A✓ becomes A✗ too
    └── [A✗ B✓]  0.08    trade the newest flip for the next: B✗ back to B✓, A✓ becomes A✗
```

And the whole queue, one request at a time (the two 1.0 paths order by batch ID, per the ties rule):

| request | returned (score) | follow-ups put on the list | the list afterwards |
| --- | --- | --- | --- |
| 1 | **A: `[]`** (1.0) | — (nothing to flip) | `D []` 1.0 · `B [A✓]` 0.9 · `C [A✓B✓]` 0.72 |
| 2 | **D: `[]`** (1.0) | — | `B [A✓]` 0.9 · `C [A✓B✓]` 0.72 |
| 3 | **B: `[A✓]`** (0.9) | `B [A✗]` (0.1) | `C [A✓B✓]` 0.72 · `B [A✗]` 0.1 |
| 4 | **C: `[A✓ B✓]`** (0.72) | `C [A✓B✗]` (0.18) | `C [A✓B✗]` 0.18 · `B [A✗]` 0.1 |

Four candidates out — the unconditional builds of A and D, B on top of A, C on top of both — in exactly the order a human would fund them. If the caller kept asking: request 5 returns `C [A✓B✗]` (0.18) and queues its two variants `C [A✗B✗]` (0.02) and `C [A✗B✓]` (0.08); then 0.1, 0.08, 0.02 follow, descending all the way.

The point is what happens when the caller *stops*. After four requests, exactly four paths have been returned and only two more exist anywhere — the two on the list. C's leaves 0.08 and 0.02 were never created; a batch nobody pulls from never even works out its flip order. Generating k paths creates k paths; the 2ⁿ space is never enumerated.

The guarantees, each with its one-line reason:

- **Candidates come out in non-increasing score.** A variant only ever adds or trades up to costlier flips, so it never outscores the path it came from; taking the highest entry first means no later path can beat an earlier one.
- **Every leaf exactly once.** Each variant is created by exactly one move from exactly one path, so continuing until the list is empty returns every batch's every path, none twice.
- **Work is proportional to what is requested.** Each request returns one path and puts at most two on the list.
- **Impossible assumptions are not silently dropped.** A side with probability 0 makes its flip ratio 0 — the path scores 0 and sorts after every possible one, but is still returned if the caller keeps asking. Nothing is discarded.

## One implementation note: scores are stored as logarithms

Everything above multiplies probabilities, and that ranking is exactly what the implementation preserves — but it cannot multiply literally. A batch with 7,100 dependencies at 0.9 each has a best score of 0.9⁷¹⁰⁰ ≈ 10⁻³²⁵, smaller than the smallest number float64 can represent: the product rounds to exactly 0, every path of that batch ties at zero, and the order is lost.

So the implementation stores every score as its logarithm, which turns multiplication into addition:

```
log(p₁ × p₂ × ⋯ × pₙ) = log p₁ + log p₂ + ⋯ + log pₙ
```

Because log is strictly increasing, comparing the logs orders paths exactly as comparing the products would — and the sums stay comfortably finite: that wide batch's best value is 7,100 × log 0.9 ≈ −748, an ordinary float64. On C: 0.72 becomes log 0.9 + log 0.8 = −0.328, and a flip's ratio becomes a subtraction (B's flip costs log 0.2 − log 0.8 = −1.386, taking −0.328 to −1.715, and e^−1.715 ≈ 0.18 — the score we started with). The log is what a candidate carries out of the generator as its **ranking score**; it orders candidates within one run and means nothing across runs — the next run rescores from scratch.

## Example across two runs

The generator keeps no state between runs: each run starts from the queue's current state, and what changed since last time shows up as fewer assumptions to make. (What the rest of the system does with the candidates — funding, cancelling, merging — is covered in [speculation.md](speculation.md).)

**Run 1.** The queue is as above; the four most probable paths are the ones the trace returned:

```
A: []  1.0   ·   D: []  1.0   ·   B: [A✓]  0.9   ·   C: [A✓ B✓]  0.72
```

**Between runs.** A's build fails. A had one unconditional path, so no other future exists in which it passes: A's outcome is now known.

**Run 2.** A is no longer a batch being evaluated, so it proposes nothing. As a *dependency* it is now a fact, not an assumption: every path fixes A to the known outcome (*fails*), and A's level vanishes from the trees — the paths still record A✗, but nothing branches on it anymore:

```
 batch B — nothing left to assume        batch C — only B still undecided

     [A✗]  1.0                                    start 1.0
                                            ┌────────┴────────┐
  A's failure is a fact;                 B✓ ×0.8           B✗ ×0.2
  the tree is a single leaf                 │                 │
                                       [A✗ B✓] 0.8       [A✗ B✗] 0.2
```

The first candidates returned are `B: [A✗]` and `D: []`, both at score 1.0, then `C: [A✗ B✓]` at 0.8. Look at B's closely: it is the *same path* that scored 0.1 in run 1 — the assumptions are identical, so its identity is identical — but what was a long shot is now a certainty, because A's failure stopped being a probability and became a fact. Scores mean nothing across runs; a path's identity is what survives, which is how the rest of the system recognizes paths it already acted on. And no path returned in run 2 contradicts the known outcome: every path assuming A✓ is simply no longer generated.

## Edge cases and ties

Paths make live assumptions only about what is genuinely undecided; everything else is settled before generation starts.

- **A dependency whose outcome is known** is fixed to it — Succeeded fixes *succeeds*; Failed or Cancelled fixes *fails* — and drops out of the search, exactly as A did in run 2. The fixed assumption is still recorded on every path, so paths stay self-contained. A dependency that is still being cancelled is not yet known and stays a live assumption.
- **A dependency absent from the run's snapshot** cannot be scored. Each run hands the generator the batches the controller read, and a stored dependency ID with no batch among them has nothing to score. It stays a live assumption with a fixed default probability of succeeding, 0.95, a constant in the generator chosen on the observation that nearly every batch a queue accepts does build successfully; it only affects ranking, never which paths exist.
- **Exact ties need a fixed order**, or two runs over the same queue could propose different paths. Ties break, in order: higher score, then fewer flips, then which flips were taken, then the batch's ID. Fewer-flips comes before batch ID so that every batch's most likely path is offered before any batch's equally-scored deviation: a dependency at exactly 0.5 flips for free, and ordering on the batch instead would let one batch's tied deviations absorb the whole budget before another batch's most likely path was offered at all. Two batches each with one dependency at 0.5 therefore return as *first batch's most likely path, second batch's most likely path, first's flip, second's flip* — the batches interleave. Because every combination reaches the list exactly once and the order is total, every step has a unique winner and repeated runs agree.

## Alternatives considered

**Enumerate and sort.** Compute every path of every batch, sort by score, return from the top. Simplest possible model, but the work is 2ⁿ per batch per run whether or not anyone asks for more than two paths. Rejected for cost.

**A forward decision tree.** Keep *partial* paths on the shared list and split the highest-scoring one at its next level — one copy assuming succeeds, one assuming fails — until complete paths fall out the bottom. The state is arguably more intuitive (an entry on the list *is* a partial path with a real score), but the formulation only makes sense walking a batch's chain from its top, and that requires the full transitive closure of its dependencies — not the direct conflicts batches actually store. The closure is exactly what makes it slow: every inherited level doubles a batch's leaves, adding speculation paths about batches it never touches — paths that cannot change its build, yet still get generated, ranked, and considered for budget. Nor can the closure be derived on the fly: walking the DAG fresh each run means the derived set changes shape as links finalize, are cancelled, or drop out of the snapshot, and since a path's identity is the hash of its full assumption list, the same logical path could change identity from one run to the next — breaking the matching that lets an already-funded path keep its CI slot instead of being rebuilt — while the controller's validation and snapshot reads would all have to walk the same chains the same way. Rejected: it needs data we deliberately do not keep, and it creates paths nobody needs. The chosen design sidesteps all of it — a path covers exactly the batch's stored dependency list, fixed at creation.
