# Best-First Speculation Path Generation

## Summary

The default generator returns the most likely complete build path across the whole queue, one path per call.

The code has one setup method, `Generate`, and one repeated method, `Next`:

1. `Generate` records a preferred assumption and the cost of flipping to its opposite for each unresolved direct dependency. It totals the best score for each eligible head; resolved dependencies remain fixed facts.
2. `Generate` pushes one lightweight best-path candidate per head into a global heap. Each head also owns a stream that enumerates its remaining paths on demand; at this point the stream has done no work beyond that total.
3. `Next` removes the highest-ranked candidate from the global heap, advances only that head's stream, inserts the head's next candidate, and constructs and returns the complete path that was removed.

The global heap always contains only the current best candidate from each head, so removing its top is a k-way merge over per-head best-first streams: it yields the best path across the whole queue however the heads interleave, without ever comparing more than one candidate per head.

This RFC walks through those steps in the same order as the code. It documents the existing implementation and does not propose a code change. The generator only ranks and returns paths. The allocator maps those paths to statuses and stops calling `Next` when the build budget is full. See [speculation.md](speculation.md) for that allocation flow.

## Running example

A, B, and C all conflict directly with one another. D has no conflicts:

```
C ──▶ B ──▶ A
└──────────▶ A

D
```

Each arrow points from a batch to a direct dependency:

```
A.Dependencies = []
B.Dependencies = [A]
C.Dependencies = [A, B]
D.Dependencies = []
```

C contains A because C directly conflicts with A, not because B depends on A. The generator uses these stored direct dependencies and does not compute a transitive closure.

The scorer estimates:

| Dependency | Success | Failure |
| --- | ---: | ---: |
| A | 0.9 | 0.1 |
| B | 0.8 | 0.2 |

The batch being built is written before its assumptions. For example, `C [A succeeds, B fails]` means “build C assuming A succeeds and B fails.” The code calls C the path's **head**.

## The snapshot is a caller precondition

`Generate` receives the queue's live batches as a snapshot and takes it as given. A well-formed snapshot carries unique, non-empty batch IDs, includes every batch a head's direct dependencies reference, and gives no head an empty, duplicate, or self dependency. Those are preconditions the caller owns, established where the snapshot is assembled. The generator does not re-check them: it is on the hot path of every run, the checks it could make are the ones an assembled-correctly snapshot can never fail, and paying for them here only spreads the same contract across two places. A malformed snapshot yields undefined candidates rather than an error.

A dependency the generator cannot price is the one bad input it absorbs, because it arrives from the injected scorer rather than from the caller and there is no earlier point that could catch it. Three cases take the same 0.95 default: a score outside `[0, 1]` or `NaN`, a scorer call that returned an error, and a dependency the snapshot never carried. The default is optimistic on purpose, so a dependency nobody could estimate keeps its head's preferred path near the front instead of burying it or failing the whole run on one number — and failing the whole run is the real hazard, because `Generate` seeds the heap for every head at once, so one unpriceable dependency would otherwise cost the queue every candidate it had. A batch absent from the snapshot is never passed to the scorer at all: it would resolve to a zero-valued batch belonging to no queue, so scoring it would price some other batch entirely or fail on the empty queue name. Any deliberate defaulting still belongs to the scorer implementation, which knows what information it does and does not have; this is only the floor under it.

## Step 1: `Generate` prepares each head

`Generate` scores each unique unresolved dependency once, however many heads wait on it. If A appears in both B's and C's dependency lists, the scorer is still called for A only once.

For each unresolved direct dependency, `Generate` records:

- the **preferred assumption** — the more likely outcome, choosing succeeds on an exact tie;
- the score contributed by the preferred assumption; and
- `flipCost`, the score change for taking the opposite assumption instead.

### Choose the preferred assumption

For A, success is preferred because it is more likely:

```
preferred: success at 0.9
opposite:  failure at 0.1
```

For B:

```
preferred: success at 0.8
opposite:  failure at 0.2
```

If failure were more likely, failure would be preferred. If both outcomes were 0.5, the implementation would prefer success. The opposite assumption is not stored anywhere: it is always the other side of the preferred one, so a path that flips a dependency computes it on the spot.

### Calculate `flipCost`

A flip replaces the preferred probability already included in the best path with the opposite probability. If the other dependencies contribute `other factors`, then:

```
old probability = other factors × preferred probability
new probability = other factors × opposite probability
```

For B, changing success to failure does this:

```
old probability = other factors × 0.8
new probability = other factors × 0.2
                = old probability ÷ 0.8 × 0.2
```

Equivalently:

```
new probability = old probability × (opposite / preferred)
```

The intuitive probability multiplier for flipping B is:

```
0.2 / 0.8 = 0.25
```

The implementation stores logarithmic scores, so multiplication becomes addition. Its stored `flipCost` is:

```
flipCost = log(opposite) - log(preferred)
         = log(opposite / preferred)
```

The equivalent score update is:

```
new score = old score + log(opposite) - log(preferred)
          = old score + flipCost
```

The implementation does not repeatedly divide and multiply a running probability. For stable floating-point results, it recomputes every path score from the head's best score plus the `flipCost` of each taken flip:

```
path score = best score + sum of taken flipCost values
```

For the example:

| Dependency flipped | Probability multiplier | Stored `flipCost` |
| --- | ---: | ---: |
| B | `0.2 / 0.8 = 0.25` | `-1.386` |
| A | `0.1 / 0.9 ≈ 0.111` | `-2.197` |

A value closer to zero loses less probability and is therefore a cheaper flip.

### Total each head's best score

`Generate` chooses the preferred assumption for every unresolved dependency and combines their probabilities. Resolved dependencies contribute probability 1 because their assumptions are fixed facts.

```
A []                         = 1.0
B [A succeeds]               = 0.9
C [A succeeds, B succeeds]   = 0.9 × 0.8 = 0.72
D []                         = 1.0
```

In code, these products are sums of logarithms. Conceptually they are the same probabilities shown above.

At this point, `Generate` has each head's best score and the information needed to create flips. It has not constructed every full path, and it has not sorted any head's flips; that work waits until the head's stream first advances.

## Step 2: `Generate` seeds the global heap

`Generate` pushes one lightweight candidate for every eligible head into the global heap:

```
candidate = head + no flips + best score
```

The head's stream and its candidate hold different information:

```text
stream for C
├── dependencies: A and B
├── best score: 0.72
├── sorted flips: not prepared yet
└── local heap: empty

candidate for C in the global heap
├── head: C
├── taken flips: none
└── score: 0.72
```

The candidate can say “no flips” without a sorted flip list. It means the best path: use the preferred assumption for every dependency. `Generate` therefore does not need to prepare or sort C's flips.

The starting global heap, shown in ranked order for readability, is:

```
┌───────────────────────────────────────┐
│ NEXT  A []                       1.00 │
│       D []                       1.00 │
│       B [A succeeds]             0.90 │
│       C [A succeeds, B succeeds] 0.72 │
└───────────────────────────────────────┘
```

The actual heap is not fully sorted. It guarantees only that the highest-ranked candidate can be removed next.

The candidate is a compact description of a path, not a constructed `SpeculationPath`. The full path is built only when `Next` returns it.

## Step 3: `Next` returns one path

Every call follows the same loop:

```
remove the highest-ranked candidate from the global heap
          │
          ▼
advance that head's stream:
    prepare and sort the head's flips if this is its first advance
    remove the best flip subset from the head's local heap
    push that subset's extend and swap successors into the local heap
          │
          ▼
push the removed subset into the global heap as the head's next candidate
          │
          ▼
construct and return the complete path for the candidate removed first
```

In pseudocode:

```
Next():
    current = globalHeap.removeHighest()

    next, ok = current.stream.advance()
    if ok:
        globalHeap.push(next)

    return buildPath(current.head, current.takenFlips)

stream.advance():
    prepareAndSortFlipsIfFirstAdvance()      # also seeds the local heap with the cheapest single flip

    if the local heap is empty:
        return nothing                       # the head is exhausted and is not reinserted

    subset = localHeap.removeBest()
    if a flip past subset's last exists:
        localHeap.push(subset with the next flip added)     # extend
        localHeap.push(subset with its last flip traded)    # swap
    return subset
```

On C's first advance, the stream prepares and sorts the flip list owned by C:

```text
stream for C
├── flip 0: flip B, cost -1.386
└── flip 1: flip A, cost -2.197
```

It then seeds the local heap with the subset that takes only flip 0 — the best path's lone successor — and immediately removes it as the advance's result, pushing its extend and swap successors behind it:

```text
candidate just returned              C's next candidate in the global heap
head: C                              head: C
taken flip indexes: []               taken flip indexes: [0]
meaning: no flips                    meaning: flip B
path: C [A succeeds, B succeeds]     path: C [A succeeds, B fails]
score: 0.72                          score: 0.18
```

The stream owns the sorted definitions of flips 0 and 1. Candidates and subsets share those definitions and store only the indexes they take. No subset with a taken flip index exists before the stream's flip list is prepared.

### Sort a head's flips

The first time a head's stream advances, its flips are sorted from cheapest to most expensive.

In the probability-product view, a multiplier closer to 1 is cheaper because it preserves more of the path's probability:

```
cheapest = closest to 1

1.0 ───── 0.25 ───── 0.111 ───── 0
           B flip      A flip       impossible outcome
```

The logarithm maps 1 to 0. In the stored-score view, a `flipCost` closer to 0 is therefore cheaper:

```
closest to zero = cheapest
          │
          ▼
0 ───── -1.386 ───── -2.197 ───── -∞
          B flip       A flip       impossible outcome
```

These are the same ordering:

```
larger probability multiplier  = less probability lost = cheaper flip
larger logarithmic flipCost    = closer to zero         = cheaper flip
```

For C:

```
flip 0: B, cost -1.386
flip 1: A, cost -2.197
```

Equal costs keep dependency order.

A flip's **index** — the 0 and 1 above — is its position in this cheapest-first list. The sort scrambles queue order, so each flip also records its `dependencyIndex`, the dependency's position in the head's stored dependency list; that is how a built path knows which slot the flip rewrites.

This order matters because the walk moves forward through the flip list. Trading B's cheaper flip for A's more expensive flip lowers probability:

```
C [A succeeds, B fails] 0.18
└── trade B for A ──▶ C [A fails, B succeeds] 0.08
```

If A came first, the trade would move from 0.08 to 0.18. A successor would then be better than its parent, and the heaps could return paths in the wrong order.

### Extend and swap

After sorting, C's flip list is:

```
first:  flip B   cheaper
second: flip A   more expensive
```

The stream walks this list in order. It starts with no flips, so the first available change is flip B:

```
current path: C [A succeeds, B succeeds] 0.72
current flips: none

next flip:    flip B
successor:    C [A succeeds, B fails] 0.18
```

When that B-flipped path later leaves the local heap, the next flip in the sorted list is A. There are two ways to introduce it:

- **Extend:** keep the B flip and also flip A. The result flips both A and B.
- **Swap:** trade the B flip for A's. The result flips only A.

```
extend:
    current flips B + next flip A
    → C [A fails, B fails] 0.02

swap:
    trade current flip B for next flip A
    → C [A fails, B succeeds] 0.08
```

C's complete tree is therefore:

```
C [A succeeds, B succeeds] 0.72             no flips
└── flip B ──▶ C [A succeeds, B fails] 0.18   B
    ├── extend ──▶ C [A fails, B fails] 0.02       B and A
    └── swap ──▶ C [A fails, B succeeds] 0.08   A
```

The tree's four paths are the four possible combinations: no flips, B only, A and B, and A only. None repeats.

The best path does not create the A-only path directly. That path is reached later by swapping B for A. Giving it both routes would create a duplicate. Walking flips in sorted order with extend/swap gives every combination one route through the tree.

The stream pushes both the extend and swap subsets into the head's local heap. It does not choose between 0.02 and 0.08 now — a later advance takes whichever is better, and the global heap decides when the head is due at all.

### Two heaps, one invariant

The global heap contains only the current best candidate from each head with paths left. The head's local heap holds the flip subsets reached but not yet handed out — the paths queued behind that candidate.

Advancing a stream maintains both at once. The subset removed from the local heap becomes the head's next candidate in the global heap, and its extend and swap successors — the only subsets whose parent has just been consumed — are pushed into the local heap, so the local heap's best is always the head's next path after its current candidate.

That invariant is why removing the global top is safe: every path not yet handed out is either some head's current candidate or ranks at or below its own head's current candidate — the standard k-way merge over already ordered streams. A head with nothing left is simply not reinserted, which is how the merge learns a stream is exhausted.

## Full walkthrough

The first two calls return A and D. Neither has a dependency, so their streams have nothing more and neither is reinserted.

The third call returns B's best path; B's stream produces its flipped path as B's next candidate:

```
return B [A succeeds] 0.9
insert B [A fails]    0.1
```

The fourth call returns C's best path. C's stream sorts its flips, produces the B-flipped path as C's next candidate, and pushes that path's extend and swap successors into C's local heap:

```
return     C [A succeeds, B succeeds] 0.72
insert     C [A succeeds, B fails]    0.18
local heap C [A fails, B succeeds]    0.08
local heap C [A fails, B fails]       0.02
```

The fifth call returns that 0.18 path; the local heap's best becomes C's next candidate:

```
return C [A succeeds, B fails]  0.18
insert C [A fails, B succeeds]  0.08
```

The global heap then contains:

```
┌───────────────────────────────────────┐
│ NEXT  B [A fails]                0.10 │
│       C [A fails, B succeeds]    0.08 │
└───────────────────────────────────────┘
```

The complete sequence is:

| Call | Returned path | Probability | The head's next candidate |
| ---: | --- | ---: | --- |
| 1 | `A []` | 1.00 | none — A is exhausted |
| 2 | `D []` | 1.00 | none |
| 3 | `B [A succeeds]` | 0.90 | `B [A fails]` at 0.10 |
| 4 | `C [A succeeds, B succeeds]` | 0.72 | `C [A succeeds, B fails]` at 0.18; extend and swap push 0.02 and 0.08 into C's local heap |
| 5 | `C [A succeeds, B fails]` | 0.18 | `C [A fails, B succeeds]` at 0.08 |
| 6 | `B [A fails]` | 0.10 | none |
| 7 | `C [A fails, B succeeds]` | 0.08 | `C [A fails, B fails]` at 0.02 |
| 8 | `C [A fails, B fails]` | 0.02 | none |

A and D tie at 1.0, so batch ID puts A first. Other exact ties prefer fewer flips, then batch ID between heads and taken flip indexes within a head — so equally scored heads interleave instead of one head draining its whole subtree first.

## What work is eager and what work is lazy

`Generate` must eagerly:

- score every unique unresolved direct dependency needed by an eligible head, substituting the default for any score that is not a probability;
- choose each unresolved dependency's preferred assumption and calculate its `flipCost`; and
- total the best score for every head.

The generator defers:

- sorting a head's flips until that head's stream first advances — which happens the first time one of its paths is handed out;
- creating later subsets until their parent leaves the local heap; and
- constructing a full `SpeculationPath` until that path is returned.

This laziness is what keeps the exponential space affordable. A head with n unresolved dependencies has 2ⁿ paths, but nothing ever materializes that space: each call returns one path, inserts one candidate, and pushes at most two successor subsets. Pulling k paths therefore costs k constructed paths, at most k flip sorts (one per head actually reached), and heap operations logarithmic in the number of heads and in k — regardless of how many paths the space behind them holds. Only draining every path of every head would touch all 2ⁿ, and no caller does: the allocator stops as soon as the build budget is spent.

## Why logarithms are necessary

The explanation uses probability products because they are intuitive. The code uses logarithms because multiplying many small probabilities can round down to zero in `float64`, destroying their order.

```
log(p1 × p2 × ... × pn) = log(p1) + log(p2) + ... + log(pn)
```

The ordering stays the same. `CandidatePath.RankingScore` contains this logarithmic value. It is recomputed for every run and is not compared across runs.

## Resolved dependencies

- `Succeeded` fixes an assumption to succeeds.
- `Failed` or `Cancelled` fixes an assumption to fails.
- `Cancelling` remains undecided because cancellation may lose a race with completion.
- `Landing` also remains undecided, because a land can fail. It is tempting to treat it as committed to landing and skip the scorer call, but that puts a state-specific policy inside the search: whether a path betting against a landing batch is worth funding is a question of price, and price belongs to the scorer. The allocator draws the same line — "no batch state enters this decision" — and the generator holds it too. Nothing is lost by staying open: a single passed path still waits for the land result, while passed paths covering every outcome let the controller bypass the dependency (see [speculation.md](speculation.md)). Funding the unlikely side spends budget, which is the allocator's to ration.
- A fixed assumption stays in the returned path but contributes probability 1 and has no flip.
- A shared dependency is scored once per run.

## Why the algorithm works

- Every candidate in the global heap stands for a complete path that can be returned immediately, and the heap contains each head's current best.
- Sorting flips cheapest-first ensures extend and swap never produce a subset that outranks its parent.
- Extend and swap give every flip combination exactly one parent, so no path repeats.
- The global heap's top therefore never ranks below any path not yet handed out.
- Each call returns one path, advances one stream, and pushes at most two successor subsets.
- A probability-zero path sorts last but is never silently removed.

## Alternatives considered

### Generate every path, then rank and return

For a head with n direct dependencies, generate all 2ⁿ paths, calculate every probability, sort them, and return them in order.

This is simple, but it performs all the work up front even when the build budget needs only a few paths. We reject it because both time and memory grow exponentially with the number of dependencies.

### One global heap instead of two levels

The lazy walk does not require the per-head split. A single global heap also works: seed it with every head's no-flip candidate, and when a candidate is removed, push its extend and swap successors — computed over its own head's sorted flip list — straight back into the same heap. This produces the same paths in the same order at the same asymptotic cost, and it holds the same subsets in memory overall; the split only changes which heap a waiting subset sits in. It is also less code: one heap type instead of two.

We reject it because the fused structure is harder to understand and to keep correct, and that cognitive overhead outlives the lines saved. The two-level structure matches how the correctness argument decomposes into two standard, independently checkable patterns: each head's stream is a best-first enumeration over one sorted flip list, and the global heap is a k-way merge of streams that are already ordered. A reader can verify each half on its own — first that one stream never rises, while holding only one head in mind, then that merging ordered streams is safe, without thinking about flips at all. The single heap fuses those into one argument: why the top is safe to remove has to be reasoned across every head's partially enumerated subtree at once, and any future change must re-establish that fused argument rather than one half of it.

The split also keeps each heap's contents describable in one sentence. The global heap holds exactly one candidate per live head — which is what makes ties between heads break cleanly on head ID, so equally scored heads interleave — and a head's local heap holds only that head's frontier. A single heap mixes candidates from the same head with candidates from other heads, so its tie-break has to compare taken flip indexes across heads: positions in different heads' sorted flip lists, which are not semantically comparable.

### Walk the DAG and include transitive dependencies

Instead of using only a head's stored direct dependencies, walk the DAG and generate paths over its full transitive dependency closure. Consider:

```
C ──▶ B ──▶ A

D
```

Here `C.Dependencies = [B]`; A is only a transitive dependency through B.

C needs two paths for its one direct dependency:

```
C [B succeeds]
C [B fails]
```

A DAG walk would add A and produce four paths:

```
C [A succeeds, B succeeds]
C [A succeeds, B fails]
C [A fails, B succeeds]
C [A fails, B fails]
```

Those extra A branches over-speculate. C does not directly conflict with A, so A does not create a separate way to build C; its effect already flows through B. A longer chain makes the problem much worse: if the full closure contains n ancestors, a head with one direct dependency grows from two paths to 2ⁿ paths. We reject the DAG walk because it creates those unnecessary paths.
