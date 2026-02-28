# RFC: Top-K Speculation Path Generation with Scoring

## Metadata

| Field | Value                            |
|-------|----------------------------------|
| **Author** | Preetam Dwivedi<preetam@uber.com> |
| **Status** | In Review                        |
| **Created** | 2026-02-25                       |
| **Updated** | 2026-02-26                       |

## Summary

SubmitQueue processes batches in dependency order — a batch can only land after all its predecessors land. Without speculation, each batch waits for its predecessors to finish building and testing before it can start, creating a serial pipeline where total latency grows linearly with queue depth.

Speculation breaks this serial dependency by starting builds for a batch **before** its predecessors finish, guessing which predecessors will pass. Each guess is a **speculation path** — a specific combination of predecessors assumed to pass. If the guess matches reality, the batch's build is ready to land immediately with zero wait time.

The challenge: a batch with N predecessors has 2^N possible speculation paths (each predecessor either passes or fails). We can't build all of them. Given a success probability for each predecessor (from a Scorer), this RFC describes an algorithm that efficiently selects the **K most probable paths** — the best bets to speculatively build — using log-space arithmetic and a min-heap, in O(N log N + K log K) time without enumerating all 2^N possibilities.

## Background

### What is speculation?

SubmitQueue processes batches in dependency order. Batch B4 might depend on B1, B2, B3 — meaning B4 can only land if B1, B2, and B3 all land first.

Naively, we'd wait for B1, B2, B3 to finish, then start B4. That's slow. Instead, we **speculate**: start building B4 right now, guessing which predecessors will pass. If we guess right, B4 is ready to land immediately. If we guess wrong, we throw away that build and try another guess.

Each "guess" is a **speculation path** — a list of predecessors we assume will pass.

### The combinatorial problem

For batch B4 with dependencies [B1, B2, B3], each predecessor either passes or fails. That gives us 2^3 = 8 possible worlds:

```
Path                Meaning
──────────────────────────────────────────────
[B1, B2, B3]        all three pass
[B1, B2]            B1 and B2 pass, B3 fails
[B1, B3]            B1 and B3 pass, B2 fails
[B1]                only B1 passes
[B2, B3]            B2 and B3 pass, B1 fails
[B2]                only B2 passes
[B3]                only B3 passes
[]                  all three fail
```

With 3 deps, 8 paths is manageable. With 10 deps it's 1,024. With 20 it's over a million. With 50 it's over a quadrillion. We cannot build all of them.

**We need to pick the K most likely paths** — the best bets to spend build resources on.

### What is a scorer?

A **Scorer** is an extension that answers: "how likely is this batch to pass?" It returns a probability between 0 and 1 for each predecessor batch.

```
Scorer says:
  B1 → 0.9   (90% chance of passing)
  B2 → 0.7   (70% chance of passing)
  B3 → 0.6   (60% chance of passing)
```

These probabilities are inputs to the top-K algorithm.

### How the old system works

The old system builds a **SpeculationTree** — a binary tree where each level represents one predecessor and each branch represents pass or fail. For N predecessors, this tree has up to 2^N nodes, one per possible speculation path.

Two controls limit the tree's size:

- **`speculationDepth`** caps N (the number of predecessors to consider). Below that cap, the tree is exhaustive — every combination is generated.
- **`minScore`** prunes during traversal. Each node is scored using `score = Π P(pass) × Π (1-P(fail))` — the same probability formula this RFC uses. If a node's score drops below `minScore`, its children are never visited. Since children always have lower scores than parents (multiplying by a number < 1 shrinks the product), the entire subtree is safely pruned.

The surviving paths are collected, sorted by score, and capped at `maxPrioritizedContextsLimit`.

### Where the old system falls short

1. **Exponential worst case.** If most paths score above `minScore`, the system still visits O(2^N) nodes. The pruning only helps when the threshold is high enough to cut large subtrees — but setting it too high risks missing valid paths.

2. **Unpredictable output.** The result count depends on the probability distribution and threshold, not on what you actually need. You might get 3 paths or 30,000 — neither is controllable.

3. **Not probability-ordered.** BFS visits paths level-by-level, not best-first. The system must collect all surviving paths, then sort them. It can't stop early at "the K best" because it doesn't know which K are best until it's visited everything above the threshold.

4. **Product arithmetic.** Scores are computed as floating-point products. At large N, these products underflow to 0.0, making the threshold comparison meaningless — all paths score 0.0 and the system can't distinguish them.

This RFC replaces the tree enumeration with a top-K algorithm that produces exactly K paths in probability order, using log-space arithmetic that stays numerically stable at any scale.

## Path Probability

**Independence assumption:** We model each predecessor's outcome as independent — whether B1 passes doesn't affect whether B2 passes. This lets us compute a path's probability as a simple product. If predecessors are correlated in practice (e.g., two batches touching the same code are likely to fail together), the Scorer should account for this by adjusting the probabilities it returns. The algorithm itself doesn't care about correlations — it takes whatever probabilities the Scorer provides and finds the top-K paths.

Each predecessor independently passes or fails. The probability of a specific path is the product of each predecessor's contribution:

- If we **include** B1 (assume it passes): contribute P(B1) = 0.9
- If we **exclude** B1 (assume it fails): contribute 1 - P(B1) = 0.1

**Example: Path [B1, B2] (B1 and B2 pass, B3 fails)**

```
P(path) = P(B1 passes) × P(B2 passes) × P(B3 fails)
        = 0.9 × 0.7 × (1 - 0.6)
        = 0.9 × 0.7 × 0.4
        = 0.252
```

All 8 paths with their probabilities:

```
Path              Calculation                      Probability
─────────────────────────────────────────────────────────────────
[B1, B2, B3]      0.9 × 0.7 × 0.6               = 0.378
[B1, B2]          0.9 × 0.7 × 0.4               = 0.252
[B1, B3]          0.9 × 0.3 × 0.6               = 0.162
[B1]              0.9 × 0.3 × 0.4               = 0.108
[B2, B3]          0.1 × 0.7 × 0.6               = 0.042
[B2]              0.1 × 0.7 × 0.4               = 0.028
[B3]              0.1 × 0.3 × 0.6               = 0.018
[]                0.1 × 0.3 × 0.4               = 0.012
                                           Total = 1.000
```

We want these paths sorted by probability (descending) and we only want the top K.

## Finding Top-K Using a Heap

The brute-force approach — enumerate all 2^N paths, sort, take top K — is O(2^N). We need something smarter. The idea: instead of computing all paths, start from the best path and systematically explore the next-best deviations using a **max-heap**.

This section develops the full algorithm using product-space arithmetic. It works correctly for small N. Later we'll see where it breaks and how logarithms fix it.

### The optimal path

For each dep, the **preferred** bet is whichever outcome is more likely:

```
B1: P=0.9  → prefer include  (90% pass > 10% fail)
B2: P=0.7  → prefer include  (70% pass > 30% fail)
B3: P=0.6  → prefer include  (60% pass > 40% fail)
```

The **optimal path** uses every dep's preferred choice: `[B1, B2, B3]` with probability 0.9 × 0.7 × 0.6 = 0.378. This is always the most probable path — result #1.

(If a dep had P < 0.5, say P=0.3, we'd prefer to exclude it — betting it fails is smarter than betting it passes.)

### Flipping

Every other path is the optimal path with some deps **flipped** — switched from their preferred choice to their non-preferred choice. "Flip B3" means we change B3 from included (our preferred bet) to excluded.

Each flip has a **penalty** — a ratio that tells us how much worse the path becomes:

```
                 preferred    non-preferred    flip ratio
                 choice       choice           (non-preferred / preferred)
B3 (P=0.6):     0.6          0.4              0.4/0.6 = 0.667   (least damage)
B2 (P=0.7):     0.7          0.3              0.3/0.7 = 0.429
B1 (P=0.9):     0.9          0.1              0.1/0.9 = 0.111   (most damage)
```

The flip ratio is always less than 1 (the non-preferred choice is always worse). Multiplying a path's probability by a flip ratio makes it smaller — the path becomes less likely.

**B3 is cheapest to flip** (ratio closest to 1) because we're nearly uncertain about it (60/40). Changing our bet barely hurts. **B1 is most expensive** (ratio closest to 0) because we're very confident about it (90/10). Betting against B1 is a big sacrifice.

To get any path's probability from the optimal: multiply the optimal probability by the flip ratios of all flipped deps.

```
Path [B1, B2]  = flip B3        = 0.378 × 0.667             = 0.252
Path [B1, B3]  = flip B2        = 0.378 × 0.429             = 0.162
Path [B1]      = flip B3 + B2   = 0.378 × 0.667 × 0.429    = 0.108
```

**Flip ratio** = non-preferred / preferred (always < 1). We multiply the optimal path's probability by this ratio to get the flipped path's probability. It must be < 1 because the non-preferred choice is always worse — flipping makes the probability smaller.

### Sorting by flip ratio

Sort deps by ascending damage — cheapest flips first:

```
Sorted index 0: B3  ratio=0.667  (cheapest)
Sorted index 1: B2  ratio=0.429
Sorted index 2: B1  ratio=0.111  (most expensive)
```

**This sorting is critical.** It establishes the invariant that makes the heap work: since flip ratios are sorted so that each next ratio is smaller (more damaging), extend always multiplies by a number <= the current, and swap always replaces with a more damaging flip. This guarantees every child subset has a smaller combined ratio than its parent — so the heap always emits subsets in the correct order.

Now the problem becomes: **enumerate subsets of {0, 1, 2} in order of descending combined ratio** (most probable first). Each subset = which deps to flip from the optimal path.

### The max-heap with extend and swap

We use a **max-heap** (priority queue) that always gives us the subset with the highest combined ratio (= most probable path not yet emitted). When we pop a subset, we generate exactly two children:

**Extend**: keep everything we flipped, ALSO flip the next dep in sorted order.
- Formula: `new_ratio = combined × next_flip_ratio`
- Example: `{B3}` → extend → `{B3, B2}` (also flip B2)
- `0.667 × 0.429 = 0.286`

**Swap**: undo the last flip, flip the next dep instead.
- Formula: `new_ratio = combined / last_flip_ratio × next_flip_ratio`
- Example: `{B3}` → swap → `{B2}` (unflip B3, flip B2 instead)
- `0.667 / 0.667 × 0.429 = 0.429`

These two operations form a binary tree that covers every possible subset exactly once:

```
                          {0}
                         /   \
                   extend     swap
                   /             \
               {0, 1}            {1}
               /    \            /   \
          {0,1,2}  {0,2}     {1,2}   {2}

7 subsets in the tree + 1 empty set (optimal path) = 8 total = 2^3 ✓
```

Every child has a smaller combined ratio than its parent (the sorting invariant guarantees this). The heap naturally emits subsets in the right order.

### Full walkthrough (using products)

Deps sorted by flip ratio: [B3=0.667, B2=0.429, B1=0.111].

**Result #1**: Optimal path `[B1, B2, B3]`, probability = 0.378. No flips. Seed the heap with `{0}` (flip B3).

```
Heap: [ {0} ratio=0.667 ]
```

**Pop `{0}`** → flip B3 → path `[B1, B2]`
- probability = 0.378 × 0.667 = 0.252

Result #2. Generate children:
- Extend: `{0, 1}` → also flip B2, combined ratio = 0.667 × 0.429 = 0.286
- Swap: `{1}` → unflip B3, flip B2, combined ratio = 0.667 / 0.667 × 0.429 = 0.429

```
Heap: [ {1} ratio=0.429,  {0,1} ratio=0.286 ]
```

**Pop `{1}`** → flip B2 → path `[B1, B3]`
- probability = 0.378 × 0.429 = 0.162

Result #3. Children:
- Extend: `{1, 2}` → also flip B1, combined ratio = 0.429 × 0.111 = 0.048
- Swap: `{2}` → unflip B2, flip B1, combined ratio = 0.429 / 0.429 × 0.111 = 0.111

```
Heap: [ {0,1} ratio=0.286,  {2} ratio=0.111,  {1,2} ratio=0.048 ]
```

**Pop `{0,1}`** → flip B3+B2 → path `[B1]`
- probability = 0.378 × 0.286 = 0.108

Result #4. Children:
- Extend: `{0,1,2}` → also flip B1, combined ratio = 0.286 × 0.111 = 0.032
- Swap: `{0,2}` → unflip B2, flip B1, combined ratio = 0.286 / 0.429 × 0.111 = 0.074

```
Heap: [ {2} ratio=0.111,  {0,2} ratio=0.074,  {1,2} ratio=0.048,  {0,1,2} ratio=0.032 ]
```

**If K=4, we'd stop here** — pop 4 results and ignore the rest of the heap. With N=3 the savings are small (4 out of 8), but with N=20 we'd get our top-4 out of 1,048,576 paths — each heap pop generates at most 2 children, so we'd touch roughly 4 + 8 = 12 subsets instead of a million.

Let's continue to verify the algorithm produces all 8 in the correct order:

**Pop `{2}`** → flip B1 → path `[B2, B3]`, probability = 0.042. No children (no next index).

**Pop `{0,2}`** → flip B3+B1 → path `[B2]`, probability = 0.028. No children.

**Pop `{1,2}`** → flip B2+B1 → path `[B3]`, probability = 0.018. No children.

**Pop `{0,1,2}`** → flip all → path `[]`, probability = 0.012. No children.

All 8 paths emitted in probability order:

```
#   Flipped      Path           Ratio    Probability
─────────────────────────────────────────────────────
1   (none)       [B1, B2, B3]   1.000    0.378
2   {B3}         [B1, B2]       0.667    0.252
3   {B2}         [B1, B3]       0.429    0.162
4   {B3,B2}      [B1]           0.286    0.108
5   {B1}         [B2, B3]       0.111    0.042
6   {B3,B1}      [B2]           0.074    0.028
7   {B2,B1}      [B3]           0.048    0.018
8   {all}        []             0.032    0.012
```

The algorithm correctly produces all 8 paths in descending probability order. The heap-based approach works — it finds the top-K without computing all 2^N paths upfront.

But there's a catch. Look at the ratio column: 0.667, 0.286, 0.111, 0.074, 0.048, 0.032. With only 3 deps these numbers are fine. What happens with 50 deps?

## Where Products Break Down

### Numerical underflow

Each flip ratio is a number less than 1. When you multiply many numbers less than 1, the result shrinks toward zero exponentially:

```
 5 deps flipped:  0.667 × 0.429 × 0.111 × ... ≈ 0.003
10 deps flipped:  ≈ 0.0000001
20 deps flipped:  ≈ 10^-15
50 deps flipped:  ≈ 10^-40   ← below float64 precision, rounds to 0.0
```

Once the combined ratio rounds to 0, all remaining paths look identical (probability = 0). The heap compares 0.0 vs 0.0 and can no longer distinguish the 5th best path from the 50th. The ordering breaks.

### Division amplifies errors

The swap operation requires **dividing out** the old flip ratio and multiplying the new one. Division of small floating-point numbers amplifies rounding errors. After a chain of multiplies and divides, errors compound and the ordering becomes unreliable — even before reaching full underflow.

### We need a different number representation

The algorithm itself is correct — optimal path, flipping, extend/swap, the heap. The problem is purely numerical: products of ratios lose precision at scale. We need a way to represent flip costs that doesn't shrink to zero when combined.

## The Solution: Logarithms

A logarithm has one property:

```
log(A × B) = log(A) + log(B)
```

It converts multiplication into addition. That's the entire fix. The algorithm stays exactly the same — optimal path, flipping, extend/swap, min-heap — but every multiply becomes an add, every divide becomes a subtract, and the numbers stay in a stable range.

### Converting flip ratios to flip costs

The flip ratio is non-preferred / preferred — a number less than 1. Its log is negative:

```
log(flip ratio of B3) = log(0.4/0.6) = log(0.667) = -0.405   ← negative
```

Negative numbers are awkward — "smaller = worse" is counterintuitive. We want positive numbers where bigger = worse. So we invert the fraction before taking the log:

```
flip cost = -log(non-preferred / preferred) = log(preferred / non-preferred)
```

**Flip cost** = log(preferred / non-preferred) (always > 0). Bigger cost = more damage = worse path. A min-heap pops the smallest cost (least-damaged path) first.

```
B3 (P=0.6):  log(0.6/0.4) = log(1.5)  = 0.405
B2 (P=0.7):  log(0.7/0.3) = log(2.33) = 0.847
B1 (P=0.9):  log(0.9/0.1) = log(9.0)  = 2.197
```

Flip ratios (< 1) shrink when combined. Flip costs (> 0) grow when combined:

```
Products:  flip B3 AND B2  →  0.667 × 0.429 = 0.286    → shrinks toward 0
Logs:      flip B3 AND B2  →  0.405 + 0.847 = 1.252    → grows, stays precise
```

### The same algorithm, with addition

Everything works the same as before, but with costs instead of ratios:

| Operation | Products | Logarithms |
|-----------|----------|------------|
| Combine two flips | multiply ratios | add costs |
| Extend (also flip next dep) | `combined × next_ratio` | `combined + next_cost` |
| Swap (unflip last, flip next) | `combined / last_ratio × next_ratio` | `combined - last_cost + next_cost` |
| Heap type | max-heap (biggest ratio first) | min-heap (smallest cost first) |
| Heap comparison at N=50 | 0.0 vs 0.0 (broken) | 42.1 vs 47.3 (works) |

The max-heap becomes a min-heap (lowest cost = highest probability), but the extend/swap tree is identical. The walkthrough produces the same paths in the same order.

### The min-heap with extend and swap

The same extend/swap tree, but with a **min-heap** instead of a max-heap. The heap pops the smallest cost first (= highest probability path). When we pop a subset, we generate exactly two children:

**Extend**: keep everything we flipped, ALSO flip the next dep in sorted order.
- Formula: `new_cost = combined + next_flip_cost`
- Example: `{B3}` → extend → `{B3, B2}` (also flip B2)
- `0.405 + 0.847 = 1.252`

**Swap**: undo the last flip, flip the next dep instead.
- Formula: `new_cost = combined - last_flip_cost + next_flip_cost`
- Example: `{B3}` → swap → `{B2}` (unflip B3, flip B2 instead)
- `0.405 - 0.405 + 0.847 = 0.847`

The same binary tree as the product section, with costs instead of ratios:

```
                          {0}
                         /   \
                   extend     swap
                   /             \
               {0, 1}            {1}
               /    \            /   \
          {0,1,2}  {0,2}     {1,2}   {2}
```

Every child has a higher cost than its parent (because the next dep's flip cost is >= the current dep's, so extend adds a positive number, and swap replaces with a more expensive flip). The min-heap pops the lowest cost first — the least-damaged, most probable path.

### Walkthrough (log space)

Deps sorted by flip cost: [B3=0.405, B2=0.847, B1=2.197].

**Result #1**: Optimal path `[B1, B2, B3]`, cost = 0. Seed the heap with `{0}` (flip B3).

```
Heap: [ {0} cost=0.405 ]
```

**Pop `{0}`** → flip B3 → path `[B1, B2]`, cost = 0.405

Result #2. Generate children:
- Extend: `{0, 1}` → also flip B2, cost = 0.405 + 0.847 = 1.252
- Swap: `{1}` → unflip B3, flip B2, cost = 0.405 - 0.405 + 0.847 = 0.847

```
Heap: [ {1} cost=0.847,  {0,1} cost=1.252 ]
```

**Pop `{1}`** → flip B2 → path `[B1, B3]`, cost = 0.847

Result #3. Children:
- Extend: `{1, 2}` → also flip B1, cost = 0.847 + 2.197 = 3.044
- Swap: `{2}` → unflip B2, flip B1, cost = 0.847 - 0.847 + 2.197 = 2.197

```
Heap: [ {0,1} cost=1.252,  {2} cost=2.197,  {1,2} cost=3.044 ]
```

**Pop `{0,1}`** → flip B3+B2 → path `[B1]`, cost = 1.252

Result #4. Children:
- Extend: `{0,1,2}` → also flip B1, cost = 1.252 + 2.197 = 3.449
- Swap: `{0,2}` → unflip B2, flip B1, cost = 1.252 - 0.847 + 2.197 = 2.602

```
Heap: [ {2} cost=2.197,  {0,2} cost=2.602,  {1,2} cost=3.044,  {0,1,2} cost=3.449 ]
```

Continuing for completeness:

**Pop `{2}`** → flip B1 → path `[B2, B3]`, cost = 2.197. No children.

**Pop `{0,2}`** → flip B3+B1 → path `[B2]`, cost = 2.602. No children.

**Pop `{1,2}`** → flip B2+B1 → path `[B3]`, cost = 3.044. No children.

**Pop `{0,1,2}`** → flip all → path `[]`, cost = 3.449. No children.

All 8 paths in probability order:

```
#   Flipped      Path           Cost         Probability
──────────────────────────────────────────────────────────
1   (none)       [B1, B2, B3]   0            0.378
2   {B3}         [B1, B2]       0.405        0.252
3   {B2}         [B1, B3]       0.847        0.162
4   {B3,B2}      [B1]           1.252        0.108
5   {B1}         [B2, B3]       2.197        0.042
6   {B3,B1}      [B2]           2.602        0.028
7   {B2,B1}      [B3]           3.044        0.018
8   {all}        []             3.449        0.012
```

Same paths, same order, same probabilities as the product walkthrough. The only difference is what the heap tracks — costs (0.405, 1.252, ...) instead of ratios (0.667, 0.286, ...).

At N=50, these costs would be numbers like 0.4, 1.2, 3.4, 47.3 — perfectly representable in float64. The product ratios would be 0.667, 0.286, 0.000...0001, 0.0 — numerically dead.

### Recovering probability from cost

To convert a flip cost back to a probability:

```
probability = e^(optimal_log_score - total_flip_cost)
```

Where `optimal_log_score = log(0.9) + log(0.7) + log(0.6) = -0.973`. Computed once upfront.

### Summary

|                   | Products | Logarithms |
|-------------------|---|---|
| Path probability  | multiply N terms | sum N terms |
| Combining flips   | multiply ratios | add costs |
| Heap operations   | multiply / divide | add / subtract |
| At scale (N=50)   | ratios underflow to 0.0 | costs stay distinct (e.g. 47.3) |
| Work to get top-K | O(K) in theory, numerically broken | O(K), numerically stable |

## Why Extend/Swap Covers All Subsets

Every subset of {0, 1, ..., N-1} has a **largest element** j. That subset was either:

- **Extended** from a parent whose largest element was j-1 (child has all of parent's elements plus j)
- **Swapped** from a parent whose largest element was j-1 (child replaces j-1 with j)

This means every subset appears as exactly one node in the extend/swap tree. No duplicates, no gaps. The tree structure also guarantees that every child costs at least as much as its parent (in log space) or has a smaller combined ratio (in product space), so the heap always pops subsets in the correct order.

## The Core Reduction

The entire algorithm reduces to one insight:

> **Finding the top-K speculation paths = finding the K subsets with the smallest flip-cost sums.**

Each subset of dependencies to flip maps to exactly one speculation path. The flip-cost sum determines the path's probability rank. The min-heap with extend/swap enumerates these subsets in ascending sum order, producing exactly K paths without touching the rest.

## Pseudocode

```
function GenerateTopK(currentID, dependencyIDs, probabilities, K):
    N = len(dependencyIDs)

    // Step 1: Find optimal path and compute flip costs
    for each dep i in dependencyIDs:
        p = probabilities[dep_i]                        // from Scorer
        preferred = max(p, 1-p)
        non_preferred = min(p, 1-p)
        included[i] = (p >= 0.5)                        // optimal choice
        flip_cost[i] = log(preferred / non_preferred)   // always > 0

    // Step 2: Sort by ascending flip cost
    sorted_deps = sort deps by flip_cost ascending

    // Step 3: Emit optimal path (no flips)
    emit path from included[] with cost = 0

    // Step 4: Seed heap and enumerate
    push {flipped: [0], cost: flip_cost[sorted[0]]} onto min-heap

    while heap is not empty AND results < K:
        entry = pop min from heap
        emit path by toggling included[] at entry.flipped indices

        j = entry.last_index
        if j + 1 < N:
            // Extend: also flip next dep
            push {flipped: entry.flipped + [j+1],
                  cost: entry.cost + flip_cost[sorted[j+1]]}

            // Swap: unflip j, flip j+1 instead
            push {flipped: entry.flipped[:-1] + [j+1],
                  cost: entry.cost - flip_cost[sorted[j]] + flip_cost[sorted[j+1]]}
```

## Complexity

| Metric | Brute Force: O(2^N × N) | Top-K: O(N log N + K log K) |
|--------|-------------|-------|
| **Space** | O(2^N) | O(K) |
| **N=10, K=32** | 10,240 | 193 |
| **N=20, K=32** | 20,971,520 | 246 |
| **N=50, K=32** | ~5.6 × 10^16 | 442 |

Both product and log approaches have identical algorithmic complexity — the algorithm is the same, only the arithmetic differs. Products break numerically at scale, not algorithmically.

The top-K algorithm's cost is dominated by sorting the N flip costs (O(N log N)) and popping K entries from the heap (O(K log K)). It never touches the other 2^N - K paths.

## Edge Cases

**No dependencies**: Single path with empty base and score 1.0 (certainty).

**K >= 2^N**: All paths are returned. The algorithm naturally exhausts the heap before reaching K.

**All probabilities = 0.5**: All flip costs are 0 (log(1) = 0). All paths have equal probability. The ordering among them is arbitrary but the algorithm still produces valid results.

**Probability near 0 or 1**: Clamped to [epsilon, 1-epsilon] to avoid log(0) which is negative infinity.

**Scorer failure**: Falls back to 0.5 for all deps (maximum uncertainty). Every path gets equal probability. Top-K still runs correctly, producing arbitrarily-ordered but valid paths.

## References

- Lawler, E.L. (1972). "A procedure for computing the K best solutions to discrete optimization problems and its application to the shortest path problem." *Management Science* 18(7): 401-405. — Foundation for ordered subset enumeration via successor generation.
- The extend/swap pattern is a specialization of the Lawler-Murty procedure for enumerating solutions to combinatorial problems in cost order.