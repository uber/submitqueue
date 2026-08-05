# Probability-Ordered Speculation Generator

## Status

Implemented.

## Summary

SubmitQueue's default speculation implementation needs one global stream of candidate paths across every batch currently in `BatchStateSpeculating`. A candidate path chooses one assumption for every dependency of its head: the dependency succeeds or it fails. Building every combination eagerly is infeasible because a head with `N` unresolved dependencies has `2^N` paths.

The `bestfirst` generator produces this stream lazily. It scores unresolved dependencies once, enumerates each head's assignments in descending probability without materializing the full power set, and merges those per-head streams into one global best-first iterator.

The implementation lives under `submitqueue/extension/speculation/generator`: the parent package defines the generator and iterator contracts, while `generator/bestfirst` contains the default probability-ranked implementation.

## Goals

1. Yield coherent candidate paths only for heads in `BatchStateSpeculating`.
2. Never contradict a dependency whose outcome is already terminal.
3. Rank candidates by the probability that all unresolved dependency assumptions match their eventual outcomes.
4. Rank globally across disconnected components and multiple heads.
5. Generate only the prefix requested by the allocator rather than all `2^N` paths.
6. Keep ordering numerically stable for deep dependency sets.
7. Avoid scoring the same shared dependency more than once per speculation run.
8. Preserve the head's dependency order in every emitted path.

## Non-goals

- The generator does not spend the build budget; that belongs to the allocator.
- The generator does not inspect path sets or suppress previously materialized paths; the allocator reconciles candidates with stored path sets.
- The generator does not propose cancellations, merges, or failure verdicts.
- The default implementation does not model correlation between dependency outcomes.
- The default implementation does not emit `DependencyAssumptionIgnored`; conflict relaxation requires a separate policy.

## Contract

Generation receives one queue snapshot containing all in-flight batches plus terminal batches still referenced as dependencies. Only batches in `BatchStateSpeculating` become heads. All other batches are facts or probability inputs.

The snapshot must include every batch named by a generated head's `Dependencies` field. Missing dependencies, duplicate batch IDs, repeated dependencies, self-dependencies, empty IDs, and dependencies in the zero-value unknown state are malformed input and abort generation.

The returned iterator yields `entity.CandidatePath` values. Exhaustion is represented by `ok=false`, not an error. Generation and iteration both honor context cancellation, and a cancelled `Next` call does not consume a candidate.

## Dependency outcomes

Terminal dependency states are evidence rather than predictions:

| Batch state | Path assumption |
|---|---|
| `BatchStateSucceeded` | `DependencyAssumptionSucceeds` |
| `BatchStateFailed` | `DependencyAssumptionFails` |
| `BatchStateCancelled` | `DependencyAssumptionFails` |

Every other nonzero state remains unresolved and is scored. In particular, `BatchStateCancelling` remains unresolved because cancellation is best-effort and a merge may still win the race.

Every dependency remains present in the emitted path, including resolved dependencies. This keeps the path self-describing while ensuring every candidate agrees with known facts.

## Probability model

For an unresolved dependency `d`, the injected scorer supplies:

```text
p[d] = P(d eventually succeeds)
```

The default generator assumes unresolved outcomes are independent. For candidate `c`, let `U(c)` be its unresolved dependencies:

```text
P(c) = product over d in U(c):
         p[d]       when c assumes d succeeds
         1 - p[d]   when c assumes d fails
```

Resolved dependencies do not contribute a factor because generation is already conditioned on their known outcomes.

The head's own score is not included. A path result is useful whether the head's validation passes or fails; the ranking question is whether the path's dependency assumptions become the actual world.

Scorer outputs must be finite values in the inclusive range `[0, 1]`. Invalid values abort the run rather than silently changing the model. Exact `0` and `1` are preserved: paths betting against them receive ranking score `0`.

## Architecture

```text
 Queue snapshot
      │
      ├── validate IDs, states, and dependency references
      │
      ├── score every shared unresolved dependency once
      │
      └── build one lazy stream per Speculating head
                    │
                    ▼
       ┌───────────────────────────┐
       │ Per-head assignment stream│
       │ MAP path, then cheapest    │
       │ combinations of flips     │
       └─────────────┬─────────────┘
                     │ one current candidate per head
                     ▼
       ┌───────────────────────────┐
       │ Global max-heap           │
       │ ordered by log probability│
       └─────────────┬─────────────┘
                     │
                     ▼
              Iterator.Next()
```

The global heap contains only the current best candidate from each head. When `Next` removes a candidate, it advances only that candidate's local stream and inserts the head's next candidate. This is a k-way merge over already ordered streams.

## Per-head best-first enumeration

### Optimal assignment

For each unresolved dependency:

```text
preferred(d) = succeeds when p[d] >= 0.5
               fails otherwise
```

Choosing every dependency's preferred outcome gives the maximum-probability assignment for the head.

### Flips

Every other assignment is the optimal assignment with some dependencies flipped to their less likely outcomes.

For probabilities strictly between `0` and `1`, flipping dependency `d` has log penalty:

```text
flipCost(d) = log(max(p[d], 1-p[d]) / min(p[d], 1-p[d]))
```

The probability of a flipped assignment is:

```text
log P(assignment) = log P(optimal) - sum(flipCost(d) for flipped d)
```

Dependencies are sorted by ascending flip cost. A dependency near `0.5` is cheap to flip because the scheduler is uncertain about it; a dependency near `0` or `1` is expensive to flip.

### Extend/swap subset enumeration

The local min-heap stores subsets of sorted dependencies to flip. After popping a subset whose greatest sorted index is `j`, the generator creates at most two successors:

```text
extend: keep the current flips and also flip j+1
swap:   replace flip j with flip j+1
```

Pseudocode:

```text
emit optimal assignment
seed heap with {0}

while heap is not empty:
    current = pop lowest flip cost
    emit assignment produced by current.flips

    j = current.lastIndex
    if j+1 exists:
        push current.flips + {j+1}
        push current.flips - {j} + {j+1}
```

The extend/swap tree reaches every subset exactly once. Because flip costs are sorted, no generated child has a better nonzero probability than its parent.

Exact zero-probability outcomes require special handling because an infinite flip cost makes `infinity - infinity` undefined during swap. The implementation tracks zero-probability flips as an integer count alongside the finite log cost. Any positive count produces probability zero while preserving complete, deterministic enumeration.

## Numerical ordering

The iterator orders candidates using internal log probabilities, not the public `RankingScore`.

Multiplying many probabilities can underflow to zero, and converting a very negative log probability back to `float64` can also underflow. Two deep paths may therefore both expose `RankingScore=0` even though one is mathematically more likely. The internal log values remain distinct and preserve the correct iteration order.

Equal-probability candidates use deterministic tie-breaking. Heads are initialized by ID, local equal-cost entries use insertion sequence, and the global heap uses head and path identity.

## Worked example

Consider two disconnected components. Arrows point from a dependency to its dependent.

```text
Component 1

A (0.80) ──┐
            ├──▶ C
B (0.60) ──┘

Component 2

E (0.30) ─────▶ F
```

`C` and `F` are Speculating heads. `A`, `B`, and `E` are unresolved dependencies. The values in parentheses are scorer probabilities of success.

### Step 1: create `C`'s local stream

`C` has four assignments:

| Rank | Assignment | Probability |
|---:|---|---:|
| 1 | `A=succeeds, B=succeeds` | `0.80 * 0.60 = 0.48` |
| 2 | `A=succeeds, B=fails` | `0.80 * 0.40 = 0.32` |
| 3 | `A=fails, B=succeeds` | `0.20 * 0.60 = 0.12` |
| 4 | `A=fails, B=fails` | `0.20 * 0.40 = 0.08` |

The optimal assignment is `A=succeeds, B=succeeds`. Flipping `B` is cheaper than flipping `A`, so the second candidate changes only `B`.

### Step 2: create `F`'s local stream

`E` is more likely to fail than succeed:

| Rank | Assignment | Probability |
|---:|---|---:|
| 1 | `E=fails` | `0.70` |
| 2 | `E=succeeds` | `0.30` |

### Step 3: initialize the global heap

The global heap initially contains only each head's first candidate:

```text
F: E=fails                     0.70
C: A=succeeds, B=succeeds     0.48
```

### Step 4: pull the first candidate

`Next` returns `F: E=fails` with score `0.70`, advances only `F`, and inserts `F: E=succeeds` with score `0.30`.

```text
C: A=succeeds, B=succeeds     0.48
F: E=succeeds                 0.30
```

### Step 5: pull the second candidate

`Next` returns `C: A=succeeds, B=succeeds` with score `0.48`, advances only `C`, and inserts `C: A=succeeds, B=fails` with score `0.32`.

```text
C: A=succeeds, B=fails        0.32
F: E=succeeds                 0.30
```

### Step 6: continue the merge

The final global order is:

```text
1. F: E=fails                     0.70
2. C: A=succeeds, B=succeeds     0.48
3. C: A=succeeds, B=fails        0.32
4. F: E=succeeds                 0.30
5. C: A=fails, B=succeeds        0.12
6. C: A=fails, B=fails           0.08
```

No disconnected-component special case is required; the global heap naturally merges them.

## Evidence update example

The generator is intentionally run-scoped rather than stateful across queue updates. Suppose a later speculation run sees `E` resolved to succeeded.

`F` then has one coherent path:

```text
F: E=succeeds    score 1.0
```

The previous `E=fails` candidate is no longer generated because it contradicts a resolved fact. The allocator and controller reconcile any previously materialized path against the new snapshot.

## Correctness properties

### Coherence

Resolved dependency outcomes map to exactly one assumption, and unresolved dependencies map to exactly one of succeeds/fails in every candidate. Therefore each emitted path is complete and cannot contradict known evidence.

### No duplicate assignments

Every unresolved assignment corresponds to one subset of variables flipped from the optimal assignment. The extend/swap tree enumerates every subset exactly once, so a local stream cannot repeat a path.

### Per-head ordering

For nonzero paths, assignment probability decreases monotonically as total flip cost increases. The local min-heap therefore emits assignments in descending probability order. Zero-probability paths follow all nonzero paths.

### Global ordering

The global heap contains the next unconsumed item from every per-head ordered stream. Removing the greatest item and replacing it with the same stream's successor is the standard k-way merge invariant, so the returned sequence is globally ordered.

## Complexity

Let `D` be the number of unique unresolved dependency batches referenced by Speculating heads, `H` the number of Speculating heads, `N_h` the unresolved dependency count for head `h`, and `K` the number of candidates actually pulled.

- Snapshot indexing and validation: `O(number of batches + dependency references)`.
- Scoring: `D` scorer calls.
- Per-head initialization: `O(N_h log N_h)` to sort flip costs.
- Global initialization: `O(H)`.
- Each pull: global heap work `O(log H)`, local heap work up to `O(log K_h)`, plus `O(number of head dependencies)` to materialize the self-describing output path.
- Enumeration state grows with the consumed prefix rather than `2^N`.

The worst case remains exponential if a caller exhausts every path. The allocator is expected to pull only enough candidates to fill or compare against the finite build budget.

## Alternatives considered

### Eager power-set generation

Generating all assignments and sorting them is simple but requires exponential time and memory before the first candidate can be consumed.

### Probability threshold

A minimum score can prune low-probability branches, but output size becomes distribution-dependent and the generator cannot guarantee that it has found the best finite prefix without exploring all branches above the threshold.

### Persisted ranking

Ranking scores depend on the current queue snapshot and become stale when any dependency resolves or receives a new score. Scores therefore remain transient on `CandidatePath` and are never persisted.

### Include the head's score

Multiplying by the head's own success probability would prioritize paths likely to produce a passing build rather than paths likely to match reality. The generator's contract ranks dependency assumptions, so it excludes the head score. An alternate generator may choose a different ranking policy without changing correctness.

## Future work

- A correlated-outcome generator can replace the independent product model while retaining the same iterator contract.
- A conflict-relaxing generator can add `DependencyAssumptionIgnored` choices and rank them using an explicit relaxation policy.
- A graph-impact ranking can combine assignment probability with critical-path or business value when the scheduling objective changes from expected applicability to full-queue makespan.
