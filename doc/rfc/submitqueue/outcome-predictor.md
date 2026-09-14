# Outcome Scorer

How likely a batch is to reach Succeeded, given the change's content plus what this speculate run has already observed.

See [speculation.md](speculation.md) for batches, paths, heads, and the Speculator. This document specifies the dependency probability the default Generator uses to rank paths.

## The idea

`bestfirst` ranks a path by the probability that every unresolved dependency assumption holds. For each dependency it calls **one** extension: `Scorer.Score(ctx, batch, paths)` — the probability of *succeeds*; it uses the complement for *fails*.

That number has two parts, composed as one scorer:

1. A **base** price for the change from content signals such as its size. Heuristic and composite supply this. They implement the same `Score` and ignore `paths`.
2. An **evidence** layer that revises the base with facts the speculate run already holds: a path *passed*, a path *failed*, the batch is *merging*, the batch is *cancelling*.

Evidence is the scorer the queue exposes. The base sits under it. There is no sibling Predictor factory.

This is a logit-linear model (a GLM with a logit link) with configured weights:

```
logit(p') = logit(p_base) + sum_i w_i x_i
```

`p_base` is the heuristic or composite price (the offset). `x_i` are binary features from the path set and batch state. `w_i = log(factor_i)` are YAML weights, not a fitted likelihood.

**Default is a no-op.** Every factor starts at `1` (`w = 0`), so `Score` returns the base price until someone sets a factor.

## What a factor is

A factor is an odds multiplier for one feature. It is not itself a probability: `10` does not mean `0.10`, and `0.3` does not mean the batch is 30% likely to succeed. Equivalently `w = log(factor)` on the logit.

| Value | Weight | Meaning |
| --- | --- | --- |
| `1` | `0` | Leave the base price alone (the default if the key is omitted) |
| greater than `1` | positive | More likely to reach Succeeded |
| between `0` and `1` (exclusive) | negative | Less likely to reach Succeeded |

Config accepts any finite value greater than `0`; there is no finite upper cap.

The unconfigured base prices every batch at `0.5`. From that price, one factor `f` produces `sigmoid(logit(0.5) + log(f)) = f / (1 + f)`:

| Factor | Price |
| --- | --- |
| `1` | 0.50 |
| `10` | ~0.91 |
| `12` | ~0.92 |
| `0.3` | ~0.23 |
| `0.25` | 0.20 |

A base price of `0.6` with `pathPassed: 10` becomes about `0.94`. `merging: 12` on top of that becomes about `0.995`.

`pathFailed: 0.3` from `0.5` becomes about `0.23`. It applies at most once because the path set has one current entry for the all-*succeeds* path; retry attempts replace that entry rather than adding evidence. `0` is rejected because it would pin matching batches at probability 0.

Odds revision keeps the result in `(0, 1)` and makes the same factor mean the same thing at any base price. Neutral factors return the base unchanged, including `0` or `1`. Adding to the probability provides neither property. A clamp at a small epsilon is a numerical guard around those endpoints, not part of the linear predictor.

This is **not** a fitted GLM: weights are configured, not trained; there is no extra intercept (`p_base` is the offset); features are hand-defined, not learned.

YAML. Evidence is the scorer; the content provider is `base`:

```yaml
scorer:
  type: evidence
  factors:
    pathPassed: 10
    pathFailed: 0.3
    merging: 12
    cancelling: 0.1
  base:
    type: heuristic
```

The example values above are guesses, for reading the tables. The shipped default is to omit `factors` (every factor `1`). Omitted `type` is `evidence`. Omitted `base` is the default heuristic. `type: heuristic` and `type: composite` belong on `base` (and on composite `components`), not at the top level.

Profiles may set `factors` under `defaults.scorer` and revise them per queue. An omitted key keeps the inherited value — from defaults, or `1` when neither side named it. A queue `scorer` block overlays named factor keys; a present `base` replaces the default base wholesale.

## Evidence

| YAML key | When it applies | Typical direction |
| --- | --- | --- |
| `pathPassed` | Once, if a path that assumes every dependency *succeeds* has *passed* | Up |
| `pathFailed` | Once, if the path that assumes every dependency *succeeds* has *failed* | Down |
| `merging` | While the batch is *merging* | Up |
| `cancelling` | While the batch is *cancelling* | Down |

`bestfirst` already treats a terminal batch as a fact (*Succeeded*, *Failed*, *Cancelled*). The scorer is not asked. *Merging* is not terminal: a merge can still fail, so how much it is worth stays a price.

### Only the *succeeds* path counts

The batch being priced is itself a head, so the run may have built it more than once under different assumptions about *its* dependencies. Only one of those builds is evidence.

Take `C` depending on `B`, and `B` depending on `A`. Ranking `C`'s candidates needs the probability that `B` reaches Succeeded, so the Generator calls `Score` with `B` and `B`'s path set. That set can hold two finished builds:

| `B`'s path | What was compiled |
| --- | --- |
| `B` with `A` *succeeds* | `B` on top of `A`'s changes |
| `B` with `A` *fails* | `B` without them |

`B` merges after `A` does, so the first build is a build of the code that will actually land: if it *passed*, `B` is likely to merge, and `pathPassed` applies.

The second is a different set of changes. `B` may call something `A` introduces and fail to compile on its own — a *failed* result that says nothing about `B` merging in the normal case. Counting it would push `B` down the ranking over a build it was never going to need, while a green build of the real combination sits in the same set.

So `pathPassed` and `pathFailed` both look only at paths that assume every dependency *succeeds*. Results on any other path are skipped. This is a filter on which results are evidence, not a check on whether an assumption came true — nothing here revisits that.

## Rejected alternatives

Design choices a reader might suggest after the sections above. Each names the alternative, why it fails here, and what this RFC does instead.

### A sibling Predictor factory

Keep `Score(ctx, batch)` for content and add `Predict(ctx, batch, paths)` as a second extension. Ranking only needs one number per unresolved batch; two factories duplicate the per-queue seam. **Instead:** one `Scorer.Score(ctx, batch, paths)`. Evidence is a scorer implementation that wraps a base.

### Put `paths` only on heuristic and composite

Every content backend reads the path set. We tried forwarding path sets through composite: components discarded them. **Instead:** heuristic and composite implement the same `Score` and ignore `paths`. Evidence is the layer that reads them.

### One fitted model for content and evidence

Train a single estimate over diff shape and build outcomes together. Content signals and situation signals change at different rates, need different amounts of data, and would force every queue onto the same content scorer. **Instead:** the base stays per-queue; evidence weights layer on in YAML. `p_base` remains the GLM offset if someone later fits `w`.

### Treat *merging* and *cancelling* as settled in the Generator

Rank a *merging* batch like Succeeded and a *cancelling* batch like Cancelled. We tried and reverted: a merge can still fail, so the rank was wrong once outcomes diverged. **Instead:** only terminal states short-circuit in the Generator; *merging* and *cancelling* are scorer features (see [Evidence](#evidence)).

### Let the scorer read the path-set store

`Score` loads path sets from storage on each call — smaller API, fewer parameters. Each call can see a different snapshot mid-run (stale or split-brain relative to the rank the Generator is building). **Instead:** the speculate run reads all path sets as one snapshot and passes the matching set for each dependency.
