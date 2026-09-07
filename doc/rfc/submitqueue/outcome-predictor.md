# Outcome Predictor

How likely a batch is to reach Succeeded, given the scorer's price for the change plus what this speculate run has already observed.

See [speculation.md](speculation.md) for batches, paths, heads, and the Speculator. This document is the price the default Generator ranks on.

## The idea

The **scorer** prices the change (lines, files, who wrote it). That number does not move after the batch is admitted.

The **predictor** prices the situation. It starts from the scorer's price and revises it with facts the speculate run already holds: a path *passed*, a path *failed*, the batch is *merging*, the batch is *cancelling*.

`bestfirst` ranks a path by the probability that every unresolved assumption holds. It now asks the predictor for that probability, not the scorer. Two heads whose changes score the same can rank differently once one of them has a *passed* build.

They are two contracts because they answer different questions. Putting path-set evidence on `Score` was tried: every content scorer took a parameter it discarded.

**Default is a no-op.** Every factor starts at `1`, so the predictor returns the scorer's price until someone sets a factor.

## What a factor is

A factor revises the scorer's price. It is not itself a probability: `10` does not mean `0.10`, and `0.3` does not mean the batch is 30% likely to succeed.

| Value | Meaning |
| --- | --- |
| `1` | Leave the scorer's price alone (the default if the key is omitted) |
| greater than `1` | More likely to reach Succeeded |
| between `0` and `1` | Less likely to reach Succeeded |

Config rejects `0` and negatives. There is no upper cap.

The unconfigured scorer prices every batch at `0.5`. From that price, one factor `f` produces:

| Factor | Price |
| --- | --- |
| `1` | 0.50 |
| `10` | ~0.91 |
| `12` | ~0.92 |
| `0.3` | ~0.23 |
| `0.25` | 0.20 |

A scorer price of `0.6` with `pathPassed: 10` becomes about `0.94`. `merging: 12` on top of that becomes about `0.995`.

`pathFailed: 0.3` from `0.5` becomes about `0.23`. A second *failed* path of the same kind multiplies again. `0` is rejected: it would pin the batch at probability 0 for the rest of the run.

The arithmetic multiplies odds (`p / (1-p)`), then converts back, so the result stays in `(0, 1)` and the same factor means the same thing at any scorer price. Adding to the probability does neither.

YAML:

```yaml
predictor:
  type: evidence
  factors:
    pathPassed: 10
    pathFailed: 0.3
    merging: 12
    cancelling: 0.1
```

The example values above are guesses, for reading the tables. The shipped default is to omit `factors` (every factor `1`).

## Evidence

| YAML key | When it applies | Typical direction |
| --- | --- | --- |
| `pathPassed` | Once, if a path that assumes every dependency *succeeds* has *passed* | Up |
| `pathFailed` | Once per *failed* path that assumes every dependency *succeeds* | Down |
| `merging` | While the batch is *merging* | Up |
| `cancelling` | While the batch is *cancelling* | Down |

`bestfirst` already treats a terminal batch as a fact (*Succeeded*, *Failed*, *Cancelled*). The predictor is not asked. *Merging* is not terminal: a merge can still fail, so how much it is worth stays a price.

### Only the *succeeds* path counts

The batch being priced is itself a head, so the run may have built it more than once under different assumptions about *its* dependencies. Only one of those builds is evidence.

Take `C` depending on `B`, and `B` depending on `A`. Ranking `C`'s candidates needs the probability that `B` reaches Succeeded, so the Generator calls `Predict` with `B` and `B`'s path set. That set can hold two finished builds:

| `B`'s path | What was compiled |
| --- | --- |
| `B` with `A` *succeeds* | `B` on top of `A`'s changes |
| `B` with `A` *fails* | `B` without them |

`B` merges after `A` does, so the first build is a build of the code that will actually land: if it *passed*, `B` is likely to merge, and `pathPassed` applies.

The second is a different set of changes. `B` may call something `A` introduces and fail to compile on its own — a *failed* result that says nothing about `B` merging in the normal case. Counting it would push `B` down the ranking over a build it was never going to need, while a green build of the real combination sits in the same set.

So `pathPassed` and `pathFailed` both look only at paths that assume every dependency *succeeds*. Results on any other path are skipped. This is a filter on which results are evidence, not a check on whether an assumption came true — nothing here revisits that.

## Rejected alternatives

Design choices a reader might suggest after the sections above. Each names the alternative, why it fails here, and what this RFC does instead.

### Fold path evidence into `Score`

Give `Score` the speculate run's path sets so one call returns a situation-aware price. We tried it: content scorers took the parameter and discarded it; the composite forwarded evidence it never read. **Instead:** keep `Score` for the change; add `Predictor` for the situation (see [The idea](#the-idea)).

### One model for content and evidence

Train or tune a single estimate over diff shape and build outcomes together. Content signals and situation signals change at different rates, need different amounts of data, and would force every queue onto the same content scorer. **Instead:** scorer stays per-queue; evidence factors layer on in YAML.

### Treat *merging* and *cancelling* as settled in the Generator

Rank a *merging* batch like Succeeded and a *cancelling* batch like Cancelled. We tried and reverted: a merge can still fail, so the rank was wrong once outcomes diverged. **Instead:** only terminal states short-circuit in the Generator; *merging* and *cancelling* are predictor factors (see [Evidence](#evidence)).

### Let the predictor read the path-set store

`Predict` loads path sets from storage on each call — smaller API, fewer parameters. Each call can see a different snapshot mid-run (stale or split-brain relative to the rank the Generator is building). **Instead:** the speculate run reads path sets once per dependency and passes them in.
