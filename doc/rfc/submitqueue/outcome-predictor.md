# Outcome Predictor

How likely a batch is to succeed, from a Scorer's price plus what the pipeline has since observed about the batch.

## Problem

Nothing the pipeline learns about a batch changes its price. A batch whose build has passed, whose dependencies have landed, and which is being merged is priced exactly as it was before anything was known about it — on the size of its diff. The queue holds that evidence in memory during the run that needs it, and throws it away.

The prices are also fixed numbers someone typed. The heuristic scorer (`submitqueue/extension/speculation/scorer/heuristic`) maps lines changed onto a bucket table, an unconfigured queue prices everything at 0.5, and the bestfirst generator (`submitqueue/extension/speculation/generator/bestfirst`) uses 0.95 for anything it could not price. Nothing improves them over time.

This document covers the first problem only. Pricing a change from its content is a separate question, and it stays in whatever Scorer a queue configures for it.

## What it does

The predictor is its own contract, separate from the Scorer. It is handed a batch and what that batch's builds have done, and it returns how likely the batch is to reach Succeeded. It is built with a Scorer: it asks the Scorer for the batch's price, then multiplies the **odds** of that price by one factor per piece of evidence.

The generator depends on the predictor, not on the Scorer.

A batch the base prices at 0.6 has odds of 1.5 to 1. Its build passes, worth ten times the odds: 15 to 1, or 0.94. It reaches merging, worth another twelve: 180 to 1, or 0.995. The generator already resolves a batch to certainty once it is terminal, so the predictor never has to handle that case.

Multiplying odds keeps the answer a probability without clamping, and it makes each factor mean the same thing wherever it applies — the same evidence is worth the same amount whether the base said 0.5 or 0.95. Adding to the probability directly has neither property.

The factors are what gets configured, and later learned. Written as logs and summed, this is ordinary logistic regression, so fitting it needs nothing special.

**All factors at one, the predictor returns the base price unchanged.** Turning it on is a no-op until someone sets a factor.

## Where the line falls

**The scorer prices the change. The predictor prices the situation.**

Lines, files, which directories, who wrote it — that describes the change, and it belongs in the scorer. Builds, batch state, dependencies, time waiting — that describes where the batch sits right now, and it belongs in the predictor.

That is also why they are two contracts rather than one. The evidence is an input the scorer has no use for: put it on `Score` and every implementation that prices content — all three that exist — takes a parameter it discards. A parameter every implementation throws away belongs to a different contract.

Keeping them apart means either side can be replaced without redoing the other, and it leaves the scorer per-queue configurable, which it already is.

## What it looks at

| Evidence | Direction |
| --- | --- |
| A build passed for this batch | Strongly up — the biggest factor, with the caveat below |
| Builds failed for this batch, counted | Down |
| How old the passing build is | Down as it ages — trunk moves |
| A build is running | Slightly up: better than never tried |
| The batch is merging | Strongly up — it cleared speculation and is pushing, but can still lose a race |
| The batch is cancelling | Strongly down, though not to zero — cancelling is best effort |
| How many dependencies it has | Down — more assumptions that can break |
| How long it has been in the queue | Down — usually it has been invalidated before |
| The queue's recent landing rate | Moves everything, so a bad week does not need re-fitting |

Two of these are decisions rather than just numbers.

**A passing build only counts under the assumptions it was built with.** A dependency can hold a green build for a path assuming *its* dependency succeeds, while the candidate being priced assumes that one fails. That build says nothing about the second case. So the evidence is narrower than "a build passed": it is "a build passed on the all-succeed path", which is right whenever the candidate assumes the same — the common case — and must not count otherwise.

**Merging and cancelling belong here and nowhere else.** The generator may only tell terminal from non-terminal; the allocator reads path status only. Pinning a merging dependency to certain inside the generator was tried and reverted, correctly — a merge can fail, so nothing is settled. How much a state is worth is a price, and that price is the predictor's.

## Getting the evidence

Batch state comes free: the predictor is handed the batch, which carries it. The build evidence does not. The speculate controller reads each in-flight head's path set once per run and hands it to the Speculator; the Generator never sees it.

So the Generator takes the path sets alongside the batches, and hands each batch its own set when it asks for a prediction. The Scorer contract does not change at all.

The alternative — the predictor reads the path-set store itself — needs no contract change but re-reads what the run already holds, once per dependency, and can see a newer version than the rest of the run is working from. The run reads once so that two decisions in it can never disagree about the world, and that is worth more than the plumbing costs.

## Fitting the factors

Written by hand first. A factor is "how much does this multiply the odds", which is a number an engineer can propose and a reviewer can argue with, so the first version needs no data at all.

Learning them later needs three things:

- **A record written when the price is set** — the batch, the base price, each piece of evidence, the result, and the predictor version. Writing it when the outcome arrives instead would record values the serving path never has.
- **Outcomes joined to it.** Succeeded is a yes, failed is a no, and a batch the author cancelled is neither, so it is left out — counting it as a failure would teach the predictor that abandoned work is bad code. Batch outcomes are not logged today; they can be derived from request logs through the request-batch store.
- **An awareness that the data is biased.** Only paths that got funded produce outcomes, so the records show what the previous ranking already believed. Fitted factors will lean toward agreeing with it.

The fitted result is a small versioned file of named factors loaded at wiring time, with the evidence list hashed so a file that does not match the code fails at startup rather than mispricing quietly.

## Judging it

Whether the probabilities are honest is measurable, but it is not the point. The point is builds per landed change, share of builds spent on paths later thrown away, and how long the queue takes. Measuring those before production means replaying recorded runs against a candidate predictor. No such harness exists, and that is the main thing between fitted factors and trusting them.

## Rollout

1. Start recording prices and outcomes. Nothing else works without it.
2. Ship the predictor with all factors at one — identical output — then set factors for builds and batch state by hand. This is the largest win and it needs no data.
3. Fit the factors. Run it alongside the current ranking first, then one queue, then everywhere.

## Not in scope

Worth doing, separately: predicting how long and how large a build will be, so the allocator can rank by value per CI-minute rather than by probability alone; per-directory history in the base; and pricing a dependency against the specific assumptions a path makes about its own dependencies, which would remove the caveat on build evidence.

## Rejected

**One contract, with the evidence on `Score`.** Tried: every scorer that prices content took a parameter it discarded, and the composite forwarded one it never read. The evidence is not part of pricing a change.

**One estimate over content and evidence together.** They change at different rates and need different amounts of data, and it would force every queue onto the same content scorer.

**Adding to the probability instead of multiplying odds.** Leaves the range, needs clamping, and the same increment means different things at different prices.

**More dimensions on the bucket table.** A second dimension squares it, a third makes it unwritable, and every cell is still a guess.

**Putting state in the generator.** Tried and reverted: a merge can fail, so nothing is settled, and how much a state is worth is a price.

**A scoring stage.** Prices only mean anything inside the run that produced them; storing them would make them stale by construction.
