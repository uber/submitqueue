# Simulator

Design notes for a simulator that verifies and benchmarks changes to orchestrator logic before they ship: per-stage record and replay, controller replay, and whole-pipeline simulation. This document captures **design decisions and rationale only**; the code lands after this RFC is reviewed.

## Problem

Verifying a change to orchestrator logic — a new `conflict.Analyzer`, a different `Scorer` bucketing, a `Speculator` policy tweak, a `Merger` strategy — today means either trusting a unit test against synthetic fixtures or shipping the change and watching production. Neither answers "does this behave differently than what's running now, and is the difference an improvement." A simulator closes that gap: replay real (or synthetic) traffic through a candidate implementation, compare it against the incumbent, and report where and by how much they diverge.

The requirement is narrower than "simulate the whole queue": every stage — validation, conflict detection, scoring, speculation, building, merging — must be independently swappable and comparable, not just the system as a whole. That shapes the design more than the end-to-end case does.

Three distinct uses pull on it, and they are worth separating because they need different machinery and carry different risk. **Comparison** asks whether a candidate implementation produces different output than the incumbent on the same input; it needs a corpus and a diff, and it is nearly free of modelling assumptions. **Benchmarking** asks whether the difference improves the system; it needs a running pipeline, a model of build outcomes, and a defensible objective function, and every one of those introduces error. **Correctness exercise** asks whether the system upholds its invariants under adversarial scheduling; it needs no corpus and no historical data at all, and it catches a class of bug that neither of the others touches.

## Three layers

**Layer 1 — extension replay.** Every behavioral seam in this codebase is a `Factory` interface with an identity-in, resolve-internally contract (`conflict.Analyzer`, `Scorer`, `Speculator`, `BuildRunner`, `Validator`, `Merger` — see [extension-contract.md](extension-contract.md)). That uniformity means one mechanism covers all of them: record (identity, output) pairs for a stage, then replay the same identities through a candidate implementation and diff its outputs against the recorded baseline. This runs in isolation — no other stage, no queue, no storage wiring beyond what the stage's own resolver needs — so it is fast enough to run per change, and it answers a narrower, more trustworthy question than an end-to-end run: does this stage's *output* differ, given identical input, independent of what anything downstream would have done with it.

**Layer 2 — controller replay.** A large share of the correctness logic lives in controllers rather than extensions: per [speculation.md](speculation.md), a speculation run is "all controller code, except step 3." Finalization, refutation, output validation, and the merge gate are controller-owned and unreachable from Layer 1, while Layer 3 is too heavy to run on every change. Layer 2 fills that gap by feeding a recorded queue snapshot to a controller and diffing the actions and writes it emits. It is unusually tractable here because the speculate run is deliberately built as a pure function of one read — "every run recomputes the whole queue from that single read," with no dependence on any earlier run — which is exactly the property replay wants. Controllers whose behavior is similarly snapshot-determined can join the same mechanism.

**Layer 3 — system simulation.** Some questions are about consequences rather than verdicts: does a less conservative `conflict.Analyzer` actually reduce land time once batching and speculation react to the looser dependency graph? That requires the real pipeline running forward — gateway, orchestrator, and Runway wired together in one process, in-memory storage and message-queue backends standing in for MySQL (the only backend either extension has today), with fake implementations only at the boundaries that talk to genuinely external systems (source control, CI, git). Layer 3 is where knock-on effects on land-time and cost distributions become visible, and it is also the only layer that can exercise the concurrency invariants described under [Invariants and fault injection](#invariants-and-fault-injection).

The layers share one idea — a recorded corpus of (identity, output) pairs — and differ in what they do with it: Layer 1 diffs a candidate against the corpus directly, Layer 2 replays a snapshot through a controller, Layer 3 runs the real controllers forward, consulting the corpus or a candidate at each stage as it goes.

## Reproducibility per stage

Not every stage's output can be regenerated on demand. The distinction decides whether a stage belongs in the "recompute and cache" bucket or the "read the audit trail, never regenerate" bucket — and only the former lets replay run against arbitrary candidate implementations rather than a single frozen historical answer.

| Stage | Extension | Reproducible? | Why |
|---|---|---|---|
| validate | `Validator` | Yes | Resolves change content at a fixed commit; deterministic given that identity. |
| validate (dry-run) | `Merger.CheckMergeability` | Yes, if the checked base is pinned | The git operation is a pure function of two fixed commits. Production resolves the base live (the target branch's current tip), which looks non-deterministic only because "current tip" moves — pin the specific base sha that was actually checked, recorded as part of the identity, and it is exactly as reproducible as any other git operation. |
| batch | `conflict.Analyzer` | Yes | [`pathoverlap`](../../submitqueue/extension/conflict/pathoverlap/pathoverlap.go) resolves each batch's changed paths through an injected `changeset.Resolver` — a pure function of git shas, cacheable the same way changed-target lookups are. |
| speculate | `Scorer` | Yes | [`heuristic.Score`](../../submitqueue/extension/scorer/heuristic/scorer.go) resolves a batch's changes through the same kind of resolver and buckets a static value function against static config — no live or mutable input. |
| speculate | `Speculator` | Yes | Deterministic given the batch and dependency-graph state and the Scorer's output; no external dependency. |
| build | `BuildRunner` | **No** | Pass/fail and duration depend on the CI backend's condition at execution time — infra load, flakiness — which no identity pins. See [The build oracle](#the-build-oracle). |
| merge | `Merger.Merge` | Yes, for the mechanical part | Computing the resulting commit for a given (pinned base, change, strategy) is a pure git operation, replayable the same way `CheckMergeability` is. |

Merge deserves a specific note, because the naive assumption is that concurrent pushes make it a race like build's infra-dependence. They do not, by construction: [`merge.go`](../../submitqueue/orchestrator/controller/merge/merge.go) publishes to Runway's merge topic partitioned by `batch.Queue`, and [`platform/consumer`](../../platform/consumer/consumer.go) dispatches each partition key to exactly one goroutine draining it in order — every merge for a given queue runs through one serial worker, never two at once. The base commit for the Nth batch in a queue is deterministically whatever the (N-1)th produced, recoverable from the sequential history with no ambiguity. The only residual risk is a push to the same branch from outside SubmitQueue entirely — a hotfix, another automation, a branch-protection bypass — which is an operating assumption to state explicitly ("no unmanaged pushes during the replay window"), not an architectural gap this design needs to close.

Only `BuildRunner` is genuinely irreproducible. Everything else can be recomputed rather than merely looked up, which is what makes candidate-versus-baseline replay possible at all for those stages.

## The build oracle

A replayed run has a recorded build result only when that exact path was built in history. Speculation means most paths a candidate explores never were, and any change to conflict analysis or batching produces batch compositions that never existed. The simulator therefore needs an explicit model of "would this have passed, and how long would it have taken." That model is the largest single source of error in any system-level number the simulator produces, so it belongs in the design as a named, swappable component rather than an implicit assumption — the same way the analyzer and speculator are.

The recommended default is **failure attribution**: mine per-change outcomes from history, classify each change as good or bad from the builds it appeared in, and fail any batch containing a bad change. Duration is modelled separately and more crudely — the longest recorded duration among a batch's members, or a draw from the recorded distribution for batches of that size. Attribution is cheap, needs only data the audit trail already holds, and degrades honestly.

Its blind spot must be stated loudly: two changes that each pass alone can still fail together, and those interaction failures are exactly the semantic conflicts that conflict analysis exists to catch. An oracle built on attribution cannot see them, so it **systematically flatters an aggressive conflict analyzer**. Any simulator result claiming a looser analyzer is safe has to be qualified by that bias, or corroborated by a mechanism that can observe interaction failures at all — shadow execution, or a post-hoc build of the combined pair. Modelling build outcomes from historical results rather than re-executing them is the same approach Uber's earlier internal merge queue took, and it hit the same limitation.

## Corpus collection

Collecting continuously does not require instrumenting every extension call on the hot path. Most of what a corpus needs already accumulates as a side effect of normal operation, through the storage extension every queue already writes to:

- [`entity.RequestLog`](../../submitqueue/entity/request_log.go) — the full state-transition audit trail, already timestamped.
- [`entity.Batch`](../../submitqueue/entity/batch.go) — its dependency list is `conflict.Analyzer`'s actual output for that batch.
- [`entity.Build`](../../submitqueue/entity/build.go) — `BuildRunner`'s outcome and duration, the one value that must come from here rather than being recomputed.
- [`entity.SpeculationPathSet`](../../submitqueue/entity/speculation.go) — the `Speculator`'s resulting path choices.

What is missing is narrower than a general recorder: the *resolved content* a deterministic stage used (changed paths, changeset detail) is not stored, but per the table above it is reconstructible from an identity that is. A resolve-and-cache layer in front of `changeset.Resolver`, keyed on that identity so repeated replays against different candidates do not re-resolve the same content, covers every deterministic stage.

`Scorer`'s raw output is the one value with no persisted trace — only the `Speculator`'s downstream path choice survives. Because `Scorer.Score` is itself deterministic, historical scores remain recomputable by resolving the batch and re-scoring with the baseline configuration. The gap only bites if a future scorer stops being a pure function of resolved batch content, at which point it needs an explicit persisted field.

**Provenance and versioning.** A recorded output is a valid baseline only if it is known what produced it. Every corpus entry carries the implementation name and version, the queue configuration it ran under, and the commit that built it. Interface drift is not hypothetical in a repository under active development, and a corpus that outlives an interface change must be rejected loudly at replay time rather than silently misattributed.

**Storing identity, not content.** The mining approach has a security property worth making explicit: the corpus holds identities — change URIs, commit shas, path keys — while content is re-resolved at replay time from the source of truth. No durable second copy of proprietary source accumulates, and retention policy applies to a much smaller and less sensitive artifact.

**Tiering.** Three corpus sizes keep the tool usable: a small one checked into the repository that runs in seconds on every change, a medium one exercised nightly, and the full historical corpus available on demand for changes that warrant it. Without the small tier, the machinery exists but nobody drives it.

### Shadow recording

For any stage where a candidate implementation already exists, recording is strictly better than replaying: run the candidate in production alongside the incumbent on live traffic, record both outputs, and act only on the incumbent's. The comparison is then against real traffic with no counterfactual, no resolve-and-cache layer, and no fidelity question at all — and the recording *is* the corpus.

This also unifies two things this document otherwise treats separately. If collection runs continuously anyway, collecting *several* implementations concurrently makes N-way comparison a byproduct rather than a feature, and reduces replay to the case that genuinely needs it: an implementation that did not exist when the data was recorded.

## Evaluating conflict detection

Conflict analysis is the driving use case, and it needs a different notion of "diff" than the other stages. Two analyzers disagreeing tells you *where* they differ, not *which is right*. The useful frame is precision and recall over two error classes with sharply asymmetric costs:

- A **false negative** — the analyzer misses a real conflict — lets a broken combination onto the trunk. It is approximable from history: a trunk build failure, revert, or fix-forward shortly after a batch lands is the observable signature.
- A **false positive** — the analyzer reports a conflict that would not have materialised — costs throughput through unnecessary serialization. It has no historical signature, because the counterfactual was never run, but it can be estimated after the fact by merging and building the pair that was held apart.

A candidate should be reported against both, never against agreement rate alone: an analyzer that agrees with the incumbent 99% of the time may still be strictly worse if its 1% of disagreements are all false negatives. This is also where the build oracle's blind spot binds hardest, and the strongest argument for shadow execution over pure replay when evaluating a new analyzer.

## What to measure

A comparison needs a decision rule, and the obvious metric is a trap. **Throughput is largely uninformative under fixed-trace replay**: the queue drains whatever the trace delivers, so total landed count is a property of the input, not the policy. It becomes meaningful only under saturation, or when a policy changes the failure rate enough to alter how many requests land at all. Reporting it as a headline number invites false confidence.

The axes that actually move are:

- **Latency** — land-time distribution, reported at p50, p95, and p99, per queue.
- **CI cost** — builds started per landed change, the currency speculation spends to buy latency.
- **Safety** — escaped conflicts and trunk breakages, per [Evaluating conflict detection](#evaluating-conflict-detection).
- **Fairness** — worst-case land time and per-request-size breakdown. A policy that improves the median by starving large changes looks good and is not.

Latency and cost trade against each other, so a single scalar verdict hides the trade. Report the pair; where the question is a policy rather than a bug fix, sweep the build budget so each candidate is a curve rather than a point.

One structural constraint on any system-level report: once two runs diverge they have different batches, different paths, and different builds, with no correspondence to diff. **The request is the only identity that survives divergence**, so per-request outcomes are the finest granularity at which two Layer 3 runs can be compared; everything else is compared as a distribution.

## Comparison model

Start pairwise — baseline against one candidate on the same corpus — with the report shaped as a table keyed by (identity, variant) so that N-way is the general case rather than a rewrite. The variant axis is deliberately generic: it holds competing implementations, or the same implementation at different parameter values. That makes a parameter sweep and an N-way implementation comparison the same operation over the same report shape, which matters because sweeps (build budget, arrival multiplier, conflict granularity) are how most policy questions are actually answered.

Because the timing model below is deliberately not deterministic, two runs of the same configuration differ. Every Layer 3 comparison therefore needs repetition and an interval, and no improvement or regression should be reported that falls inside the run-to-run band. Synthetic workloads take an explicit seed so a run can be reproduced exactly.

## Trusting the simulator

A simulator that produces confidently wrong numbers is worse than none, because people act on it. The design therefore includes a standing **backtest**: replay a historical window with the baseline configuration unchanged and compare the simulator's output against what actually happened in that window. A simulator that cannot reproduce reality when nothing has changed cannot be trusted to predict the effect of a change. This runs continuously rather than once, and its residual error is published alongside every result — an improvement smaller than the simulator's own error against reality is not a result.

Two fidelity limits cannot be engineered away and are documented instead:

**Arrivals are not exogenous.** Replay treats the arrival stream as independent of queue performance, and it is not. Faster landing changes how people submit, stack, retry, and abandon work; historical cancellations are the sharpest case, because they happened when a person lost patience at a particular moment and would not have happened under a faster policy. Fixed-trace replay consequently overstates the benefit of an improvement. The mitigation is to report sensitivity across several arrival-rate multipliers rather than a single trace speed.

**The build oracle is a model, not a record.** Its bias is described above; it applies to every Layer 3 number that depends on a batch composition history never built.

## Invariants and fault injection

The strongest guarantees in this repository are statements about behavior under adverse scheduling — idempotency, optimistic concurrency, persist-before-publish, at-least-once delivery. A Layer 3 harness whose in-memory backends only ever behave *nicely* exercises none of them, and will report a green run for code that breaks in production on the first redelivery.

Two capabilities close that gap, and both are cheap once Layer 3 exists. **Fault injection** lets the in-memory backends behave adversarially within contract: duplicate deliveries, reorder across partitions, expire visibility timeouts, lose compare-and-swap races, fail storage calls, and stall builds past their timeout. **Continuous invariant checking** asserts throughout a run rather than only at the end:

- A batch never reaches Succeeded without a passed path whose assumptions match how every dependency actually resolved.
- The build budget is never exceeded.
- Terminal states are terminal — nothing transitions out of one.
- No change lands twice.
- Every request eventually reaches a terminal state.

Together with a synthetic workload generator this is a property-based tester for the concurrency design, and it covers ground no other layer of testing in the repository reaches today.

## Timing model

Layer 1 and Layer 2 need no timing model — both are input-to-output diffing. Layer 3's latency metrics do, and the two ways to get there trade differently.

Uber's earlier internal merge queue replayed a full historical trace with correct relative timing in a fraction of real time, using a virtual clock: a discrete-event loop over a priority queue of timestamped work items, advancing an in-memory current instant from event to event, with nothing ever sleeping. A day of recorded traffic replays in the time it takes to process its events, and every elapsed-duration measurement stays correct because it is computed against virtual timestamps. That worked cleanly because the simulator was single-threaded — one agenda, nothing to coordinate.

This orchestrator is not. `platform/consumer` dispatches per-partition-key work across real goroutines and channels, and virtualizing time cleanly under real concurrency is a materially harder problem: full determinism means replacing the scheduler, not just the clock, which is closer to a deterministic-simulation framework than to a clock injection.

The recommendation is to virtualize only what models elapsed time under our own control — the build oracle's modelled duration and the workload generator's inter-arrival scheduling — through a shared logical clock, and accept that the surrounding plumbing runs in real but fast wall-clock time. This is not bit-for-bit deterministic, which is why comparisons carry intervals, but it delivers the practical win of replaying a large trace in a small fraction of the time it took to record. Full determinism is worth scoping separately if it proves necessary; it should not block a first working version.

## Delivery

Ordered by value delivered per unit of assumption taken on, which is not the order the layers are numbered in:

1. **Layer 1 for `conflict.Analyzer`**, baselined on mined batch dependencies with the resolve-and-cache layer beneath it. The smallest useful thing, and directly the mechanism a new analyzer needs on arrival.
2. **Shadow recording** for stages with a live candidate, which upgrades the corpus from historical to real-traffic and makes N-way comparison fall out for free.
3. **Layer 3 skeleton with synthetic workload, invariant checking, and fault injection.** This needs no corpus, no oracle, and no fidelity argument, so it is unblocked immediately — and it catches a class of concurrency bug nothing else in the repository tests.
4. **Trace replay, the build oracle, the metric set, and the backtest.** The benchmarking capability proper, which depends on every modelling assumption in this document and should not be trusted before the backtest exists.
5. **Layer 2 controller replay**, which can slot in any time after (1).

## Rejected

- **A standalone simulation model of the pipeline.** Far faster to write than wiring the real services, and divergent from the code it claims to model within a release or two. The harness runs the real controllers and extensions against substituted backends instead, so pipeline logic under test is the shipping logic.
- **Instrumenting every extension call in production.** The obvious way to build a corpus, and largely redundant: the entities normal operation already persists carry most of what replay needs. A new hot-path write path for data that is already durable costs latency and buys little.
- **Re-executing builds during replay.** Faithful for the paths history actually built, impossible for the rest, and prohibitively expensive at trace scale. A modelled outcome with a declared bias is the honest trade — see [The build oracle](#the-build-oracle).
- **Full deterministic scheduling.** The strongest fidelity story available, but it means replacing the scheduler rather than injecting a clock, which is a project in its own right. Repetitions and reported intervals absorb the residual noise at a small fraction of the cost.
- **Synthetic workloads only.** Hermetic, cheap, and the right basis for the correctness work, but the benchmarking questions turn on conflict structure and arrival bursts that only real traffic exhibits. Synthetic generation stays; it does not stand alone.
- *Acknowledged:* every Layer 3 number depends on the build oracle, and no amount of harness engineering removes that dependency. The backtest bounds the error rather than eliminating it, which is why the simulator is positioned as a comparison instrument and not a forecast.

## Non-goals

- Wiring this into Uber's internal wrapper around this repository. That wrapper's `conflict.Analyzer` is still the conservative "everything conflicts" fake, so simulating against it is uninformative until a real analyzer is wired in. The design here is meant to carry over unchanged when that happens, because the wrapper's extensions are the same interfaces this document reasons about.
- Replaying traffic sourced from outside this repository's own persistence and resolvers.
- Predicting absolute production numbers. The simulator is a comparison instrument; its output is a delta between variants under identical assumptions, not a forecast.

## Open questions

- Corpus storage backend — object storage, a queryable warehouse table, or an abstract sink interface that defers the choice to implementation.
- Whether recording is opt-in per queue or always on, and what retention the corpus warrants given it holds identities rather than content.
- Whether shadow recording should be a permanent production capability or a temporary mechanism stood up per evaluation.
- How the false-positive estimate for conflict analysis is produced in practice, given it requires building pairs the queue deliberately kept apart.
- Which controllers beyond speculate are snapshot-determined enough to join Layer 2.
