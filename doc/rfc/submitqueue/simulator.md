# Simulator

Design notes for a simulator that verifies and benchmarks changes to orchestrator logic before they ship: per-extension record and replay, controller replay, and whole-pipeline simulation. This document captures **design decisions and rationale only**; the code lands after this RFC is reviewed.

## Problem

Changing orchestrator logic today leaves two options: trust a unit test against synthetic fixtures, or ship the change and watch production. Neither answers the question that matters. Does this behave differently from what is running now, and is the difference an improvement?

A simulator closes that gap. It replays real or synthetic traffic through a candidate implementation, compares the result against the incumbent, and reports where and by how much the two diverge.

The requirement is narrower than "simulate the whole queue." Every stage — validation, conflict detection, scoring, speculation, building, merging — must be independently swappable and comparable, and so must the seams inside a stage. Speculation is the case that makes this concrete: [speculation.md](speculation.md) describes two pluggable components, a Generator that enumerates candidate paths and an Allocator that funds them, and a change to one is not a change to the other. A harness that could substitute only the whole `Speculator` could evaluate neither. That constraint shapes the design more than the end-to-end case does.

Two uses pull on it. They need different machinery and carry very different risk, so they are worth naming separately:

- **Comparison** asks whether a candidate produces different output from the incumbent on the same input. It needs a corpus and a diff, and almost no modeling assumptions.
- **Benchmarking** asks whether that difference improves the system. It needs a running pipeline, a model of build outcomes, and a defensible objective function. Every one of those introduces error.

## Scope

The simulator models SubmitQueue and Runway, which together form a closed pre-merge loop. Runway is stateless — no request store, no database, idempotency derived from the VCS — so the harness substitutes storage for one domain only.

Stovepipe is out of scope by design rather than by convenience. It is a post-merge trunk-health poller with no coupling to SubmitQueue in either direction, and its only outputs are hooks, whose contract states that hook outcomes never write pipeline state. Nothing Stovepipe learns can reach the queue. Escaped conflicts, where changes pass individually and break together, surface inside SubmitQueue's own build stage, so measuring them needs no post-merge modeling.

One hazard does live in the gap between the two. Runway performs transforming merges: rebase and squash-rebase rewrite commits, so the tree that lands is not always the tree that was validated. A break introduced by the transform itself is invisible to pre-merge validation, and therefore to any simulation of it. That is a limit on what the safety metric can claim, not a component to model.

Concurrency-correctness work is also out of scope — invariant assertions, fault injection, and controlled interleaving exercise the delivery contract rather than orchestrator policy, need no corpus and no build oracle, and belong with end-to-end testing.

## The unit of substitution

Swappability is defined at the finest seam a queue can wire independently, not at the granularity of a pipeline stage.

| Stage | Substitutable seams |
|---|---|
| validate | `Validator`; `MergeChecker`, designed but not yet wired |
| batch | `conflict.Analyzer`, and the path projection it is constructed with |
| speculate | `Generator` and `Allocator`, each on its own and jointly as the `Speculator` composing them; the `Scorer` the Generator is built with |
| build | `BuildRunner` |
| merge | `Merger`, which is Runway's rather than SubmitQueue's, and the default strategy it is constructed with |

A variant may differ in as many seams as the question needs. Pinning all but one is what makes a difference *attributable*: comparing two Generators under the same Allocator and Scorer is the only way to read the ranking-quality and starvation [diagnostics](#diagnostics) apart. The seam list belongs to the extension contract rather than to this document, and grows with it.

A seam must be substitutable on its own. Nothing can supply a `Generator`'s `Iterator` without supplying the Generator, so the Generator is the unit; construction parameters such as the path projection are knobs rather than seams.

Storage, queue configuration, and change providers are substituted as infrastructure rather than as variants: what the harness replaces in order to run, not what it compares.

## Three layers

**Layer 1 — extension replay.** Every seam is a `Factory` taking identity and resolving what it needs internally (see [extension-contract.md](extension-contract.md)), so one mechanism covers all of them, and a composed seam is reached by assembling it from a candidate part and incumbent ones. Record the (identity, output) pairs an extension produced, replay those identities through a candidate, and diff. Nothing else runs — no other stage, no queue, no storage beyond the extension's own resolver — which is what makes it cheap enough for every change and its answer hard to contaminate.

**Layer 2 — controller replay.** Finalization, refutation, output validation, and the merge gate are controller-owned, so Layer 1 cannot reach them and Layer 3 is too heavy to run on every change. Layer 2 feeds a recorded queue snapshot to a controller and diffs the actions and writes it emits. A speculate run is a pure function of a single read, so a snapshot is a complete input; other snapshot-determined controllers can use the same mechanism.

**Layer 3 — system simulation.** Some questions are about consequences rather than verdicts: does a looser `conflict.Analyzer` reduce land time once batching and speculation react to the changed dependency graph? That needs the real pipeline running forward — gateway, orchestrator, and Runway in one process, in-memory storage and message queues in place of MySQL, and fakes only at the boundaries reaching source control, CI, and git.

Isolation is a property of the wiring rather than of configuration: a call reaching a boundary the harness has not faked fails the run. Intercepting only the boundaries known when the harness was written fails quietly, because the first new dependency anyone adds reaches production unreported.

## How it works

| Component | Responsibility | Layer |
|---|---|---|
| Corpus store | identities, recorded inputs and outputs, provenance | all |
| Recorder | mines history, records at the seam, or shadows in production | all |
| Replayer | drives one extension from corpus identities | 1 |
| Snapshot replayer | drives one controller from a recorded queue snapshot | 2 |
| Harness | wires gateway, orchestrator, and Runway in-process over substituted backends | 3 |
| Build oracle | pass/fail and duration for compositions never built | 3 |
| Logical clock | virtualizes modeled durations and arrival scheduling | 3 |
| Comparator | normalizes expected variation, diffs, classifies the outcome | all |
| Reporter | aggregates to a table keyed by (identity, variant), with intervals | all |

A Layer 1 run:

```
1 select   corpus tier and filter, from selection-as-code
2 load     identity and recorded inputs per entry, from the change and batch stores
3 run      incumbent → baseline output
           candidate → candidate output        ◀── the only variant difference
4 verify   the incumbent reproduces its recorded output, or the entry is a corpus fault
5 compare  normalize expected variation on both sides, diff, classify the outcome
6 report   per (identity, variant), with mismatches counted by field path
```

A Layer 3 run:

```
1 construct  real controllers; in-memory storage and queues; fakes at source
             control, CI, and git; oracle and logical clock injected
2 schedule   arrivals onto the logical clock from the trace
3 drive      gateway accepts → the pipeline runs forward
               each extension call resolves to the configured variant
               the build stage asks the oracle for pass/fail and duration
               the clock advances to the next scheduled event
4 collect    per-request outcomes, and diagnostics per seam
5 repeat     to the harness's fixed repetition policy, stochastic inputs held common
6 report     outcome distributions with intervals, diagnostics alongside
```

Layer 2 sits between the two: it loads one recorded queue snapshot, runs a single controller against it, and diffs the actions and writes that controller emits.

## What a difference means

A replay produces one of three outcomes, and collapsing them into pass and fail destroys the instrument.

- **Harness fault.** The run did not complete: a fake failed, a resolver timed out, a candidate panicked. Says nothing about either implementation.
- **Corpus fault.** The run completed, but the baseline no longer reproduces its own recorded output. Every candidate number derived from that entry is void.
- **Behavioral difference.** Both implementations ran and disagreed. This is the measurement, not an error.

The third is why output equality cannot be the pass condition: a candidate `conflict.Analyzer` reporting different conflicts is doing the thing it was written to do, and a harness built around equality would report every interesting change as broken. What must hold instead is narrower — a candidate is free to change what an extension decides, and not free to change how the pipeline responds to a decision.

## Reproducibility per extension

An extension reproducible from a pinned identity can be replayed against any candidate, including on inputs history never saw. One that is not can only be compared against the single answer history recorded. That decides how its corpus is collected (see [Corpus collection](#corpus-collection)).

| Extension | Reproducible? | Why |
|---|---|---|
| `Validator` | Yes | Resolves change content at a fixed commit. |
| `conflict.Analyzer` | Yes | [`pathoverlap`](../../submitqueue/extension/conflict/pathoverlap/pathoverlap.go) resolves changed paths through a `changeset.Resolver`, a pure function of git shas. |
| `Scorer` | Yes | [`heuristic.Score`](../../submitqueue/extension/scorer/heuristic/scorer.go) buckets a static value function over resolved content. No live input. |
| `Speculator` | Yes | Determined by the batch, the dependency graph, and the Scorer's output; the Generator and Allocator inherit the property. |
| `MergeChecker`, `Merger` | Yes, if the checked base is pinned | Pure git operations over fixed commits. They look non-deterministic only because production resolves the base against a moving tip; record the base sha as part of the identity and they replay exactly. |
| `BuildRunner` | **No** | Pass, fail, and duration depend on the CI backend's condition at execution time. No identity pins that. See [The build oracle](#the-build-oracle). |

`BuildRunner` is the only genuine gap, and it is the one whose output drives refutation and the merge gate downstream.

## The build oracle

A replayed run has a recorded result only for the paths history actually built, and both speculation and any change to batching or conflict analysis produce compositions that never existed. The simulator therefore needs a model of whether a build would have passed and how long it would have taken. That model is the largest single source of error in every system-level number it reports, so it is a named, swappable component rather than an implicit assumption.

The recommended default is **failure attribution**: classify each change as good or bad from the builds it appeared in, and fail any batch containing a bad change. `build` records a terminal status, so that much can be mined today.

Duration cannot. The `build` row carries no timestamps and no elapsed time, so there is no recorded distribution to draw from and no per-change duration to take a maximum over. Either the build stage starts recording it, or duration is derived from the gap between the trigger and the signal in the request log, which attributes queueing delay to the build. A latency benchmark depends on resolving that, and it is the second reason the [hook framework](../hook-framework.md) matters.

Its blind spot is directional. Two changes that each pass alone can still fail together, and those interaction failures are precisely the semantic conflicts conflict analysis exists to catch. Attribution cannot see them, so it **systematically flatters an aggressive conflict analyzer**, and an aggressive batcher for the same reason. It can miss an interaction failure but never invent one, so a safety claim must be qualified by that bias or corroborated by something that observes interactions directly: shadow execution, or building the withheld pair after the fact.

## Corpus collection

Continuous collection does not require instrumenting every extension call on the hot path, and normal persistence does not cover everything replay needs either. Three mechanisms apply. Which one an extension uses depends on what its persisted trace actually contains.

**Read from history** where persistence already holds what replay needs:

- [`entity.ChangeInfo`](../../submitqueue/entity/change_provider.go), in the `change` table — the changed paths, line counts, and author an extension consumed. `pathoverlap` and `heuristic.Score` reach these through `changeset.Resolver`, which reads the change store rather than fetching live, so their inputs are already durable.
- [`entity.Build`](../../submitqueue/entity/build.go) — the build's terminal status, the one value that can only come from here.
- [`entity.RequestLog`](../../submitqueue/entity/request_log.go) — the append-only, timestamped state-transition trail, from which land times are derived.

The request log is the interim trace source. The [hook framework](../hook-framework.md) designs a durable, cross-domain event on every lifecycle transition whose outcomes never write pipeline state, which is a purpose-built observation point. It is unbuilt, so the corpus should anticipate the switch rather than encode request-log specifics.

**Record at the seam** where the persisted form is a lossy projection rather than the output itself. `conflict.Analyzer` is the case that matters, and the loss is easy to miss. The analyzer returns `[]entity.Conflict`, each carrying a batch ID *and* a `ConflictType`, and the batch controller reduces that to a list of IDs before storing it ([`batch.go`](../../submitqueue/orchestrator/controller/batch/batch.go)). Three things go missing in the reduction:

- the `ConflictType` is erased;
- repeated entries for one pair collapse;
- and only batches that happened to be in flight were evaluated.

[`entity.Batch`](../../submitqueue/entity/batch.go)'s dependency list is therefore strong evidence of what the *system did*, which is what Layer 3 needs, and a weak baseline for what the *analyzer said*, which is what Layer 1 compares. Capturing the raw return value at the seam is small, exact, and the same mechanism as [shadow recording](#shadow-recording).

The analyzer's *input* needs recording for a different reason: it is handed the batches in flight at that moment, and `batch` rows are mutated in place with no timestamps. History therefore retains each batch's final state and no way to reconstruct which were in flight when.

**What persistence does not keep is time.** Every table but `request_log` holds current state, overwritten as it changes: `batch` carries no timestamps, `speculation_path_set` keeps only the latest set per head, and `build` records a terminal status with no duration. The corpus can reconstruct what the system ended up with, and cannot reconstruct the sequence that produced it. Anything a benchmark needs about *when* — build duration, the order paths were funded, how long a batch waited — has to be recorded rather than mined. That is the strongest argument for the hook framework above, and until it exists these are the measurements the harness cannot take.

Two cases need the incumbent recomputed even where history holds an answer. **Counterfactual compositions** have no stored output at all, so the incumbent runs fresh alongside the candidate. And **re-deriving a baseline history does hold** is the corpus's integrity check: if the incumbent no longer reproduces its own stored value, the inputs are stale and every number from that entry is void.

**Regenerate rather than maintain.** A corpus of checked-in fixtures rots, and every system that keeps one refreshes it by hand after someone notices a test failing for the wrong reason. Treat it as derived instead: keep a small, version-independent description of what to exercise, and re-author entries from it against whichever implementation is under test. Exactly one artifact then crosses a revision boundary — the recorded (identity, output) pair, in a format carrying a declared version — which is what keeps versioning honest.

Every entry still carries its provenance: the implementation name and version, the queue configuration, and the commit that built it. Interface drift is not hypothetical in a repository under active development, and an entry that outlives an interface change must be rejected loudly at replay time rather than silently misattributed. Entries mined from history cannot be regenerated and have only that guarantee.

**Publish nothing that does not reproduce itself.** Before an entry enters the corpus, the implementation that authored it must replay it and reproduce it. This is the integrity check above applied at write time rather than read time. It is what lets a corpus fault be reported as its own outcome, instead of surfacing later as a phantom regression against an unrelated candidate.

**Identity.** Entries key on the canonical change URI defined in [change-uri.md](../change-uri.md). That contract has exactly one valid spelling per change, with full lowercase SHAs and verbatim path segments, and its parsers validate rather than normalize. The corpus must not invent a normalization the rest of the system rejects.

**Bounded content.** The corpus holds identities and the derived metadata an extension consumed — change URIs, commit shas, changed-path sets — never file contents or diffs, so retention applies to a small and comparatively insensitive artifact.

**Tiering.** A small corpus versioned with the repository and running in seconds on every change, a medium one nightly, and the full history on demand. The small tier versions the description rather than frozen outputs, so it stays subject to the regeneration rule above. Without it the machinery exists but nobody drives it.

**Selection as code.** The filters that define a corpus, such as which states, which queues, and which window, are sampling decisions that bias every result derived from it. They belong in versioned code, reviewable and diffable like any other input, not in a query pasted into a runbook.

### Shadow recording

For any extension where a candidate implementation already exists, recording beats replaying. Run the candidate in production alongside the incumbent on live traffic, record both outputs, and act only on the incumbent's. The comparison is then against real traffic, with no counterfactual and no fidelity question, and it captures the inputs and timing that persistence does not keep. The recording *is* the corpus.

Collecting several implementations concurrently then makes N-way comparison a byproduct, and reduces replay to the case that genuinely needs it: an implementation that did not exist when the data was recorded.

Shadow execution needs a structural guard rather than a convention: a shadow-mode call carries an explicit allowlist of what it may execute, and anything outside it returns without acting. Read-only extensions are safe to shadow directly. Anything that writes is not — running `Merger.Merge` twice does not produce a comparison, it produces two merges — so write-side changes are validated by a staged rollout instead.

## Evaluating conflict detection

Conflict analysis is the driving use case, and two analyzers disagreeing tells you *where* they differ, not *which is right*. The useful frame is two error classes whose costs are sharply asymmetric:

- A **false negative**, missing a real conflict, lets a broken combination onto the trunk. It is approximable from history: a trunk build failure, revert, or fix-forward shortly after a batch lands is the observable signature.
- A **false positive**, reporting a conflict that would never have materialized, costs throughput through needless serialization. It has no historical signature, since the counterfactual never ran, but can be estimated after the fact by merging and building the pair that was held apart.

Report a candidate against both, never against agreement rate alone: an analyzer agreeing with the incumbent 99% of the time is strictly worse if its 1% of disagreements are all false negatives. This is also where the build oracle's bias bites hardest, and the strongest argument for shadow execution over pure replay here.

## What to measure

The obvious metric is a trap. **Throughput is largely uninformative under fixed-trace replay**: the queue drains whatever the trace delivers, so total landed count is a property of the input rather than the policy. It becomes meaningful only under saturation, or when a policy changes the failure rate enough to alter how many requests land at all.

**Outcomes** decide whether a candidate is better. **Diagnostics** explain why it moved, and localize a regression.

### Outcomes

- **Latency** — land-time distribution per queue, from p50 through p99. Merge-queue latency is long-tailed and often bimodal, so the middle of the distribution carries information that three widely-spaced percentiles hide.
- **CI cost** — builds started per landed change, the currency speculation spends to buy latency.
- **Safety** — escaped conflicts and trunk breakages, per [Evaluating conflict detection](#evaluating-conflict-detection).
- **Fairness** — worst-case land time, broken down by request size. A policy that improves the median by starving large changes looks good and is not.

Latency and cost trade against each other, so a single scalar verdict hides the trade. Report the pair. Where the question is a policy rather than a bug fix, sweep the build budget so each candidate is a curve rather than a point.

Report these bucketed by hour as well as in aggregate. A policy that keeps up during peak and one that defers work into the quiet hours produce the same totals over a long enough window while behaving nothing alike.

### Diagnostics

These are defined against this system's own model — heads, paths, dependency assumptions, and a queue-wide build budget — so they do not carry over from a queue whose speculation ran linearly over queue order.

- **Waste** — budget-time spent on paths whose result never reached a verdict. Not the same as cancelled builds: a cancelling path charges the budget until terminal, and a path can pass and still be wasted when a dependency later resolves against its assumption.
- **Refutation rate** — how often a resolved dependency breaks an assumption, which reads prediction quality directly.
- **Dependency cost** — the interval between a head having a passing build and that head merging. The merge gate is strict, so this is exactly what the dependency graph costs, and what a looser `conflict.Analyzer` sets out to reduce.
- **Coverage** — whether the funded paths span every combination of dependency outcomes, and the **bypass rate** it enables.
- **Ranking quality** — the interval between the first path funded for a head and the funding of the path that passed, isolating the Generator's ranking from whether the outcome was right. Measured as that timing gap rather than by comparing scores, which are recomputed per run and not comparable across runs.
- **Starvation** — its cross-head counterpart: whether the Allocator spreads budget or lets one head monopolize it.
- **Pipeline health** — dead-letter rate, per-stage queue lag, and compare-and-swap contention.

Once two runs diverge they have different batches, paths, and builds, with nothing to line up one-to-one. **The request is the only identity that survives divergence**, so per-request outcomes are the finest granularity at which two Layer 3 runs can be compared, and everything else is compared as a distribution.

## Comparison model

Start pairwise, with the report shaped as a table keyed by (identity, variant), so N-way is the general case rather than a rewrite. The variant axis is deliberately generic: competing implementations, or one implementation at different parameter values. A sweep and an N-way comparison become the same operation over the same report shape, which matters because sweeps over build budget, arrival multiplier, or conflict granularity are how most policy questions get answered.

Levers the production code hard-codes belong on that axis too — a sweep over a constant is exactly the evidence that would justify making it configurable.

The timing model below is deliberately not deterministic, so two runs of the same configuration differ. Every Layer 3 comparison therefore needs repetition and an interval, and no result falling inside the run-to-run band should be reported. Synthetic workloads take an explicit seed so a run can be reproduced exactly.

How many repetitions, and over which windows, is fixed by the harness rather than chosen per analysis; intervals chosen per analyst cannot be compared across two people's results. Replicating across several historical windows matters at least as much as replicating within one, since between-window spread is what shows whether a result generalizes past the day it was measured on.

Stochastic inputs are held common where possible. Two policies compared over the same window draw the same build-oracle outcomes, so the difference reflects the policy rather than the luck of the draw.

## Trusting the simulator

A simulator that produces confidently wrong numbers is worse than none, because people act on it. The design therefore includes a standing **backtest**: replay a historical window with the baseline configuration unchanged, and compare the simulator's output against what actually happened in that window. A simulator that cannot reproduce reality when nothing has changed cannot be trusted to predict the effect of a change. This runs continuously rather than once, and its residual error is published alongside every result. An improvement smaller than the simulator's own error against reality is not a result.

The backtest windows are a fixed, versioned set rather than a fresh sample each time. Re-drawing them makes the error figure incomparable between runs, and what matters is whether accuracy is drifting, which is only visible against a stable set. Choose windows that include the awkward periods — an incident, a backlog, a burst — not only the calm ones.

A second and stronger check is available, because policy changes ship here regularly. Every real change to queue behavior is a natural experiment: run the simulator on the window before the change and compare its prediction against the shift observed afterward. Accumulating those cases is the only evidence that answers whether the simulator has ever been right about something that mattered, and the set is worth keeping permanently.

Two fidelity limits cannot be engineered away, and are documented instead.

**Arrivals are not exogenous.** Replay treats the arrival stream as independent of queue performance, and it is not. Faster landing changes how people submit, stack, retry, and abandon work. Historical cancellations are the sharpest case: they happened because a person lost patience at a particular moment, and would not have happened under a faster policy. Fixed-trace replay consequently overstates the benefit of an improvement. The mitigation is to report sensitivity across several arrival-rate multipliers rather than a single trace speed.

**The build oracle is a model, not a record.** Its bias is described above, and it applies to every Layer 3 number that depends on a batch composition history never built.

## Timing model

Layer 1 and Layer 2 need no timing model, since both are input-to-output diffing. Layer 3's latency metrics do, and the two ways to get there trade differently.

Uber's earlier internal merge queue replayed a full trace with correct relative timing in a fraction of real time, using a virtual clock. That clock is a discrete-event loop over a priority queue of timestamped work items, advancing an in-memory instant from event to event with nothing ever sleeping. Elapsed durations stay correct because they are computed against virtual timestamps. That worked because the simulator was single-threaded.

This orchestrator is not. `platform/consumer` dispatches per-partition-key work across real goroutines and channels, so full determinism means replacing the scheduler rather than injecting a clock — a deterministic-simulation framework, and a project in its own right.

The recommendation is to virtualize only what models elapsed time under our own control: the build oracle's modeled duration and the workload generator's inter-arrival scheduling, through a shared logical clock. Those dominate a trace's elapsed time, so compressing them delivers the practical win while the surrounding plumbing runs in real but fast wall-clock time. It is not bit-for-bit deterministic, which is why comparisons carry intervals.

Accepting nondeterministic scheduling is not the same as accepting nondeterministic *outcomes*. When two units of work compete for a scarce resource, the winner must be settled by a stable key rather than by whichever goroutine arrived first. Build budget is exactly such a resource, and "which path took the last slot under pressure" is a question this simulator exists to answer. Relatedly, all randomness draws from one seed the run records. Both are cheap up front and awkward to retrofit; simulators elsewhere have shipped with a deterministic event core and a nondeterministic contention path underneath, precisely because the second was left to individual components.
