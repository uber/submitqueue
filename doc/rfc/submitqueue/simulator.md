# Simulator

Design notes for a simulator that verifies and benchmarks changes to orchestrator logic before they ship: per-stage record and replay, controller replay, and whole-pipeline simulation. This document captures **design decisions and rationale only**; the code lands after this RFC is reviewed.

## Problem

Changing orchestrator logic today leaves two options: trust a unit test against synthetic fixtures, or ship the change and watch production. Neither answers the question that matters. Does this behave differently from what is running now, and is the difference an improvement?

A simulator closes that gap. It replays real or synthetic traffic through a candidate implementation, compares the result against the incumbent, and reports where and by how much the two diverge.

The requirement is narrower than "simulate the whole queue." Every stage — validation, conflict detection, scoring, speculation, building, merging — must be independently swappable and comparable, and so must the seams inside a stage. Speculation is the case that makes this concrete: [speculation.md](speculation.md) describes two pluggable components, a Generator that enumerates candidate paths and an Allocator that funds them, and a change to one is not a change to the other. A harness that could substitute only the whole `Speculator` could evaluate neither. That constraint shapes the design more than the end-to-end case does.

Three uses pull on it. They need different machinery and carry very different risk, so they are worth naming separately:

- **Comparison** asks whether a candidate produces different output from the incumbent on the same input. It needs a corpus and a diff, and almost no modeling assumptions.
- **Benchmarking** asks whether that difference improves the system. It needs a running pipeline, a model of build outcomes, and a defensible objective function. Every one of those introduces error.
- **Correctness exercise** asks whether the system holds its invariants under adverse scheduling. It needs no corpus and no historical data at all, and it catches a class of bug the other two cannot see.

## Scope

The simulator models SubmitQueue and Runway, which together form a closed pre-merge loop. Runway is stateless — no request store, no database, idempotency derived from the VCS — so the harness substitutes storage for one domain only.

Stovepipe is out of scope by design rather than by convenience. It is a post-merge trunk-health poller with no coupling to SubmitQueue in either direction, and its only outputs are hooks, whose contract states that hook outcomes never write pipeline state. Nothing Stovepipe learns can reach the queue. Escaped conflicts, where changes pass individually and break together, surface inside SubmitQueue's own build stage, so measuring them needs no post-merge modeling.

One hazard does live in the gap between the two. Runway performs transforming merges: rebase and squash-rebase rewrite commits, so the tree that lands is not always the tree that was validated. A break introduced by the transform itself is invisible to pre-merge validation, and therefore to any simulation of it. That is a limit on what the safety metric can claim, not a component to model.

## Designing against the unbuilt system

Much of what makes this queue interesting is designed but not yet built, and the simulator is worth more as an instrument for designing those pieces than as a regression test for the pieces that already exist. Three are open policy questions with no obvious right answer, which is exactly the shape of question a simulator answers well.

**Batch grouping.** `Batch.Contains` is a list and the merge path already emits one step per member, but the batch controller writes a single-element slice every time, so a batch holds exactly one request today. The TODO above that code conditions grouping on capacity pressure: accumulate arriving requests into one batch when builds are scarce, or late-join a request into a batch that has not started. How long to wait and how large to let a batch grow are unanswered, and the cost of guessing wrong is failure attribution, since a failed batch of five is a single observation about five changes.

**Conflict relaxation.** Described in [speculation.md](speculation.md) as not implemented: drop the weakest dependencies so a head does not wait on them. Which dependencies are safe to drop is precisely a question of how much escaped-conflict risk buys how much latency.

**Bypass large diff.** A batch whose passed builds cover every way its dependencies could resolve can merge immediately, ahead of them. The Speculator covers that space when it is cheap to do so; the controller-side gate that would act on complete coverage is not built.

Other levers exist but cannot be tuned. The build budget, the allocator's only rationing lever, is a compiled constant of 4 shared by every queue, with a TODO to move it onto `QueueConfig`. Poll cadence is likewise package-level. A budget sweep is among the first things a benchmark would run, and its output is exactly the evidence that would justify making these per-queue.

One more shapes cancellation cost. Cancelling a single request today fails its whole batch, with no path for innocent members to re-enter the queue. That is harmless while batches hold one request and becomes expensive the moment grouping lands — itself a result worth producing before the feature ships.

The consequence for this design is that the harness treats all of these as parameters from the outset rather than constants to be retrofitted. Batch size and accumulation policy, build budget, relaxation aggressiveness, and coverage-based early merge belong on the variant axis of the [comparison model](#comparison-model) whether or not the production code reads them yet.

The same applies to the seams themselves. Extensions and sub-extensions will arrive after this document, so the harness derives its variant axes from the extension contract rather than from a list of the seams that happen to exist today.

## The unit of substitution

Swappability is defined at the finest seam a queue can wire independently, not at the granularity of a pipeline stage. The stage is where a substitution is observed; the interface is what gets substituted.

| Stage | Substitutable seams |
|---|---|
| validate | `Validator` |
| batch | `conflict.Analyzer`, and the path projection it is constructed with |
| speculate | `Scorer`; `Speculator`, and separately the Generator and Allocator it composes |
| build | `BuildRunner` |
| merge | `Merger`, and the merge strategy it applies |

A composed extension is substitutable both as a whole and in its parts, and the harness has to support holding every part but one fixed. Comparing two Generators under the same Allocator and the same Scorer is the only way to attribute a change in ranking quality to the Generator, and the [diagnostics](#diagnostics) below already measure the two separately. Measuring them apart while being unable to substitute them apart would be an odd place to land.

Two consequences follow. A seam joins the variant axis the moment a queue can wire it independently, which is why that list belongs to the extension contract rather than to this document. And wiring a seam proves nothing about exercising it: a corpus that never produced a batch under budget pressure says nothing about an Allocator, however faithfully that Allocator was substituted. Coverage is therefore reported per seam, and a seam the corpus never exercised is reported as untested rather than as agreeing.

Storage, counters, and change providers are substituted too, but as infrastructure rather than as variants. They are what the harness replaces in order to run at all, not what it compares.

## Three layers

**Layer 1 — extension replay.** Every behavioral seam in this codebase is a `Factory` interface that takes identity and resolves what it needs internally: `conflict.Analyzer`, `Scorer`, `Speculator`, `BuildRunner`, `Validator`, `Merger` (see [extension-contract.md](extension-contract.md)). That uniformity means one mechanism covers all of them, and it reaches the composed seams above by substituting a `Speculator` assembled from a candidate Generator and the incumbent Allocator, or the reverse. Record the (identity, output) pairs a stage produced, then replay those identities through a candidate and diff the results. Nothing else runs: no other stage, no queue, no storage beyond the stage's own resolver. It is fast enough for every change, and it answers a narrower question than an end-to-end run, which is exactly what makes the answer trustworthy. Does this stage's output differ, given identical input, independent of what anything downstream would have done with it?

**Layer 2 — controller replay.** Much of the correctness logic lives in controllers rather than extensions. A speculation run is "all controller code, except step 3," per [speculation.md](speculation.md): finalization, refutation, output validation, and the merge gate are all controller-owned. Layer 1 cannot reach them, and Layer 3 is too heavy to run on every change. Layer 2 fills the gap by feeding a recorded queue snapshot to a controller and diffing the actions and writes it emits. This is unusually tractable here, because a speculate run is built as a pure function of a single read: "every run recomputes the whole queue from that single read," with no dependence on any earlier run. That is precisely the property replay wants. Other controllers that are similarly snapshot-determined can use the same mechanism.

**Layer 3 — system simulation.** Some questions are about consequences rather than verdicts. Does a less conservative `conflict.Analyzer` actually reduce land time, once batching and speculation react to the looser dependency graph? Answering that needs the real pipeline running forward: gateway, orchestrator, and Runway in one process, with in-memory storage and message-queue backends in place of MySQL (the only backend either has today), and fakes only at the boundaries that reach genuinely external systems such as source control, CI, and git. Layer 3 is where knock-on effects on land-time and cost distributions become visible. It is also the only layer that can exercise the concurrency invariants described under [Invariants and fault injection](#invariants-and-fault-injection).

Isolation from real systems is a property of the wiring rather than of configuration. A call reaching a boundary the harness has not faked fails the run; it does not fall through. Intercepting the boundaries known when the harness was written is the arrangement that fails quietly, because the first new dependency anyone adds talks to production and nothing reports it.

All three consume the same recorded corpus of (identity, output) pairs and differ only in what they do with it. Layer 1 diffs a candidate against it directly. Layer 2 replays a snapshot through a controller. Layer 3 runs the real controllers forward, consulting the corpus or a candidate at each stage as it goes.

## What a difference means

A replay produces one of four outcomes, and collapsing them into pass and fail destroys the instrument. Record-and-replay systems that report a harness failure the same way they report a genuine regression teach their users to ignore both.

- **Harness fault.** The run did not complete: a fake failed, a resolver timed out, a candidate panicked. Says nothing about either implementation.
- **Corpus fault.** The run completed, but the baseline no longer reproduces its own recorded output. The inputs are stale, a ref has moved, or the assumed configuration is wrong. Every candidate number derived from that entry is void.
- **Behavioral difference.** Both implementations ran and disagreed. This is the measurement, not an error.
- **Invariant violation.** A property that must hold under every implementation did not. Always a bug, in the candidate or in the harness, and never a result.

The third of those is what separates this tool from the replay systems it otherwise borrows from. A workflow engine replaying a durable history treats any divergence as a defect, and can do so because its workflows are contractually pure functions of that history: the only question is whether a new binary still honors it, so "the replayer accepted the history" is a complete oracle. Nothing here carries that contract. A candidate `conflict.Analyzer` reporting different conflicts is not failing, it is doing the thing it was written to do, and a harness built around output equality would report every interesting change as broken.

What must hold regardless of implementation is narrower, and worth stating because it is what the harness actually asserts. The [invariants](#invariants-and-fault-injection) hold throughout every run, whatever the stages decide. A request's outcome must be explicable by the stage outputs it saw: a candidate is free to change what a stage decides and not free to change how the pipeline responds to a decision. Because the timing model is deliberately not deterministic, that second property is asserted as an invariant during a run rather than checked by comparing two runs event for event.

## Reproducibility per stage

Not every stage's output can be regenerated on demand, and that decides how its corpus is collected (see [Corpus collection](#corpus-collection)). A stage reproducible from a pinned identity can be replayed against any candidate. A stage that is not can only be compared against the single answer history happened to record.

| Stage | Extension | Reproducible? | Why |
|---|---|---|---|
| validate | `Validator` | Yes | Resolves change content at a fixed commit; deterministic given that identity. |
| validate (dry-run) | `Merger.CheckMergeability` | Yes, if the checked base is pinned | A pure function of two fixed commits. Production resolves the base live, against the target branch's current tip, which only looks non-deterministic because the tip moves. Record the base sha actually checked as part of the identity and it replays like any other git operation. |
| batch | `conflict.Analyzer` | Yes | [`pathoverlap`](../../submitqueue/extension/conflict/pathoverlap/pathoverlap.go) resolves each batch's changed paths through an injected `changeset.Resolver`, a pure function of git shas. |
| speculate | `Scorer` | Yes | [`heuristic.Score`](../../submitqueue/extension/scorer/heuristic/scorer.go) resolves a batch's changes through the same kind of resolver, then buckets a static value function against static config. No live or mutable input. |
| speculate | `Speculator` | Yes | Deterministic given the batch and dependency-graph state and the Scorer's output. No external dependency. |
| build | `BuildRunner` | **No** | Pass/fail and duration depend on the CI backend's condition at execution time, such as infra load and flakiness. No identity pins that. See [The build oracle](#the-build-oracle). |
| merge | `Merger.Merge` | Yes, for the mechanical part | Computing the resulting commit for a given base, change, and strategy is a pure git operation, replayable the same way `CheckMergeability` is. |

Only `BuildRunner` is genuinely irreproducible. Everything else can be recomputed rather than merely looked up, and that is what makes candidate-versus-baseline replay possible for those stages at all.

## The build oracle

A replayed run has a recorded build result only for the paths history actually built. Speculation means most paths a candidate explores were never built, and any change to conflict analysis or batching produces batch compositions that never existed. The simulator therefore needs an explicit model of whether a build would have passed and how long it would have taken. That model is the largest single source of error in every system-level number the simulator reports, so it belongs in the design as a named, swappable component rather than an implicit assumption.

The recommended default is **failure attribution**. Mine per-change outcomes from history, classify each change as good or bad from the builds it appeared in, and fail any batch containing a bad change. Duration is modeled separately and more crudely: the longest recorded duration among a batch's members, or a draw from the recorded distribution for batches of that size. Attribution is cheap, needs only what the audit trail already holds, and fails in predictable ways.

Its blind spot needs stating plainly. Two changes that each pass alone can still fail together, and those interaction failures are precisely the semantic conflicts that conflict analysis exists to catch. Attribution cannot see them, so it **systematically flatters an aggressive conflict analyzer**. Any result claiming a looser analyzer is safe has to be qualified by that bias, or corroborated by something that can observe interaction failures directly: shadow execution, or building the combined pair after the fact. Uber's earlier internal merge queue modeled build outcomes the same way and hit the same limit.

## Corpus collection

Continuous collection does not require instrumenting every extension call on the hot path, and normal persistence does not cover everything replay needs either. Three mechanisms apply. Which one a stage uses depends on what its persisted trace actually contains.

**Read from history** where the stored value is losslessly the stage's output:

- [`entity.Build`](../../submitqueue/entity/build.go) — `BuildRunner`'s outcome and duration, the one value that can only come from here.
- [`entity.SpeculationPathSet`](../../submitqueue/entity/speculation.go) — the `Speculator`'s chosen paths.
- [`entity.RequestLog`](../../submitqueue/entity/request_log.go) — the timestamped state-transition audit trail, from which land times are derived.

The request log is the interim trace source, not the intended one. The [hook framework](../hook-framework.md) designs a durable event published on every lifecycle transition, ordered per subject, with the explicit invariant that hook outcomes never write pipeline state — a pure observation point, and cross-domain rather than scoped to SubmitQueue request statuses the way the request log is. Hooks are unbuilt, so the harness derives traces from the log today and inherits its narrower reach. The corpus should anticipate the switch rather than encode request-log specifics.

**Record at the seam** where the persisted form is a lossy projection rather than the output itself. `conflict.Analyzer` is the case that matters, and the loss is easy to miss. The analyzer returns `[]entity.Conflict`, each carrying a batch ID *and* a `ConflictType`. The batch controller reduces that to a list of IDs before storing it ([`batch.go`](../../submitqueue/orchestrator/controller/batch/batch.go)): the type is erased, repeated entries for one pair collapse, only batches that happened to be in flight were evaluated, and what survives is the transitive closure rather than what the analyzer directly reported. [`entity.Batch`](../../submitqueue/entity/batch.go)'s dependency list is therefore strong evidence of what the *system did*, which is what Layer 3 needs, and a weak baseline for what the *analyzer said*, which is what Layer 1 compares. Capturing the raw return value at the seam is small, exact, and the same mechanism as [shadow recording](#shadow-recording).

**Resolve and cache the inputs**, in every case. The resolved content a stage consumed, such as changed paths and changeset detail, is persisted nowhere. Without it no candidate can run at all: a stored verdict records what the old implementation decided and does nothing to let a new one decide. A resolve-and-cache layer in front of `changeset.Resolver` supplies it, keyed by content rather than by a filename or run label, so an edited input cannot silently reuse a stale entry.

That cache is a durability mechanism, not a speed optimization. Stored identities stop resolving over time as branches are deleted, refs garbage-collected, pull requests closed, and history rewritten. A corpus meant to accumulate indefinitely cannot assume a six-month-old sha is still resolvable on demand, so content resolved once is kept.

Two situations need computation even where history holds an answer.

**Counterfactual compositions have no stored output at all.** Once Layer 3 diverges, batches form that never existed, and history was never asked what the incumbent would have said about them. The baseline implementation runs fresh alongside the candidate.

**Re-deriving a baseline that history does hold is the corpus's integrity check.** If replaying the incumbent against re-resolved inputs fails to reproduce the stored value, then the inputs are stale, the source has moved, or the assumed configuration is wrong. That has to surface before any candidate number is believed. It is the Layer 1 analogue of the [backtest](#trusting-the-simulator).

`Scorer`'s raw output has no persisted trace either, since only the `Speculator`'s downstream path choice survives. Because `Scorer.Score` is deterministic, historical scores stay recomputable from resolved batch content. That holds only while it remains a pure function of that content; a scorer that reads anything else needs recording at the seam, as conflict does.

**Provenance and versioning.** A recorded output is a valid baseline only if it is known what produced it. Every corpus entry carries the implementation name and version, the queue configuration it ran under, and the commit that built it. Interface drift is not hypothetical in a repository under active development, and a corpus that outlives an interface change must be rejected loudly at replay time rather than silently misattributed.

**Regenerate rather than maintain.** A corpus of checked-in fixtures rots. Every system that keeps one ends up refreshing it by hand, after someone notices a test failing for the wrong reason. The alternative is to treat the corpus as derived: keep a small, version-independent description of what to exercise, and re-author the entries from it against whichever implementation is under test. Exactly one artifact then crosses a revision boundary — the recorded (identity, output) pair, in a format carrying a declared version. Seeds, selection filters, and everything else the harness used to produce an entry are same-revision implementation details and are never exchanged. Keeping that boundary narrow is what makes versioning it honest. Entries mined from history cannot be regenerated and do not get this property; they carry the provenance above and are rejected when their format no longer parses, which is the weaker guarantee that history-derived data can support.

Compatibility is worth checking in both directions. Replaying a candidate's corpus against the incumbent asks whether the change can be reverted without invalidating everything recorded since it shipped. That is a real question during an incident and a cheap one to answer once the machinery exists.

**Publish nothing that does not reproduce itself.** Before an entry enters the corpus, the implementation that authored it replays it and must reproduce it. An entry failing that check indicates a harness defect, and admitting it to the corpus would surface later as a phantom regression against some unrelated candidate. This is the integrity check above applied at write time rather than read time, and it is what allows a corpus fault to be reported as its own outcome instead of being discovered through confusion.

**Identity.** Entries key on the canonical change URI defined in [change-uri.md](../change-uri.md). That contract has exactly one valid spelling per change, with full lowercase SHAs and verbatim path segments, and its parsers validate rather than normalize. The corpus must not invent a normalization the rest of the system rejects.

**Bounded content.** The corpus holds identities and the derived metadata a stage consumed: change URIs, commit shas, changed-path sets. It never holds file contents or diffs, so retention applies to a small and comparatively insensitive artifact. This is a bound rather than an identity-only guarantee, since resolved path sets are deliberately retained for the durability reason above.

**Tiering.** Three corpus sizes keep the tool usable: a small one checked into the repository that runs in seconds on every change, a medium one exercised nightly, and the full historical corpus on demand for changes that warrant it. Without the small tier, the machinery exists but nobody drives it.

**Selection as code.** The filters that define a corpus, such as which states, which queues, and which window, are sampling decisions that bias every result derived from it. They belong in versioned code, reviewable and diffable like any other input, not in a query pasted into a runbook.

### Shadow recording

For any stage where a candidate implementation already exists, recording beats replaying. Run the candidate in production alongside the incumbent on live traffic, record both outputs, and act only on the incumbent's. The comparison is then against real traffic with no counterfactual, no resolve-and-cache layer, and no fidelity question at all, and the recording *is* the corpus.

This also unifies two things the rest of this document treats separately. If collection runs continuously anyway, collecting several implementations concurrently makes N-way comparison a byproduct rather than a feature. It also reduces replay to the case that genuinely needs it: an implementation that did not exist when the data was recorded.

Shadow execution needs a structural guard rather than a convention. A shadow-mode call carries an explicit allowlist of what it may execute, and anything outside that list returns without acting. Read-only stages are safe to shadow directly. Anything that writes is not, and `Merger.Merge` is the case that matters: running it twice does not produce a comparison, it produces two merges. Write-side changes are validated by a staged rollout instead.

## Diffing

Comparing two stage outputs is not equality on serialized bytes, and most of the difference between a useful diff and an unusable one lies in what it declines to report.

**Canonicalize before comparing, in a separate pass.** Expected variation — generated identifiers, wall-clock timestamps, collection ordering — is normalized by a stage that runs before the comparison rather than by special cases inside the comparator. Keeping the two apart leaves the comparator generic and the normalization rules reviewable on their own terms. Timestamps are shifted by the record-to-replay offset rather than dropped, so an implementation that computes a duration wrongly still diverges and is still caught.

**Declare field rules as configuration.** Two rules are enough to start: ignore a path entirely, or assert only that it is present without comparing its value. The second is what generated identifiers need, and it is strictly better than ignoring them, because a missing identifier is a real defect that an ignore rule would hide.

**Align collections by declared key, not by position.** Two runs order a dependency list or a path set differently for reasons carrying no information. Aligning entries by a content hash over declared key fields removes that noise cheaply. Structural-similarity matching, which record-and-replay systems reach for when replay regenerates identifiers and no stable key survives, is not needed here and should not be built: requests, batches, and paths all carry stable identity.

**Report divergence per field path rather than as a verdict.** A count of mismatches by path is what tells a reader that a candidate differs only in conflict type, or only on batches above a certain size. It is also how new sources of benign variation are found, which are then promoted into the rules above. That loop is a permanent part of operating the tool, not a one-time setup step.

## Evaluating conflict detection

Conflict analysis is the driving use case, and it needs a different notion of "diff" than the other stages. Two analyzers disagreeing tells you *where* they differ, not *which is right*. The useful frame is precision and recall over two error classes whose costs are sharply asymmetric:

- A **false negative**, where the analyzer misses a real conflict, lets a broken combination onto the trunk. It is approximable from history, since a trunk build failure, revert, or fix-forward shortly after a batch lands is the observable signature.
- A **false positive**, where the analyzer reports a conflict that would never have materialized, costs throughput through unnecessary serialization. It has no historical signature, because the counterfactual was never run, but it can be estimated after the fact by merging and building the pair that was held apart.

A candidate should be reported against both, never against agreement rate alone. An analyzer that agrees with the incumbent 99% of the time may still be strictly worse if its 1% of disagreements are all false negatives. This is also where the build oracle's blind spot bites hardest, and the strongest argument for shadow execution over pure replay when evaluating a new analyzer.

## What to measure

A comparison needs a decision rule, and the obvious metric is a trap. **Throughput is largely uninformative under fixed-trace replay.** The queue drains whatever the trace delivers, so total landed count is a property of the input rather than the policy. It becomes meaningful only under saturation, or when a policy changes the failure rate enough to alter how many requests land at all. Reporting it as a headline number invites false confidence.

Two tiers of metric serve different purposes. **Outcomes** decide whether a candidate is better. **Diagnostics** explain why it moved, and localize a regression to the stage that caused it.

### Outcomes

- **Latency** — land-time distribution per queue, from p50 through p99. Merge-queue latency is long-tailed and often bimodal, so the middle of the distribution carries information that three widely-spaced percentiles hide.
- **CI cost** — builds started per landed change, the currency speculation spends to buy latency.
- **Safety** — escaped conflicts and trunk breakages, per [Evaluating conflict detection](#evaluating-conflict-detection).
- **Fairness** — worst-case land time, broken down by request size. A policy that improves the median by starving large changes looks good and is not.

Latency and cost trade against each other, so a single scalar verdict hides the trade. Report the pair. Where the question is a policy rather than a bug fix, sweep the build budget so each candidate is a curve rather than a point.

Report these bucketed by hour as well as in aggregate. A policy that keeps up during peak and one that defers work into the quiet hours produce the same totals over a long enough window while behaving nothing alike.

### Diagnostics

These are defined against this system's own model — heads, paths, dependency assumptions, and a queue-wide build budget — so they do not carry over from a queue whose speculation ran linearly over queue order.

**Waste.** Budget-time spent on paths whose result never reached a verdict. This is not a count of cancelled builds. A cancelling path keeps charging the build budget until it reaches terminal, and a path can complete, pass, and still be wasted when a dependency later resolves against its assumption. Its companion is the **refutation rate**, how often a resolved dependency breaks a path's assumption, which reads prediction quality directly.

**Dependency cost.** The interval between a head having a passing build and that head merging. The merge gate is strict, so this is precisely what the dependency graph costs, and therefore what a less conservative `conflict.Analyzer` sets out to reduce. Alongside it sits **coverage**, whether the funded paths span every combination of dependency outcomes, since coverage rather than one lucky path is what makes an early merge sound, and the **bypass rate** that coverage enables.

**Ranking quality.** The interval between the first path funded for a head and the funding of the path that ultimately passed. This isolates whether the Generator ranked well, separately from whether the outcome was right. Its cross-head counterpart is **starvation**: whether the Allocator spreads budget across competing heads or lets one monopolize it.

**Pipeline health.** Dead-letter rate and per-stage queue lag, which a message-driven pipeline has and a polling loop does not, plus compare-and-swap contention as the visible symptom of optimistic-concurrency pressure.

One structural constraint applies to any system-level report. Once two runs diverge they have different batches, different paths, and different builds, with nothing to line up one-to-one. **The request is the only identity that survives divergence.** Per-request outcomes are therefore the finest granularity at which two Layer 3 runs can be compared, and everything else is compared as a distribution.

Two notes on borrowing vocabulary. The gateway already defines the customer-facing status strings a report should use as state labels, so the harness should adopt those rather than invent parallel names. And the speculation generator's ranking score is explicitly recomputed per run and not comparable across runs, so ranking quality has to be measured as the timing gap described above rather than by comparing scores. Everything else here is this benchmark's own definition; no design doc in the repository specifies land time, cost, or throughput as a contract.

## Comparison model

Start pairwise, baseline against one candidate on the same corpus, with the report shaped as a table keyed by (identity, variant). N-way is then the general case rather than a rewrite. The variant axis is deliberately generic: it holds competing implementations, or one implementation at different parameter values. A parameter sweep and an N-way implementation comparison become the same operation over the same report shape, which matters because sweeps over build budget, arrival multiplier, or conflict granularity are how most policy questions actually get answered.

The timing model below is deliberately not deterministic, so two runs of the same configuration differ. Every Layer 3 comparison therefore needs repetition and an interval, and no improvement or regression should be reported that falls inside the run-to-run band. Synthetic workloads take an explicit seed so a run can be reproduced exactly.

How many repetitions, and over which windows, is fixed by the harness rather than chosen per analysis. Letting each user pick produces intervals that cannot be compared between two people's results, and the resulting inconsistency is corrosive: it is the failure mode that makes a simulation platform distrusted, and it arrives through convenience rather than through any single bad decision. Replicating across several historical windows matters at least as much as replicating within one, since between-window spread is what shows whether a result generalizes past the day it was measured on.

Where a variant's stochastic inputs can be held common, they should be. Two policies compared over the same window draw the same build-oracle outcomes, so the difference between them reflects the policy rather than the luck of the draw. This costs nothing and tightens every interval the harness reports.

## Trusting the simulator

A simulator that produces confidently wrong numbers is worse than none, because people act on it. The design therefore includes a standing **backtest**: replay a historical window with the baseline configuration unchanged, and compare the simulator's output against what actually happened in that window. A simulator that cannot reproduce reality when nothing has changed cannot be trusted to predict the effect of a change. This runs continuously rather than once, and its residual error is published alongside every result. An improvement smaller than the simulator's own error against reality is not a result.

The backtest windows are a fixed, versioned set rather than a fresh sample each time. Re-drawing the window on every run makes the error figure incomparable between runs, which defeats the purpose of publishing it: what matters is whether accuracy is drifting, and that is only visible against a stable set. Windows should be chosen to include the awkward periods — an incident, a backlog, a burst — and not only the calm ones.

There is a second and stronger check available, because policy changes ship here regularly. Every real change to queue behavior is a natural experiment: run the simulator on the window before the change, and compare its prediction against the shift actually observed afterward. Accumulating those cases builds the only evidence that answers "has this thing ever been right about something that mattered." A prediction validated this way is worth more than any amount of internal consistency, and the accumulated set is worth keeping as a permanent artifact rather than as a one-off validation exercise.

Two fidelity limits cannot be engineered away, and are documented instead.

**Arrivals are not exogenous.** Replay treats the arrival stream as independent of queue performance, and it is not. Faster landing changes how people submit, stack, retry, and abandon work. Historical cancellations are the sharpest case: they happened because a person lost patience at a particular moment, and would not have happened under a faster policy. Fixed-trace replay consequently overstates the benefit of an improvement. The mitigation is to report sensitivity across several arrival-rate multipliers rather than a single trace speed.

**The build oracle is a model, not a record.** Its bias is described above, and it applies to every Layer 3 number that depends on a batch composition history never built.

## Invariants and fault injection

The strongest guarantees in this repository are statements about behavior under adverse scheduling: idempotency, optimistic concurrency, persist-before-publish, at-least-once delivery. A Layer 3 harness whose in-memory backends only ever behave *nicely* exercises none of them, and will report a green run for code that breaks in production on the first redelivery.

Two capabilities close that gap, and both are cheap once Layer 3 exists. **Fault injection** lets the in-memory backends behave adversarially within the queue's documented contract: duplicate deliveries, reorder within the freedom the contract allows, expire visibility timeouts, lose compare-and-swap races, fail storage calls, and stall builds past their timeout.

Reproducing that contract faithfully matters more than it sounds, because several of its behaviors are load-bearing and easy for a fake to smooth over. A postponed message is a barrier that blocks its own partition until the delay elapses, while a nacked message deliberately does not block. Postpone resets the attempt count, so only consecutive failures count toward dead-lettering. The acked watermark advances only across a contiguous prefix, so one stuck message holds it back behind later acked ones. Consumer groups are fully isolated from each other's retries. The contract also carries a documented footgun the harness should be able to trigger rather than prevent: strict per-partition serialization depends on the visibility timeout exceeding processing time, and degrades silently to concurrent delivery when it does not.

Determinism needs one specific guard, in the place it is easiest to lose. When two units of work compete for a scarce resource, which one wins must be settled by a stable key rather than by whichever goroutine arrived first. Build budget is exactly such a resource, and "which path took the last slot under pressure" is a question this simulator exists to answer; a harness resolving it by scheduling luck cannot answer it reproducibly. Relatedly, every source of randomness draws its seed from one place that the run records, rather than each component seeding itself. Both are cheap to design in and awkward to retrofit. Simulators elsewhere have shipped with a rigorously deterministic event core and a nondeterministic contention path underneath it, precisely because the second was left to individual components to get right.

**Continuous invariant checking** asserts throughout a run rather than only at the end:

- A batch never reaches Succeeded without a passed path whose assumptions match how every dependency actually resolved.
- The build budget is never exceeded.
- Terminal states are terminal, and nothing transitions out of one.
- No change lands twice.
- Every request eventually reaches a terminal state.

Together with a synthetic workload generator, this is a property-based tester for the concurrency design, and it covers ground no other layer of testing in the repository reaches today.

## Timing model

Layer 1 and Layer 2 need no timing model, since both are input-to-output diffing. Layer 3's latency metrics do, and the two ways to get there trade differently.

Uber's earlier internal merge queue replayed a full historical trace with correct relative timing in a fraction of real time, using a virtual clock: a discrete-event loop over a priority queue of timestamped work items, advancing an in-memory current instant from event to event, with nothing ever sleeping. A day of recorded traffic replays in the time it takes to process its events, and every elapsed-duration measurement stays correct because it is computed against virtual timestamps. That worked cleanly because the simulator was single-threaded, with one agenda and nothing to coordinate.

This orchestrator is not. `platform/consumer` dispatches per-partition-key work across real goroutines and channels, and virtualizing time cleanly under real concurrency is a materially harder problem. Full determinism means replacing the scheduler rather than just the clock, which is closer to building a deterministic-simulation framework than to injecting a clock.

The recommendation is to virtualize only what models elapsed time under our own control: the build oracle's modeled duration, and the workload generator's inter-arrival scheduling, both through a shared logical clock. The surrounding plumbing runs in real but fast wall-clock time. This is not bit-for-bit deterministic, which is why comparisons carry intervals, but it delivers the practical win of replaying a large trace in a small fraction of the time it took to record. Full determinism is worth scoping separately if it proves necessary. It should not block a first working version.

Control over interleaving, though, is not all-or-nothing between accepting noise and replacing the scheduler. The [consumer gate](../consumer-gate.md) already installs middleware ahead of every controller's `Process` call. Closing a gate parks a delivery and postpones it rather than letting it enter the controller; the park is observable, so a harness can wait for a stop to take effect instead of sleeping on it; and opening releases within one re-check. The contract is deliberately storage-agnostic, so an in-memory gate drops into the same seam the file-based implementation uses in end-to-end tests. That gives the harness stop, observe, and release at controller and partition granularity, which is enough to pin down the interleavings a correctness scenario cares about without reproducing wall-clock timing exactly. Message-level breakpoints are named in that RFC as a deferred extension of the same mechanism, and are the natural next increment if scenario control needs to get finer.

## Delivery

Ordered by how much each step delivers relative to the assumptions it requires, which is not the order the layers are numbered in:

1. **Layer 1 for `conflict.Analyzer`**, baselined on mined batch dependencies with the resolve-and-cache layer beneath it. The smallest useful thing, and directly the mechanism a new analyzer needs on arrival. It carries the pieces every later stage reuses: the four-outcome report, the canonicalize-then-diff split, and the self-reproduction check on write.
2. **Layer 1 for the speculation seams**, substituting a candidate Generator under a fixed Allocator and the reverse. This is where per-seam substitution earns its keep, and it pairs with the ranking-quality and starvation diagnostics that already measure the two apart.
3. **Shadow recording** for stages with a live candidate, which upgrades the corpus from historical to real-traffic and makes N-way comparison fall out for free.
4. **Layer 3 skeleton with synthetic workload, invariant checking, and fault injection.** This needs no corpus, no oracle, and no fidelity argument, so it is unblocked immediately, and it catches a class of concurrency bug nothing else in the repository tests.
5. **Trace replay, the build oracle, the metric set, and the backtest.** The benchmarking capability proper. It depends on every modeling assumption in this document and should not be trusted before the backtest exists. This step is also what unlocks the policy work in [Designing against the unbuilt system](#designing-against-the-unbuilt-system) — batch grouping, budget tuning, relaxation, and coverage-based early merge all need forward simulation and an outcome model to evaluate.
6. **Layer 2 controller replay**, which can slot in any time after (1).

## Rejected

- **A standalone simulation model of the pipeline.** Far faster to write than wiring the real services, and divergent from the code it claims to model within a release or two. The harness runs the real controllers and extensions against substituted backends instead, so the pipeline logic under test is the shipping logic.
- **Instrumenting every extension call in production.** The obvious way to build a corpus, and largely redundant, since the entities normal operation already persists carry most of what replay needs. A new hot-path write for data that is already durable costs latency and buys little.
- **Re-executing builds during replay.** Faithful for the paths history actually built, impossible for the rest, and prohibitively expensive at trace scale. A modeled outcome with a declared bias is the honest trade; see [The build oracle](#the-build-oracle).
- **Full deterministic scheduling.** The strongest fidelity story available, but it means replacing the scheduler rather than injecting a clock, which is a project in its own right. Repetitions and reported intervals absorb the residual noise at a small fraction of the cost.
- **A corpus of checked-in fixtures.** The obvious route to a reviewable, deterministic baseline, and it rots: interfaces move, entries stop parsing, and refreshing them is manual work that happens only after a confusing failure. Deriving entries from a versioned description costs more up front and retires a permanent maintenance tax; see [Corpus collection](#corpus-collection).
- **Structural-similarity matching to align records.** Necessary where replay regenerates identifiers and no stable key survives, and unnecessary here for exactly that reason. Content-hash alignment on declared keys covers this system's cases at a fraction of the complexity.
- **Synthetic workloads only.** Hermetic, cheap, and the right basis for the correctness work, but the benchmarking questions turn on conflict structure and arrival bursts that only real traffic exhibits. Synthetic generation stays; it does not stand alone.
- *Acknowledged:* every Layer 3 number depends on the build oracle, and no amount of harness engineering removes that dependency. The backtest bounds the error rather than eliminating it, which is why the simulator is positioned as a comparison instrument and not a forecast.

## Non-goals

- Wiring this into Uber's internal wrapper around this repository. That wrapper's `conflict.Analyzer` is still the conservative "everything conflicts" fake, so simulating against it is uninformative until a real analyzer is wired in. The design here carries over unchanged when that happens, because the wrapper's extensions are the same interfaces this document reasons about.
- Replaying traffic sourced from outside this repository's own persistence and resolvers.
- Predicting absolute production numbers. The simulator is a comparison instrument, and its output is a delta between variants under identical assumptions rather than a forecast.

## Open questions

- Corpus storage backend: object storage, a queryable warehouse table, or an abstract sink interface that defers the choice to implementation.
- Whether recording is opt-in per queue or always on, and what retention the corpus warrants given what it retains.
- Whether shadow recording should be a permanent production capability or a temporary mechanism stood up per evaluation.
- How the false-positive estimate for conflict analysis is produced in practice, given it requires building pairs the queue deliberately kept apart.
- Which controllers beyond speculate are snapshot-determined enough to join Layer 2.
- Which of the four outcomes in [What a difference means](#what-a-difference-means) should gate CI, given that a behavioral difference is the expected result rather than an exceptional one.
- How often the corpus regenerates, and what bounds staleness in the history-mined entries that cannot be regenerated at all.
