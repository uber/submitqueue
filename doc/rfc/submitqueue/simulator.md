# Simulator

Verifying a change to orchestrator logic — a new `conflict.Analyzer`, a different `Scorer` bucketing, a `Speculator` policy tweak, a `Merger` strategy — today means either trusting a unit test against synthetic fixtures or shipping the change and watching production. Neither answers "does this behave differently than what's running now, and is the difference an improvement." A simulator closes that gap: replay real (or synthetic) traffic through a candidate implementation, compare it against the incumbent, and report where and by how much they diverge.

The ask driving this design was explicitly narrower than "simulate the whole queue": every stage — validation, conflict detection, scoring, speculation, building, merging — should be independently swappable and comparable, not just the system as a whole. That shapes the design more than the end-to-end case does.

## Two layers

**Layer 1 — per-stage record/replay.** Every behavioral seam in this codebase is already a `Factory` interface with an identity-in, resolve-internally contract (`conflict.Analyzer`, `Scorer`, `Speculator`, `BuildRunner`, `Validator`, `Merger` — see [extension-contract.md](extension-contract.md)). That uniformity means one mechanism covers all of them: record (identity, output) pairs for a stage, then replay the same identities through a candidate implementation and diff its outputs against the recorded baseline. This runs in isolation — no other stage, no queue, no storage wiring beyond what the stage's own resolver needs — so it is fast enough to run per change and answers a narrower, more useful question than an end-to-end run: does this stage's *output* differ, given identical input, independent of what anything downstream would have done with it.

**Layer 2 — end-to-end pipeline simulation.** Some questions Layer 1 cannot answer, because they are about consequences, not verdicts: does a less conservative `conflict.Analyzer` actually reduce land time once batching and speculation react to the looser dependency graph? That requires the real pipeline running forward — gateway, orchestrator, and Runway wired together in one process, in-memory storage and message-queue backends standing in for MySQL (the only backend either extension has today), with fake implementations only at the boundaries that talk to genuinely external systems (GitHub/Phabricator, CI, git). Layer 2 is where a stage change's knock-on effects on land-time and throughput distributions become visible.

The two layers share one idea — a recorded corpus of (identity, output) pairs per stage — and differ in what they do with it: Layer 1 diffs a candidate against the corpus directly; Layer 2 runs the real controllers forward, consulting the corpus (or a candidate) at each stage as it goes.

## Reproducibility per stage

Not every stage's output can be regenerated on demand. The distinction matters because it decides whether a stage belongs in the "recompute and cache, like TargetAnalyzer" bucket or the "read the audit trail, never regenerate" bucket — and only one of the two lets replay run against arbitrary candidate implementations rather than a single frozen historical answer.

| Stage | Extension | Reproducible? | Why |
|---|---|---|---|
| validate | `Validator` | Yes | Resolves change content at a fixed commit; deterministic given that identity. |
| validate (dry-run) | `Merger.CheckMergeability` | Yes, if the checked base is pinned | The git operation is a pure function of two fixed commits. Production resolves the base live (the target branch's current tip), which looks non-deterministic only because "current tip" moves — pin the specific base sha that was actually checked, recorded as part of the identity, and it is exactly as reproducible as any other git operation. |
| batch | `conflict.Analyzer` | Yes | [`pathoverlap`](../../submitqueue/extension/conflict/pathoverlap/pathoverlap.go) resolves each batch's changed paths through an injected `changeset.Resolver` — a pure function of git shas, cacheable the same way TargetAnalyzer responses are. |
| speculate | `Scorer` | Yes | [`heuristic.Score`](../../submitqueue/extension/scorer/heuristic/scorer.go) resolves a batch's changes through the same kind of `changeset.Resolver` and buckets a static value function against static config — no live or mutable input. |
| speculate | `Speculator` | Yes | Deterministic given the batch/dependency-graph state and the Scorer's output; no external dependency. |
| build | `BuildRunner` | **No** | Pass/fail and duration depend on the CI backend's condition at execution time — infra load, flakiness — which no identity pins. This is the one stage where replay must read the persisted [`entity.Build`](../../submitqueue/entity/build.go) record and never regenerate. |
| merge | `Merger.Merge` | Yes, for the mechanical part | Computing the resulting commit for a given (pinned base, change, strategy) is a pure git operation, replayable the same way `CheckMergeability` is. |

Merge deserves a specific note, because the naive assumption is that concurrent pushes make it a race like build's infra-dependence. They do not, by construction: [`merge.go`](../../submitqueue/orchestrator/controller/merge/merge.go) publishes to Runway's merge topic partitioned by `batch.Queue`, and [`platform/consumer`](../../platform/consumer/consumer.go) dispatches each partition key to exactly one goroutine draining it in order — every merge for a given queue runs through one serial worker, never two at once. The base commit for the Nth batch in a queue is deterministically "whatever the (N-1)th batch produced," recoverable from the sequential history with no ambiguity. The only residual risk is a push to the same branch from outside SubmitQueue entirely — a hotfix, another automation, a branch-protection bypass — which is an operating assumption to state explicitly ("no unmanaged pushes during the replay window"), not an architectural gap this design needs to close.

Only `BuildRunner` is genuinely irreproducible. Everything else can be recomputed and cached rather than merely looked up, which is what makes candidate-vs-baseline replay possible at all for those stages.

## Corpus collection

"Collect continuously" does not have to mean instrumenting every extension call on the hot path. Most of what a corpus needs already accumulates as a side effect of normal operation, through the storage extension every queue already writes to:

- [`entity.RequestLog`](../../submitqueue/entity/request_log.go) — the full state-transition audit trail, already timestamped.
- [`entity.Batch.Dependencies`](../../submitqueue/entity/batch.go) — `conflict.Analyzer`'s actual output for that batch.
- [`entity.Build`](../../submitqueue/entity/build.go) — `BuildRunner`'s outcome and duration, the one value that must come from here rather than being recomputed.
- [`entity.SpeculationPathSet`](../../submitqueue/entity/speculation.go) — the `Speculator`'s resulting path choices.

What is missing is narrower than a general recorder: the *resolved content* a deterministic stage used (changed paths, changeset detail) is not stored anywhere, but — per the reproducibility table above — it is cheaply reconstructible from an identity that is stored (a change URI, a commit sha), the same way the v1 simulator re-fetched TargetAnalyzer responses for a historical base+diff sha rather than storing them ahead of time. A resolve-and-cache layer in front of `changeset.Resolver` (keyed on the identity, so repeated replay runs against different candidates do not re-resolve the same content) covers this for every deterministic stage.

The one real gap: `Scorer`'s raw output is not persisted anywhere — only the `Speculator`'s downstream path choice is. Since `Scorer.Score` is itself deterministic (see above), historical scores are still recomputable by resolving the batch and re-scoring with the *baseline* bucket config — the gap only bites if a future scorer implementation stops being a pure function of resolved batch content, at which point it would need an explicit persisted field.

Two questions this RFC leaves open rather than settling: where the corpus (and the resolve-and-cache layer's results) should live durably, and whether collection should be opt-in per queue or blanket. Both are implementation decisions that do not change the design above.

## Comparison model

Start pairwise — baseline implementation vs. one candidate, diffed on the same corpus. Design the report/aggregation shape so N-way (multiple candidates lined up side by side on the same corpus, the way v1's simulator ran several speculation-selection strategies concurrently against the same replayed trace) is an extension of the same shape rather than a rewrite: a pairwise diff is the N=2 case of a table keyed by (identity, implementation) → output.

`conflict.Analyzer` warrants a different reading of "diff" than the other stages: disagreement between two analyzers is a three-way question — does the candidate agree with the baseline, and where it does not, is the candidate's answer correct or a regression — not simply "different, flag it." The other stages (`Scorer`, `Speculator`, `BuildRunner`) are closer to a pure behavior/performance comparison with no independently knowable right answer. The report format should keep these distinguishable rather than treating every disagreement the same way.

## Timing model

Layer 1 needs no timing model — it is pure input/output diffing. Layer 2's system-level metrics (land time, throughput) do, and the two ways to get there have a real tradeoff.

v1's simulator replays a full historical trace with correct relative timing in a fraction of real time, using a virtual clock: a discrete-event loop over a priority queue of timestamped work items, advancing an in-memory "current instant" from event to event with no `Thread.sleep` anywhere in the loop (see `submitqueue-simulator/src/main/java/com/uber/submitqueue/simulator/Simulation.java` in Fievel). That works cleanly because it is single-threaded — one agenda, nothing to coordinate. This codebase's orchestrator is not: `platform/consumer` dispatches per-partition-key work across real goroutines and channels. Virtualizing time cleanly under real concurrency is a materially harder problem than v1's — full determinism would mean replacing the scheduler, not just the clock, closer to a FoundationDB-style deterministic-simulation framework than a clock injection.

Recommendation: virtualize only the pieces that model elapsed time under our own control — `BuildRunner`'s fake modeled duration, the workload generator's inter-arrival scheduling — through a shared logical clock, and accept that the surrounding goroutine/channel plumbing still runs in real (but fast) wall-clock time. This is not bit-for-bit deterministic across runs, but it gets the practical win (a large trace replays in a small fraction of the time it took to record) without taking on a scheduler rewrite as a prerequisite. Full determinism is worth scoping separately if it turns out to matter later; it should not block a first working version.

## Non-goals

- Wiring this into Uber's internal `code_merge/submitqueue` wrapper. That wrapper's `conflict.Analyzer` is still the OSS "conservative — everything conflicts" fake; simulating against it is not informative until a real analyzer (the forthcoming open-source Tango TargetAnalyzer, or an equivalent) is wired in. The design here is meant to carry over unchanged when that happens — the wrapper's extensions are the same interfaces this document reasons about.
- Full deterministic scheduling (see Timing model).
- Replaying traffic sourced from outside this repo's own persistence (e.g., pulling from a different system's logs). Corpus collection here assumes the data comes from this codebase's own storage and resolvers.

## Open questions

- Corpus storage backend — blob storage (mirroring v1's Terrablob cache for TargetAnalyzer responses), a queryable data-warehouse table, or an abstract sink interface left for implementation time to decide.
- Sampling policy for high-QPS stages, if mining existing persistence turns out to be insufficient on its own.
- Whether recording/replay is opt-in per queue or always on.
