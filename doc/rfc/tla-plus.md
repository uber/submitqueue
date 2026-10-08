# Model Checking with TLA+

Before changing how stages, queues, or services hand work to each other, we write the protocol down in TLA+ and let the TLC model checker try every ordering of its steps. This RFC proposes when that is required, where the specs live, how CI runs them, and how they stay tied to the Go code. It also explains why more e2e and integration testing cannot do this job.

## Problem

SubmitQueue's worst bugs are not mistakes inside one controller. They are mistakes in the protocol between controllers: a message redelivered after a lost ack, a compare-and-swap lost to another writer, a dead letter reconciled while another stage is still working, a service answering a question the other side has stopped asking. Each controller is correct on its own, and the system is wrong.

The record so far:

| Issue | What went wrong | Found by | Within reach of a spec |
|---|---|---|---|
| [#352](https://github.com/uber/submitqueue/issues/352) | Re-trigger publishes reused message IDs and were silently deduped; the pipeline stalled | e2e test hung | Yes: a queue model with dedup |
| [#821](https://github.com/uber/submitqueue/issues/821) | One batch that could not be scored stopped every batch in the queue | Demo run | Partly: the zero-value batch is a code bug; "one batch's error halts the queue" is a rule a spec states |
| [#818](https://github.com/uber/submitqueue/issues/818) | DLQ reported `error` for a change that had landed | Demo run, after the push to a real repo | Yes |
| [#822](https://github.com/uber/submitqueue/issues/822) | A batch stranded in `speculating` with nothing to wake it | Demo run | Yes |
| [#823](https://github.com/uber/submitqueue/issues/823) | A batch merged on an assumption that had not come true, putting an unbuilt combination on the trunk | Reading the predicates by hand | Yes |
| [#824](https://github.com/uber/submitqueue/issues/824) | An acked message was redelivered at a fifth of its visibility timeout | Demo run under load | No as a design check: the code strayed from the queue's design. Checking runs against the spec would flag it |
| [#817](https://github.com/uber/submitqueue/issues/817) | A batch orphaned in `created` wedged its queue for 50+ minutes | Demo run, 65 of 100 PRs in | Yes |
| [#825](https://github.com/uber/submitqueue/issues/825) | A cancel during batch promotion let a cancelled request reach speculation | Code review | Yes |
| [#826](https://github.com/uber/submitqueue/issues/826) | The batch DLQ failed requests that another live batch owned | Code review | Yes |
| [#819](https://github.com/uber/submitqueue/issues/819) | Land and landsignal DLQs fail a `landing` batch that Runway merges | TLC | Found by one |
| [#820](https://github.com/uber/submitqueue/issues/820) | Runway's DLQ answers `FAILED` for a push that landed | TLC | Found by one |

Nine of the eleven are protocol bugs, and every one of those nine came from a demo run, a hung test, or a careful reviewer. None came from a test written to find it. #819 and #820 were found in an afternoon with a 250-line spec, before anyone saw them happen. #825 and #826 were spotted in review and are still open: the code paths they describe are unchanged on `main`, because nothing short of building the exact ordering would show the bug.

## Why e2e and integration tests cannot close the gap

They are necessary, and they find the code bugs in that table (#821, #824). They cannot find protocol bugs reliably, for reasons that more tests do not fix:

- **They try one ordering per run.** A protocol bug needs a particular fault at a particular step. #819 needs a lost ack after the Runway publish, followed by storage errors on every retry. No run produces that by chance.
- **Each ordering they pin is built by hand.** `TestCancel_CaughtPreBatch_NeverLands` is deterministic only because it closes a [consumer gate](consumer-gate.md) on Runway's conflict check, to catch the cancel before batching. That pins one ordering. #825's ordering, a cancel between the claim and the promote, needs a different gate at a different stage, and nobody thought to build it until review found the bug. A test checks the orderings its author imagined, while TLC checks every ordering the model allows.
- **They need scale that CI does not have.** #817 appeared 65 PRs into a 100-PR run, and #824 on a 20-PR run but not on smaller ones.
- **They show the symptom, not the cause.** #824 could not be root-caused because the row that would have explained it had already been garbage-collected. TLC returns the shortest sequence of steps that breaks the rule, each one named for a controller.
- **They need the code to exist.** A spec compares designs before any are built. #825 and #826 were caught by reviewers working out orderings in their heads during review of a fix. That works, but it doesn't scale, and it misses some.
- **Unit tests see one controller.** Every protocol bug above sits between two controllers or two services. #825's acceptance criteria ask for a hand-written test of the two writers interleaving, which can only be written after the ordering is already known.

## Decision

### When a spec is required

A change needs a spec, or an update to an existing one, when it alters any of these:

- the hand-off between pipeline stages,
- DLQ reconciliation,
- the order in which two writers compare-and-swap the same record,
- message IDs or dedup,
- a contract with another service,
- the queue's delivery semantics.

Everything else, including most controller logic, extensions, and APIs, does not.

### What a spec contains

A spec models one protocol and states what it leaves out. Each step is named for the controller it stands for and points at its file. The rules it checks are the repository's own rules: a terminal batch keeps its outcome, a reported result matches the target branch, persist before publish, every stuck entity eventually reaches a terminal state. One configuration checks today's behavior. Design alternatives are constants that are checked side by side, so choosing between them becomes a table rather than a debate.

### Where specs live and how they run

Specs live under `spec/{domain}/{protocol}/`, mirroring the domain folders. A spec that crosses services lives with the domain whose rule it checks. A shared model of the message queue lives under `spec/platform/`, and pipeline specs build on it. Specs do not live next to RFCs, because they need to share the queue model.

`tool/tlc` runs TLC under Bazel: the TLA+ tools JAR is pinned in `MODULE.bazel` and runs on a downloaded JDK, so nobody installs Java or fetches a JAR by hand, locally or in CI. Each spec has a matrix file listing its design constants and the expected verdict for every combination; `tlc_matrix_test` turns it into a test that `make test` runs, failing when any verdict changes. `bazel run //tool/tlc -- <matrix.json>` prints the table. A matrix stays small enough to check in under a minute; larger explorations are tagged `manual`.

### How specs stay tied to the code

A spec is a second description of the system, and it drifts unless something connects the two. That connection is built in steps, and each step stands on its own:

1. **Review.** A PR that changes a protocol from the list above updates the spec in the same PR. A protocol bug report includes the TLC trace, and its fix shows the trace gone.
2. **Check runs against the spec.** Map the request log and controller logs from e2e runs onto spec steps, and have TLC confirm that each recorded run is one the spec allows. This is the step that would have flagged #824.
3. **Replay specs against the code.** Have TLC list orderings, and play each one against real controllers wired to in-memory stores and queues. Controllers already take their dependencies as injected interfaces, which makes this unusually practical here.

## Evidence: the land outcome spec

[`spec/submitqueue/landoutcome`](../../spec/submitqueue/landoutcome/LandOutcome.tla) models one batch going from `landing` to a terminal state, across `land`, Runway's merge controller and its DLQ, `landsignal`, and the orchestrator's DLQs. It checks three rules: a failed batch is not on the branch, a succeeded batch is, and a landing batch eventually settles. Four constants hold the design choices, and its [matrix](../../spec/submitqueue/landoutcome/matrix.json) checks all 24 combinations in about six seconds.

| Orchestrator DLQ on a landing batch | Runway DLQ answers | Runway remembers verdicts | Answers dedup on request ID | Result |
|---|---|---|---|---|
| fail it (today) | any | any | any | Failed while merged ([#819](https://github.com/uber/submitqueue/issues/819)) |
| any | FAILED (today) | any | any | Failed while merged ([#820](https://github.com/uber/submitqueue/issues/820)) |
| leave it | checks the branch | any | any | Stuck in `landing` forever |
| re-send to Runway | checks the branch | no (today) | any | A stale FAILED wins |
| re-send to Runway | checks the branch | yes | yes (today) | Replay swallowed by dedup |
| re-send to Runway | checks the branch | yes | no | All three rules hold |

TLC found two live bugs. It also showed that the two obvious fixes are wrong, each in a way that does not show up in a quick run: one trades a wrong answer for a batch that never finishes, and the other trips over the dedup behavior from #352. It also turned four assumptions about Runway that no document states into explicit choices.

## What we do not get

- **Proof for every size.** TLC checks small instances, such as three batches and two faults. Protocol bugs nearly always show up at that size, and every bug above did, but a pass is strong evidence, not proof.
- **Code bugs.** #821's zero-value batch is invisible to a spec. Step 2 of tying specs to code catches the code straying from the spec, which is a different thing.
- **Correctness for free.** A spec that leaves out the wrong detail passes and means nothing. The first version of the matrix runner reported TLC parse errors as passes, and concurrent TLC runs sharing a temp directory failed intermittently. Specs and their harnesses need review like any other code.
- **Performance answers.** TLC says nothing about latency, throughput, or build budget sizing.

## Rollout

| Phase | Work | Done when |
|---|---|---|
| 1 | Choose the land outcome contract for #819 and #820 from the matrix | The fix for both issues ships with its matrix row passing |
| 2 | Queue model (redelivery, dedup, hold); batch creation and cancel; speculation outcomes | Each spec reproduces its historical bugs (#352, #822, #823, #817, #825, #826) and passes once the fix is in the model |
| 3 | Check e2e runs against the specs | A deliberately introduced deviation is flagged |
| 4 | Replay spec orderings against real controllers | Decided after phase 3 |

Phase 2 is the real test of this proposal. If a spec cannot reproduce the bug it was written for, or takes much more than a week to write, we stop at phase 1 and keep specs only for cross-service contracts.

## Alternatives considered

- **More e2e tests with fault injection.** Still one ordering per run, and still a new lever for every step that needs a fault. This complements specs but cannot replace them.
- **Deterministic simulation of the real code.** This means running the services under one scheduler with every store, queue, clock, and external call faked, then exploring orderings and faults. It is the strongest option because it tests the code itself, but it needs all of that infrastructure first. Phase 4 is the cheap first step toward it, and it is not ruled out.
- **Property-based tests of controllers.** These generate inputs for one controller at a time. Every bug above lives between controllers.
- **Other specification languages.** Quint has the same logic as TLA+ with a syntax closer to Go, and runs on the same checkers, so it remains an option for spec authors. P targets message-passing systems and can generate tests, but its tooling is thinner. Alloy is strong for data shapes and weaker for orderings over time. TLA+ has the most mature checker and the largest body of industrial use.

## Open questions

- Should spec review require a second reader who knows TLA+? Without one, an abstraction mistake gets no review.
- Do specs get written in PlusCal, which reads closer to Go, or in plain TLA+?
- Should Runway gain the per-request state that the one fully passing land design requires, or is there a cheaper design that also passes? Phase 1 answers this.
