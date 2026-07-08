# Build Prioritizer

Vendor-agnostic interface for the queue-wide policy that rations a shared build budget across every in-flight batch in a queue.

See the [Speculation RFC](../../../../doc/rfc/submitqueue/speculation.md) for the end-to-end design and how prioritization fits into the orchestrator pipeline.

## Why prioritization is not under speculation

Speculation seams (enumerator, scorer, selector) are **per batch** — they run inside `speculate`, which is partitioned per batch. Prioritization is **queue-wide** and lives at the **build stage**, where all of a queue's selected paths converge and the build budget is known. It is a different vantage point with a different lifetime, so it sits in its own `prioritization/` family rather than under `speculation/`.

## Prioritizer

Selection is per batch and blind to other batches, so it cannot ration a shared budget: if every batch selected generously, their combined demand could swamp CI. The prioritizer closes that gap. It sees every selected build across all of the queue's in-flight batches, ranks them by each build's score (plus any fairness or tie-break policy), and admits only the subset that fits the queue's concurrent-build budget. Selection expresses *desire* per batch; prioritization reconciles that desire against *supply* — it is the queue-wide enforcer.

The **store is the source of truth**, and the prioritizer is bound to its queue at construction — so it takes no arguments. It reads everything it needs for the whole queue from storage through read access injected at its factory: the pending builds and each build's speculation-path score, which serves as the admission priority. It ranks them and returns the admitted subset; it never writes — dispatching the admitted builds is the controller's job. The likely implementation is lightweight: priority-ordered consumption under a concurrency cap makes "top-N by score" emerge naturally; an explicit admit-top-N with preemption or fairness is the fallback when ordering alone is not enough.

## Factory

A per-queue factory returns the prioritizer for a queue, following the repo's extension contract. It is handed only the queue identity; read access to the build and tree stores, the prioritization limit, fairness policy, and capacity signals are injected at construction by the integrator in the wiring layer. Prioritization itself stays config-free.
