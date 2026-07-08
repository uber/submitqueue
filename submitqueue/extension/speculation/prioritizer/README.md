# Speculation Prioritizer

Vendor-agnostic interface for the queue-wide policy that rations a shared build budget across every in-flight batch in a queue.

See the [Speculation RFC](/doc/rfc/submitqueue/speculation.md) for the end-to-end design and how prioritization fits into the orchestrator pipeline.

## Prioritizer

Selection is per batch and blind to other batches, so it cannot ration a shared budget: if every batch promoted generously, their combined demand could swamp CI. The prioritizer closes that gap. It sees every path across all of the queue's in-flight batches that is running or wants to run, ranks them by each path's score (plus any fairness or tie-break policy), and admits only the subset that fits the queue's concurrent-build budget. Selection expresses *desire* per batch; prioritization reconciles that desire against *supply* — it is the queue-wide enforcer.

The controller hands the prioritizer the queue's candidate paths directly — every path that is `Selected` (wants a slot) or `Prioritized`/`Building` (holds a slot), each carrying its identity and score. It returns **sparse decisions**, each naming a path by its ID: `Promote` to admit a pending path, `Cancel` to preempt a running one. Paths it omits are left as-is, and it returns at most one decision per path. It never writes: the controller maps each decision back to its tree (it remembers which tree each candidate came from), maps it to a status transition (`Promote` → `Prioritized`, `Cancel` → `Cancelling`) applied under that tree's optimistic lock, and enacts it, staying the single writer.

`Prioritized` means **admitted under the queue's build budget and cleared to build, but not yet building** — the path holds a slot and is waiting for the build stage to dispatch it and a build signal to confirm it (at which point it becomes `Building`). It is deliberately not called "admitted": batches are admitted at several points in the queue's lifecycle, while `Prioritized` names exactly the grant this seam makes. Contrast it with `Selected`, which is only the batch-level *desire* for a slot — a selected path has no claim on the budget until the prioritizer promotes it.

**Whether to preempt is the prioritizer's own policy**, swappable per queue. A sticky-slots implementation never emits `Cancel` for a running path — it only fills free slots, and a higher-priority path waits until a slot frees. A preemptive implementation ranks running and pending paths together and may `Cancel` a running path to admit a higher-scored one. Both read the same input through the same interface; only the ranking/eviction logic differs. (Preemption discards in-flight CI work, so "fill free slots only" is a common default.)

Prioritization is queue-wide — a different vantage point than the per-batch seams (enumerator, scorer, selector) — but it acts in the same currency: it ranks on the same path `Score` the scorer produces (a probability in [0, 1] under the [path scorer](../pathscorer)'s contract — the prioritizer needs no other assumption about the values, only that all paths in a queue share the scale) and emits the same `PathDecision` the selector does. Where the queue-wide reconcile runs in the pipeline is an integration detail; the contract here is unaffected by it.

## Factory

A per-queue factory returns the prioritizer for a queue, following the repo's extension contract. It is handed only the queue identity; the prioritization limit, fairness policy, and capacity signals are injected at construction by the integrator in the wiring layer, which resolves per-queue settings through `queueconfig`. Prioritization itself stays config-free.
