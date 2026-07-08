# Speculation Path Selector

Vendor-agnostic interface for deciding what the orchestrator should do with each path in a batch's enumerated speculation tree.

See the [Speculation RFC](/doc/rfc/submitqueue/speculation.md) for the end-to-end design and how selection fits into the orchestrator pipeline.

## Selector

A selector is the **policy** — the part that decides how aggressively to spend build resources. *Given the candidate paths in the batch's tree and their current status, what should we do with each, right now?* It returns an **action** per path — `Promote` (advance it one stage toward running) or `Cancel`. Strategies span a spectrum: build only the single optimistic path (cheapest — bet on the happy case), build every candidate (maximum parallelism, maximum build cost), or a top-K / budget-bounded subset in between.

The selector decides only where to spend build resources. It does **not** decide merging: a path becomes mergeable when its build passed and its base matches what actually landed, which is deterministic, not a policy choice — so the controller finalizes it on its own.

The controller hands the selector the batch's **speculation tree** directly — the subject it decides over. The controller is the single writer: it reconciles each path's status (candidate, selected, prioritized, building, passed, failed, cancelling, cancelled) from the latest builds and dependency states, and it maps each of the selector's decisions to a status transition — `Promote` → `Selected`, `Cancel` → `Cancelling` (or `Cancelled` when nothing is building) — applied under the tree's optimistic lock and persisted. The selector's only output is decisions, each naming a path by its ID, at most one per path; it **never** writes status. This keeps it a deterministic policy over the tree it is given.

Selection expresses **desire, not admission**: `Selected` means this batch wants to spend a build slot on the path, while `Prioritized` means the queue-wide prioritizer has actually admitted it under the shared build budget and it is cleared to build. A promoted path waits in `Selected` until the prioritizer clears it — with free budget it would move on promptly, under contention it may wait indefinitely or be dropped. That split exists because a selector sees only its own batch and cannot ration a budget shared across the whole queue.

Because it is re-run on every build signal, a selector can start narrow — build the optimistic path first — and widen later, committing more paths only once earlier bets resolve. Returning no action for a path leaves it as-is. Policy parameters — a top-K cap, a build budget, an experiment toggle — are configured when the selector is constructed rather than passed through this contract.

## Factory

A per-queue factory returns the selector for a queue, following the repo's extension contract. It is handed only the queue identity and nothing else; policy knobs — a top-K cap, a build budget, an experiment toggle — are injected at construction by the integrator in the wiring layer, which resolves per-queue settings through `queueconfig`. Selection itself stays config-free.
