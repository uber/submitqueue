# Speculation Path Selector

Vendor-agnostic interface for deciding what the orchestrator should do with each path in a batch's enumerated speculation tree.

See the [Speculation RFC](../../../../doc/rfc/submitqueue/speculation.md) for the end-to-end design and how selection fits into the orchestrator pipeline.

## Selector

A selector is the **policy** — the part that decides how aggressively to spend build resources. *Given the candidate paths in the batch's tree and their current status, what should we do with each, right now?* It returns an **action** per path — `Build` or `Cancel`. Strategies span a spectrum: build only the single optimistic path (cheapest — bet on the happy case), build every candidate (maximum parallelism, maximum build cost), or a top-K / budget-bounded subset in between.

The selector decides only where to spend build resources. It does **not** decide merging: a path becomes mergeable when its build passed and its base matches what actually landed, which is deterministic, not a policy choice — so the controller finalizes it on its own.

The **store is the source of truth**. The selector is handed only the batch identity and loads that batch's tree from storage through read access injected at its factory. The controller is the single writer — it reconciles each path's status (candidate, building, passed, failed, cancelled) from the latest builds and dependency states and persists it, plus the score — so the stored tree the selector reads is always the up-to-date input. The selector's only output is actions; it **never** writes status. This keeps it a deterministic policy over stored state.

Because it is re-run on every build signal, a selector can start narrow — build the optimistic path first — and widen later, committing more paths only once earlier bets resolve. Returning no action for a path leaves it as-is. Policy parameters — a top-K cap, a build budget, an experiment toggle — are configured when the selector is constructed rather than passed through this contract.

## Factory

A per-queue factory returns the selector for a queue, following the repo's extension contract. It is handed only the queue identity and nothing else; policy knobs — a top-K cap, a build budget, an experiment toggle — are injected at construction by the integrator in the wiring layer, which resolves per-queue settings through `queueconfig`. Selection itself stays config-free.
