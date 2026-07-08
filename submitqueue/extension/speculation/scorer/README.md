# Speculation Path Scorer

Vendor-agnostic interface for scoring the paths in a batch's **speculation tree** — the predicted-success probability of each candidate bet, recomputed as the batch's world changes.

See the [Speculation RFC](../../../../doc/rfc/submitqueue/speculation.md) for the end-to-end design and how scoring fits into the orchestrator pipeline.

## Scorer

A path's score is a **prediction**: *how likely is this bet to pay off, right now?* The scorer answers it from the current state — the per-batch success probabilities of a path's base batches (`entity.Batch.Score`, set by the score stage), which of those dependencies have already landed or had their build pass (resolved assumptions raise confidence), and optionally other signals such as how long the batch has waited or historical pass rates. The score is the common currency the [selector](../selector) and prioritizer both rank on, so keeping it current is what makes both act on the latest reality.

Because it is a prediction over live state, the scorer is **re-run on every respeculate**, right after the controller reconciles path status — so when a dependency lands, its build passes, or a sibling path fails, the surviving paths' scores are recomputed before anything is selected or prioritized. The controller drives *when* to rescore (it is part of reconciliation) and persists the result; the scorer owns the *formula*.

This is the per-**path** scorer, distinct from the per-**batch** [score stage](../../scorer), which sets `entity.Batch.Score`. The path scorer consumes those batch scores to score whole paths. The **store is the source of truth**: the scorer is handed only the batch identity and loads what it needs — the batch's speculation tree (already carrying the controller-reconciled statuses) and its dependency batches — through read access injected at its factory. It never writes: it returns the scored tree and the controller persists the scores, so the controller stays the single writer of tree state. Path structure and status pass through unchanged; only `Score` is recomputed.

## Factory

A per-queue factory returns the scorer for a queue, following the repo's extension contract. It is handed only the queue identity; read access to the tree and batch stores, scoring knobs, and any extra signals are injected at construction by the integrator in the wiring layer, which resolves per-queue settings through `queueconfig`. Scoring itself stays config-free.
