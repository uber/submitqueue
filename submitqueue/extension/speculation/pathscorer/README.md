# Speculation Path Scorer

Vendor-agnostic interface for scoring the paths in a batch's **speculation tree** — the predicted-success probability of each candidate bet, recomputed as the batch's world changes.

See the [Speculation RFC](/doc/rfc/submitqueue/speculation.md) for the end-to-end design and how scoring fits into the orchestrator pipeline.

## Scorer

A path's score is a **prediction**: *how likely is this bet to pay off, right now?* The scorer answers it from the current state — the per-batch success probabilities of a path's base batches (`entity.Batch.Score`, set by the score stage), which of those dependencies have already landed or had their build pass (resolved assumptions raise confidence), and optionally other signals such as how long the batch has waited or historical pass rates. The score is the common currency the [selector](../selector) and prioritizer both rank on, so keeping it current is what makes both act on the latest reality.

Because it is a prediction over live state, the scorer is **re-run on every respeculate**, right after the controller reconciles path status — so when a dependency lands, its build passes, or a sibling path fails, the surviving paths' scores are recomputed before anything is selected or prioritized. The controller drives *when* to rescore (it is part of reconciliation) and persists the result; the scorer owns the *formula*.

This is the per-**path** scorer, distinct from the per-**batch** [score stage](../../scorer), which sets `entity.Batch.Score`. The path scorer consumes those batch scores to score whole paths. The controller hands it the batch's **speculation tree** directly — the subject it scores — and any richer signal an implementation needs (the dependency batches' scores, historical pass rates) is injected at its factory, not passed in. It never writes: its only output is per-path scores, each naming a path by its ID, and the controller merges them into the tree and persists — the controller stays the single writer of tree state, and everything else about a path (structure, status) never passes through the scorer at all. Paths omitted from the result keep their last persisted score.

Scores are **probabilities in [0, 1]** — 0 is a bet certain to lose, 1 a bet certain to pay off. That is the contract every implementation must satisfy, and the controller enforces the range when it consumes the result. The selector and prioritizer rank on these values, so implementations sharing a queue must agree on this scale.

## Factory

A per-queue factory returns the scorer for a queue, following the repo's extension contract. It is handed only the queue identity; scoring knobs and read access to any extra signals are injected at construction by the integrator in the wiring layer, which resolves per-queue settings through `queueconfig`. Scoring itself stays config-free.
