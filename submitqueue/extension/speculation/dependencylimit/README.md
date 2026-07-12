# Speculation Dependency Limit

Vendor-agnostic "how much" policy that bounds how many **active** (in-flight, non-terminal) dependencies a batch may speculate over.

See the [Speculation RFC](/doc/rfc/submitqueue/speculation.md) for the end-to-end design and how limits fit into the two-layer speculation model.

## Dependency Limit

Speculation splits into *decision seams* (what to build) and *limit policies* (how much to allow). The dependency limit is the first limit: it is the **eligibility gate** for speculation. A batch becomes eligible to enumerate only when its count of active dependencies is at or below the current limit; otherwise it waits. Nothing is dropped — as dependencies land they leave the active set, the count shrinks, and the batch is admitted. The gate applies even to the fully-stacked happy path, so a very long chain is not speculated in full at once.

The value is **signal-driven**, not a fixed constant. Its primary input is the build system's available capacity, so a period of CI pressure can shrink how deep the queue speculates, but a policy may also weigh historical pass rates, cost budgets, time of day, or an experiment toggle. Because the value is dynamic, a change to the limit alone — not only a landing dependency or a DAG change — can newly admit a waiting batch.

Unlike the selection and prioritization limits, the dependency limit is **not injected into a decision seam**. It gates eligibility *before* enumeration and needs active-dependency reconciliation, which is controller orchestration — so the controller holds it, consults it on every respeculate, and applies it, keeping the enumerator pure.

## Factory

A per-queue factory returns the limit policy for a queue, following the repo's extension contract. It is handed only the queue identity; the signals a policy weighs — a capacity feed, historical metrics, config — are injected at construction by the integrator in the wiring layer. Computing the limit itself takes no further inputs.
