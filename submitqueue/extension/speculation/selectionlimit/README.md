# Speculation Selection Limit

Vendor-agnostic "how much" policy that bounds how many paths a batch may build in parallel.

See the [Speculation RFC](../../../../doc/rfc/submitqueue/speculation.md) for the end-to-end design and how limits fit into the two-layer speculation model.

## Selection Limit

The selection limit is the [selector](../selector)'s companion. The selector decides **which** of a batch's paths are worth building — its ranking over the tree; the selection limit decides **how many** of them may run at once. Keeping "which" and "how much" separate keeps selector logic free of resource accounting and lets the bound scale with build resources without touching that logic.

The value is **signal-driven**, not a fixed constant. Its primary input is the build system's available capacity, but a policy may also weigh historical pass rates, cost budgets, time of day, or an experiment toggle.

Unlike the dependency limit — which the controller holds and applies as an eligibility gate — the selection limit is **injected into the seam that uses it**: the selector is constructed with it and calls it itself, never receiving it as a method parameter. This follows the repo's extension-contract pattern (dependencies injected at the `Factory`), keeps the selector interface limit-free and stable, and lets the limit be swapped independently of selector logic.

## Factory

A per-queue factory returns the limit policy for a queue, following the repo's extension contract. It is handed only the queue identity; the signals a policy weighs — a capacity feed, historical metrics, config — are injected at construction by the integrator in the wiring layer, which is also where the limit is handed to the selector. Computing the limit itself takes no further inputs.
