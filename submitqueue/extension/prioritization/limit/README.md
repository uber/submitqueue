# Prioritization Limit

Vendor-agnostic "how much" policy that bounds how many builds a queue may run at once — the queue's concurrent-build budget.

See the [Speculation RFC](../../../../doc/rfc/submitqueue/speculation.md) for the end-to-end design and how limits fit into the two-layer speculation model.

## Prioritization Limit

The prioritization limit is the [prioritizer](../prioritizer)'s companion. The prioritizer decides **which** of the queue's pending builds run — its ranking across all in-flight batches; the prioritization limit decides **how many** fit at once. It is the queue-wide resource knob, the ultimate cap on speculation's demand on CI.

The value is **signal-driven**, not a fixed constant. Its primary input is the build system's available capacity, but a policy may also weigh cost budgets, time of day, or an experiment toggle.

It is **injected into the prioritizer** at construction and called by it, never passed as a method parameter — following the repo's extension-contract pattern, keeping the prioritizer interface limit-free and stable, and letting the limit be swapped independently of prioritizer logic.

## Factory

A per-queue factory returns the limit policy for a queue, following the repo's extension contract. It is handed only the queue identity; the signals a policy weighs — a capacity feed, cost budgets, config — are injected at construction by the integrator in the wiring layer, which is also where the limit is handed to the prioritizer. Computing the limit itself takes no further inputs.
