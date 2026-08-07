# speculator

The `speculator` package defines the one speculation extension the speculate controller calls. A `Speculator` decides **which speculation paths to build and which running ones to cancel**, within the queue's build budget — and nothing else. It can never express a verdict: whether a batch merges or fails is fixed by the facts and computed by the controller, so swapping in a different `Speculator` changes which paths run, never a batch's outcome.

`Speculate` is handed the queue's in-flight batches plus any finalized batches still referenced as dependencies (each with its dependency list and state) and every path set for them — live and recently finished, so a `Speculator` will not re-propose a path that already passed or failed. It returns a list of build and cancel actions; a path it wants left as-is has no entry in the result. The controller validates the output (dropping builds it shouldn't propose and rejecting cancels of passed paths), so an implementation may read extra injected data without affecting correctness.

`Cancel` is a `Speculator`'s only cancel power — preempting an in-flight path to free budget for a better candidate. Correctness cancels (refuting a path whose bet a resolved dependency broke, and batch cancellation) belong to the controller and are not routed through the extension.

Like the other extensions, a `Speculator` is selected **per queue** by the wiring layer through the `Config` (queue name) and `Factory` interface. Budget, clock, and any extra data are injected at construction by the integrator, not carried on the contract.

[`standard`](standard/README.md) is the default implementation: it funds the most promising paths first until the build budget is spent.

## Adding a backend

Create a package under `speculator/<backend>/` whose `New(...)` returns a `speculator.Speculator`, injecting whatever it needs at construction. Resolve any content it requires internally; do not add a `Config` or `Factory` implementation here — per-queue routing and the factory adapter live in the wiring layer.
