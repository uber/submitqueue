# allocator

The `allocator` package defines a composition point used by the `standard` `Speculator`: an `Allocator` spends the queue's build budget over a candidate iterator. Like `generator`, it is **not** a controller-facing extension — an alternate `Speculator` need not use or expose this split — so there is no `Config` or `Factory` here; an `Allocator` is chosen when the `standard` `Speculator` is constructed.

`Allocate` pulls candidates in the order the iterator yields them and matches them against the queue's current path sets by path ID. A pending or building path keeps the slot it already holds rather than starting a second attempt. A path whose build already finished comes back round as a candidate — the generator enumerates the whole space — and is skipped. Neither draws on free budget.

Pending, building, and cancelling paths all charge the budget; a cancelling build holds its slot until it reaches a terminal state. Terminal paths charge nothing. `Allocate` returns the build and cancel actions that spend what is left.

A cancelled or expired context aborts the run with its error and no actions. The output is all-or-nothing on purpose: a partial list would fund an arbitrary prefix of the ranking, leaving the budget half-spent on whatever was pulled first.

Because cancellation is best-effort, an `Allocator` should not spend capacity it only expects a cancel to release. A build cancelled to make room keeps charging the budget until that cancel reaches a terminal state, so the queue converges over successive runs instead of oversubscribing the hard cap in one pass.

How the budget is measured is the implementation's choice. The simplest unit is a build: every path costs one slot, whatever its build does. An `Allocator` that understands build size — target count, historical cost — could weight paths instead and pack the budget more tightly.
