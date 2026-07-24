# sticky

The `sticky` `Allocator` fills only free budget slots and leaves every in-flight build running. Against the budget it counts the paths that still hold a slot: `pending`, `building`, and `cancelling` — a cancel is only a request, and the build keeps its slot until it actually stops. Only path status enters this — no batch state does, since the budget tracks builds occupying CI.

It then proposes a build for each new candidate, in order, until the budget fills. Two kinds of candidate are skipped, for different reasons. A candidate matching an already-funded path is holding a slot already — it is counted above — so it needs no additional slot and no second build action. A candidate whose path is terminal in the path sets holds no slot and needs none. Neither consumes free budget. It never proposes a cancel.

The budget is measured in builds: one slot per path, no matter what that path's build does. That is deliberately coarse — a twelve-hour build and a two-minute build cost the same slot. An allocator that understands build size (target count, historical duration) could pack the same budget more effectively; `sticky` trades that for simplicity.

A stored path with no status at all is a data defect, and `sticky` fails fast: `Allocate` returns an error rather than guessing what the record means.

The trade-off: a better candidate arriving while the budget is full waits for a running build to finish rather than displacing it. `sticky` never discards work already started, and it cannot react to a newly attractive path mid-flight. Its counterpart is a preempting `Allocator` that cancels a low-value in-flight path to fund a better one, spending a cancel now to converge faster on the next run.
