# standard

The `standard` `Speculator` funds the queue's most promising speculation paths first, until the build budget is spent.

Each run it considers candidate paths in descending order of their probability of being the future that actually happens, and proposes builds down that ranking. Paths already pending or building keep the slot they hold rather than restarting; paths whose builds already finished are skipped for as long as their records remain in the supplied path sets, so a finished path can be proposed again — for a retry, say — once retention drops it; new builds fill whatever budget remains.

When the budget runs out, everything below the cut waits for a later run. That is safe because the propose-side cannot invent a batch verdict: the speculate controller still decides land from the persisted paths, including complete coverage of unsettled dependencies.

Both halves are swappable. The ranking is the `Generator`'s: the default `bestfirst` scores each path by the probability that all its assumptions hold. The budget policy is the `Allocator`'s: the default `sticky` fills only free slots and never preempts, where a preempting allocator would cancel a low-value in-flight path to fund a better one.

`standard` itself decides nothing — it connects the `Generator`'s stream to the `Allocator` — so changing prioritization or budget behavior means swapping a part, not writing a new `Speculator`.

The `Generator`/`Allocator` split is internal to `standard`, not part of the `Speculator` contract: an alternate `Speculator` could compute build and cancel decisions directly.
