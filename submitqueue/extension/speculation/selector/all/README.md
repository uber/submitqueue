# All Selector

The all `selector.Selector` scans a batch's speculation tree and returns a Promote decision for every path currently in the Candidate status, in tree order — it builds every candidate path, maximum parallelism at maximum build cost. It has no opinion on any path in another status — Selected, Prioritized, Building, or a terminal status — and simply omits those, leaving them as-is for the controller. If the tree has no candidate paths, it returns no decisions.

This is the least discriminating selection policy: it never narrows the candidate set, so it always asks to build every path the enumerator produced for a batch. It is the baseline and the counterpart to the queue-wide prioritizer, which is where an actual budget is enforced — the all selector expresses maximal per-batch desire, and the prioritizer (see the [prioritizer](../../prioritizer) extension, such as its `sticky` implementation) reconciles that desire against the queue's shared build capacity.
