# Queue health sampling

The sampler reads the active-request identity index, reloads current request and build state, and returns a bounded queue-level observation. It does not change concurrency or lifecycle decisions. Hosts schedule observations and own metrics, execution-pause suppression, and thresholds.

Pending work with capacity is a condition, not the latest head's age: alerting on a sustained condition prevents a stream of newer heads from continually resetting a stuck scheduler's timer. A nonterminal build's age starts at the retained trigger occurrence and includes time waiting at the runner; it is not pure execution time. Dispatch gaps, terminal-build finalization gaps, unaccounted occupied slots, and an uningested ref head are separate signals.

Terminal identities are removed only after reloading an immutable terminal request. A bounded primary-key scan never truncates into a healthy result: exceeding the observation budget, missing lifecycle timestamps, missing indexed requests, or a queue version change invalidates the whole sample. Hosts must record observation freshness separately and must not replace a failed observation with healthy zeroes or retain old health samples indefinitely.

Create the tracking table before upgrading ingestion. Drain or backfill pre-upgrade nonterminal requests before enabling dependent alerts. An unindexed nonterminal latest request produces an observation failure, while occupied slots without tracked processing requests produce the unaccounted-slot signal. Keep alerts non-paging until that rollout prerequisite has been verified. A rollback to writers without tracking invalidates the index's completeness and requires the same drain or backfill before re-enabling readers.
