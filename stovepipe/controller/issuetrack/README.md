# Issue tracking

`issuetrack.Controller` consumes `IssueTrack` notifications in a single queue partition. It alone maintains operational `OpenIssue` membership and records `IssueLog` transitions; build detection and immutable per-build observations remain in `buildanalysis`. The controller rejects messages whose partition key differs from the payload queue. Producers and consumers must preserve per-queue serialization; the progress row is not a distributed lock or a substitute for that delivery contract.

## Reconciliation

A refresh notification traverses its build's observations in bounded task/unit batches. A missing issue is opened using this build as its first-seen build. Repeated observations preserve existing openings and do not create another transition. Final reconciliation also traverses open issues and resolves those absent from the source build. Cancelled builds may add confirmed observations but never establish resolution. Final notifications must name the request's winning terminal build; duplicate-build losers are ignored.

The final-notification contract is an input requirement, not something terminal status alone proves: detection must have succeeded, its persisted observations must be complete and authoritative for the queue's relevant validation scope, and in-flight analysis must not add unaccounted results after the final handoff. Accumulated observations are not automatically a current failure snapshot. A producer that cannot satisfy those requirements must not request absence-based resolution. No producer or consumer is wired by this implementation; the production handoff and recovery policy remain follow-up work.

## Source ordering and interrupted passes

Notification decisions are separate from storage writes. New and resumed work use the same execution path, which reloads the build named by saved progress. After resuming, the incoming notification is reconsidered only if the completed work did not already satisfy it.

`IssueTrackingStore` retains one progress row per queue, including the source request, build, phase, and optimistic version. Request IDs are compared through `CompareRequestID`; provider build IDs and timestamps are not order keys. An older request cannot modify current issues. Later refresh notifications and duplicate finals for an already-finalized request are ignored. Same-request refresh notifications remain eligible until finalization. The progress row remains even when there are no open issues.

Before applying any issue writes, the controller conditionally records a refreshing or finalizing phase. It records the completed phase only after issue and history writes succeed. A subsequent delivery first finishes pending work, including when the original delivery was retried or dead-lettered; only then may it admit a newer source. A permanent pending failure therefore needs operational repair rather than silently skipping a partially applied transition. Controller code owns version arithmetic; storage performs guarded replacement only.

Opening history is repaired on replay if an open row was created before its log write failed. Resolution is retained before deleting the matching opening, so failed history persistence leaves the issue available for retry. Duplicate transition identities preserve their original timestamps. Replaying a whole pass avoids a multi-row transaction or a durable cursor over mutable membership. The operational progress and accepted writes are serialized, not an atomic database snapshot.

This controller does not publish fix requests, expose a user-facing issue read model, or materialize `IssueSummary`. Those are separate responsibilities. Lifecycle history is retained here; a log-derived summary/materializer can be added without changing the observation handoff or the source-order guard.
