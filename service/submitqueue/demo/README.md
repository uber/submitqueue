# Demo workload runner

This package is the reusable part of the SubmitQueue demo. The OSS command in the sibling `requests` package and internal commands import `github.com/uber/submitqueue/service/submitqueue/demo`; deployment configuration, authentication, source construction, and connection ownership remain with those commands.

`Run` generates changes through a supplied source. `RunExisting` takes revisions already resolved by the caller and never invokes the source. Both return known changes, accepted request identifiers, and the last observed request histories, including on partial failure. A request identifier is opaque and must always be used with its queue.

Independent changes are created and submitted with bounded concurrency. Each can enter the queue as soon as it is ready. Burst mode prepares every change before submitting any request. Stacked mode creates a chain sequentially, prepares its members, and submits their ordered URIs as one request.

Sources own their base branch or snapshot. A stack link receives the preceding change's branch and head; a source must honor that parent or fail rather than silently retargeting it. The OSS providers retain their frozen-base behavior. A remote source that resolves a branch at creation time can use a moving base without promising identical commit ancestry across independent changes.

Readiness is optional and only runs when submission is enabled. A command can wait for real approvals or checks without baking that policy into the workload generator. Existing-change stack validation is the caller's responsibility.

The runner owns polling and interactive terminal cleanup. It waits for recorded histories rather than sampling only current status, preserving fast transitions and build metadata. Monitoring failures are not request failures: permanent errors stop the local watch without inventing a terminal server state, and temporary failures retain the last known status and history.

Mutating source and submission calls are never retried automatically. A timeout may hide a successful remote operation; inspect the reported artifacts before retrying. Stopping the command does not cancel submitted work, close PRs, or delete branches.

Workload layout is deterministic for a supplied `RunID`; an empty identifier receives a timestamp and random suffix so simultaneous runs do not collide. File/folder controls and the existing OSS command flags and Make targets are unchanged.
