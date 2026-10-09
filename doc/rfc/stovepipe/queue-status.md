# Queue status lookup

Callers need to look up whether a queue is enabled, the commit at which that state took effect, and whether execution is currently paused. They may also need the enablement policy covering a particular commit, including an intermediate commit with no validation request. A current enablement flag loses earlier intervals after disable/re-enable; validation events and last-green describe validation results rather than queue enablement. A pause flag describes execution only. `GetQueueStatus` provides these queue-status answers through one lookup endpoint.

## Lookup contract

`GetQueueStatus(queue, change_uri)` returns current applied policy and an independent queue-level execution observation. An empty `change_uri` selects current-only lookup. A successful commit lookup also returns `policy_for_commit`; that commit need not have a validation request. See the [published contract](../../../api/stovepipe/proto/stovepipe.proto) for fields and comments.

Each policy has an enabled/disabled state, a monotonically increasing revision, an inclusive commit boundary, and application time in Unix milliseconds. Initial explicitly registered queues have disabled revision 1 with no boundary. Lookup never registers a queue or creates history. Both policy fields resolve from the same pinned current pointer, even if a concurrent change commits during the lookup.

Execution is separate: RUNNING means unpaused at queue scope, PAUSED means queue-wide execution is paused, and UNKNOWN means the control could not be determined. Pausing does not change enablement. Its timestamp is observation time, not the pause transition time. Scoped stage or partition pauses need separate reporting. Execution read failure preserves a valid policy; caller cancellation ends the lookup.

## Boundaries and history

Boundaries move forward along the queue lineage; equal boundaries are allowed and the latest revision wins. For ENABLED at A, DISABLED at D, and ENABLED at G, a lookup for C returns enabled revision 2 while F returns disabled revision 3, even though current policy is enabled revision 4. Ancestry is inclusive.

The controller pins `SourceControl.Latest()`, checks that the requested commit and latest policy boundary belong to that tip, then walks committed policy occurrences newest-first until a boundary covers the commit. It returns the retained initial disabled policy only after reaching a validated initial occurrence. Rewritten or unrelated lineage cannot fall back to disabled. Missing records are consistency failures, not lookup misses. Calls scale with the policy transitions examined, not batch size.

The production adapter uses the existing code-gateway path: `IsAncestor` maps to `IsAncestorOf` / `CompareCommits`, with IDENTICAL and HEAD_AHEAD representing inclusive ancestry. No local Git checkout, intermediate-commit index, or new SourceControl method is required. The adapter must validate URI repository/ref identity and preserve dependency failures instead of classifying every RPC failure as not-found. Arbitrary merged side-branch ordering and lineage resets require an explicit policy before they are supported.

## Applying a policy

Host configuration or administration calls `queuepolicy.Writer.Initialize` for explicitly registered queues and `Apply` to commit a change. Apply requires a caller-owned operation ID, the expected policy revision, the desired state, and an explicitly chosen immutable boundary URI. Reuse the same request on retry; do not select a fresh tip. Stale revisions require rereading and an explicit new decision. Successful replay of a committed operation returns the original occurrence even after later changes. Redundant same-state changes are rejected.

The writer creates an immutable transition, then conditionally advances the current pointer with a controller-computed storage version. A losing proposal is unreachable and never applied. The predecessor chain provides committed history using only primary-key operations; no cross-record transaction, secondary index, or multi-record atomic write is required. Partial initialization and interrupted writes can be retried. Unreachable proposals are retained to preserve idempotency; future cleanup needs a retention policy that preserves retry guarantees.

The applied policy records queue enablement independently of admission and validation execution; disabled queues may continue shadow validation. Keep disabled queues and their history readable. Publish optional notifications only after the pointer update succeeds; durable notification delivery and a public history/replay RPC are outside this change.

## Host integration and rollout

1. Apply the new MySQL schema before using the policy store. Register configured queues before serving lookups; the standalone example initializes them as disabled and reports RUNNING because it uses the no-op execution gate.
2. In the production host, wire the lookup controller to its existing queue-bound code-gateway SourceControl factory. Supply a queueexecution.Reader using the same Flipr control as the consumer gate, evaluating only queue-wide constraints. Do not turn a stage-scoped pause into a whole-queue answer.
3. Connect the actual enable/disable administration path to Writer.Apply and expose only durably applied state. For queues already enabled at migration time, initialize and apply their agreed activation boundary before serving the endpoint. Do not replace an existing applied policy with a bare desired flag or startup default.
4. Callers can omit the commit for a current-status lookup or supply an immutable commit URI for the policy covering that commit. Retain the returned revision and defer policy-dependent decisions when resolution fails. An answer is relative to its revision; later transitions can cover the same commit, so reconciliation must refresh it.

The endpoint reports status; each caller defines how that status affects its workflow. A current-status response alone cannot enumerate transitions missed between polls. Callers that need every enable/disable occurrence require a durable history/replay contract in a follow-up change.

## Failure contract

The gRPC adapter returns INVALID_ARGUMENT for malformed selectors, NOT_FOUND for an unregistered policy, FAILED_PRECONDITION for unknown commits or unresolved lineage, and INTERNAL for inconsistent retained policy history. Dependency errors keep their original identity for host-specific classification; cancellation is preserved. Every supplied but unresolved commit fails the request rather than returning a successful current-only or disabled answer.

## Validation

Behavior tests cover intermediate commits, inclusive/equal boundaries, delayed ingestion across disable/re-enable, pinned policy reads, pause independence, unknown execution, rewritten lineage, missing history, operation replay, interrupted writes, and concurrent policy proposals. MySQL integration tests exercise byte-exact operation IDs, conditional versions, immutable records, and queue isolation.
