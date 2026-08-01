# Consumer Hold

Letting a queue controller finish a delivery with "not now": the message is postponed for a chosen delay, its partition waits with it, and the wait costs no retry attempts, no new messages, and no blocked worker.

## Problem

A queue controller signals its outcome only through the result of processing: success acknowledges the message, a retryable failure requeues it, and a non-retryable failure rejects it to the dead-letter queue. The framework owns those outcomes; controllers cannot touch the delivery's lifecycle directly. That model has no vocabulary for the fourth thing controllers actually need to say: *this message is fine, but it must wait — check back in a few seconds.*

Two stovepipe stages need exactly that. The process stage must wait for a build slot when its queue is at its concurrency limit, and the buildsignal stage must wait between polls of a build that is still running. Both fake the wait the only way the current contract allows: acknowledge the delivery, then publish a fresh copy of the same message back to their own topic with a delayed visibility.

The workaround works, but every part of it is a smell:

- A consumer must also be a publisher to its own topic, carrying a publisher and topic registry it needs for nothing else.
- The queue's idempotent publish deduplicates on topic, partition, and message id, and the just-processed delivery's row is still present — so every republish must mint a fresh message id. The two stages grew two different id-minting schemes (a wall-clock suffix and a deterministic generation counter), one of which shipped a bug where the poll loop silently stalled after one tick.
- Each cycle is a fresh message, so the delivery attempt count restarts every time and the dead-letter backstop never fires for the waiting message itself.
- The republish is the loop's only continuation. A transient publish failure therefore kills the wait loop outright — the one failure mode the loop cannot afford.
- Every wait tick acknowledges one row and inserts another, churning the message log while nothing happens.

The process stage's design doc weighed this against parking the delivery in flight and extending its visibility, chose neither, and explicitly deferred to a future consumer-owned primitive. This RFC is that primitive.

## Decisions

### Hold is an intent recorded on the delivery, honored on success

The controller-facing delivery view gains a hold operation: record "postpone this delivery for N milliseconds" and keep processing. Recording is free of side effects — nothing touches the queue until the controller returns. When processing finishes successfully and a hold was recorded, the framework postpones the delivery instead of acknowledging it. When processing fails, the failure outcome wins: the recorded hold is discarded with a log line and a counter, and the normal retry or reject path runs. If the controller records more than one hold, the last one wins.

A hold is not an error, so it is not signaled as one — the repo's rule that errors are for failures, not control flow, rules out a sentinel error, and the controller contract keeps its existing shape, so no controller or mock changes. The delivery view is already the controller's channel for business-level delivery concerns (extending the visibility timeout of long work); hold joins it there.

### Postpone is a fourth outcome in the queue contract

The extension's delivery contract gains a postpone operation alongside acknowledge, requeue, and reject. Postpone finalizes the delivery — the worker returns, and later attempts to acknowledge or requeue the same delivery are rejected the same way a double acknowledge is — and schedules the message to redeliver after the delay.

Postpone is deliberately distinct from its two neighbors. Requeueing with a delay means "this delivery failed"; it counts toward the attempt limit that dead-letters poison messages, and it must, or genuinely broken messages would never dead-letter. Extending the visibility timeout means "I am still working"; the worker stays occupied, the partition stays blocked behind an in-flight delivery, and a missed extension lapses into a redelivery that costs a retry attempt. Postpone means "done for now, wake the partition later": the worker is released immediately, there is no lease to keep alive, and the redelivery is not a failure.

### A postponed message is a barrier: its partition waits with it

Holding exists to pause work, so a postponed message pauses its partition. The subscriber does not consume the partition past a postponed message while its delay runs; when the delay elapses, the postponed message redelivers first, in order, and consumption resumes behind it. A controller that keeps holding on each redelivery keeps the partition paused — that is the backoff loop, and it costs nothing between wake-ups: no goroutine waits, no lease is renewed, no buffer fills, and other partitions of the same topic are untouched.

This is the opposite of the requeue path on purpose. A failed message must not halt its partition, so a requeued message is skipped and later messages flow past it. A held message is a deliberate wait, so later messages queue up behind it. The two behaviors encode the two meanings: failure steps aside, waiting holds the line.

The barrier acts where messages are fetched, so it is best-effort at the boundary: deliveries already fetched into the consumer's in-memory buffer before the hold still process, bounded by the subscription's batch size. That is the same at-least-once posture the rest of the system already lives with, and controllers are already required to be idempotent and order-tolerant.

While a message is postponed it remains unacknowledged, so the partition's acknowledged watermark — and with it, log cleanup — waits with it. That is the same property any in-flight or requeued message has, and it is bounded by the hold delay.

### A postponed redelivery is not a failure

Redelivery is what advances a message's attempt count, and the dead-letter check runs against that count — so without care, a handful of holds would dead-letter a perfectly healthy message. Postpone therefore resets the failure accounting: the store marks the delivery state as postponed, the next delivery skips the attempt increment and clears the mark, and the attempt count the controller observes after a hold reads as a first attempt. Only consecutive failures since the last success or deliberate hold count toward dead-lettering. A visibility lapse while the delay runs is harmless — the mark survives until the next delivery consumes it.

The reset is a considered choice over merely exempting the one redelivery while preserving prior failure history: a controller that ran to completion and chose to wait has demonstrated the message is processable, so poison accounting should restart. The alternative remains implementable behind the same mark if experience demands it.

The honest consequence: a wait loop that stays healthy forever — a hung build whose status never turns terminal — never dead-letters, because nothing ever fails. The status quo has the identical property (its fresh publishes also restart the count), so hold does not weaken the backstop; it makes the gap explicit. Bounding a lawful-looking wait is domain knowledge (only the owning stage knows how long a build may run), so loop bounds stay with the domain, and postponement counts are exposed through metrics so an eternal waiter is visible operationally. A framework-level maximum hold horizon was rejected: the framework cannot distinguish a queue lawfully at its concurrency limit for hours from a wedged one.

### Liveness is framework-owned

If the postpone write itself fails, the framework logs and abandons the delivery, exactly as it does when an acknowledge fails: the visibility timeout lapses and the queue redelivers normally, at the cost of one retry attempt. The wait loop's continuation therefore never depends on a publish or any other side effect succeeding — the failure mode that kills today's republish loop cannot end a hold loop.

### The contract fits the technology space

The postpone wording binds only what every plausible backend can express. The repo's SQL-backed queue implements it exactly: per-partition polling makes the barrier a natural stop condition, and the delivery-state row carries the postponed mark. An in-memory queue is trivial. Log-backed consumers get the barrier for free by not advancing the partition's position. Lease-counting backends whose receive counter cannot be reset satisfy the accounting clause approximately — the contract requires a postponed redelivery not count against the failure budget to the extent the backend can express it, with the dead-letter guarantee stated per backend.

## Rejected

- **Acknowledge and republish with delayed visibility (status quo).** Every smell in the problem statement, and the loop's liveness hangs on a publish succeeding.
- **Park in flight and extend visibility.** Blocks a worker goroutine per waiting partition for the whole wait, backs messages up into the consumer's per-partition buffer until the topic's routing loop stalls other partitions, and a late renewal costs retry attempts — the attempt limit can dead-letter a healthy message that merely waited too long. Extension keeps its real job: one long unit of *active* work.
- **A non-blocking postpone that lets later messages flow past the held one.** If the partition should keep flowing, success already says that — acknowledge and let the next message carry the work. The only reason to hold is that the partition must wait, so the primitive blocks; offering both semantics doubles the contract for a case with no user.
- **Signaling hold through the processing result** — a sentinel error breaks the errors-are-failures rule, and a richer result type rewrites every controller and mock for one field.
- **A framework-imposed maximum hold horizon.** Dead-letters lawful long waits; bounds belong to the domain that understands them.
- **Driving the wait through the consumer gate's write surface from inside controllers.** The gate is the same postpone mechanism applied *before* the controller by an *external* decision — a stop/observe/start lever for tests and operators, opened from outside. A controller's own wait needs no gate state to flip: it holds its delivery directly. The two are easy to tell apart: the gate is someone stopping the controller at the door before it sees the message; hold is the controller seeing the work and deciding to come back later.

## Follow-ups

- Migrate the stovepipe process and buildsignal stages from acknowledge-and-republish to hold, deleting their message-id minting schemes and self-publish plumbing.
- Migrate the orchestrator's buildsignal stage, which carries the same republish pattern.
- Once those migrations land, the delayed-visibility publish operation on the publisher contract has no remaining callers — its only production use was the self-republish pattern hold replaces — and it can be removed, shrinking the set of wait mechanisms by one.
- Give buildsignal a domain-level bound on total poll duration so a hung build eventually fails instead of polling forever.
- Consumer and message-queue READMEs describe the new outcome when the implementation lands.
