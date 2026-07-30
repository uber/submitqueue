// Copyright (c) 2025 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package speculate

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	corebatch "github.com/uber/submitqueue/submitqueue/core/batch"
	"github.com/uber/submitqueue/submitqueue/core/publish"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/speculator"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// Controller handles speculate queue messages.
//
// Each message is a dirty signal: it names the batch that changed, but only so
// the queue wakes up. The controller then re-plans that whole queue from a
// single read — see run — asking the Speculator which paths are worth building
// within the budget and cancelling the ones a resolved dependency has ruled
// out. Nothing carries over between runs, so duplicated or reordered signals
// are harmless and a later run repairs whatever an earlier one left half-done.
//
// Batch verdicts are still the naive per-batch state machine below, which
// advances the triggering batch one step:
//
//   - Created → admit to Speculating so the Speculator can act on it.
//   - Speculating → if all deps are Succeeded, publish to merge and
//     transition to Merging; otherwise no-op (or fail-fast if a dep is
//     in a non-succeeding terminal state).
//   - Cancelling → cancel any in-flight Build entity, respeculate
//     dependents, CAS to terminal Cancelled, publish to conclude.
//   - Merging → no-op (owned by the merge controller).
//   - Terminal → re-fan-out to conclude for self-healing in case a
//     prior publish was lost.
//
// Waiting on every dependency is strictly stricter than waiting on the ones a
// passed path assumed, so this is correct while the path-aware finalization
// that replaces it is written — it just does not yet collect the speedup the
// paths are earning.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	stores        storage.Factory
	speculators   speculator.Factory
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// opName is the metric operation name shared by every emit in this package.
const opName = "process"

// NewController creates a new speculate controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores storage.Factory,
	speculators speculator.Factory,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("speculate_controller"),
		metricsScope:  scope.SubScope("speculate_controller"),
		stores:        stores,
		speculators:   speculators,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process re-plans the triggering batch's queue, then advances that batch one
// step along the legacy per-batch state machine (see the package doc).
// Returns nil to ack (success), or error to nack (retry).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	msg := delivery.Message()

	bid, err := entity.BatchIDFromBytes(msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		return fmt.Errorf("failed to deserialize batch ID: %w", err)
	}

	store, err := c.stores.For(storage.Config{QueueName: bid.Queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_resolve_errors", 1)
		// Non-retryable: a missing or unresolvable queue is a malformed message.
		return fmt.Errorf("failed to resolve storage for queue %q: %w", bid.Queue, err)
	}

	batch, err := store.GetBatchStore().Get(ctx, bid.ID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get batch %s: %w", bid.ID, err)
	}

	// The payload's queue must match the batch's authoritative queue; a
	// mismatch is a malformed message. Non-retryable — reject to the DLQ.
	if bid.Queue != "" && bid.Queue != batch.Queue {
		metrics.NamedCounter(c.metricsScope, opName, "queue_mismatch", 1)
		return fmt.Errorf("payload queue %q does not match queue %q of batch %s", bid.Queue, batch.Queue, batch.ID)
	}

	// Cancelling intent: the cancel controller has handed this batch off to
	// speculate to drive to terminal. Cancel in-flight builds, fan out to
	// dependents, CAS to terminal Cancelled, and publish to conclude.
	if batch.State == entity.BatchStateCancelling {
		return c.cancelBatch(ctx, store, batch)
	}

	// Terminal state: re-fan-out for self-healing in case a previous publish
	// was lost. Always re-publish to conclude (idempotent on the batch ID).
	// For Cancelled specifically also re-publish to dependents — a crash
	// between the terminal CAS and the dependent publish would otherwise
	// leave them stuck waiting on a Cancelled dep.
	if batch.State.IsTerminal() {
		metrics.NamedCounter(c.metricsScope, opName, "self_heal_terminal", 1)
		// Repair the membership record for the same crash window: a prior
		// attempt may have CAS'd to terminal without completing the record move.
		if err := corebatch.EnsureRecord(ctx, store, batch); err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
			return err
		}
		if batch.State == entity.BatchStateCancelled {
			if err := c.respeculateDependents(ctx, store, batch); err != nil {
				return err
			}
		}
		return c.fanout(ctx, batch.ID, batch.Queue)
	}

	// A Created batch is admitted before the run so the Speculator can see it:
	// proposals may only target Speculating heads.
	wasCreated := batch.State == entity.BatchStateCreated
	if wasCreated {
		admitted, err := c.admit(ctx, store, batch)
		if err != nil {
			return err
		}
		batch = admitted
	}

	if err := c.run(ctx, store, batch.Queue); err != nil {
		return err
	}

	// A freshly admitted head stops here rather than falling through to the
	// finalizer. The run above funded its first paths and nothing has been
	// built yet — finalizing on the same message would let a batch with no
	// dependencies merge before any build had run.
	if wasCreated {
		return nil
	}

	// Merging is owned by the merge controller, which has its own self-heal.
	if batch.State == entity.BatchStateMerging {
		metrics.NamedCounter(c.metricsScope, opName, "noop_merging", 1)
		// A redelivery can land here after a tryFinalize attempt crashed
		// between its CAS and the record move; repair before acking.
		if err := corebatch.EnsureRecord(ctx, store, batch); err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
			return err
		}
		return nil
	}

	if batch.State == entity.BatchStateSpeculating {
		return c.tryFinalize(ctx, store, batch)
	}

	metrics.NamedCounter(c.metricsScope, opName, "unexpected_state", 1)
	return fmt.Errorf("unexpected batch state %q for batch %s", batch.State, batch.ID)
}

// admit moves a batch from Created to Speculating, which is what makes it
// visible to the Speculator as an action target. It returns the batch with the
// new state and version so the caller keeps writing against a current copy.
//
// It no longer dispatches anything itself: which paths to build for this head
// is the run's decision, taken over the whole queue rather than one batch at a
// time.
func (c *Controller) admit(ctx context.Context, store storage.Storage, batch entity.Batch) (entity.Batch, error) {
	newVersion := batch.Version + 1
	batch.State = entity.BatchStateSpeculating
	if err := store.GetBatchStore().Update(ctx, batch, batch.Version, newVersion); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return batch, fmt.Errorf("failed to update batch %s state to speculating: %w", batch.ID, err)
	}
	batch.Version = newVersion
	batch.State = entity.BatchStateSpeculating

	metrics.NamedCounter(c.metricsScope, opName, "admitted", 1)
	c.logger.Infow("admitted batch to speculation",
		"batch_id", batch.ID,
		"queue", batch.Queue,
		"dependencies", batch.Dependencies,
	)
	return batch, nil
}

// tryFinalize publishes to merge and transitions to Merging iff every
// dependency batch has reached Succeeded. Cancelled deps are treated as
// out-of-the-way: the cancelled batch will never land, so it can no longer
// conflict — drop it from the chain and proceed. Failed deps still cascade
// via failOnDependency. If some deps are still in flight, the call is a
// no-op and waits for the next event.
//
// TODO: when a dependency fails we currently fail this batch outright.
// We will need to respeculate the failed paths — drop the failed dep
// from the chain and re-issue speculation for the surviving ordering(s)
// — instead of cascading the failure into requests that could still land.
func (c *Controller) tryFinalize(ctx context.Context, store storage.Storage, batch entity.Batch) error {
	deps, err := c.fetchDependencies(ctx, store, batch)
	if err != nil {
		return err
	}

	pending := make([]string, 0, len(deps))
	for _, d := range deps {
		switch d.State {
		case entity.BatchStateSucceeded:
			// ok
		case entity.BatchStateCancelled:
			// Out-of-the-way: the cancelled batch will never land, so it can
			// no longer conflict. Drop it from the chain and continue.
			metrics.NamedCounter(c.metricsScope, opName, "dependency_cancelled_skipped", 1)
			c.logger.Infow("dependency cancelled; dropping from speculation chain",
				"batch_id", batch.ID,
				"dependency_id", d.ID,
			)
		case entity.BatchStateFailed:
			return c.failOnDependency(ctx, store, batch, d)
		default:
			pending = append(pending, d.ID)
		}
	}

	if len(pending) > 0 {
		metrics.NamedCounter(c.metricsScope, opName, "waiting_on_deps", 1)
		c.logger.Debugw("dependencies still in flight; waiting",
			"batch_id", batch.ID,
			"pending_dependency_ids", pending,
		)
		return nil
	}

	if err := c.publishBatchID(ctx, topickey.TopicKeyMerge, batch.ID, batch.Queue, batch.Queue); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish to merge: %w", err)
	}

	if _, err := corebatch.Transition(ctx, store, batch, entity.BatchStateMerging); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return err
	}

	return nil
}

// failOnDependency transitions a Speculating batch to Failed when one of its
// dependencies has reached a non-succeeding terminal state, then publishes to
// the conclude queue so the request store and request log get reconciled.
// Without this transition the batch would sit in Speculating forever — no
// downstream event ever fires for it again.
func (c *Controller) failOnDependency(ctx context.Context, store storage.Storage, batch entity.Batch, dep entity.Batch) error {
	metrics.NamedCounter(c.metricsScope, opName, "dependency_failed", 1)
	c.logger.Warnw("dependency in non-succeeding terminal state; failing batch",
		"batch_id", batch.ID,
		"dependency_id", dep.ID,
		"dependency_state", string(dep.State),
	)

	batch, err := corebatch.Transition(ctx, store, batch, entity.BatchStateFailed)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return err
	}

	if err := c.publishBatchID(ctx, topickey.TopicKeyConclude, batch.ID, batch.Queue, batch.Queue); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish to conclude: %w", err)
	}

	return nil
}

// cancelBatch drives a batch from BatchStateCancelling to BatchStateCancelled.
// The cancel controller records the user's intent (Cancelling) and hands the
// batch off; speculate owns the rest because all the work that must precede
// the terminal write — flipping in-flight builds, respeculating dependents —
// already lives in the speculate domain. The terminal transition is the
// single writer of every non-Cancelling batch state across the system.
//
// Order matters for correctness:
//
//  1. Cancel the in-flight Build entity (build.ID == batch.ID; one Get + one
//     Update covers all builds for this batch). A future external CI
//     integration hooks in here. Idempotent: tolerate ErrNotFound (no build
//     was scheduled), skip if already terminal.
//
//  2. CAS the batch to terminal Cancelled. This must happen BEFORE the
//     dependent fan-out: tryFinalize only drops a Cancelled dep from the
//     chain, so dependents woken with the dep still in Cancelling would
//     wait pending and never get pinged again.
//
//  3. Re-publish each downstream dependent to speculate so they can drop
//     this cancelled batch from their chain and advance (or finalize, if
//     this was their last outstanding dep).
//
//  4. Publish to conclude so contained requests reach RequestStateCancelled.
//
// A crash between steps 2 and 3/4 is recovered on redelivery via the
// terminal self-heal branch, which re-runs the dependent fan-out and the
// conclude publish for already-Cancelled batches.
//
// storage.ErrVersionMismatch on the terminal CAS is returned as-is because it
// is intrinsically retryable; the redelivery will land in the
// self-heal branch and complete the fan-out.
func (c *Controller) cancelBatch(ctx context.Context, store storage.Storage, batch entity.Batch) error {
	metrics.NamedCounter(c.metricsScope, opName, "cancel_batch", 1)
	c.logger.Infow("cancelling batch",
		"batch_id", batch.ID,
		"queue", batch.Queue,
	)

	// TODO(respeculate-collateral): re-enqueue Land for every request in batch.Contains
	// except the user-cancelled request. Today the whole batch dies (per spec) and the
	// collateral requests need a fresh request ID and a re-publish to TopicKeyStart so
	// they can be re-batched without the cancelled change.

	if err := c.cancelBuild(ctx, store, batch); err != nil {
		return err
	}

	batch, err := corebatch.Transition(ctx, store, batch, entity.BatchStateCancelled)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return err
	}

	if err := c.respeculateDependents(ctx, store, batch); err != nil {
		return err
	}

	if err := c.publishBatchID(ctx, topickey.TopicKeyConclude, batch.ID, batch.Queue, batch.Queue); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish to conclude: %w", err)
	}

	return nil
}

// cancelBuild flips any in-flight Build entity for the batch to
// BuildStatusCancelled. Builds use build.ID == batch.ID, so a single Get
// covers every build scheduled for the batch. Tolerates ErrNotFound (no
// build was ever scheduled — the batch was cancelled before speculation
// started building) and skips already-terminal builds.
//
// This is the hook point for a future external CI integration: today the
// system has no external runner, so the local state flip is the complete
// cancellation. Once a runner exists, it must be invoked here before the
// local Update.
func (c *Controller) cancelBuild(ctx context.Context, store storage.Storage, batch entity.Batch) error {
	build, err := store.GetBuildStore().Get(ctx, batch.ID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			metrics.NamedCounter(c.metricsScope, opName, "cancel_build_not_found", 1)
			return nil
		}
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get build for batch %s: %w", batch.ID, err)
	}

	if build.Status.IsTerminal() {
		metrics.NamedCounter(c.metricsScope, opName, "cancel_build_already_terminal", 1)
		return nil
	}

	updatedBuild := build
	updatedBuild.Status = entity.BuildStatusCancelled
	if err := store.GetBuildStore().Update(ctx, updatedBuild); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to cancel build for batch %s: %w", batch.ID, err)
	}
	metrics.NamedCounter(c.metricsScope, opName, "cancel_build_done", 1)
	return nil
}

// respeculateDependents publishes a speculate event for every batch that
// depends on the given batch. The batch controller creates a BatchDependent
// row (with Dependents possibly empty) for every batch it persists, so a
// missing row at this point is a storage invariant violation, not a normal
// "no dependents" case — surface ErrNotFound as a regular storage error so
// the message nacks and either an operator or the batch controller's own
// crash-recovery can resolve the inconsistency.
//
// Called both from the cancelBatch terminal flow and from the terminal
// self-heal branch on redelivery of an already-Cancelled batch.
func (c *Controller) respeculateDependents(ctx context.Context, store storage.Storage, batch entity.Batch) error {
	bd, err := store.GetBatchDependentStore().Get(ctx, batch.ID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get batch dependents for batch %s: %w", batch.ID, err)
	}

	for _, depID := range bd.Dependents {
		// Alternative: process each dependent inline (load batch, run the
		// equivalent of tryFinalize) instead of publishing back to ourselves.
		// Rejected for now: per-message retry isolation, fresh per-dependent
		// reads, consumer-pool parallelism / backpressure, and the existing
		// state-machine dispatch in Process all argue for the publish. Revisit
		// if the extra message hop ever shows up as latency or cost.
		if err := c.publishBatchID(ctx, topickey.TopicKeySpeculate, depID, batch.Queue, batch.Queue); err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
			return fmt.Errorf("failed to publish dependent batch %s to speculate: %w", depID, err)
		}
		metrics.NamedCounter(c.metricsScope, opName, "dependent_respeculated", 1)
	}
	return nil
}

// fetchDependencies loads each batch in batch.Dependencies. Any storage error
// is surfaced as a retryable infra failure; missing dependencies should not
// happen in practice, but if one does it is treated the same as a transient
// fetch failure (i.e. the message is retried).
func (c *Controller) fetchDependencies(ctx context.Context, store storage.Storage, batch entity.Batch) ([]entity.Batch, error) {
	deps := make([]entity.Batch, 0, len(batch.Dependencies))
	for _, depID := range batch.Dependencies {
		d, err := store.GetBatchStore().Get(ctx, depID)
		if err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "dependency_fetch_errors", 1)
			return nil, fmt.Errorf("failed to get dependency batch %s of %s: %w", depID, batch.ID, err)
		}
		deps = append(deps, d)
	}
	return deps, nil
}

// fanout re-publishes downstream events for a batch that has already reached
// a terminal state. Used for self-healing when a previous publish was lost:
// re-sending to conclude guarantees request-state reconciliation.
func (c *Controller) fanout(ctx context.Context, batchID, queue string) error {
	if err := c.publishBatchID(ctx, topickey.TopicKeyConclude, batchID, queue, queue); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish to conclude: %w", err)
	}
	return nil
}

// publishBatchID publishes a batch ID to the topic behind key, stamped with
// the batch's queue and partitioned by partitionKey. The batch ID doubles as
// the message ID, so the queue deduplicates repeat publishes for the same
// batch against rows it has not collected yet.
//
// queue and partitionKey are separate because they answer different questions.
// queue is what the payload asserts the batch belongs to, and the consumer
// resolves its storage from it, so it must always be the real queue. The
// partition key only decides what stays serialized behind what: most publishes
// want the queue, so a queue's batches are processed in order, but the build
// dispatch partitions by batch so heads dispatch in parallel.
func (c *Controller) publishBatchID(ctx context.Context, key consumer.TopicKey, batchID, queue, partitionKey string) error {
	payload, err := entity.BatchID{ID: batchID, Queue: queue}.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize batch ID: %w", err)
	}
	return publish.Message(ctx, c.registry, key, batchID, payload, partitionKey)
}

// Name returns the controller name for logging and metrics.
func (c *Controller) Name() string {
	return "speculate"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
