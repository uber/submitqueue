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
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// Controller handles speculate queue messages.
//
// Naive happy-path algorithm: assume every in-flight build will pass and
// treat batch.Dependencies + [batch.ID] as the single speculation chain.
// Per invocation, the controller advances the batch one step in the
// state machine:
//
//   - Created → publish to build, transition to Speculating.
//   - Speculating       → if all deps are Succeeded, publish to merge and
//     transition to Merging; otherwise no-op (or fail-fast if a dep is
//     in a non-succeeding terminal state).
//   - Cancelling        → cancel any in-flight Build entity, respeculate
//     dependents, CAS to terminal Cancelled, publish to conclude. The
//     cancel controller hands the batch off in this state and speculate
//     drives it to terminal.
//   - Merging           → no-op (owned by the merge controller).
//   - Terminal          → re-fan-out to conclude for self-healing in case a
//     prior publish was lost. For terminal Cancelled, also re-fan-out
//     dependents so a crash between the terminal CAS and the dependent
//     publish does not strand them.
//
// The controller is re-triggered on every relevant downstream event
// (buildsignal, merge), so each call simply re-evaluates the current
// state and either advances or waits.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	store         storage.Storage
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// opName is the metric operation name shared by every emit in this file.
const opName = "process"

// NewController creates a new speculate controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	store storage.Storage,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("speculate_controller"),
		metricsScope:  scope.SubScope("speculate_controller"),
		store:         store,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process advances a batch one step along the naive happy-path.
// Returns nil to ack (success), or error to nack (retry).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	msg := delivery.Message()

	bid, err := entity.BatchIDFromBytes(msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		return fmt.Errorf("failed to deserialize batch ID: %w", err)
	}

	batch, err := c.store.GetBatchStore().Get(ctx, bid.ID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get batch %s: %w", bid.ID, err)
	}

	// Cancelling intent: the cancel controller has handed this batch off to
	// speculate to drive to terminal. Cancel in-flight builds, fan out to
	// dependents, CAS to terminal Cancelled, and publish to conclude.
	if batch.State == entity.BatchStateCancelling {
		return c.cancelBatch(ctx, batch)
	}

	// Terminal state: re-fan-out for self-healing in case a previous publish
	// was lost. Always re-publish to conclude (idempotent on the batch ID).
	// For Cancelled specifically also re-publish to dependents — a crash
	// between the terminal CAS and the dependent publish would otherwise
	// leave them stuck waiting on a Cancelled dep.
	if batch.State.IsTerminal() {
		metrics.NamedCounter(c.metricsScope, opName, "self_heal_terminal", 1)
		if batch.State == entity.BatchStateCancelled {
			if err := c.respeculateDependents(ctx, batch); err != nil {
				return err
			}
		}
		return c.fanout(ctx, batch.ID, batch.Queue)
	}

	// Merging is owned by the merge controller, which has its own self-heal.
	if batch.State == entity.BatchStateMerging {
		metrics.NamedCounter(c.metricsScope, opName, "noop_merging", 1)
		return nil
	}

	switch batch.State {
	case entity.BatchStateCreated:
		return c.startSpeculation(ctx, batch)
	case entity.BatchStateSpeculating:
		return c.tryFinalize(ctx, batch)
	default:
		metrics.NamedCounter(c.metricsScope, opName, "unexpected_state", 1)
		return fmt.Errorf("unexpected batch state %q for batch %s", batch.State, batch.ID)
	}
}

// startSpeculation kicks off CI for this batch on top of the speculative head
// (batch.Dependencies assumed to all pass), then transitions to Speculating.
func (c *Controller) startSpeculation(ctx context.Context, batch entity.Batch) error {
	c.logger.Infow("starting speculation",
		"batch_id", batch.ID,
		"speculation_chain", append(append([]string{}, batch.Dependencies...), batch.ID),
	)

	if err := c.publish(ctx, topickey.TopicKeyBuild, batch.ID, batch.Queue); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish to build: %w", err)
	}

	// Optimistic CAS: if the version has already advanced (concurrent speculate),
	// the next event will see the new state and behave correctly.
	newVersion := batch.Version + 1
	if err := c.store.GetBatchStore().UpdateState(ctx, batch.ID, batch.Version, newVersion, entity.BatchStateSpeculating); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to update batch %s state to speculating: %w", batch.ID, err)
	}

	metrics.NamedCounter(c.metricsScope, opName, "started_speculation", 1)
	return nil
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
func (c *Controller) tryFinalize(ctx context.Context, batch entity.Batch) error {
	deps, err := c.fetchDependencies(ctx, batch)
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
			return c.failOnDependency(ctx, batch, d)
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

	if err := c.publish(ctx, topickey.TopicKeyMerge, batch.ID, batch.Queue); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish to merge: %w", err)
	}

	newVersion := batch.Version + 1
	if err := c.store.GetBatchStore().UpdateState(ctx, batch.ID, batch.Version, newVersion, entity.BatchStateMerging); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to update batch %s state to merging: %w", batch.ID, err)
	}

	return nil
}

// failOnDependency transitions a Speculating batch to Failed when one of its
// dependencies has reached a non-succeeding terminal state, then publishes to
// the conclude queue so the request store and request log get reconciled.
// Without this transition the batch would sit in Speculating forever — no
// downstream event ever fires for it again.
func (c *Controller) failOnDependency(ctx context.Context, batch entity.Batch, dep entity.Batch) error {
	metrics.NamedCounter(c.metricsScope, opName, "dependency_failed", 1)
	c.logger.Warnw("dependency in non-succeeding terminal state; failing batch",
		"batch_id", batch.ID,
		"dependency_id", dep.ID,
		"dependency_state", string(dep.State),
	)

	newVersion := batch.Version + 1
	if err := c.store.GetBatchStore().UpdateState(ctx, batch.ID, batch.Version, newVersion, entity.BatchStateFailed); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to update batch %s state to failed: %w", batch.ID, err)
	}

	if err := c.publish(ctx, topickey.TopicKeyConclude, batch.ID, batch.Queue); err != nil {
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
//     UpdateStatus covers all builds for this batch). A future external CI
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
func (c *Controller) cancelBatch(ctx context.Context, batch entity.Batch) error {
	metrics.NamedCounter(c.metricsScope, opName, "cancel_batch", 1)
	c.logger.Infow("cancelling batch",
		"batch_id", batch.ID,
		"queue", batch.Queue,
	)

	// TODO(respeculate-collateral): re-enqueue Land for every request in batch.Contains
	// except the user-cancelled request. Today the whole batch dies (per spec) and the
	// collateral requests need a fresh request ID and a re-publish to TopicKeyStart so
	// they can be re-batched without the cancelled change.

	if err := c.cancelBuild(ctx, batch); err != nil {
		return err
	}

	newVersion := batch.Version + 1
	if err := c.store.GetBatchStore().UpdateState(ctx, batch.ID, batch.Version, newVersion, entity.BatchStateCancelled); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to update batch %s state to cancelled: %w", batch.ID, err)
	}
	batch.Version = newVersion
	batch.State = entity.BatchStateCancelled

	if err := c.respeculateDependents(ctx, batch); err != nil {
		return err
	}

	if err := c.publish(ctx, topickey.TopicKeyConclude, batch.ID, batch.Queue); err != nil {
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
// local UpdateStatus.
func (c *Controller) cancelBuild(ctx context.Context, batch entity.Batch) error {
	build, err := c.store.GetBuildStore().Get(ctx, batch.ID)
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

	if err := c.store.GetBuildStore().UpdateStatus(ctx, batch.ID, entity.BuildStatusCancelled); err != nil {
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
func (c *Controller) respeculateDependents(ctx context.Context, batch entity.Batch) error {
	bd, err := c.store.GetBatchDependentStore().Get(ctx, batch.ID)
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
		if err := c.publish(ctx, topickey.TopicKeySpeculate, depID, batch.Queue); err != nil {
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
func (c *Controller) fetchDependencies(ctx context.Context, batch entity.Batch) ([]entity.Batch, error) {
	deps := make([]entity.Batch, 0, len(batch.Dependencies))
	for _, depID := range batch.Dependencies {
		d, err := c.store.GetBatchStore().Get(ctx, depID)
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
func (c *Controller) fanout(ctx context.Context, batchID, partitionKey string) error {
	if err := c.publish(ctx, topickey.TopicKeyConclude, batchID, partitionKey); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish to conclude: %w", err)
	}
	return nil
}

// publish publishes a batch ID to the specified topic key.
func (c *Controller) publish(ctx context.Context, key consumer.TopicKey, batchID string, partitionKey string) error {
	bid := entity.BatchID{ID: batchID}
	payload, err := bid.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize batch ID: %w", err)
	}

	msg := entityqueue.NewMessage(batchID, payload, partitionKey, nil)

	q, ok := c.registry.Queue(key)
	if !ok {
		return fmt.Errorf("no queue registered for topic key %s", key)
	}

	topicName, ok := c.registry.TopicName(key)
	if !ok {
		return fmt.Errorf("no topic name registered for topic key %s", key)
	}

	if err := q.Publisher().Publish(ctx, topicName, msg); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
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
