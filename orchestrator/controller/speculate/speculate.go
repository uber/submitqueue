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
	"fmt"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/entity"
	entityqueue "github.com/uber/submitqueue/entity/queue"
	"github.com/uber/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// Controller handles speculate queue messages.
//
// Naive happy-path algorithm: assume every in-flight build will pass and
// treat batch.Dependencies + [batch.ID] as the single speculation chain.
// Per invocation, the controller advances the batch one step in the
// state machine:
//
//   - Created or Scored → publish to build, transition to Speculating.
//   - Speculating       → if all deps are Succeeded, publish to merge and
//     transition to Merging; otherwise no-op (or fail-fast if a dep is
//     in a non-succeeding terminal state).
//   - Merging           → no-op (owned by the merge controller).
//   - Terminal          → re-fan-out to conclude for self-healing in case a
//     prior publish was lost.
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
	c.metricsScope.Counter("received").Inc(1)

	msg := delivery.Message()

	bid, err := entity.BatchIDFromBytes(msg.Payload)
	if err != nil {
		c.metricsScope.Counter("deserialize_errors").Inc(1)
		return fmt.Errorf("failed to deserialize batch ID: %w", err)
	}

	batch, err := c.store.GetBatchStore().Get(ctx, bid.ID)
	if err != nil {
		c.metricsScope.Counter("storage_errors").Inc(1)
		return fmt.Errorf("failed to get batch %s: %w", bid.ID, err)
	}

	// Terminal state: re-fan-out to conclude for self-healing. The batch is
	// already done; if a previous publish was lost, downstream stages will
	// otherwise never reconcile. Re-publishing is safe because conclude is
	// idempotent on the batch ID.
	if batch.State.IsTerminal() {
		c.metricsScope.Counter("self_heal_terminal").Inc(1)
		return c.fanout(ctx, batch.ID, batch.Queue)
	}

	// Merging is owned by the merge controller, which has its own self-heal.
	if batch.State == entity.BatchStateMerging {
		c.metricsScope.Counter("noop_merging").Inc(1)
		return nil
	}

	switch batch.State {
	case entity.BatchStateCreated, entity.BatchStateScored:
		return c.startSpeculation(ctx, batch)
	case entity.BatchStateSpeculating:
		return c.tryFinalize(ctx, batch)
	default:
		c.metricsScope.Counter("unexpected_state").Inc(1)
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

	if err := c.publish(ctx, consumer.TopicKeyBuild, batch.ID, batch.Queue); err != nil {
		c.metricsScope.Counter("publish_errors").Inc(1)
		return fmt.Errorf("failed to publish to build: %w", err)
	}

	// Optimistic CAS: if the version has already advanced (concurrent speculate),
	// the next event will see the new state and behave correctly.
	newVersion := batch.Version + 1
	if err := c.store.GetBatchStore().UpdateState(ctx, batch.ID, batch.Version, newVersion, entity.BatchStateSpeculating); err != nil {
		c.metricsScope.Counter("storage_errors").Inc(1)
		return fmt.Errorf("failed to update batch %s state to speculating: %w", batch.ID, err)
	}

	c.metricsScope.Counter("started_speculation").Inc(1)
	return nil
}

// tryFinalize publishes to merge and transitions to Merging iff every
// dependency batch has reached Succeeded. If any dep is Failed/Cancelled,
// the batch cannot land on top of it; we mark it Failed and hand off to
// conclude so the request state and log are reconciled. Otherwise (some
// deps still in flight) it no-ops and waits for the next event.
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
		case entity.BatchStateFailed, entity.BatchStateCancelled:
			return c.failOnDependency(ctx, batch, d)
		default:
			pending = append(pending, d.ID)
		}
	}

	if len(pending) > 0 {
		c.metricsScope.Counter("waiting_on_deps").Inc(1)
		c.logger.Debugw("dependencies still in flight; waiting",
			"batch_id", batch.ID,
			"pending_dependency_ids", pending,
		)
		return nil
	}

	if err := c.publish(ctx, consumer.TopicKeyMerge, batch.ID, batch.Queue); err != nil {
		c.metricsScope.Counter("publish_errors").Inc(1)
		return fmt.Errorf("failed to publish to merge: %w", err)
	}

	newVersion := batch.Version + 1
	if err := c.store.GetBatchStore().UpdateState(ctx, batch.ID, batch.Version, newVersion, entity.BatchStateMerging); err != nil {
		c.metricsScope.Counter("storage_errors").Inc(1)
		return fmt.Errorf("failed to update batch %s state to merging: %w", batch.ID, err)
	}

	c.metricsScope.Counter("processed").Inc(1)
	return nil
}

// failOnDependency transitions a Speculating batch to Failed when one of its
// dependencies has reached a non-succeeding terminal state, then publishes to
// the conclude queue so the request store and request log get reconciled.
// Without this transition the batch would sit in Speculating forever — no
// downstream event ever fires for it again.
func (c *Controller) failOnDependency(ctx context.Context, batch entity.Batch, dep entity.Batch) error {
	c.metricsScope.Counter("dependency_failed").Inc(1)
	c.logger.Warnw("dependency in non-succeeding terminal state; failing batch",
		"batch_id", batch.ID,
		"dependency_id", dep.ID,
		"dependency_state", string(dep.State),
	)

	newVersion := batch.Version + 1
	if err := c.store.GetBatchStore().UpdateState(ctx, batch.ID, batch.Version, newVersion, entity.BatchStateFailed); err != nil {
		c.metricsScope.Counter("storage_errors").Inc(1)
		return fmt.Errorf("failed to update batch %s state to failed: %w", batch.ID, err)
	}

	if err := c.publish(ctx, consumer.TopicKeyConclude, batch.ID, batch.Queue); err != nil {
		c.metricsScope.Counter("publish_errors").Inc(1)
		return fmt.Errorf("failed to publish to conclude: %w", err)
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
			c.metricsScope.Counter("dependency_fetch_errors").Inc(1)
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
	if err := c.publish(ctx, consumer.TopicKeyConclude, batchID, partitionKey); err != nil {
		c.metricsScope.Counter("publish_errors").Inc(1)
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
