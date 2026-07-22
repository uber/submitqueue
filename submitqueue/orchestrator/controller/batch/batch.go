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

package batch

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/extension/counter"
	"github.com/uber/submitqueue/platform/metrics"
	corerequest "github.com/uber/submitqueue/submitqueue/core/request"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/conflict"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// Controller handles batch queue messages.
// It consumes validated requests, groups them into batches, and publishes to the score stage.
// Implements consumer.Controller interface for integration with the consumer.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	registry      consumer.TopicRegistry
	counter       counter.Counter
	store         storage.Storage
	analyzers     conflict.Factory
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// NewController creates a new batch controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	registry consumer.TopicRegistry,
	counter counter.Counter,
	store storage.Storage,
	analyzers conflict.Factory,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("batch_controller"),
		metricsScope:  scope.SubScope("batch_controller"),
		registry:      registry,
		counter:       counter,
		store:         store,
		analyzers:     analyzers,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process processes a batch delivery from the queue.
// Deserializes the request, groups into batch, and publishes to the score topic.
// Returns nil to ack (success), or error to nack (retry).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	const opName = "process"

	op := metrics.Begin(c.metricsScope, opName, metrics.LongLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()

	// Deserialize request ID from payload
	rid, err := entity.RequestIDFromBytes(msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		return fmt.Errorf("failed to deserialize request ID: %w", err)
	}

	// Fetch request from storage
	request, err := c.store.GetRequestStore().Get(ctx, rid.ID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get request %s: %w", rid.ID, err)
	}

	c.logger.Infow("received batch event",
		"request_id", request.ID,
		"queue", request.Queue,
		"state", string(request.State),
		"version", request.Version,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	// Short-circuit if the request has been halted — either it already reached a
	// terminal state, or the cancel controller has recorded a cancellation intent
	// (RequestStateCancelling). A halted request must never spawn a new batch.
	if entity.IsRequestStateHalted(request.State) {
		c.metricsScope.Counter("skipped_halted").Inc(1)
		c.logger.Infow("skipping batch for halted request",
			"request_id", request.ID,
			"state", string(request.State),
		)
		return nil
	}

	// TODO: if capacity is full, wait here for other requests to accumulate to batch them together, or include a request into an existing batch if it's not too late.

	// Generate a globally unique batch ID.
	seq, err := c.counter.Next(ctx, "batch/"+request.Queue)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "counter_errors", 1)
		return fmt.Errorf("failed to generate batch ID for queue=%s: %w", request.Queue, err)
	}

	batch := entity.Batch{
		ID:       fmt.Sprintf("%s/batch/%d", request.Queue, seq),
		Queue:    request.Queue,
		Contains: []string{request.ID},
		State:    entity.BatchStateCreated,
		Version:  1,
	}

	// Get active batches for this queue and ask the conflict analyzer which
	// of them the new batch must serialize behind. The dependency set drives
	// the speculation graph downstream.
	activeBatches, err := c.store.GetBatchStore().GetByQueueAndStates(ctx, request.Queue, entity.DependencyBatchStates())
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "batch_store_errors", 1)
		return fmt.Errorf("failed to get active batches for queue=%s: %w", request.Queue, err)
	}

	// Dedupe by batch ID since a single (analyzed, in-flight) pair may be
	// reported with multiple Conflict entries when different conflict types
	// apply; the dependency graph only tracks the relation.
	analyzer, err := c.analyzers.For(conflict.Config{QueueName: batch.Queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "conflict_analyzer_errors", 1)
		return fmt.Errorf("failed to build conflict analyzer for queue=%s: %w", batch.Queue, err)
	}
	conflicts, err := analyzer.Analyze(ctx, batch, activeBatches)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "conflict_analyzer_errors", 1)
		return fmt.Errorf("failed to analyze conflicts for batchID=%s: %w", batch.ID, err)
	}

	seen := make(map[string]struct{}, len(conflicts))
	conflictingIDs := make([]string, 0, len(conflicts))
	for _, cf := range conflicts {
		if _, ok := seen[cf.BatchID]; ok {
			continue
		}
		seen[cf.BatchID] = struct{}{}
		conflictingIDs = append(conflictingIDs, cf.BatchID)
	}

	batch.Dependencies = conflictingIDs

	// Update reverse index for each conflicting batch (BatchDependent =
	// "batches that depend on me"). One UpdateDependents call per conflict.
	for _, depID := range conflictingIDs {
		existing, err := c.store.GetBatchDependentStore().Get(ctx, depID)
		if err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "batch_dependent_store_errors", 1)
			return fmt.Errorf("failed to get batch dependent for batchID=%s: %w", depID, err)
		}

		dependents := append(existing.Dependents, batch.ID)

		newVersion := existing.Version + 1
		if err := c.store.GetBatchDependentStore().UpdateDependents(ctx, depID, existing.Version, newVersion, dependents); err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "batch_dependent_store_errors", 1)
			return fmt.Errorf("failed to update batch dependent index for existing batchID=%s and new batchID=%s: %w", depID, batch.ID, err)
		}
	}

	// Create new reverse index entry for the new batch. It would be empty for now, but will be updated as new batches are created that conflict with this batch.
	bd := entity.BatchDependent{
		BatchID:    batch.ID,
		Dependents: []string{},
		Version:    1,
	}

	if err := c.store.GetBatchDependentStore().Create(ctx, bd); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "batch_dependent_store_errors", 1)
		return fmt.Errorf("failed to create batch dependent index for new batchID=%s: %w", batch.ID, err)
	}

	// Claim the request for this batch with a CAS-write that transitions the
	// request to RequestStateBatched. This CAS is the serialization point
	// between the batch controller and the cancel controller — without it, the
	// two would race over an empty interleaving and produce an orphan batch
	// containing a cancelled request.
	//
	// Concrete race that this CAS closes (T1..T7 are wall-clock orderings of
	// independent batch- and cancel-controller goroutines):
	//
	//   T1 batch.Get(R)                       → R{State: Validated, Version: 1}
	//   T2 cancel.Get(R)                      → R{State: Validated, Version: 1}
	//   T3 cancel.markCancelling CAS 1→2      → R{State: Cancelling, Version: 2}
	//   T4 cancel.findActiveBatch(R)          → none (batch has not been Created yet)
	//   T5 cancel.cancelRequest CAS 2→3       → R{State: Cancelled,  Version: 3}
	//   T6 batch.IsRequestStateHalted(R)      → false (stale in-memory copy from T1)
	//   T7 batch.BatchStore.Create(B{[R]})    → orphan batch containing a cancelled R
	//
	// After T7 the orphan batch flows through score → speculate → merge → conclude;
	// conclude does NOT gate on the source request state when writing the terminal
	// state, so it would CAS the request from Cancelled back to Landed, silently
	// undoing the user's cancel.
	//
	// The CAS below collapses that window. Whichever of batch.UpdateState(...,
	// RequestStateBatched) and cancel.markCancelling(... RequestStateCancelling)
	// reaches storage first wins; the loser sees storage.ErrVersionMismatch:
	//   - If cancel won: this CAS fails. We ack the message (cancel will drive R
	//     to its terminal state on its own; no batch is needed). The reverse-index
	//     entry above becomes a dangling BatchDependent — tolerated per the
	//     "downstream should handle stale entries" contract on this store.
	//   - If batch won: cancel.markCancelling will fail with ErrVersionMismatch
	//     on its next attempt, re-fetch R, observe RequestStateBatched, and take
	//     the batch-cancellation branch (which terminates the whole batch).
	//
	// Note on re-delivery: a retry of a batch message that already CAS'd R to
	// Batched but failed before/after BatchStore.Create lands in this code with
	// R already in RequestStateBatched. The top-level IsRequestStateHalted check
	// does NOT include Batched (Batched is forward-progress, not halted), so we
	// reach here and re-CAS Batched → Batched (a version-only bump). The bump
	// keeps the same serialization invariant on every attempt — if cancel sneaks
	// in between our Get and this CAS, our version is stale and we abandon, just
	// like the first-delivery case. The cost is an extra batch (the previous
	// attempt may have already created one) which is tolerated per the comment
	// on BatchStore.Create below.
	//
	// Residual window: a thin race remains between this CAS and BatchStore.Create.
	// During that window cancel.findActiveBatch can still observe R in Batched
	// with no batch yet persisted, and take the request-only cancel path — which
	// then leaves R in Cancelled and the batch we are about to create orphaned.
	// Fully closing this requires cancel-side wait/retry when its pre-CAS
	// observation was RequestStateBatched; deferred to a follow-up since the
	// window is narrow (one storage round-trip) and the user-visible outcome
	// (request cancelled) is still correct — the orphan batch just gets
	// reconciled by conclude as if it had no requests to act on.
	newRequestVersion := request.Version + 1
	if err := c.store.GetRequestStore().UpdateState(ctx, request.ID, request.Version, newRequestVersion, entity.RequestStateBatched); err != nil {
		// ErrVersionMismatch == cancel (or another writer) advanced R first. Ack
		// the message: there is nothing for us to do, and retrying would not help
		// since the new state of R is now visible to the cancel pipeline.
		if errors.Is(err, storage.ErrVersionMismatch) {
			c.metricsScope.Counter("request_claim_lost_race").Inc(1)
			c.logger.Infow("abandoning batch creation; request advanced concurrently (likely cancel)",
				"request_id", request.ID,
				"request_version", request.Version,
				"unused_batch_id", batch.ID,
			)
			return nil
		}
		c.metricsScope.Counter("request_claim_errors").Inc(1)
		return fmt.Errorf("failed to claim request %s for batch %s: %w", request.ID, batch.ID, err)
	}
	request.Version = newRequestVersion
	request.State = entity.RequestStateBatched

	// Persist batch to storage.
	// This is the final operation that concludes the batch creation process. If it fails, BatchDependents will be pointing to a batch id that does not exist.
	// We do not reuse batch ids, a retry of this operation will create a new batch with a new ID. The downstream logic that operates on BatchDependent should be able to handle stale entries.
	if err := c.store.GetBatchStore().Create(ctx, batch); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "batch_store_errors", 1)
		return fmt.Errorf("failed to create batch in batch store: %w", err)
	}

	c.logger.Infow("batch created",
		"batch_id", batch.ID,
		"request_id", request.ID,
		"queue", request.Queue,
		"dependency_count", len(batch.Dependencies),
	)

	// Record the "batched" status in the request log. This status corresponds to
	// the RequestStateBatched transition CAS'd above, so it carries the request
	// version for reconciliation (unlike the batch-level "scored" status). The
	// message ID is scoped to (requestID, status), so a redelivery that creates a
	// fresh batch re-emits "batched" with a different batch_id but is deduped to
	// the first entry — acceptable, the request is batched either way.
	logEntry := entity.NewRequestLog(request.ID, entity.RequestStatusBatched, request.Version, "", map[string]string{
		"batch_id": batch.ID,
	})
	if err := corerequest.PublishLog(ctx, c.registry, logEntry, request.ID); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "request_log_errors", 1)
		return fmt.Errorf("failed to publish request log for request %s: %w", request.ID, err)
	}

	// Publish to score topic for further processing.
	// If it fails and the controller retries, a new batch will be created with the new batch ID but the same request ID.
	// The downstream logic should be able to handle stale entries by looking at the state of the batch.
	if err := c.publish(ctx, topickey.TopicKeyScore, batch.ID, batch.Queue); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish batch ID to score topic: %w", err)
	}

	c.logger.Infow("published batch to score topic",
		"batch_id", batch.ID,
		"topic_key", topickey.TopicKeyScore,
	)

	return nil // Success - message will be acked
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
	return "batch"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
