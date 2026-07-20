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

	op := metrics.Begin(c.metricsScope, opName)
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()

	// Deserialize request ID from payload
	rid, err := entity.RequestIDFromBytes(msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		return fmt.Errorf("failed to deserialize request ID: %w", err)
	}

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

	progress, err := classifyRequest(request.State)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "unexpected_state_errors", 1)
		return fmt.Errorf("cannot batch request %s: %w", request.ID, err)
	}
	switch progress {
	case requestDownstreamProgressed:
		metrics.NamedCounter(c.metricsScope, opName, "downstream_progressed", 1)
		return nil
	case requestSuperseded:
		metrics.NamedCounter(c.metricsScope, opName, "skipped_halted", 1)
		return nil
	}

	requestBatch, assignmentExisted, err := c.resolveRequestBatch(ctx, request)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "request_batch_store_errors", 1)
		return err
	}

	if progress == requestNeedsTransition {
		newVersion := request.Version + 1
		if err := c.store.GetRequestStore().UpdateState(ctx, request.ID, request.Version, newVersion, entity.RequestStateBatched); err != nil {
			if errors.Is(err, storage.ErrVersionMismatch) {
				metrics.NamedCounter(c.metricsScope, opName, "request_claim_conflicts", 1)
			}
			metrics.NamedCounter(c.metricsScope, opName, "request_claim_errors", 1)
			return fmt.Errorf("failed to claim request %s for batch %s: %w", request.ID, requestBatch.BatchID, err)
		}
		request.Version = newVersion
		request.State = entity.RequestStateBatched
	}

	batch, err := c.reconcileBatch(ctx, request, requestBatch, assignmentExisted)
	if err != nil {
		return err
	}

	switch batch.State {
	case entity.BatchStateCreated:
		// This controller owns the Created batch fanout. Replay all of it
		// because an earlier delivery may have stopped after any write or
		// publish.
	case entity.BatchStateScored, entity.BatchStateSpeculating, entity.BatchStateMerging:
		metrics.NamedCounter(c.metricsScope, opName, "batch_downstream_progressed", 1)
		return nil
	case entity.BatchStateCancelling, entity.BatchStateSucceeded, entity.BatchStateFailed, entity.BatchStateCancelled:
		metrics.NamedCounter(c.metricsScope, opName, "batch_superseded", 1)
		return nil
	default:
		metrics.NamedCounter(c.metricsScope, opName, "unexpected_batch_state_errors", 1)
		return fmt.Errorf("batch %s has invalid state %q for batch reconciliation", batch.ID, batch.State)
	}

	if err := c.reconcileBatchDependents(ctx, batch); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "batch_dependent_store_errors", 1)
		return err
	}

	logEntry := entity.NewRequestLog(request.ID, entity.RequestStatusBatched, request.Version, "", map[string]string{
		"batch_id": batch.ID,
	})
	if err := corerequest.PublishLog(ctx, c.registry, logEntry, request.ID); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "request_log_errors", 1)
		return fmt.Errorf("failed to publish request log for request %s: %w", request.ID, err)
	}

	if err := c.publish(ctx, topickey.TopicKeyScore, batch.ID, batch.Queue); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish batch ID to score topic: %w", err)
	}

	c.logger.Infow("published batch to score topic",
		"batch_id", batch.ID,
		"topic_key", topickey.TopicKeyScore,
	)
	return nil
}

type requestProgress int

const (
	requestNeedsTransition requestProgress = iota
	requestTransitionApplied
	requestDownstreamProgressed
	requestSuperseded
)

func classifyRequest(state entity.RequestState) (requestProgress, error) {
	switch state {
	case entity.RequestStateStarted, entity.RequestStateValidated:
		return requestNeedsTransition, nil
	case entity.RequestStateBatched:
		return requestTransitionApplied, nil
	case entity.RequestStateProcessing:
		return requestDownstreamProgressed, nil
	case entity.RequestStateCancelling, entity.RequestStateLanded, entity.RequestStateError, entity.RequestStateCancelled:
		return requestSuperseded, nil
	default:
		return requestNeedsTransition, fmt.Errorf("unexpected request state %q", state)
	}
}

// resolveRequestBatch returns the durable assignment for this logical batch
// operation. A new assignment is written before the request CAS so every retry
// can reconstruct the same batch ID.
func (c *Controller) resolveRequestBatch(ctx context.Context, request entity.Request) (entity.RequestBatch, bool, error) {
	store := c.store.GetRequestBatchStore()
	requestBatch, err := store.Get(ctx, request.ID)
	if err == nil {
		if err := validateRequestBatch(request, requestBatch); err != nil {
			return entity.RequestBatch{}, false, err
		}
		return requestBatch, true, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return entity.RequestBatch{}, false, fmt.Errorf("failed to get batch assignment for request %s: %w", request.ID, err)
	}

	// request_batch predates this controller contract. Recover an active legacy
	// batch once rather than assigning a second batch to an already-batched
	// request.
	if request.State == entity.RequestStateBatched {
		batch, found, err := c.findLegacyBatch(ctx, request)
		if err != nil {
			return entity.RequestBatch{}, false, err
		}
		if !found {
			return entity.RequestBatch{}, false, fmt.Errorf("batched request %s has no durable batch assignment or active batch", request.ID)
		}
		requestBatch = entity.RequestBatch{RequestID: request.ID, BatchID: batch.ID, Version: 1}
		if err := store.Create(ctx, requestBatch); err != nil {
			if !errors.Is(err, storage.ErrAlreadyExists) {
				return entity.RequestBatch{}, false, fmt.Errorf("failed to recover batch assignment for request %s: %w", request.ID, err)
			}
			requestBatch, err = store.Get(ctx, request.ID)
			if err != nil {
				return entity.RequestBatch{}, false, fmt.Errorf("failed to reload concurrent batch assignment for request %s: %w", request.ID, err)
			}
			if err := validateRequestBatch(request, requestBatch); err != nil {
				return entity.RequestBatch{}, false, err
			}
		}
		return requestBatch, true, nil
	}

	seq, err := c.counter.Next(ctx, "batch/"+request.Queue)
	if err != nil {
		return entity.RequestBatch{}, false, fmt.Errorf("failed to generate batch ID for queue=%s: %w", request.Queue, err)
	}
	requestBatch = entity.RequestBatch{
		RequestID: request.ID,
		BatchID:   fmt.Sprintf("%s/batch/%d", request.Queue, seq),
		Version:   1,
	}
	if err := store.Create(ctx, requestBatch); err == nil {
		return requestBatch, false, nil
	} else if !errors.Is(err, storage.ErrAlreadyExists) {
		return entity.RequestBatch{}, false, fmt.Errorf("failed to reserve batch assignment for request %s: %w", request.ID, err)
	}

	requestBatch, err = store.Get(ctx, request.ID)
	if err != nil {
		return entity.RequestBatch{}, false, fmt.Errorf("failed to reload concurrent batch assignment for request %s: %w", request.ID, err)
	}
	if err := validateRequestBatch(request, requestBatch); err != nil {
		return entity.RequestBatch{}, false, err
	}
	return requestBatch, true, nil
}

func validateRequestBatch(request entity.Request, requestBatch entity.RequestBatch) error {
	if requestBatch.RequestID != request.ID {
		return fmt.Errorf("batch assignment key mismatch: requested %s, got %s", request.ID, requestBatch.RequestID)
	}
	if requestBatch.BatchID == "" {
		return fmt.Errorf("batch assignment for request %s has an empty batch ID", request.ID)
	}
	return nil
}

func (c *Controller) findLegacyBatch(ctx context.Context, request entity.Request) (entity.Batch, bool, error) {
	batches, err := c.store.GetBatchStore().GetByQueueAndStates(ctx, request.Queue, entity.ActiveBatchStates())
	if err != nil {
		return entity.Batch{}, false, fmt.Errorf("failed to find legacy batch for request %s: %w", request.ID, err)
	}

	var found entity.Batch
	for _, batch := range batches {
		if !contains(batch.Contains, request.ID) {
			continue
		}
		if found.ID != "" {
			return entity.Batch{}, false, fmt.Errorf("request %s belongs to multiple active batches: %s and %s", request.ID, found.ID, batch.ID)
		}
		found = batch
	}
	return found, found.ID != "", nil
}

func (c *Controller) reconcileBatch(ctx context.Context, request entity.Request, requestBatch entity.RequestBatch, assignmentExisted bool) (entity.Batch, error) {
	if assignmentExisted {
		batch, err := c.store.GetBatchStore().Get(ctx, requestBatch.BatchID)
		if err == nil {
			if err := validateBatch(request, requestBatch, batch); err != nil {
				return entity.Batch{}, err
			}
			return batch, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return entity.Batch{}, fmt.Errorf("failed to get assigned batch %s: %w", requestBatch.BatchID, err)
		}
	}

	batch := entity.Batch{
		ID:       requestBatch.BatchID,
		Queue:    request.Queue,
		Contains: []string{request.ID},
		State:    entity.BatchStateCreated,
		Version:  1,
	}

	activeBatches, err := c.store.GetBatchStore().GetByQueueAndStates(ctx, request.Queue, entity.DependencyBatchStates())
	if err != nil {
		return entity.Batch{}, fmt.Errorf("failed to get active batches for queue=%s: %w", request.Queue, err)
	}
	analyzer, err := c.analyzers.For(conflict.Config{QueueName: batch.Queue})
	if err != nil {
		return entity.Batch{}, fmt.Errorf("failed to build conflict analyzer for queue=%s: %w", batch.Queue, err)
	}
	conflicts, err := analyzer.Analyze(ctx, batch, activeBatches)
	if err != nil {
		return entity.Batch{}, fmt.Errorf("failed to analyze conflicts for batchID=%s: %w", batch.ID, err)
	}

	seen := make(map[string]struct{}, len(conflicts))
	for _, conflict := range conflicts {
		if _, ok := seen[conflict.BatchID]; ok {
			continue
		}
		seen[conflict.BatchID] = struct{}{}
		batch.Dependencies = append(batch.Dependencies, conflict.BatchID)
	}

	if err := c.store.GetBatchStore().Create(ctx, batch); err != nil {
		if !errors.Is(err, storage.ErrAlreadyExists) {
			return entity.Batch{}, fmt.Errorf("failed to create batch %s: %w", batch.ID, err)
		}
		batch, err = c.store.GetBatchStore().Get(ctx, batch.ID)
		if err != nil {
			return entity.Batch{}, fmt.Errorf("failed to reload concurrent batch %s: %w", requestBatch.BatchID, err)
		}
		if err := validateBatch(request, requestBatch, batch); err != nil {
			return entity.Batch{}, err
		}
	}

	c.logger.Infow("batch reconciled",
		"batch_id", batch.ID,
		"request_id", request.ID,
		"queue", request.Queue,
		"dependency_count", len(batch.Dependencies),
	)
	return batch, nil
}

func validateBatch(request entity.Request, requestBatch entity.RequestBatch, batch entity.Batch) error {
	if batch.ID != requestBatch.BatchID {
		return fmt.Errorf("assigned batch ID mismatch for request %s: expected %s, got %s", request.ID, requestBatch.BatchID, batch.ID)
	}
	if batch.Queue != request.Queue {
		return fmt.Errorf("assigned batch %s has queue %s, expected %s", batch.ID, batch.Queue, request.Queue)
	}
	if !contains(batch.Contains, request.ID) {
		return fmt.Errorf("assigned batch %s does not contain request %s", batch.ID, request.ID)
	}
	return nil
}

func (c *Controller) reconcileBatchDependents(ctx context.Context, batch entity.Batch) error {
	store := c.store.GetBatchDependentStore()
	own := entity.BatchDependent{
		BatchID:    batch.ID,
		Dependents: []string{},
		Version:    1,
	}
	if err := store.Create(ctx, own); err != nil && !errors.Is(err, storage.ErrAlreadyExists) {
		return fmt.Errorf("failed to create dependent index for batch %s: %w", batch.ID, err)
	}

	for _, dependencyID := range batch.Dependencies {
		if err := c.ensureDependent(ctx, dependencyID, batch.ID); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) ensureDependent(ctx context.Context, dependencyID, dependentID string) error {
	store := c.store.GetBatchDependentStore()
	existing, err := store.Get(ctx, dependencyID)
	if err != nil {
		return fmt.Errorf("failed to get dependent index for batch %s: %w", dependencyID, err)
	}
	if contains(existing.Dependents, dependentID) {
		return nil
	}

	dependents := append(append([]string(nil), existing.Dependents...), dependentID)
	newVersion := existing.Version + 1
	if err := store.UpdateDependents(ctx, dependencyID, existing.Version, newVersion, dependents); err != nil {
		if errors.Is(err, storage.ErrVersionMismatch) {
			c.metricsScope.Counter("dependent_version_conflicts").Inc(1)
		}
		return fmt.Errorf("failed to add dependent %s to batch %s: %w", dependentID, dependencyID, err)
	}
	return nil
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
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
