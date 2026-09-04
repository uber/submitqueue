// Copyright (c) 2026 Uber Technologies, Inc.
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

// Package dependencyanalysis decides which batch carries a request, resolves
// what that batch must serialize behind, and promotes it from Creating to
// Created.
//
// # Why this is its own stage
//
// Created is dependency-eligible: the next batch's analysis will pick it up and
// serialize behind it. A batch may therefore only reach Created if it is certain
// to be admitted afterwards, or it becomes a permanent dependency nothing can
// resolve and the queue wedges behind it.
//
// Everything that makes a batch real happens here, in one stage: the enrolment
// decision, the association, the request claim, the dependency set, and the
// promotion. The batch stage upstream only mints an ID and hands it over, so a
// redelivery there costs an unreachable Creating row and nothing else — which is
// what lets this stage be the single place that decides.
//
// # Partitioning
//
// Messages must be partitioned by queue. Analysis reads the queue's
// dependency-eligible batches, so two batches of one queue analyzed concurrently
// would each see the other still in Creating, neither would serialize behind the
// other, and both would speculate as though the other did not exist. Serial
// consumption is also what makes the enrolment check safe: it reads, decides and
// writes without another batch of the same queue interleaving.
//
// # Idempotency
//
// Redelivery is expected and every step tolerates it. A batch already past
// Creating skips analysis entirely and only re-announces. Within analysis, the
// reverse-index and association writes are individually idempotent, because a
// failure part-way through leaves the state at Creating and the retry re-enters
// here.
package dependencyanalysis

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/platform/publish"
	corebatch "github.com/uber/submitqueue/submitqueue/core/batch"
	corerequest "github.com/uber/submitqueue/submitqueue/core/request"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/conflict"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// Controller handles dependency-analysis queue messages.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	registry      consumer.TopicRegistry
	stores        storage.Factory
	analyzers     conflict.Factory
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

const opName = "process"

// NewController creates a new dependency-analysis controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores storage.Factory,
	analyzers conflict.Factory,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("dependency_analysis_controller"),
		metricsScope:  scope.SubScope("dependency_analysis_controller"),
		registry:      registry,
		stores:        stores,
		analyzers:     analyzers,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process enrols a batch's requests, resolves its dependencies, promotes it to
// Created, and hands it to speculate.
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

	c.logger.Infow("received dependency-analysis event",
		"batch_id", batch.ID,
		"queue", batch.Queue,
		"state", string(batch.State),
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	switch {
	case entity.IsBatchStateHalted(batch.State):
		// Cancelled or concluded while the analysis message was in flight.
		// Promoting it now would hand speculate a batch nobody expects to land.
		metrics.NamedCounter(c.metricsScope, opName, "skipped_halted", 1)
		return nil

	case batch.State == entity.BatchStateCreating:
		enrolled, err := c.requestEnrolledInAnotherBatch(ctx, store, batch)
		if err != nil {
			return err
		}
		if enrolled {
			// A previous batch message for this request was redelivered and minted
			// this batch too. The first one through here owns the request; this one
			// stays Creating, where nothing can reach it.
			metrics.NamedCounter(c.metricsScope, opName, "skipped_already_enrolled", 1)
			return nil
		}

		halted, err := c.firstHaltedRequestID(ctx, store, batch)
		if err != nil {
			return err
		}
		if halted != "" {
			// Nothing has claimed the request yet, so cancel owned it outright and
			// has already written its outcome.
			metrics.NamedCounter(c.metricsScope, opName, "skipped_halted_request", 1)
			c.logger.Infow("abandoning batch; contained request is halted",
				"batch_id", batch.ID,
				"request_id", halted,
			)
			return nil
		}

		dependencies, err := c.resolveDependencies(ctx, store, batch)
		if err != nil {
			return err
		}
		if err := c.writeDependentIndexes(ctx, store, batch, dependencies); err != nil {
			return err
		}
		if err := c.associateRequestsWithBatch(ctx, store, batch); err != nil {
			return err
		}
		claimed, err := c.claimRequestsForBatch(ctx, store, batch)
		if err != nil {
			return err
		}
		if !claimed {
			// Cancel reached the request first and owns its outcome. Leave the
			// batch in Creating; promoting it would hand speculate a batch built
			// on a request that is on its way out.
			return nil
		}

		// Transition writes the whole batch, so the dependency set and the state
		// land in one compare-and-swap.
		batch.Dependencies = dependencies
		batch, err = corebatch.Transition(ctx, store, batch, entity.BatchStateCreated)
		if err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "batch_store_errors", 1)
			return fmt.Errorf("failed to mark batch %s created: %w", batch.ID, err)
		}
		c.logger.Infow("batch created",
			"batch_id", batch.ID,
			"queue", batch.Queue,
			"dependency_count", len(batch.Dependencies),
		)

	case batch.State == entity.BatchStateCreated:
		// A prior attempt transitioned but lost its announcement, so
		// re-announcing is the whole job.
		metrics.NamedCounter(c.metricsScope, opName, "reannounced", 1)

	default:
		// Speculating or landing: the announcement landed and the batch has
		// already moved past this stage.
		metrics.NamedCounter(c.metricsScope, opName, "already_admitted", 1)
		return nil
	}

	// Reported here rather than at batch creation: until the transition above
	// lands, the batch has no dependency set and nothing in the queue can see
	// it, so "batched" would promise more than had happened. Deduped per
	// (request, status), so the re-announce path re-publishes harmlessly.
	if err := corerequest.PublishBatchLogs(ctx, c.registry, batch.Queue, batch.Contains,
		entity.RequestStatusBatched, "", map[string]string{"batch_id": batch.ID},
	); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "request_log_errors", 1)
		return fmt.Errorf("failed to publish request logs for batch %s: %w", batch.ID, err)
	}

	return c.publishToSpeculate(ctx, batch)
}

// requestEnrolledInAnotherBatch reports whether one of the batch's requests is already
// carried by a different batch that got past Creating.
//
// The batch stage mints a batch per delivery, so a lost ack leaves two, each
// with its own hand-off. This is where that is resolved: the topic is
// partitioned by queue and consumed in order, so the first hand-off through
// here enrols the request and the second finds it and stops. Without the check
// the same change would end up in two live batches, both admitted, both landed.
func (c *Controller) requestEnrolledInAnotherBatch(ctx context.Context, store storage.Storage, batch entity.Batch) (bool, error) {
	for _, requestID := range batch.Contains {
		existing, stale, err := corebatch.FindByRequestID(ctx, store, requestID)
		if err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "batch_lookup_errors", 1)
			return false, err
		}
		if stale > 0 {
			metrics.NamedCounter(c.metricsScope, opName, "stale_batch_associations", int64(stale))
		}
		for _, other := range existing {
			if other.ID == batch.ID || other.State == entity.BatchStateCreating {
				continue
			}
			c.logger.Infow("abandoning batch; request is already carried by another",
				"batch_id", batch.ID,
				"request_id", requestID,
				"enrolled_in", other.ID,
				"enrolled_state", string(other.State),
			)
			return true, nil
		}
	}
	return false, nil
}

// associateRequestsWithBatch links the batch to its requests. This is the record that makes the
// batch findable from a request, so writing it is what enrols them.
func (c *Controller) associateRequestsWithBatch(ctx context.Context, store storage.Storage, batch entity.Batch) error {
	for _, requestID := range batch.Contains {
		association := entity.RequestBatch{RequestID: requestID, BatchID: batch.ID, Version: 1}
		if err := store.GetRequestBatchStore().Create(ctx, association); err != nil && !errors.Is(err, storage.ErrAlreadyExists) {
			metrics.NamedCounter(c.metricsScope, opName, "request_batch_store_errors", 1)
			return fmt.Errorf("failed to associate request %s with batch %s: %w", requestID, batch.ID, err)
		}
	}
	return nil
}

// claimRequestsForBatch CASes each of the batch's requests to RequestStateBatched, which is
// what enrols them. It reports whether every request was claimed; false means
// cancel reached one of them first and owns its outcome, so the batch must be
// abandoned in Creating rather than promoted.
//
// Two guards make the claim the serialization point against cancel, and both
// are load-bearing. The state check rejects a cancellation that completed
// before this read: the read is taken here rather than at the top of the stage,
// so it carries a fresh version, and a version guard alone would compare equal
// and write Batched straight over Cancelled. The version guard then rejects a
// cancellation landing in the remaining window between this read and the write.
//
// A redelivery re-applies Batched → Batched as a version-only bump, which keeps
// both guards in force on every attempt.
func (c *Controller) claimRequestsForBatch(ctx context.Context, store storage.Storage, batch entity.Batch) (bool, error) {
	for _, requestID := range batch.Contains {
		request, err := store.GetRequestStore().Get(ctx, requestID)
		if err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
			return false, fmt.Errorf("failed to get request %s of batch %s: %w", requestID, batch.ID, err)
		}

		if entity.IsRequestStateHalted(request.State) {
			metrics.NamedCounter(c.metricsScope, opName, "request_claim_lost_race", 1)
			c.logger.Infow("abandoning batch; request was halted during analysis",
				"batch_id", batch.ID,
				"request_id", requestID,
				"request_state", string(request.State),
			)
			return false, nil
		}

		newVersion := request.Version + 1
		claimed := request
		claimed.State = entity.RequestStateBatched
		if err := store.GetRequestStore().Update(ctx, claimed, request.Version, newVersion); err != nil {
			if errors.Is(err, storage.ErrVersionMismatch) {
				metrics.NamedCounter(c.metricsScope, opName, "request_claim_lost_race", 1)
				c.logger.Infow("abandoning batch; request advanced concurrently (likely cancel)",
					"batch_id", batch.ID,
					"request_id", requestID,
				)
				return false, nil
			}
			metrics.NamedCounter(c.metricsScope, opName, "request_claim_errors", 1)
			return false, fmt.Errorf("failed to claim request %s for batch %s: %w", requestID, batch.ID, err)
		}
	}
	return true, nil
}

// firstHaltedRequestID returns the first request in the batch the user has given up
// on, or the empty string if every member is still live.
func (c *Controller) firstHaltedRequestID(ctx context.Context, store storage.Storage, batch entity.Batch) (string, error) {
	for _, requestID := range batch.Contains {
		request, err := store.GetRequestStore().Get(ctx, requestID)
		if err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
			return "", fmt.Errorf("failed to get request %s of batch %s: %w", requestID, batch.ID, err)
		}
		if entity.IsRequestStateHalted(request.State) {
			return requestID, nil
		}
	}
	return "", nil
}

// resolveDependencies asks the queue's conflict analyzer which in-flight batches the new
// batch must serialize behind. The read goes through the queue's per-state
// membership records; classification uses each batch's own hydrated state, so
// a stale record can never misreport a batch.
func (c *Controller) resolveDependencies(ctx context.Context, store storage.Storage, batch entity.Batch) ([]string, error) {
	inFlight, err := corebatch.ListByStates(ctx, store, entity.DependencyBatchStates())
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "batch_store_errors", 1)
		return nil, fmt.Errorf("failed to get active batches for queue=%s: %w", batch.Queue, err)
	}

	analyzer, err := c.analyzers.For(conflict.Config{QueueName: batch.Queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "conflict_analyzer_errors", 1)
		return nil, fmt.Errorf("failed to build conflict analyzer for queue=%s: %w", batch.Queue, err)
	}
	conflicts, err := analyzer.Analyze(ctx, batch, inFlight)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "conflict_analyzer_errors", 1)
		return nil, fmt.Errorf("failed to analyze conflicts for batchID=%s: %w", batch.ID, err)
	}

	// Dedupe by batch ID since a single (analyzed, in-flight) pair may be
	// reported with multiple Conflict entries when different conflict types
	// apply; the dependency graph only tracks the relation.
	seen := make(map[string]struct{}, len(conflicts))
	dependencies := make([]string, 0, len(conflicts))
	for _, cf := range conflicts {
		if _, ok := seen[cf.BatchID]; ok {
			continue
		}
		seen[cf.BatchID] = struct{}{}
		dependencies = append(dependencies, cf.BatchID)
	}
	return dependencies, nil
}

// writeDependentIndexes creates the batch's own reverse-index row and lists it as a dependent
// of everything it depends on.
//
// Both writes are idempotent on their own: a failure part-way through the loop
// leaves the batch in Creating, so the retry re-enters here and would
// otherwise duplicate whatever the first pass already wrote.
func (c *Controller) writeDependentIndexes(ctx context.Context, store storage.Storage, batch entity.Batch, dependencies []string) error {
	own := entity.BatchDependent{
		BatchID:    batch.ID,
		Dependents: []string{},
		Version:    1,
	}
	if err := store.GetBatchDependentStore().Create(ctx, own); err != nil && !errors.Is(err, storage.ErrAlreadyExists) {
		metrics.NamedCounter(c.metricsScope, opName, "batch_dependent_store_errors", 1)
		return fmt.Errorf("failed to create batch dependent index for new batchID=%s: %w", batch.ID, err)
	}

	for _, dependencyID := range dependencies {
		existing, err := store.GetBatchDependentStore().Get(ctx, dependencyID)
		if err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "batch_dependent_store_errors", 1)
			return fmt.Errorf("failed to get batch dependent for batchID=%s: %w", dependencyID, err)
		}
		if slices.Contains(existing.Dependents, batch.ID) {
			continue
		}

		updated := existing
		updated.Dependents = append(append([]string(nil), existing.Dependents...), batch.ID)
		newVersion := existing.Version + 1
		if err := store.GetBatchDependentStore().Update(ctx, updated, existing.Version, newVersion); err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "batch_dependent_store_errors", 1)
			return fmt.Errorf("failed to update batch dependent index for existing batchID=%s and new batchID=%s: %w", dependencyID, batch.ID, err)
		}
	}
	return nil
}

// publishToSpeculate hands the batch to the speculate stage.
//
// The message ID is the bare batch ID, with no cause: a batch announces its own
// creation once in its life, so a redelivery that re-announces it is meant to be
// dropped. Every later publish about the same batch names its cause and so
// cannot collide with this row — see publish.IntentID.
func (c *Controller) publishToSpeculate(ctx context.Context, batch entity.Batch) error {
	payload, err := entity.BatchID{ID: batch.ID, Queue: batch.Queue}.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize batch ID: %w", err)
	}

	if err := publish.Message(ctx, c.registry, topickey.TopicKeySpeculate, publish.IntentID(batch.ID), payload, batch.Queue); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish batch ID to speculate topic: %w", err)
	}

	c.logger.Infow("published batch to speculate topic",
		"batch_id", batch.ID,
		"topic_key", topickey.TopicKeySpeculate,
	)
	return nil
}

// Name returns the controller name for logging and metrics.
func (c *Controller) Name() string {
	return "dependency-analysis"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
