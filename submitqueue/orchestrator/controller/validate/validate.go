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

package validate

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/errs"
	coremetrics "github.com/uber/submitqueue/core/metrics"
	entityqueue "github.com/uber/submitqueue/entity/messagequeue"
	"github.com/uber/submitqueue/submitqueue/core/consumer"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/changeprovider"
	"github.com/uber/submitqueue/submitqueue/extension/mergechecker"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// Controller handles validate queue messages.
// It consumes requests, performs validation checks (duplicate detection via the change store,
// merge conflicts, change metadata fetch), and publishes to the batch stage. Validation logic
// is extensible to support additional checks. Implements consumer.Controller.
type Controller struct {
	logger          *zap.SugaredLogger
	metricsScope    tally.Scope
	store           storage.Storage
	registry        consumer.TopicRegistry
	mergeCheckers   mergechecker.Factory
	changeProviders changeprovider.Factory
	topicKey        consumer.TopicKey
	consumerGroup   string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// NewController creates a new validate controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	store storage.Storage,
	registry consumer.TopicRegistry,
	mergeCheckers mergechecker.Factory,
	changeProviders changeprovider.Factory,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:          logger.Named("validate_controller"),
		metricsScope:    scope.SubScope("validate_controller"),
		store:           store,
		registry:        registry,
		mergeCheckers:   mergeCheckers,
		changeProviders: changeProviders,
		topicKey:        topicKey,
		consumerGroup:   consumerGroup,
	}
}

// Process processes a validate delivery from the queue.
// Runs duplicate detection, merge-conflict check, change metadata fetch, then publishes to batch.
// Returns nil to ack (success or non-retryable rejection), error to nack (retry).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	op := coremetrics.Begin(c.metricsScope, "process")
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()

	// Deserialize request ID from payload
	rid, err := entity.RequestIDFromBytes(msg.Payload)
	if err != nil {
		coremetrics.NamedCounter(c.metricsScope, "process", "deserialize_errors", 1)
		return fmt.Errorf("failed to deserialize request ID: %w", err)
	}

	// Fetch request from storage
	request, err := c.store.GetRequestStore().Get(ctx, rid.ID)
	if err != nil {
		coremetrics.NamedCounter(c.metricsScope, "process", "storage_errors", 1)
		return fmt.Errorf("failed to get request %s: %w", rid.ID, err)
	}

	c.logger.Infow("received validate event",
		"request_id", request.ID,
		"queue", request.Queue,
		"state", string(request.State),
		"version", request.Version,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	// Short-circuit if the request has been halted — either it already reached a
	// terminal state, or the cancel controller has recorded a cancellation intent
	// (RequestStateCancelling). Without this guard we would still publish to batch
	// and spawn a batch for a request that should never proceed.
	if entity.IsRequestStateHalted(request.State) {
		coremetrics.NamedCounter(c.metricsScope, "process", "skipped_halted", 1)
		c.logger.Infow("skipping validate for halted request",
			"request_id", request.ID,
			"state", string(request.State),
		)
		return nil
	}

	// Duplicate detection: look for any other in-flight request that has already
	// claimed an overlapping URI in this queue. Per-queue partition leasing
	// (see core/consumer + extension/messagequeue) guarantees serial processing within
	// a queue, so the read-then-claim sequence below is race-free.
	if dupID, err := c.checkDuplicate(ctx, request); err != nil {
		return err
	} else if dupID != "" {
		c.logger.Infow("duplicate request detected",
			"request_id", request.ID,
			"queue", request.Queue,
			"duplicate_id", dupID,
		)
		coremetrics.NamedCounter(c.metricsScope, "process", "duplicate_requests", 1)
		return errs.NewUserError(fmt.Errorf("request %s is a duplicate of in-flight request %s", request.ID, dupID))
	}

	// Merge conflict check
	mergeChecker, err := c.mergeCheckers.For(mergechecker.Config{QueueName: request.Queue})
	if err != nil {
		coremetrics.NamedCounter(c.metricsScope, "process", "merge_check_errors", 1)
		return fmt.Errorf("failed to build merge checker for queue %s: %w", request.Queue, err)
	}
	mergeResult, err := mergeChecker.Check(ctx, request.Change)
	if err != nil {
		coremetrics.NamedCounter(c.metricsScope, "process", "merge_check_errors", 1)
		return fmt.Errorf("merge check failed: %w", err)
	}
	if !mergeResult.Mergeable {
		c.logger.Infow("request not mergeable",
			"request_id", request.ID,
			"queue", request.Queue,
			"reason", mergeResult.Reason,
		)
		coremetrics.NamedCounter(c.metricsScope, "process", "not_mergeable", 1)
		return errs.NewUserError(fmt.Errorf("request %s is not mergeable: %s", request.ID, mergeResult.Reason))
	}

	// Fetch change metadata
	changeProvider, err := c.changeProviders.For(changeprovider.Config{QueueName: request.Queue})
	if err != nil {
		coremetrics.NamedCounter(c.metricsScope, "process", "change_provider_errors", 1)
		return fmt.Errorf("failed to build change provider for queue %s: %w", request.Queue, err)
	}
	changeInfos, err := changeProvider.Get(ctx, request.Change)
	if err != nil {
		coremetrics.NamedCounter(c.metricsScope, "process", "change_provider_errors", 1)
		return fmt.Errorf("failed to fetch change information for request %s: %w", request.ID, err)
	}

	c.logger.Infow("fetched change information",
		"request_id", request.ID,
		"change_count", len(changeInfos),
		"total_files", totalFiles(changeInfos),
	)

	// Publish to batch topic
	if err := c.publish(ctx, consumer.TopicKeyBatch, request.ID, request.Queue); err != nil {
		coremetrics.NamedCounter(c.metricsScope, "process", "publish_errors", 1)
		return fmt.Errorf("failed to publish to batch: %w", err)
	}

	c.logger.Infow("published request to batch",
		"request_id", request.ID,
		"topic_key", consumer.TopicKeyBatch,
	)

	return nil // Success - message will be acked
}

// checkDuplicate looks for any other in-flight request whose URIs overlap with this
// request's. The change rows are written upstream by the start controller; validate
// is read-only here. For each URI it queries the change store, walks the returned
// candidates skipping self/duplicates/orphans/terminals, and short-circuits on the
// first live duplicate. Returns that request_id, or "" if none.
//
// Per-URI / per-record reads keep the contract backend-agnostic; the typical request
// has 1-5 URIs, so the loop is cheap.
func (c *Controller) checkDuplicate(ctx context.Context, request entity.Request) (string, error) {
	seenOwners := make(map[string]struct{})
	for _, uri := range request.Change.URIs {
		records, err := c.store.GetChangeStore().GetByURI(ctx, request.Queue, uri)
		if err != nil {
			coremetrics.NamedCounter(c.metricsScope, "process", "change_store_query_errors", 1)
			return "", fmt.Errorf("failed to query change store for request %s uri=%s: %w", request.ID, uri, err)
		}
		for _, rec := range records {
			if rec.RequestID == request.ID {
				continue // skip rows belonging to this request itself
			}
			if _, ok := seenOwners[rec.RequestID]; ok {
				continue
			}
			seenOwners[rec.RequestID] = struct{}{}

			owner, err := c.store.GetRequestStore().Get(ctx, rec.RequestID)
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			if err != nil {
				coremetrics.NamedCounter(c.metricsScope, "process", "storage_errors", 1)
				return "", fmt.Errorf("failed to look up overlapping request %s: %w", rec.RequestID, err)
			}
			if !entity.IsRequestStateTerminal(owner.State) {
				return rec.RequestID, nil
			}
		}
	}
	return "", nil
}

// publish publishes a request ID to the specified topic key.
func (c *Controller) publish(ctx context.Context, key consumer.TopicKey, requestID string, partitionKey string) error {
	rid := entity.RequestID{ID: requestID}
	payload, err := rid.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize request ID: %w", err)
	}

	msg := entityqueue.NewMessage(requestID, payload, partitionKey, nil)

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

// totalFiles returns the total number of files across all changeInfos.
func totalFiles(infos []changeprovider.ChangeInfo) int {
	total := 0
	for _, info := range infos {
		total += len(info.ChangedFiles)
	}
	return total
}

// Name returns the controller name for logging and metrics.
func (c *Controller) Name() string {
	return "validate"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
