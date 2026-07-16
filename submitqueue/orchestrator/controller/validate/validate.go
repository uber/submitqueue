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
	"time"

	"github.com/uber-go/tally"
	changepb "github.com/uber/submitqueue/api/base/change/protopb"
	strategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	"github.com/uber/submitqueue/platform/base/mergestrategy"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	coremetrics "github.com/uber/submitqueue/platform/metrics"
	corerequest "github.com/uber/submitqueue/submitqueue/core/request"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/changeprovider"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"github.com/uber/submitqueue/submitqueue/extension/validator"
	"go.uber.org/zap"
)

// Controller handles validate queue messages.
// It consumes requests, performs local validation checks (duplicate detection via the change store
// and change metadata fetch), then kicks off the asynchronous merge-conflict check by publishing the
// full check request to runway's merge-conflict-check queue. Validation logic is extensible to
// support additional checks. Implements consumer.Controller.
type Controller struct {
	logger          *zap.SugaredLogger
	metricsScope    tally.Scope
	store           storage.Storage
	registry        consumer.TopicRegistry
	changeProviders changeprovider.Factory
	validators      validator.Factory
	runwayTopicKey  consumer.TopicKey
	topicKey        consumer.TopicKey
	consumerGroup   string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// NewController creates a new validate controller for the orchestrator.
// runwayTopicKey is the runway-owned topic the merge-conflict check request is
// published to (TopicKeyMergeConflictCheck).
// validators is an optional factory for custom validation checks; pass nil to skip.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	store storage.Storage,
	registry consumer.TopicRegistry,
	changeProviders changeprovider.Factory,
	validators validator.Factory,
	runwayTopicKey consumer.TopicKey,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:          logger.Named("validate_controller"),
		metricsScope:    scope.SubScope("validate_controller"),
		store:           store,
		registry:        registry,
		changeProviders: changeProviders,
		validators:      validators,
		runwayTopicKey:  runwayTopicKey,
		topicKey:        topicKey,
		consumerGroup:   consumerGroup,
	}
}

// Process processes a validate delivery from the queue.
// Runs duplicate detection, change metadata fetch, and change claiming, then kicks off the
// asynchronous merge-conflict check by publishing the full check request to runway.
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
	// (see platform/consumer + platform/extension/messagequeue) guarantees serial processing within
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

	// Fetch change metadata
	changeProvider, err := c.changeProviders.For(changeprovider.Config{QueueName: request.Queue})
	if err != nil {
		coremetrics.NamedCounter(c.metricsScope, "process", "change_provider_errors", 1)
		return fmt.Errorf("failed to build change provider for queue %s: %w", request.Queue, err)
	}
	changeInfos, err := changeProvider.Get(ctx, request)
	if err != nil {
		coremetrics.NamedCounter(c.metricsScope, "process", "change_provider_errors", 1)
		return fmt.Errorf("failed to fetch change information for request %s: %w", request.ID, err)
	}

	c.logger.Infow("fetched change information",
		"request_id", request.ID,
		"change_count", len(changeInfos),
		"total_files", totalFiles(changeInfos),
	)

	// Run custom validation checks if a validator factory was provided.
	if c.validators != nil {
		cfg := validator.Config{
			QueueName: request.Queue,
		}
		v, err := c.validators.For(cfg)
		if err != nil {
			coremetrics.NamedCounter(c.metricsScope, "process", "validator_errors", 1)
			return fmt.Errorf("failed to build validator for request %s: %w", request.ID, err)
		}
		if v != nil {
			if err := v.Validate(ctx, request); err != nil {
				coremetrics.NamedCounter(c.metricsScope, "process", "custom_validation_failures", 1)
				return fmt.Errorf("custom validation failed for request %s: %w", request.ID, err)
			}
		}
	}

	// Claim each URI in the change store with its provider details. The claim is
	// created here — after duplicate detection and the merge/provider checks — so a
	// rejected request never leaves a claim, and the record is written once with its
	// details (immutable thereafter; no separate enrichment update). Create is
	// idempotent per (queue, uri, request_id), so redelivery is a no-op.
	if err := c.claimChanges(ctx, request, changeInfos); err != nil {
		coremetrics.NamedCounter(c.metricsScope, "process", "change_store_errors", 1)
		return fmt.Errorf("failed to claim change records for request %s: %w", request.ID, err)
	}

	logEntry := entity.NewRequestLog(request.ID, entity.RequestStatusValidating, request.Version, "", map[string]string{
		"controller": "validate",
	})
	if err := corerequest.PublishLog(ctx, c.registry, logEntry, request.ID); err != nil {
		coremetrics.NamedCounter(c.metricsScope, "process", "log_publish_errors", 1)
		return fmt.Errorf("failed to publish validating request log for %s: %w", request.ID, err)
	}

	// Kick off the asynchronous merge-conflict check: hand the full check request
	// to runway via its merge-conflict-check queue, keyed by the request id (the
	// client-owned correlation id) so a redelivery republishes the same id and the
	// result correlates straight back. At validate time the check is a single step
	// (candidate vs target branch).
	req := &runwaymq.MergeRequest{
		Id:        request.ID,
		QueueName: request.Queue,
		Steps: []*runwaymq.MergeStep{
			{
				StepId:   request.ID,
				Changes:  []*changepb.Change{{Uris: request.Change.URIs}},
				Strategy: toProtoStrategy(request.LandStrategy),
			},
		},
	}
	if err := c.publishMergeCheck(ctx, req); err != nil {
		coremetrics.NamedCounter(c.metricsScope, "process", "publish_errors", 1)
		return fmt.Errorf("failed to publish to runway merge-conflict-check: %w", err)
	}

	c.logger.Infow("published merge conflict check to runway",
		"request_id", request.ID,
		"topic_key", c.runwayTopicKey,
	)

	return nil // Success - message will be acked
}

// checkDuplicate looks for any other in-flight request whose URIs overlap with this
// request's. It reads the change store before this request claims its own URIs
// (claimChanges runs later in Process), so it only sees rows written by other
// requests. For each URI it queries the change store, walks the returned candidates
// skipping self/duplicates/orphans/terminals, and short-circuits on the first live
// duplicate. Returns that request_id, or "" if none. Per-queue partition leasing
// serializes validate within a queue, so this read-then-claim sequence is race-free.
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

// publishMergeCheck serializes the runway check request and publishes it to the
// runway merge-conflict-check topic, partitioned by queue.
func (c *Controller) publishMergeCheck(ctx context.Context, req *runwaymq.MergeRequest) error {
	payload, err := runwaymq.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to serialize merge conflict check request: %w", err)
	}

	msg := entityqueue.NewMessage(req.Id, payload, req.QueueName, nil)

	q, ok := c.registry.Queue(c.runwayTopicKey)
	if !ok {
		return fmt.Errorf("no queue registered for topic key %s", c.runwayTopicKey)
	}

	topicName, ok := c.registry.TopicName(c.runwayTopicKey)
	if !ok {
		return fmt.Errorf("no topic name registered for topic key %s", c.runwayTopicKey)
	}

	if err := q.Publisher().Publish(ctx, topicName, msg); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

// toProtoStrategy maps the shared mergestrategy.MergeStrategy entity to the proto
// Strategy enum carried on the wire. An unknown strategy maps to DEFAULT, letting
// runway apply the queue's configured default.
func toProtoStrategy(s mergestrategy.MergeStrategy) strategypb.Strategy {
	switch s {
	case mergestrategy.MergeStrategyRebase:
		return strategypb.Strategy_REBASE
	case mergestrategy.MergeStrategySquashRebase:
		return strategypb.Strategy_SQUASH_REBASE
	case mergestrategy.MergeStrategyMerge:
		return strategypb.Strategy_MERGE
	default:
		return strategypb.Strategy_DEFAULT
	}
}

// claimChanges persists one ChangeRecord per fetched ChangeInfo, capturing the
// provider details at claim time. The record's identity (queue, uri, request_id)
// and its Details are written together in a single immutable Create — there is no
// later mutation. Create is idempotent on its primary key, so a redelivery (or a
// prior partial attempt) is a no-op and the first write wins.
func (c *Controller) claimChanges(ctx context.Context, request entity.Request, infos []entity.ChangeInfo) error {
	now := time.Now().UnixMilli()
	for _, info := range infos {
		record := entity.ChangeRecord{
			URI:       info.URI,
			RequestID: request.ID,
			Queue:     request.Queue,
			Details:   info.Details,
			CreatedAt: now,
			UpdatedAt: now,
			Version:   1,
		}
		if err := c.store.GetChangeStore().Create(ctx, record); err != nil {
			return fmt.Errorf("failed to claim uri=%s for request %s: %w", info.URI, request.ID, err)
		}
	}
	return nil
}

// totalFiles returns the total number of files across all changeInfos.
func totalFiles(infos []entity.ChangeInfo) int {
	total := 0
	for _, info := range infos {
		total += info.Details.FileCount()
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
