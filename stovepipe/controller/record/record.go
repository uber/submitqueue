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

// Package record holds the record-stage queue controller. It consumes Record
// messages (a request id) published by buildsignal once a build reaches a
// terminal status, and turns that outcome into durable validation state. See
// doc/rfc/stovepipe/steps/record.md.
//
// Phase 1 records only the queue's last-green bookmark, which process reads to
// choose an incremental build baseline. Validation facts and downstream hooks
// are not implemented yet.
package record

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/core/loader"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

// Controller consumes Record messages and advances the queue's last-green
// bookmark when the request's build succeeded. Implements consumer.Controller.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	stores        storage.Factory
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// _opName is the metric operation name shared by every emit in this file.
const _opName = "record"

// NewController creates a new record controller.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores storage.Factory,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("record_controller"),
		metricsScope:  scope.SubScope("record_controller"),
		stores:        stores,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process loads the request referenced by the delivery and, when its build
// succeeded, advances the queue's last-green bookmark. Returns nil to ack
// (success) or an error to nack (retry) / reject (DLQ).
//
// buildsignal stamps the outcome on the request before publishing here, so a
// request without a build outcome is a producer invariant violation rather
// than a state this stage waits for.
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	msg := delivery.Message()

	rec := &stovepipemq.Record{}
	if err := stovepipemq.Unmarshal(msg.Payload, rec); err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "deserialize_errors", 1)
		// Non-retryable: a malformed message will never succeed regardless of retries.
		return fmt.Errorf("failed to deserialize record: %w", err)
	}

	store, err := c.stores.For(storage.Config{QueueName: rec.GetQueueName()})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "storage_resolve_errors", 1)
		// Non-retryable: a missing or unresolvable queue is a malformed message.
		return fmt.Errorf("failed to resolve storage for queue %q: %w", rec.GetQueueName(), err)
	}

	request, err := c.loadRequest(ctx, store, rec.Id)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "storage_errors", 1)
		return err
	}

	// The payload's queue must match the request's authoritative queue; a
	// mismatch is a malformed message. Non-retryable — reject to the DLQ.
	if rec.GetQueueName() != "" && rec.GetQueueName() != request.Queue {
		metrics.NamedCounter(c.metricsScope, _opName, "queue_mismatch", 1)
		return fmt.Errorf("payload queue %q does not match queue %q of request %s", rec.GetQueueName(), request.Queue, request.ID)
	}

	switch request.State {
	case entity.RequestStateSucceeded:
		if err := c.advanceLastGreen(ctx, store, request); err != nil {
			metrics.NamedCounter(c.metricsScope, _opName, "storage_errors", 1)
			return err
		}
		return nil

	case entity.RequestStateFailed, entity.RequestStateCancelled:
		// A verdict, but not a green one: nothing to record in phase 1. A
		// cancelled build decided nothing about the commit at all.
		metrics.NamedCounter(c.metricsScope, _opName, "not_green", 1)
		return nil

	case entity.RequestStateSuperseded:
		// Terminal without a build outcome. buildsignal never publishes for a
		// superseded request, so this is unreachable in practice.
		metrics.NamedCounter(c.metricsScope, _opName, "superseded", 1)
		return nil

	default:
		// Non-retryable: buildsignal publishes only after committing the
		// outcome, so a non-terminal request here is a broken invariant that
		// retrying cannot fix.
		metrics.NamedCounter(c.metricsScope, _opName, "invariant_errors", 1)
		return fmt.Errorf("request %s reached record in non-terminal state %q", request.ID, request.State)
	}
}

// advanceLastGreen points the queue's bookmark at request, retrying on version
// conflicts. The bookmark only moves forward: a candidate whose id is not newer
// than the stored one is skipped without a write, which also makes a redelivery
// of the same request a no-op.
//
// Greenness comes from request.State rather than a persisted validation fact,
// which is what record.md specifies. The two agree — a fact's degree is derived
// from the same immutable state — and this reads the fact once the fact store
// lands.
func (c *Controller) advanceLastGreen(ctx context.Context, store storage.Storage, request entity.Request) error {
	queueStore := store.GetQueueStore()

	for {
		queueRow, err := queueStore.Get(ctx, request.Queue)
		if err != nil {
			return fmt.Errorf("failed to load queue %s to advance last green: %w", request.Queue, err)
		}

		newer, err := isNewerRequest(request.Queue, request.ID, queueRow.LastGreenRequestID)
		if err != nil {
			// Non-retryable: re-parsing the same ids cannot start succeeding.
			return err
		}
		if !newer {
			return nil
		}

		updated := queueRow
		updated.LastGreenURI = request.URI
		updated.LastGreenRequestID = request.ID
		newVersion := queueRow.Version + 1
		if err := queueStore.Update(ctx, updated, queueRow.Version, newVersion); err != nil {
			if errors.Is(err, storage.ErrVersionMismatch) {
				continue
			}
			return fmt.Errorf("failed to advance last green for queue %s: %w", request.Queue, err)
		}

		metrics.NamedCounter(c.metricsScope, _opName, "last_green_advanced", 1)
		c.logger.Infow("advanced last green bookmark",
			"queue", request.Queue,
			"request_id", request.ID,
			"last_green_uri", request.URI,
		)
		return nil
	}
}

// isNewerRequest reports whether candidate was ingested after current. An empty
// current means the bookmark has never been set, so any candidate is newer.
func isNewerRequest(queue, candidate, current string) (bool, error) {
	if current == "" {
		return true, nil
	}
	cmp, err := entity.CompareRequestID(queue, candidate, current)
	if err != nil {
		return false, fmt.Errorf("failed to compare request ids for queue %s: %w", queue, err)
	}
	return cmp > 0, nil
}

// loadRequest loads the request by id.
func (c *Controller) loadRequest(ctx context.Context, store storage.Storage, id string) (entity.Request, error) {
	return loader.ByID(ctx, id, store.GetRequestStore().Get, "request")
}

// Name returns the controller name for logging and metrics.
func (c *Controller) Name() string {
	return "record"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
