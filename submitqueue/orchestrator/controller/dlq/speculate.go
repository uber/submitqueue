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

package dlq

import (
	"context"
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	corebatch "github.com/uber/submitqueue/submitqueue/core/batch"
	"github.com/uber/submitqueue/submitqueue/core/publish"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// speculateController is the DLQ reconciler for the speculate topic.
//
// Speculate does not share the generic batch reconciler because it is not a
// batch-scoped stage. Its message names one batch, but a run re-plans the
// entire queue: it lists every in-flight batch, hands them all to the
// Speculator, and commits outcomes for any of them. A failure is therefore
// often about a different batch than the message names, or about the queue
// itself — and failing the named batch regardless would terminate one that was
// never at fault while leaving the real culprit running.
//
// So this reconciler reads the failure's subjects and acts on those. It also
// republishes to speculate afterwards, because a dead letter here consumes an
// edge the queue needed. Speculation is driven only by messages; a batch
// admitted to Speculating produces no build to signal and no merge to conclude,
// so once the message that would have funded it is gone, nothing is left to
// look at it again. Without the republish the failure of one batch silently
// strands every other batch in the queue.
type speculateController struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	stores        storage.Factory
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify speculateController implements consumer.Controller at compile time.
var _ consumer.Controller = (*speculateController)(nil)

// NewDLQSpeculateController builds the DLQ controller for the speculate topic.
// topicKey must be the DLQ topic key (typically TopicKey(primary)).
func NewDLQSpeculateController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores storage.Factory,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) consumer.Controller {
	name := string(topicKey) + "_controller"
	return &speculateController{
		logger:        logger.Named(name),
		metricsScope:  scope.SubScope(name),
		stores:        stores,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process reconciles a single dead-lettered speculate message.
func (c *speculateController) Process(ctx context.Context, delivery consumer.Delivery) error {
	const opName = "process"

	msg := delivery.Message()

	bid, err := entity.BatchIDFromBytes(msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		return fmt.Errorf("failed to decode batch id from dlq payload: %w", err)
	}
	if bid.ID == "" {
		metrics.NamedCounter(c.metricsScope, opName, "empty_id_errors", 1)
		return fmt.Errorf("dlq payload decoded to empty batch id")
	}

	store, err := c.stores.For(storage.Config{QueueName: bid.Queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_resolve_errors", 1)
		// Non-retryable: a missing or unresolvable queue is a malformed message.
		return fmt.Errorf("failed to resolve storage for queue %q: %w", bid.Queue, err)
	}

	lastError, failureMeta := failureContext(delivery)
	culprits, attribution := c.blame(delivery, bid.ID)
	failureMeta["dlq.attribution"] = attribution

	c.logger.Warnw("dlq message received",
		"batch_id", bid.ID,
		"queue", bid.Queue,
		"attempt", delivery.Attempt(),
		"attribution", attribution,
		"culprits", culprits,
		"dlq_last_error", lastError,
	)
	metrics.NamedCounter(c.metricsScope, opName, "reconciled", 1,
		metrics.NewTag("attribution", attribution))

	progressed := false
	for _, batchID := range culprits {
		transitioned, err := failBatch(ctx, store, c.registry, c.logger, batchID, lastError, failureMeta)
		if err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "reconcile_errors", 1)
			return err
		}
		progressed = progressed || transitioned
	}

	if !progressed {
		// A redelivery of a reconcile already done. Republishing here would
		// hand the queue the same message again on every redelivery, forever.
		return nil
	}

	return c.retrigger(ctx, store, bid.Queue)
}

// blame decides which batches to fail, and reports how that was decided so the
// answer is visible on the request afterwards rather than inferred.
//
// A failure that names batches is taken at its word — those are the batches the
// run was actually working on when it failed. Anything else falls back to the
// batch on the message: that is a guess, but DLQ reconciliation exists so
// requests cannot sit non-terminal forever, and that guarantee has to hold even
// when nothing can say which batch was at fault.
func (c *speculateController) blame(delivery consumer.Delivery, payloadBatchID string) ([]string, string) {
	f, failed := delivery.Failure()
	if !failed {
		return []string{payloadBatchID}, "unattributed"
	}

	if batches := f.IDsOfType(entity.SubjectTypeBatch); len(batches) > 0 {
		return batches, "batch"
	}

	// A queue subject says no batch was at fault — a failure listing or
	// planning the queue. There is still no other way to release the message's
	// requests, so the named batch is failed, labelled for what it is.
	if len(f.IDsOfType(entity.SubjectTypeQueue)) > 0 {
		return []string{payloadBatchID}, "queue"
	}

	return []string{payloadBatchID}, "unattributed"
}

// retrigger wakes the queue if it still holds live batches, restoring the edge
// this dead letter consumed.
//
// It names a batch that is still live rather than the one just failed, and runs
// only after a reconcile that actually transitioned something. Together those
// make it terminate: each pass fails one more batch and hands the queue another
// run, so a queue that is genuinely broken drains to empty — every batch
// recorded with a reason — instead of stranding, while a queue whose failure
// was transient or queue-wide simply recovers on the next run.
func (c *speculateController) retrigger(ctx context.Context, store storage.Storage, queue string) error {
	const opName = "process"

	live, err := corebatch.ListByStates(ctx, store, entity.ActiveBatchStates())
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to list live batches of queue %s: %w", queue, err)
	}
	if len(live) == 0 {
		return nil
	}

	// ListByStates does not promise an order, so pick deterministically. Any
	// live batch would wake the queue — the run re-plans all of it — but a
	// stable choice keeps the behaviour reproducible.
	next := live[0].ID
	for _, b := range live[1:] {
		if b.ID < next {
			next = b.ID
		}
	}

	payload, err := entity.BatchID{ID: next, Queue: queue}.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize batch ID: %w", err)
	}

	// A distinct message ID every time: the queue deduplicates on
	// (topic, partition, ID) against rows it has not collected yet, so reusing
	// the batch ID would make this wake-up a silent no-op.
	if err := publish.Message(ctx, c.registry, topickey.TopicKeySpeculate, publish.UniqueID(next), payload, queue); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to re-trigger speculation for queue %s: %w", queue, err)
	}

	metrics.NamedCounter(c.metricsScope, opName, "retriggered", 1)
	c.logger.Infow("re-triggered speculation after dlq reconcile",
		"queue", queue,
		"batch_id", next,
		"live_batches", len(live),
	)
	return nil
}

// Name returns the controller name for logging and metrics.
func (c *speculateController) Name() string {
	return string(c.topicKey)
}

// TopicKey returns the topic key this controller subscribes to.
func (c *speculateController) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *speculateController) ConsumerGroup() string {
	return c.consumerGroup
}
