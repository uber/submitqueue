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

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/base/failure"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/platform/publish"
	corebatch "github.com/uber/submitqueue/submitqueue/core/batch"
	corerequest "github.com/uber/submitqueue/submitqueue/core/request"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/speculator"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// Controller handles speculate queue messages: each one is a dirty signal
// naming a batch whose queue should be re-planned. The package doc has the
// full model; Process below is the entry point.
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

// Process re-plans the triggering batch's queue. The run does almost all of
// the work — see run.go — and the message's own batch matters in only two
// places on the way in:
//
//   - Created: the batch is admitted first, which makes it visible to the
//     Speculator (proposals may only target Speculating heads). Reaching an
//     outcome on it in the same run is safe: a merge needs a passed path, and
//     a head admitted this instant has no paths at all.
//   - Already terminal: its conclude publish is repeated in case a previous
//     one was lost — idempotent on the batch ID — and the run that follows is
//     how dependents learn of an outcome no run has seen yet (a batch
//     finalized by another stage, e.g. the merge signal recording a landed
//     push, was never seen breaking the paths that bet against it).
//
// Everything else — funding paths, cancelling broken ones, driving a
// cancellation to terminal, reaching outcomes — happens inside the run, which
// covers this batch along with the rest of its queue.
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
		return c.attributed(fmt.Errorf("failed to resolve storage for queue %q: %w", bid.Queue, err),
			entity.QueueSubject(bid.Queue))
	}

	batch, err := store.GetBatchStore().Get(ctx, bid.ID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return c.attributed(fmt.Errorf("failed to get batch %s: %w", bid.ID, err),
			entity.BatchSubject(bid.ID))
	}

	// The payload's queue must match the batch's authoritative queue; a
	// mismatch is a malformed message. Non-retryable — reject to the DLQ.
	if bid.Queue != "" && bid.Queue != batch.Queue {
		metrics.NamedCounter(c.metricsScope, opName, "queue_mismatch", 1)
		return c.attributed(fmt.Errorf("payload queue %q does not match queue %q of batch %s", bid.Queue, batch.Queue, batch.ID),
			entity.BatchSubject(batch.ID))
	}

	if batch.State.IsTerminal() {
		metrics.NamedCounter(c.metricsScope, opName, "self_heal_terminal", 1)
		if err := c.fanout(ctx, batch.ID, batch.Queue); err != nil {
			return err
		}
		return c.run(ctx, store, batch)
	}

	if batch.State == entity.BatchStateCreated {
		if _, err := c.admit(ctx, store, batch); err != nil {
			return err
		}
	}

	// A Merging batch has left the set finalize walks, so a message naming it
	// is the only thing that will look at it again.
	if batch.State == entity.BatchStateMerging {
		metrics.NamedCounter(c.metricsScope, opName, "self_heal_merging", 1)
		if err := c.dispatchMerge(ctx, batch); err != nil {
			return c.attributed(err, entity.BatchSubject(batch.ID))
		}
	}

	return c.run(ctx, store, batch)
}

// admit moves a batch from Created to Speculating, which is what makes it
// visible to the Speculator as an action target. It returns the batch with
// the new state and version so the caller keeps writing against a current
// copy. Which paths to build for the new head is not decided here — that is
// the run's call, taken over the whole queue rather than one batch at a time.
//
// The members are told before the write, not after. This branch runs only from
// Created, so a redelivery of a message whose transition already committed
// skips admit entirely and would never re-publish a log lost on the way out;
// publishing first means a failure nacks with nothing yet changed, and a crash
// in between re-publishes under the same occurrence and dedupes. The cost is
// that a transition which then loses its compare-and-swap leaves one entry for
// a batch that never speculated, superseded by whatever the winner wrote.
func (c *Controller) admit(ctx context.Context, store storage.Storage, batch entity.Batch) (entity.Batch, error) {
	if err := corerequest.PublishBatchLogs(ctx, c.registry, batch.Queue, batch.Contains,
		entity.RequestStatusSpeculating, batch.ID, map[string]string{"batch_id": batch.ID},
	); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "request_log_errors", 1)
		return batch, c.attributed(fmt.Errorf("failed to publish request logs for batch %s: %w", batch.ID, err),
			entity.BatchSubject(batch.ID))
	}

	// Through Transition for the same reason as applyOutcome: the membership
	// record has to leave the created bucket with the batch.
	updated, err := corebatch.Transition(ctx, store, batch, entity.BatchStateSpeculating)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return batch, c.attributed(err, entity.BatchSubject(batch.ID))
	}

	metrics.NamedCounter(c.metricsScope, opName, "admitted", 1)
	c.logger.Infow("admitted batch to speculation",
		"batch_id", updated.ID,
		"queue", updated.Queue,
		"dependencies", updated.Dependencies,
	)
	return updated, nil
}

// fanout re-publishes downstream events for a batch that has already reached
// a terminal state. Used for self-healing when a previous publish was lost:
// re-sending to conclude guarantees request-state reconciliation.
//
// Distinct per publish: this exists precisely because an earlier conclude may
// have gone missing, and the conclude the batch sent when it turned terminal
// is exactly what a stable ID would dedup against.
func (c *Controller) fanout(ctx context.Context, batchID, queue string) error {
	if err := c.publishBatchID(ctx, topickey.TopicKeyConclude, publish.UniqueID(batchID), batchID, queue, queue); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return c.attributed(fmt.Errorf("failed to publish to conclude: %w", err),
			entity.BatchSubject(batchID))
	}
	return nil
}

// publishBatchID publishes a batch ID to the topic behind key under msgID,
// stamped with the batch's queue and partitioned by partitionKey.
//
// Callers choose msgID, because this controller publishes for two different
// kinds of reason. A hand-off that happens once in a batch's life — dispatching
// it to merge, concluding it — names its cause with publish.IntentID, so a
// redelivery that re-derives the same decision is deduplicated instead of
// enacting it twice. A repeat-until-effective nudge — a dispatch re-sent until
// the build stage records it, a fan-out repeated in case an earlier one was
// lost — has no stable cause to name, since the condition that provokes it is
// unchanged across runs, and takes publish.UniqueID so the queue can never
// swallow the repeat that finally lands.
//
// queue and partitionKey are separate because they answer different questions.
// queue is what the payload asserts the batch belongs to, and the consumer
// resolves its storage from it, so it must always be the real queue. The
// partition key only decides what stays serialized behind what: most publishes
// want the queue, so a queue's batches are processed in order, but the build
// dispatch partitions by batch so heads dispatch in parallel.
func (c *Controller) publishBatchID(ctx context.Context, key consumer.TopicKey, msgID, batchID, queue, partitionKey string) error {
	return c.publishBatchIDWithMetadata(ctx, key, msgID, batchID, queue, partitionKey, nil)
}

// publishBatchIDWithMetadata is publishBatchID with side-band message metadata
// attached to the delivery (nil for none). Used to carry a failed batch's reason
// to conclude without persisting it as batch state.
func (c *Controller) publishBatchIDWithMetadata(ctx context.Context, key consumer.TopicKey, msgID, batchID, queue, partitionKey string, metadata map[string]string) error {
	payload, err := entity.BatchID{ID: batchID, Queue: queue}.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize batch ID: %w", err)
	}
	return publish.MessageWithMetadata(ctx, c.registry, key, msgID, payload, partitionKey, metadata)
}

// attributed records what a failure was about and counts it by subject type.
//
// It exists because this stage's message names one batch but its work covers
// the whole queue: a run reads every in-flight batch, hands them all to the
// Speculator, and commits outcomes for any of them. So most failures here are
// not about the batch on the message — they are about some other batch, or
// about the queue itself — and a reconciler that assumed otherwise would
// terminate a batch that was never at fault.
//
// Only the attribution is added. Whether the error is retryable stays with the
// classifiers, which read the cause underneath this wrapper unchanged.
func (c *Controller) attributed(err error, subjects ...failure.Subject) error {
	for _, s := range subjects {
		metrics.NamedCounter(c.metricsScope, opName, "attributed_failure", 1,
			metrics.NewTag("subject_type", s.Type))
	}
	return errs.Attribute(err, subjects...)
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
