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

// Package merge implements the trigger stage for the asynchronous merge. It
// consumes a batch ready to land, builds the full merge request from the
// batch's member requests (one step per request, in Contains order), and
// publishes it to runway's merge queue using the batch id as the client-owned
// correlation id. Runway performs the merge out of process and publishes the
// result to the merge-signal queue, which the mergesignal stage consumes and
// correlates back to the batch by that id.
//
// # The hand-off gate
//
// Speculation finalizes optimistically: a batch is published here as soon as
// its base is itself published for merge, not once it has landed. This stage
// is the guard that makes the optimism safe. Every delivery re-derives the
// batch's merge readiness from current dependency state (the shared
// speculation.PathMergeConfirmed / PathMergePossible rules) and takes one of
// three actions: confirmed hands off to runway; possible — a base still
// Merging, or Cancelling on its way to a terminal state — parks the batch,
// acking now and re-checking after WaitDelayMs; refuted — a base failed —
// drops the trigger and nudges speculate, which owns failing the batch.
// Runway therefore only ever sees batches whose bases actually landed,
// whatever order messages arrive in.
//
// # Ordering
//
// Two queues with different contracts are in play here, and only one of them
// is ordered. The trigger topic this stage consumes is an unordered work
// queue of "re-evaluate this batch" wake-ups: every delivery re-reads state,
// so processing order is irrelevant, and a parked batch acks and re-queues
// itself rather than holding the partition. Per-partition FIFO could not be
// trusted for correctness anyway — nack backoff delivers later offsets past
// a backing-off earlier one. Runway's queue is where dependency order binds,
// and there the gate yields an invariant stronger than ordering: for every
// dependency edge, the dependent's MergeRequest is published only after the
// base is terminal, so a chain's requests never even coexist in flight and
// runway needs no cross-request ordering. That is also what makes the
// request shape valid at all — a request carries only its own batch's steps,
// counting on the base's changes already being on the target. Batches with
// no dependency edge have no overlapping targets, so their merges commute
// and need no ordering in the first place.
//
// Concretely, take a chain B1 <- B2 <- B3, all finalized optimistically and
// all in Merging. B1 (no unsettled base) confirms and hands off; B2 and B3
// are possible-but-not-confirmed, so each parks. Their delayed re-checks now
// interleave on the trigger topic in whatever order — B3 ahead of B2 one
// cycle, B2 ahead of B3 the next — and none of it matters: B3's re-check
// cannot confirm before B2 has landed no matter when it runs, it just parks
// again. When B1's verdict returns, mergesignal fans out, speculate wakes
// each dependent, and Merging supervision re-arms the waiting batch's
// trigger immediately — so B2 confirms at event speed and the chain hands
// off link by link, each link paced by its runway round-trip. The
// WaitDelayMs re-check is a backstop for a lost or dedup-swallowed wake, not
// the pace. The cost of a parked chain of depth D is D futile re-checks per
// cycle — a few point reads and one delayed publish each — which is why
// WaitDelayMs can be raised freely on a hot queue.
package merge

import (
	"context"
	"fmt"
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/zap"

	changepb "github.com/uber/submitqueue/api/base/change/protopb"
	strategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	"github.com/uber/submitqueue/platform/base/mergestrategy"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/core/speculation"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

// WaitDelayMs is the delay before re-checking a batch whose merge is
// triggered but whose base dependency has not settled yet (Merging or
// Cancelling). The re-check is the backup wake channel — the prompt one is
// the settling base's fan-out re-arming the trigger through speculate's
// Merging supervision — so raising it trades only backstop latency, never
// chain pace (see the package doc's Ordering section). Var (not const) so
// tests can shorten it; the orchestrator always uses the default.
var WaitDelayMs int64 = 2000

// Controller handles merge queue messages. Implements consumer.Controller.
//
// It loads the batch and its member requests, assembles the full merge request
// (one step per member request, in Contains order, each carrying that request's
// change and land strategy), and publishes it to runway's merge queue. Runway
// performs the merge out of process and returns the result on the merge-signal
// queue; the mergesignal stage consumes it and transitions the batch. This
// controller therefore performs no state transition itself.
type Controller struct {
	logger         *zap.SugaredLogger
	metricsScope   tally.Scope
	store          storage.Storage
	registry       consumer.TopicRegistry
	runwayTopicKey consumer.TopicKey
	topicKey       consumer.TopicKey
	consumerGroup  string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// NewController creates a new merge controller for the orchestrator.
// runwayTopicKey is the runway-owned topic this controller publishes merge
// requests to (TopicKeyMerge).
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	store storage.Storage,
	registry consumer.TopicRegistry,
	runwayTopicKey consumer.TopicKey,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:         logger.Named("merge_controller"),
		metricsScope:   scope.SubScope("merge_controller"),
		store:          store,
		registry:       registry,
		runwayTopicKey: runwayTopicKey,
		topicKey:       topicKey,
		consumerGroup:  consumerGroup,
	}
}

// Process publishes the full merge request to runway once the batch's
// dependency outcomes are confirmed. Speculation triggers this stage
// optimistically — possibly while a base dependency is still unsettled — so
// the hand-off is gated: confirmed hands off, still-possible re-checks
// after WaitDelayMs, refuted acks and drops (speculate owns failing the
// batch). Returns nil to ack (success), or error to nack/reject.
//
// Error classification: deserialize failures are non-retryable and reject
// to DLQ. Storage reads and every publish here — to runway, the delayed
// self re-check, and the refuted-batch speculate nudge — retry for
// transient causes via the shared MySQL classifier and otherwise
// dead-letter; the publishes are what keep the merge alive, so an enqueue
// blip should replay rather than strand the batch.
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	const opName = "process"

	op := metrics.Begin(c.metricsScope, opName)
	defer func() { op.Complete(retErr) }()

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

	c.logger.Infow("received merge event",
		"batch_id", batch.ID,
		"queue", batch.Queue,
		"state", string(batch.State),
		"version", batch.Version,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	// Short-circuit halted batches (terminal or cancelling): no merge should be
	// kicked off for a batch that will not proceed. Unlike the old synchronous
	// merge there is no terminal re-fan-out here — the mergesignal stage owns the
	// state transition and fan-out once runway's result returns, so a redelivery
	// at this stage simply acks. This also ends the delayed re-check cycle for a
	// batch that speculate failed while it was waiting.
	if entity.IsBatchStateHalted(batch.State) {
		metrics.NamedCounter(c.metricsScope, opName, "skipped_halted", 1)
		c.logger.Infow("skipping merge for halted batch",
			"batch_id", batch.ID,
			"state", string(batch.State),
		)
		return nil
	}

	// The optimistic hand-off gate: only publish to runway once some path's
	// merge assumptions are confirmed by dependency outcomes. Ordering is
	// enforced by state, not by queue position — see the package doc's
	// Ordering section for the full model: why this trigger topic is
	// unordered, why a dependency chain's runway requests never coexist in
	// flight, and what a parked chain costs per re-check cycle.
	confirmed, possible, err := c.mergeReadiness(ctx, batch)
	if err != nil {
		return err
	}
	if !confirmed {
		if possible {
			// A base is still unsettled (Merging or Cancelling): it will
			// land, cancel out of the way, or fail — all of which settle
			// this classification one way or the other. Keep the attempt
			// alive by re-checking after a delay; each cycle mints a
			// fresh message ID because the queue dedups publishes on
			// (topic, partition, id) even against consumed rows — reusing
			// the consumed message's ID would silently end the cycle. The
			// delay is a backstop, not the chain's pace: a settling base's
			// terminal fan-out re-arms this trigger promptly through
			// speculate's Merging supervision (see the package doc's
			// Ordering section).
			//
			// TODO(merge-wait-model): the park-and-poll model is a
			// deliberate first cut — it rides an unordered queue as a
			// delay timer and burns one futile re-check per parked batch
			// per cycle, O(chain depth) every WaitDelayMs. Figure out a
			// better model: wake purely on dependency events with a
			// hardened delivery path, back the delay off by time spent in
			// Merging, or park only the chain's frontier batch.
			metrics.NamedCounter(c.metricsScope, opName, "waiting_on_base", 1)
			c.logger.Debugw("base dependency not settled; delaying runway hand-off",
				"batch_id", batch.ID,
			)
			if err := c.republishSelfAfter(ctx, batch, WaitDelayMs); err != nil {
				metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
				return fmt.Errorf("failed to re-publish merge trigger for batch %s: %w", batch.ID, err)
			}
			return nil
		}
		// No path can confirm anymore — a base failed after the optimistic
		// finalize. Never hand off: runway must not see a batch whose base
		// did not land. Speculate's Merging supervision fails the batch —
		// and it is nudged directly with a minted message ID, because the
		// dependent-side terminal fan-out publishes under the batch ID and
		// the queue's (topic, partition, id) dedup can silently swallow
		// that wake against an un-GC'd consumed row.
		metrics.NamedCounter(c.metricsScope, opName, "refuted", 1)
		c.logger.Warnw("no path can confirm; dropping merge trigger",
			"batch_id", batch.ID,
		)
		if err := c.publishSpeculateNudge(ctx, batch); err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
			return fmt.Errorf("failed to nudge speculate for refuted batch %s: %w", batch.ID, err)
		}
		return nil
	}

	// Build the full payload runway needs to perform the merge. The batch id is
	// the client-owned correlation id, so a redelivery republishes the same id
	// and runway dedupes on it; the result is matched straight back to the batch.
	req, err := c.buildMergeRequest(ctx, batch)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to build merge request for batch %s: %w", batch.ID, err)
	}

	if err := c.publish(ctx, c.runwayTopicKey, req, batch.Queue); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish to runway merge: %w", err)
	}

	c.logger.Infow("published merge to runway",
		"batch_id", batch.ID,
		"steps", len(req.Steps),
		"topic_key", c.runwayTopicKey,
	)

	return nil // Success - message will be acked
}

// mergeReadiness classifies the batch's merge assumptions against its
// dependencies' current states: confirmed when some path's assumptions are
// fully confirmed (speculation.PathMergeConfirmed), else possible when some
// path may still confirm because a base is still unsettled
// (speculation.PathMergePossible), else neither. A batch with no dependencies is
// trivially confirmed — speculation only triggers a merge once some path's
// own build Passed, and with nothing to wait on that cannot regress — so
// the common no-dependency case costs no extra reads.
func (c *Controller) mergeReadiness(ctx context.Context, batch entity.Batch) (confirmed, possible bool, err error) {
	if len(batch.Dependencies) == 0 {
		return true, true, nil
	}

	depByID := make(map[string]entity.Batch, len(batch.Dependencies))
	for _, depID := range batch.Dependencies {
		d, derr := c.store.GetBatchStore().Get(ctx, depID)
		if derr != nil {
			metrics.NamedCounter(c.metricsScope, "process", "storage_errors", 1)
			return false, false, fmt.Errorf("failed to get dependency batch %s of %s: %w", depID, batch.ID, derr)
		}
		depByID[depID] = d
	}

	// A merge trigger only exists because speculate finalized from this
	// batch's tree, and trees are never deleted — a miss is corrupted
	// state, surfaced as an error so the message dead-letters loudly.
	tree, terr := c.store.GetSpeculationTreeStore().Get(ctx, batch.ID)
	if terr != nil {
		metrics.NamedCounter(c.metricsScope, "process", "storage_errors", 1)
		return false, false, fmt.Errorf("failed to get speculation tree for batch %s: %w", batch.ID, terr)
	}

	for _, p := range tree.Paths {
		if speculation.PathMergeConfirmed(p, depByID) {
			return true, true, nil
		}
		if speculation.PathMergePossible(p, depByID) {
			possible = true
		}
	}
	return false, possible, nil
}

// republishSelfAfter re-publishes the batch's merge trigger to this
// controller's own topic after delayMs, minting a fresh message ID per
// cycle: the queue dedups publishes on (topic, partition, id) against
// un-GC'd rows including consumed ones, so reusing an ID would silently end
// the re-check cycle.
func (c *Controller) republishSelfAfter(ctx context.Context, batch entity.Batch, delayMs int64) error {
	payload, err := entity.BatchID{ID: batch.ID}.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize batch ID: %w", err)
	}

	msgID := fmt.Sprintf("%s/mergewait/%d", batch.ID, time.Now().UnixMilli())
	msg := entityqueue.NewMessage(msgID, payload, batch.Queue, nil)

	q, ok := c.registry.Queue(c.topicKey)
	if !ok {
		return fmt.Errorf("no queue registered for topic key %s", c.topicKey)
	}
	topicName, ok := c.registry.TopicName(c.topicKey)
	if !ok {
		return fmt.Errorf("no topic name registered for topic key %s", c.topicKey)
	}
	return q.Publisher().PublishAfter(ctx, topicName, msg, delayMs)
}

// publishSpeculateNudge wakes the speculate stage for a batch whose merge
// trigger was refuted, minting a fresh message ID so the queue's
// (topic, partition, id) publish dedup cannot swallow it the way it can a
// terminal fan-out wake published under the bare batch ID.
func (c *Controller) publishSpeculateNudge(ctx context.Context, batch entity.Batch) error {
	payload, err := entity.BatchID{ID: batch.ID}.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize batch ID: %w", err)
	}

	msgID := fmt.Sprintf("%s/mergerefuted/%d", batch.ID, time.Now().UnixMilli())
	msg := entityqueue.NewMessage(msgID, payload, batch.Queue, nil)

	q, ok := c.registry.Queue(topickey.TopicKeySpeculate)
	if !ok {
		return fmt.Errorf("no queue registered for topic key %s", topickey.TopicKeySpeculate)
	}
	topicName, ok := c.registry.TopicName(topickey.TopicKeySpeculate)
	if !ok {
		return fmt.Errorf("no topic name registered for topic key %s", topickey.TopicKeySpeculate)
	}
	if err := q.Publisher().Publish(ctx, topicName, msg); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}
	return nil
}

// buildMergeRequest loads the batch's member requests and assembles the runway
// merge request: one MergeStep per request, in Contains order, attributed by
// request id and carrying that request's change and land strategy.
func (c *Controller) buildMergeRequest(ctx context.Context, batch entity.Batch) (*runwaymq.MergeRequest, error) {
	steps := make([]*runwaymq.MergeStep, 0, len(batch.Contains))
	for _, requestID := range batch.Contains {
		request, err := c.store.GetRequestStore().Get(ctx, requestID)
		if err != nil {
			return nil, fmt.Errorf("failed to get request %s: %w", requestID, err)
		}
		steps = append(steps, &runwaymq.MergeStep{
			StepId:   request.ID,
			Changes:  []*changepb.Change{{Uris: request.Change.URIs}},
			Strategy: toProtoStrategy(request.LandStrategy),
		})
	}
	return &runwaymq.MergeRequest{
		Id:        batch.ID,
		QueueName: batch.Queue,
		Steps:     steps,
	}, nil
}

// toProtoStrategy maps the shared mergestrategy.MergeStrategy entity to the
// proto Strategy enum carried on the wire. An unknown strategy maps to DEFAULT,
// letting runway apply the queue's configured default.
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

// publish serializes the runway merge request and publishes it to the given
// topic key, partitioned by queue.
func (c *Controller) publish(ctx context.Context, key consumer.TopicKey, req *runwaymq.MergeRequest, partitionKey string) error {
	payload, err := runwaymq.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to serialize merge request: %w", err)
	}

	msg := entityqueue.NewMessage(req.Id, payload, partitionKey, nil)

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
	return "merge"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
