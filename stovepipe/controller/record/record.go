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
// terminal status, and turns that outcome into durable validation state.
//
// The durable state is a ValidationFact per validated commit, plus the queue's
// last-green bookmark, which process reads to choose an incremental build
// baseline. A green commit is also promoted, moving the queue's promotion ref so
// downstream systems can pull the latest green commit by name. Downstream hooks
// are not implemented yet.
package record

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/core/loader"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

// Controller consumes Record messages, records the build's validation fact, and
// when that fact is green advances the queue's last-green bookmark and promotes
// the commit. Implements consumer.Controller.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	stores        storage.Factory
	sourceControl sourcecontrol.Factory
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// _opName is the metric operation name shared by every emit in this file.
const _opName = "record"

// wholeRepositoryProject is the project component of a fact covering the whole
// repository rather than one project within it. Per-project facts need target-graph
// attribution that this stage does not do, so every fact it writes is whole-repository.
const wholeRepositoryProject = ""

// NewController creates a new record controller.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores storage.Factory,
	sourceControl sourcecontrol.Factory,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	name := string(topicKey) + "_controller"
	return &Controller{
		logger:        logger.Named(name),
		metricsScope:  scope.SubScope(name),
		stores:        stores,
		sourceControl: sourceControl,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process loads the request referenced by the delivery, records its validation
// fact and, when that fact is green, advances the queue's last-green bookmark and
// promotes the commit. Returns nil to ack (success) or an error to nack (retry) /
// reject (DLQ).
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
	c = c.forQueue(rec.GetQueueName())

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
	case entity.RequestStateSucceeded, entity.RequestStateFailed:
		fact, created, err := c.recordFact(ctx, store, request)
		if err != nil {
			return err
		}
		if !fact.IsGreen() {
			metrics.NamedCounter(c.metricsScope, _opName, "not_green", 1)
			// Only the writer of the fact reports the latency: a redelivery adopts
			// the stored fact instead, and a second sample would count one break
			// twice in the distribution.
			if created {
				c.reportFailureDetectionLatency(ctx, request)
			}
			return nil
		}
		holdsBookmark, err := c.advanceLastGreen(ctx, store, request)
		if err != nil {
			metrics.NamedCounter(c.metricsScope, _opName, "storage_errors", 1)
			return err
		}
		if !holdsBookmark {
			// A later green commit already holds the bookmark, so it also owns
			// the promotion ref: promoting this older commit would move the ref
			// backwards.
			return nil
		}
		return c.promote(ctx, request)

	case entity.RequestStateCancelled:
		// A cancelled build decided nothing about the commit, so it establishes
		// no fact. The identity stays unclaimed; the next commit re-validates.
		metrics.NamedCounter(c.metricsScope, _opName, "cancelled", 1)
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

// recordFact writes the request's outcome as an immutable whole-repository fact and
// returns the fact that is actually stored, which is not always the one just built:
// facts are first-writer-wins, so an identity already claimed by this same request —
// a redelivery after the write but before the bookmark advanced — yields the stored
// fact instead. Every decision downstream reads that stored fact rather than the
// request, so a redelivery cannot reach a different verdict than the original. The
// second return reports whether this call is the one that wrote the fact, which is
// how a caller tells the original delivery from a redelivery.
func (c *Controller) recordFact(ctx context.Context, store storage.Storage, request entity.Request) (entity.ValidationFact, bool, error) {
	factStore := store.GetValidationFactStore()

	fact := entity.ValidationFact{
		URI:       request.URI,
		Project:   wholeRepositoryProject,
		Degree:    degreeFor(request.State),
		RequestID: request.ID,
		CreatedAt: time.Now().UnixMilli(),
	}

	err := factStore.Create(ctx, fact)
	switch {
	case err == nil:
		metrics.NamedCounter(c.metricsScope, _opName, "fact_created", 1)
		c.logger.Infow("recorded validation fact",
			"queue", request.Queue,
			"request_id", request.ID,
			"uri", request.URI,
			"degree", fact.Degree,
		)
		return fact, true, nil

	case errors.Is(err, storage.ErrAlreadyExists):
		stored, getErr := factStore.Get(ctx, request.URI, wholeRepositoryProject)
		if getErr != nil {
			metrics.NamedCounter(c.metricsScope, _opName, "storage_errors", 1)
			return entity.ValidationFact{}, false, fmt.Errorf("failed to load the existing fact for uri %s: %w", request.URI, getErr)
		}
		if stored.RequestID != request.ID {
			// Two requests validating one URI would break the dedup ingest
			// enforces, so this is a broken invariant rather than a race to
			// resolve. Non-retryable: the stored fact is immutable.
			metrics.NamedCounter(c.metricsScope, _opName, "invariant_errors", 1)
			return entity.ValidationFact{}, false, fmt.Errorf(
				"fact for uri %s is owned by request %s, not %s", request.URI, stored.RequestID, request.ID)
		}
		metrics.NamedCounter(c.metricsScope, _opName, "fact_exists", 1)
		return stored, false, nil

	default:
		metrics.NamedCounter(c.metricsScope, _opName, "storage_errors", 1)
		return entity.ValidationFact{}, false, fmt.Errorf("failed to create the fact for uri %s: %w", request.URI, err)
	}
}

// reportFailureDetectionLatency records how long the break this build failed on went
// undetected, measured from the commit timestamp of the base it validated against. A
// histogram rather than a gauge because the distribution over failures is the point:
// how long a break typically survives, not how long the last one did.
//
// Unlike the last-green age, there is no later moment to sample this from — an elapsed
// time is only meaningful against the failure that just became known — so the
// source-control lookup cannot be moved off the delivery path onto a clock. It is
// confined to failures and made once the fact is durable, and every way it can fail is
// counted and swallowed so a reporting fault cannot retry an outcome already recorded.
func (c *Controller) reportFailureDetectionLatency(ctx context.Context, request entity.Request) {
	queueTag := metrics.NewTag("queue", request.Queue)
	strategyTag := metrics.NewTag("strategy", string(request.BuildStrategy))

	// Only a strategy that validates a delta pins a base commit, so a full build has
	// no baseline to measure from. Its failures are counted rather than timed: absent
	// here is the ordinary case, not a fault.
	if request.BaseURI == "" {
		metrics.NamedCounter(c.metricsScope, _opName, "failure_detection_missing", 1, queueTag, strategyTag)
		return
	}

	sourceControl, err := c.sourceControl.For(sourcecontrol.Config{QueueName: request.Queue})
	if err != nil {
		c.failureDetectionUnobserved(request, "resolve_source_control", err)
		return
	}

	info, err := sourceControl.ChangeInfo(ctx, request.BaseURI)
	if err != nil {
		c.failureDetectionUnobserved(request, "get_change_info", err)
		return
	}

	// SourceControl must report a positive creation timestamp, so a missing one is a
	// broken extension contract rather than a lookup failure. Measuring from 1970
	// would drop a decades-long sample into the distribution.
	if info.CreatedAt <= 0 {
		c.failureDetectionUnobserved(request, "undated_change", nil)
		return
	}

	// A base dated in the future means the provider's clock disagrees with ours; a
	// negative latency would corrupt the distribution rather than describe it.
	latency := time.Since(time.UnixMilli(info.CreatedAt))
	if latency < 0 {
		c.failureDetectionUnobserved(request, "future_change", nil)
		return
	}

	metrics.NamedHistogram(c.metricsScope, _opName, "failure_detection_latency", metrics.ChangeAgeBuckets,
		queueTag, strategyTag,
	).RecordDuration(latency)
}

// failureDetectionUnobserved counts a latency that could not be observed, tagged with
// the step that failed so an unmeasurable failure can be told apart from a broken
// dependency.
func (c *Controller) failureDetectionUnobserved(request entity.Request, step string, err error) {
	metrics.NamedCounter(c.metricsScope, _opName, "failure_detection_errors", 1,
		metrics.NewTag("queue", request.Queue),
		metrics.NewTag("step", step),
	)
	c.logger.Warnw("failed to observe how long the build failure went undetected",
		"queue", request.Queue,
		"base_uri", request.BaseURI,
		"step", step,
		"error", err,
	)
}

// degreeFor maps a request's build outcome onto a whole-repository degree. Only the
// endpoints are produced: a whole-repository build is either clean or it is not, and
// intermediate degrees need per-project attribution that this stage does not do.
func degreeFor(state entity.RequestState) float64 {
	if state == entity.RequestStateSucceeded {
		return entity.DegreeGreen
	}
	return entity.DegreeBroken
}

// advanceLastGreen points the queue's bookmark at request, retrying on version
// conflicts, and reports whether request holds the bookmark afterwards. The
// bookmark only moves forward: an older candidate is skipped without a write and
// does not hold it, while a redelivery of the request that already set it holds it
// without a write, so the promotion that follows is retried.
//
// The bookmark is a cache of "newest green URI" derived from the facts, so it is
// advanced only after the green fact is durable. Losing the advance to a crash is
// recoverable — the redelivery reloads the same fact and retries — whereas a
// bookmark with no fact behind it would point at greenness nothing recorded.
func (c *Controller) advanceLastGreen(ctx context.Context, store storage.Storage, request entity.Request) (bool, error) {
	queueStore := store.GetQueueStore()

	for {
		queueRow, err := queueStore.Get(ctx, request.Queue)
		if err != nil {
			return false, fmt.Errorf("failed to load queue %s to advance last green: %w", request.Queue, err)
		}

		cmp, err := compareToBookmark(request.Queue, request.ID, queueRow.LastGreenRequestID)
		if err != nil {
			// Non-retryable: re-parsing the same ids cannot start succeeding.
			return false, err
		}
		if cmp < 0 {
			return false, nil
		}
		if cmp == 0 {
			return true, nil
		}

		updated := queueRow
		updated.LastGreenURI = request.URI
		updated.LastGreenRequestID = request.ID
		newVersion := queueRow.Version + 1
		if err := queueStore.Update(ctx, updated, queueRow.Version, newVersion); err != nil {
			if errors.Is(err, storage.ErrVersionMismatch) {
				continue
			}
			return false, fmt.Errorf("failed to advance last green for queue %s: %w", request.Queue, err)
		}

		metrics.NamedCounter(c.metricsScope, _opName, "last_green_advanced", 1)
		c.logger.Infow("advanced last green bookmark",
			"queue", request.Queue,
			"request_id", request.ID,
			"last_green_uri", request.URI,
		)
		c.emitLastGreenTimestamp(ctx, request)
		return true, nil
	}
}

// emitLastGreenTimestamp emits the creation time of the change the bookmark now
// points at, once that bookmark is durable. Reporting is best-effort so an
// observability failure cannot turn a successful record operation into a retry,
// which is why each cause is counted and logged separately instead of returned.
func (c *Controller) emitLastGreenTimestamp(ctx context.Context, request entity.Request) {
	queueTag := metrics.NewTag("queue", request.Queue)

	sourceControl, err := c.sourceControl.For(sourcecontrol.Config{QueueName: request.Queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "last_green_timestamp_resolve_errors", 1, queueTag)
		c.logger.Warnw("failed to resolve source control to report the last green timestamp",
			"queue", request.Queue,
			"error", err,
		)
		return
	}

	info, err := sourceControl.ChangeInfo(ctx, request.URI)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "last_green_timestamp_errors", 1, queueTag)
		c.logger.Warnw("failed to look up the last green change timestamp",
			"queue", request.Queue,
			"uri", request.URI,
			"error", err,
		)
		return
	}

	// SourceControl must report a positive creation timestamp, so a missing one
	// is a broken extension contract rather than a lookup failure. Emitting it
	// anyway would publish a 1970 timestamp and read as an infinitely stale queue.
	if info.CreatedAt <= 0 {
		metrics.NamedCounter(c.metricsScope, _opName, "last_green_timestamp_invalid", 1, queueTag)
		c.logger.Warnw("source control reported no creation timestamp for the last green change",
			"queue", request.Queue,
			"uri", request.URI,
			"created_at", info.CreatedAt,
		)
		return
	}

	// The gauge carries the creation time as Unix seconds, so subtracting it
	// from the current time yields the age of the last-green change in seconds.
	metrics.NamedGauge(
		c.metricsScope,
		_opName,
		"last_green_timestamp_seconds",
		float64(time.UnixMilli(info.CreatedAt).Unix()),
		queueTag,
	)
}

// promote points the queue's promotion ref at the request's commit so downstream
// systems can pull the latest green commit by name. Which ref that is — and whether
// the queue has one at all — is source-control configuration, so this stage names
// only the commit.
//
// Like the bookmark, the ref is a cache of the facts, so it moves only after the
// green fact is durable. Promotion is idempotent, so a redelivery repeats it
// harmlessly. A commit that a rewritten history dropped from the ref cannot be
// promoted by any retry, so that case is counted and skipped rather than failed.
func (c *Controller) promote(ctx context.Context, request entity.Request) error {
	sc, err := c.sourceControl.For(sourcecontrol.Config{QueueName: request.Queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "source_control_errors", 1,
			metrics.NewTag("stage", "resolve"),
		)
		return fmt.Errorf("failed to resolve source control for queue %s: %w", request.Queue, err)
	}

	if err := sc.Promote(ctx, request.URI); err != nil {
		if sourcecontrol.IsNotFound(err) {
			metrics.NamedCounter(c.metricsScope, _opName, "promotions_skipped", 1,
				metrics.NewTag("reason", "unknown_uri"),
			)
			c.logger.Warnw("green commit is no longer on the queue's ref; skipping promotion",
				"queue", request.Queue,
				"request_id", request.ID,
				"uri", request.URI,
			)
			return nil
		}

		metrics.NamedCounter(c.metricsScope, _opName, "source_control_errors", 1,
			metrics.NewTag("stage", "promote"),
		)
		return fmt.Errorf("failed to promote uri %s of queue %s: %w", request.URI, request.Queue, err)
	}

	metrics.NamedCounter(c.metricsScope, _opName, "promotions", 1)
	c.logger.Infow("promoted green commit",
		"queue", request.Queue,
		"request_id", request.ID,
		"uri", request.URI,
	)
	return nil
}

// compareToBookmark orders candidate against the request id currently holding the
// bookmark, by ingest order, using the sign convention of entity.CompareRequestID.
// An empty current means the bookmark has never been set, so any candidate is newer.
func compareToBookmark(queue, candidate, current string) (int, error) {
	if current == "" {
		return 1, nil
	}
	cmp, err := entity.CompareRequestID(queue, candidate, current)
	if err != nil {
		return 0, fmt.Errorf("failed to compare request ids for queue %s: %w", queue, err)
	}
	return cmp, nil
}

// loadRequest loads the request by id.
func (c *Controller) loadRequest(ctx context.Context, store storage.Storage, id string) (entity.Request, error) {
	return loader.ByID(ctx, id, store.GetRequestStore().Get, "request")
}

// Name returns the controller name for logging and metrics.
func (c *Controller) Name() string {
	return string(c.topicKey)
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}

func (c *Controller) forQueue(queue string) *Controller {
	if queue == "" {
		return c
	}
	scoped := *c
	scoped.metricsScope = c.metricsScope.Tagged(map[string]string{"queue": queue})
	return &scoped
}
