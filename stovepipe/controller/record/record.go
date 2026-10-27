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
// The durable state is a ValidationFact per validated commit and project. Build
// completion owns the queue's last-green bookmark, which process reads to choose
// an incremental build baseline. A green commit is also promoted after its facts
// are persisted, moving the queue's promotion ref so downstream systems can pull
// the latest green commit by name.
//
// The outcome is then announced as a hook event, so integrations outside the
// pipeline learn that a commit is green or broken. See
// doc/rfc/stovepipe/steps/record.md for the event shape and
// doc/rfc/hook-framework.md for the delivery promise.
package record

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/core/hookevent"
	"github.com/uber/submitqueue/stovepipe/core/loader"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/projectstatus"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

// Controller consumes Record messages, records the build's validation fact, and
// when that fact is green and holds the queue's last-green bookmark promotes
// the commit. Implements consumer.Controller.
type Controller struct {
	logger                       *zap.SugaredLogger
	metricsScope                 tally.Scope
	materializer                 requestlog.Materializer
	registry                     consumer.TopicRegistry
	stores                       storage.Factory
	projectStatusResolverFactory projectstatus.ResolverFactory
	sourceControl                sourcecontrol.Factory
	topicKey                     consumer.TopicKey
	consumerGroup                string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// _opName is the metric operation name shared by every emit in this file.
const _opName = "record"

// wholeRepositoryProject is the project component of a fact covering the whole
// repository rather than one project within it.
const wholeRepositoryProject = ""

// NewController creates a new record controller.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores storage.Factory,
	materializer requestlog.Materializer,
	projectStatusResolverFactory projectstatus.ResolverFactory,
	sourceControl sourcecontrol.Factory,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	name := string(topicKey) + "_controller"
	return &Controller{
		logger:                       logger.Named(name),
		metricsScope:                 scope.SubScope(name),
		materializer:                 materializer,
		registry:                     registry,
		stores:                       stores,
		projectStatusResolverFactory: projectStatusResolverFactory,
		sourceControl:                sourceControl,
		topicKey:                     topicKey,
		consumerGroup:                consumerGroup,
	}
}

// Process loads the request referenced by the delivery, records its validation
// fact, applies that fact to the caches derived from it, and announces the
// outcome as a hook event. Returns nil to ack (success) or an error to nack
// (retry) / reject (DLQ).
//
// buildsignal stamps the outcome on the request before publishing here, so a
// request without a build outcome is a producer invariant violation rather
// than a state this stage waits for.
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	msg := delivery.Message()

	rec := &stovepipemq.Record{}
	if err := stovepipemq.Unmarshal(msg.Payload, rec); err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "deserialize_errors", 1, metrics.TagsFromContext(ctx)...)
		// Non-retryable: a malformed message will never succeed regardless of retries.
		return fmt.Errorf("failed to deserialize record: %w", err)
	}
	if err := entityqueue.ValidatePayloadQueue(msg, rec.GetQueueName()); err != nil {
		return fmt.Errorf("invalid message identity: %w", err)
	}
	store, err := c.stores.For(storage.Config{QueueName: rec.GetQueueName()})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "storage_resolve_errors", 1, metrics.TagsFromContext(ctx)...)
		// Non-retryable: a missing or unresolvable queue is a malformed message.
		return fmt.Errorf("failed to resolve storage for queue %q: %w", rec.GetQueueName(), err)
	}

	request, err := c.loadRequest(ctx, store, rec.Id)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "storage_errors", 1, metrics.TagsFromContext(ctx)...)
		return err
	}

	// The payload's queue must match the request's authoritative queue; a
	// mismatch is a malformed message. Non-retryable — reject to the DLQ.
	if rec.GetQueueName() != "" && rec.GetQueueName() != request.Queue {
		metrics.NamedCounter(c.metricsScope, _opName, "queue_mismatch", 1, metrics.TagsFromContext(ctx)...)
		return fmt.Errorf("payload queue %q does not match queue %q of request %s", rec.GetQueueName(), request.Queue, request.ID)
	}

	switch request.State {
	case entity.RequestStateSucceeded, entity.RequestStateFailed:
		results, err := c.resolveProjectResults(ctx, request)
		if err != nil {
			return err
		}
		fact, created, err := recordFact(ctx, c.logger, c.metricsScope, store, request, results)
		if err != nil {
			return err
		}
		if err := persistValidationFactRecordedLog(ctx, c.materializer, store, request, fact); err != nil {
			return err
		}
		if err := c.persistProjectFacts(ctx, store, request, results); err != nil {
			return err
		}
		if err := c.applyFactToDerivedCaches(ctx, store, request, fact, created); err != nil {
			return err
		}
		return publishHookEvent(ctx, c.logger, c.metricsScope, c.registry, request, hookevent.NewValidationRepositoryRecorded(request))

	case entity.RequestStateCancelled:
		// A cancelled build decided nothing about the commit, so it establishes
		// no fact. The identity stays unclaimed; the next commit re-validates.
		metrics.NamedCounter(c.metricsScope, _opName, "cancelled", 1, metrics.TagsFromContext(ctx)...)
		return publishHookEvent(ctx, c.logger, c.metricsScope, c.registry, request, hookevent.NewValidationRepositoryCancelled(request))

	case entity.RequestStateSuperseded:
		// Terminal without a build outcome. buildsignal never publishes for a
		// superseded request, so this is unreachable in practice.
		metrics.NamedCounter(c.metricsScope, _opName, "superseded", 1, metrics.TagsFromContext(ctx)...)
		return nil

	default:
		// Non-retryable: buildsignal publishes only after committing the
		// outcome, so a non-terminal request here is a broken invariant that
		// retrying cannot fix.
		metrics.NamedCounter(c.metricsScope, _opName, "invariant_errors", 1, metrics.TagsFromContext(ctx)...)
		return fmt.Errorf("request %s reached record in non-terminal state %q", request.ID, request.State)
	}
}

func (c *Controller) resolveProjectResults(ctx context.Context, request entity.Request) ([]projectstatus.Result, error) {
	resolver, err := c.projectStatusResolverFactory.For(projectstatus.ResolverConfig{QueueName: request.Queue})
	if err != nil {
		return nil, fmt.Errorf("resolve project status resolver for queue %q: %w", request.Queue, err)
	}
	results, err := resolver.Resolve(ctx, request, request.TerminalBuildID)
	if err != nil {
		return nil, fmt.Errorf("resolve project status results for request %q: %w", request.ID, err)
	}

	if err := validateProjectStatusResults(request, results); err != nil {
		return nil, err
	}
	return results, nil
}

func (c *Controller) persistProjectFacts(ctx context.Context, store storage.Storage, request entity.Request, results []projectstatus.Result) error {
	for _, result := range results {
		fact, _, err := recordValidationFact(ctx, c.logger, c.metricsScope, store.GetValidationFactStore(), entity.ValidationFact{
			URI:       request.URI,
			Project:   result.Project,
			Degree:    result.Degree,
			RequestID: request.ID,
			CreatedAt: time.Now().UnixMilli(),
		})
		if err != nil {
			return err
		}

		entry, err := entity.NewProjectFactByDegree(fact)
		if err != nil {
			return err
		}

		if err := store.GetProjectFactByDegreeStore().Create(ctx, entry); err != nil && !errors.Is(err, storage.ErrAlreadyExists) {
			return fmt.Errorf("failed to index project fact for request %q and project %q: %w", request.ID, fact.Project, err)
		}
	}
	return c.persistProjectFactsRecordedLog(ctx, store, request, len(results))
}

func validateProjectStatusResults(request entity.Request, results []projectstatus.Result) error {
	seen := make(map[string]struct{}, len(results))
	for _, result := range results {
		if result.Project == "" {
			return fmt.Errorf("project status result for request %q has an empty project", request.ID)
		}
		if _, ok := seen[result.Project]; ok {
			return fmt.Errorf("project status result for request %q contains duplicate project %q", request.ID, result.Project)
		}
		seen[result.Project] = struct{}{}
		if math.IsNaN(result.Degree) || result.Degree < entity.DegreeGreen || result.Degree > entity.DegreeBroken {
			return fmt.Errorf("project status result for request %q and project %q has invalid degree %v", request.ID, result.Project, result.Degree)
		}
	}
	return nil
}

func (c *Controller) persistProjectFactsRecordedLog(
	ctx context.Context,
	store storage.Storage,
	request entity.Request,
	count int,
) error {
	log := requestlog.NewRequestEventLog(
		request,
		entity.RequestEventProjectFactsRecorded,
		"all",
		map[string]string{requestlog.MetadataKeyProjectFactCount: strconv.Itoa(count)},
	)
	if err := c.materializer.PersistLog(ctx, store, log); err != nil {
		return fmt.Errorf("failed to record project facts for request %s: %w", request.ID, err)
	}
	return nil
}

// applyFactToDerivedCaches follows the persisted fact: a green fact promotes the
// commit when this request holds the queue's bookmark, set by build completion.
// A broken fact skips promotion, and instead reports how long the break it names
// went undetected.
func (c *Controller) applyFactToDerivedCaches(
	ctx context.Context,
	store storage.Storage,
	request entity.Request,
	fact entity.ValidationFact,
	createdFact bool,
) error {
	if !fact.IsGreen() {
		metrics.NamedCounter(c.metricsScope, _opName, "not_green", 1, metrics.TagsFromContext(ctx)...)
		// Only the writer of the fact reports the latency: a redelivery adopts
		// the stored fact instead, and a second sample would count one break
		// twice in the distribution.
		if createdFact {
			c.reportFailureDetectionLatency(ctx, request)
		}
		return nil
	}

	queue, err := store.GetQueueStore().Get(ctx, request.Queue)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "storage_errors", 1, metrics.TagsFromContext(ctx)...)
		return err
	}
	if queue.LastGreenRequestID != "" {
		if _, err := entity.CompareRequestID(request.ID, queue.LastGreenRequestID); err != nil {
			return err
		}
	}
	if queue.LastGreenRequestID != request.ID || queue.LastGreenURI != request.URI {
		// Only the current baseline owner can promote: promoting an older commit
		// would move the ref backwards.
		return nil
	}
	c.emitLastGreenTimestamp(ctx, request)
	return c.promote(ctx, request)
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
	strategyTag := metrics.NewTag("strategy", string(request.BuildStrategy))

	// Only a strategy that validates a delta pins a base commit, so a full build has
	// no baseline to measure from. Its failures are counted rather than timed: absent
	// here is the ordinary case, not a fault.
	if request.BaseURI == "" {
		metrics.NamedCounter(c.metricsScope, _opName, "failure_detection_missing", 1, metrics.TagsFromContext(ctx, strategyTag)...)
		return
	}

	sourceControl, err := c.sourceControl.For(sourcecontrol.Config{QueueName: request.Queue})
	if err != nil {
		c.failureDetectionUnobserved(ctx, request, "resolve_source_control", err)
		return
	}

	info, err := sourceControl.ChangeInfo(ctx, request.BaseURI)
	if err != nil {
		c.failureDetectionUnobserved(ctx, request, "get_change_info", err)
		return
	}

	// SourceControl must report a positive creation timestamp, so a missing one is a
	// broken extension contract rather than a lookup failure. Measuring from 1970
	// would drop a decades-long sample into the distribution.
	if info.CreatedAt <= 0 {
		c.failureDetectionUnobserved(ctx, request, "undated_change", nil)
		return
	}

	// A base dated in the future means the provider's clock disagrees with ours; a
	// negative latency would corrupt the distribution rather than describe it.
	latency := time.Since(time.UnixMilli(info.CreatedAt))
	if latency < 0 {
		c.failureDetectionUnobserved(ctx, request, "future_change", nil)
		return
	}

	metrics.NamedHistogram(c.metricsScope, _opName, "failure_detection_latency", metrics.ChangeAgeBuckets,
		metrics.TagsFromContext(ctx, strategyTag)...,
	).RecordDuration(latency)
}

// failureDetectionUnobserved counts a latency that could not be observed, tagged with
// the step that failed so an unmeasurable failure can be told apart from a broken
// dependency.
func (c *Controller) failureDetectionUnobserved(ctx context.Context, request entity.Request, step string, err error) {
	metrics.NamedCounter(c.metricsScope, _opName, "failure_detection_errors", 1,
		metrics.TagsFromContext(ctx, metrics.NewTag("step", step))...,
	)
	c.logger.Warnw("failed to observe how long the build failure went undetected",
		"queue", request.Queue,
		"base_uri", request.BaseURI,
		"step", step,
		"error", err,
	)
}

// degreeFor maps a request's build outcome onto a whole-repository degree.
// A failed build with project results has a partial degree.
func degreeFor(state entity.RequestState, results []projectstatus.Result) float64 {
	if state == entity.RequestStateSucceeded {
		return entity.DegreeGreen
	}
	if len(results) > 0 {
		return entity.DegreePartial
	}
	return entity.DegreeBroken
}

// emitLastGreenTimestamp emits the creation time of the change the bookmark now
// points at, once that bookmark is durable. Reporting is best-effort so an
// observability failure cannot turn a successful record operation into a retry,
// which is why each cause is counted and logged separately instead of returned.
func (c *Controller) emitLastGreenTimestamp(ctx context.Context, request entity.Request) {
	sourceControl, err := c.sourceControl.For(sourcecontrol.Config{QueueName: request.Queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "last_green_timestamp_resolve_errors", 1, metrics.TagsFromContext(ctx)...)
		c.logger.Warnw("failed to resolve source control to report the last green timestamp",
			"queue", request.Queue,
			"error", err,
		)
		return
	}

	info, err := sourceControl.ChangeInfo(ctx, request.URI)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "last_green_timestamp_errors", 1, metrics.TagsFromContext(ctx)...)
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
		metrics.NamedCounter(c.metricsScope, _opName, "last_green_timestamp_invalid", 1, metrics.TagsFromContext(ctx)...)
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
		metrics.TagsFromContext(ctx)...,
	)
}

// promote points the queue's promotion ref at the request's commit so downstream
// systems can pull the latest green commit by name. Which ref that is — and whether
// the queue has one at all — is source-control configuration, so this stage names
// only the commit.
//
// The ref is a cache of the facts, so it moves only after the
// green fact is durable. Promotion is idempotent, so a redelivery repeats it
// harmlessly. A commit that a rewritten history dropped from the ref cannot be
// promoted by any retry, so that case is counted and skipped rather than failed.
func (c *Controller) promote(ctx context.Context, request entity.Request) error {
	sc, err := c.sourceControl.For(sourcecontrol.Config{QueueName: request.Queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "source_control_errors", 1,
			metrics.TagsFromContext(ctx, metrics.NewTag("stage", "resolve"))...,
		)
		return fmt.Errorf("failed to resolve source control for queue %s: %w", request.Queue, err)
	}

	if err := sc.Promote(ctx, request.URI); err != nil {
		if sourcecontrol.IsNotFound(err) {
			metrics.NamedCounter(c.metricsScope, _opName, "promotions_skipped", 1,
				metrics.TagsFromContext(ctx, metrics.NewTag("reason", "unknown_uri"))...,
			)
			c.logger.Warnw("green commit is no longer on the queue's ref; skipping promotion",
				"queue", request.Queue,
				"request_id", request.ID,
				"uri", request.URI,
			)
			return nil
		}

		metrics.NamedCounter(c.metricsScope, _opName, "source_control_errors", 1,
			metrics.TagsFromContext(ctx, metrics.NewTag("stage", "promote"))...,
		)
		return fmt.Errorf("failed to promote uri %s of queue %s: %w", request.URI, request.Queue, err)
	}

	metrics.NamedCounter(c.metricsScope, _opName, "promotions", 1, metrics.TagsFromContext(ctx)...)
	c.logger.Infow("promoted green commit",
		"queue", request.Queue,
		"request_id", request.ID,
		"uri", request.URI,
	)
	return nil
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
