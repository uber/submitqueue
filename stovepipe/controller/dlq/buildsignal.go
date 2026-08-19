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
	"errors"
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/platform/publish"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

// _buildSignalOpName is the metric operation name shared by every emit in this file.
const _buildSignalOpName = "buildsignal_dlq"

// buildSignalController is the DLQ reconciler for the buildsignal stage. The
// payload names a build, not a request, so it takes one more step than the
// process reconciler: read the build to get its RequestID, then fail that
// request via failRequest.
//
// This DLQ is the one that matters most. A request only reaches buildsignal
// after process admitted it, so it holds one of the queue's in_flight_count
// build slots, and buildsignal's terminal path is the only thing that gives that
// slot back. Once a poll message dead-letters — a Status call that stayed broken
// through every retry, an unknown build id, a storage write that kept failing —
// nothing else in the pipeline will look at that build again. Without this
// reconciler the request stays processing for good and the slot is never
// returned, so the queue loses one slot per incident until it has none left and
// stops admitting work.
//
// The Build row keeps whatever non-terminal status the runner last reported.
// There is nothing useful to fix: record decides greenness from Request.State,
// not Build.Status, and writing a terminal status here would claim we saw an
// outcome we never saw.
type buildSignalController struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	stores        storage.Factory
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify buildSignalController implements consumer.Controller at compile time.
var _ consumer.Controller = (*buildSignalController)(nil)

// NewDLQBuildSignalController creates a DLQ controller for the buildsignal stage's
// dead-letter topic. topicKey is typically
// dlq.TopicKey(stovepipemq.TopicKeyBuildSignal).
func NewDLQBuildSignalController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores storage.Factory,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) consumer.Controller {
	name := string(topicKey) + "_controller"
	return &buildSignalController{
		logger:        logger.Named(name),
		metricsScope:  scope.SubScope(name),
		stores:        stores,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process reconciles a single DLQ delivery for the buildsignal topic. Returns nil
// to ack (success) or an error to nack (retry) — pair this controller only with a
// consumer wired with errs.AlwaysRetryableProcessor so a transient reconcile
// failure retries instead of dead-lettering the DLQ message itself.
func (c *buildSignalController) Process(ctx context.Context, delivery consumer.Delivery) error {
	msg := delivery.Message()

	sig := &stovepipemq.BuildSignal{}
	if err := stovepipemq.Unmarshal(msg.Payload, sig); err != nil {
		metrics.NamedCounter(c.metricsScope, _buildSignalOpName, "deserialize_errors", 1)
		// Retried rather than acked, for the same deployment-skew reason the
		// process reconciler gives: a newer producer's payload decodes fine once
		// the rollout finishes, and acking here would skip the slot release
		// without saying so.
		return fmt.Errorf("failed to decode dlq payload: %w", err)
	}
	if sig.Id == "" {
		metrics.NamedCounter(c.metricsScope, _buildSignalOpName, "empty_id_errors", 1)
		return fmt.Errorf("dlq payload decoded to empty build id")
	}

	store, err := c.stores.For(storage.Config{QueueName: sig.GetQueueName()})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _buildSignalOpName, "storage_resolve_errors", 1)
		// Non-retryable: a missing or unresolvable queue is a malformed message.
		return fmt.Errorf("failed to resolve storage for queue %q: %w", sig.GetQueueName(), err)
	}

	dmeta := delivery.Metadata()
	c.logger.Warnw("dlq message received",
		"build_id", sig.Id,
		"attempt", delivery.Attempt(),
		"dlq_original_topic", dmeta["dlq.original_topic"],
		"dlq_failure_count", dmeta["dlq.failure_count"],
		"dlq_last_error", dmeta["dlq.last_error"],
	)

	build, err := store.GetBuildStore().Get(ctx, sig.Id)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			// The build row was never written — a crash between Trigger and
			// Create. There is no request to recover from this payload; the build
			// stage's own DLQ handles the request that triggered it.
			c.logger.Warnw("dlq reconcile: build not found, skipping", "build_id", sig.Id)
			metrics.NamedCounter(c.metricsScope, _buildSignalOpName, "build_not_found", 1)
			return nil
		}
		metrics.NamedCounter(c.metricsScope, _buildSignalOpName, "build_store_errors", 1)
		return fmt.Errorf("failed to get build %s: %w", sig.Id, err)
	}

	if build.RequestID == "" {
		// Defensive: a build with no request has nothing to reconcile and no slot
		// to release. Ack it so the DLQ does not grow forever.
		c.logger.Errorw("dlq reconcile: build has empty request id, skipping", "build_id", sig.Id)
		metrics.NamedCounter(c.metricsScope, _buildSignalOpName, "build_missing_request", 1)
		return nil
	}

	request, found, err := loadRequest(ctx, store, c.logger, build.RequestID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _buildSignalOpName, "reconcile_errors", 1)
		return err
	}
	if !found {
		return nil
	}

	// The primary controller commits the outcome before publishing record work.
	// A failure in that handoff must replay record rather than treating the
	// already-terminal request as fully reconciled.
	if request.State.HasBuildOutcome() {
		if err := c.publishRecord(ctx, request.ID, request.Queue); err != nil {
			metrics.NamedCounter(c.metricsScope, _buildSignalOpName, "record_publish_errors", 1)
			return fmt.Errorf("failed to publish record for request %s: %w", request.ID, err)
		}
		metrics.NamedCounter(c.metricsScope, _buildSignalOpName, "record_republished", 1)
		return nil
	}

	if err := failLoadedRequest(ctx, store, c.logger, request); err != nil {
		metrics.NamedCounter(c.metricsScope, _buildSignalOpName, "reconcile_errors", 1)
		return err
	}

	metrics.NamedCounter(c.metricsScope, _buildSignalOpName, "reconciled", 1)
	return nil
}

// publishRecord resumes the primary controller's interrupted terminal handoff.
func (c *buildSignalController) publishRecord(ctx context.Context, requestID, queue string) error {
	payload, err := stovepipemq.Marshal(&stovepipemq.Record{Id: requestID, QueueName: queue})
	if err != nil {
		return fmt.Errorf("failed to serialize record: %w", err)
	}
	return publish.Message(ctx, c.registry, stovepipemq.TopicKeyRecord, publish.IntentID(requestID), payload, requestID)
}

// Name returns the controller name for logging and metrics.
func (c *buildSignalController) Name() string {
	return string(c.topicKey)
}

// TopicKey returns the topic key this controller subscribes to.
func (c *buildSignalController) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *buildSignalController) ConsumerGroup() string {
	return c.consumerGroup
}
