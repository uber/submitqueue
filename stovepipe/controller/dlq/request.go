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
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// RequestRef is the request a request-scoped DLQ payload refers to.
type RequestRef struct {
	// ID is the request id to reconcile. Format: "request/<queue>/<counter>".
	ID string
	// Queue is the name of the queue owning the request, used to resolve its store.
	// Empty on payloads written before the field existed.
	Queue string
}

// RequestRefDecoder recovers the referenced request from a DLQ message's raw bytes.
// The request-scoped stages carry a distinct payload type each, so the wiring injects
// the decoder matching the topic being drained.
type RequestRefDecoder func(payload []byte) (RequestRef, error)

// requestRefPayload is the shape the request-scoped pipeline payloads share: the
// affected request's id plus its queue. ProcessRequest, BuildRequest, and Record all
// satisfy it, which is what lets one reconciler serve all three of their DLQs.
type requestRefPayload interface {
	proto.Message
	GetId() string
	GetQueueName() string
}

// DecodeProcessRequest decodes the payload the process topic carries.
func DecodeProcessRequest(payload []byte) (RequestRef, error) {
	return decodeRequestRef(payload, &stovepipemq.ProcessRequest{})
}

// DecodeBuildRequest decodes the payload the build topic carries.
func DecodeBuildRequest(payload []byte) (RequestRef, error) {
	return decodeRequestRef(payload, &stovepipemq.BuildRequest{})
}

// DecodeRecord decodes the payload the record topic carries.
func DecodeRecord(payload []byte) (RequestRef, error) {
	return decodeRequestRef(payload, &stovepipemq.Record{})
}

// decodeRequestRef decodes payload into msg and reads the request identity off it.
// The three payload types are separate messages that happen to carry identical
// fields, so decoding into the right one is all that varies — and it must stay the
// right one: contracts evolve per stage, so treating them as interchangeable would
// make one stage's added field silently misread at another.
func decodeRequestRef[T requestRefPayload](payload []byte, msg T) (RequestRef, error) {
	if err := stovepipemq.Unmarshal(payload, msg); err != nil {
		return RequestRef{}, err
	}
	return RequestRef{ID: msg.GetId(), Queue: msg.GetQueueName()}, nil
}

// requestController is the DLQ reconciler for the request-scoped stages: process,
// build, and record. It is registered once per stage against that stage's DLQ topic
// (see TopicKey) with the decoder matching the stage's payload, and on each delivery
// decodes the request id, then drives the referenced request to a terminal failed
// state via failRequest.
//
// How much each stage's reconciliation actually has to do differs, even though the
// steps are identical — a request dead-lettering at build is still processing and
// holding a slot, while one dead-lettering at record has already been driven
// terminal by buildsignal. See README.md for the per-stage reasoning.
type requestController struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	stores        storage.Factory
	decode        RequestRefDecoder
	opName        string
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify requestController implements consumer.Controller at compile time.
var _ consumer.Controller = (*requestController)(nil)

// NewRequestController creates a DLQ controller for a request-scoped stage's
// dead-letter topic. topicKey is typically dlq.TopicKey of the primary topic, and
// decode must match that topic's payload shape.
func NewRequestController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores storage.Factory,
	decode RequestRefDecoder,
	topicKey consumer.TopicKey,
	consumerGroup string,
) consumer.Controller {
	name := string(topicKey) + "_controller"
	return &requestController{
		logger:        logger.Named(name),
		metricsScope:  scope.SubScope(name),
		stores:        stores,
		decode:        decode,
		opName:        string(topicKey),
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process reconciles a single DLQ delivery for a request-scoped topic. Returns nil to
// ack (success) or an error to nack (retry) — pair this controller only with a consumer
// wired with errs.AlwaysRetryableProcessor so a transient reconcile failure retries
// instead of dead-lettering the DLQ message itself.
func (c *requestController) Process(ctx context.Context, delivery consumer.Delivery) error {
	msg := delivery.Message()

	ref, err := c.decode(msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, c.opName, "deserialize_errors", 1)
		// Decoding the same bytes normally fails deterministically, but this error is
		// still retried: the DLQ consumer's AlwaysRetryableProcessor (see Process doc)
		// classifies every error as retryable. That is deliberate — the recoverable
		// cause is deployment skew, where a newer producer's payload shape reaches a
		// not-yet-upgraded consumer and decodes fine once the rollout completes. A
		// genuinely malformed payload exhausts the DLQ subscription's MaxAttempts
		// backstop and is dropped by the subscriber with a warning log; acking it here
		// instead would skip reconciliation silently and leave the referenced request
		// non-terminal.
		return fmt.Errorf("failed to decode dlq payload: %w", err)
	}
	if ref.ID == "" {
		metrics.NamedCounter(c.metricsScope, c.opName, "empty_id_errors", 1)
		return fmt.Errorf("dlq payload decoded to empty request id")
	}

	store, err := c.stores.For(storage.Config{QueueName: ref.Queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, c.opName, "storage_resolve_errors", 1)
		// Non-retryable: a missing or unresolvable queue is a malformed message.
		return fmt.Errorf("failed to resolve storage for queue %q: %w", ref.Queue, err)
	}

	dmeta := delivery.Metadata()
	c.logger.Warnw("dlq message received",
		"request_id", ref.ID,
		"attempt", delivery.Attempt(),
		"dlq_original_topic", dmeta["dlq.original_topic"],
		"dlq_failure_count", dmeta["dlq.failure_count"],
		"dlq_last_error", dmeta["dlq.last_error"],
	)

	if err := failRequest(ctx, store, c.logger, ref.ID); err != nil {
		metrics.NamedCounter(c.metricsScope, c.opName, "reconcile_errors", 1)
		return err
	}

	return nil
}

// Name returns the controller name for logging and metrics.
func (c *requestController) Name() string {
	return string(c.topicKey)
}

// TopicKey returns the topic key this controller subscribes to.
func (c *requestController) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *requestController) ConsumerGroup() string {
	return c.consumerGroup
}
