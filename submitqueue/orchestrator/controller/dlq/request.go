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
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// RequestIDDecoder extracts the affected request ID from the raw payload bytes
// of a DLQ message. Different primary topics carry different payload shapes
// (LandRequest on start, CancelRequest on cancel, RequestID on validate /
// batch), so the caller injects the right decoder for the topic being
// reconciled. Returning an empty ID is treated as a decode failure.
type RequestIDDecoder func(payload []byte) (string, error)

// DecodeLandRequestID extracts the request ID from a LandRequest payload
// (the shape used by the start topic).
func DecodeLandRequestID(payload []byte) (string, error) {
	lr, err := entity.LandRequestFromBytes(payload)
	if err != nil {
		return "", err
	}
	return lr.ID, nil
}

// DecodeCancelRequestID extracts the request ID from a CancelRequest payload
// (the shape used by the cancel topic).
func DecodeCancelRequestID(payload []byte) (string, error) {
	cr, err := entity.CancelRequestFromBytes(payload)
	if err != nil {
		return "", err
	}
	return cr.ID, nil
}

// DecodeRequestID extracts the request ID from a RequestID payload (the shape
// used by the validate and batch topics).
func DecodeRequestID(payload []byte) (string, error) {
	rid, err := entity.RequestIDFromBytes(payload)
	if err != nil {
		return "", err
	}
	return rid.ID, nil
}

// requestController is the DLQ reconciler for request-scoped pipeline stages.
// It is registered once per primary request-scoped topic (start, cancel,
// validate, batch) with the matching decoder. On each delivery it decodes the
// request ID and transitions the request to RequestStateError if it is not
// already halted.
type requestController struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	store         storage.Storage
	registry      consumer.TopicRegistry
	decode        RequestIDDecoder
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify requestController implements consumer.Controller at compile time.
var _ consumer.Controller = (*requestController)(nil)

// NewDLQRequestController builds a DLQ controller for a request-scoped topic.
// topicKey must be the DLQ topic key (typically TopicKey(primary)); decode
// must match the payload shape of the primary topic this DLQ drains.
func NewDLQRequestController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	store storage.Storage,
	registry consumer.TopicRegistry,
	decode RequestIDDecoder,
	topicKey consumer.TopicKey,
	consumerGroup string,
) consumer.Controller {
	name := string(topicKey) + "_controller"
	return &requestController{
		logger:        logger.Named(name),
		metricsScope:  scope.SubScope(name),
		store:         store,
		registry:      registry,
		decode:        decode,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process reconciles a single DLQ delivery for a request-scoped topic.
func (c *requestController) Process(ctx context.Context, delivery consumer.Delivery) error {
	const opName = "process"

	msg := delivery.Message()

	requestID, err := c.decode(msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		// Malformed DLQ payload is non-retryable: a re-delivery will decode the
		// same bytes and fail the same way. The framework will route this DLQ
		// message to its own DLQ if one is configured; otherwise the message is
		// acked and dropped after the error is logged.
		return fmt.Errorf("failed to decode dlq payload: %w", err)
	}
	if requestID == "" {
		metrics.NamedCounter(c.metricsScope, opName, "empty_id_errors", 1)
		return fmt.Errorf("dlq payload decoded to empty request id")
	}

	dmeta := delivery.Metadata()
	c.logger.Warnw("dlq message received",
		"request_id", requestID,
		"attempt", delivery.Attempt(),
		"dlq_original_topic", dmeta["dlq.original_topic"],
		"dlq_failure_count", dmeta["dlq.failure_count"],
		"dlq_last_error", dmeta["dlq.last_error"],
	)

	if err := failRequest(ctx, c.store, c.registry, c.logger, requestID, dmeta["dlq.last_error"]); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "reconcile_errors", 1)
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
