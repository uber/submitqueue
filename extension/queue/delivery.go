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

package queue

//go:generate mockgen -source=delivery.go -destination=mock/delivery_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/entity/queue"
)

// Delivery represents a message delivered by a Subscriber.
// Provides access to the message and methods to acknowledge or reject it.
//
// Implementations must be safe for concurrent Message() calls.
// Ack/Nack/ExtendVisibilityTimeout should not be called concurrently on the same instance.
type Delivery interface {
	// Message returns the delivered message.
	Message() queue.Message

	// Ack acknowledges successful processing of the message.
	// The message will be removed from the queue and not redelivered.
	Ack(ctx context.Context) error

	// Nack negatively acknowledges the message, indicating processing failure.
	// The message will be requeued for redelivery after requeueAfterMillis.
	// If requeueAfterMillis is 0, the message is requeued immediately.
	Nack(ctx context.Context, requeueAfterMillis int64) error

	// Reject moves the message to the dead letter queue.
	// Use for poison pill messages that should never be retried.
	// reason is stored as last_error in the DLQ for debugging.
	// If DLQ is not configured, the message is acked (removed from queue).
	Reject(ctx context.Context, reason string) error

	// ExtendVisibilityTimeout extends the time before this message becomes
	// visible to other consumers. Use when processing takes longer than expected.
	ExtendVisibilityTimeout(ctx context.Context, durationMillis int64) error

	// DeliveryID returns a backend-specific identifier for this delivery.
	DeliveryID() string

	// Attempt returns how many times this message has been delivered.
	// Starts at 1 for first delivery.
	Attempt() int

	// ReceivedAt returns when this delivery was received (Unix milliseconds).
	ReceivedAt() int64

	// Metadata returns backend-specific delivery metadata.
	Metadata() map[string]string
}
