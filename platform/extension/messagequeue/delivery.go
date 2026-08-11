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

package messagequeue

//go:generate mockgen -source=delivery.go -destination=mock/delivery_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/platform/base/failure"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
)

// Delivery represents a message delivered by a Subscriber.
// Provides access to the message and methods to acknowledge or reject it.
//
// Implementations must be safe for concurrent Message() calls.
// Ack/Nack/ExtendVisibilityTimeout should not be called concurrently on the same instance.
type Delivery interface {
	// Message returns the delivered message.
	Message() entityqueue.Message

	// Ack acknowledges successful processing of the message.
	// The message will be removed from the queue and not redelivered.
	Ack(ctx context.Context) error

	// Nack negatively acknowledges the message, indicating processing failure.
	// The message is requeued for redelivery immediately; the visibility
	// timeout is what spaces retries (a crash or missed ack redelivers on the
	// same schedule). The redelivery counts toward the failure budget.
	//
	// f describes the failure. It is carried so that the redelivery which
	// finally exhausts the budget can dead-letter with the reason that caused
	// it, rather than with a generic one — a nack whose reason is dropped
	// leaves the eventual dead letter unable to say what went wrong.
	Nack(ctx context.Context, f failure.Failure) error

	// Postpone finishes this delivery as "processed successfully, redeliver
	// later": the message becomes invisible for delayMs and acts as a barrier —
	// its partition is not consumed past it until it redelivers, in order.
	// Unlike Nack, the redelivery does not count against the failure budget
	// (retry limit / DLQ); postponing resets the failure streak.
	// Postpone is terminal for this delivery, like Ack/Nack/Reject.
	Postpone(ctx context.Context, delayMs int64) error

	// Reject moves the message to the dead letter entityqueue.
	// Use for poison pill messages that should never be retried.
	// f is recorded with the dead-lettered message for diagnosis and is what
	// Failure returns when it is redelivered from the DLQ.
	// If DLQ is not configured, the message is acked (removed from queue).
	Reject(ctx context.Context, f failure.Failure) error

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

	// Failure returns why this message was dead-lettered, and whether it was
	// dead-lettered at all. It reports false for a message delivered from its
	// original topic, so a DLQ consumer can distinguish "no failure recorded"
	// from a failure that recorded nothing.
	Failure() (failure.Failure, bool)
}
