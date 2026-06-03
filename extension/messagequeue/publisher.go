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

//go:generate mockgen -source=publisher.go -destination=mock/publisher_mock.go -package=mock

import (
	"context"

	entityqueue "github.com/uber/submitqueue/entity/messagequeue"
)

// Publisher publishes messages to topics.
// Implementations must be thread-safe.
type Publisher interface {
	// Publish sends a message to the specified topic.
	Publish(ctx context.Context, topic string, message entityqueue.Message) error

	// PublishAfter sends a message that becomes visible to subscribers only
	// after delayMs from now. It is a fresh publish — not a redelivery — so
	// it does not consume a delivery_state retry slot. delayMs <= 0 is
	// equivalent to Publish.
	//
	// Use for "postpone this work" semantics (e.g. spacing out repeated
	// poll cycles for a single key). Use Nack with a delay for "this
	// delivery failed, try again" — the two signals stay separate so
	// retry_count and DLQ behaviour remain meaningful.
	PublishAfter(ctx context.Context, topic string, message entityqueue.Message, delayMs int64) error

	// Close gracefully shuts down the publisher, flushing pending messages.
	Close() error
}
