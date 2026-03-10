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

//go:generate mockgen -source=subscriber.go -destination=mock/subscriber_mock.go -package=mock

import (
	"context"
)

// Subscriber consumes messages from topics.
// Implementations must be thread-safe.
type Subscriber interface {
	// Subscribe starts consuming messages from the specified topic with the given config.
	// Returns a channel of Delivery instances and an error if subscription fails.
	//
	// Each subscription can have its own configuration for polling, batching,
	// retries, and dead letter queue behavior.
	//
	// The channel is closed when the subscriber is closed or context is cancelled.
	// Implementations should handle infrastructure errors internally (e.g., reconnect).
	//
	// Each Delivery provides the message and methods to acknowledge or reject it.
	// Consumers should call delivery.Ack() or delivery.Nack() for each delivery.
	Subscribe(ctx context.Context, topic string, config SubscriptionConfig) (<-chan Delivery, error)

	// Close gracefully shuts down the subscriber.
	// All delivery channels will be closed.
	// Idempotent - safe to call multiple times.
	Close() error
}
