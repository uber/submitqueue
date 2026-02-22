package queue

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
