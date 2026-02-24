package queue

//go:generate mockgen -source=publisher.go -destination=mock/publisher.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/entity/queue"
)

// Publisher publishes messages to topics.
// Implementations must be thread-safe.
type Publisher interface {
	// Publish sends a message to the specified topic.
	Publish(ctx context.Context, topic string, message queue.Message) error

	// Close gracefully shuts down the publisher, flushing pending messages.
	Close() error
}
