package consumer

//go:generate mockgen -source=controller.go -destination=mock/controller_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/entity/queue"
	extqueue "github.com/uber/submitqueue/extension/queue"
)

// Delivery is the consumer package's view of a queue delivery.
// It exists to hide Ack/Nack from controllers — the Consumer framework handles those
// automatically based on the error returned from Process(). Controllers only see
// message data, metadata, and ExtendVisibilityTimeout (a business-level concern for
// long-running processing).
//
// To signal outcome from Process():
//   - Return nil to ack the message (success).
//   - Return an error to nack the message for retry.
//   - Return NonRetryableError to ack a poison pill message (removes it from the queue).
type Delivery interface {
	// Message returns the delivered message.
	Message() queue.Message

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

// deliveryWrapper wraps extension/queue.Delivery and exposes only the safe subset of methods.
// Hides Ack/Nack from controllers - Consumer handles those automatically.
type deliveryWrapper struct {
	delivery extqueue.Delivery
}

func (d *deliveryWrapper) Message() queue.Message {
	return d.delivery.Message()
}

func (d *deliveryWrapper) ExtendVisibilityTimeout(ctx context.Context, durationMillis int64) error {
	return d.delivery.ExtendVisibilityTimeout(ctx, durationMillis)
}

func (d *deliveryWrapper) DeliveryID() string {
	return d.delivery.DeliveryID()
}

func (d *deliveryWrapper) Attempt() int {
	return d.delivery.Attempt()
}

func (d *deliveryWrapper) ReceivedAt() int64 {
	return d.delivery.ReceivedAt()
}

func (d *deliveryWrapper) Metadata() map[string]string {
	return d.delivery.Metadata()
}

// Controller processes queue deliveries. Controllers contain business logic and are registered with the Consumer.
// The Controller interface enables clean separation of concerns:
// - Controller focuses on business logic (deserialize, process, return error status)
// - Consumer handles infrastructure (subscription, ack/nack, metrics, lifecycle)
type Controller interface {
	// Process processes a delivery. Controller receives consumer.Delivery (not extension/queue.Delivery)
	// which prevents direct Ack/Nack calls - Consumer handles those automatically.
	// Return nil to ack the message (success), error to nack and retry,
	// or NonRetryableError to ack a poison pill message.
	Process(ctx context.Context, delivery Delivery) error

	// Name returns the controller name for logging and metrics.
	Name() string

	// TopicKey returns the topic key this controller subscribes to.
	TopicKey() TopicKey

	// ConsumerGroup returns the consumer group for offset tracking.
	// Multiple controllers can share a consumer group to load-balance across workers.
	// Different consumer groups consume independently.
	ConsumerGroup() string
}
