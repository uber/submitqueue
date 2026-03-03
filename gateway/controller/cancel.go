package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/entity/queue"
	extqueue "github.com/uber/submitqueue/extension/queue"
	pb "github.com/uber/submitqueue/gateway/protopb"
	"go.uber.org/zap"
)

// CancelController handles cancel business logic for the gateway
type CancelController struct {
	logger       *zap.SugaredLogger
	metricsScope tally.Scope
	publisher    extqueue.Publisher
	topic        string // Topic to publish cancel requests to (e.g., "cancel")
}

// NewCancelController creates a new instance of the gateway cancel controller.
// topic: the queue topic to publish cancel requests to (e.g., "cancel")
func NewCancelController(logger *zap.SugaredLogger, scope tally.Scope, publisher extqueue.Publisher, topic string) *CancelController {
	return &CancelController{
		logger:       logger,
		metricsScope: scope,
		publisher:    publisher,
		topic:        topic,
	}
}

// Cancel handles the cancel request and returns a response.
// It publishes the sqid to the cancel topic for async processing.
// No validation is performed — the orchestrator handles that.
func (c *CancelController) Cancel(ctx context.Context, req *pb.CancelRequest) (*pb.CancelResponse, error) {
	start := time.Now()
	defer func() {
		c.metricsScope.Timer("cancel_request_latency").Record(time.Since(start))
	}()

	c.metricsScope.Counter("cancel_request_count").Inc(1)

	sqid := req.Sqid

	// TODO: Insert the request to the event store

	if err := c.publishToQueue(ctx, sqid); err != nil {
		c.logger.Errorw("failed to publish cancel request to queue",
			"sqid", sqid,
			"error", err,
		)
		return nil, fmt.Errorf("CancelController failed to publish cancel request to queue: %w", err)
	}

	c.logger.Infow("cancel request published to queue",
		"sqid", sqid,
		"topic", c.topic,
	)
	c.metricsScope.Counter("cancel_publish_success").Inc(1)

	return &pb.CancelResponse{
		Sqid: sqid,
	}, nil
}

// publishToQueue publishes a cancel request to the cancel queue for async processing.
func (c *CancelController) publishToQueue(ctx context.Context, sqid string) error {
	payload := []byte(sqid)

	// Create queue message
	// - Message ID: sqid for idempotency
	// - Payload: sqid as bytes
	// - Partition key: sqid (ensures ordering per request)
	msg := queue.NewMessage(sqid, payload, sqid, nil)

	if err := c.publisher.Publish(ctx, c.topic, msg); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}
