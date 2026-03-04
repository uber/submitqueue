package controller

import (
	"context"
	"fmt"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/metrics"
	"github.com/uber/submitqueue/entity"
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
func (c *CancelController) Cancel(ctx context.Context, req *pb.CancelRequest) (resp *pb.CancelResponse, retErr error) {
	op := metrics.Begin(c.metricsScope, "cancel")
	defer func() { op.Complete(retErr) }()

	sqid := req.Sqid

	if sqid == "" {
		return &pb.CancelResponse{
			Error: &pb.Error{Message: "sqid is required"},
		}, nil
	}

	// TODO -
	// Look up event store to see if sqid exists -
	//  - if found and request is already in a terminal state; return cancellation failed with appropriate
	//    error message.
	//  - if found and request is not in a terminal state; publish a cancel entity onto the cancel topic
	//    to be picked up by the orchestrator.
	//  - if not found; publish a cancel entity onto the cancel topic
	//    to be picked up by the orchestrator.
	cancel := entity.Cancel{
		Sqid: sqid,
	}

	if err := c.publishToQueue(ctx, cancel); err != nil {
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

	return &pb.CancelResponse{
		Sqid:          sqid,
		CurrentStatus: pb.RequestStatus_CANCELLATION_ACCEPTED,
	}, nil
}

// publishToQueue publishes a cancel entity to the cancel queue for async processing.
func (c *CancelController) publishToQueue(ctx context.Context, cancel entity.Cancel) error {
	payload, err := cancel.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize cancel entity: %w", err)
	}

	// Create queue message
	// - Message ID: sqid for idempotency
	// - Payload: serialized Cancel entity
	// - Partition key: empty (no inherent ordering for cancel requests)
	msg := queue.NewMessage(cancel.Sqid, payload, "", nil)

	if err := c.publisher.Publish(ctx, c.topic, msg); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}
