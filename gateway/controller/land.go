package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/extension/counter"
	"github.com/uber/submitqueue/extension/storage"
	pb "github.com/uber/submitqueue/gateway/protopb"
	"go.uber.org/zap"
)

// LandController handles land business logic for the gateway
type LandController struct {
	logger       *zap.Logger
	metricsScope tally.Scope
	store        storage.Storage
	counter      counter.Counter
}

// NewLandController creates a new instance of the gateway land controller
func NewLandController(logger *zap.Logger, scope tally.Scope, store storage.Storage, counter counter.Counter) *LandController {
	return &LandController{
		logger:       logger,
		metricsScope: scope,
		store:        store,
		counter:      counter,
	}
}

// Land handles the land request and returns a response
func (c *LandController) Land(ctx context.Context, req *pb.LandRequest) (*pb.LandResponse, error) {
	start := time.Now()
	defer func() {
		c.metricsScope.Timer("land_request_latency").Record(time.Since(start))
	}()

	c.metricsScope.Counter("land_request_count").Inc(1)

	change := entity.Change{
		Source: req.Change.GetSource(),
		IDs:    req.Change.GetIds(),
	}

	// TODO: validate that queue is configured. Return error if not.
	queue := req.Queue

	// TODO: pass default queue land strategy to resolver function to process a default.
	strategy, err := resolveRequestLandStrategy(req.Strategy)
	if err != nil {
		return nil, fmt.Errorf("LandController failed to map strategy for queue=%s: %w", req.Queue, err)
	}

	// Generate a globally unique request ID for the land request.
	seq, err := c.counter.Next(ctx, "request/"+queue)
	if err != nil {
		return nil, fmt.Errorf("LandController failed to generate request ID for queue=%s: %w", queue, err)
	}

	request := entity.Request{
		ID:           fmt.Sprintf("%s/%d", queue, seq),
		Queue:        queue,
		Change:       change,
		LandStrategy: strategy,
		State:        entity.RequestStateNew,
		Version:      1,
	}

	if err := c.store.GetRequestStore().Create(ctx, request); err != nil {
		return nil, fmt.Errorf("LandController failed to create request for queue=%s: %w", req.Queue, err)
	}

	c.logger.Debug("land request received",
		zap.String("queue", req.Queue),
		zap.String("sqid", request.ID),
	)

	return &pb.LandResponse{
		Sqid: request.ID,
	}, nil
}

// protoStrategyToEntity maps a proto Strategy enum to the entity RequestLandStrategy.
func resolveRequestLandStrategy(s pb.Strategy) (entity.RequestLandStrategy, error) {
	switch s {
	case pb.Strategy_DEFAULT:
		// TODO: resolve default strategy based on queue configuration
		return entity.RequestLandStrategyRebase, nil
	case pb.Strategy_REBASE:
		return entity.RequestLandStrategyRebase, nil
	case pb.Strategy_SQUASH_REBASE:
		return entity.RequestLandStrategySquashRebase, nil
	case pb.Strategy_MERGE:
		return entity.RequestLandStrategyMerge, nil
	default:
		return entity.RequestLandStrategyUnknown, fmt.Errorf("unknown proto strategy: %v", s)
	}
}
