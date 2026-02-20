package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/entities"
	"github.com/uber/submitqueue/extensions/counter"
	"github.com/uber/submitqueue/extensions/storage"
	pb "github.com/uber/submitqueue/gateway/protopb"
	"go.uber.org/zap"
)

// LandController handles land business logic for the gateway
type LandController struct {
	logger       *zap.Logger
	metricsScope tally.Scope
	storeFactory storage.StoreFactory
	counter      counter.Counter
}

// NewLandController creates a new instance of the gateway land controller
func NewLandController(logger *zap.Logger, scope tally.Scope, storeFactory storage.StoreFactory, counter counter.Counter) *LandController {
	return &LandController{
		logger:       logger,
		metricsScope: scope,
		storeFactory: storeFactory,
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

	change := entities.Change{
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

	request := entities.Request{
		ID:           fmt.Sprintf("%s/%d", queue, seq),
		Queue:        queue,
		Change:       change,
		LandStrategy: strategy,
		State:        entities.RequestStateNew,
		Version:      1,
	}

	if err := c.storeFactory.GetRequestStore().Create(ctx, request); err != nil {
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
func resolveRequestLandStrategy(s pb.Strategy) (entities.RequestLandStrategy, error) {
	switch s {
	case pb.Strategy_DEFAULT:
		// TODO: resolve default strategy based on queue configuration
		return entities.RequestLandStrategyRebase, nil
	case pb.Strategy_REBASE:
		return entities.RequestLandStrategyRebase, nil
	case pb.Strategy_SQUASH_REBASE:
		return entities.RequestLandStrategySquashRebase, nil
	case pb.Strategy_MERGE:
		return entities.RequestLandStrategyMerge, nil
	default:
		return entities.RequestLandStrategyUnknown, fmt.Errorf("unknown proto strategy: %v", s)
	}
}
