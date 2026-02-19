package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/entities"
	"github.com/uber/submitqueue/extensions/storage"
	pb "github.com/uber/submitqueue/gateway/protopb"
	"go.uber.org/zap"
)

// LandController handles land business logic for the gateway
type LandController struct {
	logger       *zap.Logger
	metricsScope tally.Scope
	storeFactory storage.StoreFactory
}

// NewLandController creates a new instance of the gateway land controller
func NewLandController(logger *zap.Logger, scope tally.Scope, storeFactory storage.StoreFactory) *LandController {
	return &LandController{
		logger:       logger,
		metricsScope: scope,
		storeFactory: storeFactory,
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
	strategy := entities.RequestLandStrategy(int(req.Strategy))

	request, err := c.storeFactory.GetRequestStore().Create(ctx, req.Queue, change, strategy, entities.RequestStateNew)
	if err != nil {
		return nil, fmt.Errorf("LandController failed to create request for queue=%s: %w", req.Queue, err)
	}

	sqid := request.GetID()

	c.logger.Debug("land request received",
		zap.String("queue", req.Queue),
		zap.String("sqid", sqid),
	)

	return &pb.LandResponse{
		Sqid: sqid,
	}, nil
}
