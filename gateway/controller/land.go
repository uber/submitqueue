package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/uber-go/tally/v4"
	pb "github.com/uber/submitqueue/gateway/protopb"
	"go.uber.org/zap"
)

// LandController handles land business logic for the gateway
type LandController struct {
	logger       *zap.Logger
	metricsScope tally.Scope
}

// NewLandController creates a new instance of the gateway land controller
func NewLandController(logger *zap.Logger, scope tally.Scope) *LandController {
	return &LandController{
		logger:       logger,
		metricsScope: scope,
	}
}

// Land handles the land request and returns a response
func (c *LandController) Land(ctx context.Context, req *pb.LandRequest) (*pb.LandResponse, error) {
	start := time.Now()
	defer func() {
		c.metricsScope.Timer("land_request_latency").Record(time.Since(start))
	}()

	c.metricsScope.Counter("land_request_count").Inc(1)

	// TODO: Implement proper SQID generation and send the request to the appropriate queue. So far unix time to make it sequential.
	sqid := fmt.Sprintf("%d", time.Now().Unix())

	c.logger.Debug("land request received",
		zap.String("queue", req.Queue),
		zap.String("sqid", sqid),
	)

	return &pb.LandResponse{
		Sqid: sqid,
	}, nil
}
