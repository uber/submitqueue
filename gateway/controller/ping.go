package controller

import (
	"context"
	"os"
	"time"

	"github.com/uber-go/tally/v4"
	pb "github.com/uber/submitqueue/gateway/protopb"
	"go.uber.org/zap"
)

// PingController handles ping business logic for the gateway
type PingController struct {
	logger       *zap.Logger
	metricsScope tally.Scope
}

// NewPingController creates a new instance of the gateway ping controller
func NewPingController(logger *zap.Logger, scope tally.Scope) *PingController {
	if logger == nil {
		logger = zap.NewNop()
	}
	if scope == nil {
		scope = tally.NoopScope
	}

	return &PingController{
		logger:       logger,
		metricsScope: scope,
	}
}

// Ping handles the ping request and returns a response
func (c *PingController) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	start := time.Now()
	defer func() {
		c.metricsScope.Timer("ping_latency").Record(time.Since(start))
	}()

	c.metricsScope.Counter("ping_requests_total").Inc(1)

	message := "pong"
	isEcho := false
	if req.Message != "" {
		message = "echo: " + req.Message
		isEcho = true
		c.metricsScope.Counter("echo_requests_total").Inc(1)
	}

	hostname, _ := os.Hostname()

	c.logger.Info("ping request received",
		zap.String("message", req.Message),
		zap.Bool("is_echo", isEcho),
		zap.String("hostname", hostname),
	)

	return &pb.PingResponse{
		Message:     message,
		ServiceName: "gateway",
		Timestamp:   time.Now().Unix(),
		Hostname:    hostname,
	}, nil
}
