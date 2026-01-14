package controller

import (
	"context"
	"os"
	"time"

	pb "github.com/uber/submitqueue/gateway/protopb"
)

// PingServiceImpl implements the GatewayService gRPC service for the gateway
type PingServiceImpl struct {
	pb.UnimplementedGatewayServiceServer
}

// NewPingService creates a new instance of the gateway ping service
func NewPingService() *PingServiceImpl {
	return &PingServiceImpl{}
}

// Ping handles the ping request and returns a response
func (s *PingServiceImpl) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	message := "pong"
	if req.Message != "" {
		message = "echo: " + req.Message
	}

	hostname, _ := os.Hostname()

	return &pb.PingResponse{
		Message:     message,
		ServiceName: "gateway",
		Timestamp:   time.Now().Unix(),
		Hostname:    hostname,
	}, nil
}
