package controller

import (
	"context"
	"os"
	"time"

	pb "github.com/uber/submitqueue/orchestrator/protopb"
)

// PingServiceImpl implements the OrchestratorService gRPC service for the orchestrator
type PingServiceImpl struct {
	pb.UnimplementedOrchestratorServiceServer
}

// NewPingService creates a new instance of the orchestrator ping service
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
		ServiceName: "orchestrator",
		Timestamp:   time.Now().Unix(),
		Hostname:    hostname,
	}, nil
}
