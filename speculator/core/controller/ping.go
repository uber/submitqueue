package controller

import (
	"context"
	"os"
	"time"

	pb "github.com/uber/submitqueue/speculator/protopb"
)

// PingServiceImpl implements the SpeculatorService gRPC service for the speculator
type PingServiceImpl struct {
	pb.UnimplementedSpeculatorServiceServer
}

// NewPingService creates a new instance of the speculator ping service
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
		ServiceName: "speculator",
		Timestamp:   time.Now().Unix(),
		Hostname:    hostname,
	}, nil
}
