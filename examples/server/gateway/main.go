package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/gateway/core/controller"
	pb "github.com/uber/submitqueue/gateway/protopb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// GatewayServer wraps the controller and implements the gRPC service interface
type GatewayServer struct {
	pb.UnimplementedSubmitQueueGatewayServer
	controller *controller.PingController
}

// Ping delegates to the controller
func (s *GatewayServer) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return s.controller.Ping(ctx, req)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start gateway server: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// Initialize development logger (human-readable console output)
	logger, err := zap.NewDevelopment()
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}
	defer logger.Sync()

	// Initialize metrics scope
	scope := tally.NewTestScope("gateway", nil)
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			snapshot := scope.Snapshot()
			logger.Info("metrics snapshot",
				zap.Any("counters", snapshot.Counters()),
				zap.Any("gauges", snapshot.Gauges()),
				zap.Any("timers", snapshot.Timers()),
			)
		}
	}()

	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Create ping controller and wrap it for gRPC
	pingController := controller.NewPingController(logger, scope)
	gatewayServer := &GatewayServer{
		controller: pingController,
	}
	pb.RegisterSubmitQueueGatewayServer(grpcServer, gatewayServer)

	// Register reflection service for debugging with grpcurl
	reflection.Register(grpcServer)

	// Listen on port 8081
	port := ":8081"
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %w", port, err)
	}

	fmt.Printf("Gateway gRPC server is running on %s\n", port)
	fmt.Println("Press Ctrl+C to stop.")

	// Start server in a goroutine
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to serve: %v\n", err)
		}
	}()

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down gateway server...")
	grpcServer.GracefulStop()

	return nil
}
