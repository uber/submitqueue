package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/uber/submitqueue/orchestrator/core/controller"
	pb "github.com/uber/submitqueue/orchestrator/protopb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start orchestrator server: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Register the ping service
	pingService := controller.NewPingService()
	pb.RegisterOrchestratorServiceServer(grpcServer, pingService)

	// Register reflection service for debugging with grpcurl
	reflection.Register(grpcServer)

	// Listen on port 8082
	port := ":8082"
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %w", port, err)
	}

	fmt.Printf("Orchestrator gRPC server is running on %s\n", port)
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

	fmt.Println("\nShutting down orchestrator server...")
	grpcServer.GracefulStop()

	return nil
}
