// Copyright (c) 2025 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/stovepipe/controller"
	pb "github.com/uber/submitqueue/stovepipe/protopb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// StovepipeServer wraps the controller and implements the gRPC service interface
type StovepipeServer struct {
	pb.UnimplementedSubmitQueueStovepipeServer
	pingController *controller.PingController
}

// Ping delegates to the controller
func (s *StovepipeServer) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return s.pingController.Ping(ctx, req)
}

func main() {
	code := 0
	if err := run(); err != nil {
		if errors.Is(err, context.Canceled) {
			fmt.Println("Stovepipe server stopped by signal")

			// Return 143 (128 + SIGTERM) as per POSIX standard if the application receives any termination signal from the OS. Ideally we should return 128+SIGINT for SIGINT and 128+SIGTERM for SIGTERM,
			// but it will require a special processing not yet available in the standard library.
			code = 128 + int(syscall.SIGTERM)
		} else {
			fmt.Fprintf(os.Stderr, "Stovepipe server failure: %v\n", err)
			// TODO: classify errors and implement a binary protocol for exit codes, so far 1 for everything
			code = 1
		}
	}
	os.Exit(code)
}

func run() error {
	// Set up signal handling early so retry loops can respond to SIGTERM
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Initialize development logger (human-readable console output)
	logger, err := zap.NewDevelopment()
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}
	defer logger.Sync()

	// Initialize metrics scope
	scope := tally.NewTestScope("stovepipe", nil)
	metricsStopCh := make(chan interface{}, 1)
	metricsWgDone := sync.WaitGroup{}
	metricsWgDone.Add(1)
	go func() {
		defer metricsWgDone.Done()

		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-metricsStopCh:
				return
			case <-ticker.C:
				snapshot := scope.Snapshot()
				logger.Info("metrics snapshot",
					zap.Any("counters", snapshot.Counters()),
					zap.Any("gauges", snapshot.Gauges()),
					zap.Any("timers", snapshot.Timers()),
				)
			}
		}
	}()

	defer func() {
		close(metricsStopCh)
		metricsWgDone.Wait()
	}()

	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Create ping controller and wrap it for gRPC
	pingController := controller.NewPingController(logger, scope)
	stovepipeServer := &StovepipeServer{
		pingController: pingController,
	}
	pb.RegisterSubmitQueueStovepipeServer(grpcServer, stovepipeServer)

	// Register reflection service for debugging with grpcurl
	reflection.Register(grpcServer)

	// Listen on configurable port
	port := os.Getenv("PORT")
	if port == "" {
		port = ":8083"
	}
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %w", port, err)
	}

	fmt.Printf("Stovepipe gRPC server is running on %s\n", port)
	fmt.Println("Press Ctrl+C to stop, or send a SIGTERM.")

	// Start server in a goroutine and wait for it to finish
	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- grpcServer.Serve(listener)
	}()

	// Wait for interrupt signal or server critical error
	// If interruption is signaled, gracefully stop the server
	// If an error happens during shutdown, return the actual error, not the context cancellation error
	var serverErr error
	select {
	case <-ctx.Done():
		fmt.Println("Shutting down stovepipe server due to interruption signal...")

		// Set the error to the context cancellation error to be surfaced as a desired exit code by the main function
		// to indicate that the server was stopped as intended
		// It may be overridden by the server error if any
		err = ctx.Err()

		// stop GRPC server and wait for it to exit
		grpcServer.GracefulStop()
		serverErr = <-serverErrCh
	case serverErr = <-serverErrCh:
		fmt.Println("Shutting down stovepipe server due to critical GRPC server error...")
	}

	if serverErr != nil {
		err = fmt.Errorf("GRPC server exited with error: %w", serverErr)
	}

	return err
}
