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

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/pushqueue/entity"
	"github.com/uber/submitqueue/pushqueue/extension/landqueue"
	"github.com/uber/submitqueue/pushqueue/extension/vcs"
	"github.com/uber/submitqueue/pushqueue/gateway/controller"
	pb "github.com/uber/submitqueue/pushqueue/gateway/protopb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// GatewayServer wraps the controllers and implements the gRPC service interface.
type GatewayServer struct {
	pb.UnimplementedPushQueueGatewayServer
	pingController *controller.PingController
	landController *controller.LandController
}

// Ping delegates to the controller.
func (s *GatewayServer) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return s.pingController.Ping(ctx, req)
}

// Land delegates to the controller.
func (s *GatewayServer) Land(ctx context.Context, req *pb.LandRequest) (*pb.LandResponse, error) {
	return s.landController.Land(ctx, req)
}

// CheckMergeability delegates to the controller.
func (s *GatewayServer) CheckMergeability(ctx context.Context, req *pb.CheckMergeabilityRequest) (*pb.CheckMergeabilityResponse, error) {
	return s.landController.CheckMergeability(ctx, req)
}

func main() {
	code := 0
	if err := run(); err != nil {
		if errors.Is(err, context.Canceled) {
			fmt.Println("PushQueue gateway server stopped by signal")
			code = 128 + int(syscall.SIGTERM)
		} else {
			fmt.Fprintf(os.Stderr, "PushQueue gateway server failure: %v\n", err)
			code = 1
		}
	}
	os.Exit(code)
}

func run() error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger, err := zap.NewDevelopment()
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}
	defer logger.Sync()

	scope := tally.NewTestScope("pushqueue_gateway", nil)
	metricsStopCh := make(chan any, 1)
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

	grpcServer := grpc.NewServer()

	pingController := controller.NewPingController(logger.Sugar(), scope)
	landController := controller.NewLandController(logger.Sugar(), scope, noopVCS{}, noopQueue{})
	srv := &GatewayServer{
		pingController: pingController,
		landController: landController,
	}
	pb.RegisterPushQueueGatewayServer(grpcServer, srv)

	reflection.Register(grpcServer)

	port := os.Getenv("PORT")
	if port == "" {
		port = ":8084"
	}
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %w", port, err)
	}

	fmt.Printf("PushQueue gateway gRPC server is running on %s\n", port)
	fmt.Println("Press Ctrl+C to stop, or send a SIGTERM.")

	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- grpcServer.Serve(listener)
	}()

	var serverErr error
	select {
	case <-ctx.Done():
		fmt.Println("Shutting down pushqueue gateway server due to interruption signal...")
		err = ctx.Err()
		grpcServer.GracefulStop()
		serverErr = <-serverErrCh
	case serverErr = <-serverErrCh:
		fmt.Println("Shutting down pushqueue gateway server due to critical GRPC server error...")
	}

	if serverErr != nil {
		err = fmt.Errorf("GRPC server exited with error: %w", serverErr)
	}

	return err
}

// noopVCS is a placeholder VCS that errors on every operation.
type noopVCS struct{}

func (noopVCS) CheckMergeability(_ context.Context, _ entity.QueueTarget, items []entity.LandItem) ([]vcs.MergeabilityResult, error) {
	results := make([]vcs.MergeabilityResult, len(items))
	for i := range results {
		results[i] = vcs.MergeabilityResult{Mergeable: false, Reason: "noop VCS: not configured"}
	}
	return results, nil
}

func (noopVCS) Prepare(_ context.Context, _ entity.QueueTarget, _ []entity.LandItem) error {
	return fmt.Errorf("noop VCS: not configured")
}

func (noopVCS) Push(_ context.Context, _ entity.QueueTarget, _ []entity.LandItem) (vcs.PushResult, error) {
	return vcs.PushResult{}, fmt.Errorf("noop VCS: not configured")
}

func (noopVCS) Finalize(_ context.Context, _ entity.QueueTarget, _ []entity.LandItem) error {
	return fmt.Errorf("noop VCS: not configured")
}

// noopQueue is a placeholder Queue that passes through immediately.
type noopQueue struct{}

var _ landqueue.Queue = noopQueue{}

func (noopQueue) Enqueue(_ context.Context, _ entity.QueueTarget, _ []entity.LandItem) error {
	return nil
}

func (noopQueue) Wait(_ context.Context, _ entity.QueueTarget) error {
	return nil
}

func (noopQueue) Dequeue(_ context.Context, _ entity.QueueTarget) error {
	return nil
}
