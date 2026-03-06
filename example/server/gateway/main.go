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
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/uber-go/tally/v4"
	mysqlcounter "github.com/uber/submitqueue/extension/counter/mysql"
	queueMySQL "github.com/uber/submitqueue/extension/queue/mysql"
	mysqlstorage "github.com/uber/submitqueue/extension/storage/mysql"
	"github.com/uber/submitqueue/gateway/controller"
	pb "github.com/uber/submitqueue/gateway/protopb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// GatewayServer wraps the controller and implements the gRPC service interface
type GatewayServer struct {
	pb.UnimplementedSubmitQueueGatewayServer
	pingController *controller.PingController
	landController *controller.LandController
}

// Ping delegates to the controller
func (s *GatewayServer) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return s.pingController.Ping(ctx, req)
}

// Land delegates to the controller
func (s *GatewayServer) Land(ctx context.Context, req *pb.LandRequest) (*pb.LandResponse, error) {
	return s.landController.Land(ctx, req)
}

func main() {
	code := 0
	if err := run(); err != nil {
		if errors.Is(err, context.Canceled) {
			fmt.Println("Gateway server stopped by signal")

			// Return 143 (128 + SIGTERM) as per POSIX standard if the application receives any termination signal from the OS. Ideally we should return 128+SIGINT for SIGINT and 128+SIGTERM for SIGTERM,
			// but it will require a special processing not yet available in the standard library.
			code = 128 + int(syscall.SIGTERM)
		} else {
			fmt.Fprintf(os.Stderr, "Gateway server failure: %v\n", err)
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
	scope := tally.NewTestScope("gateway", nil)
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

	// Open application database connection
	// Docker Compose healthchecks ensure MySQL is ready before service starts
	appDSN := os.Getenv("MYSQL_DSN")
	if appDSN == "" {
		return fmt.Errorf("MYSQL_DSN environment variable is required")
	}
	appDB, err := sql.Open("mysql", appDSN)
	if err != nil {
		return fmt.Errorf("failed to open app database: %w", err)
	}
	defer appDB.Close()

	// Initialize counter from shared app database connection
	cnt := mysqlcounter.NewCounter(appDB, scope.SubScope("counter"))

	// Open queue database connection
	queueDSN := os.Getenv("QUEUE_MYSQL_DSN")
	if queueDSN == "" {
		return fmt.Errorf("QUEUE_MYSQL_DSN environment variable is required")
	}
	queueDB, err := sql.Open("mysql", queueDSN)
	if err != nil {
		return fmt.Errorf("failed to open queue database: %w", err)
	}
	defer queueDB.Close()

	// Initialize queue
	mysqlQueue, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           queueDB,
		Logger:       logger,
		MetricsScope: scope.SubScope("queue"),
	})
	if err != nil {
		return fmt.Errorf("failed to create queue: %w", err)
	}
	defer mysqlQueue.Close()

	logger.Info("initialized dependencies",
		zap.String("app_dsn", appDSN),
		zap.String("queue_dsn", queueDSN),
	)

	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Initialize request log store from shared app database connection
	requestLogStore := mysqlstorage.NewRequestLogStore(appDB, scope.SubScope("request_log_store"))

	// Create controllers and wrap them for gRPC
	pingController := controller.NewPingController(logger, scope)
	landController := controller.NewLandController(logger.Sugar(), scope, cnt, mysqlQueue.Publisher(), requestLogStore, "request")
	gatewayServer := &GatewayServer{
		pingController: pingController,
		landController: landController,
	}

	pb.RegisterSubmitQueueGatewayServer(grpcServer, gatewayServer)

	// Register reflection service for debugging with grpcurl
	reflection.Register(grpcServer)

	// Listen on configurable port
	port := os.Getenv("PORT")
	if port == "" {
		port = ":8081"
	}
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %w", port, err)
	}

	fmt.Printf("Gateway gRPC server is running on %s\n", port)
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
		fmt.Println("Shutting down gateway server due to interruption signal...")

		// Set the error to the context cancellation error to be surfaced as a desired exit code by the main function
		// to indicate that the server was stopped as intended
		// It may be overridden by the server error if any
		err = ctx.Err()

		// stop GRPC server and wait for it to exit
		grpcServer.GracefulStop()
		serverErr = <-serverErrCh
	case serverErr = <-serverErrCh:
		fmt.Println("Shutting down gateway server due to critical GRPC server error...")
	}

	if serverErr != nil {
		err = fmt.Errorf("GRPC server exited with error: %w", serverErr)
	}

	return err
}
