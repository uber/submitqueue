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
	"github.com/uber-go/tally"
	pb "github.com/uber/submitqueue/api/stovepipe/gateway/protopb"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	genericerrs "github.com/uber/submitqueue/platform/errs/generic"
	mysqlerrs "github.com/uber/submitqueue/platform/errs/mysql"
	mysqlcounter "github.com/uber/submitqueue/platform/extension/counter/mysql"
	extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"
	queueMySQL "github.com/uber/submitqueue/platform/extension/messagequeue/mysql"
	"github.com/uber/submitqueue/stovepipe/core/topickey"
	mysqlstorage "github.com/uber/submitqueue/stovepipe/extension/storage/mysql"
	"github.com/uber/submitqueue/stovepipe/gateway/controller"
	logctrl "github.com/uber/submitqueue/stovepipe/gateway/controller/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

// GatewayServer wraps the controllers and implements the gRPC service interface.
type GatewayServer struct {
	pb.UnimplementedStovepipeGatewayServer
	pingController   *controller.PingController
	ingestController *controller.IngestController
}

// Ping delegates to the controller.
func (s *GatewayServer) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return s.pingController.Ping(ctx, req)
}

// Ingest delegates to the controller.
func (s *GatewayServer) Ingest(ctx context.Context, req *pb.IngestRequest) (*pb.IngestResponse, error) {
	return s.ingestController.Ingest(ctx, req)
}

func main() {
	code := 0
	if err := run(); err != nil {
		if errors.Is(err, context.Canceled) {
			fmt.Println("Stovepipe gateway server stopped by signal")

			// Return 143 (128 + SIGTERM) as per POSIX standard if the application receives any termination signal from the OS. Ideally we should return 128+SIGINT for SIGINT and 128+SIGTERM for SIGTERM,
			// but it will require a special processing not yet available in the standard library.
			code = 128 + int(syscall.SIGTERM)
		} else {
			fmt.Fprintf(os.Stderr, "Stovepipe gateway server failure: %v\n", err)
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
	scope := tally.NewTestScope("stovepipe_gateway", nil)
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

	// Open application database connection.
	// Docker Compose healthchecks ensure MySQL is ready before service starts.
	appDSN := os.Getenv("MYSQL_DSN")
	if appDSN == "" {
		return fmt.Errorf("MYSQL_DSN environment variable is required")
	}
	appDB, err := sql.Open("mysql", appDSN)
	if err != nil {
		return fmt.Errorf("failed to open app database: %w", err)
	}
	defer appDB.Close()

	// Initialize counter from shared app database connection. The ingest controller uses it to
	// mint a SPID per ingest request.
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

	// Subscriber name for the log-topic consumer. It must be unique per running
	// instance: SubscriberName identifies a subscriber for partition leases, so
	// two gateway processes on the same host (sharing HOSTNAME) would otherwise
	// contend for the same lease. Append the PID to keep co-located instances
	// distinct; the PID is stable for the life of the process. Offset tracking
	// stays keyed on the shared ConsumerGroup ("gateway-log"), not this name.
	// Falls back to a time-seeded name when HOSTNAME is unset (e.g. local runs).
	hostname := os.Getenv("HOSTNAME")
	if hostname == "" {
		hostname = fmt.Sprintf("stovepipe-gateway-%d", time.Now().Unix())
	}
	subscriberName := fmt.Sprintf("%s-%d", hostname, os.Getpid())

	// Build the topic registry. The gateway publishes ingest requests to the start of the
	// orchestrator pipeline (TopicKeyStart) — publish-only. It additionally consumes the log topic
	// (TopicKeyLog): the gateway is the sole writer of the request log, persisting entries that the
	// orchestrator publishes there.
	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: topickey.TopicKeyStart, Name: "start", Queue: mysqlQueue},
		{
			Key:   topickey.TopicKeyLog,
			Name:  "log",
			Queue: mysqlQueue,
			Subscription: extqueue.DefaultSubscriptionConfig(
				subscriberName, "gateway-log",
			),
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create topic registry: %w", err)
	}

	// Create gRPC server with a unary interceptor that translates user-input
	// validation errors (anything in the chain that matches controller.ErrInvalidRequest)
	// into codes.InvalidArgument so gRPC clients can distinguish bad input from
	// infrastructure failures. Other errors pass through unchanged.
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(
		func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
			resp, err := handler(ctx, req)
			if err != nil && controller.IsInvalidRequest(err) {
				return nil, status.Error(codes.InvalidArgument, err.Error())
			}
			return resp, err
		},
	))

	// Initialize storage from the shared app database connection. The ingest controller writes the
	// accepted entry to the request log directly; the log consumer (registered below) is the sole
	// persister of request log entries published by the orchestrator.
	store, err := mysqlstorage.NewStorage(appDB, scope.SubScope("storage"))
	if err != nil {
		return fmt.Errorf("failed to create storage: %w", err)
	}
	requestLogStore := store.GetRequestLogStore()

	// Create controllers and wrap them for gRPC
	pingController := controller.NewPingController(logger, scope)
	ingestController := controller.NewIngestController(logger.Sugar(), scope, cnt, requestLogStore, registry)
	gatewayServer := &GatewayServer{
		pingController:   pingController,
		ingestController: ingestController,
	}

	pb.RegisterStovepipeGatewayServer(grpcServer, gatewayServer)

	// Register reflection service for debugging with grpcurl
	reflection.Register(grpcServer)

	// Create the queue consumer and register the log controller. The gateway is
	// the sole persister of the request log: the orchestrator publishes entries
	// to the log topic and this consumer writes them to storage.
	logConsumer := consumer.New(logger.Sugar(), scope.SubScope("consumer"), registry,
		errs.NewClassifierProcessor(
			// Storage (stovepipe/extension/storage/mysql) and queue (platform/extension/messagequeue/mysql)
			// both run on the same MySQL driver, so a single classifier covers
			// errors surfaced from either backend.
			genericerrs.Classifier,
			mysqlerrs.Classifier,
		),
	)

	logController := logctrl.NewController(logger.Sugar(), scope, requestLogStore, topickey.TopicKeyLog, "gateway-log")
	if err := logConsumer.Register(logController); err != nil {
		return fmt.Errorf("failed to register log controller: %w", err)
	}

	if err := logConsumer.Start(ctx); err != nil {
		// The error can also be a result of a context cancellation due to SIGINT or SIGTERM.
		// This is expected, just propagate it.
		return fmt.Errorf("failed to start log consumer: %w", err)
	}
	logger.Info("log consumer started")

	// Listen on configurable port
	port := os.Getenv("PORT")
	if port == "" {
		port = ":8083"
	}
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %w", port, err)
	}

	fmt.Printf("Stovepipe gateway gRPC server is running on %s\n", port)
	fmt.Println("Press Ctrl+C to stop, or send a SIGTERM.")

	// Start server in a goroutine and wait for it to finish
	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- grpcServer.Serve(listener)
	}()

	// Wait for interrupt signal or server critical error.
	// If interruption is signaled, gracefully stop the server.
	// If the server exits with an error, cancel the context to signal the consumer.
	// After this, stop the consumer.
	// If an error happens during shutdown, return the actual error, not the context cancellation error.
	var serverErr error
	select {
	case <-ctx.Done():
		fmt.Println("Shutting down stovepipe gateway server due to interruption signal...")

		// Set the error to the context cancellation error to be surfaced as a desired exit code by the main function
		// to indicate that the server was stopped as intended
		// It may be overridden by the server error if any
		err = ctx.Err()

		// stop GRPC server and wait for it to exit
		grpcServer.GracefulStop()
		serverErr = <-serverErrCh
	case serverErr = <-serverErrCh:
		fmt.Println("Shutting down stovepipe gateway server due to critical GRPC server error...")

		// Cancel the context to signal cancellation to the queue consumer
		cancel()
	}

	if serverErr != nil {
		serverErr = fmt.Errorf("GRPC server exited with error: %w", serverErr)
	}

	// Stop the consumer with a 30s timeout; by this time the context should be
	// cancelled and the processing threads may already be exiting; recollect them.
	errStop := logConsumer.Stop(30000)
	if errStop != nil {
		errStop = fmt.Errorf("failed to stop consumer: %w", errStop)
	}

	if errStop != nil || serverErr != nil {
		// Override context cancellation error with the shutdown error. The server
		// error is the primary/root failure, so it leads; the consumer-stop error
		// is secondary cleanup.
		err = errors.Join(serverErr, errStop)
	}

	return err
}
