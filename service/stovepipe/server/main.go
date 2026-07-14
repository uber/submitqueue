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
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	genericerrs "github.com/uber/submitqueue/platform/errs/generic"
	mysqlerrs "github.com/uber/submitqueue/platform/errs/mysql"
	extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"
	queueMySQL "github.com/uber/submitqueue/platform/extension/messagequeue/mysql"
	"github.com/uber/submitqueue/service/stovepipe/server/mapper"
	"github.com/uber/submitqueue/stovepipe/controller"
	"github.com/uber/submitqueue/stovepipe/controller/process"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	queueconfigdefault "github.com/uber/submitqueue/stovepipe/extension/queueconfig/default"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	sourcecontrolfake "github.com/uber/submitqueue/stovepipe/extension/sourcecontrol/fake"
	storageMySQL "github.com/uber/submitqueue/stovepipe/extension/storage/mysql"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// StovepipeServer wraps the controllers and implements the gRPC service interface.
type StovepipeServer struct {
	pb.UnimplementedStovepipeServer
	pingController   *controller.PingController
	ingestController *controller.IngestController
}

// Ping delegates to the controller.
func (s *StovepipeServer) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return s.pingController.Ping(ctx, req)
}

// Ingest maps the wire request to an entity, delegates to the controller, and maps
// the result back to the wire response.
func (s *StovepipeServer) Ingest(ctx context.Context, req *pb.IngestRequest) (*pb.IngestResponse, error) {
	result, err := s.ingestController.Ingest(ctx, mapper.ProtoToIngestRequest(req))
	if err != nil {
		return nil, err
	}
	return mapper.IngestResultToProto(result), nil
}

// inMemoryCounter is a minimal, process-local counter.Counter used to wire the example
// server. It is not durable; a real deployment supplies a persistent implementation
// (e.g. platform/extension/counter/mysql).
type inMemoryCounter struct {
	mu     sync.Mutex
	values map[string]int64
}

func newInMemoryCounter() *inMemoryCounter {
	return &inMemoryCounter{values: make(map[string]int64)}
}

// Next returns the next value in the sequence for the given domain, starting at 1.
func (c *inMemoryCounter) Next(_ context.Context, domain string) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.values[domain]++
	return c.values[domain], nil
}

// fakeSourceControlFactory is the example SourceControl factory. It seeds each queue with a
// deterministic single-commit history so ingest resolves a stable head URI (and re-ingesting
// the same queue exercises the dedup path). A real deployment supplies a VCS-backed factory.
type fakeSourceControlFactory struct{}

func (fakeSourceControlFactory) For(cfg sourcecontrol.Config) (sourcecontrol.SourceControl, error) {
	return sourcecontrolfake.New([]string{fmt.Sprintf("git://%s/HEAD", cfg.QueueName)}), nil
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

	// Storage database (request + request_uri tables).
	storageDSN := os.Getenv("STORAGE_MYSQL_DSN")
	if storageDSN == "" {
		return fmt.Errorf("STORAGE_MYSQL_DSN environment variable is required")
	}
	storageDB, err := sql.Open("mysql", storageDSN)
	if err != nil {
		return fmt.Errorf("failed to open storage database: %w", err)
	}
	defer storageDB.Close()

	store, err := storageMySQL.NewStorage(storageDB, scope.SubScope("storage"))
	if err != nil {
		return fmt.Errorf("failed to create storage: %w", err)
	}
	defer store.Close()

	// Queue database (messaging infrastructure for the process stage).
	queueDSN := os.Getenv("QUEUE_MYSQL_DSN")
	if queueDSN == "" {
		return fmt.Errorf("QUEUE_MYSQL_DSN environment variable is required")
	}
	queueDB, err := sql.Open("mysql", queueDSN)
	if err != nil {
		return fmt.Errorf("failed to open queue database: %w", err)
	}
	defer queueDB.Close()

	mysqlQueue, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           queueDB,
		Logger:       logger,
		MetricsScope: scope.SubScope("queue"),
	})
	if err != nil {
		return fmt.Errorf("failed to create queue: %w", err)
	}
	defer mysqlQueue.Close()

	subscriberName := os.Getenv("HOSTNAME")
	if subscriberName == "" {
		subscriberName = fmt.Sprintf("stovepipe-%d", time.Now().Unix())
	}

	registry, err := newTopicRegistry(mysqlQueue, subscriberName)
	if err != nil {
		return fmt.Errorf("failed to create topic registry: %w", err)
	}

	// Consumer running the process stage.
	primaryConsumer := consumer.New(logger.Sugar(), scope.SubScope("consumer"), registry,
		errs.NewClassifierProcessor(
			genericerrs.Classifier,
			mysqlerrs.Classifier,
		),
	)

	processController := process.NewController(logger.Sugar(), scope, store, queueconfigdefault.NewStore(), stovepipemq.TopicKeyProcess, "stovepipe-process")
	if err := primaryConsumer.Register(processController); err != nil {
		return fmt.Errorf("failed to register process controller: %w", err)
	}

	if err := primaryConsumer.Start(ctx); err != nil {
		return fmt.Errorf("failed to start consumer: %w", err)
	}
	logger.Info("consumer started")

	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Create controllers and wrap them for gRPC
	pingController := controller.NewPingController(logger, scope)
	ingestController := controller.NewIngestController(
		logger.Sugar(),
		scope,
		newInMemoryCounter(),
		fakeSourceControlFactory{},
		store,
		registry,
	)
	srv := &StovepipeServer{
		pingController:   pingController,
		ingestController: ingestController,
	}
	pb.RegisterStovepipeServer(grpcServer, srv)

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
	var serverErr error
	select {
	case <-ctx.Done():
		fmt.Println("Shutting down stovepipe server due to interruption signal...")

		// Set the error to the context cancellation error to be surfaced as a desired exit code by the main function
		// to indicate that the server was stopped as intended. It may be overridden by the server error if any.
		err = ctx.Err()

		// stop GRPC server and wait for it to exit
		grpcServer.GracefulStop()
		serverErr = <-serverErrCh
	case serverErr = <-serverErrCh:
		fmt.Println("Shutting down stovepipe server due to critical GRPC server error...")
		cancel()
	}

	if serverErr != nil {
		serverErr = fmt.Errorf("GRPC server exited with error: %w", serverErr)
	}

	consumerStopErr := primaryConsumer.Stop(30000)
	if consumerStopErr != nil {
		consumerStopErr = fmt.Errorf("failed to stop consumer: %w", consumerStopErr)
	}

	if consumerStopErr != nil || serverErr != nil {
		err = errors.Join(err, consumerStopErr, serverErr)
	}

	return err
}

// newTopicRegistry builds the TopicRegistry for Stovepipe's internal pipeline queues. ingest
// publishes to the process topic and the process consumer subscribes to it.
func newTopicRegistry(q extqueue.Queue, subscriberName string) (consumer.TopicRegistry, error) {
	return consumer.NewTopicRegistry([]consumer.TopicConfig{
		{
			Key:   stovepipemq.TopicKeyProcess,
			Name:  "process",
			Queue: q,
			Subscription: extqueue.DefaultSubscriptionConfig(
				subscriberName, "stovepipe-process",
			),
		},
	})
}
