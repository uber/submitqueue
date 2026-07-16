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
	"github.com/uber/submitqueue/stovepipe/controller/build"
	"github.com/uber/submitqueue/stovepipe/controller/buildsignal"
	"github.com/uber/submitqueue/stovepipe/controller/dlq"
	"github.com/uber/submitqueue/stovepipe/controller/process"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/extension/buildrunner"
	buildrunnerfake "github.com/uber/submitqueue/stovepipe/extension/buildrunner/fake"
	queueconfigdefault "github.com/uber/submitqueue/stovepipe/extension/queueconfig/default"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	sourcecontrolfake "github.com/uber/submitqueue/stovepipe/extension/sourcecontrol/fake"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
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

// fakeBuildRunnerFactory is the example BuildRunner factory: every queue shares the same
// stateless fake runner, which succeeds unless a caller embeds a failure marker in the head
// URI. A real deployment supplies a backend-specific factory (e.g. Buildkite, per queue).
type fakeBuildRunnerFactory struct{}

func (fakeBuildRunnerFactory) For(_ buildrunner.Config) (buildrunner.BuildRunner, error) {
	return buildrunnerfake.New(), nil
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

	// Two consumers share the topic registry but apply different error classification
	// policies. The primary consumer runs the standard classifier walk. The DLQ consumer
	// uses AlwaysRetryableProcessor so every non-nil error from a DLQ controller is
	// forced retryable — reconciliation must redeliver on any failure because the DLQ
	// subscription is a final destination (DLQ.Enabled is false on it, so there is no
	// further DLQ to fall back on).
	primaryConsumer := consumer.New(logger.Sugar(), scope.SubScope("consumer"), registry,
		errs.NewClassifierProcessor(
			genericerrs.Classifier,
			mysqlerrs.Classifier,
		),
	)
	dlqConsumer := consumer.New(logger.Sugar(), scope.SubScope("consumer-dlq"), registry,
		errs.AlwaysRetryableProcessor,
	)

	// Each factory is constructed once and threaded through every consumer of
	// it, so a real (stateful) backend introduced later is shared rather than
	// silently duplicated across controllers.
	scf := fakeSourceControlFactory{}
	brf := fakeBuildRunnerFactory{}

	primaryCount, err := registerPrimaryControllers(primaryConsumer, logger.Sugar(), scope, store, registry, scf, brf)
	if err != nil {
		return err
	}
	dlqCount, err := registerDLQControllers(dlqConsumer, logger.Sugar(), scope, store, registry)
	if err != nil {
		return err
	}
	logger.Info("controllers registered", zap.Int("primary", primaryCount), zap.Int("dlq", dlqCount))

	// Start consumers. DLQ first because Start begins processing messages
	// immediately; if the primary consumer then fails to start, the half we
	// already started is the DLQ side, whose work is idempotent reconciliation
	// and is safe to interrupt mid-flight for rollback.
	if err := dlqConsumer.Start(ctx); err != nil {
		return fmt.Errorf("failed to start dlq consumer: %w", err)
	}
	if err := primaryConsumer.Start(ctx); err != nil {
		stopErr := dlqConsumer.Stop(30000)
		return errors.Join(fmt.Errorf("failed to start consumer: %w", err), stopErr)
	}
	logger.Info("consumers started")

	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Create controllers and wrap them for gRPC
	pingController := controller.NewPingController(logger, scope)
	ingestController := controller.NewIngestController(
		logger.Sugar(),
		scope,
		newInMemoryCounter(),
		scf,
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

	// Stop consumers in reverse start order: primary first, then DLQ. The primary
	// pipeline writes the state that DLQ reconciliation reads, so draining primary
	// first means in-flight DLQ reconciliation finishes against a settled primary
	// rather than racing its shutdown.
	primaryStopErr := primaryConsumer.Stop(30000)
	dlqStopErr := dlqConsumer.Stop(30000)
	consumerStopErr := errors.Join(primaryStopErr, dlqStopErr)
	if consumerStopErr != nil {
		consumerStopErr = fmt.Errorf("failed to stop consumers: %w", consumerStopErr)
	}

	if consumerStopErr != nil || serverErr != nil {
		err = errors.Join(err, consumerStopErr, serverErr)
	}

	return err
}

// registerPrimaryControllers creates the primary-pipeline queue controllers and
// registers them with c, returning how many were registered.
func registerPrimaryControllers(
	c consumer.Consumer,
	logger *zap.SugaredLogger,
	scope tally.Scope,
	store storage.Storage,
	registry consumer.TopicRegistry,
	scf sourcecontrol.Factory,
	brf buildrunner.Factory,
) (int, error) {
	var count int

	processController := process.NewController(
		logger,
		scope,
		store,
		queueconfigdefault.NewStore(),
		scf,
		registry,
		stovepipemq.TopicKeyProcess,
		"stovepipe-process",
	)
	if err := c.Register(processController); err != nil {
		return count, fmt.Errorf("failed to register process controller: %w", err)
	}
	count++

	buildController := build.NewController(logger, scope, store, brf, registry, stovepipemq.TopicKeyBuild, "stovepipe-build")
	if err := c.Register(buildController); err != nil {
		return count, fmt.Errorf("failed to register build controller: %w", err)
	}
	count++

	buildSignalController := buildsignal.NewController(logger, scope, store, brf, registry, stovepipemq.TopicKeyBuildSignal, "stovepipe-buildsignal")
	if err := c.Register(buildSignalController); err != nil {
		return count, fmt.Errorf("failed to register buildsignal controller: %w", err)
	}
	count++

	return count, nil
}

// registerDLQControllers creates one DLQ reconciler per primary stage and
// registers them with c, returning how many were registered.
func registerDLQControllers(
	c consumer.Consumer,
	logger *zap.SugaredLogger,
	scope tally.Scope,
	store storage.Storage,
	registry consumer.TopicRegistry,
) (int, error) {
	var count int

	processDLQController := dlq.NewController(logger, scope, store, dlq.TopicKey(stovepipemq.TopicKeyProcess), "stovepipe-process-dlq")
	if err := c.Register(processDLQController); err != nil {
		return count, fmt.Errorf("failed to register process dlq controller: %w", err)
	}
	count++

	return count, nil
}

// newTopicRegistry builds the TopicRegistry for Stovepipe's internal pipeline queues. ingest
// publishes to the process topic and the process consumer subscribes to it; process publishes
// to the build topic and the build consumer subscribes to it; build publishes to the buildsignal
// topic and the buildsignal consumer subscribes to it, and also republishes to itself while
// polling. buildsignal publishes to the record topic once a build reaches a terminal status; it
// has no Subscription yet since no consumer for it exists until the record stage lands.
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
		{
			Key:   stovepipemq.TopicKeyBuild,
			Name:  "build",
			Queue: q,
			Subscription: extqueue.DefaultSubscriptionConfig(
				subscriberName, "stovepipe-build",
			),
		},
		{
			Key:   stovepipemq.TopicKeyBuildSignal,
			Name:  "buildsignal",
			Queue: q,
			Subscription: extqueue.DefaultSubscriptionConfig(
				subscriberName, "stovepipe-buildsignal",
			),
		},
		{
			Key:   stovepipemq.TopicKeyRecord,
			Name:  "record",
			Queue: q,
		},
		{
			Key:          dlq.TopicKey(stovepipemq.TopicKeyProcess),
			Name:         "process_dlq",
			Queue:        q,
			Subscription: extqueue.DLQSubscriptionConfig(subscriberName, "stovepipe-process-dlq"),
		},
	})
}
