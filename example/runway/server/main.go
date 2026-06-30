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
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	pb "github.com/uber/submitqueue/api/runway/protopb"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	genericerrs "github.com/uber/submitqueue/platform/errs/generic"
	mysqlerrs "github.com/uber/submitqueue/platform/errs/mysql"
	extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"
	queueMySQL "github.com/uber/submitqueue/platform/extension/messagequeue/mysql"
	"github.com/uber/submitqueue/runway/controller"
	"github.com/uber/submitqueue/runway/controller/merge"
	"github.com/uber/submitqueue/runway/controller/mergeconflictcheck"
	"github.com/uber/submitqueue/runway/extension/merger"
	"github.com/uber/submitqueue/runway/extension/merger/noop"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// RunwayServer wraps the controller and implements the gRPC service interface.
type RunwayServer struct {
	pb.UnimplementedRunwayServer
	pingController *controller.PingController
}

// Ping delegates to the controller.
func (s *RunwayServer) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return s.pingController.Ping(ctx, req)
}

func main() {
	code := 0
	if err := run(); err != nil {
		if errors.Is(err, context.Canceled) {
			fmt.Println("Runway server stopped by signal")

			// Return 143 (128 + SIGTERM) as per POSIX standard if the application receives any termination signal from the OS. Ideally we should return 128+SIGINT for SIGINT and 128+SIGTERM for SIGTERM,
			// but it will require a special processing not yet available in the standard library.
			code = 128 + int(syscall.SIGTERM)
		} else {
			fmt.Fprintf(os.Stderr, "Runway server failure: %v\n", err)
			// TODO: classify errors and implement a binary protocol for exit codes, so far 1 for everything
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

	scope := tally.NewTestScope("runway", nil)
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

	logger.Info("initialized queue", zap.String("dsn", queueDSN))

	subscriberName := os.Getenv("HOSTNAME")
	if subscriberName == "" {
		subscriberName = fmt.Sprintf("runway-%d", time.Now().Unix())
	}

	registry, err := newTopicRegistry(mysqlQueue, subscriberName)
	if err != nil {
		return fmt.Errorf("failed to create topic registry: %w", err)
	}

	primaryConsumer := consumer.New(logger.Sugar(), scope.SubScope("consumer"), registry,
		errs.NewClassifierProcessor(
			genericerrs.Classifier,
			mysqlerrs.Classifier,
		),
	)

	mergerFactory := newMergerFactory()

	mergeConflictCheckController := mergeconflictcheck.NewController(mergeconflictcheck.Params{
		Logger:        logger.Sugar(),
		Scope:         scope,
		MergerFactory: mergerFactory,
		Registry:      registry,
		TopicKey:      runwaymq.TopicKeyMergeConflictCheck,
		ConsumerGroup: "runway-mergeconflictcheck",
	})
	if err := primaryConsumer.Register(mergeConflictCheckController); err != nil {
		return fmt.Errorf("failed to register merge-conflict-check controller: %w", err)
	}

	mergeController := merge.NewController(merge.Params{
		Logger:        logger.Sugar(),
		Scope:         scope,
		MergerFactory: mergerFactory,
		Registry:      registry,
		TopicKey:      runwaymq.TopicKeyMerge,
		ConsumerGroup: "runway-merge",
	})
	if err := primaryConsumer.Register(mergeController); err != nil {
		return fmt.Errorf("failed to register merge controller: %w", err)
	}
	logger.Info("controllers registered", zap.Int("primary", 2))

	if err := primaryConsumer.Start(ctx); err != nil {
		return fmt.Errorf("failed to start primary consumer: %w", err)
	}
	logger.Info("consumer started")

	grpcServer := grpc.NewServer()

	pingController := controller.NewPingController(logger, scope)
	srv := &RunwayServer{
		pingController: pingController,
	}
	pb.RegisterRunwayServer(grpcServer, srv)

	reflection.Register(grpcServer)

	port := os.Getenv("PORT")
	if port == "" {
		port = ":8086"
	}
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %w", port, err)
	}

	fmt.Printf("Runway gRPC server is running on %s\n", port)
	fmt.Println("Press Ctrl+C to stop, or send a SIGTERM.")

	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- grpcServer.Serve(listener)
	}()

	var serverErr error
	select {
	case <-ctx.Done():
		fmt.Println("Shutting down runway server due to interruption signal...")

		err = ctx.Err()

		grpcServer.GracefulStop()
		serverErr = <-serverErrCh
	case serverErr = <-serverErrCh:
		fmt.Println("Shutting down runway server due to critical GRPC server error...")
		cancel()
	}

	if serverErr != nil {
		serverErr = fmt.Errorf("GRPC server exited with error: %w", serverErr)
	}

	primaryStopErr := primaryConsumer.Stop(30000)
	if primaryStopErr != nil {
		primaryStopErr = fmt.Errorf("failed to stop consumer: %w", primaryStopErr)
	}

	if primaryStopErr != nil || serverErr != nil {
		err = errors.Join(primaryStopErr, serverErr)
	}

	return err
}

// newMergerFactory returns a merger.Factory for the example server. The noop
// implementation always succeeds; a real deployment wires a VCS-backed factory.
func newMergerFactory() merger.Factory {
	return &noopMergerFactory{}
}

type noopMergerFactory struct{}

func (f *noopMergerFactory) For(_ merger.Config) (merger.Merger, error) {
	return noop.New(), nil
}

// newTopicRegistry builds the TopicRegistry for Runway's merge queues. Inbound
// topics (merge-conflict-check, merge) have subscriptions; outbound signal topics
// are publish-only.
func newTopicRegistry(q extqueue.Queue, subscriberName string) (consumer.TopicRegistry, error) {
	return consumer.NewTopicRegistry([]consumer.TopicConfig{
		{
			Key:   runwaymq.TopicKeyMergeConflictCheck,
			Name:  "merge-conflict-check",
			Queue: q,
			Subscription: extqueue.DefaultSubscriptionConfig(
				subscriberName, "runway-mergeconflictcheck",
			),
		},
		{
			Key:   runwaymq.TopicKeyMergeConflictCheckSignal,
			Name:  "merge-conflict-check-signal",
			Queue: q,
		},
		{
			Key:   runwaymq.TopicKeyMerge,
			Name:  "merge",
			Queue: q,
			Subscription: extqueue.DefaultSubscriptionConfig(
				subscriberName, "runway-merge",
			),
		},
		{
			Key:   runwaymq.TopicKeyMergeSignal,
			Name:  "merge-signal",
			Queue: q,
		},
	})
}
