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
	pb "github.com/uber/submitqueue/api/runway/orchestrator/protopb"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	genericerrs "github.com/uber/submitqueue/platform/errs/generic"
	mysqlerrs "github.com/uber/submitqueue/platform/errs/mysql"
	extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"
	queueMySQL "github.com/uber/submitqueue/platform/extension/messagequeue/mysql"
	"github.com/uber/submitqueue/runway/core/topickey"
	"github.com/uber/submitqueue/runway/extension/vcs"
	"github.com/uber/submitqueue/runway/extension/vcs/noop"
	"github.com/uber/submitqueue/runway/orchestrator/controller"
	"github.com/uber/submitqueue/runway/orchestrator/controller/merge"
	"github.com/uber/submitqueue/runway/orchestrator/controller/mergeconflictcheck"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// OrchestratorServer wraps the controller and implements the gRPC service interface.
type OrchestratorServer struct {
	pb.UnimplementedRunwayOrchestratorServer
	pingController *controller.PingController
}

// Ping delegates to the controller.
func (s *OrchestratorServer) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return s.pingController.Ping(ctx, req)
}

// noopVCSFactory adapts the noop VCS into the vcs.Factory interface.
type noopVCSFactory struct {
	instance *noop.VCS
}

func (f *noopVCSFactory) For(_ vcs.Config) (vcs.VCS, error) {
	return f.instance, nil
}

func main() {
	code := 0
	if err := run(); err != nil {
		if errors.Is(err, context.Canceled) {
			fmt.Println("Runway orchestrator server stopped by signal")
			code = 128 + int(syscall.SIGTERM)
		} else {
			fmt.Fprintf(os.Stderr, "Runway orchestrator server failure: %v\n", err)
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

	scope := tally.NewTestScope("runway_orchestrator", nil)
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
		subscriberName = fmt.Sprintf("runway-orchestrator-%d", time.Now().Unix())
	}

	registry, err := newTopicRegistry(mysqlQueue, subscriberName)
	if err != nil {
		return fmt.Errorf("failed to create topic registry: %w", err)
	}

	vcsFactory := &noopVCSFactory{instance: noop.New()}

	primaryConsumer := consumer.New(logger.Sugar(), scope.SubScope("consumer"), registry,
		errs.NewClassifierProcessor(
			genericerrs.Classifier,
			mysqlerrs.Classifier,
		),
	)

	checkController := mergeconflictcheck.NewController(mergeconflictcheck.Params{
		Logger:        logger.Sugar(),
		Scope:         scope,
		Registry:      registry,
		VCSFactory:    vcsFactory,
		TopicKey:      topickey.TopicKeyMergeConflictCheck,
		ConsumerGroup: "runway-merge-conflict-check",
	})
	if err := primaryConsumer.Register(checkController); err != nil {
		return fmt.Errorf("failed to register merge-conflict-check controller: %w", err)
	}

	mergeController := merge.NewController(merge.Params{
		Logger:        logger.Sugar(),
		Scope:         scope,
		Registry:      registry,
		VCSFactory:    vcsFactory,
		TopicKey:      topickey.TopicKeyMerge,
		ConsumerGroup: "runway-merge",
	})
	if err := primaryConsumer.Register(mergeController); err != nil {
		return fmt.Errorf("failed to register merge controller: %w", err)
	}

	logger.Info("controllers registered", zap.Int("primary", 2))

	if err := primaryConsumer.Start(ctx); err != nil {
		return fmt.Errorf("failed to start primary consumer: %w", err)
	}

	fmt.Println("Runway orchestrator server is running (consumer + gRPC)")
	fmt.Println("Press Ctrl+C to stop, or send a SIGTERM.")

	grpcServer := grpc.NewServer()

	pingController := controller.NewPingController(logger, scope)
	srv := &OrchestratorServer{
		pingController: pingController,
	}
	pb.RegisterRunwayOrchestratorServer(grpcServer, srv)

	reflection.Register(grpcServer)

	port := os.Getenv("PORT")
	if port == "" {
		port = ":8085"
	}
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %w", port, err)
	}

	fmt.Printf("Runway orchestrator gRPC server is running on %s\n", port)

	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- grpcServer.Serve(listener)
	}()

	var serverErr error
	select {
	case <-ctx.Done():
		fmt.Println("Shutting down runway orchestrator server due to interruption signal...")

		err = ctx.Err()

		grpcServer.GracefulStop()
		serverErr = <-serverErrCh
	case serverErr = <-serverErrCh:
		fmt.Println("Shutting down runway orchestrator server due to critical gRPC server error...")
		cancel()
	}

	if serverErr != nil {
		serverErr = fmt.Errorf("gRPC server exited with error: %w", serverErr)
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

func newTopicRegistry(q extqueue.Queue, subscriberName string) (consumer.TopicRegistry, error) {
	return consumer.NewTopicRegistry([]consumer.TopicConfig{
		{
			Key:   topickey.TopicKeyMergeConflictCheck,
			Name:  "merge-conflict-checker",
			Queue: q,
			Subscription: extqueue.DefaultSubscriptionConfig(
				subscriberName, "runway-merge-conflict-check",
			),
		},
		{
			Key:   topickey.TopicKeyMerge,
			Name:  "merger",
			Queue: q,
			Subscription: extqueue.DefaultSubscriptionConfig(
				subscriberName, "runway-merge",
			),
		},
		{
			Key:  topickey.TopicKeyMergeConflictCheckSignal,
			Name: "merge-conflict-checker-signal",
			Queue: q,
		},
		{
			Key:  topickey.TopicKeyMergeSignal,
			Name: "merger-signal",
			Queue: q,
		},
	})
}
