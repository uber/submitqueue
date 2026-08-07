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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/uber-go/tally"
	mergestrategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	pb "github.com/uber/submitqueue/api/runway/protopb"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	genericerrs "github.com/uber/submitqueue/platform/errs/generic"
	mysqlerrs "github.com/uber/submitqueue/platform/errs/mysql"
	"github.com/uber/submitqueue/platform/extension/consumergate"
	consumergatefile "github.com/uber/submitqueue/platform/extension/consumergate/file"
	consumergatenoop "github.com/uber/submitqueue/platform/extension/consumergate/noop"
	extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"
	queueMySQL "github.com/uber/submitqueue/platform/extension/messagequeue/mysql"
	"github.com/uber/submitqueue/runway/controller"
	"github.com/uber/submitqueue/runway/controller/dlq"
	"github.com/uber/submitqueue/runway/controller/merge"
	"github.com/uber/submitqueue/runway/controller/mergeconflictcheck"
	"github.com/uber/submitqueue/runway/extension/merger"
	"github.com/uber/submitqueue/runway/extension/merger/fake"
	gitmerger "github.com/uber/submitqueue/runway/extension/merger/git"
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

	// One gate is shared by the primary and DLQ consumers: a subscription is
	// gated by its own consumer group, so a DLQ stage is paused by its own
	// group name just like a primary stage.
	gate := newConsumerGate(logger)

	primaryConsumer := consumer.New(logger.Sugar(), scope.SubScope("consumer"), registry,
		errs.NewClassifierProcessor(
			genericerrs.Classifier,
			mysqlerrs.Classifier,
		),
		gate,
	)

	mergerFactory, err := newMergerFactory(logger, scope.SubScope("merger"))
	if err != nil {
		return fmt.Errorf("failed to create merger factory: %w", err)
	}

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

	// The DLQ consumer reconciles dead-lettered merge requests: it republishes a
	// terminal FAILED result to the signal topic so the client's correlation id
	// always resolves. It uses AlwaysRetryableProcessor so a transient publish
	// failure retries forever rather than dead-lettering again.
	dlqConsumer := consumer.New(logger.Sugar(), scope.SubScope("consumer-dlq"), registry,
		errs.AlwaysRetryableProcessor,
		gate,
	)

	mergeConflictCheckDLQController := dlq.NewController(dlq.Params{
		Logger:         logger.Sugar(),
		Scope:          scope,
		Registry:       registry,
		TopicKey:       dlq.TopicKey(runwaymq.TopicKeyMergeConflictCheck),
		SignalTopicKey: runwaymq.TopicKeyMergeConflictCheckSignal,
		ConsumerGroup:  "runway-mergeconflictcheck-dlq",
	})
	if err := dlqConsumer.Register(mergeConflictCheckDLQController); err != nil {
		return fmt.Errorf("failed to register merge-conflict-check DLQ controller: %w", err)
	}

	mergeDLQController := dlq.NewController(dlq.Params{
		Logger:         logger.Sugar(),
		Scope:          scope,
		Registry:       registry,
		TopicKey:       dlq.TopicKey(runwaymq.TopicKeyMerge),
		SignalTopicKey: runwaymq.TopicKeyMergeSignal,
		ConsumerGroup:  "runway-merge-dlq",
	})
	if err := dlqConsumer.Register(mergeDLQController); err != nil {
		return fmt.Errorf("failed to register merge DLQ controller: %w", err)
	}
	logger.Info("DLQ controllers registered", zap.Int("dlq", 2))

	if err := primaryConsumer.Start(ctx); err != nil {
		return fmt.Errorf("failed to start primary consumer: %w", err)
	}
	if err := dlqConsumer.Start(ctx); err != nil {
		return fmt.Errorf("failed to start DLQ consumer: %w", err)
	}
	logger.Info("consumers started")

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

	dlqStopErr := dlqConsumer.Stop(30000)
	if dlqStopErr != nil {
		dlqStopErr = fmt.Errorf("failed to stop DLQ consumer: %w", dlqStopErr)
	}

	if primaryStopErr != nil || dlqStopErr != nil || serverErr != nil {
		err = errors.Join(primaryStopErr, dlqStopErr, serverErr)
	}

	return err
}

// newMergerFactory returns a merger.Factory for the server. MERGER selects the
// implementation explicitly; when it is unset the choice falls back to the merge
// environment — MERGE_CHECKOUT_PATH wires the git-backed merger built from the
// MERGE_* / GIT_* environment, and its absence wires the noop merger so local
// development and compose runs need no git checkout.
func newMergerFactory(logger *zap.Logger, scope tally.Scope) (merger.Factory, error) {
	switch impl := strings.ToLower(strings.TrimSpace(os.Getenv("MERGER"))); impl {
	case "fake":
		// Marker-driven outcomes, for e2e tests that need Runway to fail on
		// demand without a git checkout. Never production.
		logger.Info("MERGER=fake; using marker-driven fake merger")
		return &fakeMergerFactory{merger: fake.New()}, nil
	case "noop":
		logger.Info("MERGER=noop; using noop merger")
		return &noopMergerFactory{}, nil
	case "", "git":
		// Fall through to the merge-environment default below.
	default:
		return nil, fmt.Errorf("invalid MERGER %q", impl)
	}

	checkoutPath := os.Getenv("MERGE_CHECKOUT_PATH")
	if checkoutPath == "" {
		logger.Info("MERGE_CHECKOUT_PATH not set; using noop merger")
		return &noopMergerFactory{}, nil
	}

	defaultStrategy, err := parseStrategy(os.Getenv("MERGE_DEFAULT_STRATEGY"))
	if err != nil {
		return nil, err
	}

	target := envOr("MERGE_TARGET", "main")
	m, err := gitmerger.NewMerger(gitmerger.Params{
		CheckoutPath:    checkoutPath,
		Remote:          envOr("MERGE_REMOTE", "origin"),
		Target:          target,
		DefaultStrategy: defaultStrategy,
		Runtime: gitmerger.GitRuntime{
			Executable:  os.Getenv("GIT_EXECUTABLE"),
			ExecPath:    os.Getenv("GIT_EXEC_PATH"),
			TemplateDir: os.Getenv("GIT_TEMPLATE_DIR"),
		},
		CommitterName:  os.Getenv("MERGE_COMMITTER_NAME"),
		CommitterEmail: os.Getenv("MERGE_COMMITTER_EMAIL"),
		FetchRefspecs:  splitRefspecs(os.Getenv("MERGE_FETCH_REFSPECS")),
		CheckStaleness: envBool("MERGE_CHECK_STALENESS", true),
		// Off by default: it lifts git's refusal to join two unrelated history
		// graphs, which is a safeguard everywhere except a queue whose purpose
		// is importing one repository's history into another.
		AllowUnrelatedHistories: envBool("MERGE_ALLOW_UNRELATED_HISTORIES", false),
		Logger:                  logger.Sugar(),
		MetricsScope:            scope,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to build git merger: %w", err)
	}
	logger.Info("git merger configured",
		zap.String("checkout", checkoutPath),
		zap.String("target", target),
		zap.String("default_strategy", defaultStrategy.String()),
	)
	return &gitMergerFactory{merger: m}, nil
}

// gitMergerFactory returns a single git-backed merger for every queue. The
// merger owns one checkout and serializes its own operations, so one instance
// is shared across queues. A deployment that lands multiple targets wires a
// factory with a per-queue map instead.
type gitMergerFactory struct {
	merger merger.Merger
}

func (f *gitMergerFactory) For(_ merger.Config) (merger.Merger, error) {
	return f.merger, nil
}

type noopMergerFactory struct{}

func (f *noopMergerFactory) For(_ merger.Config) (merger.Merger, error) {
	return noop.New(), nil
}

// fakeMergerFactory shares one fake merger across queues so the synthetic
// revision ids it mints stay unique for the lifetime of the process.
type fakeMergerFactory struct {
	merger merger.Merger
}

func (f *fakeMergerFactory) For(_ merger.Config) (merger.Merger, error) {
	return f.merger, nil
}

// parseStrategy maps the MERGE_DEFAULT_STRATEGY env value to a concrete merge
// strategy, defaulting to REBASE when unset. DEFAULT is rejected because it
// cannot itself be the default a step resolves to.
func parseStrategy(name string) (mergestrategypb.Strategy, error) {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "", "REBASE":
		return mergestrategypb.Strategy_REBASE, nil
	case "SQUASH_REBASE":
		return mergestrategypb.Strategy_SQUASH_REBASE, nil
	case "MERGE":
		return mergestrategypb.Strategy_MERGE, nil
	case "PROMOTE":
		return mergestrategypb.Strategy_PROMOTE, nil
	default:
		return mergestrategypb.Strategy_DEFAULT, fmt.Errorf("invalid MERGE_DEFAULT_STRATEGY %q", name)
	}
}

// splitRefspecs parses the comma-separated MERGE_FETCH_REFSPECS value. Empty
// entries are dropped so a trailing comma is harmless.
func splitRefspecs(v string) []string {
	var out []string
	for _, part := range strings.Split(v, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// envBool reads a boolean environment value, returning fallback when unset or
// unparseable.
func envBool(key string, fallback bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return parsed
}

// envOr returns the environment value for key, or fallback when unset.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
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
			Name:  "runway-merge",
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
		// DLQ topics: the reconciler consumes these and republishes a FAILED
		// result to the corresponding signal topic. Names match the primary
		// topic name plus the "_dlq" suffix the subscriber uses when
		// dead-lettering (see dlq.TopicKey / DefaultSubscriptionConfig).
		{
			Key:   dlq.TopicKey(runwaymq.TopicKeyMergeConflictCheck),
			Name:  "merge-conflict-check_dlq",
			Queue: q,
			Subscription: extqueue.DLQSubscriptionConfig(
				subscriberName, "runway-mergeconflictcheck-dlq",
			),
		},
		{
			Key:   dlq.TopicKey(runwaymq.TopicKeyMerge),
			Name:  "runway-merge_dlq",
			Queue: q,
			Subscription: extqueue.DLQSubscriptionConfig(
				subscriberName, "runway-merge-dlq",
			),
		},
	})
}

// newConsumerGate enables the file-backed consumer gate only when
// CONSUMER_GATE_DIR is explicitly configured. The file implementation is for
// E2E and single-host development; normal service deployments use the no-op
// implementation.
func newConsumerGate(logger *zap.Logger) consumergate.Gate {
	dir := os.Getenv("CONSUMER_GATE_DIR")
	if dir == "" {
		logger.Info("consumer gate disabled")
		return consumergatenoop.New()
	}
	logger.Info("consumer gate configured", zap.String("dir", dir))
	return consumergatefile.New(dir)
}
