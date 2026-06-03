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
	"golang.org/x/oauth2"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	genericerrs "github.com/uber/submitqueue/core/errs/generic"
	mysqlerrs "github.com/uber/submitqueue/core/errs/mysql"
	"github.com/uber/submitqueue/core/httpclient"
	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/extension/buildrunner"
	buildnoop "github.com/uber/submitqueue/extension/buildrunner/noop"
	"github.com/uber/submitqueue/extension/changeprovider"
	githubprovider "github.com/uber/submitqueue/extension/changeprovider/github"
	"github.com/uber/submitqueue/extension/changestore"
	mysqlchangestore "github.com/uber/submitqueue/extension/changestore/mysql"
	"github.com/uber/submitqueue/extension/conflict/all"
	"github.com/uber/submitqueue/extension/counter"
	mysqlcounter "github.com/uber/submitqueue/extension/counter/mysql"
	"github.com/uber/submitqueue/extension/mergechecker"
	githubchecker "github.com/uber/submitqueue/extension/mergechecker/github"
	"github.com/uber/submitqueue/extension/pusher"
	gitpusher "github.com/uber/submitqueue/extension/pusher/git"
	extqueue "github.com/uber/submitqueue/extension/queue"
	queueMySQL "github.com/uber/submitqueue/extension/queue/mysql"
	"github.com/uber/submitqueue/extension/scorer/heuristic"
	"github.com/uber/submitqueue/extension/storage"
	mysqlstorage "github.com/uber/submitqueue/extension/storage/mysql"
	"github.com/uber/submitqueue/orchestrator/controller"
	"github.com/uber/submitqueue/orchestrator/controller/batch"
	"github.com/uber/submitqueue/orchestrator/controller/build"
	"github.com/uber/submitqueue/orchestrator/controller/buildsignal"
	"github.com/uber/submitqueue/orchestrator/controller/conclude"
	logctrl "github.com/uber/submitqueue/orchestrator/controller/log"
	"github.com/uber/submitqueue/orchestrator/controller/merge"
	"github.com/uber/submitqueue/orchestrator/controller/score"
	"github.com/uber/submitqueue/orchestrator/controller/speculate"
	"github.com/uber/submitqueue/orchestrator/controller/start"
	"github.com/uber/submitqueue/orchestrator/controller/validate"
	pb "github.com/uber/submitqueue/orchestrator/protopb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// OrchestratorServer wraps the controller and implements the gRPC service interface
type OrchestratorServer struct {
	pb.UnimplementedSubmitQueueOrchestratorServer
	controller *controller.PingController
}

// Ping delegates to the controller
func (s *OrchestratorServer) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return s.controller.Ping(ctx, req)
}

func main() {
	code := 0
	if err := run(); err != nil {
		if errors.Is(err, context.Canceled) {
			fmt.Println("Orchestrator server stopped by signal")

			// Return 143 (128 + SIGTERM) as per POSIX standard if the application receives any termination signal from the OS. Ideally we should return 128+SIGINT for SIGINT and 128+SIGTERM for SIGTERM,
			// but it will require a special processing not yet available in the standard library.
			code = 128 + int(syscall.SIGTERM)
		} else {
			fmt.Fprintf(os.Stderr, "Orchestrator server failure: %v\n", err)
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
	scope := tally.NewTestScope("orchestrator", nil)
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

	// Open app database connection for counter
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

	cnt := mysqlcounter.NewCounter(appDB, scope.SubScope("counter"))

	store, err := mysqlstorage.NewStorage(appDB, scope.SubScope("storage"))
	if err != nil {
		return fmt.Errorf("failed to create storage: %w", err)
	}

	changeStore := mysqlchangestore.NewChangeStore(appDB, scope.SubScope("changestore"))

	// Open queue database connection
	// Docker Compose healthchecks ensure MySQL is ready before service starts
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

	logger.Info("initialized queue", zap.String("dsn", queueDSN))

	// Create topic registry
	subscriberName := os.Getenv("HOSTNAME")
	if subscriberName == "" {
		subscriberName = fmt.Sprintf("orchestrator-%d", time.Now().Unix())
	}

	registry, err := newTopicRegistry(mysqlQueue, subscriberName)
	if err != nil {
		return fmt.Errorf("failed to create topic registry: %w", err)
	}

	// Create consumer.
	c := consumer.New(logger.Sugar(), scope.SubScope("consumer"), registry,
		genericerrs.Classifier,
		// Storage (extension/storage/mysql) and queue (extension/queue/mysql)
		// both run on the same MySQL driver, so a single classifier covers
		// errors surfaced from either backend.
		mysqlerrs.Classifier,
	)

	// Create merge checker
	mc, err := newMergeChecker(logger, scope)
	if err != nil {
		return fmt.Errorf("failed to create merge checker: %w", err)
	}

	// Create change provider
	cp, err := newChangeProvider(logger, scope)
	if err != nil {
		return fmt.Errorf("failed to create change provider: %w", err)
	}

	// Create pusher
	psh, err := newPusher(logger, scope)
	if err != nil {
		return fmt.Errorf("failed to create pusher: %w", err)
	}

	// Create build runner. The noop runner is the pass-through default
	// (every build immediately succeeds) until a real backend is wired in.
	br := buildnoop.New()

	// Register controllers
	if err := registerControllers(c, logger.Sugar(), scope, registry, mc, cp, psh, br, cnt, store, changeStore); err != nil {
		return err
	}

	logger.Info("controllers registered", zap.Int("count", 9))

	// Start consumers
	if err := c.Start(ctx); err != nil {
		// The error can also be a result of a context cancellation due to SIGINT or SIGTERM.
		// This is expected, just propagate it.
		return fmt.Errorf("failed to start consumers: %w", err)
	}
	logger.Info("consumer started")

	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Create ping controller and wrap it for gRPC
	pingController := controller.NewPingController(logger, scope)
	orchestratorServer := &OrchestratorServer{
		controller: pingController,
	}
	pb.RegisterSubmitQueueOrchestratorServer(grpcServer, orchestratorServer)

	// Register reflection service for debugging with grpcurl
	reflection.Register(grpcServer)

	// Listen on configurable port
	port := os.Getenv("PORT")
	if port == "" {
		port = ":8082"
	}
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %w", port, err)
	}

	fmt.Printf("Orchestrator gRPC server is running on %s\n", port)
	fmt.Println("Press Ctrl+C to stop, or send a SIGTERM.")

	// Start server in a goroutine and wait for it to finish
	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- grpcServer.Serve(listener)
	}()

	// Wait for interrupt signal or server critical error
	// If interruption is signaled, gracefully stop the server
	// If server exits with an error, cancel the context to signal cancellation to the queue consumers
	// After this, stop consumers
	// If an error happens during shutdown, return the actual error, not the context cancellation error
	var serverErr error
	select {
	case <-ctx.Done():
		fmt.Println("Shutting down orchestrator server due to interruption signal...")

		// Set the error to the context cancellation error to be surfaced as a desired exit code by the main function
		// to indicate that the server was stopped as intended
		// It may be overridden by the server error if any
		err = ctx.Err()

		// stop GRPC server and wait for it to exit
		grpcServer.GracefulStop()
		serverErr = <-serverErrCh
	case serverErr = <-serverErrCh:
		fmt.Println("Shutting down orchestrator server due to critical GRPC server error...")

		// Cancel the context to signal cancellation to the queue consumers
		cancel()
	}

	if serverErr != nil {
		serverErr = fmt.Errorf("GRPC server exited with error: %w", serverErr)
	}

	// Stop consumers with 30s timeout, by this time the context should be cancelled and the processing threads may already be exiting; recollect them
	errStop := c.Stop(30000)
	if errStop != nil {
		errStop = fmt.Errorf("failed to stop consumers: %w", errStop)
	}

	if errStop != nil || serverErr != nil {
		// Override context cancellation error with the shutdown error
		err = errors.Join(errStop, serverErr)
	}

	// Return the error to be surfaced as a desired exit code by the main function
	return err
}

// newTopicRegistry builds the TopicRegistry with all topic and subscription configs.
func newTopicRegistry(q extqueue.Queue, subscriberName string) (consumer.TopicRegistry, error) {
	return consumer.NewTopicRegistry([]consumer.TopicConfig{
		{
			Key:   consumer.TopicKeyStart,
			Name:  "start",
			Queue: q,
			Subscription: extqueue.DefaultSubscriptionConfig(
				subscriberName, "orchestrator-start",
			),
		},
		{
			Key:   consumer.TopicKeyValidate,
			Name:  "validate",
			Queue: q,
			Subscription: extqueue.DefaultSubscriptionConfig(
				subscriberName, "orchestrator-validate",
			),
		},
		{
			Key:   consumer.TopicKeyBatch,
			Name:  "batch",
			Queue: q,
			Subscription: extqueue.DefaultSubscriptionConfig(
				subscriberName, "orchestrator-batch",
			),
		},
		{
			Key:   consumer.TopicKeyScore,
			Name:  "score",
			Queue: q,
			Subscription: extqueue.DefaultSubscriptionConfig(
				subscriberName, "orchestrator-score",
			),
		},
		{
			Key:   consumer.TopicKeySpeculate,
			Name:  "speculate",
			Queue: q,
			Subscription: extqueue.DefaultSubscriptionConfig(
				subscriberName, "orchestrator-speculate",
			),
		},
		{
			Key:   consumer.TopicKeyBuild,
			Name:  "build",
			Queue: q,
			Subscription: extqueue.DefaultSubscriptionConfig(
				subscriberName, "orchestrator-build",
			),
		},
		{
			Key:   consumer.TopicKeyBuildSignal,
			Name:  "buildsignal",
			Queue: q,
			Subscription: extqueue.DefaultSubscriptionConfig(
				subscriberName, "orchestrator-buildsignal",
			),
		},
		{
			Key:   consumer.TopicKeyMerge,
			Name:  "merge",
			Queue: q,
			Subscription: extqueue.DefaultSubscriptionConfig(
				subscriberName, "orchestrator-merge",
			),
		},
		{
			Key:   consumer.TopicKeyConclude,
			Name:  "conclude",
			Queue: q,
			Subscription: extqueue.DefaultSubscriptionConfig(
				subscriberName, "orchestrator-conclude",
			),
		},
		{
			Key:   consumer.TopicKeyLog,
			Name:  "log",
			Queue: q,
			Subscription: extqueue.DefaultSubscriptionConfig(
				subscriberName, "orchestrator-log",
			),
		},
	})
}

// registerControllers creates all pipeline controllers and registers them with the consumer.
// Pipeline:
//
//   request → validate → batch → score → speculate → build → buildsignal ─┐
//                                        ↑     ↘             ↻ poll       │
//                                        │      merge → conclude          │
//                                        │        │                       │
//                                        └────────┴───────────────────────┘

func registerControllers(c consumer.Consumer, logger *zap.SugaredLogger, scope tally.Scope, registry consumer.TopicRegistry, mc mergechecker.MergeChecker, cp changeprovider.ChangeProvider, psh pusher.Pusher, br buildrunner.BuildRunner, cnt counter.Counter, store storage.Storage, changeStore changestore.ChangeStore) error {
	requestController := start.NewController(
		logger,
		scope,
		store,
		changeStore,
		registry,
		consumer.TopicKeyStart,
		"orchestrator-start",
	)
	if err := c.Register(requestController); err != nil {
		return fmt.Errorf("failed to register request controller: %w", err)
	}

	validateController := validate.NewController(
		logger,
		scope,
		store,
		changeStore,
		registry,
		mc,
		cp,
		consumer.TopicKeyValidate,
		"orchestrator-validate",
	)
	if err := c.Register(validateController); err != nil {
		return fmt.Errorf("failed to register validate controller: %w", err)
	}

	batchController := batch.NewController(
		logger,
		scope,
		registry,
		cnt,
		store,
		// TODO: replace with a real conflict analyzer (e.g. one backed by
		// Tango target analysis). The "all" stub serializes the queue.
		all.New(),
		consumer.TopicKeyBatch,
		"orchestrator-batch",
	)
	if err := c.Register(batchController); err != nil {
		return fmt.Errorf("failed to register batch controller: %w", err)
	}

	scoreController := score.NewController(
		logger,
		scope,
		store,
		// TODO: replace with a real scorer
		heuristic.New(
			[]heuristic.Bucket{
				{Min: 0, Max: 1, Score: 0.95},
				{Min: 2, Max: 5, Score: 0.80},
				{Min: 6, Max: 20, Score: 0.60},
				{Min: 21, Max: 1<<31 - 1, Score: 0.40},
			},
			func(_ context.Context, change entity.Change) (int, error) {
				return len(change.URIs), nil
			},
			scope.SubScope("scorer"),
		),
		registry,
		consumer.TopicKeyScore,
		"orchestrator-score",
	)
	if err := c.Register(scoreController); err != nil {
		return fmt.Errorf("failed to register score controller: %w", err)
	}

	speculateController := speculate.NewController(
		logger,
		scope,
		store,
		registry,
		consumer.TopicKeySpeculate,
		"orchestrator-speculate",
	)
	if err := c.Register(speculateController); err != nil {
		return fmt.Errorf("failed to register speculate controller: %w", err)
	}

	buildController := build.NewController(
		logger,
		scope,
		store,
		br,
		registry,
		consumer.TopicKeyBuild,
		"orchestrator-build",
	)
	if err := c.Register(buildController); err != nil {
		return fmt.Errorf("failed to register build controller: %w", err)
	}

	buildsignalController := buildsignal.NewController(
		logger,
		scope,
		store,
		br,
		registry,
		consumer.TopicKeyBuildSignal,
		"orchestrator-buildsignal",
	)
	if err := c.Register(buildsignalController); err != nil {
		return fmt.Errorf("failed to register buildsignal controller: %w", err)
	}

	mergeController := merge.NewController(
		logger,
		scope,
		store,
		registry,
		psh,
		consumer.TopicKeyMerge,
		"orchestrator-merge",
	)
	if err := c.Register(mergeController); err != nil {
		return fmt.Errorf("failed to register merge controller: %w", err)
	}

	concludeController := conclude.NewController(
		logger,
		scope,
		store,
		registry,
		consumer.TopicKeyConclude,
		"orchestrator-conclude",
	)
	if err := c.Register(concludeController); err != nil {
		return fmt.Errorf("failed to register conclude controller: %w", err)
	}

	logController := logctrl.NewController(
		logger,
		scope,
		store,
		consumer.TopicKeyLog,
		"orchestrator-log",
	)
	if err := c.Register(logController); err != nil {
		return fmt.Errorf("failed to register log controller: %w", err)
	}

	return nil
}

// getEnv returns environment variable value or default if not set.
func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

// parseTimeout parses a duration from environment variable with fallback to default.
// Returns defaultVal if envVal is empty or cannot be parsed.
func parseTimeout(envVal string, defaultVal time.Duration) time.Duration {
	if envVal == "" {
		return defaultVal
	}
	if d, err := time.ParseDuration(envVal); err == nil {
		return d
	}
	return defaultVal
}

// newMergeChecker creates a MergeChecker for GitHub (github.com).
// Configured via GITHUB_BASE_URL, GITHUB_TOKEN, and GITHUB_TIMEOUT environment variables.
func newMergeChecker(logger *zap.Logger, scope tally.Scope) (mergechecker.MergeChecker, error) {
	client, err := httpclient.NewClient(getEnv("GITHUB_BASE_URL", "https://api.github.com"))
	if err != nil {
		return nil, fmt.Errorf("failed to build GitHub HTTP client: %w", err)
	}

	if token := os.Getenv("GITHUB_TOKEN"); token != "" {
		ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})
		client.Transport = &oauth2.Transport{Source: ts, Base: client.Transport}
	}

	client.Timeout = parseTimeout(os.Getenv("GITHUB_TIMEOUT"), 30*time.Second)

	github := githubchecker.NewMergeChecker(githubchecker.Params{
		HTTPClient:   client,
		Logger:       logger.Sugar(),
		MetricsScope: scope.SubScope("mergechecker"),
	})

	return mergechecker.NewMultiChecker(map[string]mergechecker.MergeChecker{
		"github": github,
	}), nil
}

// newChangeProvider creates a ChangeProvider for GitHub (github.com).
// Configured via GITHUB_BASE_URL, GITHUB_TOKEN, and GITHUB_TIMEOUT environment variables.
func newChangeProvider(logger *zap.Logger, scope tally.Scope) (changeprovider.ChangeProvider, error) {
	client, err := httpclient.NewClient(getEnv("GITHUB_BASE_URL", "https://api.github.com"))
	if err != nil {
		return nil, fmt.Errorf("failed to build GitHub HTTP client: %w", err)
	}

	if token := os.Getenv("GITHUB_TOKEN"); token != "" {
		ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})
		client.Transport = &oauth2.Transport{Source: ts, Base: client.Transport}
	}

	client.Timeout = parseTimeout(os.Getenv("GITHUB_TIMEOUT"), 30*time.Second)

	return githubprovider.NewProvider(githubprovider.Params{
		HTTPClient:   client,
		Logger:       logger.Sugar(),
		MetricsScope: scope.SubScope("changeprovider"),
	}), nil
}

// newPusher creates a git-backed Pusher bound to the configured checkout
// path, remote, and target branch. Configured via PUSHER_CHECKOUT_PATH,
// PUSHER_REMOTE (default "origin"), and PUSHER_TARGET (default "main").
//
// If PUSHER_CHECKOUT_PATH is not set the orchestrator falls back to a
// no-op pusher that errors when invoked. This keeps the example server
// runnable in environments that don't exercise the merge controller
// (e.g. ping-only integration tests, local dev without a git checkout).
func newPusher(logger *zap.Logger, scope tally.Scope) (pusher.Pusher, error) {
	checkout := os.Getenv("PUSHER_CHECKOUT_PATH")
	if checkout == "" {
		logger.Warn("PUSHER_CHECKOUT_PATH not set; using no-op pusher (merge controller will fail if invoked)")
		return noopPusher{}, nil
	}
	return gitpusher.NewPusher(gitpusher.Params{
		CheckoutPath: checkout,
		Remote:       getEnv("PUSHER_REMOTE", "origin"),
		Target:       getEnv("PUSHER_TARGET", "main"),
		Logger:       logger.Sugar(),
		MetricsScope: scope.SubScope("pusher"),
	}), nil
}

// noopPusher is a fallback Pusher used when PUSHER_CHECKOUT_PATH is not
// configured. It returns an error on every Push so the merge controller
// (which treats non-ErrConflict errors as transient and nacks the message)
// will not silently report success. It exists so the orchestrator can
// still start up — and serve Ping / accept enqueues — in environments
// that don't run the merge step.
type noopPusher struct{}

func (noopPusher) Push(_ context.Context, _ []entity.Change) (pusher.Result, error) {
	return pusher.Result{}, fmt.Errorf("pusher not configured: set PUSHER_CHECKOUT_PATH to enable pushing")
}
