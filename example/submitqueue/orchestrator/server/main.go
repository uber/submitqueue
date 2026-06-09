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

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/core/errs"
	genericerrs "github.com/uber/submitqueue/core/errs/generic"
	mysqlerrs "github.com/uber/submitqueue/core/errs/mysql"
	"github.com/uber/submitqueue/core/httpclient"
	"github.com/uber/submitqueue/extension/counter"
	mysqlcounter "github.com/uber/submitqueue/extension/counter/mysql"
	extqueue "github.com/uber/submitqueue/extension/messagequeue"
	queueMySQL "github.com/uber/submitqueue/extension/messagequeue/mysql"
	"github.com/uber/submitqueue/submitqueue/core/consumer"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/buildrunner"
	buildfake "github.com/uber/submitqueue/submitqueue/extension/buildrunner/fake"
	"github.com/uber/submitqueue/submitqueue/extension/changeprovider"
	cpfake "github.com/uber/submitqueue/submitqueue/extension/changeprovider/fake"
	githubprovider "github.com/uber/submitqueue/submitqueue/extension/changeprovider/github"
	"github.com/uber/submitqueue/submitqueue/extension/conflict"
	"github.com/uber/submitqueue/submitqueue/extension/conflict/all"
	conflictfake "github.com/uber/submitqueue/submitqueue/extension/conflict/fake"
	"github.com/uber/submitqueue/submitqueue/extension/conflict/none"
	"github.com/uber/submitqueue/submitqueue/extension/mergechecker"
	mcfake "github.com/uber/submitqueue/submitqueue/extension/mergechecker/fake"
	githubchecker "github.com/uber/submitqueue/submitqueue/extension/mergechecker/github"
	"github.com/uber/submitqueue/submitqueue/extension/pusher"
	pushfake "github.com/uber/submitqueue/submitqueue/extension/pusher/fake"
	gitpusher "github.com/uber/submitqueue/submitqueue/extension/pusher/git"
	"github.com/uber/submitqueue/submitqueue/extension/scorer"
	"github.com/uber/submitqueue/submitqueue/extension/scorer/composite"
	scorerfake "github.com/uber/submitqueue/submitqueue/extension/scorer/fake"
	"github.com/uber/submitqueue/submitqueue/extension/scorer/heuristic"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	mysqlstorage "github.com/uber/submitqueue/submitqueue/extension/storage/mysql"
	"github.com/uber/submitqueue/submitqueue/orchestrator/controller"
	"github.com/uber/submitqueue/submitqueue/orchestrator/controller/batch"
	"github.com/uber/submitqueue/submitqueue/orchestrator/controller/build"
	"github.com/uber/submitqueue/submitqueue/orchestrator/controller/buildsignal"
	"github.com/uber/submitqueue/submitqueue/orchestrator/controller/cancel"
	"github.com/uber/submitqueue/submitqueue/orchestrator/controller/conclude"
	"github.com/uber/submitqueue/submitqueue/orchestrator/controller/dlq"
	"github.com/uber/submitqueue/submitqueue/orchestrator/controller/merge"
	"github.com/uber/submitqueue/submitqueue/orchestrator/controller/score"
	"github.com/uber/submitqueue/submitqueue/orchestrator/controller/speculate"
	"github.com/uber/submitqueue/submitqueue/orchestrator/controller/start"
	"github.com/uber/submitqueue/submitqueue/orchestrator/controller/validate"
	pb "github.com/uber/submitqueue/submitqueue/orchestrator/protopb"
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

	// Two consumers share the topic registry but apply different error
	// classification policies. The primary consumer runs the standard
	// per-node classifier walk. The DLQ consumer uses the AlwaysRetryableProcessor
	// so every non-nil error from a DLQ controller is forced retryable —
	// reconciliation must redeliver on any failure because the DLQ
	// subscriptions are final destinations (there is no further DLQ).
	primaryConsumer := consumer.New(logger.Sugar(), scope.SubScope("consumer"), registry,
		errs.NewClassifierProcessor(
			genericerrs.Classifier,
			// Storage (extension/storage/mysql) and queue (extension/messagequeue/mysql)
			// both run on the same MySQL driver, so a single classifier covers
			// errors surfaced from either backend.
			mysqlerrs.Classifier,
		),
	)
	dlqConsumer := consumer.New(logger.Sugar(), scope.SubScope("consumer-dlq"), registry,
		errs.AlwaysRetryableProcessor,
	)

	// Build the per-queue extension registry: each queue resolves to its own
	// set of extension implementations (scorer, conflict analyzer, …), falling
	// back to a baseline profile for queues without an explicit entry. This is
	// the single place queue topology is known; the extension packages stay
	// queue-agnostic.
	queues, err := newQueueRegistry(logger, scope)
	if err != nil {
		return fmt.Errorf("failed to build queue registry: %w", err)
	}

	// Per-extension factories all resolve against the registry by queue name.
	mcf := mergeCheckerFactory{queues}
	cpf := changeProviderFactory{queues}
	pshf := pusherFactory{queues}
	brf := buildRunnerFactory{queues}
	scf := scorerFactory{queues}
	cof := analyzerFactory{queues}

	// Register controllers
	primaryCount, err := registerPrimaryControllers(primaryConsumer, logger.Sugar(), scope, registry, mcf, cpf, pshf, brf, scf, cof, cnt, store)
	if err != nil {
		return err
	}
	dlqCount, err := registerDLQControllers(dlqConsumer, logger.Sugar(), scope, store)
	if err != nil {
		return err
	}

	logger.Info("controllers registered", zap.Int("primary", primaryCount), zap.Int("dlq", dlqCount))

	// Start consumers. DLQ first because Start begins processing
	// messages immediately; if the second (primary) consumer fails to
	// start, the half we already started is the DLQ side, whose work
	// is idempotent reconciliation and is safe to interrupt mid-flight
	// for rollback.
	if err := dlqConsumer.Start(ctx); err != nil {
		// The error can also be a result of a context cancellation due to SIGINT or SIGTERM.
		// This is expected, just propagate it.
		return fmt.Errorf("failed to start dlq consumer: %w", err)
	}
	if err := primaryConsumer.Start(ctx); err != nil {
		// Best-effort: stop the dlq consumer we just started so the
		// caller does not need to know which half failed. Aggregate both
		// errors with errors.Join so the operator sees the original cause.
		stopErr := dlqConsumer.Stop(30000)
		return errors.Join(
			fmt.Errorf("failed to start primary consumer: %w", err),
			stopErr,
		)
	}
	logger.Info("consumers started")

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

	// Stop consumers with 30s timeout in reverse start order: primary
	// first, then DLQ. The primary pipeline writes the state that DLQ
	// reconciliation reads, so draining primary first means in-flight
	// DLQ reconciliation finishes against a settled primary rather than
	// racing its shutdown.
	primaryStopErr := primaryConsumer.Stop(30000)
	dlqStopErr := dlqConsumer.Stop(30000)
	errStop := errors.Join(primaryStopErr, dlqStopErr)
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
	// primaryTopics enumerates the {key, name, group-suffix} for every primary
	// pipeline topic. The DLQ topic for each is derived by appending "_dlq" to
	// both the topic name and the consumer group; the topic-key suffix is
	// owned by the dlq package (dlq.TopicKey).
	type topicSpec struct {
		key         consumer.TopicKey
		name        string
		groupSuffix string
	}
	primaryTopics := []topicSpec{
		{consumer.TopicKeyStart, "start", "orchestrator-start"},
		{consumer.TopicKeyCancel, "cancel", "orchestrator-cancel"},
		{consumer.TopicKeyValidate, "validate", "orchestrator-validate"},
		{consumer.TopicKeyBatch, "batch", "orchestrator-batch"},
		{consumer.TopicKeyScore, "score", "orchestrator-score"},
		{consumer.TopicKeySpeculate, "speculate", "orchestrator-speculate"},
		{consumer.TopicKeyBuild, "build", "orchestrator-build"},
		{consumer.TopicKeyBuildSignal, "buildsignal", "orchestrator-buildsignal"},
		{consumer.TopicKeyMerge, "merge", "orchestrator-merge"},
		{consumer.TopicKeyConclude, "conclude", "orchestrator-conclude"},
	}

	configs := make([]consumer.TopicConfig, 0, 2*len(primaryTopics))
	for _, t := range primaryTopics {
		configs = append(configs, consumer.TopicConfig{
			Key:   t.key,
			Name:  t.name,
			Queue: q,
			Subscription: extqueue.DefaultSubscriptionConfig(
				subscriberName, t.groupSuffix,
			),
		})
		// DLQ subscription for the same primary stage. DLQ is disabled here
		// to avoid a "_dlq_dlq" cascade: if DLQ reconciliation itself fails,
		// the consumer retries forever and the failure is surfaced via logs
		// and metrics rather than being moved to a second-level dead-letter
		// topic that nobody consumes.
		//
		// MaxAttempts is bumped to a very high value so the per-message
		// retry budget effectively never runs out — this pairs with the
		// AlwaysRetryableProcessor wired into the DLQ consumer to guarantee
		// reconciliation eventually converges instead of being silently
		// dropped after the default retry count.
		dlqSub := extqueue.DefaultSubscriptionConfig(
			subscriberName, t.groupSuffix+"-dlq",
		)
		dlqSub.DLQ.Enabled = false
		dlqSub.Retry.MaxAttempts = 1000
		configs = append(configs, consumer.TopicConfig{
			Key:          dlq.TopicKey(t.key),
			Name:         t.name + "_dlq",
			Queue:        q,
			Subscription: dlqSub,
		})
	}

	// Publish-only: the orchestrator emits request log entries to the log
	// topic but never persists them. The gateway is the sole consumer that
	// writes the request log to storage, so the orchestrator registers no
	// consuming subscription (and therefore no log DLQ) for this topic.
	configs = append(configs, consumer.TopicConfig{
		Key:   consumer.TopicKeyLog,
		Name:  "log",
		Queue: q,
	})

	return consumer.NewTopicRegistry(configs)
}

// registerPrimaryControllers creates all pipeline controllers and registers
// them with the primary consumer. Pipeline:
//
//	request → validate → batch → score → speculate → build → buildsignal ─┐
//	                                     ↑     ↘             ↻ poll       │
//	                                     │      merge → conclude          │
//	                                     │        │                       │
//	                                     └────────┴───────────────────────┘

// TODO(wiring abstraction): queueExtensions + queueRegistry currently live here
// as example-local wiring. Evaluate promoting them into a defined abstraction in
// the submitqueue domain layer (e.g. submitqueue/core/...) — NOT extension/* and
// NOT cross-domain core/, since the bundle names submitqueue-specific extensions.
// Do this only when a trigger lands: (1) a second consumer needs the same wiring
// (a real prod server, or an e2e harness building real per-queue profiles);
// (2) per-queue config becomes data-driven (build profiles from queueconfig.Store
// /queues.yaml instead of Go literals); or (3) the bundle grows lifecycle
// (Close/health/hot-reload). Until then, keep it local — extracting now adds
// indirection for one hardcoded consumer. See also queueconfig.Store, which holds
// the per-queue *data* half; a promoted Registry would build impl bundles from it.
//
// queueExtensions is the full set of extension implementations for a single
// queue. Grouping them per queue (rather than per extension) lets the wiring
// read as "for this queue, here are its scorer, analyzer, pusher, …", and lets
// a queue profile start from a baseline and override only what differs.
type queueExtensions struct {
	mergeChecker   mergechecker.MergeChecker
	changeProvider changeprovider.ChangeProvider
	pusher         pusher.Pusher
	buildRunner    buildrunner.BuildRunner
	scorer         scorer.Scorer
	analyzer       conflict.Analyzer
}

// queueRegistry maps a queue name to its extensions, falling back to a default
// profile for queues without an explicit entry. It is the single place that
// knows the queue topology; the extension packages remain queue-agnostic.
type queueRegistry struct {
	byQueue map[string]queueExtensions
	def     queueExtensions
}

// get returns the extensions for the named queue, or the default profile.
func (r queueRegistry) get(queue string) queueExtensions {
	if e, ok := r.byQueue[queue]; ok {
		return e
	}
	return r.def
}

// The per-extension factories below are thin adapters: each satisfies its
// extension's Factory contract by resolving the queue's profile from the
// registry. All routing logic lives here in the wiring layer.
type mergeCheckerFactory struct{ reg queueRegistry }

func (f mergeCheckerFactory) For(cfg mergechecker.Config) (mergechecker.MergeChecker, error) {
	return f.reg.get(cfg.QueueName).mergeChecker, nil
}

type changeProviderFactory struct{ reg queueRegistry }

func (f changeProviderFactory) For(cfg changeprovider.Config) (changeprovider.ChangeProvider, error) {
	return f.reg.get(cfg.QueueName).changeProvider, nil
}

type pusherFactory struct{ reg queueRegistry }

func (f pusherFactory) For(cfg pusher.Config) (pusher.Pusher, error) {
	return f.reg.get(cfg.QueueName).pusher, nil
}

type buildRunnerFactory struct{ reg queueRegistry }

func (f buildRunnerFactory) For(cfg buildrunner.Config) (buildrunner.BuildRunner, error) {
	return f.reg.get(cfg.QueueName).buildRunner, nil
}

type scorerFactory struct{ reg queueRegistry }

func (f scorerFactory) For(cfg scorer.Config) (scorer.Scorer, error) {
	return f.reg.get(cfg.QueueName).scorer, nil
}

type analyzerFactory struct{ reg queueRegistry }

func (f analyzerFactory) For(cfg conflict.Config) (conflict.Analyzer, error) {
	return f.reg.get(cfg.QueueName).analyzer, nil
}

func registerPrimaryControllers(c consumer.Consumer, logger *zap.SugaredLogger, scope tally.Scope, registry consumer.TopicRegistry, mcf mergechecker.Factory, cpf changeprovider.Factory, pshf pusher.Factory, brf buildrunner.Factory, scf scorer.Factory, cof conflict.Factory, cnt counter.Counter, store storage.Storage) (int, error) {
	var count int
	requestController := start.NewController(
		logger,
		scope,
		store,
		registry,
		consumer.TopicKeyStart,
		"orchestrator-start",
	)
	if err := c.Register(requestController); err != nil {
		return count, fmt.Errorf("failed to register request controller: %w", err)
	}
	count++

	cancelController := cancel.NewController(
		logger,
		scope,
		store,
		registry,
		consumer.TopicKeyCancel,
		"orchestrator-cancel",
	)
	if err := c.Register(cancelController); err != nil {
		return count, fmt.Errorf("failed to register cancel controller: %w", err)
	}
	count++

	validateController := validate.NewController(
		logger,
		scope,
		store,
		registry,
		mcf,
		cpf,
		consumer.TopicKeyValidate,
		"orchestrator-validate",
	)
	if err := c.Register(validateController); err != nil {
		return count, fmt.Errorf("failed to register validate controller: %w", err)
	}
	count++

	batchController := batch.NewController(
		logger,
		scope,
		registry,
		cnt,
		store,
		cof,
		consumer.TopicKeyBatch,
		"orchestrator-batch",
	)
	if err := c.Register(batchController); err != nil {
		return count, fmt.Errorf("failed to register batch controller: %w", err)
	}
	count++

	scoreController := score.NewController(
		logger,
		scope,
		store,
		scf,
		registry,
		consumer.TopicKeyScore,
		"orchestrator-score",
	)
	if err := c.Register(scoreController); err != nil {
		return count, fmt.Errorf("failed to register score controller: %w", err)
	}
	count++

	speculateController := speculate.NewController(
		logger,
		scope,
		store,
		registry,
		consumer.TopicKeySpeculate,
		"orchestrator-speculate",
	)
	if err := c.Register(speculateController); err != nil {
		return count, fmt.Errorf("failed to register speculate controller: %w", err)
	}
	count++

	buildController := build.NewController(
		logger,
		scope,
		store,
		brf,
		registry,
		consumer.TopicKeyBuild,
		"orchestrator-build",
	)
	if err := c.Register(buildController); err != nil {
		return count, fmt.Errorf("failed to register build controller: %w", err)
	}
	count++

	buildsignalController := buildsignal.NewController(
		logger,
		scope,
		store,
		brf,
		registry,
		consumer.TopicKeyBuildSignal,
		"orchestrator-buildsignal",
	)
	if err := c.Register(buildsignalController); err != nil {
		return count, fmt.Errorf("failed to register buildsignal controller: %w", err)
	}
	count++

	mergeController := merge.NewController(
		logger,
		scope,
		store,
		registry,
		pshf,
		consumer.TopicKeyMerge,
		"orchestrator-merge",
	)
	if err := c.Register(mergeController); err != nil {
		return count, fmt.Errorf("failed to register merge controller: %w", err)
	}
	count++

	concludeController := conclude.NewController(
		logger,
		scope,
		store,
		registry,
		consumer.TopicKeyConclude,
		"orchestrator-conclude",
	)
	if err := c.Register(concludeController); err != nil {
		return count, fmt.Errorf("failed to register conclude controller: %w", err)
	}
	count++

	return count, nil
}

// registerDLQControllers creates one DLQ reconciler per primary stage and
// registers them with the DLQ consumer. Each reconciler drives the affected
// request or batch into a terminal Error/Failed state so the gateway stops
// reporting it as stuck-in-progress.
func registerDLQControllers(c consumer.Consumer, logger *zap.SugaredLogger, scope tally.Scope, store storage.Storage) (int, error) {
	dlqScope := scope.SubScope("dlq")
	dlqRegs := []struct {
		name string
		ctl  consumer.Controller
	}{
		{"start_dlq", dlq.NewDLQRequestController(logger, dlqScope, store, dlq.DecodeLandRequestID, dlq.TopicKey(consumer.TopicKeyStart), "orchestrator-start-dlq")},
		{"cancel_dlq", dlq.NewDLQRequestController(logger, dlqScope, store, dlq.DecodeCancelRequestID, dlq.TopicKey(consumer.TopicKeyCancel), "orchestrator-cancel-dlq")},
		{"validate_dlq", dlq.NewDLQRequestController(logger, dlqScope, store, dlq.DecodeRequestID, dlq.TopicKey(consumer.TopicKeyValidate), "orchestrator-validate-dlq")},
		{"batch_dlq", dlq.NewDLQRequestController(logger, dlqScope, store, dlq.DecodeRequestID, dlq.TopicKey(consumer.TopicKeyBatch), "orchestrator-batch-dlq")},
		{"score_dlq", dlq.NewDLQBatchController(logger, dlqScope, store, dlq.TopicKey(consumer.TopicKeyScore), "orchestrator-score-dlq")},
		{"speculate_dlq", dlq.NewDLQBatchController(logger, dlqScope, store, dlq.TopicKey(consumer.TopicKeySpeculate), "orchestrator-speculate-dlq")},
		{"build_dlq", dlq.NewDLQBatchController(logger, dlqScope, store, dlq.TopicKey(consumer.TopicKeyBuild), "orchestrator-build-dlq")},
		{"buildsignal_dlq", dlq.NewDLQBuildSignalController(logger, dlqScope, store, dlq.TopicKey(consumer.TopicKeyBuildSignal), "orchestrator-buildsignal-dlq")},
		{"merge_dlq", dlq.NewDLQBatchController(logger, dlqScope, store, dlq.TopicKey(consumer.TopicKeyMerge), "orchestrator-merge-dlq")},
		{"conclude_dlq", dlq.NewDLQBatchController(logger, dlqScope, store, dlq.TopicKey(consumer.TopicKeyConclude), "orchestrator-conclude-dlq")},
	}
	var count int
	for _, reg := range dlqRegs {
		if err := c.Register(reg.ctl); err != nil {
			return count, fmt.Errorf("failed to register %s controller: %w", reg.name, err)
		}
		count++
	}

	return count, nil
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

// newMergeChecker creates a MergeChecker for GitHub (github.com), configured via
// GITHUB_BASE_URL, GITHUB_TOKEN, and GITHUB_TIMEOUT. When GITHUB_TOKEN is unset
// it returns the fake merge checker (every change mergeable unless a URI carries
// a failure marker, see mergechecker/fake), keeping the example runnable without
// GitHub and letting e2e tests drive unmergeable scenarios via request payloads.
func newMergeChecker(logger *zap.Logger, scope tally.Scope) (mergechecker.MergeChecker, error) {
	if os.Getenv("GITHUB_TOKEN") == "" {
		logger.Warn("GITHUB_TOKEN not set; using fake merge checker (every change mergeable unless URI-marked)")
		return mcfake.New(), nil
	}

	client, err := httpclient.NewClient(getEnv("GITHUB_BASE_URL", "https://api.github.com"))
	if err != nil {
		return nil, fmt.Errorf("failed to build GitHub HTTP client: %w", err)
	}

	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: os.Getenv("GITHUB_TOKEN")})
	client.Transport = &oauth2.Transport{Source: ts, Base: client.Transport}

	client.Timeout = parseTimeout(os.Getenv("GITHUB_TIMEOUT"), 30*time.Second)

	return githubchecker.NewMergeChecker(githubchecker.Params{
		HTTPClient:   client,
		Logger:       logger.Sugar(),
		MetricsScope: scope.SubScope("mergechecker"),
	}), nil
}

// newChangeProvider creates a ChangeProvider for GitHub (github.com), configured
// via GITHUB_BASE_URL, GITHUB_TOKEN, and GITHUB_TIMEOUT. When GITHUB_TOKEN is
// unset it returns the fake change provider (one empty ChangeInfo per URI unless
// a URI carries a failure marker, see changeprovider/fake).
func newChangeProvider(logger *zap.Logger, scope tally.Scope) (changeprovider.ChangeProvider, error) {
	if os.Getenv("GITHUB_TOKEN") == "" {
		logger.Warn("GITHUB_TOKEN not set; using fake change provider (empty change info unless URI-marked)")
		return cpfake.New(), nil
	}

	client, err := httpclient.NewClient(getEnv("GITHUB_BASE_URL", "https://api.github.com"))
	if err != nil {
		return nil, fmt.Errorf("failed to build GitHub HTTP client: %w", err)
	}

	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: os.Getenv("GITHUB_TOKEN")})
	client.Transport = &oauth2.Transport{Source: ts, Base: client.Transport}

	client.Timeout = parseTimeout(os.Getenv("GITHUB_TIMEOUT"), 30*time.Second)

	return githubprovider.NewProvider(githubprovider.Params{
		HTTPClient:   client,
		Logger:       logger.Sugar(),
		MetricsScope: scope.SubScope("changeprovider"),
	}), nil
}

// newPusher creates a git-backed Pusher bound to the configured checkout path,
// remote, and target branch (PUSHER_CHECKOUT_PATH, PUSHER_REMOTE default
// "origin", PUSHER_TARGET default "main"). When PUSHER_CHECKOUT_PATH is unset it
// returns the fake pusher (commits succeed unless a change URI carries a failure
// marker, see pusher/fake), keeping the example runnable without a git checkout.
func newPusher(logger *zap.Logger, scope tally.Scope) (pusher.Pusher, error) {
	checkout := os.Getenv("PUSHER_CHECKOUT_PATH")
	if checkout == "" {
		logger.Warn("PUSHER_CHECKOUT_PATH not set; using fake pusher (commits succeed unless URI-marked)")
		return pushfake.New(), nil
	}
	return gitpusher.NewPusher(gitpusher.Params{
		CheckoutPath: checkout,
		Remote:       getEnv("PUSHER_REMOTE", "origin"),
		Target:       getEnv("PUSHER_TARGET", "main"),
		Logger:       logger.Sugar(),
		MetricsScope: scope.SubScope("pusher"),
	}), nil
}

// newQueueRegistry builds the per-queue extension profiles for the example.
// Edge integrations (merge checker, change provider, pusher) and the build
// runner form a shared baseline; each per-queue profile starts from that
// baseline and overrides only the extensions that differ — here the scorer and
// conflict analyzer. Queues without an explicit profile fall back to the
// baseline. This is the one place queue topology lives; extension packages stay
// queue-agnostic.
func newQueueRegistry(logger *zap.Logger, scope tally.Scope) (queueRegistry, error) {
	mc, err := newMergeChecker(logger, scope)
	if err != nil {
		return queueRegistry{}, fmt.Errorf("failed to create merge checker: %w", err)
	}
	cp, err := newChangeProvider(logger, scope)
	if err != nil {
		return queueRegistry{}, fmt.Errorf("failed to create change provider: %w", err)
	}
	psh, err := newPusher(logger, scope)
	if err != nil {
		return queueRegistry{}, fmt.Errorf("failed to create pusher: %w", err)
	}

	// batchLines buckets a batch by total lines changed across all its changes —
	// larger batches are likelier to fail to land.
	batchLines := func(_ context.Context, changes entity.BatchChanges) (int, error) {
		return changes.TotalLinesChanged(), nil
	}

	// Baseline profile: shared edge integrations + a fake build runner (every
	// build succeeds unless a head URI carries a failure marker), plus permissive
	// defaults for scorer and conflict. The build runner instance is shared by
	// the build and buildsignal controllers (same profile, same instance) so a
	// build's recorded outcome survives across their separate factory lookups.
	//
	// The scorer is wrapped by scorerfake so a change URI carrying
	// "sq-fake=score-error" forces a scoring error end-to-end; it is a pure
	// passthrough otherwise. The analyzer is wrapped by conflictfake with a nil
	// predicate (passthrough) — swap the predicate (e.g. conflictfake.FailAlways)
	// on a queue to exercise the analyzer error path, as e2e-conflict-error-queue
	// below does.
	base := queueExtensions{
		mergeChecker:   mc,
		changeProvider: cp,
		pusher:         psh,
		buildRunner:    buildfake.New(),
		scorer: scorerfake.New(heuristic.New(
			[]heuristic.Bucket{{Min: 0, Max: 1<<31 - 1, Score: 0.5}},
			batchLines, scope.SubScope("scorer.default"),
		)),
		// TODO: replace the delegate with a real analyzer (e.g. Tango target
		// analysis). "all" serializes the queue conservatively.
		analyzer: conflictfake.New(all.New(), nil),
	}

	// test-queue: bucketed heuristic scorer; conservative (serialized) conflicts
	// inherited from the baseline.
	testQueue := base
	testQueue.scorer = scorerfake.New(heuristic.New(
		[]heuristic.Bucket{
			{Min: 0, Max: 1, Score: 0.95},
			{Min: 2, Max: 5, Score: 0.80},
			{Min: 6, Max: 20, Score: 0.60},
			{Min: 21, Max: 1<<31 - 1, Score: 0.40},
		},
		batchLines, scope.SubScope("scorer.test-queue"),
	))

	// e2e-test-queue: composite scorer; no conflicts (maximum parallelism).
	e2eQueue := base
	e2eQueue.analyzer = conflictfake.New(none.New(), nil)
	e2eQueue.scorer = scorerfake.New(composite.New(
		map[string]scorer.Scorer{
			"size": heuristic.New([]heuristic.Bucket{{Min: 0, Max: 1<<31 - 1, Score: 0.8}}, batchLines, scope),
			"flat": heuristic.New([]heuristic.Bucket{{Min: 0, Max: 1<<31 - 1, Score: 0.6}}, batchLines, scope),
		},
		composite.Avg, scope.SubScope("scorer.e2e-test-queue"),
	))

	// e2e-conflict-error-queue: every conflict analysis fails, exercising the
	// analyzer error path. Scorer/edge integrations inherit the baseline.
	conflictErrQueue := base
	conflictErrQueue.analyzer = conflictfake.New(all.New(), conflictfake.FailAlways)

	return queueRegistry{
		def: base,
		byQueue: map[string]queueExtensions{
			"test-queue":               testQueue,
			"e2e-test-queue":           e2eQueue,
			"e2e-conflict-error-queue": conflictErrQueue,
		},
	}, nil
}
