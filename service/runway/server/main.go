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
	"sync/atomic"
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

	mergerFactory, err := newMergerFactory(ctx, logger, scope.SubScope("merger"))
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

// newMergerFactory builds the mergers for the server.
//
// MERGER pins every queue to one implementation explicitly, which is how a test
// holds the service to a fake without a git checkout. Left unset, each queue
// resolves its own merge target through the merge configuration, so a
// deployment can serve several repositories from one Runway.
//
// The fake is reachable only through MERGER, never through the configuration
// file: an implementation whose outcomes are steered by markers in a change URI
// has no business being selectable by a production config.
func newMergerFactory(ctx context.Context, logger *zap.Logger, scope tally.Scope) (merger.Factory, error) {
	switch impl := strings.ToLower(strings.TrimSpace(os.Getenv("MERGER"))); impl {
	case "fake":
		// Marker-driven outcomes, for e2e tests that need Runway to fail on
		// demand without a git checkout. Never production.
		logger.Info("MERGER=fake; using marker-driven fake merger for every queue")
		return &fakeMergerFactory{seq: new(atomic.Uint64)}, nil
	case "noop":
		logger.Info("MERGER=noop; using noop merger for every queue")
		return &noopMergerFactory{seq: new(atomic.Uint64)}, nil
	case "", "git":
		// Fall through to the configured per-queue merge targets.
	default:
		return nil, fmt.Errorf("invalid MERGER %q", impl)
	}

	cfg, err := loadMergeConfigFromEnv(logger)
	if err != nil {
		return nil, err
	}

	// The git runtime is resolved only when something actually needs it, so a
	// deployment running nothing but the noop merger does not require git to be
	// installed at all.
	var runtime gitmerger.GitRuntime
	if cfg.usesGit() {
		runtime, err = resolveGitRuntime(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve git runtime: %w", err)
		}
		logger.Info("git runtime resolved",
			zap.String("executable", runtime.Executable),
			zap.String("exec_path", runtime.ExecPath),
			zap.String("template_dir", runtime.TemplateDir),
		)
	}

	builder := &mergerBuilder{
		ctx:      ctx,
		logger:   logger,
		scope:    scope,
		runtime:  runtime,
		byTarget: make(map[string]merger.Factory),
		seq:      new(atomic.Uint64),
	}

	fallback, err := builder.build(cfg.Defaults.Merger, "defaults")
	if err != nil {
		return nil, err
	}

	byQueue := make(map[string]merger.Factory, len(cfg.Queues))
	for _, q := range cfg.Queues {
		if q.Merger == nil {
			continue
		}
		m, err := builder.build(*q.Merger, fmt.Sprintf("queue %q", q.Name))
		if err != nil {
			return nil, err
		}
		byQueue[q.Name] = m
	}

	logger.Info("mergers configured",
		zap.String("default_type", cfg.Defaults.Merger.Type),
		zap.Int("queue_overrides", len(byQueue)),
	)
	return mergerRegistry{byQueue: byQueue, fallback: fallback}, nil
}

// loadMergeConfigFromEnv reads the merge configuration file when one is
// configured, and otherwise reconstructs the equivalent single-queue
// configuration from the MERGE_* environment.
//
// The environment path predates the file and remains the shortest way to run
// one merge target, so it stays supported rather than being migrated away;
// expressing it as the same config type means there is still only one code path
// building mergers.
func loadMergeConfigFromEnv(logger *zap.Logger) (mergeConfig, error) {
	if path := os.Getenv("MERGE_CONFIG_PATH"); path != "" {
		cfg, err := loadMergeConfig(path)
		if err != nil {
			return mergeConfig{}, err
		}
		logger.Info("merge config loaded", zap.String("path", path), zap.Int("queues", len(cfg.Queues)))
		return cfg, nil
	}

	checkoutPath := os.Getenv("MERGE_CHECKOUT_PATH")
	if checkoutPath == "" {
		logger.Info("neither MERGE_CONFIG_PATH nor MERGE_CHECKOUT_PATH set; using noop merger")
		return mergeConfig{Defaults: queueMergeConfig{Merger: mergerConfig{Type: mergerTypeNoop}}}, nil
	}

	checkStaleness := envBool("MERGE_CHECK_STALENESS", true)
	cfg := mergeConfig{Defaults: queueMergeConfig{Merger: mergerConfig{
		Type:            mergerTypeGit,
		CheckoutPath:    checkoutPath,
		Remote:          envOr("MERGE_REMOTE", "origin"),
		Target:          envOr("MERGE_TARGET", "main"),
		DefaultStrategy: os.Getenv("MERGE_DEFAULT_STRATEGY"),
		CheckStaleness:  &checkStaleness,
		// Off by default: it lifts git's refusal to join two unrelated history
		// graphs, which is a safeguard everywhere except a queue whose purpose
		// is importing one repository's history into another.
		AllowUnrelatedHistories: envBool("MERGE_ALLOW_UNRELATED_HISTORIES", false),
		UpdateHeadBranch:        envBool("MERGE_UPDATE_HEAD_BRANCH", false),
		FetchRefspecs:           splitRefspecs(os.Getenv("MERGE_FETCH_REFSPECS")),
		CommitterName:           os.Getenv("MERGE_COMMITTER_NAME"),
		CommitterEmail:          os.Getenv("MERGE_COMMITTER_EMAIL"),
	}}}
	if err := cfg.normalizeAndValidate(); err != nil {
		return mergeConfig{}, fmt.Errorf("invalid MERGE_* environment: %w", err)
	}
	return cfg, nil
}

// mergerBuilder constructs merger factories, reusing one git instance per merge
// target.
//
// Sharing matters: a git merger serializes its own operations against the
// working tree it owns, so two queues landing on the same target must be the
// same instance to be serialized against each other. Two instances would hold
// separate locks over one working tree and reset it out from under each other
// mid-merge. A noop target has no such constraint and is built per queue, so it
// carries the queue's own config.
type mergerBuilder struct {
	ctx     context.Context
	logger  *zap.Logger
	scope   tally.Scope
	runtime gitmerger.GitRuntime
	// byTarget caches one merger per checkout path. Keying on the path alone is
	// safe only because validation has already rejected two queues that share a
	// checkout and disagree anywhere in their merger config: the cached instance
	// is built from whichever queue arrived first, so a divergent second queue
	// would silently run with the first one's settings. Widening what may share
	// a checkout means widening that check with it.
	byTarget map[string]merger.Factory
	// seq is the process-wide counter the noop mergers mint revision ids from,
	// held here so ids stay unique across every queue.
	seq *atomic.Uint64
}

func (b *mergerBuilder) build(cfg mergerConfig, where string) (merger.Factory, error) {
	if cfg.Type != mergerTypeGit {
		return &noopMergerFactory{seq: b.seq}, nil
	}

	if existing, ok := b.byTarget[cfg.CheckoutPath]; ok {
		return existing, nil
	}

	if cfg.provisions() {
		if err := provisionCheckout(b.ctx, b.logger.Sugar(), b.runtime, cfg); err != nil {
			return nil, fmt.Errorf("%s: failed to provision checkout: %w", where, err)
		}
	}

	m, err := gitmerger.NewMerger(gitmerger.Params{
		CheckoutPath:            cfg.CheckoutPath,
		Remote:                  cfg.Remote,
		Target:                  cfg.Target,
		DefaultStrategy:         cfg.strategy(),
		Runtime:                 b.runtime,
		MaxPushAttempts:         cfg.MaxPushAttempts,
		FetchRefspecs:           cfg.FetchRefspecs,
		CheckStaleness:          *cfg.CheckStaleness,
		UpdateHeadBranch:        cfg.UpdateHeadBranch,
		AllowUnrelatedHistories: cfg.AllowUnrelatedHistories,
		CommitterName:           cfg.CommitterName,
		CommitterEmail:          cfg.CommitterEmail,
		Logger:                  b.logger.Sugar(),
		MetricsScope:            b.scope,
	})
	if err != nil {
		return nil, fmt.Errorf("%s: failed to build git merger: %w", where, err)
	}

	b.logger.Info("git merger configured",
		zap.String("checkout", cfg.CheckoutPath),
		zap.String("target", cfg.Target),
		zap.String("default_strategy", cfg.strategy().String()),
		zap.Bool("update_head_branch", cfg.UpdateHeadBranch),
	)
	f := &gitMergerFactory{merger: m}
	b.byTarget[cfg.CheckoutPath] = f
	return f, nil
}

// gitMergerFactory returns one git-backed merger for every queue routed to it.
// The merger owns one checkout and serializes its own operations, so a single
// instance is shared — which is why this is the one merger factory that does
// not forward its Config. Queues landing on different targets get different
// instances of it, resolved through mergerRegistry.
type gitMergerFactory struct {
	merger merger.Merger
}

func (f *gitMergerFactory) For(_ merger.Config) (merger.Merger, error) {
	return f.merger, nil
}

// noopMergerFactory builds a noop merger per queue, bound to that queue's
// config. The synthetic revision-id counter is held here rather than on the
// merger so ids stay unique across every queue in the process.
type noopMergerFactory struct {
	seq *atomic.Uint64
}

func (f *noopMergerFactory) For(cfg merger.Config) (merger.Merger, error) {
	return noop.New(cfg, f.seq), nil
}

// fakeMergerFactory builds a fake merger per queue, bound to that queue's
// config. As with noop, the revision-id counter lives on the factory so ids
// stay unique for the lifetime of the process.
type fakeMergerFactory struct {
	seq *atomic.Uint64
}

func (f *fakeMergerFactory) For(cfg merger.Config) (merger.Merger, error) {
	return fake.New(cfg, f.seq), nil
}

// mergerRegistry resolves each queue's merger factory, falling back to the
// default for a queue with no entry of its own.
//
// It holds factories rather than mergers so the queue's Config reaches the
// implementation: whether an entry yields one shared instance or a fresh
// per-queue one is the factory's business, not the registry's.
type mergerRegistry struct {
	byQueue  map[string]merger.Factory
	fallback merger.Factory
}

func (r mergerRegistry) For(cfg merger.Config) (merger.Merger, error) {
	if f, ok := r.byQueue[cfg.QueueName]; ok {
		return f.For(cfg)
	}
	return r.fallback.For(cfg)
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
