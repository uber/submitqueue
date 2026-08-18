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
	pb "github.com/uber/submitqueue/api/submitqueue/orchestrator/protopb"
	genericerrs "github.com/uber/submitqueue/platform/errs/generic"
	mysqlerrs "github.com/uber/submitqueue/platform/errs/mysql"
	"github.com/uber/submitqueue/platform/extension/consumergate"
	consumergatefile "github.com/uber/submitqueue/platform/extension/consumergate/file"
	consumergatenoop "github.com/uber/submitqueue/platform/extension/consumergate/noop"
	"github.com/uber/submitqueue/platform/extension/counter"
	mysqlcounter "github.com/uber/submitqueue/platform/extension/counter/mysql"
	hooknoop "github.com/uber/submitqueue/platform/extension/hook/noop"
	queueMySQL "github.com/uber/submitqueue/platform/extension/messagequeue/mysql"
	"github.com/uber/submitqueue/platform/pipeline"
	"github.com/uber/submitqueue/submitqueue/core/changeset"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	mysqlstorage "github.com/uber/submitqueue/submitqueue/extension/storage/mysql"
	"github.com/uber/submitqueue/submitqueue/extension/validator"
	validatorfake "github.com/uber/submitqueue/submitqueue/extension/validator/fake"
	"github.com/uber/submitqueue/submitqueue/orchestrator"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// OrchestratorServer wraps the controller and implements the gRPC service interface
type OrchestratorServer struct {
	pb.UnimplementedSubmitQueueOrchestratorServer
	controllers orchestrator.Controllers
}

// Ping delegates to the controller
func (s *OrchestratorServer) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return s.controllers.Ping.Ping(ctx, req)
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

	cnt := counterFactory{db: appDB, scope: scope.SubScope("counter")}

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

	// Subscriber name for consumer group identity
	subscriberName := os.Getenv("HOSTNAME")
	if subscriberName == "" {
		subscriberName = fmt.Sprintf("orchestrator-%d", time.Now().Unix())
	}

	// Build per-queue extension profiles (host-private). Each queue resolves
	// to its own set of extension implementations (conflict analyzer, …),
	// falling back to a baseline profile for queues without an explicit entry.
	storageFty := storageFactory{backend: store}
	profilesCfg, err := loadProfilesConfigFromEnv(logger)
	if err != nil {
		return fmt.Errorf("failed to load extension profiles: %w", err)
	}
	profiles, err := newProfiles(logger, scope, changeset.New(storageFty), storageFty, profilesCfg)
	if err != nil {
		return fmt.Errorf("failed to build profiles: %w", err)
	}

	// Populate the orchestrator's Deps — the library's public API. Factory
	// fields are thin adapters that cross the host/library boundary via the
	// existing Factory interfaces.
	deps := orchestrator.Deps{
		Logger:         logger.Sugar(),
		Scope:          scope,
		Storage:        profiles.StorageFactory(),
		Counter:        cnt,
		BuildRunner:    profiles.BuildRunnerFactory(),
		ChangeProvider: profiles.ChangeProviderFactory(),
		Analyzer:       profiles.AnalyzerFactory(),
		Speculator:     profiles.SpeculatorFactory(),
		Validator:      validatorFactory{},
		Hook:           hooknoop.New(),
	}

	// Assemble the pipeline: one call builds the topic registry, creates
	// primary and DLQ consumers, eagerly constructs all controllers, and
	// returns a single lifecycle.Component the host drives with Start/Stop.
	pl, err := pipeline.Construct(
		logger.Sugar(),
		scope,
		mysqlQueue,
		subscriberName,
		deps,
		orchestrator.Stages,
		pipeline.PublishOnly(orchestrator.PublishOnlyTopics...),
		pipeline.Classifiers(
			genericerrs.Classifier,
			// Storage (submitqueue/extension/storage/mysql) and queue
			// (platform/extension/messagequeue/mysql) both run on the same
			// MySQL driver, so a single classifier covers errors surfaced
			// from either backend.
			mysqlerrs.Classifier,
		),
		pipeline.Gate(newConsumerGate(logger)),
	)
	if err != nil {
		return fmt.Errorf("failed to construct pipeline: %w", err)
	}

	// Start the pipeline (extra components → primary consumer → DLQ consumer).
	if err := pl.Start(ctx); err != nil {
		return fmt.Errorf("failed to start pipeline: %w", err)
	}
	logger.Info("pipeline started")

	// Create gRPC server and wire RPC controllers
	grpcServer := grpc.NewServer()

	ctls := orchestrator.NewControllers(deps)
	orchestratorServer := &OrchestratorServer{controllers: ctls}
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
	var serverErr error
	select {
	case <-ctx.Done():
		fmt.Println("Shutting down orchestrator server due to interruption signal...")

		// Set the error to the context cancellation error to be surfaced as a desired exit code by the main function
		err = ctx.Err()

		// Stop GRPC server and wait for it to exit
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

	// Stop the pipeline (DLQ consumer → primary consumer → extra components,
	// reverse of start order). Use a fresh context with a 30s timeout so
	// shutdown proceeds even after the parent context is cancelled.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer stopCancel()
	plStopErr := pl.Stop(stopCtx)
	if plStopErr != nil {
		plStopErr = fmt.Errorf("failed to stop pipeline: %w", plStopErr)
	}

	if plStopErr != nil || serverErr != nil {
		// Override context cancellation error with the shutdown error
		err = errors.Join(plStopErr, serverErr)
	}

	// Return the error to be surfaced as a desired exit code by the main function
	return err
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

// loadProfilesConfigFromEnv reads the extension profiles file when one is
// configured, and otherwise returns the built-in example topology.
//
// Keeping the built-in as the fallback means a deployment that sets no config
// path — including the compose stack and the e2e suite — gets exactly the
// behavior it had before profiles became configurable.
func loadProfilesConfigFromEnv(logger *zap.Logger) (profilesConfig, error) {
	path := os.Getenv("PROFILES_CONFIG_PATH")
	if path == "" {
		logger.Info("PROFILES_CONFIG_PATH not set; using built-in example profiles")
		return defaultProfilesConfig(), nil
	}
	cfg, err := loadProfilesConfig(path)
	if err != nil {
		return profilesConfig{}, err
	}
	logger.Info("extension profiles loaded", zap.String("path", path), zap.Int("queues", len(cfg.Queues)))
	return cfg, nil
}

// defaultProfilesConfig is the example topology used when no configuration file
// is supplied: fake edge integrations everywhere, with the conflict analyzer
// varied per queue to exercise its different behaviors.
//
// The change provider is routing rather than fake so that setting a provider token
// in the environment is enough to reach a real provider, as it has always been —
// routing falls back to the fake provider on its own when no token is set.
func defaultProfilesConfig() profilesConfig {
	provider := changeProviderConfig{
		Type:   changeProviderTypeRouting,
		GitHub: &githubProviderConfig{TokenEnv: defaultGitHubTokenEnv, BaseURL: getEnv("GITHUB_BASE_URL", defaultGitHubBaseURL), Timeout: os.Getenv("GITHUB_TIMEOUT")},
	}
	// Phabricator needs an endpoint as well as a token, and has no default one
	// to fall back on, so it joins the routing set only once told where to go.
	if endpoint := os.Getenv("PHAB_API_ENDPOINT"); endpoint != "" {
		provider.Phabricator = &phabProviderConfig{TokenEnv: defaultPhabTokenEnv, Endpoint: endpoint}
	}

	cfg := profilesConfig{
		Defaults: queueProfileConfig{
			ChangeProvider: provider,
			BuildRunner:    buildRunnerConfig{Type: buildRunnerTypeFake},
			Analyzer:       analyzerConfig{Type: analyzerTypeAll},
		},
		Queues: []namedQueueProfileConfig{
			// Bucketed scoring: smaller batches are likelier to land, so they
			// rank ahead of larger ones. Conflicts stay conservative.
			{Name: "test-queue", Scorer: &scorerConfig{
				Type: scorerTypeHeuristic,
				Buckets: []bucketConfig{
					{Min: 0, Max: 1, Score: 0.95},
					{Min: 2, Max: 5, Score: 0.80},
					{Min: 6, Max: 20, Score: 0.60},
					{Min: 21, Max: maxBucket, Score: 0.40},
				},
			}},
			// Maximum parallelism: nothing ever conflicts. Scored by a
			// composite, which exercises the combining path.
			{Name: "e2e-test-queue",
				Analyzer: &analyzerConfig{Type: analyzerTypeNone},
				Scorer: &scorerConfig{
					Type:    scorerTypeComposite,
					Combine: combineAvg,
					Components: map[string]scorerConfig{
						"size": {Type: scorerTypeHeuristic, Buckets: []bucketConfig{{Min: 0, Max: maxBucket, Score: 0.8}}},
						"flat": {Type: scorerTypeHeuristic, Buckets: []bucketConfig{{Min: 0, Max: maxBucket, Score: 0.6}}},
					},
				},
			},
			// Every analysis fails, exercising the analyzer error path.
			{Name: "e2e-conflict-error-queue", Analyzer: &analyzerConfig{Type: analyzerTypeAll, FailAlways: true}},
			// Serializes only batches changing the same file.
			{Name: "file-overlap-queue", Analyzer: &analyzerConfig{Type: analyzerTypePathOverlap, By: pathOverlapByFile}},
		},
	}
	// Built from constants, so a validation failure here is a programming error
	// rather than a deployment one; normalizing keeps it on the same path as a
	// file-supplied config.
	if err := cfg.normalizeAndValidate(); err != nil {
		panic(fmt.Sprintf("built-in profiles are invalid: %v", err))
	}
	return cfg
}

// getEnv returns environment variable value or default if not set.
func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

// storageFactory adapts the MySQL storage backend's queue binding to the
// storage.Factory seam. Routing every queue to the single shared backend is
// this host's policy; a deployment that splits queues across backends swaps
// this adapter for a routing one.
type storageFactory struct {
	backend *mysqlstorage.Storage
}

// For returns the queue-scoped store aggregate bound to the queue named in config.
func (f storageFactory) For(config storage.Config) (storage.Storage, error) {
	return f.backend.For(config.QueueName)
}

// counterFactory adapts the MySQL counter backend to the counter.Factory seam.
// Routing every queue to the single shared database is this host's policy; a
// deployment that splits queues across backends swaps this adapter for a
// routing one.
type counterFactory struct {
	db    *sql.DB
	scope tally.Scope
}

// For returns the Counter bound to the queue named in config.
func (f counterFactory) For(config counter.Config) (counter.Counter, error) {
	if config.QueueName == "" {
		return nil, fmt.Errorf("queue name must not be empty")
	}
	return mysqlcounter.NewCounter(f.db, f.scope, config.QueueName), nil
}

// validatorFactory routes every queue to the always-passing fake validator.
// Choosing an impl per queue is host policy, so the adapter lives here rather
// than in the extension package. A deployment with real validation swaps this
// for a routing adapter.
type validatorFactory struct{}

// For returns the Validator bound to the queue named in config.
func (validatorFactory) For(config validator.Config) (validator.Validator, error) {
	return validatorfake.New(config), nil
}
