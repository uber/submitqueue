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
	nethttp "net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/oauth2"

	"github.com/uber-go/tally"
	pb "github.com/uber/submitqueue/api/submitqueue/orchestrator/protopb"
	genericerrs "github.com/uber/submitqueue/platform/errs/generic"
	mysqlerrs "github.com/uber/submitqueue/platform/errs/mysql"
	mysqlcounter "github.com/uber/submitqueue/platform/extension/counter/mysql"
	queueMySQL "github.com/uber/submitqueue/platform/extension/messagequeue/mysql"
	"github.com/uber/submitqueue/platform/http"
	"github.com/uber/submitqueue/platform/pipeline"
	"github.com/uber/submitqueue/submitqueue/core/changeset"
	"github.com/uber/submitqueue/submitqueue/extension/changeprovider"
	cpfake "github.com/uber/submitqueue/submitqueue/extension/changeprovider/fake"
	githubprovider "github.com/uber/submitqueue/submitqueue/extension/changeprovider/github"
	phabprovider "github.com/uber/submitqueue/submitqueue/extension/changeprovider/phabricator"
	routingprovider "github.com/uber/submitqueue/submitqueue/extension/changeprovider/routing"
	mysqlstorage "github.com/uber/submitqueue/submitqueue/extension/storage/mysql"
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

	// Subscriber name for consumer group identity
	subscriberName := os.Getenv("HOSTNAME")
	if subscriberName == "" {
		subscriberName = fmt.Sprintf("orchestrator-%d", time.Now().Unix())
	}

	// Build per-queue extension profiles (host-private). Each queue resolves
	// to its own set of extension implementations (conflict analyzer, …),
	// falling back to a baseline profile for queues without an explicit entry.
	profiles, err := newProfiles(logger, scope, changeset.New(store.GetRequestStore(), store.GetChangeStore()))
	if err != nil {
		return fmt.Errorf("failed to build profiles: %w", err)
	}

	// Populate the orchestrator's Deps — the library's public API. Factory
	// fields are thin adapters that cross the host/library boundary via the
	// existing Factory interfaces.
	deps := orchestrator.Deps{
		Logger:         logger.Sugar(),
		Scope:          scope,
		Storage:        store,
		Counter:        cnt,
		BuildRunner:    profiles.BuildRunnerFactory(),
		ChangeProvider: profiles.ChangeProviderFactory(),
		Analyzer:       profiles.AnalyzerFactory(),
		Validator:      validatorfake.NewFactory(),
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

// newChangeProvider creates a routing ChangeProvider containing GitHub and Phab ChangeProviders.
// When neither GITHUB_TOKEN nor PHAB_API_TOKEN is set, falls back to the fake change provider.
func newChangeProvider(logger *zap.Logger, scope tally.Scope) (changeprovider.ChangeProvider, error) {
	ghProvider, err := newGitHubChangeProvider(logger, scope)
	if err != nil {
		return nil, err
	}

	phabProvider, err := newPhabChangeProvider(logger, scope)
	if err != nil {
		return nil, err
	}

	if ghProvider == nil && phabProvider == nil {
		logger.Warn("no change provider tokens set; using fake change provider (empty change info unless URI-marked)")
		return cpfake.New(), nil
	}

	routingProvider, err := routingprovider.NewProvider(routingprovider.Params{
		GitHub:      ghProvider,
		Phabricator: phabProvider,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create routing change provider: %w", err)
	}
	return routingProvider, nil
}

// newGitHubChangeProvider creates a GitHub ChangeProvider configured via
// GITHUB_BASE_URL, GITHUB_TOKEN, and GITHUB_TIMEOUT. Returns nil when
// GITHUB_TOKEN is unset.
func newGitHubChangeProvider(logger *zap.Logger, scope tally.Scope) (changeprovider.ChangeProvider, error) {
	if os.Getenv("GITHUB_TOKEN") == "" {
		return nil, nil
	}

	client, err := http.NewClient(getEnv("GITHUB_BASE_URL", "https://api.github.com"))
	if err != nil {
		return nil, fmt.Errorf("failed to build GitHub HTTP client: %w", err)
	}

	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: os.Getenv("GITHUB_TOKEN")})
	client.Transport = &oauth2.Transport{Source: ts, Base: client.Transport}
	client.Timeout = parseTimeout(os.Getenv("GITHUB_TIMEOUT"), 30*time.Second)

	return githubprovider.NewProvider(githubprovider.Params{
		HTTPClient:   client,
		Logger:       logger.Sugar(),
		MetricsScope: scope.SubScope("changeprovider.github"),
	}), nil
}

// apiTokenTransport injects a Phabricator API token as a query parameter in each request.
type apiTokenTransport struct {
	token string
	next  nethttp.RoundTripper
}

func (t *apiTokenTransport) RoundTrip(req *nethttp.Request) (*nethttp.Response, error) {
	r := req.Clone(req.Context())
	q := r.URL.Query()
	q.Set("api.token", t.token)
	r.URL.RawQuery = q.Encode()
	return t.next.RoundTrip(r)
}

// newPhabChangeProvider creates a Phabricator ChangeProvider configured via PHAB_API_ENDPOINT and PHAB_API_TOKEN.
// Returns nil when PHAB_API_TOKEN or PHAB_API_ENDPOINT are unset.
func newPhabChangeProvider(logger *zap.Logger, scope tally.Scope) (changeprovider.ChangeProvider, error) {
	token := os.Getenv("PHAB_API_TOKEN")
	if token == "" {
		return nil, nil
	}

	endpoint := os.Getenv("PHAB_API_ENDPOINT")
	if endpoint == "" {
		return nil, nil
	}

	client, err := http.NewClient(endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to build Phabricator HTTP client: %w", err)
	}

	baseTransport := client.Transport.(*http.BaseURLTransport)
	baseTransport.Next = &apiTokenTransport{
		token: token,
		next:  baseTransport.Next,
	}

	return phabprovider.NewProvider(phabprovider.Params{
		HTTPClient:   client,
		Logger:       logger.Sugar(),
		MetricsScope: scope.SubScope("changeprovider.phabricator"),
	}), nil
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
