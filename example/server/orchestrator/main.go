package main

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/core/speculation"
	"github.com/uber/submitqueue/extension/counter"
	mysqlcounter "github.com/uber/submitqueue/extension/counter/mysql"
	"github.com/uber/submitqueue/extension/mergechecker"
	githubchecker "github.com/uber/submitqueue/extension/mergechecker/github"
	extqueue "github.com/uber/submitqueue/extension/queue"
	queueMySQL "github.com/uber/submitqueue/extension/queue/mysql"
	"github.com/uber/submitqueue/orchestrator/controller"
	"github.com/uber/submitqueue/orchestrator/controller/batch"
	"github.com/uber/submitqueue/orchestrator/controller/build"
	"github.com/uber/submitqueue/orchestrator/controller/buildsignal"
	"github.com/uber/submitqueue/orchestrator/controller/finalize"
	"github.com/uber/submitqueue/orchestrator/controller/merge"
	"github.com/uber/submitqueue/orchestrator/controller/mergesignal"
	"github.com/uber/submitqueue/orchestrator/controller/request"
	"github.com/uber/submitqueue/orchestrator/controller/speculate"
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
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Orchestrator server failure: %v\n", err)
		os.Exit(1)
	}
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

	cnt := mysqlcounter.NewCounter(appDB)

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

	// Create consumer
	c := consumer.New(logger.Sugar(), scope.SubScope("consumer"), registry)

	// Create merge checker
	mc := newMergeChecker(logger, scope)

	// Create speculation strategy (top-K with default probabilities).
	strategy := speculation.NewTopKStrategy(nil, speculation.DefaultK)

	// Register controllers
	if err := registerControllers(c, logger.Sugar(), scope, registry, mc, cnt, strategy); err != nil {
		return err
	}

	logger.Info("controllers registered", zap.Int("count", 8))

	// Start consumers
	if err := c.Start(ctx); err != nil {
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
	fmt.Println("Press Ctrl+C to stop.")

	// Start server in a goroutine and wait for it to finish
	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- grpcServer.Serve(listener)
	}()

	// Wait for interrupt signal or server exit
	select {
	case <-ctx.Done():
		fmt.Println("\nShutting down orchestrator server...")
		c.Stop(30000) // Stop consumers with 30s timeout
		grpcServer.GracefulStop()
		_ = <-serverErrCh // Wait for the server to exit and ignore the error
	case errCh := <-serverErrCh:
		if errCh != nil {
			err = fmt.Errorf("\nServer exited with error: %w\n", errCh)
		}
		c.Stop(30000) // Stop consumers with 30s timeout
	}

	return err
}

// newTopicRegistry builds the TopicRegistry with all topic and subscription configs.
func newTopicRegistry(q extqueue.Queue, subscriberName string) (consumer.TopicRegistry, error) {
	return consumer.NewTopicRegistry([]consumer.TopicConfig{
		{
			Key:   consumer.TopicKeyRequest,
			Name:  "request",
			Queue: q,
			Subscription: extqueue.DefaultSubscriptionConfig(
				subscriberName, "orchestrator-request",
			),
		},
		{
			Key:   consumer.TopicKeyToBatch,
			Name:  "to-batch",
			Queue: q,
			Subscription: extqueue.DefaultSubscriptionConfig(
				subscriberName, "orchestrator-batch",
			),
		},
		{
			Key:   consumer.TopicKeyBatched,
			Name:  "batched",
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
			Name:  "build-signal",
			Queue: q,
			Subscription: extqueue.DefaultSubscriptionConfig(
				subscriberName, "orchestrator-buildsignal",
			),
		},
		{
			Key:   consumer.TopicKeyToMerge,
			Name:  "to-merge",
			Queue: q,
			Subscription: extqueue.DefaultSubscriptionConfig(
				subscriberName, "orchestrator-merge",
			),
		},
		{
			Key:   consumer.TopicKeyMergeSignal,
			Name:  "merge-signal",
			Queue: q,
			Subscription: extqueue.DefaultSubscriptionConfig(
				subscriberName, "orchestrator-mergesignal",
			),
		},
		{
			Key:   consumer.TopicKeyFinalize,
			Name:  "finalize",
			Queue: q,
			Subscription: extqueue.DefaultSubscriptionConfig(
				subscriberName, "orchestrator-finalize",
			),
		},
	})
}

// registerControllers creates all pipeline controllers and registers them with the consumer.
// Pipeline: request → batch → speculate → build → build-signal
//
//	→ merge → merge-signal
//	finalize (terminal)

func registerControllers(c consumer.Consumer, logger *zap.SugaredLogger, scope tally.Scope, registry consumer.TopicRegistry, mc mergechecker.MergeChecker, cnt counter.Counter, strategy speculation.Strategy) error {
	requestController := request.NewController(
		logger,
		scope,
		registry,
		mc,
		consumer.TopicKeyRequest,
		"orchestrator-request",
	)
	if err := c.Register(requestController); err != nil {
		return fmt.Errorf("failed to register request controller: %w", err)
	}

	batchController := batch.NewController(
		logger,
		scope,
		registry,
		cnt,
		consumer.TopicKeyToBatch,
		"orchestrator-batch",
	)
	if err := c.Register(batchController); err != nil {
		return fmt.Errorf("failed to register batch controller: %w", err)
	}

	speculateController := speculate.NewController(
		logger,
		scope,
		registry,
		strategy,
		consumer.TopicKeyBatched,
		"orchestrator-speculate",
	)
	if err := c.Register(speculateController); err != nil {
		return fmt.Errorf("failed to register speculate controller: %w", err)
	}

	buildController := build.NewController(
		logger,
		scope,
		registry,
		consumer.TopicKeyBuild,
		"orchestrator-build",
	)
	if err := c.Register(buildController); err != nil {
		return fmt.Errorf("failed to register build controller: %w", err)
	}

	buildSignalController := buildsignal.NewController(
		logger,
		scope,
		registry,
		consumer.TopicKeyBuildSignal,
		"orchestrator-buildsignal",
	)
	if err := c.Register(buildSignalController); err != nil {
		return fmt.Errorf("failed to register buildsignal controller: %w", err)
	}

	mergeController := merge.NewController(
		logger,
		scope,
		registry,
		consumer.TopicKeyToMerge,
		"orchestrator-merge",
	)
	if err := c.Register(mergeController); err != nil {
		return fmt.Errorf("failed to register merge controller: %w", err)
	}

	mergeSignalController := mergesignal.NewController(
		logger,
		scope,
		registry,
		consumer.TopicKeyMergeSignal,
		"orchestrator-mergesignal",
	)
	if err := c.Register(mergeSignalController); err != nil {
		return fmt.Errorf("failed to register mergesignal controller: %w", err)
	}

	finalizeController := finalize.NewController(
		logger,
		scope,
		registry,
		consumer.TopicKeyFinalize,
		"orchestrator-finalize",
	)
	if err := c.Register(finalizeController); err != nil {
		return fmt.Errorf("failed to register finalize controller: %w", err)
	}

	return nil
}

// newMergeChecker creates a MergeChecker for GitHub (github.com).
// Configured via GITHUB_TOKEN and GITHUB_GRAPHQL_URL environment variables.
func newMergeChecker(logger *zap.Logger, scope tally.Scope) mergechecker.MergeChecker {
	graphQLURL := os.Getenv("GITHUB_GRAPHQL_URL")
	if graphQLURL == "" {
		graphQLURL = "https://api.github.com/graphql"
	}

	httpClient := &http.Client{}
	if token := os.Getenv("GITHUB_TOKEN"); token != "" {
		httpClient.Transport = &bearerTransport{token: token}
	}

	github := githubchecker.NewMergeChecker(githubchecker.Params{
		HTTPClient:   httpClient,
		GraphQLURL:   graphQLURL,
		Logger:       logger.Sugar(),
		MetricsScope: scope.SubScope("mergechecker"),
	})

	return mergechecker.NewMultiChecker(map[string]mergechecker.MergeChecker{
		"github": github,
	})
}

// bearerTransport is an http.RoundTripper that adds a Bearer token to requests.
type bearerTransport struct {
	token string
}

func (t *bearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer "+t.token)
	return http.DefaultTransport.RoundTrip(req)
}
