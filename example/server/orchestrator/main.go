package main

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	extqueue "github.com/uber/submitqueue/extension/queue"
	queueSQL "github.com/uber/submitqueue/extension/queue/sql"
	"github.com/uber/submitqueue/orchestrator/controller"
	"github.com/uber/submitqueue/orchestrator/controller/request"
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
	sqlQueue, err := queueSQL.NewQueue(queueSQL.Params{
		DB:           queueDB,
		Logger:       logger,
		MetricsScope: scope.SubScope("queue"),
	})
	if err != nil {
		return fmt.Errorf("failed to create queue: %w", err)
	}
	defer sqlQueue.Close()

	logger.Info("initialized queue", zap.String("dsn", queueDSN))

	// Create topic registry
	subscriberName := os.Getenv("HOSTNAME")
	if subscriberName == "" {
		subscriberName = fmt.Sprintf("orchestrator-%d", time.Now().Unix())
	}

	registry := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{
			{Topic: consumer.TopicRequest, Queue: sqlQueue},
			{Topic: consumer.TopicToBatch, Queue: sqlQueue},
		},
		[]extqueue.SubscriptionConfig{
			extqueue.DefaultSubscriptionConfig(
				consumer.TopicRequest.String(),
				subscriberName,
				"orchestrator-request",
			),
		},
	)

	// Create consumer
	c := consumer.New(logger.Sugar(), scope.SubScope("consumer"), registry)

	// Register request controller
	// Pipeline: request → batch → speculation → build → merge
	requestController := request.NewController(
		logger.Sugar(),
		scope,
		registry,
		consumer.TopicRequest,
		"orchestrator-request",
	)
	if err := c.Register(requestController); err != nil {
		return fmt.Errorf("failed to register request controller: %w", err)
	}

	logger.Info("controllers registered", zap.Int("count", 1))

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
