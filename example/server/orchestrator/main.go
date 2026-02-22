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
	"github.com/uber/submitqueue/consumer"
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

	// Initialize queue (optional - only if QUEUE_MYSQL_DSN is provided)
	// This allows the server to start without queue infrastructure for basic testing
	queueDSN := os.Getenv("QUEUE_MYSQL_DSN")
	var c consumer.Consumer
	if queueDSN != "" {
		queueDB, err := sql.Open("mysql", queueDSN)
		if err != nil {
			return fmt.Errorf("failed to open MySQL connection for queue: %w", err)
		}
		defer queueDB.Close()

		q, err := queueSQL.NewQueue(queueSQL.Params{
			DB:           queueDB,
			Logger:       logger,
			MetricsScope: scope.SubScope("queue"),
		})
		if err != nil {
			return fmt.Errorf("failed to create queue: %w", err)
		}
		defer q.Close()

		logger.Info("queue initialized", zap.String("dsn", queueDSN))

		// Create consumer
		subscriberName := os.Getenv("HOSTNAME")
		if subscriberName == "" {
			subscriberName = fmt.Sprintf("orchestrator-%d", time.Now().Unix())
		}

		c = consumer.New(logger.Sugar(), scope.SubScope("consumer"), q, subscriberName)

		// Register handlers for the pipeline
		requestHandler := request.NewController(logger.Sugar(), scope)
		if err := c.Register(requestHandler); err != nil {
			return fmt.Errorf("failed to register request handler: %w", err)
		}

		logger.Info("handlers registered", zap.Int("count", 1))

		// Start consumers
		ctx := context.Background()

		if err := c.Start(ctx); err != nil {
			return fmt.Errorf("failed to start consumers: %w", err)
		}

		logger.Info("consumer started")
	} else {
		logger.Warn("queue infrastructure disabled (QUEUE_MYSQL_DSN not set)")
	}

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
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigCh:
		fmt.Println("\nShutting down orchestrator server...")
		if c != nil {
			c.Stop(30000) // Stop consumers with 30s timeout
		}
		grpcServer.GracefulStop()
		_ = <-serverErrCh // Wait for the server to exit and ignore the error
	case errCh := <-serverErrCh:
		if errCh != nil {
			err = fmt.Errorf("\nServer exited with error: %w\n", errCh)
		}
		if c != nil {
			c.Stop(30000) // Stop consumers with 30s timeout
		}
	}

	return err
}
