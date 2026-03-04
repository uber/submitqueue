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
	mysqlcounter "github.com/uber/submitqueue/extension/counter/mysql"
	queueMySQL "github.com/uber/submitqueue/extension/queue/mysql"
	mysqlstorage "github.com/uber/submitqueue/extension/storage/mysql"
	"github.com/uber/submitqueue/gateway/controller"
	pb "github.com/uber/submitqueue/gateway/protopb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// GatewayServer wraps the controller and implements the gRPC service interface
type GatewayServer struct {
	pb.UnimplementedSubmitQueueGatewayServer
	pingController *controller.PingController
	landController *controller.LandController
}

// Ping delegates to the controller
func (s *GatewayServer) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return s.pingController.Ping(ctx, req)
}

// Land delegates to the controller
func (s *GatewayServer) Land(ctx context.Context, req *pb.LandRequest) (*pb.LandResponse, error) {
	return s.landController.Land(ctx, req)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Gateway server failure: %v\n", err)
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
	scope := tally.NewTestScope("gateway", nil)
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

	// Open application database connection
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

	// Initialize counter from shared app database connection
	cnt := mysqlcounter.NewCounter(appDB, scope.SubScope("counter"))

	// Open queue database connection
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

	logger.Info("initialized dependencies",
		zap.String("app_dsn", appDSN),
		zap.String("queue_dsn", queueDSN),
	)

	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Initialize request log store from shared app database connection
	requestLogStore := mysqlstorage.NewRequestLogStore(appDB, scope.SubScope("request_log_store"))

	// Create controllers and wrap them for gRPC
	pingController := controller.NewPingController(logger, scope)
	landController := controller.NewLandController(logger.Sugar(), scope, cnt, mysqlQueue.Publisher(), requestLogStore, "request")
	gatewayServer := &GatewayServer{
		pingController: pingController,
		landController: landController,
	}

	pb.RegisterSubmitQueueGatewayServer(grpcServer, gatewayServer)

	// Register reflection service for debugging with grpcurl
	reflection.Register(grpcServer)

	// Listen on configurable port
	port := os.Getenv("PORT")
	if port == "" {
		port = ":8081"
	}
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %w", port, err)
	}

	fmt.Printf("Gateway gRPC server is running on %s\n", port)
	fmt.Println("Press Ctrl+C to stop.")

	// Start server in a goroutine and wait for it to finish
	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- grpcServer.Serve(listener)
	}()

	// Wait for interrupt signal or server exit
	select {
	case <-ctx.Done():
		fmt.Println("\nShutting down gateway server...")
		grpcServer.GracefulStop()
		_ = <-serverErrCh // Wait for the server to exit and ignore the error
	case errCh := <-serverErrCh:
		if errCh != nil {
			err = fmt.Errorf("\nServer exited with error: %w\n", errCh)
		}
	}

	return err
}
