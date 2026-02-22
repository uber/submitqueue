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
	queueSQL "github.com/uber/submitqueue/extension/queue/sql"
	"github.com/uber/submitqueue/extension/storage/mysql"
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
	if s.landController == nil {
		return nil, fmt.Errorf("land API unavailable (queue infrastructure not configured)")
	}
	return s.landController.Land(ctx, req)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Gateway server failure: %v\n", err)
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

	// Initialize MySQL storage
	mysqlDSN := os.Getenv("MYSQL_DSN")
	if mysqlDSN == "" {
		mysqlDSN = "root:root@tcp(localhost:3306)/submitqueue?parseTime=true"
	}
	store, err := mysql.NewStorage(mysql.MySQLParameters{
		DSN:             mysqlDSN,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 5 * time.Minute,
	})
	if err != nil {
		return fmt.Errorf("failed to create MySQL storage: %w", err)
	}
	defer store.Close()

	// Initialize MySQL counter
	counterDB, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		return fmt.Errorf("failed to open MySQL connection for counter: %w", err)
	}
	defer counterDB.Close()
	cnt := mysqlcounter.NewCounter(counterDB)

	// Initialize queue (optional - only if QUEUE_MYSQL_DSN is provided)
	// This allows the server to start without queue infrastructure for basic testing
	// If queue is not available, only the Ping API will work (Land API requires queue)
	queueDSN := os.Getenv("QUEUE_MYSQL_DSN")

	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Create controllers and wrap them for gRPC
	pingController := controller.NewPingController(logger, scope)

	var gatewayServer *GatewayServer
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

		// Land controller requires queue publisher
		landController := controller.NewLandController(logger.Sugar(), scope, store, cnt, q.Publisher())
		gatewayServer = &GatewayServer{
			pingController: pingController,
			landController: landController,
		}
	} else {
		logger.Warn("queue infrastructure disabled (QUEUE_MYSQL_DSN not set) - only Ping API available")
		// Only Ping controller available without queue
		gatewayServer = &GatewayServer{
			pingController: pingController,
			landController: nil, // Land API will return errors if called
		}
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
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigCh:
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
