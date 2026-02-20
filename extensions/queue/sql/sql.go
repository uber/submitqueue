package sql

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"

	"github.com/uber/submitqueue/extensions/queue"
)

type queueImpl struct {
	publisher  queue.Publisher
	subscriber queue.Subscriber
	closed     bool
}

// Params holds dependencies for creating a SQL queue factory
type Params struct {
	// DB is the database connection (required)
	DB *sql.DB

	// Logger for debugging and observability (required)
	Logger *zap.Logger

	// MetricsScope for metrics collection (required)
	MetricsScope tally.Scope

	// Config holds queue configuration
	Config Config
}

// NewQueue creates a new SQL-based queue factory
func NewQueue(params Params) (queue.Queue, error) {
	if err := params.Config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// Test connection
	if err := params.DB.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	logger := params.Logger.Sugar().Named("queue.sql")
	logger.Infow("created SQL queue factory",
		"consumer_group", params.Config.ConsumerGroup,
		"worker_id", params.Config.WorkerID,
		"poll_interval", params.Config.PollInterval,
		"batch_size", params.Config.BatchSize,
	)

	// Create stores
	messageStore := newMessageStore(params.DB, params.Config, params.Logger, params.MetricsScope)
	offsetStore := newOffsetStore(params.DB, params.Config, params.Logger, params.MetricsScope)
	leaseStore := newPartitionLeaseStore(params.DB, params.Config, params.Logger, params.MetricsScope)

	queueMetrics := params.MetricsScope.SubScope("queue")

	// Create publisher and subscriber
	publisher := NewPublisher(
		params.Config,
		logger.Named("publisher"),
		queueMetrics.SubScope("publisher"),
		messageStore,
	)

	subscriber := NewSubscriber(
		params.Config,
		logger.Named("subscriber"),
		queueMetrics.SubScope("subscriber"),
		messageStore,
		offsetStore,
		leaseStore,
	)

	return &queueImpl{
		publisher:  publisher,
		subscriber: subscriber,
		closed:     false,
	}, nil
}

// Publisher returns a Publisher instance
func (q *queueImpl) Publisher() queue.Publisher {
	return q.publisher
}

// Subscriber returns a Subscriber instance
func (q *queueImpl) Subscriber() queue.Subscriber {
	return q.subscriber
}

// Close shuts down the factory and all associated resources
func (q *queueImpl) Close() error {
	if q.closed {
		return nil
	}
	q.closed = true

	// Close subscriber and publisher
	var errs []error

	if err := q.subscriber.Close(); err != nil {
		errs = append(errs, fmt.Errorf("subscriber close failed: %w", err))
	}

	if err := q.publisher.Close(); err != nil {
		errs = append(errs, fmt.Errorf("publisher close failed: %w", err))
	}

	return errors.Join(errs...)
}
