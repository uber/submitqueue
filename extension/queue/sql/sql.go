package sql

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"

	"github.com/uber/submitqueue/extension/queue"
)

type queueImpl struct {
	publisher  queue.Publisher
	subscriber queue.Subscriber
	closed     bool
}

// Params holds dependencies for creating a SQL queue
type Params struct {
	// DB is the database connection (required)
	DB *sql.DB

	// Logger for debugging and observability (required)
	Logger *zap.Logger

	// MetricsScope for metrics collection (required)
	MetricsScope tally.Scope
}

// NewQueue creates a new SQL-based queue
func NewQueue(params Params) (queue.Queue, error) {
	// Test connection
	if err := params.DB.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	logger := params.Logger.Sugar().Named("queue.sql")
	logger.Info("created SQL queue")

	// Create stores
	messageStore := newMessageStore(params.DB, params.Logger, params.MetricsScope)
	offsetStore := newOffsetStore(params.DB, params.Logger, params.MetricsScope)
	leaseStore := newPartitionLeaseStore(params.DB, params.Logger, params.MetricsScope)

	queueMetrics := params.MetricsScope.SubScope("queue")

	// Create publisher and subscriber
	publisher := NewPublisher(
		logger.Named("publisher"),
		queueMetrics.SubScope("publisher"),
		messageStore,
	)

	subscriber := NewSubscriber(
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

// Close shuts down the queue and all associated resources
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
