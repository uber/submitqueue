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

package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/zap"

	extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"
)

type queueImpl struct {
	publisher  extqueue.Publisher
	subscriber extqueue.Subscriber
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

	// OnSignal receives typed subscriber lifecycle signals (HookSignal).
	// Nil in production; used by integration tests for event-driven waits.
	OnSignal chan HookSignal
}

// NewQueue creates a new SQL-based queue
func NewQueue(params Params) (extqueue.Queue, error) {
	// Test connection
	if err := params.DB.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	logger := params.Logger.Sugar().Named("queue_mysql")
	logger.Infow("created SQL queue")

	// Create stores
	messageStore := newMessageStore(params.DB, logger, params.MetricsScope)
	offsetStore := newOffsetStore(params.DB, params.MetricsScope)
	leaseStore := newPartitionLeaseStore(params.DB, logger, params.MetricsScope)
	heartbeatStore := newSubscriberHeartbeatStore(params.DB, logger, params.MetricsScope, time.Now)
	deliveryStateStore := newDeliveryStateStore(params.DB, logger, params.MetricsScope)

	queueMetrics := params.MetricsScope.SubScope("queue")

	// Create publisher and subscriber
	publisher := NewPublisher(
		logger,
		queueMetrics.SubScope("publisher"),
		messageStore,
	)

	subscriber := NewSubscriber(
		logger,
		queueMetrics.SubScope("subscriber"),
		messageStore,
		offsetStore,
		leaseStore,
		heartbeatStore,
		deliveryStateStore,
	)
	subscriber.OnSignal = params.OnSignal

	return &queueImpl{
		publisher:  publisher,
		subscriber: subscriber,
		closed:     false,
	}, nil
}

// Publisher returns a Publisher instance
func (q *queueImpl) Publisher() extqueue.Publisher {
	return q.publisher
}

// Subscriber returns a Subscriber instance
func (q *queueImpl) Subscriber() extqueue.Subscriber {
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
