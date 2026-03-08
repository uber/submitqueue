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
	"context"
	"fmt"
	"sync"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/metrics"
	"go.uber.org/zap"

	"github.com/uber/submitqueue/entity/queue"
)

type publisher struct {
	logger       *zap.SugaredLogger
	scope        tally.Scope
	messageStore messageStore
	mu           sync.RWMutex
	closed       bool
}

// NewPublisher creates a publisher with the given dependencies
func NewPublisher(logger *zap.SugaredLogger, scope tally.Scope, messageStore messageStore) *publisher {
	return &publisher{
		logger:       logger.Named("queue_mysql_publisher"),
		scope:        scope.SubScope("queue_mysql_publisher"),
		messageStore: messageStore,
	}
}

// Publish sends a message to the specified topic
func (p *publisher) Publish(ctx context.Context, topic string, message queue.Message) (retErr error) {
	op := metrics.Begin(p.scope, "publish")
	defer func() { op.Complete(retErr) }()

	// Check if closed (under lock)
	p.mu.RLock()
	closed := p.closed
	p.mu.RUnlock()

	if closed {
		return fmt.Errorf("%w for topic: %s", ErrPublisherClosed, topic)
	}

	if err := p.messageStore.Insert(ctx, topic, []queue.Message{message}); err != nil {
		return fmt.Errorf("failed to publish message to topic %s: %w", topic, err)
	}

	p.logger.Debugw("published message", "topic", topic, "message_id", message.ID, "partition_key", message.PartitionKey)
	return nil
}

// Close gracefully shuts down the publisher
func (p *publisher) Close() error {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()

	p.logger.Info("publisher closed")
	return nil
}
