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
	"go.uber.org/zap"

	"github.com/uber/submitqueue/entity/queue"
)

type publisher struct {
	logger       *zap.SugaredLogger
	metrics      tally.Scope
	messageStore messageStore
	mu           sync.RWMutex
	closed       bool
}

// NewPublisher creates a publisher with the given dependencies
func NewPublisher(logger *zap.SugaredLogger, metrics tally.Scope, messageStore messageStore) *publisher {
	return &publisher{
		logger:       logger,
		metrics:      metrics,
		messageStore: messageStore,
	}
}

// Publish sends a message to the specified topic
func (p *publisher) Publish(ctx context.Context, topic string, message queue.Message) error {
	// Check if closed (under lock)
	p.mu.RLock()
	closed := p.closed
	p.mu.RUnlock()

	if closed {
		p.logger.Errorw("publish failure: publisher is closed", "topic", topic)
		return fmt.Errorf("publisher is closed")
	}

	if err := p.messageStore.Insert(ctx, topic, []queue.Message{message}); err != nil {
		p.metrics.Tagged(map[string]string{"topic": topic}).Counter("publish_errors").Inc(1)
		p.logger.Errorw("publish failure: message store insert error", "topic", topic, "error", err)
		return fmt.Errorf("publish message store insert error: %w", err)
	}

	p.metrics.Tagged(map[string]string{"topic": topic}).Counter("messages_published").Inc(1)
	p.logger.Debugw("published message", "topic", topic, "message_id", message.ID)

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
