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
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/zap"

	"github.com/uber/submitqueue/core/metrics"
	entityqueue "github.com/uber/submitqueue/entity/messagequeue"
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
		logger:       logger.Named("publisher"),
		scope:        scope,
		messageStore: messageStore,
	}
}

// Publish sends a message to the specified topic
func (p *publisher) Publish(ctx context.Context, topic string, message entityqueue.Message) (retErr error) {
	op := metrics.Begin(p.scope, "publish", metrics.NewTag("topic", topic))
	defer func() { op.Complete(retErr) }()

	// Check if closed (under lock)
	p.mu.RLock()
	closed := p.closed
	p.mu.RUnlock()

	if closed {
		return ErrPublisherClosed
	}

	if err := p.messageStore.Insert(ctx, topic, []entityqueue.Message{message}); err != nil {
		return fmt.Errorf("publish message store insert error: %w", err)
	}

	p.logger.Debugw("published message", logTopic, topic, logMessageID, message.ID)

	return nil
}

// PublishAfter sends a message that becomes visible to subscribers only
// after delayMs from now. The message is inserted with visible_after =
// now + delayMs; FetchByOffset skips it until that timestamp.
// delayMs <= 0 is equivalent to Publish.
func (p *publisher) PublishAfter(ctx context.Context, topic string, message entityqueue.Message, delayMs int64) (retErr error) {
	op := metrics.Begin(p.scope, "publish_after", metrics.NewTag("topic", topic))
	defer func() { op.Complete(retErr) }()

	p.mu.RLock()
	closed := p.closed
	p.mu.RUnlock()

	if closed {
		return ErrPublisherClosed
	}

	var visibleAfter int64
	if delayMs > 0 {
		visibleAfter = time.Now().UnixMilli() + delayMs
	}

	if err := p.messageStore.InsertDelayed(ctx, topic, []entityqueue.Message{message}, visibleAfter); err != nil {
		return fmt.Errorf("publish_after message store insert error: %w", err)
	}

	p.logger.Debugw("published delayed message", logTopic, topic, logMessageID, message.ID, "delay_ms", delayMs)

	return nil
}

// Close gracefully shuts down the publisher
func (p *publisher) Close() error {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()

	p.logger.Infow("publisher closed")
	return nil
}
