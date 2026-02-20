package sql

import (
	"context"
	"fmt"
	"sync"

	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"

	"github.com/uber/submitqueue/entities/queue"
)

type publisher struct {
	config       Config
	logger       *zap.SugaredLogger
	metrics      tally.Scope
	messageStore MessageStore
	mu           sync.RWMutex
	closed       bool
}

// NewPublisher creates a publisher with the given configuration and dependencies
func NewPublisher(config Config, logger *zap.SugaredLogger, metrics tally.Scope, messageStore MessageStore) *publisher {
	return &publisher{
		config:       config,
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

	// Validate topic name (SQL-safe)
	if err := validateTopicName(topic); err != nil {
		p.logger.Errorw("publish failure: invalid topic name", "topic", topic, "error", err)
		p.metrics.Tagged(map[string]string{"topic": topic}).Counter("publish_errors").Inc(1)
		return fmt.Errorf("publish invalid topic name: %w", err)
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
