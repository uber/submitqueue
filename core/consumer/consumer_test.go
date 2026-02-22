package consumer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/entity/queue"
	extqueue "github.com/uber/submitqueue/extension/queue"
	"go.uber.org/zap/zaptest"
)

// Mock Controller
type mockController struct {
	name          string
	topic         string
	consumerGroup string
	processFunc   func(ctx context.Context, delivery Delivery) error
}

func (m *mockController) Process(ctx context.Context, delivery Delivery) error {
	return m.processFunc(ctx, delivery)
}

func (m *mockController) Name() string {
	return m.name
}

func (m *mockController) Topic() string {
	return m.topic
}

func (m *mockController) ConsumerGroup() string {
	return m.consumerGroup
}

func (m *mockController) SubscriptionConfig(subscriberName string) extqueue.SubscriptionConfig {
	return extqueue.DefaultSubscriptionConfig(subscriberName, m.consumerGroup)
}

// Mock Delivery
type mockDelivery struct {
	msg          queue.Message
	attempt      int
	ackFunc      func(ctx context.Context) error
	nackFunc     func(ctx context.Context, requeueAfterMillis int64) error
	rejectFunc   func(ctx context.Context, reason string) error
	acked        bool
	nacked       bool
	rejected     bool
	rejectReason string
	nackDelayMs  int64
	done         chan struct{} // Signals when ack/nack/reject is called
	mu           sync.Mutex
}

func (m *mockDelivery) Message() queue.Message {
	return m.msg
}

func (m *mockDelivery) DeliveryID() string {
	return m.msg.ID
}

func (m *mockDelivery) Attempt() int {
	return m.attempt
}

func (m *mockDelivery) ReceivedAt() int64 {
	return time.Now().UnixMilli()
}

func (m *mockDelivery) Metadata() map[string]string {
	return nil
}

func (m *mockDelivery) Ack(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acked = true
	if m.done != nil {
		close(m.done)
	}
	if m.ackFunc != nil {
		return m.ackFunc(ctx)
	}
	return nil
}

func (m *mockDelivery) Nack(ctx context.Context, requeueAfterMillis int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nacked = true
	m.nackDelayMs = requeueAfterMillis
	if m.done != nil {
		close(m.done)
	}
	if m.nackFunc != nil {
		return m.nackFunc(ctx, requeueAfterMillis)
	}
	return nil
}

func (m *mockDelivery) Reject(ctx context.Context, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rejected = true
	m.rejectReason = reason
	if m.done != nil {
		close(m.done)
	}
	if m.rejectFunc != nil {
		return m.rejectFunc(ctx, reason)
	}
	return nil
}

func (m *mockDelivery) ExtendVisibilityTimeout(ctx context.Context, durationMillis int64) error {
	return nil
}

func (m *mockDelivery) WasAcked() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.acked
}

func (m *mockDelivery) WasNacked() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nacked
}

func (m *mockDelivery) WasRejected() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rejected
}

// Mock Subscriber
type mockSubscriber struct {
	subscribeFunc func(ctx context.Context, topic string, config extqueue.SubscriptionConfig) (<-chan extqueue.Delivery, error)
}

func (m *mockSubscriber) Subscribe(ctx context.Context, topic string, config extqueue.SubscriptionConfig) (<-chan extqueue.Delivery, error) {
	return m.subscribeFunc(ctx, topic, config)
}

func (m *mockSubscriber) Close() error {
	return nil
}

// Mock Queue
type mockQueue struct {
	subscriber extqueue.Subscriber
}

func (m *mockQueue) Publisher() extqueue.Publisher {
	return nil
}

func (m *mockQueue) Subscriber() extqueue.Subscriber {
	return m.subscriber
}

func (m *mockQueue) Close() error {
	return nil
}

func TestNew(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope
	q := &mockQueue{}

	c := New(logger, scope, q, "test-worker")

	require.NotNil(t, c)

	// Type assert to access internal fields
	impl := c.(*consumer)
	assert.Equal(t, "test-worker", impl.subscriberName)
	assert.Empty(t, impl.controllers)
	assert.Empty(t, impl.subscriptions)
}

func TestConsumer_Register(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope
	q := &mockQueue{}
	c := New(logger, scope, q, "test-worker")

	handler1 := &mockController{
		name:          "handler1",
		topic:         "topic1",
		consumerGroup: "group1",
	}
	handler2 := &mockController{
		name:          "handler2",
		topic:         "topic2",
		consumerGroup: "group2",
	}

	// Register first handler
	err := c.Register(handler1)
	require.NoError(t, err)
	assert.Len(t, c.(*consumer).controllers, 1)

	// Register second handler
	err = c.Register(handler2)
	require.NoError(t, err)
	assert.Len(t, c.(*consumer).controllers, 2)
}

func TestConsumer_Register_DuplicateTopic(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope
	q := &mockQueue{}
	c := New(logger, scope, q, "test-worker")

	handler1 := &mockController{
		name:          "handler1",
		topic:         "topic1",
		consumerGroup: "group1",
	}
	handler2 := &mockController{
		name:          "handler2",
		topic:         "topic1", // Same topic
		consumerGroup: "group2",
	}

	err := c.Register(handler1)
	require.NoError(t, err)

	err = c.Register(handler2)
	assert.Error(t, err)
}

func TestConsumer_Register_AfterStop(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope
	q := &mockQueue{}
	c := New(logger, scope, q, "test-worker")

	err := c.Stop(1000)
	require.NoError(t, err)

	handler := &mockController{
		name:          "handler1",
		topic:         "topic1",
		consumerGroup: "group1",
	}

	err = c.Register(handler)
	assert.Error(t, err)
}

func TestConsumer_Start_NoHandlers(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope
	q := &mockQueue{}
	c := New(logger, scope, q, "test-worker")

	ctx := context.Background()
	err := c.Start(ctx)
	assert.Error(t, err)
}

func TestConsumer_Start_AfterStop(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope
	q := &mockQueue{}
	c := New(logger, scope, q, "test-worker")

	handler := &mockController{
		name:          "handler1",
		topic:         "topic1",
		consumerGroup: "group1",
	}

	err := c.Register(handler)
	require.NoError(t, err)

	err = c.Stop(1000)
	require.NoError(t, err)

	ctx := context.Background()
	err = c.Start(ctx)
	assert.Error(t, err)
}

func TestConsumer_ProcessDelivery_Success(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope

	deliveryChan := make(chan extqueue.Delivery, 1)
	subscriber := &mockSubscriber{
		subscribeFunc: func(ctx context.Context, topic string, config extqueue.SubscriptionConfig) (<-chan extqueue.Delivery, error) {
			return deliveryChan, nil
		},
	}
	q := &mockQueue{subscriber: subscriber}
	c := New(logger, scope, q, "test-worker")

	handledMsg := ""
	handler := &mockController{
		name:          "test-handler",
		topic:         "test-topic",
		consumerGroup: "test-group",
		processFunc: func(ctx context.Context, delivery Delivery) error {
			handledMsg = delivery.Message().ID
			return nil // Success
		},
	}

	err := c.Register(handler)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = c.Start(ctx)
	require.NoError(t, err)

	// Send a test message
	msg := queue.NewMessage("test-msg-1", []byte("payload"), "partition1", nil)
	delivery := &mockDelivery{
		msg:     msg,
		attempt: 1,
		done:    make(chan struct{}),
	}

	deliveryChan <- delivery

	// Wait for processing to complete
	<-delivery.done

	assert.Equal(t, "test-msg-1", handledMsg)
	assert.True(t, delivery.WasAcked(), "Message should be acked on success")
	assert.False(t, delivery.WasNacked(), "Message should not be nacked on success")

	err = c.Stop(30000)
	require.NoError(t, err)
}

func TestConsumer_ProcessDelivery_Error(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope

	deliveryChan := make(chan extqueue.Delivery, 1)
	subscriber := &mockSubscriber{
		subscribeFunc: func(ctx context.Context, topic string, config extqueue.SubscriptionConfig) (<-chan extqueue.Delivery, error) {
			return deliveryChan, nil
		},
	}
	q := &mockQueue{subscriber: subscriber}
	c := New(logger, scope, q, "test-worker")

	handler := &mockController{
		name:          "test-handler",
		topic:         "test-topic",
		consumerGroup: "test-group",
		processFunc: func(ctx context.Context, delivery Delivery) error {
			return fmt.Errorf("processing failed")
		},
	}

	err := c.Register(handler)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = c.Start(ctx)
	require.NoError(t, err)

	// Send a test message
	msg := queue.NewMessage("test-msg-2", []byte("payload"), "partition1", nil)
	delivery := &mockDelivery{
		msg:     msg,
		attempt: 2,
		done:    make(chan struct{}),
	}

	deliveryChan <- delivery

	// Wait for processing to complete
	<-delivery.done

	assert.False(t, delivery.WasAcked(), "Message should not be acked on error")
	assert.True(t, delivery.WasNacked(), "Message should be nacked on error")

	err = c.Stop(30000)
	require.NoError(t, err)
}

func TestConsumer_ProcessDelivery_NonRetryableError(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope

	deliveryChan := make(chan extqueue.Delivery, 1)
	subscriber := &mockSubscriber{
		subscribeFunc: func(ctx context.Context, topic string, config extqueue.SubscriptionConfig) (<-chan extqueue.Delivery, error) {
			return deliveryChan, nil
		},
	}
	q := &mockQueue{subscriber: subscriber}
	c := New(logger, scope, q, "test-worker")

	handler := &mockController{
		name:          "test-handler",
		topic:         "test-topic",
		consumerGroup: "test-group",
		processFunc: func(ctx context.Context, delivery Delivery) error {
			return NewNonRetryableError(fmt.Errorf("bad payload"))
		},
	}

	err := c.Register(handler)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = c.Start(ctx)
	require.NoError(t, err)

	// Send a test message with non-retryable payload
	msg := queue.NewMessage("poison-msg", []byte("bad"), "partition1", nil)
	delivery := &mockDelivery{
		msg:     msg,
		attempt: 1,
		done:    make(chan struct{}),
	}

	deliveryChan <- delivery

	// Wait for processing to complete
	<-delivery.done

	assert.True(t, delivery.WasRejected(), "Non-retryable message should be rejected")
	assert.False(t, delivery.WasAcked(), "Non-retryable message should not be acked directly")
	assert.False(t, delivery.WasNacked(), "Non-retryable message should not be nacked")

	err = c.Stop(30000)
	require.NoError(t, err)
}

func TestConsumer_Stop(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope

	deliveryChan := make(chan extqueue.Delivery)
	subscriber := &mockSubscriber{
		subscribeFunc: func(ctx context.Context, topic string, config extqueue.SubscriptionConfig) (<-chan extqueue.Delivery, error) {
			return deliveryChan, nil
		},
	}
	q := &mockQueue{subscriber: subscriber}
	c := New(logger, scope, q, "test-worker")

	handler := &mockController{
		name:          "test-handler",
		topic:         "test-topic",
		consumerGroup: "test-group",
		processFunc: func(ctx context.Context, delivery Delivery) error {
			return nil
		},
	}

	err := c.Register(handler)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = c.Start(ctx)
	require.NoError(t, err)

	assert.Len(t, c.(*consumer).subscriptions, 1)

	// Stop the c
	err = c.Stop(30000)
	require.NoError(t, err)

	assert.Empty(t, c.(*consumer).subscriptions, "Subscriptions should be cleared after stop")
}

func TestConsumer_ObservabilityTags(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()

	deliveryChan := make(chan extqueue.Delivery, 10)
	subscriber := &mockSubscriber{
		subscribeFunc: func(ctx context.Context, topic string, config extqueue.SubscriptionConfig) (<-chan extqueue.Delivery, error) {
			return deliveryChan, nil
		},
	}
	q := &mockQueue{subscriber: subscriber}

	tests := []struct {
		name           string
		handlerError   error
		ackError       error
		nackError      error
		expectSuccess  bool
		expectAckCount bool
	}{
		{
			name:           "success with ack",
			handlerError:   nil,
			ackError:       nil,
			expectSuccess:  true,
			expectAckCount: true,
		},
		{
			name:           "failure with nack",
			handlerError:   fmt.Errorf("handler failed"),
			nackError:      nil,
			expectSuccess:  false,
			expectAckCount: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a fresh scope for each test
			testScope := tally.NewTestScope("consumer", nil)
			testC := New(logger, testScope, q, "test-worker")

			handler := &mockController{
				name:          "test-handler",
				topic:         "test-topic",
				consumerGroup: "test-group",
				processFunc: func(ctx context.Context, delivery Delivery) error {
					return tt.handlerError
				},
			}

			err := testC.Register(handler)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			err = testC.Start(ctx)
			require.NoError(t, err)

			// Send a test message
			msg := queue.NewMessage("msg-1", []byte("payload"), "partition1", nil)
			delivery := &mockDelivery{
				msg:     msg,
				attempt: 1,
				done:    make(chan struct{}),
				ackFunc: func(ctx context.Context) error {
					return tt.ackError
				},
				nackFunc: func(ctx context.Context, requeueAfterMillis int64) error {
					return tt.nackError
				},
			}

			deliveryChan <- delivery

			// Wait for processing to complete
			<-delivery.done

			// Verify metrics exist
			snapshot := testScope.Snapshot()

			// Check handler latency with success tag exists
			timers := snapshot.Timers()
			assert.NotEmpty(t, timers, "Should have timer metrics")

			// Check for handler latency metric
			var foundLatency bool
			for _, timer := range timers {
				if strings.Contains(timer.Name(), "controller_latency") {
					foundLatency = true
					// Verify success tag
					tags := timer.Tags()
					if tt.expectSuccess {
						assert.Equal(t, "true", tags["success"], "Should have success=true tag")
					} else {
						assert.Equal(t, "false", tags["success"], "Should have success=false tag")
					}
				}
			}
			assert.True(t, foundLatency, "Should have controller_latency metric")

			// Check counters
			counters := snapshot.Counters()
			if tt.expectAckCount {
				var foundAck bool
				for _, counter := range counters {
					if strings.Contains(counter.Name(), "ack_count") {
						foundAck = true
						assert.Greater(t, counter.Value(), int64(0), "ack_count should be > 0")
					}
				}
				assert.True(t, foundAck, "Should have ack_count metric")
			}

			_ = testC.Stop(30000)
		})
	}
}

func TestConsumer_AckNackLatencyTracking(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NewTestScope("consumer", nil)

	deliveryChan := make(chan extqueue.Delivery, 1)
	subscriber := &mockSubscriber{
		subscribeFunc: func(ctx context.Context, topic string, config extqueue.SubscriptionConfig) (<-chan extqueue.Delivery, error) {
			return deliveryChan, nil
		},
	}
	q := &mockQueue{subscriber: subscriber}
	c := New(logger, scope, q, "test-worker")

	handler := &mockController{
		name:          "test-handler",
		topic:         "test-topic",
		consumerGroup: "test-group",
		processFunc: func(ctx context.Context, delivery Delivery) error {
			return nil // Success
		},
	}

	err := c.Register(handler)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = c.Start(ctx)
	require.NoError(t, err)

	// Send a test message
	msg := queue.NewMessage("msg-1", []byte("payload"), "partition1", nil)
	delivery := &mockDelivery{
		msg:     msg,
		attempt: 1,
		done:    make(chan struct{}),
	}

	deliveryChan <- delivery

	// Wait for processing to complete
	<-delivery.done

	// Verify we have some timer metrics (latency tracking is working)
	snapshot := scope.Snapshot()
	assert.NotEmpty(t, snapshot.Timers(), "Should have timer metrics for latency tracking")
	assert.NotEmpty(t, snapshot.Counters(), "Should have counter metrics")

	err = c.Stop(30000)
	require.NoError(t, err)
}

func TestConsumer_ErrorMetrics(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NewTestScope("consumer", nil)

	deliveryChan := make(chan extqueue.Delivery, 1)
	subscriber := &mockSubscriber{
		subscribeFunc: func(ctx context.Context, topic string, config extqueue.SubscriptionConfig) (<-chan extqueue.Delivery, error) {
			return deliveryChan, nil
		},
	}
	q := &mockQueue{subscriber: subscriber}
	c := New(logger, scope, q, "test-worker")

	handler := &mockController{
		name:          "test-handler",
		topic:         "test-topic",
		consumerGroup: "test-group",
		processFunc: func(ctx context.Context, delivery Delivery) error {
			return fmt.Errorf("processing failed")
		},
	}

	err := c.Register(handler)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = c.Start(ctx)
	require.NoError(t, err)

	// Send a test message
	msg := queue.NewMessage("msg-1", []byte("payload"), "partition1", nil)
	delivery := &mockDelivery{
		msg:     msg,
		attempt: 1,
		done:    make(chan struct{}),
		nackFunc: func(ctx context.Context, requeueAfterMillis int64) error {
			return fmt.Errorf("nack failed")
		},
	}

	deliveryChan <- delivery

	// Wait for processing to complete
	<-delivery.done

	// Verify error metrics are tracked
	snapshot := scope.Snapshot()
	counters := snapshot.Counters()

	// Should have handler_errors and nack_errors
	var hasErrorMetrics bool
	for _, counter := range counters {
		if strings.Contains(counter.Name(), "errors") {
			hasErrorMetrics = true
			break
		}
	}
	assert.True(t, hasErrorMetrics, "Should track error metrics")

	err = c.Stop(30000)
	require.NoError(t, err)
}
