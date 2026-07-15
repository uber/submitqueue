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

package consumer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/extension/consumergate"
	consumergatenoop "github.com/uber/submitqueue/platform/extension/consumergate/noop"
	extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

// testController is a configurable controller used by consumer unit tests.
type testController struct {
	name          string
	topicKey      TopicKey
	consumerGroup string
	process       func(context.Context, Delivery) error
}

func (c *testController) Name() string          { return c.name }
func (c *testController) TopicKey() TopicKey    { return c.topicKey }
func (c *testController) ConsumerGroup() string { return c.consumerGroup }
func (c *testController) Process(ctx context.Context, delivery Delivery) error {
	return c.process(ctx, delivery)
}

// setupController configures a test controller.
func setupController(c *testController, name string, topicKey TopicKey, consumerGroup string, processFunc func(context.Context, Delivery) error) {
	c.name = name
	c.topicKey = topicKey
	c.consumerGroup = consumerGroup
	c.process = processFunc
}

// newRegistry creates a TopicRegistry with a mock queue and default subscription config.
func newRegistry(t *testing.T, q extqueue.Queue, topicKey TopicKey, consumerGroup string) TopicRegistry {
	t.Helper()
	reg, err := NewTopicRegistry(
		[]TopicConfig{
			{
				Key:   topicKey,
				Name:  topicKey.String(),
				Queue: q,
				Subscription: extqueue.DefaultSubscriptionConfig(
					"test-worker", consumerGroup,
				),
			},
		},
	)
	require.NoError(t, err)
	return reg
}

// setupDelivery creates a MockDelivery with standard expectations and a done channel
// that closes when Ack or Nack is called.
func setupDelivery(del *queuemock.MockDelivery, msg entityqueue.Message, ackErr, nackErr error) chan struct{} {
	done := make(chan struct{})
	del.EXPECT().Message().Return(msg).AnyTimes()
	del.EXPECT().Attempt().Return(1).AnyTimes()
	del.EXPECT().ReceivedAt().Return(time.Now().UnixMilli()).AnyTimes()
	del.EXPECT().Metadata().Return(nil).AnyTimes()
	del.EXPECT().DeliveryID().Return(msg.ID).AnyTimes()
	del.EXPECT().Ack(gomock.Any()).DoAndReturn(func(ctx context.Context) error {
		close(done)
		return ackErr
	}).MaxTimes(1)
	del.EXPECT().Nack(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, requeueAfterMillis int64) error {
		close(done)
		return nackErr
	}).MaxTimes(1)
	return done
}

func TestNew(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()

	reg, err := NewTopicRegistry(nil)
	require.NoError(t, err)

	c := New(logger, tally.NoopScope, reg, errs.NewClassifierProcessor(), consumergatenoop.New())
	require.NotNil(t, c)
}

func TestConsumer_Register(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()

	reg, _ := NewTopicRegistry(nil)
	c := New(logger, tally.NoopScope, reg, errs.NewClassifierProcessor(), consumergatenoop.New())

	handler1 := &testController{}
	setupController(handler1, "handler1", TopicKey("start"), "group1", nil)

	handler2 := &testController{}
	setupController(handler2, "handler2", TopicKey("other-topic"), "group2", nil)

	err := c.Register(handler1)
	require.NoError(t, err)

	err = c.Register(handler2)
	require.NoError(t, err)
}

func TestConsumer_Register_DuplicateTopic(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()

	reg, _ := NewTopicRegistry(nil)
	c := New(logger, tally.NoopScope, reg, errs.NewClassifierProcessor(), consumergatenoop.New())

	handler1 := &testController{}
	setupController(handler1, "handler1", TopicKey("start"), "group1", nil)

	handler2 := &testController{}
	setupController(handler2, "handler2", TopicKey("start"), "group2", nil)

	err := c.Register(handler1)
	require.NoError(t, err)

	err = c.Register(handler2)
	assert.Error(t, err)
}

func TestConsumer_Register_AfterStop(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()

	reg, _ := NewTopicRegistry(nil)
	c := New(logger, tally.NoopScope, reg, errs.NewClassifierProcessor(), consumergatenoop.New())

	err := c.Stop(1000)
	require.NoError(t, err)

	handler := &testController{}
	setupController(handler, "handler1", TopicKey("start"), "group1", nil)

	err = c.Register(handler)
	assert.Error(t, err)
}

func TestConsumer_Start_NoHandlers(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()

	reg, _ := NewTopicRegistry(nil)
	c := New(logger, tally.NoopScope, reg, errs.NewClassifierProcessor(), consumergatenoop.New())

	err := c.Start(context.Background())
	assert.Error(t, err)
}

func TestConsumer_Start_AfterStop(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()

	reg, _ := NewTopicRegistry(nil)
	c := New(logger, tally.NoopScope, reg, errs.NewClassifierProcessor(), consumergatenoop.New())

	handler := &testController{}
	setupController(handler, "handler1", TopicKey("start"), "group1", nil)

	err := c.Register(handler)
	require.NoError(t, err)

	err = c.Stop(1000)
	require.NoError(t, err)

	err = c.Start(context.Background())
	assert.Error(t, err)
}

func TestConsumer_Start_MissingSubscriptionConfig(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := zaptest.NewLogger(t).Sugar()

	mockQ := queuemock.NewMockQueue(ctrl)
	// Registry has queue but no subscription config
	reg, err := NewTopicRegistry(
		[]TopicConfig{{Key: TopicKey("start"), Name: "request", Queue: mockQ}},
	)
	require.NoError(t, err)

	c := New(logger, tally.NoopScope, reg, errs.NewClassifierProcessor(), consumergatenoop.New())

	handler := &testController{}
	setupController(handler, "handler", TopicKey("start"), "group", nil)

	err = c.Register(handler)
	require.NoError(t, err)

	err = c.Start(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no subscription config")
}

func TestConsumer_Start_SubscribeFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := zaptest.NewLogger(t).Sugar()

	mockSub := queuemock.NewMockSubscriber(ctrl)
	mockSub.EXPECT().Subscribe(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, fmt.Errorf("connection refused"))

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Subscriber().Return(mockSub)

	reg := newRegistry(t, mockQ, TopicKey("start"), "group")

	c := New(logger, tally.NoopScope, reg, errs.NewClassifierProcessor(), consumergatenoop.New())

	handler := &testController{}
	setupController(handler, "handler", TopicKey("start"), "group", nil)

	err := c.Register(handler)
	require.NoError(t, err)

	err = c.Start(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "subscribe failed")
}

func TestConsumer_ProcessDelivery_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := zaptest.NewLogger(t).Sugar()

	deliveryChan := make(chan extqueue.Delivery, 1)
	mockSub := queuemock.NewMockSubscriber(ctrl)
	mockSub.EXPECT().Subscribe(gomock.Any(), gomock.Any(), gomock.Any()).Return(deliveryChan, nil)

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Subscriber().Return(mockSub)

	reg := newRegistry(t, mockQ, TopicKey("start"), "test-group")

	c := New(logger, tally.NoopScope, reg, errs.NewClassifierProcessor(), consumergatenoop.New())

	handledMsg := ""
	handler := &testController{}
	setupController(handler, "test-handler", TopicKey("start"), "test-group",
		func(ctx context.Context, delivery Delivery) error {
			handledMsg = delivery.Message().ID
			return nil
		},
	)

	err := c.Register(handler)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = c.Start(ctx)
	require.NoError(t, err)

	msg := entityqueue.NewMessage("test-msg-1", []byte("payload"), "partition1", nil)
	mockDel := queuemock.NewMockDelivery(ctrl)
	done := setupDelivery(mockDel, msg, nil, nil)

	deliveryChan <- mockDel
	<-done

	assert.Equal(t, "test-msg-1", handledMsg)

	err = c.Stop(30000)
	require.NoError(t, err)
}

func TestConsumer_ProcessDelivery_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := zaptest.NewLogger(t).Sugar()

	deliveryChan := make(chan extqueue.Delivery, 1)
	mockSub := queuemock.NewMockSubscriber(ctrl)
	mockSub.EXPECT().Subscribe(gomock.Any(), gomock.Any(), gomock.Any()).Return(deliveryChan, nil)

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Subscriber().Return(mockSub)

	reg := newRegistry(t, mockQ, TopicKey("start"), "test-group")

	c := New(logger, tally.NoopScope, reg, errs.NewClassifierProcessor(), consumergatenoop.New())

	handler := &testController{}
	setupController(handler, "test-handler", TopicKey("start"), "test-group",
		func(ctx context.Context, delivery Delivery) error {
			return errs.NewRetryableError(fmt.Errorf("processing failed"))
		},
	)

	err := c.Register(handler)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = c.Start(ctx)
	require.NoError(t, err)

	msg := entityqueue.NewMessage("test-msg-2", []byte("payload"), "partition1", nil)
	mockDel := queuemock.NewMockDelivery(ctrl)
	done := setupDelivery(mockDel, msg, nil, nil)

	deliveryChan <- mockDel
	<-done

	err = c.Stop(30000)
	require.NoError(t, err)
}

func TestConsumer_ProcessDelivery_NonRetryableError(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := zaptest.NewLogger(t).Sugar()

	deliveryChan := make(chan extqueue.Delivery, 1)
	mockSub := queuemock.NewMockSubscriber(ctrl)
	mockSub.EXPECT().Subscribe(gomock.Any(), gomock.Any(), gomock.Any()).Return(deliveryChan, nil)

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Subscriber().Return(mockSub)

	reg := newRegistry(t, mockQ, TopicKey("start"), "test-group")

	c := New(logger, tally.NoopScope, reg, errs.NewClassifierProcessor(), consumergatenoop.New())

	handler := &testController{}
	setupController(handler, "test-handler", TopicKey("start"), "test-group",
		func(ctx context.Context, delivery Delivery) error {
			return fmt.Errorf("bad payload")
		},
	)

	err := c.Register(handler)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = c.Start(ctx)
	require.NoError(t, err)

	msg := entityqueue.NewMessage("poison-msg", []byte("bad"), "partition1", nil)
	done := make(chan struct{})
	mockDel := queuemock.NewMockDelivery(ctrl)
	mockDel.EXPECT().Message().Return(msg).AnyTimes()
	mockDel.EXPECT().Attempt().Return(1).AnyTimes()
	mockDel.EXPECT().ReceivedAt().Return(time.Now().UnixMilli()).AnyTimes()
	mockDel.EXPECT().Metadata().Return(nil).AnyTimes()
	mockDel.EXPECT().DeliveryID().Return(msg.ID).AnyTimes()
	mockDel.EXPECT().Reject(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, reason string) error {
		close(done)
		return nil
	}).Times(1)

	deliveryChan <- mockDel
	<-done

	err = c.Stop(30000)
	require.NoError(t, err)
}

func TestConsumer_Stop(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := zaptest.NewLogger(t).Sugar()

	deliveryChan := make(chan extqueue.Delivery)
	mockSub := queuemock.NewMockSubscriber(ctrl)
	mockSub.EXPECT().Subscribe(gomock.Any(), gomock.Any(), gomock.Any()).Return(deliveryChan, nil)

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Subscriber().Return(mockSub)

	reg := newRegistry(t, mockQ, TopicKey("start"), "test-group")

	c := New(logger, tally.NoopScope, reg, errs.NewClassifierProcessor(), consumergatenoop.New())

	handler := &testController{}
	setupController(handler, "test-handler", TopicKey("start"), "test-group", nil)

	err := c.Register(handler)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = c.Start(ctx)
	require.NoError(t, err)

	// Stop should complete cleanly
	err = c.Stop(30000)
	require.NoError(t, err)
}

func TestConsumer_ObservabilityTags(t *testing.T) {
	tests := []struct {
		name           string
		handlerError   error
		nackError      error
		expectSuccess  bool
		expectAckCount bool
	}{
		{
			name:           "success with ack",
			handlerError:   nil,
			nackError:      nil,
			expectSuccess:  true,
			expectAckCount: true,
		},
		{
			name:           "failure with nack",
			handlerError:   errs.NewRetryableError(fmt.Errorf("handler failed")),
			nackError:      nil,
			expectSuccess:  false,
			expectAckCount: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			logger := zaptest.NewLogger(t).Sugar()
			testScope := tally.NewTestScope("consumer", nil)

			deliveryChan := make(chan extqueue.Delivery, 1)
			mockSub := queuemock.NewMockSubscriber(ctrl)
			mockSub.EXPECT().Subscribe(gomock.Any(), gomock.Any(), gomock.Any()).Return(deliveryChan, nil)

			mockQ := queuemock.NewMockQueue(ctrl)
			mockQ.EXPECT().Subscriber().Return(mockSub)

			reg := newRegistry(t, mockQ, TopicKey("start"), "test-group")

			testC := New(logger, testScope, reg, errs.NewClassifierProcessor(), consumergatenoop.New())

			handler := &testController{}
			setupController(handler, "test-handler", TopicKey("start"), "test-group",
				func(ctx context.Context, delivery Delivery) error {
					return tt.handlerError
				},
			)

			err := testC.Register(handler)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			err = testC.Start(ctx)
			require.NoError(t, err)

			msg := entityqueue.NewMessage("msg-1", []byte("payload"), "partition1", nil)
			mockDel := queuemock.NewMockDelivery(ctrl)
			done := setupDelivery(mockDel, msg, nil, tt.nackError)

			deliveryChan <- mockDel
			<-done

			snapshot := testScope.Snapshot()

			timers := snapshot.Timers()
			assert.NotEmpty(t, timers, "Should have timer metrics")

			var foundLatency bool
			for _, timer := range timers {
				if strings.Contains(timer.Name(), "controller_latency") {
					foundLatency = true
					tags := timer.Tags()
					if tt.expectSuccess {
						assert.Equal(t, "true", tags["success"])
					} else {
						assert.Equal(t, "false", tags["success"])
					}
				}
			}
			assert.True(t, foundLatency, "Should have controller_latency metric")

			counters := snapshot.Counters()
			if tt.expectAckCount {
				var foundAck bool
				for _, counter := range counters {
					if strings.Contains(counter.Name(), "ack_count") {
						foundAck = true
						assert.Greater(t, counter.Value(), int64(0))
					}
				}
				assert.True(t, foundAck, "Should have ack_count metric")
			}

			_ = testC.Stop(30000)
		})
	}
}

func TestConsumer_AckNackLatencyTracking(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NewTestScope("consumer", nil)

	deliveryChan := make(chan extqueue.Delivery, 1)
	mockSub := queuemock.NewMockSubscriber(ctrl)
	mockSub.EXPECT().Subscribe(gomock.Any(), gomock.Any(), gomock.Any()).Return(deliveryChan, nil)

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Subscriber().Return(mockSub)

	reg := newRegistry(t, mockQ, TopicKey("start"), "test-group")

	c := New(logger, scope, reg, errs.NewClassifierProcessor(), consumergatenoop.New())

	handler := &testController{}
	setupController(handler, "test-handler", TopicKey("start"), "test-group",
		func(ctx context.Context, delivery Delivery) error { return nil },
	)

	err := c.Register(handler)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = c.Start(ctx)
	require.NoError(t, err)

	msg := entityqueue.NewMessage("msg-1", []byte("payload"), "partition1", nil)
	mockDel := queuemock.NewMockDelivery(ctrl)
	done := setupDelivery(mockDel, msg, nil, nil)

	deliveryChan <- mockDel
	<-done

	snapshot := scope.Snapshot()
	assert.NotEmpty(t, snapshot.Timers(), "Should have timer metrics for latency tracking")
	assert.NotEmpty(t, snapshot.Counters(), "Should have counter metrics")

	err = c.Stop(30000)
	require.NoError(t, err)
}

func TestConsumer_ErrorMetrics(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NewTestScope("consumer", nil)

	deliveryChan := make(chan extqueue.Delivery, 1)
	mockSub := queuemock.NewMockSubscriber(ctrl)
	mockSub.EXPECT().Subscribe(gomock.Any(), gomock.Any(), gomock.Any()).Return(deliveryChan, nil)

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Subscriber().Return(mockSub)

	reg := newRegistry(t, mockQ, TopicKey("start"), "test-group")

	c := New(logger, scope, reg, errs.NewClassifierProcessor(), consumergatenoop.New())

	handler := &testController{}
	setupController(handler, "test-handler", TopicKey("start"), "test-group",
		func(ctx context.Context, delivery Delivery) error {
			return errs.NewRetryableError(fmt.Errorf("processing failed"))
		},
	)

	err := c.Register(handler)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = c.Start(ctx)
	require.NoError(t, err)

	msg := entityqueue.NewMessage("msg-1", []byte("payload"), "partition1", nil)
	mockDel := queuemock.NewMockDelivery(ctrl)
	done := setupDelivery(mockDel, msg, nil, fmt.Errorf("nack failed"))

	deliveryChan <- mockDel
	<-done

	snapshot := scope.Snapshot()
	counters := snapshot.Counters()

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

// TestConsumer_PerPartitionProcessing verifies that a slow message on partition A
// does not block partition B from being processed.
func TestConsumer_PerPartitionProcessing(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := zaptest.NewLogger(t).Sugar()

	deliveryChan := make(chan extqueue.Delivery, 10)
	mockSub := queuemock.NewMockSubscriber(ctrl)
	mockSub.EXPECT().Subscribe(gomock.Any(), gomock.Any(), gomock.Any()).Return(deliveryChan, nil)

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Subscriber().Return(mockSub)

	reg := newRegistry(t, mockQ, TopicKey("start"), "test-group")

	c := New(logger, tally.NoopScope, reg, errs.NewClassifierProcessor(), consumergatenoop.New())

	// Track processing by partition
	partBDone := make(chan struct{})
	partABlocking := make(chan struct{})
	var partBProcessed atomic.Bool

	handler := &testController{}
	setupController(handler, "test-handler", TopicKey("start"), "test-group",
		func(ctx context.Context, delivery Delivery) error {
			pk := delivery.Message().PartitionKey
			if pk == "partition-a" {
				// Signal that partition A is blocking
				close(partABlocking)
				// Block until test is done
				<-ctx.Done()
				return nil
			}
			// Partition B processes immediately
			partBProcessed.Store(true)
			close(partBDone)
			return nil
		},
	)

	err := c.Register(handler)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = c.Start(ctx)
	require.NoError(t, err)

	// Send message to partition A (will block in controller)
	msgA := entityqueue.NewMessage("msg-a", []byte("payload-a"), "partition-a", nil)
	mockDelA := queuemock.NewMockDelivery(ctrl)
	mockDelA.EXPECT().Message().Return(msgA).AnyTimes()
	mockDelA.EXPECT().Attempt().Return(1).AnyTimes()
	mockDelA.EXPECT().ReceivedAt().Return(time.Now().UnixMilli()).AnyTimes()
	mockDelA.EXPECT().Metadata().Return(nil).AnyTimes()
	mockDelA.EXPECT().DeliveryID().Return(msgA.ID).AnyTimes()
	mockDelA.EXPECT().Ack(gomock.Any()).Return(nil).MaxTimes(1)
	mockDelA.EXPECT().Nack(gomock.Any(), gomock.Any()).Return(nil).MaxTimes(1)

	deliveryChan <- mockDelA

	// Wait for partition A to start blocking
	<-partABlocking

	// Send message to partition B (should process despite A being blocked)
	msgB := entityqueue.NewMessage("msg-b", []byte("payload-b"), "partition-b", nil)
	mockDelB := queuemock.NewMockDelivery(ctrl)
	mockDelB.EXPECT().Message().Return(msgB).AnyTimes()
	mockDelB.EXPECT().Attempt().Return(1).AnyTimes()
	mockDelB.EXPECT().ReceivedAt().Return(time.Now().UnixMilli()).AnyTimes()
	mockDelB.EXPECT().Metadata().Return(nil).AnyTimes()
	mockDelB.EXPECT().DeliveryID().Return(msgB.ID).AnyTimes()
	mockDelB.EXPECT().Ack(gomock.Any()).Return(nil).MaxTimes(1)

	deliveryChan <- mockDelB

	// Partition B should be processed (test timeout handles hangs)
	<-partBDone
	assert.True(t, partBProcessed.Load(), "partition B should have been processed")

	err = c.Stop(30000)
	require.NoError(t, err)
}

// TestConsumer_PartitionOrdering verifies that messages within a single partition
// are processed in order.
func TestConsumer_PartitionOrdering(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := zaptest.NewLogger(t).Sugar()

	deliveryChan := make(chan extqueue.Delivery, 10)
	mockSub := queuemock.NewMockSubscriber(ctrl)
	mockSub.EXPECT().Subscribe(gomock.Any(), gomock.Any(), gomock.Any()).Return(deliveryChan, nil)

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Subscriber().Return(mockSub)

	reg := newRegistry(t, mockQ, TopicKey("start"), "test-group")

	c := New(logger, tally.NoopScope, reg, errs.NewClassifierProcessor(), consumergatenoop.New())

	// Mutex + shared slice captures processing order for assertion;
	// a channel would only signal completion, not record the sequence.
	var mu sync.Mutex
	var order []string
	allDone := make(chan struct{})

	handler := &testController{}
	setupController(handler, "test-handler", TopicKey("start"), "test-group",
		func(ctx context.Context, delivery Delivery) error {
			mu.Lock()
			order = append(order, delivery.Message().ID)
			if len(order) == 3 {
				close(allDone)
			}
			mu.Unlock()
			return nil
		},
	)

	err := c.Register(handler)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = c.Start(ctx)
	require.NoError(t, err)

	// Send 3 messages to the same partition
	for i, id := range []string{"msg-1", "msg-2", "msg-3"} {
		msg := entityqueue.NewMessage(id, []byte("payload"), "same-partition", nil)
		mockDel := queuemock.NewMockDelivery(ctrl)
		mockDel.EXPECT().Message().Return(msg).AnyTimes()
		mockDel.EXPECT().Attempt().Return(1).AnyTimes()
		mockDel.EXPECT().ReceivedAt().Return(time.Now().UnixMilli()).AnyTimes()
		mockDel.EXPECT().Metadata().Return(nil).AnyTimes()
		mockDel.EXPECT().DeliveryID().Return(fmt.Sprintf("del-%d", i)).AnyTimes()
		mockDel.EXPECT().Ack(gomock.Any()).Return(nil).MaxTimes(1)

		deliveryChan <- mockDel
	}

	// Wait for all messages (test timeout handles hangs)
	<-allDone
	mu.Lock()
	assert.Equal(t, []string{"msg-1", "msg-2", "msg-3"}, order, "messages should be processed in order within a partition")
	mu.Unlock()

	err = c.Stop(30000)
	require.NoError(t, err)
}

// TestConsumer_PartitionWorkerCleanup verifies that all partition goroutines
// exit cleanly on Stop().
func TestConsumer_PartitionWorkerCleanup(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := zaptest.NewLogger(t).Sugar()

	deliveryChan := make(chan extqueue.Delivery, 10)
	mockSub := queuemock.NewMockSubscriber(ctrl)
	mockSub.EXPECT().Subscribe(gomock.Any(), gomock.Any(), gomock.Any()).Return(deliveryChan, nil)

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Subscriber().Return(mockSub)

	reg := newRegistry(t, mockQ, TopicKey("start"), "test-group")

	c := New(logger, tally.NoopScope, reg, errs.NewClassifierProcessor(), consumergatenoop.New())

	processedCount := int64(0)

	handler := &testController{}
	setupController(handler, "test-handler", TopicKey("start"), "test-group",
		func(ctx context.Context, delivery Delivery) error {
			atomic.AddInt64(&processedCount, 1)
			return nil
		},
	)

	err := c.Register(handler)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = c.Start(ctx)
	require.NoError(t, err)

	// Send messages to multiple partitions to spawn multiple goroutines
	for i := 0; i < 5; i++ {
		pk := fmt.Sprintf("partition-%d", i)
		msg := entityqueue.NewMessage(fmt.Sprintf("msg-%d", i), []byte("payload"), pk, nil)
		mockDel := queuemock.NewMockDelivery(ctrl)
		done := setupDelivery(mockDel, msg, nil, nil)
		deliveryChan <- mockDel
		<-done
	}

	// All messages should have been processed
	assert.Equal(t, int64(5), atomic.LoadInt64(&processedCount))

	// Stop should complete cleanly (no goroutine leaks or deadlocks)
	err = c.Stop(30000)
	require.NoError(t, err)
}

func TestConsumer_ConsumeLoopSurvivesCallerDeadline(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := zaptest.NewLogger(t).Sugar()

	deliveryChan := make(chan extqueue.Delivery, 1)
	mockSub := queuemock.NewMockSubscriber(ctrl)
	mockSub.EXPECT().Subscribe(gomock.Any(), gomock.Any(), gomock.Any()).Return(deliveryChan, nil)

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Subscriber().Return(mockSub)

	reg := newRegistry(t, mockQ, TopicKey("start"), "test-group")

	c := New(logger, tally.NoopScope, reg, errs.NewClassifierProcessor(), consumergatenoop.New())

	processed := make(chan string, 1)
	handler := &testController{}
	setupController(handler, "test-handler", TopicKey("start"), "test-group",
		func(ctx context.Context, delivery Delivery) error {
			processed <- delivery.Message().ID
			return nil
		},
	)

	err := c.Register(handler)
	require.NoError(t, err)

	// Start with a context that expires quickly, simulating an Fx OnStart hook.
	startCtx, startCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer startCancel()

	err = c.Start(startCtx)
	require.NoError(t, err)

	<-startCtx.Done()

	msg := entityqueue.NewMessage("after-deadline", []byte("payload"), "partition1", nil)
	mockDel := queuemock.NewMockDelivery(ctrl)
	done := setupDelivery(mockDel, msg, nil, nil)

	deliveryChan <- mockDel
	<-done

	assert.Equal(t, "after-deadline", <-processed)

	err = c.Stop(30000)
	require.NoError(t, err)
}

// fakeGate is a channel-instrumented consumergate.Gate so tests can await the
// park/release transitions instead of sleeping.
type fakeGate struct {
	mu      sync.Mutex
	closed  map[consumergate.Key]bool
	changed chan struct{}
	err     error

	parked   chan consumergate.Parked
	released chan string // message IDs
}

func newFakeGate() *fakeGate {
	return &fakeGate{
		closed:   make(map[consumergate.Key]bool),
		changed:  make(chan struct{}),
		parked:   make(chan consumergate.Parked, 16),
		released: make(chan string, 16),
	}
}

func (f *fakeGate) close(key consumergate.Key) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed[key] {
		return
	}
	f.closed[key] = true
	f.signalChanged()
}

func (f *fakeGate) open(key consumergate.Key) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.closed[key] {
		return
	}
	delete(f.closed, key)
	f.signalChanged()
}

func (f *fakeGate) signalChanged() {
	close(f.changed)
	f.changed = make(chan struct{})
}

func (f *fakeGate) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeGate) isClosed(consumerGroup, partitionKey string) bool {
	if f.closed[consumergate.Key{ConsumerGroup: consumerGroup}] {
		return true
	}
	return f.closed[consumergate.Key{ConsumerGroup: consumerGroup, PartitionKey: partitionKey}]
}

// Enter implements consumergate.Gate. It checks the err field first, then
// returns an unblocked entry for an open gate or a blocked entry for a closed
// one. The blocked entry's Watch mimics the contract: it stamps the entered
// identity on the parked descriptor, announces it on the parked channel, and a
// monitor goroutine waits for gate-state change signals until the gate opens or
// ctx is cancelled; on open it sends the message ID on the released channel and
// yields nil on the watch channel.
func (f *fakeGate) Enter(_ context.Context, key consumergate.Key) (consumergate.Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	if !f.isClosed(key.ConsumerGroup, key.PartitionKey) {
		return fakeOpenEntry{}, nil
	}
	return &fakeBlockedEntry{gate: f, key: key}, nil
}

// fakeOpenEntry is the entry handed out for an open fake gate.
type fakeOpenEntry struct{}

func (fakeOpenEntry) Blocked() bool { return false }

func (fakeOpenEntry) Watch(context.Context, consumergate.DeliveryDescriptor) <-chan error {
	ch := make(chan error, 1)
	ch <- nil
	return ch
}

// fakeBlockedEntry is the entry handed out for a closed fake gate.
type fakeBlockedEntry struct {
	gate *fakeGate
	key  consumergate.Key
}

func (*fakeBlockedEntry) Blocked() bool { return true }

func (e *fakeBlockedEntry) Watch(ctx context.Context, descriptor consumergate.DeliveryDescriptor) <-chan error {
	parked := consumergate.Parked{
		ConsumerGroup: e.key.ConsumerGroup,
		Topic:         descriptor.Topic,
		MessageID:     descriptor.MessageID,
		PartitionKey:  e.key.PartitionKey,
		Payload:       descriptor.Payload,
		Attempt:       descriptor.Attempt,
	}
	// Record synchronously so the parked descriptor is observable by the time
	// Watch returns, mirroring the file store's synchronous recordParked.
	e.gate.parked <- parked

	ch := make(chan error, 1)
	go func() {
		for {
			e.gate.mu.Lock()
			closed := e.gate.isClosed(e.key.ConsumerGroup, e.key.PartitionKey)
			changed := e.gate.changed
			e.gate.mu.Unlock()
			if !closed {
				e.gate.released <- parked.MessageID
				ch <- nil
				return
			}

			select {
			case <-ctx.Done():
				ch <- ctx.Err()
				return
			case <-changed:
			}
		}
	}()
	return ch
}

// startGatedConsumer builds a consumer with the fake gate directly as the 5th
// New arg, one registered mock controller, and a live subscription fed by the
// returned delivery channel.
func startGatedConsumer(t *testing.T, ctrl *gomock.Controller, gate consumergate.Gate, processFunc func(context.Context, Delivery) error) (Consumer, chan extqueue.Delivery) {
	t.Helper()

	deliveryChan := make(chan extqueue.Delivery, 4)
	mockSub := queuemock.NewMockSubscriber(ctrl)
	mockSub.EXPECT().Subscribe(gomock.Any(), gomock.Any(), gomock.Any()).Return(deliveryChan, nil)

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Subscriber().Return(mockSub)

	reg := newRegistry(t, mockQ, TopicKey("start"), "test-group")

	c := New(zaptest.NewLogger(t).Sugar(), tally.NoopScope, reg, errs.NewClassifierProcessor(), gate)

	handler := &testController{}
	setupController(handler, "test-handler", TopicKey("start"), "test-group", processFunc)
	require.NoError(t, c.Register(handler))

	require.NoError(t, c.Start(context.Background()))
	return c, deliveryChan
}

// gatedDelivery builds a MockDelivery that also tolerates visibility
// extensions while parked.
func gatedDelivery(ctrl *gomock.Controller, msg entityqueue.Message) (*queuemock.MockDelivery, chan struct{}) {
	mockDel := queuemock.NewMockDelivery(ctrl)
	done := setupDelivery(mockDel, msg, nil, nil)
	mockDel.EXPECT().ExtendVisibilityTimeout(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return mockDel, done
}

func TestConsumer_Gate_OpenGatePassesThrough(t *testing.T) {
	ctrl := gomock.NewController(t)
	gate := newFakeGate()

	handledMsg := ""
	c, deliveryChan := startGatedConsumer(t, ctrl, gate, func(_ context.Context, delivery Delivery) error {
		handledMsg = delivery.Message().ID
		return nil
	})

	msg := entityqueue.NewMessage("msg-1", []byte("payload"), "partition1", nil)
	mockDel, done := gatedDelivery(ctrl, msg)

	deliveryChan <- mockDel
	<-done

	assert.Equal(t, "msg-1", handledMsg)
	assert.Empty(t, gate.parked, "an open gate must not park deliveries")

	require.NoError(t, c.Stop(30000))
}

func TestConsumer_Gate_ParksThenReleases(t *testing.T) {
	ctrl := gomock.NewController(t)
	gate := newFakeGate()
	gate.close(consumergate.Key{ConsumerGroup: "test-group"})

	var processed atomic.Bool
	c, deliveryChan := startGatedConsumer(t, ctrl, gate, func(context.Context, Delivery) error {
		processed.Store(true)
		return nil
	})

	msg := entityqueue.NewMessage("msg-1", []byte("payload"), "partition1", nil)
	mockDel, done := gatedDelivery(ctrl, msg)
	deliveryChan <- mockDel

	// The parked record is written before the gate blocks, so awaiting it
	// proves the gate caught the message before the controller saw it.
	parked := <-gate.parked
	assert.Equal(t, "test-group", parked.ConsumerGroup)
	assert.Equal(t, TopicKey("start").String(), parked.Topic)
	assert.Equal(t, "msg-1", parked.MessageID)
	assert.Equal(t, "partition1", parked.PartitionKey)
	assert.Equal(t, []byte("payload"), parked.Payload)
	assert.Equal(t, 1, parked.Attempt)
	assert.False(t, processed.Load(), "controller must not run while its gate is closed")

	// Open the gate: the parked delivery proceeds, the release is recorded,
	// and the message is acked.
	gate.open(consumergate.Key{ConsumerGroup: "test-group"})
	assert.Equal(t, "msg-1", <-gate.released)
	<-done
	assert.True(t, processed.Load())

	require.NoError(t, c.Stop(30000))
}

func TestConsumer_Gate_PartitionScoped(t *testing.T) {
	ctrl := gomock.NewController(t)
	gate := newFakeGate()
	gate.close(consumergate.Key{ConsumerGroup: "test-group", PartitionKey: "gated-partition"})

	var handled sync.Map
	c, deliveryChan := startGatedConsumer(t, ctrl, gate, func(_ context.Context, delivery Delivery) error {
		handled.Store(delivery.Message().ID, true)
		return nil
	})

	gatedMsg := entityqueue.NewMessage("gated-msg", []byte("p"), "gated-partition", nil)
	gatedDel, gatedDone := gatedDelivery(ctrl, gatedMsg)
	openMsg := entityqueue.NewMessage("open-msg", []byte("p"), "open-partition", nil)
	openDel, openDone := gatedDelivery(ctrl, openMsg)

	deliveryChan <- gatedDel
	parked := <-gate.parked
	assert.Equal(t, "gated-msg", parked.MessageID)

	// Unrelated traffic keeps flowing through the same controller while one
	// partition is parked.
	deliveryChan <- openDel
	<-openDone
	_, ok := handled.Load("open-msg")
	assert.True(t, ok)
	_, ok = handled.Load("gated-msg")
	assert.False(t, ok)

	gate.open(consumergate.Key{ConsumerGroup: "test-group", PartitionKey: "gated-partition"})
	<-gatedDone
	_, ok = handled.Load("gated-msg")
	assert.True(t, ok)

	require.NoError(t, c.Stop(30000))
}

func TestConsumer_Gate_ShutdownWhileParked(t *testing.T) {
	ctrl := gomock.NewController(t)
	gate := newFakeGate()
	gate.close(consumergate.Key{ConsumerGroup: "test-group"})

	var processed atomic.Bool
	c, deliveryChan := startGatedConsumer(t, ctrl, gate, func(context.Context, Delivery) error {
		processed.Store(true)
		return nil
	})

	msg := entityqueue.NewMessage("msg-1", []byte("payload"), "partition1", nil)
	mockDel, _ := gatedDelivery(ctrl, msg)
	deliveryChan <- mockDel
	<-gate.parked

	// Stopping while parked must not stall shutdown, must not invoke the
	// controller, and must not ack/nack — the delivery is left in-flight for
	// redelivery after its visibility lapses.
	require.NoError(t, c.Stop(30000))
	assert.False(t, processed.Load())
	assert.Empty(t, gate.released, "a delivery dropped at shutdown is not released")
}

func TestConsumer_Gate_FailsOpenOnReadError(t *testing.T) {
	ctrl := gomock.NewController(t)
	gate := newFakeGate()
	gate.close(consumergate.Key{ConsumerGroup: "test-group"})
	gate.setErr(fmt.Errorf("gate medium unavailable"))

	handledMsg := ""
	c, deliveryChan := startGatedConsumer(t, ctrl, gate, func(_ context.Context, delivery Delivery) error {
		handledMsg = delivery.Message().ID
		return nil
	})

	msg := entityqueue.NewMessage("msg-1", []byte("payload"), "partition1", nil)
	mockDel, done := gatedDelivery(ctrl, msg)

	deliveryChan <- mockDel
	<-done

	assert.Equal(t, "msg-1", handledMsg, "a broken gate medium must not stall the pipeline")
	assert.Empty(t, gate.parked)

	require.NoError(t, c.Stop(30000))
}
