package consumer_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	consumermock "github.com/uber/submitqueue/core/consumer/mock"
	"github.com/uber/submitqueue/core/errs"
	"github.com/uber/submitqueue/entity/queue"
	extqueue "github.com/uber/submitqueue/extension/queue"
	queuemock "github.com/uber/submitqueue/extension/queue/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

// setupController configures a MockController with standard expectations.
func setupController(mc *consumermock.MockController, name string, topicKey consumer.TopicKey, consumerGroup string, processFunc func(context.Context, consumer.Delivery) error) {
	mc.EXPECT().Name().Return(name).AnyTimes()
	mc.EXPECT().TopicKey().Return(topicKey).AnyTimes()
	mc.EXPECT().ConsumerGroup().Return(consumerGroup).AnyTimes()
	if processFunc != nil {
		mc.EXPECT().Process(gomock.Any(), gomock.Any()).DoAndReturn(processFunc).AnyTimes()
	}
}

// newRegistry creates a TopicRegistry with a mock queue and default subscription config.
func newRegistry(t *testing.T, q extqueue.Queue, topicKey consumer.TopicKey, consumerGroup string) consumer.TopicRegistry {
	t.Helper()
	reg, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{
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
func setupDelivery(del *queuemock.MockDelivery, msg queue.Message, ackErr, nackErr error) chan struct{} {
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

	reg, err := consumer.NewTopicRegistry(nil)
	require.NoError(t, err)

	c := consumer.New(logger, tally.NoopScope, reg)
	require.NotNil(t, c)
}

func TestConsumer_Register(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := zaptest.NewLogger(t).Sugar()

	reg, _ := consumer.NewTopicRegistry(nil)
	c := consumer.New(logger, tally.NoopScope, reg)

	handler1 := consumermock.NewMockController(ctrl)
	setupController(handler1, "handler1", consumer.TopicKeyRequest, "group1", nil)

	handler2 := consumermock.NewMockController(ctrl)
	setupController(handler2, "handler2", consumer.TopicKey("other-topic"), "group2", nil)

	err := c.Register(handler1)
	require.NoError(t, err)

	err = c.Register(handler2)
	require.NoError(t, err)
}

func TestConsumer_Register_DuplicateTopic(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := zaptest.NewLogger(t).Sugar()

	reg, _ := consumer.NewTopicRegistry(nil)
	c := consumer.New(logger, tally.NoopScope, reg)

	handler1 := consumermock.NewMockController(ctrl)
	setupController(handler1, "handler1", consumer.TopicKeyRequest, "group1", nil)

	handler2 := consumermock.NewMockController(ctrl)
	setupController(handler2, "handler2", consumer.TopicKeyRequest, "group2", nil)

	err := c.Register(handler1)
	require.NoError(t, err)

	err = c.Register(handler2)
	assert.Error(t, err)
}

func TestConsumer_Register_AfterStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := zaptest.NewLogger(t).Sugar()

	reg, _ := consumer.NewTopicRegistry(nil)
	c := consumer.New(logger, tally.NoopScope, reg)

	err := c.Stop(1000)
	require.NoError(t, err)

	handler := consumermock.NewMockController(ctrl)
	setupController(handler, "handler1", consumer.TopicKeyRequest, "group1", nil)

	err = c.Register(handler)
	assert.Error(t, err)
}

func TestConsumer_Start_NoHandlers(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()

	reg, _ := consumer.NewTopicRegistry(nil)
	c := consumer.New(logger, tally.NoopScope, reg)

	err := c.Start(context.Background())
	assert.Error(t, err)
}

func TestConsumer_Start_AfterStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := zaptest.NewLogger(t).Sugar()

	reg, _ := consumer.NewTopicRegistry(nil)
	c := consumer.New(logger, tally.NoopScope, reg)

	handler := consumermock.NewMockController(ctrl)
	setupController(handler, "handler1", consumer.TopicKeyRequest, "group1", nil)

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
	reg, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: consumer.TopicKeyRequest, Name: "request", Queue: mockQ}},
	)
	require.NoError(t, err)

	c := consumer.New(logger, tally.NoopScope, reg)

	handler := consumermock.NewMockController(ctrl)
	setupController(handler, "handler", consumer.TopicKeyRequest, "group", nil)

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

	reg := newRegistry(t, mockQ, consumer.TopicKeyRequest, "group")

	c := consumer.New(logger, tally.NoopScope, reg)

	handler := consumermock.NewMockController(ctrl)
	setupController(handler, "handler", consumer.TopicKeyRequest, "group", nil)

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

	reg := newRegistry(t, mockQ, consumer.TopicKeyRequest, "test-group")

	c := consumer.New(logger, tally.NoopScope, reg)

	handledMsg := ""
	handler := consumermock.NewMockController(ctrl)
	setupController(handler, "test-handler", consumer.TopicKeyRequest, "test-group",
		func(ctx context.Context, delivery consumer.Delivery) error {
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

	msg := queue.NewMessage("test-msg-1", []byte("payload"), "partition1", nil)
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

	reg := newRegistry(t, mockQ, consumer.TopicKeyRequest, "test-group")

	c := consumer.New(logger, tally.NoopScope, reg)

	handler := consumermock.NewMockController(ctrl)
	setupController(handler, "test-handler", consumer.TopicKeyRequest, "test-group",
		func(ctx context.Context, delivery consumer.Delivery) error {
			return errs.NewRetryableError(fmt.Errorf("processing failed"))
		},
	)

	err := c.Register(handler)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = c.Start(ctx)
	require.NoError(t, err)

	msg := queue.NewMessage("test-msg-2", []byte("payload"), "partition1", nil)
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

	reg := newRegistry(t, mockQ, consumer.TopicKeyRequest, "test-group")

	c := consumer.New(logger, tally.NoopScope, reg)

	handler := consumermock.NewMockController(ctrl)
	setupController(handler, "test-handler", consumer.TopicKeyRequest, "test-group",
		func(ctx context.Context, delivery consumer.Delivery) error {
			return fmt.Errorf("bad payload")
		},
	)

	err := c.Register(handler)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = c.Start(ctx)
	require.NoError(t, err)

	msg := queue.NewMessage("poison-msg", []byte("bad"), "partition1", nil)
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

	reg := newRegistry(t, mockQ, consumer.TopicKeyRequest, "test-group")

	c := consumer.New(logger, tally.NoopScope, reg)

	handler := consumermock.NewMockController(ctrl)
	setupController(handler, "test-handler", consumer.TopicKeyRequest, "test-group", nil)

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

			reg := newRegistry(t, mockQ, consumer.TopicKeyRequest, "test-group")

			testC := consumer.New(logger, testScope, reg)

			handler := consumermock.NewMockController(ctrl)
			setupController(handler, "test-handler", consumer.TopicKeyRequest, "test-group",
				func(ctx context.Context, delivery consumer.Delivery) error {
					return tt.handlerError
				},
			)

			err := testC.Register(handler)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			err = testC.Start(ctx)
			require.NoError(t, err)

			msg := queue.NewMessage("msg-1", []byte("payload"), "partition1", nil)
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

	reg := newRegistry(t, mockQ, consumer.TopicKeyRequest, "test-group")

	c := consumer.New(logger, scope, reg)

	handler := consumermock.NewMockController(ctrl)
	setupController(handler, "test-handler", consumer.TopicKeyRequest, "test-group",
		func(ctx context.Context, delivery consumer.Delivery) error { return nil },
	)

	err := c.Register(handler)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = c.Start(ctx)
	require.NoError(t, err)

	msg := queue.NewMessage("msg-1", []byte("payload"), "partition1", nil)
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

	reg := newRegistry(t, mockQ, consumer.TopicKeyRequest, "test-group")

	c := consumer.New(logger, scope, reg)

	handler := consumermock.NewMockController(ctrl)
	setupController(handler, "test-handler", consumer.TopicKeyRequest, "test-group",
		func(ctx context.Context, delivery consumer.Delivery) error {
			return errs.NewRetryableError(fmt.Errorf("processing failed"))
		},
	)

	err := c.Register(handler)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = c.Start(ctx)
	require.NoError(t, err)

	msg := queue.NewMessage("msg-1", []byte("payload"), "partition1", nil)
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
