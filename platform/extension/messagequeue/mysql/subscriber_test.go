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
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"

	"github.com/uber/submitqueue/platform/base/failure"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"
)

// newDeliveryForTest builds a delivery against the standard fixture message,
// so a test only names the parts it cares about.
func newDeliveryForTest(sub *subscriber, attempt int, dlq extqueue.DLQConfig, retry extqueue.RetryConfig) *sqlDelivery {
	msg := entityqueue.NewMessage("msg-1", []byte("payload"), "part-1", nil)
	msg.Tenant = testTenant
	return newSQLDelivery(
		msg, "1", attempt, nil,
		sub, testTenant, "test_topic", "part-1", 100, "msg-1", "test-group",
		dlq, retry, failure.Failure{}, false,
	)
}

func testSubscriptionConfig() extqueue.SubscriptionConfig {
	return extqueue.DefaultSubscriptionConfig("test-subscriber", "test-consumer")
}

func TestRunTenantOperationsConcurrentlyWithDeadlines(t *testing.T) {
	slowStarted := make(chan struct{})
	releaseSlow := make(chan struct{})
	fastCompleted := make(chan struct{})
	operationCompleted := make(chan []tenantOperationResult[string])

	go func() {
		operationCompleted <- runTenantOperations(
			context.Background(),
			[]string{"slow", "fast"},
			time.Hour,
			func(ctx context.Context, tenant string) (string, error) {
				_, hasDeadline := ctx.Deadline()
				if !hasDeadline {
					return "", errors.New("tenant operation has no deadline")
				}
				if tenant == "slow" {
					close(slowStarted)
					<-releaseSlow
				} else {
					close(fastCompleted)
				}
				return tenant, nil
			},
		)
	}()

	<-slowStarted
	<-fastCompleted
	close(releaseSlow)
	results := <-operationCompleted
	assert.Equal(t, []tenantOperationResult[string]{
		{tenant: "slow", value: "slow"},
		{tenant: "fast", value: "fast"},
	}, results)
}

func TestRunTenantOperationsPropagatesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan string, 2)
	operationCompleted := make(chan []tenantOperationResult[struct{}])

	go func() {
		operationCompleted <- runTenantOperations(
			ctx,
			[]string{"tenant-a", "tenant-b"},
			time.Hour,
			func(ctx context.Context, tenant string) (struct{}, error) {
				started <- tenant
				<-ctx.Done()
				return struct{}{}, ctx.Err()
			},
		)
	}()

	assert.ElementsMatch(t, []string{"tenant-a", "tenant-b"}, []string{<-started, <-started})
	cancel()
	for _, result := range <-operationCompleted {
		assert.ErrorIs(t, result.err, context.Canceled)
	}
}

// newTestHeartbeatStore creates a mock heartbeat store that allows all calls
func newTestHeartbeatStore(ctrl *gomock.Controller) *MocksubscriberHeartbeatStore {
	mockHB := NewMocksubscriberHeartbeatStore(ctrl)
	mockHB.EXPECT().Heartbeat(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockHB.EXPECT().ActiveSubscribers(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]string{"self"}, nil).AnyTimes()
	mockHB.EXPECT().Deregister(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockHB.EXPECT().PurgeStale(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return mockHB
}

// newTestDeliveryStateStore creates a mock delivery state store that allows all calls
func newTestDeliveryStateStore(ctrl *gomock.Controller) *MockdeliveryStateStore {
	mockDS := NewMockdeliveryStateStore(ctrl)
	mockDS.EXPECT().MarkDelivered(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(0, nil).AnyTimes()
	mockDS.EXPECT().MarkAcked(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockDS.EXPECT().MarkNacked(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockDS.EXPECT().GetDeliveryState(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(DeliveryState{}, false, nil).AnyTimes()
	mockDS.EXPECT().AdvanceWatermark(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(int64(0), nil).AnyTimes()
	mockDS.EXPECT().ExtendVisibility(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return mockDS
}

func setupSubscriberTest(t *testing.T, mockMessageStore *MockmessageStore, mockOffsetStore *MockoffsetStore, mockLeaseStore *MockpartitionLeaseStore) extqueue.Subscriber {
	t.Helper()
	ctrl := gomock.NewController(t)
	mockHeartbeatStore := newTestHeartbeatStore(ctrl)
	mockDeliveryStateStore := newTestDeliveryStateStore(ctrl)
	// Allow watermark advancement calls from poll loop
	mockOffsetStore.EXPECT().GetAckedOffset(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(int64(0), nil).AnyTimes()
	mockMessageStore.EXPECT().GetOffsetsAbove(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	return NewSubscriber(zaptest.NewLogger(t).Sugar().Named("subscriber"), tally.NoopScope.SubScope("subscriber"), mockMessageStore, mockOffsetStore, mockLeaseStore, mockHeartbeatStore, mockDeliveryStateStore, []string{testTenant})
}

func TestSubscriber_Subscribe(t *testing.T) {
	tests := []struct {
		name          string
		topics        []string
		expectSame    bool
		expectedChans int
	}{
		{
			name:          "single topic subscription",
			topics:        []string{"test_topic"},
			expectedChans: 1,
		},
		{
			name:          "multiple different topics",
			topics:        []string{"topic1", "topic2"},
			expectedChans: 2,
		},
		{
			name:          "same topic and consumer group returns same channel",
			topics:        []string{"test_topic", "test_topic"},
			expectSame:    true,
			expectedChans: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockMessageStore := NewMockmessageStore(ctrl)
			mockOffsetStore := NewMockoffsetStore(ctrl)
			mockLeaseStore := NewMockpartitionLeaseStore(ctrl)

			// Reached via releaseAllLeases on the shutdown path, and by the
			// discovery ticker if it fires before teardown.
			mockLeaseStore.EXPECT().GetLeasedPartitions(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]string{}, nil).AnyTimes()

			sub := setupSubscriberTest(t, mockMessageStore, mockOffsetStore, mockLeaseStore)
			// Close waits for managePartitions to exit; a bare cancel would only
			// signal it. Registered after the ctrl.Finish defer above so LIFO
			// runs it first — gomock fails a mock call made after Finish.
			defer func() {
				require.NoError(t, sub.Close())
			}()

			ctx := context.Background()
			cfg := testSubscriptionConfig()

			var channels []<-chan extqueue.Delivery
			for _, topic := range tt.topics {
				ch, err := sub.Subscribe(ctx, topic, cfg)
				require.NoError(t, err)
				assert.NotNil(t, ch)
				channels = append(channels, ch)
			}

			if tt.expectSame && len(channels) == 2 {
				assert.Equal(t, channels[0], channels[1], "should return same channel for same topic and consumer group")
			}
		})
	}
}

func TestSubscriber_SubscribeRejectsInvalidRetryConfig(t *testing.T) {
	tests := []struct {
		name    string
		retry   extqueue.RetryConfig
		wantErr string
	}{
		{name: "negative max attempts", retry: extqueue.RetryConfig{MaxAttempts: -1}, wantErr: "MaxAttempts"},
		{name: "negative initial backoff", retry: extqueue.RetryConfig{InitialBackoffMs: -1}, wantErr: "InitialBackoffMs"},
		{name: "negative max backoff", retry: extqueue.RetryConfig{MaxBackoffMs: -1}, wantErr: "MaxBackoffMs"},
		{name: "initial backoff above safety ceiling", retry: extqueue.RetryConfig{InitialBackoffMs: maxRetryBackoffMs + 1}, wantErr: "InitialBackoffMs"},
		{name: "max backoff above safety ceiling", retry: extqueue.RetryConfig{MaxBackoffMs: maxRetryBackoffMs + 1}, wantErr: "MaxBackoffMs"},
		{name: "initial backoff above configured maximum", retry: extqueue.RetryConfig{InitialBackoffMs: 2000, MaxBackoffMs: 1000}, wantErr: "must not exceed MaxBackoffMs"},
		{name: "negative multiplier", retry: extqueue.RetryConfig{BackoffMultiplier: -1}, wantErr: "BackoffMultiplier"},
		{name: "NaN multiplier", retry: extqueue.RetryConfig{BackoffMultiplier: math.NaN()}, wantErr: "BackoffMultiplier"},
		{name: "infinite multiplier", retry: extqueue.RetryConfig{BackoffMultiplier: math.Inf(1)}, wantErr: "BackoffMultiplier"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			sub := setupSubscriberTest(t, NewMockmessageStore(ctrl), NewMockoffsetStore(ctrl), NewMockpartitionLeaseStore(ctrl))
			t.Cleanup(func() { require.NoError(t, sub.Close()) })

			ch, err := sub.Subscribe(context.Background(), "test_topic", extqueue.SubscriptionConfig{Retry: tt.retry})
			require.Nil(t, ch)
			require.ErrorIs(t, err, ErrInvalidConfig)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestSubscriber_SubscribeRejectsInvalidIdentifiers(t *testing.T) {
	overlong := strings.Repeat("x", maxIdentifierLength+1)
	validConfig := testSubscriptionConfig()
	tests := []struct {
		name    string
		topic   string
		config  extqueue.SubscriptionConfig
		tenants []string
	}{
		{name: "no tenants", topic: "test_topic", config: validConfig},
		{name: "overlong topic", topic: overlong, config: validConfig, tenants: []string{testTenant}},
		{name: "overlong consumer group", topic: "test_topic", config: func() extqueue.SubscriptionConfig {
			cfg := validConfig
			cfg.ConsumerGroup = overlong
			return cfg
		}(), tenants: []string{testTenant}},
		{name: "overlong subscriber name", topic: "test_topic", config: func() extqueue.SubscriptionConfig {
			cfg := validConfig
			cfg.SubscriberName = overlong
			return cfg
		}(), tenants: []string{testTenant}},
		{name: "overlong tenant", topic: "test_topic", config: validConfig, tenants: []string{overlong}},
		{name: "non-ASCII tenant", topic: "test_topic", config: validConfig, tenants: []string{"tenant-é"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sub := NewSubscriber(
				zaptest.NewLogger(t).Sugar(),
				tally.NoopScope,
				nil,
				nil,
				nil,
				nil,
				nil,
				tt.tenants,
			)

			ch, err := sub.Subscribe(context.Background(), tt.topic, tt.config)
			require.Nil(t, ch)
			require.ErrorIs(t, err, ErrInvalidConfig)
		})
	}
}

func TestSubscriber_SubscribeContextCancellation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMessageStore := NewMockmessageStore(ctrl)
	mockOffsetStore := NewMockoffsetStore(ctrl)
	mockLeaseStore := NewMockpartitionLeaseStore(ctrl)
	mockLeaseStore.EXPECT().GetLeasedPartitions(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]string{}, nil).AnyTimes()

	sub := setupSubscriberTest(t, mockMessageStore, mockOffsetStore, mockLeaseStore)
	defer func() {
		require.NoError(t, sub.Close())
	}()

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := sub.Subscribe(ctx, "test_topic", testSubscriptionConfig())
	require.NoError(t, err)

	cancel()

	// Closing deliveryCh is the last step of the shutdown path, so a blocking
	// receive is the synchronization point (test timeout handles a hang).
	_, ok := <-ch
	assert.False(t, ok, "delivery channel should close when the subscribe context is cancelled")
}

func TestSubscriber_SubscribeReplacesStaleSubscription(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMessageStore := NewMockmessageStore(ctrl)
	mockOffsetStore := NewMockoffsetStore(ctrl)
	mockLeaseStore := NewMockpartitionLeaseStore(ctrl)
	mockLeaseStore.EXPECT().GetLeasedPartitions(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]string{}, nil).AnyTimes()

	sub := setupSubscriberTest(t, mockMessageStore, mockOffsetStore, mockLeaseStore)
	defer func() {
		require.NoError(t, sub.Close())
	}()

	cfg := testSubscriptionConfig()

	ctx, cancel := context.WithCancel(context.Background())
	stale, err := sub.Subscribe(ctx, "test_topic", cfg)
	require.NoError(t, err)

	cancel()
	_, ok := <-stale
	require.False(t, ok)

	fresh, err := sub.Subscribe(context.Background(), "test_topic", cfg)
	require.NoError(t, err)
	assert.NotEqual(t, stale, fresh, "a new subscription must not reuse the stale closed channel")

	select {
	case _, ok := <-fresh:
		t.Fatalf("new subscription channel should be open, receive returned ok=%v", ok)
	default:
	}
}

func TestSQLDelivery_Ack(t *testing.T) {
	tests := []struct {
		name         string
		alreadyAcked bool
		markAckedErr error
		expectErr    bool
	}{
		{
			name: "successful ack",
		},
		{
			name:         "already acknowledged returns error",
			alreadyAcked: true,
			expectErr:    true,
		},
		{
			name:         "MarkAcked failure returns error",
			markAckedErr: fmt.Errorf("db error"),
			expectErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockMsgStore := NewMockmessageStore(ctrl)
			mockOffStore := NewMockoffsetStore(ctrl)
			mockLeaseStore := NewMockpartitionLeaseStore(ctrl)
			mockDeliveryState := NewMockdeliveryStateStore(ctrl)

			sub := NewSubscriber(
				zaptest.NewLogger(t).Sugar(),
				tally.NoopScope,
				mockMsgStore,
				mockOffStore,
				mockLeaseStore,
				newTestHeartbeatStore(ctrl),
				mockDeliveryState,
				[]string{testTenant},
			)

			d := newDeliveryForTest(sub, 1, extqueue.DLQConfig{}, extqueue.RetryConfig{})

			if tt.alreadyAcked {
				d.acknowledged = true
			}

			if !tt.alreadyAcked {
				// Ack only calls MarkAcked — watermark is deferred to poll loop
				mockDeliveryState.EXPECT().MarkAcked(
					gomock.Any(), "test-group", testTenant, "test_topic", "part-1", int64(100),
				).Return(tt.markAckedErr)
			}

			err := d.Ack(context.Background())

			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.True(t, d.acknowledged)
			}
		})
	}
}

func TestSQLDelivery_Postpone(t *testing.T) {
	tests := []struct {
		name             string
		alreadyAcked     bool
		markPostponedErr error
		expectErr        bool
	}{
		{
			name: "successful postpone",
		},
		{
			name:         "already acknowledged returns error",
			alreadyAcked: true,
			expectErr:    true,
		},
		{
			name:             "MarkPostponed failure returns error",
			markPostponedErr: fmt.Errorf("db error"),
			expectErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockMsgStore := NewMockmessageStore(ctrl)
			mockOffStore := NewMockoffsetStore(ctrl)
			mockLeaseStore := NewMockpartitionLeaseStore(ctrl)
			mockDeliveryState := NewMockdeliveryStateStore(ctrl)

			sub := NewSubscriber(
				zaptest.NewLogger(t).Sugar(),
				tally.NoopScope,
				mockMsgStore,
				mockOffStore,
				mockLeaseStore,
				newTestHeartbeatStore(ctrl),
				mockDeliveryState,
				[]string{testTenant},
			)

			d := newDeliveryForTest(sub, 1, extqueue.DLQConfig{}, extqueue.RetryConfig{})

			if tt.alreadyAcked {
				d.acknowledged = true
			}

			if !tt.alreadyAcked {
				mockDeliveryState.EXPECT().MarkPostponed(
					gomock.Any(), "test-group", testTenant, "test_topic", "part-1", int64(100), int64(5000),
				).Return(tt.markPostponedErr)
			}

			err := d.Postpone(context.Background(), 5000)

			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.True(t, d.acknowledged)
			}
		})
	}
}

func TestSQLDelivery_Reject(t *testing.T) {
	tests := []struct {
		name          string
		dlqEnabled    bool
		alreadyAcked  bool
		moveToDLQErr  error
		expectErr     bool
		expectMoveDLQ bool
		expectAck     bool
	}{
		{
			name:          "DLQ enabled moves message to DLQ",
			dlqEnabled:    true,
			expectMoveDLQ: true,
		},
		{
			name:      "DLQ disabled marks as acked",
			expectAck: true,
		},
		{
			name:         "already acknowledged returns error",
			dlqEnabled:   true,
			alreadyAcked: true,
			expectErr:    true,
		},
		{
			name:          "DLQ enabled but MoveToDLQ fails",
			dlqEnabled:    true,
			moveToDLQErr:  fmt.Errorf("db error"),
			expectErr:     true,
			expectMoveDLQ: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockMsgStore := NewMockmessageStore(ctrl)
			mockOffStore := NewMockoffsetStore(ctrl)
			mockLeaseStore := NewMockpartitionLeaseStore(ctrl)
			mockDeliveryState := NewMockdeliveryStateStore(ctrl)

			sub := NewSubscriber(
				zaptest.NewLogger(t).Sugar(),
				tally.NoopScope,
				mockMsgStore,
				mockOffStore,
				mockLeaseStore,
				newTestHeartbeatStore(ctrl),
				mockDeliveryState,
				[]string{testTenant},
			)

			dlqConfig := extqueue.DLQConfig{
				Enabled:     tt.dlqEnabled,
				TopicSuffix: "_dlq",
			}

			d := newDeliveryForTest(sub, 1, dlqConfig, extqueue.RetryConfig{})

			if tt.alreadyAcked {
				d.acknowledged = true
			}

			if tt.expectMoveDLQ {
				mockMsgStore.EXPECT().MoveToDLQ(
					gomock.Any(), testTenant, "test_topic", "part-1", "msg-1", 1, failure.New("bad payload"), "_dlq",
				).Return(tt.moveToDLQErr)

				if tt.moveToDLQErr == nil {
					mockDeliveryState.EXPECT().MarkAcked(
						gomock.Any(), "test-group", testTenant, "test_topic", "part-1", int64(100),
					).Return(nil)
				}
			}

			if tt.expectAck {
				mockDeliveryState.EXPECT().MarkAcked(
					gomock.Any(), "test-group", testTenant, "test_topic", "part-1", int64(100),
				).Return(nil)
			}

			err := d.Reject(context.Background(), failure.New("bad payload"))

			if tt.expectErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.True(t, d.acknowledged)
		})
	}
}

// A nack that spends the last of the retry budget dead-letters here, with the
// reason it was given. Left to the poll loop, the message would be
// dead-lettered on the next round with a generic reason and this one lost.
//
// The boundary matters as much as the behaviour: dead-lettering one attempt too
// early would silently cost every message a retry.
func TestSQLDelivery_NackDeadLettersWhenBudgetSpent(t *testing.T) {
	tests := []struct {
		name             string
		attempt          int
		retry            extqueue.RetryConfig
		wantDLQ          bool
		wantRetryDelayMs int64
	}{
		{
			name: "first retry uses initial delay", attempt: 1,
			retry:            extqueue.RetryConfig{MaxAttempts: 3, InitialBackoffMs: 1000, MaxBackoffMs: 30000, BackoffMultiplier: 2},
			wantRetryDelayMs: 1000,
		},
		{
			name: "second retry multiplies delay", attempt: 2,
			retry:            extqueue.RetryConfig{MaxAttempts: 3, InitialBackoffMs: 1000, MaxBackoffMs: 30000, BackoffMultiplier: 2},
			wantRetryDelayMs: 2000,
		},
		{
			name: "delay is capped", attempt: 6,
			retry:            extqueue.RetryConfig{MaxAttempts: 10, InitialBackoffMs: 1000, MaxBackoffMs: 30000, BackoffMultiplier: 2},
			wantRetryDelayMs: 30000,
		},
		{
			name: "initial delay is capped", attempt: 1,
			retry:            extqueue.RetryConfig{MaxAttempts: 3, InitialBackoffMs: 5000, MaxBackoffMs: 2000, BackoffMultiplier: 2},
			wantRetryDelayMs: 2000,
		},
		{
			name: "unset multiplier uses constant delay", attempt: 2,
			retry:            extqueue.RetryConfig{MaxAttempts: 3, InitialBackoffMs: 1000, MaxBackoffMs: 30000},
			wantRetryDelayMs: 1000,
		},
		{
			name: "fractional multiplier compounds from initial delay", attempt: 3,
			retry:            extqueue.RetryConfig{MaxAttempts: 4, InitialBackoffMs: 1, BackoffMultiplier: 1.5},
			wantRetryDelayMs: 2,
		},
		{
			name: "uncapped backoff uses safety ceiling", attempt: 3,
			retry:            extqueue.RetryConfig{MaxAttempts: 4, InitialBackoffMs: 1000, BackoffMultiplier: 10},
			wantRetryDelayMs: maxRetryBackoffMs,
		},
		{
			name: "configured cap cannot exceed safety ceiling", attempt: 3,
			retry:            extqueue.RetryConfig{MaxAttempts: 4, InitialBackoffMs: 1000, MaxBackoffMs: 120000, BackoffMultiplier: 10},
			wantRetryDelayMs: maxRetryBackoffMs,
		},
		{
			name: "unset initial delay retries immediately", attempt: 1,
			retry: extqueue.RetryConfig{MaxAttempts: 3},
		},
		{
			name: "final attempt dead-letters", attempt: 3,
			retry:   extqueue.RetryConfig{MaxAttempts: 3, InitialBackoffMs: 1000, MaxBackoffMs: 30000, BackoffMultiplier: 2},
			wantDLQ: true,
		},
		{
			name: "single-attempt budget dead-letters at once", attempt: 1,
			retry:   extqueue.RetryConfig{MaxAttempts: 1},
			wantDLQ: true,
		},
		// A zero budget is not "dead-letter immediately" — it is unconfigured,
		// and the poll loop still governs.
		{name: "unset budget never dead-letters here", attempt: 9},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockMsgStore := NewMockmessageStore(ctrl)
			mockDeliveryState := NewMockdeliveryStateStore(ctrl)

			sub := NewSubscriber(
				zaptest.NewLogger(t).Sugar(),
				tally.NoopScope,
				mockMsgStore,
				NewMockoffsetStore(ctrl),
				NewMockpartitionLeaseStore(ctrl),
				newTestHeartbeatStore(ctrl),
				mockDeliveryState,
				[]string{testTenant},
			)

			dlqConfig := extqueue.DLQConfig{Enabled: true, TopicSuffix: "_dlq"}
			d := newDeliveryForTest(sub, tt.attempt, dlqConfig, tt.retry)

			f := failure.New("boom", failure.Subject{Type: "batch", ID: "q/batch/1"})

			if tt.wantDLQ {
				mockMsgStore.EXPECT().MoveToDLQ(
					gomock.Any(), testTenant, "test_topic", "part-1", "msg-1", tt.attempt, f, "_dlq",
				).Return(nil)
				mockDeliveryState.EXPECT().MarkAcked(
					gomock.Any(), "test-group", testTenant, "test_topic", "part-1", int64(100),
				).Return(nil)
			} else {
				mockDeliveryState.EXPECT().MarkNacked(
					gomock.Any(), "test-group", testTenant, "test_topic", "part-1", int64(100), tt.wantRetryDelayMs,
				).Return(nil)
			}

			require.NoError(t, d.Nack(context.Background(), f))
			assert.True(t, d.acknowledged)
		})
	}
}

// A message arriving from its original topic has no failure to report, which is
// how a DLQ consumer tells "nothing recorded" apart from a recorded failure
// that named nothing.
func TestSQLDelivery_FailureAbsentOnNormalDelivery(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	sub := NewSubscriber(
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
		NewMockmessageStore(ctrl),
		NewMockoffsetStore(ctrl),
		NewMockpartitionLeaseStore(ctrl),
		newTestHeartbeatStore(ctrl),
		NewMockdeliveryStateStore(ctrl),
		[]string{testTenant},
	)

	d := newDeliveryForTest(sub, 1, extqueue.DLQConfig{}, extqueue.RetryConfig{})

	got, failed := d.Failure()
	assert.False(t, failed)
	assert.Equal(t, failure.Failure{}, got)
}

// TestSubscriber_Close tests subscriber close behavior
func TestSubscriber_Close(t *testing.T) {
	tests := []struct {
		name           string
		setupSub       func(ctx context.Context, sub extqueue.Subscriber) error
		closeCount     int
		subscribeAfter bool
		expectSubError bool
	}{
		{
			name: "close with active subscription",
			setupSub: func(ctx context.Context, sub extqueue.Subscriber) error {
				_, err := sub.Subscribe(ctx, "test_topic", testSubscriptionConfig())
				return err
			},
			closeCount: 1,
		},
		{
			name:       "close is idempotent",
			setupSub:   func(ctx context.Context, sub extqueue.Subscriber) error { return nil },
			closeCount: 2,
		},
		{
			name:           "subscribe after close fails",
			setupSub:       func(ctx context.Context, sub extqueue.Subscriber) error { return nil },
			closeCount:     1,
			subscribeAfter: true,
			expectSubError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockMessageStore := NewMockmessageStore(ctrl)
			mockOffsetStore := NewMockoffsetStore(ctrl)
			mockLeaseStore := NewMockpartitionLeaseStore(ctrl)

			// Expect lease operations during cleanup
			mockLeaseStore.EXPECT().GetLeasedPartitions(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]string{}, nil).AnyTimes()

			sub := setupSubscriberTest(t, mockMessageStore, mockOffsetStore, mockLeaseStore)
			ctx := context.Background()

			// Setup subscription if needed
			if tt.setupSub != nil {
				err := tt.setupSub(ctx, sub)
				require.NoError(t, err)
			}

			// Close multiple times if needed
			for i := 0; i < tt.closeCount; i++ {
				err := sub.Close()
				require.NoError(t, err)
			}

			// Try to subscribe after close if needed
			if tt.subscribeAfter {
				ch, err := sub.Subscribe(ctx, "test_topic", testSubscriptionConfig())
				if tt.expectSubError {
					require.Error(t, err)
					require.True(t, errors.Is(err, ErrSubscriberClosed))
					assert.Nil(t, ch)
				} else {
					require.NoError(t, err)
					assert.NotNil(t, ch)
				}
			}
		})
	}
}

func TestSubscriber_CloseReturnsErrorOnSupervisorTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	subscriberImpl := setupSubscriberTest(
		t,
		NewMockmessageStore(ctrl),
		NewMockoffsetStore(ctrl),
		NewMockpartitionLeaseStore(ctrl),
	)
	sub := subscriberImpl.(*subscriber)
	sub.shutdownTimeout = 0
	sub.subscriptions["test-topic:test-consumer"] = &subscription{
		topic:      "test-topic",
		config:     testSubscriptionConfig(),
		cancelFunc: func() {},
		done:       make(chan struct{}),
	}

	require.Error(t, sub.Close())
	assert.True(t, sub.closed)

	deliveries, err := sub.Subscribe(context.Background(), "another-topic", testSubscriptionConfig())
	require.ErrorIs(t, err, ErrSubscriberClosed)
	assert.Nil(t, deliveries)
}

func TestSubscriber_SubscribeDuringCloseDoesNotWaitForSupervisor(t *testing.T) {
	ctrl := gomock.NewController(t)
	subscriberImpl := setupSubscriberTest(
		t,
		NewMockmessageStore(ctrl),
		NewMockoffsetStore(ctrl),
		NewMockpartitionLeaseStore(ctrl),
	)
	sub := subscriberImpl.(*subscriber)
	sub.shutdownTimeout = time.Hour
	hung := make(chan struct{})
	sub.subscriptions["test-topic:test-consumer"] = &subscription{
		topic:      "test-topic",
		config:     testSubscriptionConfig(),
		cancelFunc: func() {},
		done:       hung,
	}

	closeErr := make(chan error, 1)
	go func() {
		closeErr <- sub.Close()
	}()

	for {
		sub.mu.RLock()
		closed := sub.closed
		sub.mu.RUnlock()
		if closed {
			break
		}
	}

	deliveries, err := sub.Subscribe(context.Background(), "another-topic", testSubscriptionConfig())
	require.ErrorIs(t, err, ErrSubscriberClosed)
	assert.Nil(t, deliveries)

	close(hung)
	require.NoError(t, <-closeErr)
}

// TestSubscriber_ReconcilePartitionWorkers tests that workers are started/stopped
// based on lease changes.
func TestSubscriber_ReconcilePartitionWorkers(t *testing.T) {
	tests := []struct {
		name          string
		initialLeases []string
		updatedLeases []string
	}{
		{
			name:          "start workers for new leases",
			initialLeases: []string{},
			updatedLeases: []string{"part-1", "part-2"},
		},
		{
			name:          "stop workers for lost leases",
			initialLeases: []string{"part-1", "part-2"},
			updatedLeases: []string{"part-1"},
		},
		{
			name:          "no changes when leases unchanged",
			initialLeases: []string{"part-1"},
			updatedLeases: []string{"part-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			mockMessageStore := NewMockmessageStore(ctrl)
			mockOffsetStore := NewMockoffsetStore(ctrl)
			mockLeaseStore := NewMockpartitionLeaseStore(ctrl)

			s := NewSubscriber(
				zaptest.NewLogger(t).Sugar(),
				tally.NoopScope,
				mockMessageStore,
				mockOffsetStore,
				mockLeaseStore,
				newTestHeartbeatStore(ctrl),
				newTestDeliveryStateStore(ctrl),
				[]string{testTenant},
			)

			// Allow offset initialization, fetch, and watermark calls from workers
			mockOffsetStore.EXPECT().Initialize(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			mockOffsetStore.EXPECT().GetAckedOffset(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(int64(0), nil).AnyTimes()
			mockMessageStore.EXPECT().FetchByOffset(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
			mockMessageStore.EXPECT().GetOffsetsAbove(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
			mockMessageStore.EXPECT().GarbageCollect(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(int64(0), nil).AnyTimes()
			mockOffsetStore.EXPECT().GetMinAckedOffset(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(int64(0), false, nil).AnyTimes()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			sub := &subscription{
				topic:      "test_topic",
				config:     testSubscriptionConfig(),
				deliveryCh: make(chan extqueue.Delivery, 100),
				workers:    make(map[entityqueue.PartitionIdentity]*partitionWorker),
			}

			// Start initial workers
			s.reconcilePartitionWorkers(ctx, sub, tenantPartitionKeys(testTenant, tt.initialLeases))

			sub.workersMu.Lock()
			assert.Equal(t, len(tt.initialLeases), len(sub.workers))
			sub.workersMu.Unlock()

			// Reconcile with updated leases
			s.reconcilePartitionWorkers(ctx, sub, tenantPartitionKeys(testTenant, tt.updatedLeases))

			sub.workersMu.Lock()
			assert.Equal(t, len(tt.updatedLeases), len(sub.workers))
			for _, pk := range tt.updatedLeases {
				assert.Contains(t, sub.workers, entityqueue.PartitionIdentity{Tenant: testTenant, PartitionKey: pk})
			}
			sub.workersMu.Unlock()

			// Cleanup: stop all workers
			cancel()
			s.stopAllWorkers(sub)
		})
	}
}

func TestSubscriber_ReconcilePartitionWorkersKeepsTenantIdentity(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockMessageStore := NewMockmessageStore(ctrl)
	mockOffsetStore := NewMockoffsetStore(ctrl)
	s := NewSubscriber(
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
		mockMessageStore,
		mockOffsetStore,
		NewMockpartitionLeaseStore(ctrl),
		newTestHeartbeatStore(ctrl),
		newTestDeliveryStateStore(ctrl),
		[]string{"tenant-a", "tenant-b"},
	)

	mockOffsetStore.EXPECT().Initialize(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockOffsetStore.EXPECT().GetAckedOffset(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(int64(0), nil).AnyTimes()
	mockOffsetStore.EXPECT().GetMinAckedOffset(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(int64(0), false, nil).AnyTimes()
	mockMessageStore.EXPECT().FetchByOffset(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	mockMessageStore.EXPECT().GetOffsetsAbove(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	ctx, cancel := context.WithCancel(context.Background())
	sub := &subscription{
		topic:      "test-topic",
		config:     testSubscriptionConfig(),
		deliveryCh: make(chan extqueue.Delivery, 2),
		workers:    make(map[entityqueue.PartitionIdentity]*partitionWorker),
	}
	partitions := []entityqueue.PartitionIdentity{
		{Tenant: "tenant-a", PartitionKey: "shared"},
		{Tenant: "tenant-b", PartitionKey: "shared"},
	}

	s.reconcilePartitionWorkers(ctx, sub, partitions)
	require.Len(t, sub.workers, 2)
	for _, partition := range partitions {
		worker, ok := sub.workers[partition]
		require.True(t, ok)
		assert.Equal(t, partition.Tenant, worker.tenant)
		assert.Equal(t, partition.PartitionKey, worker.partitionKey)
	}

	cancel()
	s.stopAllWorkers(sub)
}

func TestSubscriber_DiscoverAndReconcileWorkersIsolatesTenantFailures(t *testing.T) {
	const (
		tenantBefore     = "tenant-before"
		tenantFailed     = "tenant-failed"
		tenantAfter      = "tenant-after"
		tenantFailedLast = "tenant-failed-last"
	)

	ctrl := gomock.NewController(t)
	mockLeaseStore := NewMockpartitionLeaseStore(ctrl)
	discoveryErr := errors.New("tenant store unavailable")
	lastDiscoveryErr := errors.New("last tenant store unavailable")

	cfg := testSubscriptionConfig()
	cfg.PollIntervalMs = int64(time.Hour / time.Millisecond)

	mockLeaseStore.EXPECT().
		GetLeasedPartitions(gomock.Any(), tenantBefore, "test-topic", cfg.SubscriberName, cfg.ConsumerGroup).
		Return(nil, nil)
	mockLeaseStore.EXPECT().
		DiscoverAndAcquirePartitions(gomock.Any(), tenantBefore, "test-topic", cfg.SubscriberName, cfg.ConsumerGroup, cfg.LeaseDurationMs, 0).
		Return(1, []string{"before-new"}, nil)
	mockLeaseStore.EXPECT().
		GetLeasedPartitions(gomock.Any(), tenantBefore, "test-topic", cfg.SubscriberName, cfg.ConsumerGroup).
		Return([]string{"before-new"}, nil)
	mockLeaseStore.EXPECT().
		GetLeasedPartitions(gomock.Any(), tenantFailed, "test-topic", cfg.SubscriberName, cfg.ConsumerGroup).
		Return(nil, discoveryErr)
	mockLeaseStore.EXPECT().
		GetLeasedPartitions(gomock.Any(), tenantAfter, "test-topic", cfg.SubscriberName, cfg.ConsumerGroup).
		Return(nil, nil)
	mockLeaseStore.EXPECT().
		DiscoverAndAcquirePartitions(gomock.Any(), tenantAfter, "test-topic", cfg.SubscriberName, cfg.ConsumerGroup, cfg.LeaseDurationMs, 0).
		Return(1, []string{"after-new"}, nil)
	mockLeaseStore.EXPECT().
		GetLeasedPartitions(gomock.Any(), tenantAfter, "test-topic", cfg.SubscriberName, cfg.ConsumerGroup).
		Return([]string{"after-new"}, nil)
	mockLeaseStore.EXPECT().
		GetLeasedPartitions(gomock.Any(), tenantFailedLast, "test-topic", cfg.SubscriberName, cfg.ConsumerGroup).
		Return(nil, lastDiscoveryErr)

	s := NewSubscriber(
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
		NewMockmessageStore(ctrl),
		NewMockoffsetStore(ctrl),
		mockLeaseStore,
		newTestHeartbeatStore(ctrl),
		newTestDeliveryStateStore(ctrl),
		[]string{tenantBefore, tenantFailed, tenantAfter, tenantFailedLast},
	)

	failedWorkerDone := make(chan struct{})
	close(failedWorkerDone)
	failedWorkerKey := entityqueue.PartitionIdentity{Tenant: tenantFailed, PartitionKey: "failed-existing"}
	failedDrainSince := time.Now().Add(-time.Hour)
	sub := &subscription{
		topic:      "test-topic",
		config:     cfg,
		deliveryCh: make(chan extqueue.Delivery, 3),
		workers: map[entityqueue.PartitionIdentity]*partitionWorker{
			failedWorkerKey: {
				cancelFunc: func() {},
				done:       failedWorkerDone,
			},
		},
		lastDiscoveredPartitions: []entityqueue.PartitionIdentity{{Tenant: tenantFailed, PartitionKey: "failed-discovered"}},
		drainedSince:             map[entityqueue.PartitionIdentity]time.Time{failedWorkerKey: failedDrainSince},
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		s.stopAllWorkers(sub)
		sub.workerWg.Wait()
	})

	err := s.discoverAndReconcileWorkers(ctx, sub, true)
	require.ErrorIs(t, err, discoveryErr)
	require.ErrorIs(t, err, lastDiscoveryErr)

	sub.workersMu.Lock()
	workerKeys := make([]entityqueue.PartitionIdentity, 0, len(sub.workers))
	for key := range sub.workers {
		workerKeys = append(workerKeys, key)
	}
	discovered := append([]entityqueue.PartitionIdentity(nil), sub.lastDiscoveredPartitions...)
	sub.workersMu.Unlock()

	assert.ElementsMatch(t, []entityqueue.PartitionIdentity{
		{Tenant: tenantBefore, PartitionKey: "before-new"},
		{Tenant: tenantAfter, PartitionKey: "after-new"},
	}, workerKeys)
	assert.ElementsMatch(t, []entityqueue.PartitionIdentity{
		{Tenant: tenantBefore, PartitionKey: "before-new"},
		{Tenant: tenantFailed, PartitionKey: "failed-discovered"},
		{Tenant: tenantAfter, PartitionKey: "after-new"},
	}, discovered)
	assert.NotContains(t, sub.drainedSince, failedWorkerKey)
}

func TestSubscriber_DiscoverFailureStopsUnconfirmedWorkers(t *testing.T) {
	const tenant = "tenant-failed"
	ctrl := gomock.NewController(t)
	mockLeaseStore := NewMockpartitionLeaseStore(ctrl)
	cfg := testSubscriptionConfig()
	cfg.PollIntervalMs = int64(time.Hour / time.Millisecond)
	discoveryErr := errors.New("tenant store unavailable")

	mockLeaseStore.EXPECT().
		GetLeasedPartitions(gomock.Any(), tenant, "test-topic", cfg.SubscriberName, cfg.ConsumerGroup).
		Return(nil, discoveryErr)

	s := NewSubscriber(
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
		NewMockmessageStore(ctrl),
		NewMockoffsetStore(ctrl),
		mockLeaseStore,
		newTestHeartbeatStore(ctrl),
		newTestDeliveryStateStore(ctrl),
		[]string{tenant},
	)

	failedWorkerDone := make(chan struct{})
	close(failedWorkerDone)
	failedWorkerKey := entityqueue.PartitionIdentity{Tenant: tenant, PartitionKey: "p1"}
	sub := &subscription{
		topic:      "test-topic",
		config:     cfg,
		deliveryCh: make(chan extqueue.Delivery, 1),
		workers: map[entityqueue.PartitionIdentity]*partitionWorker{
			failedWorkerKey: {
				cancelFunc: func() {},
				done:       failedWorkerDone,
			},
		},
	}

	err := s.discoverAndReconcileWorkers(context.Background(), sub, true)
	require.ErrorIs(t, err, discoveryErr)
	assert.NotContains(t, sub.workers, failedWorkerKey)
}

func TestSubscriber_DrainedPartitionKeepsOffsetWhenLeaseReleaseFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockLeaseStore := NewMockpartitionLeaseStore(ctrl)
	cfg := testSubscriptionConfig()
	partition := entityqueue.PartitionIdentity{Tenant: testTenant, PartitionKey: "drained"}

	mockLeaseStore.EXPECT().
		GetLeasedPartitions(gomock.Any(), testTenant, "test-topic", cfg.SubscriberName, cfg.ConsumerGroup).
		Return([]string{partition.PartitionKey}, nil)
	mockLeaseStore.EXPECT().
		DiscoverAndAcquirePartitions(gomock.Any(), testTenant, "test-topic", cfg.SubscriberName, cfg.ConsumerGroup, cfg.LeaseDurationMs, 0).
		Return(0, nil, nil)
	mockLeaseStore.EXPECT().
		GetLeasedPartitions(gomock.Any(), testTenant, "test-topic", cfg.SubscriberName, cfg.ConsumerGroup).
		Return([]string{partition.PartitionKey}, nil)
	mockLeaseStore.EXPECT().
		ReleaseLease(gomock.Any(), testTenant, "test-topic", partition.PartitionKey, cfg.SubscriberName, cfg.ConsumerGroup).
		Return(errors.New("release failed"))

	s := NewSubscriber(
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
		NewMockmessageStore(ctrl),
		NewMockoffsetStore(ctrl),
		mockLeaseStore,
		newTestHeartbeatStore(ctrl),
		newTestDeliveryStateStore(ctrl),
		[]string{testTenant},
	)
	workerDone := make(chan struct{})
	close(workerDone)
	sub := &subscription{
		topic:      "test-topic",
		config:     cfg,
		deliveryCh: make(chan extqueue.Delivery),
		workers: map[entityqueue.PartitionIdentity]*partitionWorker{
			partition: {
				cancelFunc: func() {},
				done:       workerDone,
			},
		},
		drainedSince: map[entityqueue.PartitionIdentity]time.Time{partition: time.Now().Add(-time.Hour)},
	}

	require.NoError(t, s.discoverAndReconcileWorkers(context.Background(), sub, true))
	assert.Contains(t, sub.drainedSince, partition)
}

func TestSubscriber_ReleaseAllLeasesContinuesAfterErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockLeaseStore := NewMockpartitionLeaseStore(ctrl)
	releaseErr := errors.New("release failed")
	discoveryErr := errors.New("lease lookup failed")
	cfg := testSubscriptionConfig()

	mockLeaseStore.EXPECT().
		GetLeasedPartitions(gomock.Any(), "tenant-1", "test-topic", cfg.SubscriberName, cfg.ConsumerGroup).
		Return([]string{"p1", "p2"}, nil)
	mockLeaseStore.EXPECT().
		ReleaseLease(gomock.Any(), "tenant-1", "test-topic", "p1", cfg.SubscriberName, cfg.ConsumerGroup).
		Return(releaseErr)
	mockLeaseStore.EXPECT().
		ReleaseLease(gomock.Any(), "tenant-1", "test-topic", "p2", cfg.SubscriberName, cfg.ConsumerGroup).
		Return(nil)
	mockLeaseStore.EXPECT().
		GetLeasedPartitions(gomock.Any(), "tenant-2", "test-topic", cfg.SubscriberName, cfg.ConsumerGroup).
		Return(nil, discoveryErr)
	mockLeaseStore.EXPECT().
		GetLeasedPartitions(gomock.Any(), "tenant-3", "test-topic", cfg.SubscriberName, cfg.ConsumerGroup).
		Return([]string{"p3"}, nil)
	mockLeaseStore.EXPECT().
		ReleaseLease(gomock.Any(), "tenant-3", "test-topic", "p3", cfg.SubscriberName, cfg.ConsumerGroup).
		Return(nil)

	s := NewSubscriber(
		zaptest.NewLogger(t).Sugar(), tally.NoopScope,
		NewMockmessageStore(ctrl), NewMockoffsetStore(ctrl),
		mockLeaseStore, NewMocksubscriberHeartbeatStore(ctrl),
		NewMockdeliveryStateStore(ctrl),
		[]string{"tenant-1", "tenant-2", "tenant-3"},
	)
	sub := &subscription{topic: "test-topic", config: cfg}

	err := s.releaseAllLeases(context.Background(), sub)
	require.ErrorIs(t, err, releaseErr)
	require.ErrorIs(t, err, discoveryErr)
}

func TestSubscriber_DeregisterHeartbeatContinuesAfterErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockHeartbeatStore := NewMocksubscriberHeartbeatStore(ctrl)
	firstErr := errors.New("first deregistration failed")
	lastErr := errors.New("last deregistration failed")
	cfg := testSubscriptionConfig()

	mockHeartbeatStore.EXPECT().
		Deregister(gomock.Any(), "tenant-1", "test-topic", cfg.SubscriberName, cfg.ConsumerGroup).
		Return(firstErr)
	mockHeartbeatStore.EXPECT().
		Deregister(gomock.Any(), "tenant-2", "test-topic", cfg.SubscriberName, cfg.ConsumerGroup).
		Return(nil)
	mockHeartbeatStore.EXPECT().
		Deregister(gomock.Any(), "tenant-3", "test-topic", cfg.SubscriberName, cfg.ConsumerGroup).
		Return(lastErr)

	s := NewSubscriber(
		zaptest.NewLogger(t).Sugar(), tally.NoopScope,
		NewMockmessageStore(ctrl), NewMockoffsetStore(ctrl),
		NewMockpartitionLeaseStore(ctrl), mockHeartbeatStore,
		NewMockdeliveryStateStore(ctrl),
		[]string{"tenant-1", "tenant-2", "tenant-3"},
	)
	sub := &subscription{topic: "test-topic", config: cfg}

	err := s.deregisterHeartbeat(context.Background(), sub)
	require.ErrorIs(t, err, firstErr)
	require.ErrorIs(t, err, lastErr)
}

// TestSubscriber_PartitionWorkerPollAndDeliver verifies a partition worker delivers messages.
func TestSubscriber_PartitionWorkerPollAndDeliver(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockMessageStore := NewMockmessageStore(ctrl)
	mockOffsetStore := NewMockoffsetStore(ctrl)
	mockLeaseStore := NewMockpartitionLeaseStore(ctrl)
	mockDeliveryState := NewMockdeliveryStateStore(ctrl)
	metricsScope := tally.NewTestScope("test", nil)

	s := NewSubscriber(
		zaptest.NewLogger(t).Sugar(),
		metricsScope,
		mockMessageStore,
		mockOffsetStore,
		mockLeaseStore,
		newTestHeartbeatStore(ctrl),
		mockDeliveryState,
		[]string{testTenant},
	)

	cfg := testSubscriptionConfig()
	deliveryCh := make(chan extqueue.Delivery, 10)
	sub := &subscription{
		topic:      "test_topic",
		config:     cfg,
		deliveryCh: deliveryCh,
		workers:    make(map[entityqueue.PartitionIdentity]*partitionWorker),
	}

	ctx := context.Background()

	mockOffsetStore.EXPECT().Initialize(gomock.Any(), testTenant, "test_topic", "part-1", cfg.ConsumerGroup).Return(nil)
	// GetAckedOffset is called twice: once by pollAndDeliver, once by advanceWatermark
	mockOffsetStore.EXPECT().GetAckedOffset(gomock.Any(), testTenant, "test_topic", "part-1", cfg.ConsumerGroup).Return(int64(0), nil).Times(2)

	row := messageRow{
		ID:           "msg-1",
		Offset:       1,
		PartitionKey: "part-1",
		Payload:      []byte("payload"),
		PublishedAt:  time.Now().UnixMilli(),
	}
	mockMessageStore.EXPECT().FetchByOffset(gomock.Any(), testTenant, "test_topic", "part-1", int64(0), cfg.BatchSize).
		Return([]messageRow{row}, nil)

	// Delivery state checks — GetDeliveryState returns not-found (new message)
	mockDeliveryState.EXPECT().GetDeliveryState(gomock.Any(), cfg.ConsumerGroup, testTenant, "test_topic", "part-1", int64(1)).Return(DeliveryState{}, false, nil)
	// MarkDelivered returns retry count 0 (first delivery)
	mockDeliveryState.EXPECT().MarkDelivered(gomock.Any(), cfg.ConsumerGroup, testTenant, "test_topic", "part-1", int64(1), cfg.VisibilityTimeoutMs).Return(0, nil)

	// advanceWatermark called at end of pollAndDeliver
	mockMessageStore.EXPECT().GetOffsetsAbove(gomock.Any(), testTenant, "test_topic", "part-1", int64(0), watermarkAdvancementLimit).Return([]int64{1}, nil)
	mockDeliveryState.EXPECT().AdvanceWatermark(gomock.Any(), cfg.ConsumerGroup, testTenant, "test_topic", "part-1", int64(0), []int64{1}).Return(int64(0), nil)

	w := &partitionWorker{
		tenant:       testTenant,
		partitionKey: "part-1",
		sub:          sub,
		subscriber:   s,
		done:         make(chan struct{}),
	}

	require.NoError(t, w.pollAndDeliver(ctx))

	// Verify message was delivered
	select {
	case del := <-deliveryCh:
		assert.Equal(t, "msg-1", del.Message().ID)
	default:
		t.Fatal("expected delivery but channel was empty")
	}

	// Verify offset was initialized only once
	assert.True(t, w.offsetInitialized)

	// The partition key is deliberately absent: topics partitioned by an entity
	// ID mint a key per request, batch or build, and tally never reclaims the
	// subscope a tag value creates. Partition identity stays in the logs.
	snapshot := metricsScope.Snapshot()
	var foundStart bool
	for _, counter := range snapshot.Counters() {
		if counter.Name() == "test.subscriber.poll.start" {
			foundStart = true
			assert.Equal(t, "test_topic", counter.Tags()["topic"])
			assert.NotContains(t, counter.Tags(), "partition_key")
		}
	}
	assert.True(t, foundStart, "expected poll.start counter")

	var foundFinish bool
	for _, histogram := range snapshot.Histograms() {
		if histogram.Name() == "test.subscriber.poll.finish" {
			foundFinish = true
			assert.Equal(t, "success", histogram.Tags()["result"])
			assert.Equal(t, "test_topic", histogram.Tags()["topic"])
			assert.NotContains(t, histogram.Tags(), "partition_key")
		}
		assert.NotContains(t, histogram.Name(), "poll.latency")
	}
	assert.True(t, foundFinish, "expected poll.finish histogram")
}

// TestSubscriber_PollAndDeliver_GCOnBusyTicks verifies that garbage collection
// runs on a partition that delivers a message on every poll tick. GC was gated
// on idle ticks, so a continuously busy partition never reclaimed acked rows.
func TestSubscriber_PollAndDeliver_GCOnBusyTicks(t *testing.T) {
	old := gcTickInterval
	gcTickInterval = 2
	t.Cleanup(func() { gcTickInterval = old })

	ctrl := gomock.NewController(t)

	mockMessageStore := NewMockmessageStore(ctrl)
	mockOffsetStore := NewMockoffsetStore(ctrl)
	mockLeaseStore := NewMockpartitionLeaseStore(ctrl)

	s := setupSubscriberTest(t, mockMessageStore, mockOffsetStore, mockLeaseStore).(*subscriber)

	cfg := testSubscriptionConfig()
	deliveryCh := make(chan extqueue.Delivery, 10)
	sub := &subscription{
		topic:      "test_topic",
		config:     cfg,
		deliveryCh: deliveryCh,
		workers:    make(map[entityqueue.PartitionIdentity]*partitionWorker),
	}
	w := &partitionWorker{
		tenant:       testTenant,
		partitionKey: "part-1",
		sub:          sub,
		subscriber:   s,
		done:         make(chan struct{}),
	}

	row := messageRow{
		ID:           "msg-1",
		Offset:       1,
		PartitionKey: "part-1",
		Payload:      []byte("payload"),
		PublishedAt:  time.Now().UnixMilli(),
	}
	// Every poll delivers one message, so the partition never idles.
	mockMessageStore.EXPECT().FetchByOffset(gomock.Any(), testTenant, "test_topic", "part-1", int64(0), cfg.BatchSize).
		Return([]messageRow{row}, nil).Times(3)
	mockOffsetStore.EXPECT().Initialize(gomock.Any(), testTenant, "test_topic", "part-1", cfg.ConsumerGroup).Return(nil)

	// The counter reaches gcTickInterval on the second busy tick.
	mockOffsetStore.EXPECT().GetMinAckedOffset(gomock.Any(), testTenant, "test_topic", "part-1").Return(int64(1), true, nil)
	mockMessageStore.EXPECT().GarbageCollect(gomock.Any(), testTenant, "test_topic", "part-1", int64(1)).Return(int64(1), nil)

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		require.NoError(t, w.pollAndDeliver(ctx))
		select {
		case <-deliveryCh:
		default:
			t.Fatal("expected a delivery on every busy tick")
		}
	}
}

// TestSubscriber_PollAndDeliver_PostponedBarrier verifies that a postponed
// message halts the partition scan (barrier), while a nacked message is
// skipped and later offsets keep flowing.
func TestSubscriber_PollAndDeliver_PostponedBarrier(t *testing.T) {
	tests := []struct {
		name string
		// state of the first row (offset 1); rows 2 and 3 have no delivery state
		firstRowState DeliveryState
		// expectDeliveries is how many of the later rows are delivered
		expectDeliveries int
	}{
		{
			name: "postponed row is a barrier, later offsets wait",
			firstRowState: DeliveryState{
				Acked:          false,
				InvisibleUntil: time.Now().UnixMilli() + 60000,
				Postponed:      true,
			},
			expectDeliveries: 0,
		},
		{
			name: "nacked row is skipped, later offsets flow",
			firstRowState: DeliveryState{
				Acked:          false,
				InvisibleUntil: time.Now().UnixMilli() + 60000,
				Postponed:      false,
			},
			expectDeliveries: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockMessageStore := NewMockmessageStore(ctrl)
			mockOffsetStore := NewMockoffsetStore(ctrl)
			mockLeaseStore := NewMockpartitionLeaseStore(ctrl)
			mockDeliveryState := NewMockdeliveryStateStore(ctrl)

			s := NewSubscriber(
				zaptest.NewLogger(t).Sugar(),
				tally.NoopScope,
				mockMessageStore,
				mockOffsetStore,
				mockLeaseStore,
				newTestHeartbeatStore(ctrl),
				mockDeliveryState,
				[]string{testTenant},
			)

			cfg := testSubscriptionConfig()
			deliveryCh := make(chan extqueue.Delivery, 10)
			sub := &subscription{
				topic:      "test_topic",
				config:     cfg,
				deliveryCh: deliveryCh,
				workers:    make(map[entityqueue.PartitionIdentity]*partitionWorker),
			}

			ctx := context.Background()

			mockOffsetStore.EXPECT().Initialize(gomock.Any(), testTenant, "test_topic", "part-1", cfg.ConsumerGroup).Return(nil)
			mockOffsetStore.EXPECT().GetAckedOffset(gomock.Any(), testTenant, "test_topic", "part-1", cfg.ConsumerGroup).Return(int64(0), nil).Times(2)

			rows := []messageRow{
				{ID: "msg-1", Offset: 1, PartitionKey: "part-1", Payload: []byte("p1"), PublishedAt: time.Now().UnixMilli()},
				{ID: "msg-2", Offset: 2, PartitionKey: "part-1", Payload: []byte("p2"), PublishedAt: time.Now().UnixMilli()},
				{ID: "msg-3", Offset: 3, PartitionKey: "part-1", Payload: []byte("p3"), PublishedAt: time.Now().UnixMilli()},
			}
			mockMessageStore.EXPECT().FetchByOffset(gomock.Any(), testTenant, "test_topic", "part-1", int64(0), cfg.BatchSize).
				Return(rows, nil)

			mockDeliveryState.EXPECT().GetDeliveryState(gomock.Any(), cfg.ConsumerGroup, testTenant, "test_topic", "part-1", int64(1)).
				Return(tt.firstRowState, true, nil)
			if tt.expectDeliveries > 0 {
				for _, offset := range []int64{2, 3} {
					mockDeliveryState.EXPECT().GetDeliveryState(gomock.Any(), cfg.ConsumerGroup, testTenant, "test_topic", "part-1", offset).
						Return(DeliveryState{}, false, nil)
					mockDeliveryState.EXPECT().MarkDelivered(gomock.Any(), cfg.ConsumerGroup, testTenant, "test_topic", "part-1", offset, cfg.VisibilityTimeoutMs).
						Return(0, nil)
				}
			}

			mockMessageStore.EXPECT().GetOffsetsAbove(gomock.Any(), testTenant, "test_topic", "part-1", int64(0), watermarkAdvancementLimit).Return(nil, nil)
			mockDeliveryState.EXPECT().AdvanceWatermark(gomock.Any(), cfg.ConsumerGroup, testTenant, "test_topic", "part-1", int64(0), gomock.Nil()).Return(int64(0), nil)

			w := &partitionWorker{
				tenant:       testTenant,
				partitionKey: "part-1",
				sub:          sub,
				subscriber:   s,
				done:         make(chan struct{}),
			}

			require.NoError(t, w.pollAndDeliver(ctx))

			delivered := 0
			for {
				select {
				case <-deliveryCh:
					delivered++
					continue
				default:
				}
				break
			}
			assert.Equal(t, tt.expectDeliveries, delivered)
		})
	}
}

// TestSubscriber_StopAllWorkers tests that all workers are stopped gracefully.
func TestSubscriber_StopAllWorkers(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockMessageStore := NewMockmessageStore(ctrl)
	mockOffsetStore := NewMockoffsetStore(ctrl)
	mockLeaseStore := NewMockpartitionLeaseStore(ctrl)

	s := NewSubscriber(
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
		mockMessageStore,
		mockOffsetStore,
		mockLeaseStore,
		newTestHeartbeatStore(ctrl),
		newTestDeliveryStateStore(ctrl),
		[]string{testTenant},
	)

	// Allow worker polling and watermark advancement
	mockOffsetStore.EXPECT().Initialize(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockOffsetStore.EXPECT().GetAckedOffset(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(int64(0), nil).AnyTimes()
	mockMessageStore.EXPECT().FetchByOffset(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	mockMessageStore.EXPECT().GetOffsetsAbove(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	mockMessageStore.EXPECT().GarbageCollect(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(int64(0), nil).AnyTimes()
	mockOffsetStore.EXPECT().GetMinAckedOffset(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(int64(0), false, nil).AnyTimes()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub := &subscription{
		topic:      "test_topic",
		config:     testSubscriptionConfig(),
		deliveryCh: make(chan extqueue.Delivery, 100),
		workers:    make(map[entityqueue.PartitionIdentity]*partitionWorker),
	}

	// Start 3 workers
	s.startPartitionWorker(ctx, sub, entityqueue.PartitionIdentity{Tenant: testTenant, PartitionKey: "part-1"})
	s.startPartitionWorker(ctx, sub, entityqueue.PartitionIdentity{Tenant: testTenant, PartitionKey: "part-2"})
	s.startPartitionWorker(ctx, sub, entityqueue.PartitionIdentity{Tenant: testTenant, PartitionKey: "part-3"})

	sub.workersMu.Lock()
	assert.Equal(t, 3, len(sub.workers))
	sub.workersMu.Unlock()

	// Collect done channels before stopping
	sub.workersMu.Lock()
	var doneChans []chan struct{}
	for _, w := range sub.workers {
		doneChans = append(doneChans, w.done)
	}
	sub.workersMu.Unlock()

	// Stop all workers
	s.stopAllWorkers(sub)

	// Verify all done channels are closed (test timeout handles hangs)
	for _, doneCh := range doneChans {
		<-doneCh
	}
}

func TestPartitionWorker_RunPollErrorLogging(t *testing.T) {
	tests := []struct {
		name             string
		pollError        func(context.Context) error
		cancelDuringPoll bool
		expectedLogError string
	}{
		{
			name: "worker cancellation is informational",
			pollError: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
			cancelDuringPoll: true,
		},
		{
			name: "store cancellation on active worker is logged",
			pollError: func(context.Context) error {
				return context.Canceled
			},
			expectedLogError: "get acked offset: context canceled",
		},
		{
			name: "store failure is logged",
			pollError: func(context.Context) error {
				return errors.New("store unavailable")
			},
			expectedLogError: "get acked offset: store unavailable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockMessageStore := NewMockmessageStore(ctrl)
			mockOffsetStore := NewMockoffsetStore(ctrl)
			mockLeaseStore := NewMockpartitionLeaseStore(ctrl)

			pollStarted := make(chan struct{}, 1)
			mockOffsetStore.EXPECT().Initialize(gomock.Any(), testTenant, "test_topic", "part-1", "test-consumer").Return(nil)
			mockOffsetStore.EXPECT().GetAckedOffset(gomock.Any(), testTenant, "test_topic", "part-1", "test-consumer").DoAndReturn(
				func(ctx context.Context, _, _, _, _ string) (int64, error) {
					select {
					case pollStarted <- struct{}{}:
					default:
					}
					return 0, tt.pollError(ctx)
				},
			).AnyTimes()

			core, logs := observer.New(zap.InfoLevel)
			s := NewSubscriber(
				zap.New(core).Sugar(),
				tally.NoopScope,
				mockMessageStore,
				mockOffsetStore,
				mockLeaseStore,
				newTestHeartbeatStore(ctrl),
				newTestDeliveryStateStore(ctrl),
				[]string{testTenant},
			)
			s.OnSignal = make(chan HookSignal, 1)
			cfg := testSubscriptionConfig()
			cfg.PollIntervalMs = 1
			sub := &subscription{
				topic:      "test_topic",
				config:     cfg,
				deliveryCh: make(chan extqueue.Delivery, 1),
			}
			worker := &partitionWorker{
				tenant:       testTenant,
				partitionKey: "part-1",
				sub:          sub,
				subscriber:   s,
				done:         make(chan struct{}),
			}

			ctx, cancel := context.WithCancel(context.Background())
			sub.workerWg.Add(1)
			go worker.run(ctx)

			<-pollStarted
			if tt.cancelDuringPoll {
				cancel()
			} else {
				<-s.OnSignal
				cancel()
			}
			<-worker.done

			pollFailures := logs.FilterMessage("poll failed").All()
			stoppedPolls := logs.FilterMessage("poll canceled while stopping partition worker").All()
			if tt.expectedLogError != "" {
				require.NotEmpty(t, pollFailures)
				for _, entry := range pollFailures {
					assert.Equal(t, zap.ErrorLevel, entry.Level)
					assert.Equal(t, tt.expectedLogError, entry.ContextMap()["error"])
				}
			} else {
				assert.Empty(t, pollFailures)
				require.Len(t, stoppedPolls, 1)
				assert.Equal(t, zap.InfoLevel, stoppedPolls[0].Level)
			}
		})
	}
}

func TestSubscriber_FairShareCap(t *testing.T) {
	tests := []struct {
		name       string
		self       string
		active     []string // as returned by the heartbeat store, deliberately unsorted
		owned      []string
		discovered []string
		want       int
	}{
		{
			// The starvation case: P=12, N=5. Independent ceil caps were 3
			// for every rank (sum 15), so 3/3/3/3/0 was stable. Remainder
			// caps are 3,3,2,2,2 (sum 12): rank 0 gets the remainder…
			name:       "uneven split first rank gets remainder",
			self:       "s1",
			active:     []string{"s3", "s1", "s5", "s2", "s4"},
			discovered: partitionKeysN(12),
			want:       3,
		},
		{
			// …and the last rank gets the floor, not zero-forever.
			name:       "uneven split last rank gets floor",
			self:       "s5",
			active:     []string{"s3", "s1", "s5", "s2", "s4"},
			discovered: partitionKeysN(12),
			want:       2,
		},
		{
			name:       "even split",
			self:       "s2",
			active:     []string{"s2", "s1"},
			discovered: partitionKeysN(4),
			want:       2,
		},
		{
			name:       "single subscriber is unlimited",
			self:       "s1",
			active:     []string{"s1"},
			discovered: partitionKeysN(4),
			want:       0,
		},
		{
			// P < N: the remainder share is 0 for high ranks, but the cap
			// keeps the historical minimum of 1 — with fewer partitions than
			// subscribers somebody idles regardless, and the floor preserves
			// the maxPart=0-means-unlimited contract.
			name:       "fewer partitions than subscribers floors at one",
			self:       "s4",
			active:     []string{"s1", "s2", "s3", "s4"},
			discovered: partitionKeysN(2),
			want:       1,
		},
		{
			// Own heartbeat missing from the active list (write failed this
			// interval): conservative ceil over n+1 contenders, never
			// unlimited.
			name:       "missing own heartbeat falls back to ceil",
			self:       "s-missing",
			active:     []string{"s1", "s2"},
			discovered: partitionKeysN(9),
			want:       3,
		},
		{
			name:       "owned and discovered are unioned",
			self:       "s1",
			active:     []string{"s1", "s2"},
			owned:      []string{"pk-00", "pk-extra"},
			discovered: []string{"pk-00", "pk-01", "pk-02"},
			want:       2, // union = 4 partitions, rank 0 of 2 -> 2
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockHB := NewMocksubscriberHeartbeatStore(ctrl)
			mockHB.EXPECT().
				ActiveSubscribers(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(tt.active, nil).
				AnyTimes()

			s := NewSubscriber(
				zaptest.NewLogger(t).Sugar(), tally.NoopScope,
				NewMockmessageStore(ctrl), NewMockoffsetStore(ctrl),
				NewMockpartitionLeaseStore(ctrl), mockHB,
				NewMockdeliveryStateStore(ctrl),
				[]string{testTenant},
			)
			sub := &subscription{
				topic:  "test-topic",
				config: extqueue.DefaultSubscriptionConfig(tt.self, "test-cg"),
			}

			got, err := s.fairShareCap(context.Background(), sub, testTenant, tt.owned, tt.discovered)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	// The anti-starvation invariant: whenever P >= N, per-rank caps sum to
	// exactly P, so no combination of at-cap subscribers can leave a
	// subscriber starved or a partition unclaimed.
	t.Run("caps sum to partition total", func(t *testing.T) {
		for n := 2; n <= 6; n++ {
			for p := n; p <= 13; p++ {
				active := make([]string, n)
				for i := range active {
					active[i] = fmt.Sprintf("s%d", i)
				}

				ctrl := gomock.NewController(t)
				mockHB := NewMocksubscriberHeartbeatStore(ctrl)
				mockHB.EXPECT().
					ActiveSubscribers(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(active, nil).
					AnyTimes()
				s := NewSubscriber(
					zaptest.NewLogger(t).Sugar(), tally.NoopScope,
					NewMockmessageStore(ctrl), NewMockoffsetStore(ctrl),
					NewMockpartitionLeaseStore(ctrl), mockHB,
					NewMockdeliveryStateStore(ctrl),
					[]string{testTenant},
				)

				sum := 0
				for _, self := range active {
					sub := &subscription{
						topic:  "test-topic",
						config: extqueue.DefaultSubscriptionConfig(self, "test-cg"),
					}
					cap, err := s.fairShareCap(context.Background(), sub, testTenant, nil, partitionKeysN(p))
					require.NoError(t, err)
					sum += cap
				}
				require.Equal(t, p, sum, "n=%d p=%d", n, p)
			}
		}
	})
}

// partitionKeysN generates n distinct partition keys.
func partitionKeysN(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("pk-%02d", i)
	}
	return keys
}

func tenantPartitionKeys(tenant string, partitions []string) []entityqueue.PartitionIdentity {
	keys := make([]entityqueue.PartitionIdentity, len(partitions))
	for i, partition := range partitions {
		keys[i] = entityqueue.PartitionIdentity{Tenant: tenant, PartitionKey: partition}
	}
	return keys
}

func TestSubscriber_RebalanceReleasesExcess(t *testing.T) {
	ctrl := gomock.NewController(t)

	// Two active subscribers, four partitions: self is rank 0 -> cap 2.
	mockHB := NewMocksubscriberHeartbeatStore(ctrl)
	mockHB.EXPECT().
		ActiveSubscribers(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]string{"s1", "s2"}, nil)

	// The lexicographically largest partitions beyond the cap are released.
	mockLease := NewMockpartitionLeaseStore(ctrl)
	mockLease.EXPECT().
		ReleaseLease(gomock.Any(), testTenant, "test-topic", "pk-c", "s1", "test-cg").
		Return(nil)
	mockLease.EXPECT().
		ReleaseLease(gomock.Any(), testTenant, "test-topic", "pk-d", "s1", "test-cg").
		Return(nil)

	s := NewSubscriber(
		zaptest.NewLogger(t).Sugar(), tally.NoopScope,
		NewMockmessageStore(ctrl), NewMockoffsetStore(ctrl),
		mockLease, mockHB, NewMockdeliveryStateStore(ctrl),
		[]string{testTenant},
	)
	sub := &subscription{
		topic:   "test-topic",
		config:  extqueue.DefaultSubscriptionConfig("s1", "test-cg"),
		workers: make(map[entityqueue.PartitionIdentity]*partitionWorker),
	}

	owned := []string{"pk-d", "pk-a", "pk-c", "pk-b"}
	released, err := s.rebalance(context.Background(), sub, testTenant, owned)
	require.NoError(t, err)
	assert.Equal(t, []string{"pk-c", "pk-d"}, released)
	// The caller's slice is shared with lease renewal and must not be
	// reordered (regression: rebalance used to sort it in place, making the
	// subsequent renewal hit the released tail and log ErrLeaseExpired).
	assert.Equal(t, []string{"pk-d", "pk-a", "pk-c", "pk-b"}, owned)
}

func TestSubscriber_RebalanceUnderCapReleasesNothing(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockHB := NewMocksubscriberHeartbeatStore(ctrl)
	mockHB.EXPECT().
		ActiveSubscribers(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]string{"s1", "s2"}, nil)

	// No ReleaseLease expectations: owning exactly the cap sheds nothing.
	s := NewSubscriber(
		zaptest.NewLogger(t).Sugar(), tally.NoopScope,
		NewMockmessageStore(ctrl), NewMockoffsetStore(ctrl),
		NewMockpartitionLeaseStore(ctrl), mockHB, NewMockdeliveryStateStore(ctrl),
		[]string{testTenant},
	)
	sub := &subscription{
		topic:   "test-topic",
		config:  extqueue.DefaultSubscriptionConfig("s1", "test-cg"),
		workers: make(map[entityqueue.PartitionIdentity]*partitionWorker),
		// Four known partitions across two subscribers -> rank-0 cap is 2:
		// owning exactly the cap must shed nothing.
		lastDiscoveredPartitions: tenantPartitionKeys(testTenant, []string{"pk-a", "pk-b", "pk-c", "pk-d"}),
	}

	released, err := s.rebalance(context.Background(), sub, testTenant, []string{"pk-a", "pk-b"})
	require.NoError(t, err)
	assert.Empty(t, released)
}

func TestUpdateDrainedTracking(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	earlier := now.Add(-time.Minute)
	grace := 30 * time.Second
	partition := func(key string) entityqueue.PartitionIdentity {
		return entityqueue.PartitionIdentity{Tenant: testTenant, PartitionKey: key}
	}

	tests := []struct {
		name        string
		prev        map[entityqueue.PartitionIdentity]time.Time
		owned       []entityqueue.PartitionIdentity
		discovered  []entityqueue.PartitionIdentity
		wantTracked map[entityqueue.PartitionIdentity]time.Time
		wantExpired []entityqueue.PartitionIdentity
	}{
		{
			name:        "owned and discovered is not tracked",
			owned:       []entityqueue.PartitionIdentity{partition("p1")},
			discovered:  []entityqueue.PartitionIdentity{partition("p1")},
			wantTracked: map[entityqueue.PartitionIdentity]time.Time{},
		},
		{
			name:        "freshly drained starts tracking now",
			owned:       []entityqueue.PartitionIdentity{partition("p1")},
			discovered:  nil,
			wantTracked: map[entityqueue.PartitionIdentity]time.Time{partition("p1"): now},
		},
		{
			name:        "already tracked keeps original since time",
			prev:        map[entityqueue.PartitionIdentity]time.Time{partition("p1"): now.Add(-10 * time.Second)},
			owned:       []entityqueue.PartitionIdentity{partition("p1")},
			wantTracked: map[entityqueue.PartitionIdentity]time.Time{partition("p1"): now.Add(-10 * time.Second)},
		},
		{
			name:        "drained past grace expires sorted",
			prev:        map[entityqueue.PartitionIdentity]time.Time{partition("p-b"): earlier, partition("p-a"): earlier, partition("p-young"): now.Add(-time.Second)},
			owned:       []entityqueue.PartitionIdentity{partition("p-b"), partition("p-a"), partition("p-young")},
			wantTracked: map[entityqueue.PartitionIdentity]time.Time{partition("p-a"): earlier, partition("p-b"): earlier, partition("p-young"): now.Add(-time.Second)},
			wantExpired: []entityqueue.PartitionIdentity{partition("p-a"), partition("p-b")},
		},
		{
			name:        "rediscovered partition resets the clock",
			prev:        map[entityqueue.PartitionIdentity]time.Time{partition("p1"): earlier},
			owned:       []entityqueue.PartitionIdentity{partition("p1")},
			discovered:  []entityqueue.PartitionIdentity{partition("p1")},
			wantTracked: map[entityqueue.PartitionIdentity]time.Time{},
		},
		{
			name:        "no longer owned is dropped from tracking",
			prev:        map[entityqueue.PartitionIdentity]time.Time{partition("gone"): earlier},
			owned:       []entityqueue.PartitionIdentity{partition("p1")},
			wantTracked: map[entityqueue.PartitionIdentity]time.Time{partition("p1"): now},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracked, expired := updateDrainedTracking(tt.prev, tt.owned, tt.discovered, grace, now)
			assert.Equal(t, tt.wantTracked, tracked)
			assert.Equal(t, tt.wantExpired, expired)
		})
	}
}
