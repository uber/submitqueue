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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"

	"github.com/uber/submitqueue/entity/queue"
	extqueue "github.com/uber/submitqueue/extension/queue"
)

func testSubscriptionConfig() extqueue.SubscriptionConfig {
	return extqueue.DefaultSubscriptionConfig("test-subscriber", "test-consumer")
}

// newTestHeartbeatStore creates a mock heartbeat store that allows all calls
func newTestHeartbeatStore(ctrl *gomock.Controller) *MocksubscriberHeartbeatStore {
	mockHB := NewMocksubscriberHeartbeatStore(ctrl)
	mockHB.EXPECT().Heartbeat(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockHB.EXPECT().ActiveSubscribers(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]string{"self"}, nil).AnyTimes()
	mockHB.EXPECT().Deregister(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return mockHB
}

// newTestDeliveryStateStore creates a mock delivery state store that allows all calls
func newTestDeliveryStateStore(ctrl *gomock.Controller) *MockdeliveryStateStore {
	mockDS := NewMockdeliveryStateStore(ctrl)
	mockDS.EXPECT().MarkDelivered(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(0, nil).AnyTimes()
	mockDS.EXPECT().MarkAcked(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockDS.EXPECT().MarkNacked(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockDS.EXPECT().GetDeliveryState(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(DeliveryState{}, false, nil).AnyTimes()
	mockDS.EXPECT().AdvanceWatermark(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(int64(0), nil).AnyTimes()
	mockDS.EXPECT().ExtendVisibility(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return mockDS
}

func setupSubscriberTest(t *testing.T, mockMessageStore *MockmessageStore, mockOffsetStore *MockoffsetStore, mockLeaseStore *MockpartitionLeaseStore) extqueue.Subscriber {
	t.Helper()
	ctrl := gomock.NewController(t)
	mockHeartbeatStore := newTestHeartbeatStore(ctrl)
	mockDeliveryStateStore := newTestDeliveryStateStore(ctrl)
	// Allow watermark advancement calls from poll loop
	mockOffsetStore.EXPECT().GetAckedOffset(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(int64(0), nil).AnyTimes()
	mockMessageStore.EXPECT().GetOffsetsAbove(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	return NewSubscriber(zaptest.NewLogger(t).Sugar().Named("subscriber"), tally.NoopScope.SubScope("subscriber"), mockMessageStore, mockOffsetStore, mockLeaseStore, mockHeartbeatStore, mockDeliveryStateStore)
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

			sub := setupSubscriberTest(t, mockMessageStore, mockOffsetStore, mockLeaseStore)
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
			)

			msg := queue.NewMessage("msg-1", []byte("payload"), "part-1", nil)
			d := newSQLDelivery(
				msg, "1", 1, nil,
				sub, "test_topic", "part-1", 100, "msg-1", "test-group",
				extqueue.DLQConfig{},
			)

			if tt.alreadyAcked {
				d.acknowledged = true
			}

			if !tt.alreadyAcked {
				// Ack only calls MarkAcked — watermark is deferred to poll loop
				mockDeliveryState.EXPECT().MarkAcked(
					gomock.Any(), "test-group", "test_topic", "part-1", int64(100),
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
			)

			msg := queue.NewMessage("msg-1", []byte("payload"), "part-1", nil)
			dlqConfig := extqueue.DLQConfig{
				Enabled:     tt.dlqEnabled,
				TopicSuffix: "_dlq",
			}

			d := newSQLDelivery(
				msg, "1", 1, nil,
				sub, "test_topic", "part-1", 100, "msg-1", "test-group",
				dlqConfig,
			)

			if tt.alreadyAcked {
				d.acknowledged = true
			}

			if tt.expectMoveDLQ {
				mockMsgStore.EXPECT().MoveToDLQ(
					gomock.Any(), "test_topic", "part-1", "msg-1", 1, "bad payload", "_dlq",
				).Return(tt.moveToDLQErr)

				if tt.moveToDLQErr == nil {
					mockDeliveryState.EXPECT().MarkAcked(
						gomock.Any(), "test-group", "test_topic", "part-1", int64(100),
					).Return(nil)
				}
			}

			if tt.expectAck {
				mockDeliveryState.EXPECT().MarkAcked(
					gomock.Any(), "test-group", "test_topic", "part-1", int64(100),
				).Return(nil)
			}

			err := d.Reject(context.Background(), "bad payload")

			if tt.expectErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.True(t, d.acknowledged)
		})
	}
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
			mockLeaseStore.EXPECT().GetLeasedPartitions(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]string{}, nil).AnyTimes()

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
			)

			// Allow offset initialization, fetch, and watermark calls from workers
			mockOffsetStore.EXPECT().Initialize(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			mockOffsetStore.EXPECT().GetAckedOffset(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(int64(0), nil).AnyTimes()
			mockMessageStore.EXPECT().FetchByOffset(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
			mockMessageStore.EXPECT().GetOffsetsAbove(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
			mockMessageStore.EXPECT().GarbageCollect(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(int64(0), nil).AnyTimes()
			mockOffsetStore.EXPECT().GetMinAckedOffset(gomock.Any(), gomock.Any(), gomock.Any()).Return(int64(0), false, nil).AnyTimes()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			sub := &subscription{
				topic:      "test_topic",
				config:     testSubscriptionConfig(),
				deliveryCh: make(chan extqueue.Delivery, 100),
				workers:    make(map[string]*partitionWorker),
			}

			// Start initial workers
			s.reconcilePartitionWorkers(ctx, sub, tt.initialLeases)

			sub.workersMu.Lock()
			assert.Equal(t, len(tt.initialLeases), len(sub.workers))
			sub.workersMu.Unlock()

			// Reconcile with updated leases
			s.reconcilePartitionWorkers(ctx, sub, tt.updatedLeases)

			sub.workersMu.Lock()
			assert.Equal(t, len(tt.updatedLeases), len(sub.workers))
			for _, pk := range tt.updatedLeases {
				assert.Contains(t, sub.workers, pk)
			}
			sub.workersMu.Unlock()

			// Cleanup: stop all workers
			cancel()
			s.stopAllWorkers(sub)
		})
	}
}

// TestSubscriber_PartitionWorkerPollAndDeliver verifies a partition worker delivers messages.
func TestSubscriber_PartitionWorkerPollAndDeliver(t *testing.T) {
	ctrl := gomock.NewController(t)

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
	)

	cfg := testSubscriptionConfig()
	deliveryCh := make(chan extqueue.Delivery, 10)
	sub := &subscription{
		topic:      "test_topic",
		config:     cfg,
		deliveryCh: deliveryCh,
		workers:    make(map[string]*partitionWorker),
	}

	ctx := context.Background()

	mockOffsetStore.EXPECT().Initialize(gomock.Any(), "test_topic", "part-1", cfg.ConsumerGroup).Return(nil)
	// GetAckedOffset is called twice: once by pollAndDeliver, once by advanceWatermark
	mockOffsetStore.EXPECT().GetAckedOffset(gomock.Any(), "test_topic", "part-1", cfg.ConsumerGroup).Return(int64(0), nil).Times(2)

	row := messageRow{
		ID:           "msg-1",
		Offset:       1,
		PartitionKey: "part-1",
		Payload:      []byte("payload"),
		PublishedAt:  time.Now().UnixMilli(),
	}
	mockMessageStore.EXPECT().FetchByOffset(gomock.Any(), "test_topic", "part-1", int64(0), gomock.Any(), cfg.BatchSize).
		Return([]messageRow{row}, nil)

	// Delivery state checks — GetDeliveryState returns not-found (new message)
	mockDeliveryState.EXPECT().GetDeliveryState(gomock.Any(), cfg.ConsumerGroup, "test_topic", "part-1", int64(1)).Return(DeliveryState{}, false, nil)
	// MarkDelivered returns retry count 0 (first delivery)
	mockDeliveryState.EXPECT().MarkDelivered(gomock.Any(), cfg.ConsumerGroup, "test_topic", "part-1", int64(1), cfg.VisibilityTimeoutMs).Return(0, nil)

	// advanceWatermark called at end of pollAndDeliver
	mockMessageStore.EXPECT().GetOffsetsAbove(gomock.Any(), "test_topic", "part-1", int64(0), watermarkAdvancementLimit).Return([]int64{1}, nil)
	mockDeliveryState.EXPECT().AdvanceWatermark(gomock.Any(), cfg.ConsumerGroup, "test_topic", "part-1", int64(0), []int64{1}).Return(int64(0), nil)

	w := &partitionWorker{
		partitionKey: "part-1",
		sub:          sub,
		subscriber:   s,
		done:         make(chan struct{}),
	}

	w.pollAndDeliver(ctx)

	// Verify message was delivered
	select {
	case del := <-deliveryCh:
		assert.Equal(t, "msg-1", del.Message().ID)
	default:
		t.Fatal("expected delivery but channel was empty")
	}

	// Verify offset was initialized only once
	assert.True(t, w.offsetInitialized)
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
	)

	// Allow worker polling and watermark advancement
	mockOffsetStore.EXPECT().Initialize(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockOffsetStore.EXPECT().GetAckedOffset(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(int64(0), nil).AnyTimes()
	mockMessageStore.EXPECT().FetchByOffset(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	mockMessageStore.EXPECT().GetOffsetsAbove(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	mockMessageStore.EXPECT().GarbageCollect(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(int64(0), nil).AnyTimes()
	mockOffsetStore.EXPECT().GetMinAckedOffset(gomock.Any(), gomock.Any(), gomock.Any()).Return(int64(0), false, nil).AnyTimes()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub := &subscription{
		topic:      "test_topic",
		config:     testSubscriptionConfig(),
		deliveryCh: make(chan extqueue.Delivery, 100),
		workers:    make(map[string]*partitionWorker),
	}

	// Start 3 workers
	s.startPartitionWorker(ctx, sub, "part-1")
	s.startPartitionWorker(ctx, sub, "part-2")
	s.startPartitionWorker(ctx, sub, "part-3")

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
