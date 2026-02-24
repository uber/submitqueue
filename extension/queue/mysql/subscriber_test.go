package mysql

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"

	"github.com/uber/submitqueue/entity/queue"
	extqueue "github.com/uber/submitqueue/extension/queue"
)

func testSubscriptionConfig() extqueue.SubscriptionConfig {
	return extqueue.DefaultSubscriptionConfig("test-topic", "test-subscriber", "test-consumer")
}

func setupSubscriberTest(t *testing.T, mockMessageStore *MockmessageStore, mockOffsetStore *MockoffsetStore, mockLeaseStore *MockpartitionLeaseStore) extqueue.Subscriber {
	t.Helper()
	return NewSubscriber(zaptest.NewLogger(t).Sugar().Named("subscriber"), tally.NoopScope.SubScope("subscriber"), mockMessageStore, mockOffsetStore, mockLeaseStore)
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

func TestSQLDelivery_Reject(t *testing.T) {
	tests := []struct {
		name           string
		dlqEnabled     bool
		alreadyAcked   bool
		moveToDLQErr   error
		ackMessageErr  error
		expectErr      bool
		expectMoveDLQ  bool
		expectAck      bool
	}{
		{
			name:          "DLQ enabled moves message to DLQ",
			dlqEnabled:    true,
			expectMoveDLQ: true,
		},
		{
			name:      "DLQ disabled falls back to ack",
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
		{
			name:          "DLQ disabled but AckMessage fails",
			ackMessageErr: fmt.Errorf("db error"),
			expectErr:     true,
			expectAck:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockMsgStore := NewMockmessageStore(ctrl)
			mockOffStore := NewMockoffsetStore(ctrl)
			mockLeaseStore := NewMockpartitionLeaseStore(ctrl)

			sub := NewSubscriber(
				zaptest.NewLogger(t).Sugar(),
				tally.NoopScope,
				mockMsgStore,
				mockOffStore,
				mockLeaseStore,
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
					gomock.Any(), "test_topic", "msg-1", 1, "bad payload", "_dlq",
				).Return(tt.moveToDLQErr)

				if tt.moveToDLQErr == nil {
					mockOffStore.EXPECT().UpdateAckedOffset(
						gomock.Any(), "test_topic", "part-1", int64(100), "test-group",
					).Return(nil)
				}
			}

			if tt.expectAck {
				mockOffStore.EXPECT().AckMessage(
					gomock.Any(), "test_topic", "part-1", "msg-1", int64(100), "test-group", mockMsgStore,
				).Return(tt.ackMessageErr)
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
					assert.Nil(t, ch)
				} else {
					require.NoError(t, err)
					assert.NotNil(t, ch)
				}
			}
		})
	}
}
