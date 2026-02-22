package sql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"

	extqueue "github.com/uber/submitqueue/extension/queue"
)

func testSubscriptionConfig() extqueue.SubscriptionConfig {
	return extqueue.DefaultSubscriptionConfig("test-subscriber", "test-consumer")
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
