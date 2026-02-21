package sql

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"

	"github.com/uber/submitqueue/entity/queue"
	extqueue "github.com/uber/submitqueue/extension/queue"
	// mocks in same package
)

const fixedTimestamp = int64(1234567890000) // Fixed timestamp for test repeatability

func setupPublisherTest(t *testing.T, mockStore *MockmessageStore) extqueue.Publisher {
	t.Helper()

	config := DefaultConfig("test-consumer", "test-worker")

	return NewPublisher(config,
		zaptest.NewLogger(t).Sugar().Named("publisher"),
		tally.NoopScope.SubScope("publisher"),
		mockStore,
	)
}

func TestPublisher_Publish(t *testing.T) {
	tests := []struct {
		name      string
		topic     string
		messages  []queue.Message
		wantErr   bool
		setupMock func(*MockmessageStore)
	}{
		{
			name:  "publish single message",
			topic: "test_topic",
			messages: []queue.Message{
				{ID: "msg1", Payload: []byte("payload1"), PartitionKey: "part1", PublishedAt: fixedTimestamp},
			},
			wantErr: false,
			setupMock: func(m *MockmessageStore) {
				m.EXPECT().Insert(gomock.Any(), "test_topic", gomock.Any()).Return(nil).Times(1)
			},
		},
		{
			name:  "publish multiple messages",
			topic: "multi_topic",
			messages: []queue.Message{
				{ID: "msg1", Payload: []byte("p1"), PartitionKey: "part1", PublishedAt: fixedTimestamp},
				{ID: "msg2", Payload: []byte("p2"), PartitionKey: "part1", PublishedAt: fixedTimestamp},
				{ID: "msg3", Payload: []byte("p3"), PartitionKey: "part2", PublishedAt: fixedTimestamp},
			},
			wantErr: false,
			setupMock: func(m *MockmessageStore) {
				m.EXPECT().Insert(gomock.Any(), "multi_topic", gomock.Any()).Return(nil).Times(3)
			},
		},
		{
			name:     "publish empty messages is no-op",
			topic:    "empty_topic",
			messages: []queue.Message{},
			wantErr:  false,
			setupMock: func(m *MockmessageStore) {
				// No Insert expected
			},
		},
		{
			name:  "publish with metadata",
			topic: "metadata_topic",
			messages: []queue.Message{
				{
					ID:           "msg_meta",
					Payload:      []byte("payload"),
					PartitionKey: "part1",
					Metadata:     map[string]string{"key1": "val1", "key2": "val2"},
					PublishedAt:  fixedTimestamp,
				},
			},
			wantErr: false,
			setupMock: func(m *MockmessageStore) {
				m.EXPECT().Insert(gomock.Any(), "metadata_topic", gomock.Any()).Return(nil).Times(1)
			},
		},
		{
			name:  "publish with invalid topic name - uppercase",
			topic: "InvalidTopic",
			messages: []queue.Message{
				{ID: "msg1", Payload: []byte("p"), PartitionKey: "part1", PublishedAt: fixedTimestamp},
			},
			wantErr: true,
			setupMock: func(m *MockmessageStore) {
				// No Insert expected since validation fails
			},
		},
		{
			name:  "publish with invalid topic name - special chars",
			topic: "topic-with-dash",
			messages: []queue.Message{
				{ID: "msg1", Payload: []byte("p"), PartitionKey: "part1", PublishedAt: fixedTimestamp},
			},
			wantErr: true,
			setupMock: func(m *MockmessageStore) {
				// No Insert expected since validation fails
			},
		},
		{
			name:  "publish with invalid topic name - empty",
			topic: "",
			messages: []queue.Message{
				{ID: "msg1", Payload: []byte("p"), PartitionKey: "part1", PublishedAt: fixedTimestamp},
			},
			wantErr: true,
			setupMock: func(m *MockmessageStore) {
				// No Insert expected since validation fails
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockStore := NewMockmessageStore(ctrl)
			tt.setupMock(mockStore)

			pub := setupPublisherTest(t, mockStore)

			ctx := context.Background()
			var err error
			for _, msg := range tt.messages {
				err = pub.Publish(ctx, tt.topic, msg)
				if err != nil {
					break
				}
			}
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestPublisher_PublishAfterClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockmessageStore(ctrl)
	pub := setupPublisherTest(t, mockStore)

	ctx := context.Background()

	// Close the publisher
	err := pub.Close()
	require.NoError(t, err)

	// Try to publish after close
	msg := queue.NewMessage("msg1", []byte("payload"), "part1", nil)
	err = pub.Publish(ctx, "test_topic", msg)
	require.Error(t, err)
}

func TestPublisher_Close(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockmessageStore(ctrl)
	pub := setupPublisherTest(t, mockStore)

	// Close should succeed
	err := pub.Close()
	require.NoError(t, err)

	// Closing again should still succeed (idempotent)
	err = pub.Close()
	require.NoError(t, err)
}

func TestValidateTopicName(t *testing.T) {
	tests := []struct {
		name      string
		topicName string
		wantErr   bool
	}{
		{
			name:      "valid topic - lowercase letters",
			topicName: "mytopic",
			wantErr:   false,
		},
		{
			name:      "valid topic - with numbers",
			topicName: "topic123",
			wantErr:   false,
		},
		{
			name:      "valid topic - with underscores",
			topicName: "my_topic_name",
			wantErr:   false,
		},
		{
			name:      "valid topic - all valid chars",
			topicName: "abc_123_xyz",
			wantErr:   false,
		},
		{
			name:      "invalid topic - empty",
			topicName: "",
			wantErr:   true,
		},
		{
			name:      "invalid topic - uppercase",
			topicName: "MyTopic",
			wantErr:   true,
		},
		{
			name:      "invalid topic - dash",
			topicName: "my-topic",
			wantErr:   true,
		},
		{
			name:      "invalid topic - dot",
			topicName: "my.topic",
			wantErr:   true,
		},
		{
			name:      "invalid topic - space",
			topicName: "my topic",
			wantErr:   true,
		},
		{
			name:      "invalid topic - special chars",
			topicName: "topic!@#",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockStore := NewMockmessageStore(ctrl)
			pub := setupPublisherTest(t, mockStore)

			// Try to publish with this topic name
			ctx := context.Background()
			msg := queue.NewMessage("msg1", []byte("test"), "part1", nil)

			if !tt.wantErr {
				mockStore.EXPECT().Insert(gomock.Any(), tt.topicName, gomock.Any()).Return(nil).Times(1)
			}

			err := pub.Publish(ctx, tt.topicName, msg)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestPublisher_PublishMetrics(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockmessageStore(ctrl)
	mockStore.EXPECT().Insert(gomock.Any(), "metrics_test", gomock.Any()).Return(nil).Times(2)

	pub := setupPublisherTest(t, mockStore)

	ctx := context.Background()
	topic := "metrics_test"

	// Publish some messages
	messages := []queue.Message{
		{ID: "msg1", Payload: []byte("p1"), PartitionKey: "part1", PublishedAt: fixedTimestamp},
		{ID: "msg2", Payload: []byte("p2"), PartitionKey: "part1", PublishedAt: fixedTimestamp},
	}

	for _, msg := range messages {
		err := pub.Publish(ctx, topic, msg)
		require.NoError(t, err)
	}

	// Metrics should have been recorded (we're using NoopScope in tests, so just verify no errors)
	// In a real implementation, you'd use a mock metrics scope to verify calls
}

func TestPublisher_ConcurrentPublish(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const numGoroutines = 10
	const messagesPerGoroutine = 5

	mockStore := NewMockmessageStore(ctrl)
	mockStore.EXPECT().Insert(gomock.Any(), "concurrent_topic", gomock.Any()).Return(nil).Times(numGoroutines * messagesPerGoroutine)

	pub := setupPublisherTest(t, mockStore)

	ctx := context.Background()
	topic := "concurrent_topic"

	// Publish from multiple goroutines
	errCh := make(chan error, numGoroutines*messagesPerGoroutine)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			for j := 0; j < messagesPerGoroutine; j++ {
				msg := queue.Message{
					ID:           fmt.Sprintf("msg_%d_%d", id, j),
					Payload:      []byte(fmt.Sprintf("payload_%d_%d", id, j)),
					PartitionKey: fmt.Sprintf("part_%d", id),
					PublishedAt:  fixedTimestamp,
				}
				errCh <- pub.Publish(ctx, topic, msg)
			}
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines*messagesPerGoroutine; i++ {
		err := <-errCh
		require.NoError(t, err)
	}
}

func TestPublisher_PublishContextCancellation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockmessageStore(ctrl)
	mockStore.EXPECT().Insert(gomock.Any(), "test_topic", gomock.Any()).Return(context.Canceled).Times(1)

	pub := setupPublisherTest(t, mockStore)

	// Create a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	msg := queue.NewMessage("msg1", []byte("payload"), "part1", nil)

	// Should fail with context cancelled error
	err := pub.Publish(ctx, "test_topic", msg)
	require.Error(t, err)
}
