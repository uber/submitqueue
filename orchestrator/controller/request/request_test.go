package request

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/entity/queue"
	extqueue "github.com/uber/submitqueue/extension/queue"
	"github.com/uber/submitqueue/extension/queue/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

func TestNewController(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope

	controller := NewController(logger, scope)

	require.NotNil(t, controller)
	assert.Equal(t, "request", controller.Topic())
	assert.Equal(t, "orchestrator-request", controller.ConsumerGroup())
	assert.Equal(t, "request", controller.Name())
}

func TestController_Process_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope
	controller := NewController(logger, scope)

	// Create a valid request
	request := entity.Request{
		ID:           "test-queue/123",
		Queue:        "test-queue",
		Change:       entity.Change{Source: "github", IDs: []string{"PR-456"}},
		LandStrategy: entity.RequestLandStrategyRebase,
		State:        entity.RequestStateNew,
		Version:      1,
	}

	// Serialize to bytes
	payload, err := request.ToBytes()
	require.NoError(t, err)

	// Create delivery with mock
	msg := queue.NewMessage("test-queue/123", payload, "test-queue", nil)
	delivery := mock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	// Handle the delivery
	ctx := context.Background()
	err = controller.Process(ctx, delivery)

	// Should return nil (success)
	require.NoError(t, err)
}

func TestController_Process_InvalidJSON(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope
	controller := NewController(logger, scope)

	// Create delivery with invalid JSON
	invalidPayload := []byte(`{"invalid": json"}`)
	msg := queue.NewMessage("invalid-msg", invalidPayload, "partition1", nil)
	delivery := mock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	// Process the delivery
	ctx := context.Background()
	err := controller.Process(ctx, delivery)

	// Should return NonRetryableError for malformed messages
	require.Error(t, err)
	assert.True(t, consumer.IsNonRetryable(err))
}

func TestController_Process_AllRequestStates(t *testing.T) {
	tests := []struct {
		name     string
		state    entity.RequestState
		strategy entity.RequestLandStrategy
	}{
		{"new request", entity.RequestStateNew, entity.RequestLandStrategyRebase},
		{"processing request", entity.RequestStateProcessing, entity.RequestLandStrategySquashRebase},
		{"landed request", entity.RequestStateLanded, entity.RequestLandStrategyMerge},
		{"error request", entity.RequestStateError, entity.RequestLandStrategyRebase},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			logger := zaptest.NewLogger(t).Sugar()
			scope := tally.NoopScope
			controller := NewController(logger, scope)

			request := entity.Request{
				ID:           fmt.Sprintf("queue/%s", tt.state),
				Queue:        "test-queue",
				Change:       entity.Change{Source: "github", IDs: []string{"PR-1"}},
				LandStrategy: tt.strategy,
				State:        tt.state,
				Version:      1,
			}

			payload, err := request.ToBytes()
			require.NoError(t, err)

			msg := queue.NewMessage(request.ID, payload, request.Queue, nil)
			delivery := mock.NewMockDelivery(ctrl)
			delivery.EXPECT().Message().Return(msg).AnyTimes()
			delivery.EXPECT().Attempt().Return(1).AnyTimes()

			ctx := context.Background()
			err = controller.Process(ctx, delivery)

			require.NoError(t, err)
		})
	}
}

func TestController_Process_MultipleChanges(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope
	controller := NewController(logger, scope)

	request := entity.Request{
		ID:    "queue/999",
		Queue: "test-queue",
		Change: entity.Change{
			Source: "github",
			IDs:    []string{"PR-1", "PR-2", "PR-3"}, // Multiple PRs
		},
		LandStrategy: entity.RequestLandStrategySquashRebase,
		State:        entity.RequestStateNew,
		Version:      1,
	}

	payload, err := request.ToBytes()
	require.NoError(t, err)

	msg := queue.NewMessage(request.ID, payload, request.Queue, nil)
	delivery := mock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	ctx := context.Background()
	err = controller.Process(ctx, delivery)

	require.NoError(t, err)
}

func TestController_SubscriptionConfig(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope
	controller := NewController(logger, scope)

	config := controller.SubscriptionConfig("test-worker-123")

	assert.Equal(t, "test-worker-123", config.SubscriberName)
	assert.Equal(t, "orchestrator-request", config.ConsumerGroup)
	assert.Equal(t, int64(100), config.PollIntervalMs) // 100ms
	assert.Equal(t, 10, config.BatchSize)
	assert.Equal(t, int64(60000), config.VisibilityTimeoutMs) // 60s
	assert.Equal(t, 3, config.Retry.MaxAttempts)
	assert.True(t, config.DLQ.Enabled)
}

func TestController_InterfaceImplementation(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope
	controller := NewController(logger, scope)

	// Verify implements consumer.Controller interface
	var _ interface {
		Process(ctx context.Context, delivery consumer.Delivery) error
		Name() string
		Topic() string
		ConsumerGroup() string
		SubscriptionConfig(subscriberName string) extqueue.SubscriptionConfig
	} = controller
}
