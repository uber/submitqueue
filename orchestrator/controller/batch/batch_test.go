package batch

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
	queuemock "github.com/uber/submitqueue/extension/queue/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

// newTestController creates a controller with test dependencies.
func newTestController(t *testing.T, ctrl *gomock.Controller, publishErr error) *Controller {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope

	mockPub := queuemock.NewMockPublisher(ctrl)
	mockPub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, topic string, msg queue.Message) error {
			return publishErr
		},
	).AnyTimes()

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()

	registry := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Topic: consumer.TopicBatched, Queue: mockQ}},
		nil,
	)

	return NewController(logger, scope, registry, consumer.TopicToBatch, "orchestrator-batch")
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller := newTestController(t, ctrl, nil)

	require.NotNil(t, controller)
	assert.Equal(t, consumer.TopicToBatch, controller.Topic())
	assert.Equal(t, "orchestrator-batch", controller.ConsumerGroup())
	assert.Equal(t, "batch", controller.Name())
}

func TestController_Process_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	controller := newTestController(t, ctrl, nil)

	request := entity.Request{
		ID:           "test-queue/123",
		Queue:        "test-queue",
		Change:       entity.Change{URI: "github://uber/service/pull/456/abc123def"},
		LandStrategy: entity.RequestLandStrategyRebase,
		State:        entity.RequestStateNew,
		Version:      1,
	}

	payload, err := request.ToBytes()
	require.NoError(t, err)

	msg := queue.NewMessage("test-queue/123", payload, "test-queue", nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err = controller.Process(context.Background(), delivery)
	require.NoError(t, err)
}

func TestController_Process_InvalidJSON(t *testing.T) {
	ctrl := gomock.NewController(t)

	controller := newTestController(t, ctrl, nil)

	invalidPayload := []byte(`{"invalid": json"}`)
	msg := queue.NewMessage("invalid-msg", invalidPayload, "partition1", nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)

	require.Error(t, err)
	assert.True(t, consumer.IsNonRetryable(err))
}

func TestController_Process_PublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	controller := newTestController(t, ctrl, fmt.Errorf("publish failed"))

	request := entity.Request{
		ID:           "test-queue/123",
		Queue:        "test-queue",
		Change:       entity.Change{URI: "github://uber/service/pull/1/xyz789abc"},
		LandStrategy: entity.RequestLandStrategyRebase,
		State:        entity.RequestStateNew,
		Version:      1,
	}

	payload, err := request.ToBytes()
	require.NoError(t, err)

	msg := queue.NewMessage(request.ID, payload, request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err = controller.Process(context.Background(), delivery)
	assert.Error(t, err)
}

func TestController_InterfaceImplementation(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller := newTestController(t, ctrl, nil)

	var _ consumer.Controller = controller
}
