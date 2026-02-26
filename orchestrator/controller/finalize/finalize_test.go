package finalize

import (
	"context"
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
func newTestController(t *testing.T) *Controller {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope

	registry, err := consumer.NewTopicRegistry(nil)
	require.NoError(t, err)

	return NewController(logger, scope, registry, consumer.TopicKeyFinalize, "orchestrator-finalize")
}

func TestNewController(t *testing.T) {
	controller := newTestController(t)

	require.NotNil(t, controller)
	assert.Equal(t, consumer.TopicKeyFinalize, controller.TopicKey())
	assert.Equal(t, "orchestrator-finalize", controller.ConsumerGroup())
	assert.Equal(t, "finalize", controller.Name())
}

func TestController_Process_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	controller := newTestController(t)

	request := entity.Request{
		ID:           "test-queue/123",
		Queue:        "test-queue",
		Change:       entity.Change{URIs: []string{"github://uber/service/pull/456/abc123def"}},
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

	controller := newTestController(t)

	invalidPayload := []byte(`{"invalid": json"}`)
	msg := queue.NewMessage("invalid-msg", invalidPayload, "partition1", nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)

	require.Error(t, err)
	assert.True(t, consumer.IsNonRetryable(err))
}

func TestController_InterfaceImplementation(t *testing.T) {
	controller := newTestController(t)

	var _ consumer.Controller = controller
}
