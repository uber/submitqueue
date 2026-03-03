package buildsignal

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/core/errs"
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

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: consumer.TopicKeySpeculate, Name: "speculate", Queue: mockQ}},
	)
	require.NoError(t, err)

	return NewController(logger, scope, registry, consumer.TopicKeyBuildSignal, "orchestrator-buildsignal")
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller := newTestController(t, ctrl, nil)

	require.NotNil(t, controller)
	assert.Equal(t, consumer.TopicKeyBuildSignal, controller.TopicKey())
	assert.Equal(t, "orchestrator-buildsignal", controller.ConsumerGroup())
	assert.Equal(t, "buildsignal", controller.Name())
}

func TestController_Process_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	controller := newTestController(t, ctrl, nil)

	build := entity.Build{
		ID:      "build-123",
		BatchID: "test-queue/batch/1",
		Status:  entity.BuildStatusQueued,
	}

	payload, err := build.ToBytes()
	require.NoError(t, err)

	msg := queue.NewMessage("build-123", payload, "test-queue/batch/1", nil)
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
	assert.False(t, errs.IsRetryable(err))
}

func TestController_Process_PublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	controller := newTestController(t, ctrl, fmt.Errorf("publish failed"))

	build := entity.Build{
		ID:      "build-456",
		BatchID: "test-queue/batch/2",
		Status:  entity.BuildStatusRunning,
	}

	payload, err := build.ToBytes()
	require.NoError(t, err)

	msg := queue.NewMessage(build.ID, payload, build.BatchID, nil)
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
