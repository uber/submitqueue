package speculate

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	speculationmock "github.com/uber/submitqueue/core/speculation/mock"
	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/entity/queue"
	queuemock "github.com/uber/submitqueue/extension/queue/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

// newTestController creates a controller with test dependencies.
func newTestController(t *testing.T, ctrl *gomock.Controller, strategy *speculationmock.MockStrategy, publishErr error) *Controller {
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
		[]consumer.TopicConfig{
			{Key: consumer.TopicKeyBuild, Name: "build", Queue: mockQ},
			{Key: consumer.TopicKeyToMerge, Name: "to-merge", Queue: mockQ},
		},
	)
	require.NoError(t, err)

	return NewController(logger, scope, registry, strategy, consumer.TopicKeyBatched, "orchestrator-speculate")
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	strategy := speculationmock.NewMockStrategy(ctrl)
	controller := newTestController(t, ctrl, strategy, nil)

	require.NotNil(t, controller)
	assert.Equal(t, consumer.TopicKeyBatched, controller.TopicKey())
	assert.Equal(t, "orchestrator-speculate", controller.ConsumerGroup())
	assert.Equal(t, "speculate", controller.Name())
}

func TestController_Process_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	strategy := speculationmock.NewMockStrategy(ctrl)

	strategy.EXPECT().Generate(gomock.Any(), "test-queue/batch/1", []string{}).Return(
		entity.SpeculationTree{
			BatchID: "test-queue/batch/1",
			Speculations: []entity.SpeculationInfo{
				{
					Path:   entity.SpeculationPath{Base: []string{}, Head: "test-queue/batch/1"},
					Action: entity.SpeculationPathActionSchedule,
					Score:  1.0,
				},
			},
		}, nil,
	)

	controller := newTestController(t, ctrl, strategy, nil)

	batch := entity.Batch{
		ID:       "test-queue/batch/1",
		Queue:    "test-queue",
		Contains: []string{"test-queue/123"},
		State:    entity.BatchStateCreated,
		Version:  1,
	}

	payload, err := batch.ToBytes()
	require.NoError(t, err)

	msg := queue.NewMessage(batch.ID, payload, batch.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err = controller.Process(context.Background(), delivery)
	require.NoError(t, err)
}

func TestController_Process_WithDependencies(t *testing.T) {
	ctrl := gomock.NewController(t)
	strategy := speculationmock.NewMockStrategy(ctrl)

	strategy.EXPECT().Generate(gomock.Any(), "test-queue/batch/1", []string{"test-queue/batch/0"}).Return(
		entity.SpeculationTree{
			BatchID: "test-queue/batch/1",
			Speculations: []entity.SpeculationInfo{
				{
					Path:   entity.SpeculationPath{Base: []string{"test-queue/batch/0"}, Head: "test-queue/batch/1"},
					Action: entity.SpeculationPathActionSchedule,
					Score:  0.9,
				},
				{
					Path:   entity.SpeculationPath{Base: []string{}, Head: "test-queue/batch/1"},
					Action: entity.SpeculationPathActionSchedule,
					Score:  0.1,
				},
			},
		}, nil,
	)

	controller := newTestController(t, ctrl, strategy, nil)

	batch := entity.Batch{
		ID:       "test-queue/batch/1",
		Queue:    "test-queue",
		Contains: []string{"test-queue/123"},
		Dependencies: []map[string]interface{}{
			{"ID": "test-queue/batch/0"},
		},
		State:   entity.BatchStateCreated,
		Version: 1,
	}

	payload, err := batch.ToBytes()
	require.NoError(t, err)

	msg := queue.NewMessage(batch.ID, payload, batch.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err = controller.Process(context.Background(), delivery)
	require.NoError(t, err)
}

func TestController_Process_StrategyFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	strategy := speculationmock.NewMockStrategy(ctrl)

	strategy.EXPECT().Generate(gomock.Any(), gomock.Any(), gomock.Any()).Return(
		entity.SpeculationTree{}, fmt.Errorf("strategy failed"),
	)

	controller := newTestController(t, ctrl, strategy, nil)

	batch := entity.Batch{
		ID:       "test-queue/batch/1",
		Queue:    "test-queue",
		Contains: []string{"test-queue/123"},
		Dependencies: []map[string]interface{}{
			{"ID": "test-queue/batch/0"},
		},
		State:   entity.BatchStateCreated,
		Version: 1,
	}

	payload, err := batch.ToBytes()
	require.NoError(t, err)

	msg := queue.NewMessage(batch.ID, payload, batch.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err = controller.Process(context.Background(), delivery)
	require.Error(t, err)
	assert.True(t, consumer.IsNonRetryable(err))
}

func TestController_Process_InvalidJSON(t *testing.T) {
	ctrl := gomock.NewController(t)
	strategy := speculationmock.NewMockStrategy(ctrl)

	controller := newTestController(t, ctrl, strategy, nil)

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
	strategy := speculationmock.NewMockStrategy(ctrl)

	strategy.EXPECT().Generate(gomock.Any(), gomock.Any(), gomock.Any()).Return(
		entity.SpeculationTree{
			BatchID: "test-queue/batch/1",
			Speculations: []entity.SpeculationInfo{
				{
					Path:   entity.SpeculationPath{Base: []string{}, Head: "test-queue/batch/1"},
					Action: entity.SpeculationPathActionSchedule,
					Score:  1.0,
				},
			},
		}, nil,
	)

	controller := newTestController(t, ctrl, strategy, fmt.Errorf("publish failed"))

	batch := entity.Batch{
		ID:       "test-queue/batch/1",
		Queue:    "test-queue",
		Contains: []string{"test-queue/123"},
		State:    entity.BatchStateCreated,
		Version:  1,
	}

	payload, err := batch.ToBytes()
	require.NoError(t, err)

	msg := queue.NewMessage(batch.ID, payload, batch.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err = controller.Process(context.Background(), delivery)
	assert.Error(t, err)
}

func TestController_InterfaceImplementation(t *testing.T) {
	ctrl := gomock.NewController(t)
	strategy := speculationmock.NewMockStrategy(ctrl)
	controller := newTestController(t, ctrl, strategy, nil)

	var _ consumer.Controller = controller
}
