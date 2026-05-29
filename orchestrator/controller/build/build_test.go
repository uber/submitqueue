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

package build

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
	"github.com/uber/submitqueue/extension/build"
	buildmock "github.com/uber/submitqueue/extension/build/mock"
	buildnoop "github.com/uber/submitqueue/extension/build/noop"
	queuemock "github.com/uber/submitqueue/extension/queue/mock"
	storagemock "github.com/uber/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

// batchIDPayload serializes a BatchID to JSON bytes for test message payloads.
func batchIDPayload(t *testing.T, id string) []byte {
	payload, err := entity.BatchID{ID: id}.ToBytes()
	require.NoError(t, err)
	return payload
}

// testBatch returns a standard test batch for build tests.
func testBatch() entity.Batch {
	return entity.Batch{
		ID:      "test-queue/batch/1",
		Queue:   "test-queue",
		State:   entity.BatchStateCreated,
		Version: 1,
	}
}

// newMockStorage creates a MockStorage with a MockBatchStore that returns the given batch on Get.
func newMockStorage(ctrl *gomock.Controller, batch entity.Batch) *storagemock.MockStorage {
	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).AnyTimes()

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	return store
}

// newTestController creates a controller with test dependencies. bm is the
// build manager to inject; pass buildnoop.New() for the pass-through default.
func newTestController(t *testing.T, ctrl *gomock.Controller, store *storagemock.MockStorage, bm build.BuildManager, publishErr error) *Controller {
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
		[]consumer.TopicConfig{{Key: consumer.TopicKeyBuildSignal, Name: "buildsignal", Queue: mockQ}},
	)
	require.NoError(t, err)

	return NewController(logger, scope, store, bm, registry, consumer.TopicKeyBuild, "orchestrator-build")
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	batch := testBatch()
	store := newMockStorage(ctrl, batch)
	controller := newTestController(t, ctrl, store, buildnoop.New(), nil)

	require.NotNil(t, controller)
	assert.Equal(t, consumer.TopicKeyBuild, controller.TopicKey())
	assert.Equal(t, "orchestrator-build", controller.ConsumerGroup())
	assert.Equal(t, "build", controller.Name())
}

func TestController_Process_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	store := newMockStorage(ctrl, batch)
	controller := newTestController(t, ctrl, store, buildnoop.New(), nil)

	msg := queue.NewMessage(batch.ID, batchIDPayload(t, batch.ID), batch.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	require.NoError(t, err)
}

// TestController_Process_TriggersBuildWithChanges verifies the controller
// assembles one BuildChange per request in the batch, triggers the build, and
// publishes a Build carrying the manager's returned ID and status.
func TestController_Process_TriggersBuildWithChanges(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := entity.Batch{
		ID:       "test-queue/batch/1",
		Queue:    "test-queue",
		State:    entity.BatchStateCreated,
		Version:  1,
		Contains: []string{"test-queue/1", "test-queue/2"},
	}
	req1 := entity.Request{ID: "test-queue/1", Change: entity.Change{URIs: []string{"github://o/r/pull/1/aaa"}}}
	req2 := entity.Request{ID: "test-queue/2", Change: entity.Change{URIs: []string{"github://o/r/pull/2/bbb"}}}

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	mockRequestStore := storagemock.NewMockRequestStore(ctrl)
	mockRequestStore.EXPECT().Get(gomock.Any(), req1.ID).Return(req1, nil)
	mockRequestStore.EXPECT().Get(gomock.Any(), req2.ID).Return(req2, nil)
	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(mockRequestStore).AnyTimes()

	bm := buildmock.NewMockBuildManager(ctrl)
	wantChanges := []entity.BuildChange{
		{Change: req1.Change, Action: entity.ChangeActionValidate},
		{Change: req2.Change, Action: entity.ChangeActionValidate},
	}
	bm.EXPECT().Trigger(gomock.Any(), batch.Queue, wantChanges).Return("build-xyz", entity.BuildStatusRunning, nil)

	var published entity.Build
	mockPub := queuemock.NewMockPublisher(ctrl)
	mockPub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, msg queue.Message) error {
			b, err := entity.BuildFromBytes(msg.Payload)
			require.NoError(t, err)
			published = b
			return nil
		},
	)
	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()
	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: consumer.TopicKeyBuildSignal, Name: "buildsignal", Queue: mockQ}},
	)
	require.NoError(t, err)

	controller := NewController(zaptest.NewLogger(t).Sugar(), tally.NoopScope, store, bm, registry, consumer.TopicKeyBuild, "orchestrator-build")

	msg := queue.NewMessage(batch.ID, batchIDPayload(t, batch.ID), batch.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	require.NoError(t, controller.Process(context.Background(), delivery))
	assert.Equal(t, "build-xyz", published.ID)
	assert.Equal(t, batch.ID, published.BatchID)
	assert.Equal(t, entity.BuildStatusRunning, published.Status)
}

// TestController_Process_TriggerFailure verifies a build-manager failure is
// surfaced as an error (nack) and nothing is published.
func TestController_Process_TriggerFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()

	bm := buildmock.NewMockBuildManager(ctrl)
	bm.EXPECT().Trigger(gomock.Any(), batch.Queue, gomock.Any()).
		Return("", entity.BuildStatusUnknown, fmt.Errorf("provider down"))

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: consumer.TopicKeyBuildSignal, Name: "buildsignal", Queue: queuemock.NewMockQueue(ctrl)}},
	)
	require.NoError(t, err)
	controller := NewController(zaptest.NewLogger(t).Sugar(), tally.NoopScope, store, bm, registry, consumer.TopicKeyBuild, "orchestrator-build")

	msg := queue.NewMessage(batch.ID, batchIDPayload(t, batch.ID), batch.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	require.Error(t, controller.Process(context.Background(), delivery))
}

func TestController_Process_StorageFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1").Return(entity.Batch{}, fmt.Errorf("db connection lost"))
	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()

	controller := newTestController(t, ctrl, store, buildnoop.New(), nil)

	msg := queue.NewMessage("test-queue/batch/1", batchIDPayload(t, "test-queue/batch/1"), "test-queue", nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}

func TestController_Process_PublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	store := newMockStorage(ctrl, batch)
	controller := newTestController(t, ctrl, store, buildnoop.New(), fmt.Errorf("publish failed"))

	msg := queue.NewMessage(batch.ID, batchIDPayload(t, batch.ID), batch.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	assert.Error(t, err)
}

func TestController_InterfaceImplementation(t *testing.T) {
	ctrl := gomock.NewController(t)
	batch := testBatch()
	store := newMockStorage(ctrl, batch)
	controller := newTestController(t, ctrl, store, buildnoop.New(), nil)

	var _ consumer.Controller = controller
}
