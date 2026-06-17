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
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	changesetfake "github.com/uber/submitqueue/submitqueue/core/changeset/fake"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/buildrunner"
	buildfake "github.com/uber/submitqueue/submitqueue/extension/buildrunner/fake"
	buildrunnermock "github.com/uber/submitqueue/submitqueue/extension/buildrunner/mock"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
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

// newMockStorage creates a MockStorage with a MockBatchStore that returns the
// given batch on Get, a no-op MockRequestStore, and a MockBuildStore that
// accepts any Create call. Tests that care about Create arguments build their
// own MockBuildStore.
func newMockStorage(ctrl *gomock.Controller, batch entity.Batch) *storagemock.MockStorage {
	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).AnyTimes()

	mockRequestStore := storagemock.NewMockRequestStore(ctrl)

	mockBuildStore := storagemock.NewMockBuildStore(ctrl)
	mockBuildStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(mockRequestStore).AnyTimes()
	store.EXPECT().GetBuildStore().Return(mockBuildStore).AnyTimes()
	return store
}

// newTestController creates a controller with test dependencies. br is the
// build runner to inject; pass buildfake.New(changesetfake.New()) for the pass-through default.
// staticBuildRunnerFactory is a test factory that returns a fixed BuildRunner
// for any entityqueue.
type staticBuildRunnerFactory struct{ r buildrunner.BuildRunner }

func (f staticBuildRunnerFactory) For(buildrunner.Config) (buildrunner.BuildRunner, error) {
	return f.r, nil
}

// The wired registry exposes only the buildsignal topic — that is what the
// controller publishes to after the RFC refactor.
func newTestController(t *testing.T, ctrl *gomock.Controller, store *storagemock.MockStorage, br buildrunner.BuildRunner, publishErr error) *Controller {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope

	mockPub := queuemock.NewMockPublisher(ctrl)
	mockPub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, topic string, msg entityqueue.Message) error {
			return publishErr
		},
	).AnyTimes()

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: topickey.TopicKeyBuildSignal, Name: "buildsignal", Queue: mockQ}},
	)
	require.NoError(t, err)

	return NewController(logger, scope, store, staticBuildRunnerFactory{r: br}, registry, topickey.TopicKeyBuild, "orchestrator-build")
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	batch := testBatch()
	store := newMockStorage(ctrl, batch)
	controller := newTestController(t, ctrl, store, buildfake.New(changesetfake.New()), nil)

	require.NotNil(t, controller)
	assert.Equal(t, topickey.TopicKeyBuild, controller.TopicKey())
	assert.Equal(t, "orchestrator-build", controller.ConsumerGroup())
	assert.Equal(t, "build", controller.Name())
}

func TestController_Process_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	store := newMockStorage(ctrl, batch)
	controller := newTestController(t, ctrl, store, buildfake.New(changesetfake.New()), nil)

	msg := entityqueue.NewMessage(batch.ID, batchIDPayload(t, batch.ID), batch.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	require.NoError(t, err)
}

// TestController_Process_TriggersWithBaseAndHead verifies the controller hands
// BuildRunner.Trigger the base (dependency batches in order) and head (this
// batch) as identity, persists the initial Accepted Build, and publishes it to
// the buildsignal topic. The runner resolves each batch's changes itself.
func TestController_Process_TriggersWithBaseAndHead(t *testing.T) {
	ctrl := gomock.NewController(t)

	depBatch := entity.Batch{
		ID:       "test-queue/batch/dep",
		Queue:    "test-queue",
		Contains: []string{"test-queue/dep-1"},
	}
	headBatch := entity.Batch{
		ID:           "test-queue/batch/head",
		Queue:        "test-queue",
		State:        entity.BatchStateSpeculating,
		Version:      1,
		Dependencies: []string{depBatch.ID},
		Contains:     []string{"test-queue/head-1", "test-queue/head-2"},
	}

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Get(gomock.Any(), headBatch.ID).Return(headBatch, nil).AnyTimes()
	mockBatchStore.EXPECT().Get(gomock.Any(), depBatch.ID).Return(depBatch, nil).AnyTimes()

	var created entity.Build
	mockBuildStore := storagemock.NewMockBuildStore(ctrl)
	mockBuildStore.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, b entity.Build) error {
			created = b
			return nil
		},
	).Times(1)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	store.EXPECT().GetBuildStore().Return(mockBuildStore).AnyTimes()

	br := buildrunnermock.NewMockBuildRunner(ctrl)
	// base is the dependency batches (identity); head is this batch.
	br.EXPECT().Trigger(gomock.Any(), []entity.Batch{depBatch}, headBatch, gomock.Nil()).Return(entity.BuildID{ID: "build-xyz"}, nil)

	var publishedTopic string
	var published entity.BuildID
	mockPub := queuemock.NewMockPublisher(ctrl)
	mockPub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, topic string, msg entityqueue.Message) error {
			publishedTopic = topic
			bid, err := entity.BuildIDFromBytes(msg.Payload)
			require.NoError(t, err)
			published = bid
			return nil
		},
	)
	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()
	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: topickey.TopicKeyBuildSignal, Name: "buildsignal", Queue: mockQ}},
	)
	require.NoError(t, err)

	controller := NewController(zaptest.NewLogger(t).Sugar(), tally.NoopScope, store, staticBuildRunnerFactory{r: br}, registry, topickey.TopicKeyBuild, "orchestrator-build")

	msg := entityqueue.NewMessage(headBatch.ID, batchIDPayload(t, headBatch.ID), headBatch.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	require.NoError(t, controller.Process(context.Background(), delivery))

	// Only the build ID is published to buildsignal.
	assert.Equal(t, "buildsignal", publishedTopic)
	assert.Equal(t, "build-xyz", published.ID)

	// The full Build is persisted to storage (the source of truth the poll
	// loop reloads), and its ID matches what was published.
	assert.Equal(t, "build-xyz", created.ID)
	assert.Equal(t, headBatch.ID, created.BatchID)
	assert.Equal(t, entity.BuildStatusAccepted, created.Status)
	assert.Equal(t, []string{depBatch.ID}, created.SpeculationPath.Base)
	assert.Equal(t, published.ID, created.ID)
}

// TestController_Process_BuildStoreAlreadyExistsIsSwallowed covers the
// redelivery case: Create returns ErrAlreadyExists, the controller proceeds
// to publish to buildsignal anyway. The polling loop will pick up the
// existing row via UpdateStatus.
func TestController_Process_BuildStoreAlreadyExistsIsSwallowed(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).AnyTimes()
	mockBuildStore := storagemock.NewMockBuildStore(ctrl)
	mockBuildStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(storage.ErrAlreadyExists)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(storagemock.NewMockRequestStore(ctrl)).AnyTimes()
	store.EXPECT().GetBuildStore().Return(mockBuildStore).AnyTimes()

	br := buildrunnermock.NewMockBuildRunner(ctrl)
	br.EXPECT().Trigger(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(entity.BuildID{ID: "build-dup"}, nil)

	publishCalled := false
	mockPub := queuemock.NewMockPublisher(ctrl)
	mockPub.EXPECT().Publish(gomock.Any(), "buildsignal", gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ entityqueue.Message) error {
			publishCalled = true
			return nil
		},
	).Times(1)
	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()
	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: topickey.TopicKeyBuildSignal, Name: "buildsignal", Queue: mockQ}},
	)
	require.NoError(t, err)
	controller := NewController(zaptest.NewLogger(t).Sugar(), tally.NoopScope, store, staticBuildRunnerFactory{r: br}, registry, topickey.TopicKeyBuild, "orchestrator-build")

	msg := entityqueue.NewMessage(batch.ID, batchIDPayload(t, batch.ID), batch.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	require.NoError(t, controller.Process(context.Background(), delivery))
	assert.True(t, publishCalled, "publish to buildsignal must run even when Create reports ErrAlreadyExists")
}

// TestController_Process_TriggerFailure verifies a build-runner failure is
// surfaced as an error (nack) and nothing is persisted or published.
func TestController_Process_TriggerFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).AnyTimes()
	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(storagemock.NewMockRequestStore(ctrl)).AnyTimes()
	// No build store expectation: Trigger failure must short-circuit before Create.

	br := buildrunnermock.NewMockBuildRunner(ctrl)
	br.EXPECT().Trigger(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(entity.BuildID{}, fmt.Errorf("provider down"))

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: topickey.TopicKeyBuildSignal, Name: "buildsignal", Queue: queuemock.NewMockQueue(ctrl)}},
	)
	require.NoError(t, err)
	controller := NewController(zaptest.NewLogger(t).Sugar(), tally.NoopScope, store, staticBuildRunnerFactory{r: br}, registry, topickey.TopicKeyBuild, "orchestrator-build")

	msg := entityqueue.NewMessage(batch.ID, batchIDPayload(t, batch.ID), batch.Queue, nil)
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
	store.EXPECT().GetRequestStore().Return(storagemock.NewMockRequestStore(ctrl)).AnyTimes()
	store.EXPECT().GetBuildStore().Return(storagemock.NewMockBuildStore(ctrl)).AnyTimes()

	controller := newTestController(t, ctrl, store, buildfake.New(changesetfake.New()), nil)

	msg := entityqueue.NewMessage("test-queue/batch/1", batchIDPayload(t, "test-queue/batch/1"), "test-queue", nil)
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
	controller := newTestController(t, ctrl, store, buildfake.New(changesetfake.New()), fmt.Errorf("publish failed"))

	msg := entityqueue.NewMessage(batch.ID, batchIDPayload(t, batch.ID), batch.Queue, nil)
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
	controller := newTestController(t, ctrl, store, buildfake.New(changesetfake.New()), nil)

	var _ consumer.Controller = controller
}

// A batch in any halted state (terminal OR cancelling) must short-circuit:
// the build controller acks without triggering an external CI run and without
// publishing anything. Per the cancel design the speculate controller owns
// cancelling in-flight builds and driving the batch terminal, so the build
// stage simply does no work. Cancelling is included because the cancel
// controller is mid-flight; both halted branches reach the same observable
// behaviour (no build performed).
func TestController_Process_HaltedShortCircuit(t *testing.T) {
	for _, state := range []entity.BatchState{
		entity.BatchStateCancelled,
		entity.BatchStateCancelling,
		entity.BatchStateSucceeded,
		entity.BatchStateFailed,
	} {
		t.Run(string(state), func(t *testing.T) {
			ctrl := gomock.NewController(t)

			batch := testBatch()
			batch.State = state
			store := newMockStorage(ctrl, batch)

			// No Trigger expectation: a stray CI trigger on a halted batch
			// fails the test.
			br := buildrunnermock.NewMockBuildRunner(ctrl)

			// Sentinel publish error: the halted path must not publish. If it
			// does, Process surfaces this error and require.NoError catches it.
			controller := newTestController(t, ctrl, store, br, fmt.Errorf("should not publish"))

			msg := entityqueue.NewMessage(batch.ID, batchIDPayload(t, batch.ID), batch.Queue, nil)
			delivery := queuemock.NewMockDelivery(ctrl)
			delivery.EXPECT().Message().Return(msg).AnyTimes()
			delivery.EXPECT().Attempt().Return(1).AnyTimes()

			require.NoError(t, controller.Process(context.Background(), delivery))
		})
	}
}
