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

package buildsignal

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	"github.com/uber/submitqueue/platform/errs"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	buildrunnermock "github.com/uber/submitqueue/submitqueue/extension/buildrunner/mock"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

// testHarness wires a Controller against mock queues for two topic keys
// (buildsignal and speculate) so individual tests can assert which publish
// or hold happens.
type testHarness struct {
	controller   *Controller
	br           *buildrunnermock.MockBuildRunner
	buildStore   *storagemock.MockBuildStore
	batchStore   *storagemock.MockBatchStore
	signalPub    *queuemock.MockPublisher
	speculatePub *queuemock.MockPublisher
}

// staticStorageFactory resolves every queue to one fixed store aggregate.
type staticStorageFactory struct{ store storage.Storage }

// For returns the fixed store aggregate for any queue.
func (f staticStorageFactory) For(storage.Config) (storage.Storage, error) { return f.store, nil }

func newTestHarness(t *testing.T, ctrl *gomock.Controller) *testHarness {
	br := buildrunnermock.NewMockBuildRunner(ctrl)
	brFactory := buildrunnermock.NewMockFactory(ctrl)
	brFactory.EXPECT().For(gomock.Any()).Return(br, nil).AnyTimes()

	signalPub := queuemock.NewMockPublisher(ctrl)
	signalQ := queuemock.NewMockQueue(ctrl)
	signalQ.EXPECT().Publisher().Return(signalPub).AnyTimes()

	speculatePub := queuemock.NewMockPublisher(ctrl)
	speculateQ := queuemock.NewMockQueue(ctrl)
	speculateQ.EXPECT().Publisher().Return(speculatePub).AnyTimes()

	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: topickey.TopicKeyBuildSignal, Name: "buildsignal", Queue: signalQ},
		{Key: topickey.TopicKeySpeculate, Name: "speculate", Queue: speculateQ},
	})
	require.NoError(t, err)

	buildStore := storagemock.NewMockBuildStore(ctrl)
	batchStore := storagemock.NewMockBatchStore(ctrl)
	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBuildStore().Return(buildStore).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	c := NewController(
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
		staticStorageFactory{store: store},
		brFactory,
		registry,
		topickey.TopicKeyBuildSignal,
		"orchestrator-buildsignal",
	)
	return &testHarness{
		controller:   c,
		br:           br,
		buildStore:   buildStore,
		batchStore:   batchStore,
		signalPub:    signalPub,
		speculatePub: speculatePub,
	}
}

// buildDelivery builds a delivery whose payload is the build's ID, matching
// the on-queue contract: only the identifier travels, the consumer loads the
// full Build from storage. Tests that expect a hold add the expectation on
// the returned mock.
func buildDelivery(t *testing.T, ctrl *gomock.Controller, b entity.Build) *consumermock.MockDelivery {
	t.Helper()
	payload, err := entity.BuildID{ID: b.ID}.ToBytes()
	require.NoError(t, err)
	msg := entityqueue.NewMessage(b.ID, payload, b.BatchID, nil)
	d := consumermock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(msg).AnyTimes()
	d.EXPECT().Attempt().Return(1).AnyTimes()
	return d
}

func TestController_Identity(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)

	assert.Equal(t, "buildsignal", h.controller.Name())
	assert.Equal(t, topickey.TopicKeyBuildSignal, h.controller.TopicKey())
	assert.Equal(t, "orchestrator-buildsignal", h.controller.ConsumerGroup())

	var _ consumer.Controller = h.controller
}

// TestController_Process_Terminal verifies a terminal poll persists the
// status, publishes the batch ID to speculate, and does NOT hold the
// delivery for another poll.
func TestController_Process_Terminal(t *testing.T) {
	tests := []struct {
		name   string
		status entity.BuildStatus
	}{
		{"succeeded", entity.BuildStatusSucceeded},
		{"failed", entity.BuildStatusFailed},
		{"cancelled", entity.BuildStatusCancelled},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			h := newTestHarness(t, ctrl)

			build := entity.Build{ID: "b-1", BatchID: "batch-1", Status: entity.BuildStatusAccepted}
			updatedBuild := build
			updatedBuild.Status = tt.status

			h.buildStore.EXPECT().Get(gomock.Any(), build.ID).Return(build, nil)
			h.br.EXPECT().Status(gomock.Any(), entity.BuildID{ID: build.ID}).Return(tt.status, entity.BuildMetadata{}, nil)
			h.batchStore.EXPECT().Get(gomock.Any(), build.BatchID).Return(entity.Batch{ID: build.BatchID, State: entity.BatchStateSpeculating}, nil)
			h.buildStore.EXPECT().Update(gomock.Any(), updatedBuild).Return(nil)
			h.speculatePub.EXPECT().
				Publish(gomock.Any(), "speculate", gomock.AssignableToTypeOf(entityqueue.Message{})).
				DoAndReturn(func(_ context.Context, _ string, msg entityqueue.Message) error {
					bid, err := entity.BatchIDFromBytes(msg.Payload)
					require.NoError(t, err)
					assert.Equal(t, build.BatchID, bid.ID)
					return nil
				}).Times(1)
			// No Hold expected on terminal — any Hold call fails the test.

			err := h.controller.Process(context.Background(), buildDelivery(t, ctrl, build))
			require.NoError(t, err)
		})
	}
}

// TestController_Process_NonTerminal verifies a non-terminal poll persists
// the status, publishes to speculate, AND holds the delivery for the next
// poll with the per-status delay.
func TestController_Process_NonTerminal(t *testing.T) {
	tests := []struct {
		name        string
		status      entity.BuildStatus
		wantDelayMs int64
	}{
		{"accepted uses accepted delay", entity.BuildStatusAccepted, PollDelayAcceptedMs},
		{"running uses running delay", entity.BuildStatusRunning, PollDelayRunningMs},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			h := newTestHarness(t, ctrl)

			build := entity.Build{ID: "b-2", BatchID: "batch-2", Status: entity.BuildStatusAccepted}
			updatedBuild := build
			updatedBuild.Status = tt.status

			h.buildStore.EXPECT().Get(gomock.Any(), build.ID).Return(build, nil)
			h.br.EXPECT().Status(gomock.Any(), entity.BuildID{ID: build.ID}).Return(tt.status, entity.BuildMetadata{}, nil)
			h.batchStore.EXPECT().Get(gomock.Any(), build.BatchID).Return(entity.Batch{ID: build.BatchID, State: entity.BatchStateSpeculating}, nil)
			h.buildStore.EXPECT().Update(gomock.Any(), updatedBuild).Return(nil)
			h.speculatePub.EXPECT().Publish(gomock.Any(), "speculate", gomock.Any()).Return(nil).Times(1)

			d := buildDelivery(t, ctrl, build)
			d.EXPECT().Hold(tt.wantDelayMs)

			err := h.controller.Process(context.Background(), d)
			require.NoError(t, err)
		})
	}
}

func TestController_Process_StatusError(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)

	build := entity.Build{ID: "b-3", BatchID: "batch-3", Status: entity.BuildStatusAccepted}

	h.buildStore.EXPECT().Get(gomock.Any(), build.ID).Return(build, nil)
	h.batchStore.EXPECT().Get(gomock.Any(), build.BatchID).Return(entity.Batch{ID: build.BatchID, State: entity.BatchStateSpeculating}, nil)
	h.br.EXPECT().Status(gomock.Any(), entity.BuildID{ID: build.ID}).Return(entity.BuildStatusUnknown, nil, errors.New("provider down"))
	// No Update, no Publish, no Hold expected.

	err := h.controller.Process(context.Background(), buildDelivery(t, ctrl, build))
	require.Error(t, err)
	// Non-retryable: rejects to DLQ on first failure; republish is the recovery path.
	assert.False(t, errs.IsRetryable(err))
}

func TestController_Process_UpdateError(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)

	build := entity.Build{ID: "b-4", BatchID: "batch-4", Status: entity.BuildStatusAccepted}
	updatedBuild := build
	updatedBuild.Status = entity.BuildStatusRunning

	h.buildStore.EXPECT().Get(gomock.Any(), build.ID).Return(build, nil)
	h.br.EXPECT().Status(gomock.Any(), entity.BuildID{ID: build.ID}).Return(entity.BuildStatusRunning, nil, nil)
	h.batchStore.EXPECT().Get(gomock.Any(), build.BatchID).Return(entity.Batch{ID: build.BatchID, State: entity.BatchStateSpeculating}, nil)
	h.buildStore.EXPECT().Update(gomock.Any(), updatedBuild).
		Return(errors.New("db unreachable"))
	// No Publish / Hold expected after the store failure.

	err := h.controller.Process(context.Background(), buildDelivery(t, ctrl, build))
	require.Error(t, err)
	// Non-retryable: rejects to DLQ on first failure; republish is the recovery path.
	assert.False(t, errs.IsRetryable(err))
}

// TestController_Process_GetError verifies that a failure to load the Build
// from storage (only the ID is on the queue) surfaces an error. Non-retryable:
// it rejects to DLQ on first failure, consistent with other storage reads.
func TestController_Process_GetError(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)

	build := entity.Build{ID: "b-6", BatchID: "batch-6", Status: entity.BuildStatusAccepted}

	h.buildStore.EXPECT().Get(gomock.Any(), build.ID).Return(entity.Build{}, errors.New("db unreachable"))
	// No Status / Update / Publish expected once the load fails.

	err := h.controller.Process(context.Background(), buildDelivery(t, ctrl, build))
	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}

func TestController_Process_MalformedPayload(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)

	msg := entityqueue.NewMessage("bad", []byte(`{"invalid"`), "batch-bad", nil)
	d := consumermock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(msg).AnyTimes()
	d.EXPECT().Attempt().Return(1).AnyTimes()

	err := h.controller.Process(context.Background(), d)
	require.Error(t, err)
}

// A halted batch (terminal OR cancelling) must short-circuit: just ack, no
// status persist and no publish to speculate. For terminal: speculate is
// already idempotent on terminal, but skipping the publish keeps the system
// from re-emitting noise. For Cancelling: the cancel controller owns the
// terminal write and downstream fan-out, so any further pipeline work would
// race against it.
func TestController_Process_HaltedShortCircuit(t *testing.T) {
	for _, state := range []entity.BatchState{
		entity.BatchStateCancelled,
		entity.BatchStateCancelling,
		entity.BatchStateSucceeded,
		entity.BatchStateFailed,
	} {
		t.Run(string(state), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			h := newTestHarness(t, ctrl)

			build := entity.Build{ID: "b-halt", BatchID: "batch-halt", Status: entity.BuildStatusAccepted}

			h.buildStore.EXPECT().Get(gomock.Any(), build.ID).Return(build, nil)
			h.br.EXPECT().Status(gomock.Any(), entity.BuildID{ID: build.ID}).Return(entity.BuildStatusRunning, entity.BuildMetadata{}, nil)
			h.batchStore.EXPECT().Get(gomock.Any(), build.BatchID).Return(entity.Batch{ID: build.BatchID, State: state}, nil)
			// Halted: no Update, no speculate Publish, no Hold. The
			// harness publishers have no expectations, so any publish fails
			// the test.

			require.NoError(t, h.controller.Process(context.Background(), buildDelivery(t, ctrl, build)))
		})
	}
}
