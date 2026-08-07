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
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	buildrunnermock "github.com/uber/submitqueue/submitqueue/extension/buildrunner/mock"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

const (
	testPathID  = "path-abc"
	testAttempt = 2
	testBuildID = "build-7"
	testBatchID = "test-queue/batch/head"
)

// testHarness wires a Controller against mock queues for two topic keys
// (buildsignal and speculate) so individual tests can assert which Publish
// happens. Nothing may publish to buildsignal — the poll continuation is a
// hold on the delivery — so the signal publisher carries no expectations and
// any publish to it fails the test.
type testHarness struct {
	controller   *Controller
	br           *buildrunnermock.MockBuildRunner
	builds       *storagemock.MockBuildStore
	batchStore   *storagemock.MockBatchStore
	pathSets     *storagemock.MockSpeculationPathSetStore
	pathBuilds   *storagemock.MockPathBuildStore
	signalPub    *queuemock.MockPublisher
	speculatePub *queuemock.MockPublisher
}

// staticStorageFactory resolves every queue to one fixed store aggregate.
type staticStorageFactory struct{ store storage.Storage }

// For returns the fixed store aggregate for any queue.
func (f staticStorageFactory) For(storage.Config) (storage.Storage, error) { return f.store, nil }

func newTestHarness(t *testing.T, ctrl *gomock.Controller, batchState entity.BatchState) *testHarness {
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

	builds := storagemock.NewMockBuildStore(ctrl)
	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), testBatchID).Return(entity.Batch{
		ID: testBatchID, Queue: "test-queue", State: batchState, Version: 1,
	}, nil).AnyTimes()

	// The path set and link stores are wired read-only: their getters answer,
	// but no write expectation exists, so any Create/Update from this
	// controller fails the test.
	pathSets := storagemock.NewMockSpeculationPathSetStore(ctrl)
	pathBuilds := storagemock.NewMockPathBuildStore(ctrl)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBuildStore().Return(builds).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetSpeculationPathSetStore().Return(pathSets).AnyTimes()
	store.EXPECT().GetPathBuildStore().Return(pathBuilds).AnyTimes()

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
		builds:       builds,
		batchStore:   batchStore,
		pathSets:     pathSets,
		pathBuilds:   pathBuilds,
		signalPub:    signalPub,
		speculatePub: speculatePub,
	}
}

// wanted wires the kill-list reads to say the build is still wanted: its entry
// is live on the build's own attempt, and the attempt's link names this build.
func (h *testHarness) wanted() {
	h.pathSets.EXPECT().Get(gomock.Any(), testBatchID).Return(entity.SpeculationPathSet{
		Head: testBatchID,
		Paths: []entity.SpeculationPathEntry{
			{ID: testPathID, Status: entity.SpeculationPathStatusBuilding, Attempt: testAttempt},
		},
	}, nil).AnyTimes()
	h.pathBuilds.EXPECT().Get(gomock.Any(), testPathID, testAttempt).Return(entity.PathBuild{
		PathID: testPathID, Attempt: testAttempt, BuildID: testBuildID,
	}, nil).AnyTimes()
}

// delivery builds a delivery whose payload is the attempt's key, matching the
// on-queue contract: only the identifier travels, and the consumer loads the
// execution record — including the build ID — from storage. The concrete mock
// is returned so tests can expect a Hold for the next poll.
func delivery(t *testing.T, ctrl *gomock.Controller) *consumermock.MockDelivery {
	t.Helper()
	payload, err := entity.BuildID{ID: testBuildID}.ToBytes()
	require.NoError(t, err)
	msg := entityqueue.NewMessage(testBuildID, payload, testBuildID, nil)
	d := consumermock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(msg).AnyTimes()
	d.EXPECT().Attempt().Return(1).AnyTimes()
	return d
}

// testBuild returns the build under test, in the given status.
func testBuild(status entity.BuildStatus) entity.Build {
	return entity.Build{
		ID:      testBuildID,
		BatchID: testBatchID,
		PathID:  testPathID,
		Attempt: testAttempt,
		Status:  status,
	}
}

func TestController_Identity(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl, entity.BatchStateSpeculating)

	assert.Equal(t, "buildsignal", h.controller.Name())
	assert.Equal(t, topickey.TopicKeyBuildSignal, h.controller.TopicKey())
	assert.Equal(t, "orchestrator-buildsignal", h.controller.ConsumerGroup())

	var _ consumer.Controller = h.controller
}

// The poll loop records status on the build and wakes the run. It must never
// write the path set — that is the speculate run's state, and it is read here
// only as the kill list.
func TestProcess_RecordsStatusAndNeverWritesThePathSet(t *testing.T) {
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
			h := newTestHarness(t, ctrl, entity.BatchStateSpeculating)

			h.builds.EXPECT().Get(gomock.Any(), testBuildID).Return(testBuild(entity.BuildStatusRunning), nil)
			h.br.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(tt.status, nil, nil)
			h.builds.EXPECT().Update(gomock.Any(), testBuild(tt.status)).Return(nil)

			h.speculatePub.EXPECT().Publish(gomock.Any(), "speculate", gomock.Any()).Return(nil)
			// Terminal: no reschedule, no Cancel, and not even a kill-list read
			// — a finished build has nothing left to stop. Any path-set write
			// fails the test (none is expected on the mock).

			require.NoError(t, h.controller.Process(context.Background(), delivery(t, ctrl)))
		})
	}
}

// While a build is in flight the loop holds the delivery for the poll delay,
// so the same message — and the same build partition — carries every poll.
func TestProcess_NonTerminalHoldsForNextPoll(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl, entity.BatchStateSpeculating)
	h.wanted()

	h.builds.EXPECT().Get(gomock.Any(), testBuildID).Return(testBuild(entity.BuildStatusAccepted), nil)
	h.br.EXPECT().Status(gomock.Any(), gomock.Any()).Return(entity.BuildStatusRunning, nil, nil)
	h.builds.EXPECT().Update(gomock.Any(), testBuild(entity.BuildStatusRunning)).Return(nil)

	h.speculatePub.EXPECT().Publish(gomock.Any(), "speculate", gomock.Any()).Return(nil)
	d := delivery(t, ctrl)
	d.EXPECT().Hold(PollDelayRunningMs)

	require.NoError(t, h.controller.Process(context.Background(), d))
}

// An unchanged status is not rewritten on every poll of a long build.
func TestProcess_UnchangedStatusSkipsWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl, entity.BatchStateSpeculating)
	h.wanted()

	h.builds.EXPECT().Get(gomock.Any(), testBuildID).Return(testBuild(entity.BuildStatusRunning), nil)
	h.br.EXPECT().Status(gomock.Any(), gomock.Any()).Return(entity.BuildStatusRunning, nil, nil)
	// No Update expected.
	h.speculatePub.EXPECT().Publish(gomock.Any(), "speculate", gomock.Any()).Return(nil)
	d := delivery(t, ctrl)
	d.EXPECT().Hold(PollDelayRunningMs)

	require.NoError(t, h.controller.Process(context.Background(), d))
}

// TestProcess_StopsUnwantedBuilds covers the kill list: a running build is
// cancelled the moment nothing wants it — its path called off, its attempt
// superseded, its link naming a different build, or its whole batch halted —
// and the poll keeps running so a later poll records the stop.
func TestProcess_StopsUnwantedBuilds(t *testing.T) {
	tests := []struct {
		name       string
		batchState entity.BatchState
		setup      func(h *testHarness)
	}{
		{
			name:       "path cancelling",
			batchState: entity.BatchStateSpeculating,
			setup: func(h *testHarness) {
				h.pathSets.EXPECT().Get(gomock.Any(), testBatchID).Return(entity.SpeculationPathSet{
					Head: testBatchID,
					Paths: []entity.SpeculationPathEntry{
						{ID: testPathID, Status: entity.SpeculationPathStatusCancelling, Attempt: testAttempt},
					},
				}, nil)
			},
		},
		{
			name:       "path cancelled",
			batchState: entity.BatchStateSpeculating,
			setup: func(h *testHarness) {
				h.pathSets.EXPECT().Get(gomock.Any(), testBatchID).Return(entity.SpeculationPathSet{
					Head: testBatchID,
					Paths: []entity.SpeculationPathEntry{
						{ID: testPathID, Status: entity.SpeculationPathStatusCancelled, Attempt: testAttempt},
					},
				}, nil)
			},
		},
		{
			name:       "attempt superseded",
			batchState: entity.BatchStateSpeculating,
			setup: func(h *testHarness) {
				h.pathSets.EXPECT().Get(gomock.Any(), testBatchID).Return(entity.SpeculationPathSet{
					Head: testBatchID,
					Paths: []entity.SpeculationPathEntry{
						{ID: testPathID, Status: entity.SpeculationPathStatusPending, Attempt: testAttempt + 1},
					},
				}, nil)
			},
		},
		{
			name:       "link names another build",
			batchState: entity.BatchStateSpeculating,
			setup: func(h *testHarness) {
				h.pathSets.EXPECT().Get(gomock.Any(), testBatchID).Return(entity.SpeculationPathSet{
					Head: testBatchID,
					Paths: []entity.SpeculationPathEntry{
						{ID: testPathID, Status: entity.SpeculationPathStatusBuilding, Attempt: testAttempt},
					},
				}, nil)
				h.pathBuilds.EXPECT().Get(gomock.Any(), testPathID, testAttempt).Return(entity.PathBuild{
					PathID: testPathID, Attempt: testAttempt, BuildID: "build-winner",
				}, nil)
			},
		},
		{
			name:       "batch halted",
			batchState: entity.BatchStateCancelling,
			setup: func(h *testHarness) {
				// No kill-list reads: the batch state alone decides, so no
				// set/link expectations exist and any read fails the test.
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			h := newTestHarness(t, ctrl, tt.batchState)
			tt.setup(h)

			h.builds.EXPECT().Get(gomock.Any(), testBuildID).Return(testBuild(entity.BuildStatusRunning), nil)
			h.br.EXPECT().Status(gomock.Any(), gomock.Any()).Return(entity.BuildStatusRunning, nil, nil)
			h.br.EXPECT().Cancel(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(nil)

			h.speculatePub.EXPECT().Publish(gomock.Any(), "speculate", gomock.Any()).Return(nil)
			d := delivery(t, ctrl)
			d.EXPECT().Hold(PollDelayRunningMs)

			require.NoError(t, h.controller.Process(context.Background(), d))
		})
	}
}

// TestProcess_KeepsWantedBuilds is the inverse: a live entry on the build's own
// attempt, linked to this very build, must never be cancelled.
func TestProcess_KeepsWantedBuilds(t *testing.T) {
	for _, status := range []entity.SpeculationPathStatus{
		entity.SpeculationPathStatusPending,
		entity.SpeculationPathStatusBuilding,
		entity.SpeculationPathStatusPassed,
	} {
		t.Run(string(status), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			h := newTestHarness(t, ctrl, entity.BatchStateSpeculating)

			h.pathSets.EXPECT().Get(gomock.Any(), testBatchID).Return(entity.SpeculationPathSet{
				Head: testBatchID,
				Paths: []entity.SpeculationPathEntry{
					{ID: testPathID, Status: status, Attempt: testAttempt},
				},
			}, nil)
			h.pathBuilds.EXPECT().Get(gomock.Any(), testPathID, testAttempt).Return(entity.PathBuild{
				PathID: testPathID, Attempt: testAttempt, BuildID: testBuildID,
			}, nil)

			h.builds.EXPECT().Get(gomock.Any(), testBuildID).Return(testBuild(entity.BuildStatusRunning), nil)
			h.br.EXPECT().Status(gomock.Any(), gomock.Any()).Return(entity.BuildStatusRunning, nil, nil)
			h.speculatePub.EXPECT().Publish(gomock.Any(), "speculate", gomock.Any()).Return(nil)
			d := delivery(t, ctrl)
			d.EXPECT().Hold(PollDelayRunningMs)

			// No Cancel: gomock fails the test if one happens.
			require.NoError(t, h.controller.Process(context.Background(), d))
		})
	}
}

// A kill list that cannot legitimately be missing keeps the build when it is:
// a cancel is irreversible, so store anomalies err toward keeping. The halted
// check still covers every real stop that matters for such a batch.
func TestProcess_AnomalousKillListKeepsTheBuild(t *testing.T) {
	tests := []struct {
		name  string
		setup func(h *testHarness)
	}{
		{
			name: "path set missing",
			setup: func(h *testHarness) {
				h.pathSets.EXPECT().Get(gomock.Any(), testBatchID).
					Return(entity.SpeculationPathSet{}, storage.ErrNotFound)
			},
		},
		{
			name: "entry missing from the set",
			setup: func(h *testHarness) {
				h.pathSets.EXPECT().Get(gomock.Any(), testBatchID).Return(entity.SpeculationPathSet{
					Head: testBatchID,
					Paths: []entity.SpeculationPathEntry{
						{ID: "some-other-path", Status: entity.SpeculationPathStatusBuilding, Attempt: 1},
					},
				}, nil)
			},
		},
		{
			name: "link missing",
			setup: func(h *testHarness) {
				h.pathSets.EXPECT().Get(gomock.Any(), testBatchID).Return(entity.SpeculationPathSet{
					Head: testBatchID,
					Paths: []entity.SpeculationPathEntry{
						{ID: testPathID, Status: entity.SpeculationPathStatusBuilding, Attempt: testAttempt},
					},
				}, nil)
				h.pathBuilds.EXPECT().Get(gomock.Any(), testPathID, testAttempt).
					Return(entity.PathBuild{}, storage.ErrNotFound)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			h := newTestHarness(t, ctrl, entity.BatchStateSpeculating)
			tt.setup(h)

			h.builds.EXPECT().Get(gomock.Any(), testBuildID).Return(testBuild(entity.BuildStatusRunning), nil)
			h.br.EXPECT().Status(gomock.Any(), gomock.Any()).Return(entity.BuildStatusRunning, nil, nil)
			h.speculatePub.EXPECT().Publish(gomock.Any(), "speculate", gomock.Any()).Return(nil)
			d := delivery(t, ctrl)
			d.EXPECT().Hold(PollDelayRunningMs)

			// No Cancel: gomock fails the test if one happens.
			require.NoError(t, h.controller.Process(context.Background(), d))
		})
	}
}

// A failed Cancel must not fail the message: the held delivery is the only
// thing that will retry the cancel, so the poll survives and remakes the
// check.
func TestProcess_CancelFailureDoesNotFailThePoll(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl, entity.BatchStateCancelling)

	h.builds.EXPECT().Get(gomock.Any(), testBuildID).Return(testBuild(entity.BuildStatusRunning), nil)
	h.br.EXPECT().Status(gomock.Any(), gomock.Any()).Return(entity.BuildStatusRunning, nil, nil)
	h.br.EXPECT().Cancel(gomock.Any(), entity.BuildID{ID: testBuildID}).
		Return(errors.New("runner unavailable"))

	h.speculatePub.EXPECT().Publish(gomock.Any(), "speculate", gomock.Any()).Return(nil)
	d := delivery(t, ctrl)
	d.EXPECT().Hold(PollDelayRunningMs)

	require.NoError(t, h.controller.Process(context.Background(), d))
}

// A build without path coordinates predates per-path dispatch. The batch state
// is the only kill list it has: no set or link is read for it.
func TestProcess_BuildWithoutPathChecksOnlyTheBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl, entity.BatchStateSpeculating)

	legacy := testBuild(entity.BuildStatusRunning)
	legacy.PathID = ""
	legacy.Attempt = 0

	h.builds.EXPECT().Get(gomock.Any(), testBuildID).Return(legacy, nil)
	h.br.EXPECT().Status(gomock.Any(), gomock.Any()).Return(entity.BuildStatusRunning, nil, nil)
	h.speculatePub.EXPECT().Publish(gomock.Any(), "speculate", gomock.Any()).Return(nil)
	d := delivery(t, ctrl)
	d.EXPECT().Hold(PollDelayRunningMs)

	// No set/link reads and no Cancel: gomock fails the test on any of them.
	require.NoError(t, h.controller.Process(context.Background(), d))
}

// TestProcess_HaltedBatchStillRuns is the cancellation-correctness case. A
// cancelling batch reaches terminal only once its builds stop, and this loop is
// what stops them and watches them stop, so a halted batch must still be
// polled, cancelled, recorded and rescheduled. Short-circuiting strands it in
// Cancelling forever.
func TestProcess_HaltedBatchStillRuns(t *testing.T) {
	t.Run("cancels and holds for the next poll while the build runs", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		h := newTestHarness(t, ctrl, entity.BatchStateCancelling)

		h.builds.EXPECT().Get(gomock.Any(), testBuildID).Return(testBuild(entity.BuildStatusRunning), nil)
		h.br.EXPECT().Status(gomock.Any(), gomock.Any()).Return(entity.BuildStatusRunning, nil, nil)
		h.br.EXPECT().Cancel(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(nil)
		h.speculatePub.EXPECT().Publish(gomock.Any(), "speculate", gomock.Any()).Return(nil)
		d := delivery(t, ctrl)
		d.EXPECT().Hold(PollDelayRunningMs)

		require.NoError(t, h.controller.Process(context.Background(), d))
	})

	t.Run("still records the terminal status", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		h := newTestHarness(t, ctrl, entity.BatchStateCancelling)

		h.builds.EXPECT().Get(gomock.Any(), testBuildID).Return(testBuild(entity.BuildStatusRunning), nil)
		h.br.EXPECT().Status(gomock.Any(), gomock.Any()).Return(entity.BuildStatusCancelled, nil, nil)
		h.builds.EXPECT().Update(gomock.Any(), testBuild(entity.BuildStatusCancelled)).Return(nil)
		h.speculatePub.EXPECT().Publish(gomock.Any(), "speculate", gomock.Any()).Return(nil)

		// Terminal: no Cancel and no reschedule.
		require.NoError(t, h.controller.Process(context.Background(), delivery(t, ctrl)))
	})
}

func TestProcess_Errors(t *testing.T) {
	tests := []struct {
		name  string
		setup func(h *testHarness)
	}{
		{
			name: "build read failure",
			setup: func(h *testHarness) {
				h.builds.EXPECT().Get(gomock.Any(), testBuildID).
					Return(entity.Build{}, errors.New("connection reset"))
			},
		},
		{
			name: "status failure",
			setup: func(h *testHarness) {
				h.builds.EXPECT().Get(gomock.Any(), testBuildID).Return(testBuild(entity.BuildStatusRunning), nil)
				h.br.EXPECT().Status(gomock.Any(), gomock.Any()).
					Return(entity.BuildStatusUnknown, nil, errors.New("runner unavailable"))
			},
		},
		{
			name: "kill list set read failure",
			setup: func(h *testHarness) {
				h.builds.EXPECT().Get(gomock.Any(), testBuildID).Return(testBuild(entity.BuildStatusRunning), nil)
				h.br.EXPECT().Status(gomock.Any(), gomock.Any()).Return(entity.BuildStatusRunning, nil, nil)
				h.pathSets.EXPECT().Get(gomock.Any(), testBatchID).
					Return(entity.SpeculationPathSet{}, errors.New("connection reset"))
			},
		},
		{
			name: "kill list link read failure",
			setup: func(h *testHarness) {
				h.builds.EXPECT().Get(gomock.Any(), testBuildID).Return(testBuild(entity.BuildStatusRunning), nil)
				h.br.EXPECT().Status(gomock.Any(), gomock.Any()).Return(entity.BuildStatusRunning, nil, nil)
				h.pathSets.EXPECT().Get(gomock.Any(), testBatchID).Return(entity.SpeculationPathSet{
					Head: testBatchID,
					Paths: []entity.SpeculationPathEntry{
						{ID: testPathID, Status: entity.SpeculationPathStatusBuilding, Attempt: testAttempt},
					},
				}, nil)
				h.pathBuilds.EXPECT().Get(gomock.Any(), testPathID, testAttempt).
					Return(entity.PathBuild{}, errors.New("connection reset"))
			},
		},
		{
			name: "status write failure",
			setup: func(h *testHarness) {
				h.builds.EXPECT().Get(gomock.Any(), testBuildID).Return(testBuild(entity.BuildStatusRunning), nil)
				h.br.EXPECT().Status(gomock.Any(), gomock.Any()).Return(entity.BuildStatusSucceeded, nil, nil)
				h.builds.EXPECT().Update(gomock.Any(), gomock.Any()).
					Return(errors.New("connection reset"))
			},
		},
		{
			name: "speculate publish failure",
			setup: func(h *testHarness) {
				h.wanted()
				h.builds.EXPECT().Get(gomock.Any(), testBuildID).Return(testBuild(entity.BuildStatusRunning), nil)
				h.br.EXPECT().Status(gomock.Any(), gomock.Any()).Return(entity.BuildStatusRunning, nil, nil)
				h.speculatePub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).
					Return(errors.New("queue down"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			h := newTestHarness(t, ctrl, entity.BatchStateSpeculating)
			tt.setup(h)

			require.Error(t, h.controller.Process(context.Background(), delivery(t, ctrl)))
		})
	}
}
