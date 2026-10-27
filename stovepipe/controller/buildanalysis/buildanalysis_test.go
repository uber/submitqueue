// Copyright (c) 2026 Uber Technologies, Inc.
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

package buildanalysis

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/metrics"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/buildissue"
	buildissuemock "github.com/uber/submitqueue/stovepipe/extension/buildissue/mock"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

const (
	testQueue   = "validation/main"
	testBuildID = "build/42"
)

type analysisMocks struct {
	request         *entity.Request
	ctrl            *gomock.Controller
	stores          *storagemock.MockFactory
	store           *storagemock.MockStorage
	builds          *storagemock.MockBuildStore
	requests        *storagemock.MockRequestStore
	issues          *storagemock.MockBuildIssueStore
	detectorFactory *buildissuemock.MockDetectorFactory
	detector        *buildissuemock.MockDetector
	scope           tally.TestScope
}

func newTestController(t *testing.T) (*Controller, analysisMocks) {
	t.Helper()

	ctrl := gomock.NewController(t)
	m := analysisMocks{
		ctrl:            ctrl,
		requests:        storagemock.NewMockRequestStore(ctrl),
		request:         &entity.Request{ID: "request/1", Queue: testQueue, State: entity.RequestStateProcessing},
		stores:          storagemock.NewMockFactory(ctrl),
		store:           storagemock.NewMockStorage(ctrl),
		builds:          storagemock.NewMockBuildStore(ctrl),
		issues:          storagemock.NewMockBuildIssueStore(ctrl),
		detectorFactory: buildissuemock.NewMockDetectorFactory(ctrl),
		detector:        buildissuemock.NewMockDetector(ctrl),
		scope:           tally.NewTestScope("test", nil),
	}

	m.store.EXPECT().GetBuildStore().Return(m.builds).AnyTimes()
	m.store.EXPECT().GetRequestStore().Return(m.requests).AnyTimes()
	m.requests.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, string) (entity.Request, error) { return *m.request, nil }).AnyTimes()
	m.store.EXPECT().GetBuildIssueStore().Return(m.issues).AnyTimes()

	c := NewController(zap.NewNop().Sugar(), m.scope, m.stores, m.detectorFactory,
		stovepipemq.TopicKeyBuildAnalysis, "stovepipe-buildanalysis")

	return c, m
}

func analysisMessage(t *testing.T) entityqueue.Message {
	t.Helper()
	payload, err := stovepipemq.Marshal(&stovepipemq.BuildAnalysis{Id: testBuildID, QueueName: testQueue})
	require.NoError(t, err)

	msg := entityqueue.NewMessage(testBuildID, payload, testBuildID, nil)
	msg.Tenant = testQueue

	return msg
}

func (m analysisMocks) delivery(t *testing.T) *consumermock.MockDelivery {
	t.Helper()
	d := consumermock.NewMockDelivery(m.ctrl)
	d.EXPECT().Message().Return(analysisMessage(t))

	return d
}

func (m analysisMocks) expectPass(status entity.BuildStatus, issues []entity.BuildIssue, err error) *gomock.Call {
	resolve := m.stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
	load := m.builds.EXPECT().Get(gomock.Any(), testBuildID).
		Return(entity.Build{ID: testBuildID, Status: status}, nil).After(resolve)
	detector := m.detectorFactory.EXPECT().For(buildissue.DetectorConfig{QueueName: testQueue}).Return(m.detector, nil).After(load)
	return m.detector.EXPECT().Detect(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(issues, err).After(detector)
}

func TestProcessPersistsBeforeHoldingOrCompleting(t *testing.T) {
	issue := entity.BuildIssue{TaskID: "validation", UnitID: "unit-a"}

	for _, status := range []entity.BuildStatus{
		entity.BuildStatusAccepted, entity.BuildStatusRunning, entity.BuildStatusUnknown,
		entity.BuildStatusSucceeded, entity.BuildStatusFailed, entity.BuildStatusCancelled,
	} {
		for _, empty := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/empty=%t", status, empty), func(t *testing.T) {
				c, m := newTestController(t)
				var issues []entity.BuildIssue
				if !empty {
					issues = []entity.BuildIssue{issue}
				}

				last := m.expectPass(status, issues, nil)
				if !empty {
					last = m.issues.EXPECT().Create(gomock.Any(), testBuildID, issue).Return(nil).After(last)
				}

				d := m.delivery(t)
				if !status.IsTerminal() {
					d.EXPECT().Hold(PollDelayMs).After(last)
				}

				require.NoError(t, c.Process(context.Background(), d))
			})
		}
	}
}

func TestProcessAcceptsRepeatedAndTaskWideObservations(t *testing.T) {
	c, m := newTestController(t)
	issue := entity.BuildIssue{TaskID: "validation"}

	last := m.expectPass(entity.BuildStatusRunning, []entity.BuildIssue{issue, issue}, nil)
	last = m.issues.EXPECT().Create(gomock.Any(), testBuildID, issue).Return(nil).After(last)
	last = m.issues.EXPECT().Create(gomock.Any(), testBuildID, issue).
		Return(fmt.Errorf("already recorded: %w", storage.ErrAlreadyExists)).After(last)

	d := m.delivery(t)
	d.EXPECT().Hold(PollDelayMs).After(last)

	require.NoError(t, c.Process(context.Background(), d))
}

func TestProcessRetriesPartialPersistenceAndReloadsStatus(t *testing.T) {
	c, m := newTestController(t)
	a := entity.BuildIssue{TaskID: "validation", UnitID: "unit-a"}
	b := entity.BuildIssue{TaskID: "validation", UnitID: "unit-b"}
	writeErr := errs.NewRetryableError(assert.AnError)

	for _, pass := range []struct {
		status    entity.BuildStatus
		firstErr  error
		secondErr error
	}{
		{status: entity.BuildStatusRunning, secondErr: writeErr},
		{status: entity.BuildStatusRunning, firstErr: storage.ErrAlreadyExists},
		{status: entity.BuildStatusFailed, firstErr: storage.ErrAlreadyExists, secondErr: storage.ErrAlreadyExists},
	} {

		last := m.expectPass(pass.status, []entity.BuildIssue{a, b}, nil)
		last = m.issues.EXPECT().Create(gomock.Any(), testBuildID, a).Return(pass.firstErr).After(last)
		last = m.issues.EXPECT().Create(gomock.Any(), testBuildID, b).Return(pass.secondErr).After(last)

		d := m.delivery(t)
		if pass.secondErr == writeErr {
			err := c.Process(context.Background(), d)
			require.ErrorIs(t, err, writeErr)
			assert.True(t, errs.IsRetryable(err))
		} else {
			if !pass.status.IsTerminal() {
				d.EXPECT().Hold(PollDelayMs).After(last)
			}

			require.NoError(t, c.Process(context.Background(), d))
		}
	}
}

func TestProcessRunsAgainWhenBuildCompletesDuringDetection(t *testing.T) {
	c, m := newTestController(t)
	status := entity.BuildStatusRunning

	m.stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil).Times(2)
	m.builds.EXPECT().Get(gomock.Any(), testBuildID).DoAndReturn(func(context.Context, string) (entity.Build, error) {
		return entity.Build{ID: testBuildID, Status: status}, nil
	}).Times(2)

	m.detectorFactory.EXPECT().For(buildissue.DetectorConfig{QueueName: testQueue}).Return(m.detector, nil).Times(2)
	issue := entity.BuildIssue{TaskID: "validation", UnitID: "final-unit"}
	gomock.InOrder(
		m.detector.EXPECT().Detect(gomock.Any(), entity.BuildID{ID: testBuildID}).DoAndReturn(func(context.Context, entity.BuildID) ([]entity.BuildIssue, error) {
			status = entity.BuildStatusFailed
			return nil, nil
		}),
		m.detector.EXPECT().Detect(gomock.Any(), entity.BuildID{ID: testBuildID}).Return([]entity.BuildIssue{issue}, nil),
	)

	first := m.delivery(t)
	first.EXPECT().Hold(PollDelayMs)

	require.NoError(t, c.Process(context.Background(), first))

	m.issues.EXPECT().Create(gomock.Any(), testBuildID, issue).Return(nil)

	require.NoError(t, c.Process(context.Background(), m.delivery(t)))
}

func TestProcessDiscardsResultsOnDetectionError(t *testing.T) {
	for _, status := range []entity.BuildStatus{
		entity.BuildStatusAccepted, entity.BuildStatusRunning, entity.BuildStatusUnknown,
		entity.BuildStatusSucceeded, entity.BuildStatusFailed, entity.BuildStatusCancelled,
	} {
		for _, retryable := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/retryable=%t", status, retryable), func(t *testing.T) {
				c, m := newTestController(t)
				detectErr := error(assert.AnError)
				if retryable {
					detectErr = errs.NewRetryableError(detectErr)
				}

				m.expectPass(status, []entity.BuildIssue{{TaskID: "validation", UnitID: "unit"}}, detectErr)

				err := c.Process(context.Background(), m.delivery(t))
				require.ErrorIs(t, err, detectErr)
				assert.Equal(t, retryable, errs.IsRetryable(err))
			})
		}
	}
}

func TestProcessPropagatesDependencyErrors(t *testing.T) {
	for _, step := range []string{"resolve storage", "load build", "resolve detector"} {
		t.Run(step, func(t *testing.T) {
			c, m := newTestController(t)
			if step == "resolve storage" {

				m.stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(nil, assert.AnError)
			} else {

				m.stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
				if step == "load build" {
					m.builds.EXPECT().Get(gomock.Any(), testBuildID).Return(entity.Build{}, assert.AnError)
				} else {
					m.builds.EXPECT().Get(gomock.Any(), testBuildID).Return(entity.Build{ID: testBuildID}, nil)

					m.detectorFactory.EXPECT().For(buildissue.DetectorConfig{QueueName: testQueue}).Return(nil, assert.AnError)
				}
			}

			require.ErrorIs(t, c.Process(context.Background(), m.delivery(t)), assert.AnError)
		})
	}
}

func TestProcessRejectsInvalidMessagesBeforeResolvingDependencies(t *testing.T) {
	for _, kind := range []string{"malformed payload", "empty queue", "empty build ID", "empty tenant", "tenant mismatch", "metadata mismatch"} {
		t.Run(kind, func(t *testing.T) {
			c, m := newTestController(t)
			msg := analysisMessage(t)
			switch kind {
			case "malformed payload":
				msg.Payload = []byte("{")
			case "empty queue":
				msg.Payload = []byte(`{"id":"build/42"}`)
			case "empty build ID":
				msg.Payload = []byte(`{"queue_name":"validation/main"}`)
			case "empty tenant":
				msg.Tenant = ""
			case "tenant mismatch":
				msg.Tenant = "other/main"
			case "metadata mismatch":
				msg.Metadata[entityqueue.MetadataKeyQueueName] = "other/main"
			}

			d := consumermock.NewMockDelivery(m.ctrl)
			d.EXPECT().Message().Return(msg)

			err := c.Process(context.Background(), d)
			require.Error(t, err)
			assert.False(t, errs.IsRetryable(err))
		})
	}
}

func TestProcessDoesNotTreatMissingBuildAsStorageLag(t *testing.T) {
	c, m := newTestController(t)

	m.stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
	m.builds.EXPECT().Get(gomock.Any(), testBuildID).Return(entity.Build{}, storage.ErrNotFound)

	err := c.Process(context.Background(), m.delivery(t))
	require.ErrorIs(t, err, storage.ErrNotFound)
	assert.False(t, errs.IsRetryable(err))
}

func TestProcessTagsMetricsWithQueue(t *testing.T) {
	c, m := newTestController(t)

	m.stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(nil, assert.AnError)
	ctx := metrics.WithContextTags(context.Background(), metrics.NewTag("queue", testQueue))

	require.Error(t, c.Process(ctx, m.delivery(t)))

	counter, ok := m.scope.Snapshot().Counters()["test.buildanalysis_controller.process.start+queue="+testQueue]
	require.True(t, ok)
	assert.EqualValues(t, 1, counter.Value())
}

func TestProcessReturnsPersistenceErrorsWithoutHoldingOrContinuing(t *testing.T) {
	for _, status := range []entity.BuildStatus{entity.BuildStatusRunning, entity.BuildStatusFailed} {
		t.Run(string(status), func(t *testing.T) {
			c, m := newTestController(t)
			first := entity.BuildIssue{TaskID: "validation", UnitID: "first"}
			later := entity.BuildIssue{TaskID: "validation", UnitID: "later"}
			writeErr := errs.NewRetryableError(assert.AnError)

			detected := m.expectPass(status, []entity.BuildIssue{first, later}, nil)
			m.issues.EXPECT().Create(gomock.Any(), testBuildID, first).Return(writeErr).After(detected)

			require.ErrorIs(t, c.Process(context.Background(), m.delivery(t)), writeErr)
		})
	}
}

func TestProcessStopsWhenRequestEndsWithoutTerminalBuildStatus(t *testing.T) {
	for _, state := range []entity.RequestState{entity.RequestStateFailed, entity.RequestStateCancelled, entity.RequestStateSuperseded, entity.RequestStateSucceeded} {
		t.Run(string(state), func(t *testing.T) {
			c, m := newTestController(t)
			m.request.State = state

			m.stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
			m.builds.EXPECT().Get(gomock.Any(), testBuildID).Return(entity.Build{ID: testBuildID, RequestID: m.request.ID, Status: entity.BuildStatusRunning}, nil).Times(2)

			require.NoError(t, c.Process(context.Background(), m.delivery(t)))
		})
	}
}

func TestProcessStillAnalyzesTerminalBuildOfTerminalRequest(t *testing.T) {
	c, m := newTestController(t)
	m.request.State = entity.RequestStateFailed
	issue := entity.BuildIssue{TaskID: "validation", UnitID: "unit"}

	detected := m.expectPass(entity.BuildStatusFailed, []entity.BuildIssue{issue}, nil)
	m.issues.EXPECT().Create(gomock.Any(), testBuildID, issue).Return(nil).After(detected)

	require.NoError(t, c.Process(context.Background(), m.delivery(t)))
}

func TestProcessRejectsRequestFromDifferentQueue(t *testing.T) {
	c, m := newTestController(t)
	m.request.Queue = "other/main"

	m.stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
	m.builds.EXPECT().Get(gomock.Any(), testBuildID).Return(entity.Build{ID: testBuildID, RequestID: m.request.ID}, nil)

	require.Error(t, c.Process(context.Background(), m.delivery(t)))
}

func TestCompletionBetweenBuildAndRequestReadStillAnalyzes(t *testing.T) {
	c, m := newTestController(t)
	m.request.State = entity.RequestStateFailed

	m.stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
	gomock.InOrder(
		m.builds.EXPECT().Get(gomock.Any(), testBuildID).Return(entity.Build{ID: testBuildID, RequestID: m.request.ID, Status: entity.BuildStatusRunning}, nil),
		m.builds.EXPECT().Get(gomock.Any(), testBuildID).Return(entity.Build{ID: testBuildID, RequestID: m.request.ID, Status: entity.BuildStatusFailed}, nil),
	)

	m.detectorFactory.EXPECT().For(buildissue.DetectorConfig{QueueName: testQueue}).Return(m.detector, nil)
	issue := entity.BuildIssue{TaskID: "validation", UnitID: "unit"}
	detected := m.detector.EXPECT().Detect(gomock.Any(), entity.BuildID{ID: testBuildID}).Return([]entity.BuildIssue{issue}, nil)
	m.issues.EXPECT().Create(gomock.Any(), testBuildID, issue).Return(nil).After(detected)

	require.NoError(t, c.Process(context.Background(), m.delivery(t)))
}

func TestProcessRetriesErrorsAndReloadsStatus(t *testing.T) {
	c, m := newTestController(t)
	status := entity.BuildStatusRunning

	m.stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil).Times(3)
	m.builds.EXPECT().Get(gomock.Any(), testBuildID).DoAndReturn(func(context.Context, string) (entity.Build, error) {
		return entity.Build{ID: testBuildID, Status: status}, nil
	}).Times(3)

	m.detectorFactory.EXPECT().For(buildissue.DetectorConfig{QueueName: testQueue}).Return(m.detector, nil).Times(3)
	issue := entity.BuildIssue{TaskID: "validation", UnitID: "final-unit"}
	terminalErr := errs.NewRetryableError(assert.AnError)
	gomock.InOrder(
		m.detector.EXPECT().Detect(gomock.Any(), entity.BuildID{ID: testBuildID}).DoAndReturn(func(context.Context, entity.BuildID) ([]entity.BuildIssue, error) {
			status = entity.BuildStatusFailed
			return nil, terminalErr
		}),
		m.detector.EXPECT().Detect(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(nil, terminalErr),
		m.detector.EXPECT().Detect(gomock.Any(), entity.BuildID{ID: testBuildID}).Return([]entity.BuildIssue{issue}, nil),
	)

	first := m.delivery(t)

	require.ErrorIs(t, c.Process(context.Background(), first), terminalErr)

	err := c.Process(context.Background(), m.delivery(t))
	require.ErrorIs(t, err, terminalErr)
	assert.True(t, errs.IsRetryable(err))

	m.issues.EXPECT().Create(gomock.Any(), testBuildID, issue).Return(nil)

	require.NoError(t, c.Process(context.Background(), m.delivery(t)))
}

func TestAnalysisFailureRemainsVisibleInMetrics(t *testing.T) {
	c, m := newTestController(t)
	m.expectPass(entity.BuildStatusRunning, nil, assert.AnError)
	ctx := metrics.WithContextTags(context.Background(), metrics.NewTag("queue", testQueue))

	require.ErrorIs(t, c.Process(ctx, m.delivery(t)), assert.AnError)

	_, ok := m.scope.Snapshot().Histograms()["test.buildanalysis_controller.process.finish+queue="+testQueue+",result=error"]
	assert.True(t, ok)
}

func TestCancelledAnalysisDoesNotHold(t *testing.T) {
	for _, operation := range []string{"detect", "persist"} {
		t.Run(operation, func(t *testing.T) {
			c, m := newTestController(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if operation == "detect" {

				m.stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
				m.builds.EXPECT().Get(gomock.Any(), testBuildID).Return(entity.Build{ID: testBuildID, Status: entity.BuildStatusRunning}, nil)

				m.detectorFactory.EXPECT().For(buildissue.DetectorConfig{QueueName: testQueue}).Return(m.detector, nil)
				m.detector.EXPECT().Detect(gomock.Any(), entity.BuildID{ID: testBuildID}).DoAndReturn(func(context.Context, entity.BuildID) ([]entity.BuildIssue, error) {
					cancel()
					return nil, context.Canceled
				})
			} else {
				issue := entity.BuildIssue{TaskID: "validation", UnitID: "unit"}

				detected := m.expectPass(entity.BuildStatusRunning, []entity.BuildIssue{issue}, nil)
				m.issues.EXPECT().Create(gomock.Any(), testBuildID, issue).DoAndReturn(func(context.Context, string, entity.BuildIssue) error {
					cancel()
					return context.Canceled
				}).After(detected)
			}

			require.ErrorIs(t, c.Process(ctx, m.delivery(t)), context.Canceled)
		})
	}
}
