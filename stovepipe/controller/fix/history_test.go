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

package fix

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	"github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func TestSubmittedFixHistoryRepairsWithoutResubmitting(t *testing.T) {
	c, m := newTestController(t)
	issue := testIssue()
	m.expectIssue(issue)
	m.executor.EXPECT().Submit(gomock.Any(), issue).Return("fix/1", assert.AnError)
	saved := issueWithFixID(issue, "fix/1")
	m.observations.EXPECT().Update(gomock.Any(), testBuildID, saved, int32(0), int32(1)).Return(nil)
	*m.historyFailure = assert.AnError
	require.ErrorIs(t, c.processBuild(context.Background(), testQueue, testBuildID), assert.AnError)
	require.Empty(t, m.history)
	*m.historyFailure = nil
	for i := 0; i < 2; i++ {
		m.stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
		m.observations.EXPECT().List(gomock.Any(), testBuildID, storage.BuildIssueRange{Limit: buildIssuePageSize}).Return([]entity.BuildIssue{saved}, nil)
		require.NoError(t, c.processBuild(context.Background(), testQueue, testBuildID))
	}
	require.Len(t, m.history, 1)
	var log entity.RequestLog
	for _, retained := range m.history {
		log = retained
	}
	require.Equal(t, entity.RequestEventFixSubmitted, log.Event)
	require.Equal(t, map[string]string{
		requestlog.MetadataKeyBuildID: testBuildID, requestlog.MetadataKeyFixID: "fix/1",
		requestlog.MetadataKeyTaskID: issue.TaskID, requestlog.MetadataKeyUnitID: issue.UnitID,
	}, log.Metadata)
	require.Empty(t, log.State)
}

func TestFixDLQRetainsDecisionWithoutSubmitting(t *testing.T) {
	for _, decision := range []string{"associated", "eligible", "ineligible"} {
		t.Run(decision, func(t *testing.T) {
			_, m := newTestController(t)
			c := NewDLQController(zap.NewNop().Sugar(), tally.NoopScope, m.stores, m.materializer, messagequeue.TopicKeyFix, "recovery")
			issue := testIssue()
			observation := issueIdentity(issue)
			expected := entity.RequestEventFixSubmissionAbandoned
			switch decision {
			case "associated":
				observation.FixID = "fix/1"
				expected = entity.RequestEventFixSubmitted
			case "ineligible":
				expected = entity.RequestEventFixSubmissionSkipped
			}
			for i := 0; i < 3; i++ {
				m.stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
				m.observations.EXPECT().List(gomock.Any(), testBuildID, storage.BuildIssueRange{Limit: buildIssuePageSize}).Return([]entity.BuildIssue{observation}, nil)
				if decision != "associated" {
					current := storedIssue(issue)
					if decision == "ineligible" {
						current.Status = entity.IssueStatusClosed
					}
					m.issues.EXPECT().Get(gomock.Any(), issue.TaskID, issue.UnitID).Return(current, nil)
					m.observations.EXPECT().List(gomock.Any(), testBuildID, afterIssue(observation)).Return(nil, nil)
				}
				d := consumermock.NewMockDelivery(m.ctrl)
				d.EXPECT().Message().Return(repairMessage(t))
				if i == 0 {
					*m.historyFailure = assert.AnError
					require.ErrorIs(t, c.Process(context.Background(), d), assert.AnError)
					require.Empty(t, m.history)
					*m.historyFailure = nil
				} else {
					require.NoError(t, c.Process(context.Background(), d))
				}
			}
			require.Len(t, m.history, 1)
			for _, log := range m.history {
				require.Equal(t, expected, log.Event)
				require.Equal(t, testBuildID, log.Metadata[requestlog.MetadataKeyBuildID])
				if decision == "eligible" {
					require.Equal(t, "acceptance_unknown", log.Metadata[requestlog.MetadataKeyReason])
				}
			}
		})
	}
}

func TestFixDLQDiscardsInvalidMessages(t *testing.T) {
	for _, problem := range []string{"payload", "tenant", "missing build"} {
		t.Run(problem, func(t *testing.T) {
			_, m := newTestController(t)
			c := NewDLQController(zap.NewNop().Sugar(), tally.NoopScope, m.stores, m.materializer, messagequeue.TopicKeyFix, "recovery")
			msg := repairMessage(t)
			switch problem {
			case "payload":
				msg.Payload = []byte("invalid")
			case "tenant":
				msg.Tenant = "other"
			case "missing build":
				var err error
				msg.Payload, err = messagequeue.Marshal(&messagequeue.FixRequest{Id: "missing", QueueName: testQueue})
				require.NoError(t, err)
				m.stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
				m.builds.EXPECT().Get(gomock.Any(), "missing").Return(entity.Build{}, storage.ErrNotFound)
			}
			require.NoError(t, c.Process(context.Background(), m.delivery(msg)))
			require.Empty(t, m.history)
		})
	}
}

func TestSubmittedFixHistoryFitsStorageKeyWithMaximumLengthFixID(t *testing.T) {
	_, m := newTestController(t)
	observation := issueWithFixID(testIssue(), strings.Repeat("f", 255))
	request := entity.Request{ID: "1", Queue: testQueue}
	for i := 0; i < 2; i++ {
		require.NoError(t, persistFixSubmittedLog(context.Background(), m.materializer, m.store, request, testBuildID, observation))
	}
	require.Len(t, m.history, 1)
	for _, log := range m.history {
		require.LessOrEqual(t, len(log.ID), 255)
		require.Equal(t, observation.FixID, log.Metadata[requestlog.MetadataKeyFixID])
	}
}
