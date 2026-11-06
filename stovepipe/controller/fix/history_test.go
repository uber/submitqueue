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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	"github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func TestFixDLQRetainsDecisionWithoutSubmitting(t *testing.T) {
	for _, tt := range []struct {
		name        string
		fixID       string
		issueStatus entity.IssueStatus
		wantEvent   entity.RequestEvent
		wantReason  string
	}{
		{name: "associated", fixID: "fix/1", wantEvent: entity.RequestEventFixSubmitted},
		{name: "eligible", issueStatus: entity.IssueStatusOpen, wantEvent: entity.RequestEventFixSubmissionAbandoned, wantReason: "acceptance_unknown"},
		{name: "ineligible", issueStatus: entity.IssueStatusClosed, wantEvent: entity.RequestEventFixSubmissionSkipped, wantReason: "no_eligible_issue"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, m := newTestController(t)
			c := NewDLQController(zap.NewNop().Sugar(), tally.NoopScope, m.stores, m.materializer, messagequeue.TopicKeyFix, "recovery")
			issue := testIssue()
			observation := issueIdentity(issue)
			observation.FixID = tt.fixID
			for i := 0; i < 3; i++ {
				m.stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
				m.observations.EXPECT().List(gomock.Any(), testBuildID, storage.BuildIssueRange{Limit: buildIssuePageSize}).Return([]entity.BuildIssue{observation}, nil)
				if tt.fixID == "" {
					current := storedIssue(issue)
					current.Status = tt.issueStatus
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
				require.Equal(t, tt.wantEvent, log.Event)
				require.Equal(t, testBuildID, log.Metadata[requestlog.MetadataKeyBuildID])
				require.Equal(t, tt.wantReason, log.Metadata[requestlog.MetadataKeyReason])
			}
		})
	}
}

func TestFixDLQDiscardsInvalidMessages(t *testing.T) {
	for _, tt := range []struct {
		name      string
		configure func(*testing.T, controllerMocks) entityqueue.Message
	}{
		{name: "payload", configure: func(t *testing.T, _ controllerMocks) entityqueue.Message {
			msg := repairMessage(t)
			msg.Payload = []byte("invalid")
			return msg
		}},
		{name: "tenant", configure: func(t *testing.T, _ controllerMocks) entityqueue.Message {
			msg := repairMessage(t)
			msg.Tenant = "other"
			return msg
		}},
		{name: "missing build", configure: func(t *testing.T, m controllerMocks) entityqueue.Message {
			msg := repairMessage(t)
			var err error
			msg.Payload, err = messagequeue.Marshal(&messagequeue.FixRequest{Id: "missing", QueueName: testQueue})
			require.NoError(t, err)
			m.stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
			m.builds.EXPECT().Get(gomock.Any(), "missing").Return(entity.Build{}, storage.ErrNotFound)
			return msg
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, m := newTestController(t)
			c := NewDLQController(zap.NewNop().Sugar(), tally.NoopScope, m.stores, m.materializer, messagequeue.TopicKeyFix, "recovery")
			require.NoError(t, c.Process(context.Background(), m.delivery(tt.configure(t, m))))
			require.Empty(t, m.history)
		})
	}
}
