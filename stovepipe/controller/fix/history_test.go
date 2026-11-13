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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func TestFixDLQRetainsAllDispositionsWithoutSubmitting(t *testing.T) {
	c, m := newTestController(t)
	issue := testIssue()
	m.expectIssue(issue)
	m.repairs[issue] = map[string]entity.IssueRepair{"first": {Issue: issue, ExecutorID: "first", FixID: "fix"}}
	dlq := NewDLQController(c.logger, tally.NoopScope, m.stores, m.materializer, m.executors, c.topicKey, c.consumerGroup)
	require.NoError(t, dlq.Process(context.Background(), m.delivery(repairMessage(t))))
	require.Len(t, m.history, 1)
	require.Empty(t, *m.hookAttempts)
}

func TestFixDLQRetainsUnknownAcceptance(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(fmt.Sprint(pending), func(t *testing.T) {
			c, m := newTestController(t)
			issue := testIssue()
			m.expectIssue(issue)
			if pending {
				m.repairs[issue] = map[string]entity.IssueRepair{"first": {Issue: issue, ExecutorID: "first", Version: 1}}
			}
			dlq := NewDLQController(c.logger, tally.NoopScope, m.stores, m.materializer, m.executors, c.topicKey, c.consumerGroup)
			require.NoError(t, dlq.Process(context.Background(), m.delivery(repairMessage(t))))
			for _, log := range m.history {
				require.Equal(t, entity.RequestEventFixSubmissionAbandoned, log.Event)
			}
			require.Len(t, m.history, 1)
			require.Empty(t, *m.hookAttempts)
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
			c := NewDLQController(zap.NewNop().Sugar(), tally.NoopScope, m.stores, m.materializer, m.executors, messagequeue.TopicKeyFix, "recovery")
			require.NoError(t, c.Process(context.Background(), m.delivery(tt.configure(t, m))))
			require.Empty(t, m.history)
		})
	}
}
