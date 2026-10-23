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
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	"github.com/uber/submitqueue/platform/errs"
	mq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"go.uber.org/mock/gomock"
)

func repairMessage(t *testing.T) entityqueue.Message {
	t.Helper()
	payload, err := mq.Marshal(&mq.FixRequest{Id: testBuildID, QueueName: testQueue})
	require.NoError(t, err)
	message := entityqueue.NewMessage(testBuildID, payload, testBuildID, nil)
	message.Tenant = testQueue
	return message
}

func (m controllerMocks) delivery(message entityqueue.Message) *consumermock.MockDelivery {
	delivery := consumermock.NewMockDelivery(m.ctrl)
	delivery.EXPECT().Message().Return(message)
	return delivery
}

func TestProcessSubmitsBuildIssue(t *testing.T) {
	controller, m := newTestController(t)
	issue := testIssue()
	m.expectIssue(issue)
	m.executor.EXPECT().Submit(gomock.Any(), issue).Return("attempt", nil)
	m.observations.EXPECT().Update(gomock.Any(), testBuildID, issueWithFixID(issue, "attempt"), int32(0), int32(1)).Return(nil)

	require.NoError(t, controller.Process(context.Background(), m.delivery(repairMessage(t))))
}

func TestProcessRejectsInvalidMessagesBeforeListingIssues(t *testing.T) {
	for _, kind := range []string{"invalid JSON", "empty build", "empty queue", "empty tenant", "wrong tenant", "wrong metadata"} {
		t.Run(kind, func(t *testing.T) {
			controller, m := newTestController(t)
			message := repairMessage(t)
			switch kind {
			case "invalid JSON":
				message.Payload = []byte("{")
			case "empty build":
				message.Payload = []byte(`{"queue_name":"validation/main"}`)
			case "empty queue":
				message.Payload = []byte(`{"id":"Build/42 "}`)
			case "empty tenant":
				message.Tenant = ""
			case "wrong tenant":
				message.Tenant = "other/main"
			case "wrong metadata":
				message.Metadata[entityqueue.MetadataKeyQueueName] = "other/main"
			}

			err := controller.Process(context.Background(), m.delivery(message))
			require.Error(t, err)
			assert.False(t, errs.IsRetryable(err))
		})
	}
}
