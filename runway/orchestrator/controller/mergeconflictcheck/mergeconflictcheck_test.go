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

package mergeconflictcheck

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

const (
	testID           = "test-queue/1"
	testQueue        = "test-queue"
	testPartitionKey = "test-queue"
)

func newController(t *testing.T) *Controller {
	t.Helper()
	return NewController(Params{
		Logger:        zaptest.NewLogger(t).Sugar(),
		Scope:         tally.NoopScope,
		TopicKey:      runwaymq.TopicKeyMergeConflictCheck,
		ConsumerGroup: "runway-mergeconflictcheck",
	})
}

func newDelivery(t *testing.T, ctrl *gomock.Controller, payload []byte) *queuemock.MockDelivery {
	t.Helper()
	msg := entityqueue.NewMessage(testID, payload, testPartitionKey, nil)
	d := queuemock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(msg).AnyTimes()
	d.EXPECT().Attempt().Return(1).AnyTimes()
	return d
}

func requestPayload(t *testing.T, req runwaymq.MergeRequest) []byte {
	t.Helper()
	payload, err := runwaymq.Marshal(&req)
	require.NoError(t, err)
	return payload
}

func TestNewController(t *testing.T) {
	controller := newController(t)
	require.NotNil(t, controller)
	assert.Equal(t, runwaymq.TopicKeyMergeConflictCheck, controller.TopicKey())
	assert.Equal(t, "runway-mergeconflictcheck", controller.ConsumerGroup())
	assert.Equal(t, "merge-conflict-check", controller.Name())
}

func TestProcess_LogsParsedRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller := newController(t)

	req := runwaymq.MergeRequest{
		Id:        testID,
		QueueName: testQueue,
		Steps:     []*runwaymq.MergeStep{{StepId: "step-1"}},
	}
	delivery := newDelivery(t, ctrl, requestPayload(t, req))

	require.NoError(t, controller.Process(context.Background(), delivery))
}

func TestProcess_DeserializeError(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller := newController(t)

	delivery := newDelivery(t, ctrl, []byte(`{"id": not json}`))

	require.Error(t, controller.Process(context.Background(), delivery))
}
