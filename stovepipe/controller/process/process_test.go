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

package process

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

const testID = "request/monorepo/main/7"

func newController(t *testing.T, ctrl *gomock.Controller) (*Controller, *storagemock.MockRequestStore) {
	t.Helper()
	reqStore := storagemock.NewMockRequestStore(ctrl)
	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(reqStore).AnyTimes()
	c := NewController(zap.NewNop().Sugar(), tally.NewTestScope("test", nil), store, stovepipemq.TopicKeyProcess, "stovepipe-process")
	return c, reqStore
}

// delivery wraps raw payload bytes in a MockDelivery (which satisfies consumer.Delivery).
func delivery(t *testing.T, ctrl *gomock.Controller, payload []byte) consumer.Delivery {
	t.Helper()
	d := queuemock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(entityqueue.NewMessage(testID, payload, "monorepo/main", nil)).AnyTimes()
	d.EXPECT().Attempt().Return(1).AnyTimes()
	return d
}

func processPayload(t *testing.T, id string) []byte {
	t.Helper()
	b, err := stovepipemq.Marshal(&stovepipemq.ProcessRequest{Id: id})
	require.NoError(t, err)
	return b
}

func TestProcess_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, reqStore := newController(t, ctrl)
	reqStore.EXPECT().Get(gomock.Any(), testID).Return(entity.Request{ID: testID, Queue: "monorepo/main", URI: "git://x", State: entity.RequestStateAccepted, Version: 1}, nil)

	require.NoError(t, c.Process(context.Background(), delivery(t, ctrl, processPayload(t, testID))))
}

func TestProcess_NotFoundIsRetryable(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, reqStore := newController(t, ctrl)
	reqStore.EXPECT().Get(gomock.Any(), testID).Return(entity.Request{}, storage.ErrNotFound)

	err := c.Process(context.Background(), delivery(t, ctrl, processPayload(t, testID)))
	require.Error(t, err)
	assert.True(t, errs.IsRetryable(err), "a not-yet-visible request must be retryable")
}

func TestProcess_StorageErrorNotRetryable(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, reqStore := newController(t, ctrl)
	reqStore.EXPECT().Get(gomock.Any(), testID).Return(entity.Request{}, errors.New("db down"))

	err := c.Process(context.Background(), delivery(t, ctrl, processPayload(t, testID)))
	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}

func TestProcess_MalformedPayload(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, _ := newController(t, ctrl)

	err := c.Process(context.Background(), delivery(t, ctrl, []byte("not-json")))
	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}
