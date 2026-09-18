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

package record

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/platform/base/failure"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	"github.com/uber/submitqueue/platform/errs"
	requestlogmock "github.com/uber/submitqueue/stovepipe/core/requestlog/mock"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/mock/gomock"
)

func TestDLQControllerSkipsDeadLetteredPromotion(t *testing.T) {
	ctrl := gomock.NewController(t)
	inner, mocks := newControllerForTopic(t, ctrl, consumer.TopicKey("record_dlq"), "stovepipe-record-dlq")
	c := NewDLQController(inner)
	request := requestWithState(entity.RequestStateSucceeded)
	mocks.reqStore.EXPECT().Get(gomock.Any(), testID).Return(request, nil)
	expectPromotionFailedHistory(t, ctrl, inner, mocks, false)

	delivery := newDLQDelivery(t, ctrl, 1, failure.Failure{
		Message: "permission denied",
		Detail:  map[string]any{failureDetailKeyRecordStage: failureRecordStagePromotion},
	})

	require.NoError(t, c.Process(queueContext(), delivery))
	assert.NotContains(t, mocks.metricsScope.Snapshot().Counters(), "record_dlq_controller.record.promotions+queue=monorepo/main")
	assertAbandonedPromotionCount(t, mocks, "dead_lettered")
}

func TestDLQControllerAcknowledgesNewPromotionFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	inner, mocks := newControllerForTopic(t, ctrl, consumer.TopicKey("record_dlq"), "stovepipe-record-dlq")
	c := NewDLQController(inner)
	expectGreenPromotionReplay(mocks, errors.New("unavailable"))
	mocks.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateSucceeded), nil)
	expectPromotionFailedHistory(t, ctrl, inner, mocks, true)
	delivery := newDLQDelivery(t, ctrl, 1, failure.Failure{})

	require.NoError(t, c.Process(queueContext(), delivery))
	assertAbandonedPromotionCount(t, mocks, "reconciliation_failed")
}

func TestDLQControllerLeavesOtherFailuresToDLQPolicy(t *testing.T) {
	ctrl := gomock.NewController(t)
	inner, _ := newControllerForTopic(t, ctrl, consumer.TopicKey("record_dlq"), "stovepipe-record-dlq")
	c := NewDLQController(inner)
	delivery := newDLQDeliveryWithPayload(ctrl, 1, []byte("not protobuf json"), failure.Failure{})

	require.Error(t, c.Process(queueContext(), delivery))
}

func TestDLQControllerPreservesPromotionAttributionWhenHistoryFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	inner, mocks := newControllerForTopic(t, ctrl, consumer.TopicKey("record_dlq"), "stovepipe-record-dlq")
	c := NewDLQController(inner)
	mocks.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateSucceeded), nil)
	materializer := requestlogmock.NewMockMaterializer(ctrl)
	inner.materializer = materializer
	materializer.EXPECT().PersistLog(gomock.Any(), mocks.store, gomock.Any()).Return(errors.New("db down"))
	delivery := newDLQDelivery(t, ctrl, 1, failure.Failure{
		Message: "permission denied",
		Detail:  map[string]any{failureDetailKeyRecordStage: failureRecordStagePromotion},
	})

	err := c.Process(queueContext(), delivery)
	require.Error(t, err)
	assert.Equal(t, failureRecordStagePromotion, errs.Attribution(err).Detail[failureDetailKeyRecordStage])
}

func expectPromotionFailedHistory(t *testing.T, ctrl *gomock.Controller, controller *Controller, mocks recordMocks, includeValidationFact bool) {
	t.Helper()
	materializer := requestlogmock.NewMockMaterializer(ctrl)
	controller.materializer = materializer
	var calls []any
	if includeValidationFact {
		calls = append(calls, materializer.EXPECT().PersistLog(gomock.Any(), mocks.store, gomock.Any()).DoAndReturn(
			func(_ context.Context, _ storage.Storage, log entity.RequestLog) error {
				assert.Equal(t, entity.RequestEventValidationFactRecorded, log.Event)
				return nil
			},
		))
	}
	calls = append(calls, materializer.EXPECT().PersistLog(gomock.Any(), mocks.store, gomock.Any()).DoAndReturn(
		func(_ context.Context, _ storage.Storage, log entity.RequestLog) error {
			assert.Equal(t, entity.RequestEventPromotionFailed, log.Event)
			assert.Equal(t, "event/promotion_failed/repository", log.ID)
			assert.Equal(t, testID, log.RequestID)
			assert.Empty(t, log.State)
			assert.Empty(t, log.Metadata)
			return nil
		},
	))
	gomock.InOrder(calls...)
}

func expectGreenPromotionReplay(mocks recordMocks, promotionErr error) {
	mocks.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateSucceeded), nil)
	mocks.factStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(storage.ErrAlreadyExists)
	mocks.factStore.EXPECT().Get(gomock.Any(), testURI, wholeRepositoryProject).Return(entity.ValidationFact{
		URI:       testURI,
		Degree:    entity.DegreeGreen,
		RequestID: testID,
	}, nil)
	mocks.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(queueRow(testURI, testID, 3), nil)
	mocks.sourceControl.EXPECT().Promote(gomock.Any(), testURI).Return(promotionErr)
}

func newDLQDelivery(t *testing.T, ctrl *gomock.Controller, attempt int, originalFailure failure.Failure) *consumermock.MockDelivery {
	t.Helper()
	return newDLQDeliveryWithPayload(ctrl, attempt, recordPayload(t, testID), originalFailure)
}

func newDLQDeliveryWithPayload(ctrl *gomock.Controller, attempt int, payload []byte, originalFailure failure.Failure) *consumermock.MockDelivery {
	delivery := consumermock.NewMockDelivery(ctrl)
	msg := entityqueue.NewMessage(testID, payload, testID, nil)
	msg.Tenant = testQueue
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(attempt).AnyTimes()
	delivery.EXPECT().Failure().Return(originalFailure, true).AnyTimes()
	return delivery
}

func assertAbandonedPromotionCount(t *testing.T, mocks recordMocks, reason string) {
	t.Helper()
	counter, ok := mocks.metricsScope.Snapshot().Counters()["record_dlq_controller.record.promotions_abandoned+queue=monorepo/main,reason="+reason]
	require.True(t, ok)
	assert.EqualValues(t, 1, counter.Value())
}
