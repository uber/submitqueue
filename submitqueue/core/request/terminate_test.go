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

package request

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

func requestWithState(request entity.Request, state entity.RequestState) entity.Request {
	request.State = state
	return request
}

// recordingRegistry returns a registry whose publisher appends every published
// request log to *logs and returns publishErr. It lets tests assert both that a
// terminal log was (or was not) published and what version/status it carried.
func recordingRegistry(t *testing.T, ctrl *gomock.Controller, publishErr error) (consumer.TopicRegistry, *[]entity.RequestLog) {
	t.Helper()
	logs := &[]entity.RequestLog{}
	mockPub := queuemock.NewMockPublisher(ctrl)
	mockPub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, msg entityqueue.Message) error {
			log, err := entity.RequestLogFromBytes(msg.Payload)
			require.NoError(t, err)
			*logs = append(*logs, log)
			return publishErr
		},
	).AnyTimes()

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: topickey.TopicKeyLog, Name: "log", Queue: mockQ}},
	)
	require.NoError(t, err)
	return registry, logs
}

func TestTerminateRequest(t *testing.T) {
	const requestID = "q/1"

	validated := entity.Request{ID: requestID, Queue: "q", State: entity.RequestStateValidated, Version: 3}
	originalValidated := validated

	testCases := map[string]struct {
		targetState entity.RequestState
		lastError   string
		metadata    map[string]string
		mockFunc    func(rs *storagemock.MockRequestStore)
		publishErr  error
		wantResult  TerminationResult
		errMsg      string
		errIs       error
		// wantLog, when non-nil, asserts the single published terminal log.
		wantLog func(t *testing.T, log entity.RequestLog)
	}{
		"non-terminal target rejected": {
			targetState: entity.RequestStateValidated,
			mockFunc:    func(rs *storagemock.MockRequestStore) {},
			wantResult:  TerminationResult{Outcome: TerminationOutcomeUnknown},
			errMsg:      "non-terminal request state: validated",
		},
		"request not found": {
			targetState: entity.RequestStateError,
			mockFunc: func(rs *storagemock.MockRequestStore) {
				rs.EXPECT().Get(gomock.Any(), requestID).Return(entity.Request{}, storage.ErrNotFound)
			},
			wantResult: TerminationResult{Outcome: TerminationOutcomeNotFound},
		},
		"get infra error": {
			targetState: entity.RequestStateError,
			mockFunc: func(rs *storagemock.MockRequestStore) {
				rs.EXPECT().Get(gomock.Any(), requestID).Return(entity.Request{}, fmt.Errorf("db down"))
			},
			wantResult: TerminationResult{Outcome: TerminationOutcomeUnknown},
			errMsg:     "failed to get request q/1: db down",
		},
		"reconciled from non-terminal state": {
			targetState: entity.RequestStateError,
			lastError:   "boom",
			metadata:    map[string]string{"source": "validate"},
			mockFunc: func(rs *storagemock.MockRequestStore) {
				rs.EXPECT().Get(gomock.Any(), requestID).Return(validated, nil)
				rs.EXPECT().Update(gomock.Any(), requestWithState(validated, entity.RequestStateError), int32(3), int32(4)).Return(nil)
			},
			wantResult: TerminationResult{
				Outcome:     TerminationOutcomeSuccess,
				BeforeState: entity.RequestStateValidated,
				AfterState:  entity.RequestStateError,
			},
			wantLog: func(t *testing.T, log entity.RequestLog) {
				assert.Equal(t, entity.RequestStatusError, log.Status)
				assert.Equal(t, int32(4), log.RequestVersion)
				assert.Equal(t, "boom", log.LastError)
				assert.Equal(t, "validate", log.Metadata["source"])
			},
		},
		"already in target terminal state republishes log": {
			targetState: entity.RequestStateError,
			mockFunc: func(rs *storagemock.MockRequestStore) {
				already := entity.Request{ID: requestID, State: entity.RequestStateError, Version: 5}
				rs.EXPECT().Get(gomock.Any(), requestID).Return(already, nil)
			},
			wantResult: TerminationResult{
				Outcome:     TerminationOutcomeAlreadyInTargetState,
				BeforeState: entity.RequestStateError,
				AfterState:  entity.RequestStateError,
			},
			wantLog: func(t *testing.T, log entity.RequestLog) {
				assert.Equal(t, entity.RequestStatusError, log.Status)
				assert.Equal(t, int32(5), log.RequestVersion)
			},
		},
		"already in target terminal state returns republish error": {
			targetState: entity.RequestStateError,
			mockFunc: func(rs *storagemock.MockRequestStore) {
				already := entity.Request{ID: requestID, State: entity.RequestStateError, Version: 5}
				rs.EXPECT().Get(gomock.Any(), requestID).Return(already, nil)
			},
			publishErr: fmt.Errorf("connection refused"),
			wantResult: TerminationResult{Outcome: TerminationOutcomeUnknown},
			errMsg:     "failed to publish request log for q/1",
		},
		"diverged terminal state is left untouched": {
			targetState: entity.RequestStateError,
			mockFunc: func(rs *storagemock.MockRequestStore) {
				landed := entity.Request{ID: requestID, State: entity.RequestStateLanded, Version: 7}
				rs.EXPECT().Get(gomock.Any(), requestID).Return(landed, nil)
			},
			wantResult: TerminationResult{
				Outcome:     TerminationOutcomeDiverged,
				BeforeState: entity.RequestStateLanded,
				AfterState:  entity.RequestStateLanded,
			},
		},
		"update version mismatch returned as-is": {
			targetState: entity.RequestStateError,
			mockFunc: func(rs *storagemock.MockRequestStore) {
				rs.EXPECT().Get(gomock.Any(), requestID).Return(validated, nil)
				rs.EXPECT().Update(gomock.Any(), requestWithState(validated, entity.RequestStateError), int32(3), int32(4)).Return(storage.ErrVersionMismatch)
			},
			wantResult: TerminationResult{Outcome: TerminationOutcomeUnknown},
			errMsg:     "version mismatch",
			errIs:      storage.ErrVersionMismatch,
		},
		"publish error after reconcile": {
			targetState: entity.RequestStateError,
			mockFunc: func(rs *storagemock.MockRequestStore) {
				rs.EXPECT().Get(gomock.Any(), requestID).Return(validated, nil)
				rs.EXPECT().Update(gomock.Any(), requestWithState(validated, entity.RequestStateError), int32(3), int32(4)).Return(nil)
			},
			publishErr: fmt.Errorf("connection refused"),
			wantResult: TerminationResult{Outcome: TerminationOutcomeUnknown},
			errMsg:     "failed to publish request log for q/1",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			requestStore := storagemock.NewMockRequestStore(ctrl)
			store := storagemock.NewMockStorage(ctrl)
			store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()
			tc.mockFunc(requestStore)

			registry, logs := recordingRegistry(t, ctrl, tc.publishErr)

			res, err := TerminateRequest(context.Background(), store, registry, requestID, tc.targetState, tc.lastError, tc.metadata)

			assert.Equal(t, originalValidated, validated)
			assert.Equal(t, tc.wantResult, res)
			if tc.errMsg != "" {
				assert.ErrorContains(t, err, tc.errMsg)
			} else {
				assert.NoError(t, err)
			}
			if tc.errIs != nil {
				assert.ErrorIs(t, err, tc.errIs)
			}

			if tc.wantLog != nil {
				require.Len(t, *logs, 1)
				tc.wantLog(t, (*logs)[0])
			} else if tc.publishErr == nil {
				assert.Empty(t, *logs)
			}
		})
	}
}
