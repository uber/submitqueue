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
	"go.uber.org/mock/gomock"

	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/extension/storage/mock"
)

func TestGetCurrentStateFromRequestLog(t *testing.T) {
	tests := []struct {
		name     string
		logs     []entity.RequestLog
		expected CurrentState
	}{
		{
			name: "single record",
			logs: []entity.RequestLog{
				{RequestID: "q/1", TimestampMs: 1000, Status: entity.RequestStatusNew, RequestVersion: 1, LastError: "", Metadata: map[string]string{}},
			},
			expected: CurrentState{Status: entity.RequestStatusNew, LastError: "", Metadata: map[string]string{}},
		},
		{
			name: "terminal status wins over later non-terminal",
			logs: []entity.RequestLog{
				{RequestID: "q/1", TimestampMs: 1000, Status: entity.RequestStatusNew, RequestVersion: 1, LastError: "", Metadata: map[string]string{}},
				{RequestID: "q/1", TimestampMs: 2000, Status: entity.RequestStatusLanded, RequestVersion: 3, LastError: "", Metadata: map[string]string{"batch": "b1"}},
				{RequestID: "q/1", TimestampMs: 3000, Status: entity.RequestStatusProcessing, RequestVersion: 0, LastError: "", Metadata: map[string]string{}},
			},
			expected: CurrentState{Status: entity.RequestStatusLanded, LastError: "", Metadata: map[string]string{"batch": "b1"}},
		},
		{
			name: "terminal error status with last error",
			logs: []entity.RequestLog{
				{RequestID: "q/1", TimestampMs: 1000, Status: entity.RequestStatusNew, RequestVersion: 1, LastError: "", Metadata: map[string]string{}},
				{RequestID: "q/1", TimestampMs: 2000, Status: entity.RequestStatusError, RequestVersion: 4, LastError: "merge conflict", Metadata: map[string]string{"step": "merge"}},
			},
			expected: CurrentState{Status: entity.RequestStatusError, LastError: "merge conflict", Metadata: map[string]string{"step": "merge"}},
		},
		{
			name: "multiple terminal records picks highest version",
			logs: []entity.RequestLog{
				{RequestID: "q/1", TimestampMs: 1000, Status: entity.RequestStatusError, RequestVersion: 2, LastError: "timeout", Metadata: map[string]string{}},
				{RequestID: "q/1", TimestampMs: 2000, Status: entity.RequestStatusLanded, RequestVersion: 5, LastError: "", Metadata: map[string]string{"final": "true"}},
				{RequestID: "q/1", TimestampMs: 3000, Status: entity.RequestStatusError, RequestVersion: 3, LastError: "conflict", Metadata: map[string]string{}},
			},
			expected: CurrentState{Status: entity.RequestStatusLanded, LastError: "", Metadata: map[string]string{"final": "true"}},
		},
		{
			name: "same version terminal records uses timestamp tiebreaker",
			logs: []entity.RequestLog{
				{RequestID: "q/1", TimestampMs: 1000, Status: entity.RequestStatusError, RequestVersion: 3, LastError: "first", Metadata: map[string]string{}},
				{RequestID: "q/1", TimestampMs: 2000, Status: entity.RequestStatusError, RequestVersion: 3, LastError: "second", Metadata: map[string]string{}},
			},
			expected: CurrentState{Status: entity.RequestStatusError, LastError: "second", Metadata: map[string]string{}},
		},
		{
			name: "terminal status without version is not terminal",
			logs: []entity.RequestLog{
				{RequestID: "q/1", TimestampMs: 1000, Status: entity.RequestStatusLanded, RequestVersion: 0, LastError: "", Metadata: map[string]string{}},
				{RequestID: "q/1", TimestampMs: 2000, Status: entity.RequestStatusProcessing, RequestVersion: 0, LastError: "", Metadata: map[string]string{"source": "gw"}},
			},
			expected: CurrentState{Status: entity.RequestStatusProcessing, LastError: "", Metadata: map[string]string{"source": "gw"}},
		},
		{
			name: "no terminal records falls back to latest timestamp",
			logs: []entity.RequestLog{
				{RequestID: "q/1", TimestampMs: 1000, Status: entity.RequestStatusNew, RequestVersion: 1, LastError: "", Metadata: map[string]string{}},
				{RequestID: "q/1", TimestampMs: 3000, Status: entity.RequestStatusValidated, RequestVersion: 2, LastError: "", Metadata: map[string]string{}},
				{RequestID: "q/1", TimestampMs: 2000, Status: entity.RequestStatusProcessing, RequestVersion: 0, LastError: "", Metadata: map[string]string{}},
			},
			expected: CurrentState{Status: entity.RequestStatusValidated, LastError: "", Metadata: map[string]string{}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockStore := storagemock.NewMockRequestLogStore(ctrl)
			mockStore.EXPECT().List(gomock.Any(), "q/1").Return(tt.logs, nil)

			result, err := GetCurrentStateFromRequestLog(context.Background(), mockStore, "q/1")
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetCurrentStateFromRequestLog_NoRecords(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockStore := storagemock.NewMockRequestLogStore(ctrl)
	mockStore.EXPECT().List(gomock.Any(), "q/1").Return(nil, storage.ErrNotFound)

	_, err := GetCurrentStateFromRequestLog(context.Background(), mockStore, "q/1")
	assert.Error(t, err)
	assert.True(t, storage.IsNotFound(err))
}

func TestGetCurrentStateFromRequestLog_StoreError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockStore := storagemock.NewMockRequestLogStore(ctrl)
	mockStore.EXPECT().List(gomock.Any(), "q/1").Return(nil, fmt.Errorf("db connection lost"))

	_, err := GetCurrentStateFromRequestLog(context.Background(), mockStore, "q/1")
	assert.Error(t, err)
}
