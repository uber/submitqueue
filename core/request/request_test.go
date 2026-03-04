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
				{RequestID: "q/1", TimestampMs: 1000, Status: "new", RequestVersion: 1, LastError: "", Metadata: map[string]string{}},
			},
			expected: CurrentState{Status: "new", LastError: "", Metadata: map[string]string{}},
		},
		{
			name: "terminal status wins over later non-terminal",
			logs: []entity.RequestLog{
				{RequestID: "q/1", TimestampMs: 1000, Status: "new", RequestVersion: 1, LastError: "", Metadata: map[string]string{}},
				{RequestID: "q/1", TimestampMs: 2000, Status: "landed", RequestVersion: 3, LastError: "", Metadata: map[string]string{"batch": "b1"}},
				{RequestID: "q/1", TimestampMs: 3000, Status: "processing", RequestVersion: 0, LastError: "", Metadata: map[string]string{}},
			},
			expected: CurrentState{Status: "landed", LastError: "", Metadata: map[string]string{"batch": "b1"}},
		},
		{
			name: "terminal error status with last error",
			logs: []entity.RequestLog{
				{RequestID: "q/1", TimestampMs: 1000, Status: "new", RequestVersion: 1, LastError: "", Metadata: map[string]string{}},
				{RequestID: "q/1", TimestampMs: 2000, Status: "error", RequestVersion: 4, LastError: "merge conflict", Metadata: map[string]string{"step": "merge"}},
			},
			expected: CurrentState{Status: "error", LastError: "merge conflict", Metadata: map[string]string{"step": "merge"}},
		},
		{
			name: "multiple terminal records picks highest version",
			logs: []entity.RequestLog{
				{RequestID: "q/1", TimestampMs: 1000, Status: "error", RequestVersion: 2, LastError: "timeout", Metadata: map[string]string{}},
				{RequestID: "q/1", TimestampMs: 2000, Status: "landed", RequestVersion: 5, LastError: "", Metadata: map[string]string{"final": "true"}},
				{RequestID: "q/1", TimestampMs: 3000, Status: "error", RequestVersion: 3, LastError: "conflict", Metadata: map[string]string{}},
			},
			expected: CurrentState{Status: "landed", LastError: "", Metadata: map[string]string{"final": "true"}},
		},
		{
			name: "same version terminal records uses timestamp tiebreaker",
			logs: []entity.RequestLog{
				{RequestID: "q/1", TimestampMs: 1000, Status: "error", RequestVersion: 3, LastError: "first", Metadata: map[string]string{}},
				{RequestID: "q/1", TimestampMs: 2000, Status: "error", RequestVersion: 3, LastError: "second", Metadata: map[string]string{}},
			},
			expected: CurrentState{Status: "error", LastError: "second", Metadata: map[string]string{}},
		},
		{
			name: "terminal status without version is not terminal",
			logs: []entity.RequestLog{
				{RequestID: "q/1", TimestampMs: 1000, Status: "landed", RequestVersion: 0, LastError: "", Metadata: map[string]string{}},
				{RequestID: "q/1", TimestampMs: 2000, Status: "processing", RequestVersion: 0, LastError: "", Metadata: map[string]string{"source": "gw"}},
			},
			expected: CurrentState{Status: "processing", LastError: "", Metadata: map[string]string{"source": "gw"}},
		},
		{
			name: "no terminal records falls back to latest timestamp",
			logs: []entity.RequestLog{
				{RequestID: "q/1", TimestampMs: 1000, Status: "new", RequestVersion: 1, LastError: "", Metadata: map[string]string{}},
				{RequestID: "q/1", TimestampMs: 3000, Status: "validated", RequestVersion: 2, LastError: "", Metadata: map[string]string{}},
				{RequestID: "q/1", TimestampMs: 2000, Status: "processing", RequestVersion: 0, LastError: "", Metadata: map[string]string{}},
			},
			expected: CurrentState{Status: "validated", LastError: "", Metadata: map[string]string{}},
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
