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

package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/submitqueue/entity"
	basestorage "github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/mock/gomock"
)

func TestList_ConsistencyErrors(t *testing.T) {
	receipt := entity.RequestReceipt{Queue: "q", RequestID: "1", ReceivedAtMs: 100}
	summary := entity.RequestSummary{Queue: "q", RequestID: "1", ReceivedAtMs: 100, Status: entity.RequestStatusAccepted}
	tests := []struct {
		name          string
		changeReceipt func(*entity.RequestReceipt)
		changeSummary func(*entity.RequestSummary)
		readErr       error
	}{
		{name: "receipt from another queue", changeReceipt: func(r *entity.RequestReceipt) { r.Queue = "other" }},
		{name: "receipt has no request ID", changeReceipt: func(r *entity.RequestReceipt) { r.RequestID = "" }},
		{name: "receipt has no time", changeReceipt: func(r *entity.RequestReceipt) { r.ReceivedAtMs = 0 }},
		{name: "receipt has negative time", changeReceipt: func(r *entity.RequestReceipt) { r.ReceivedAtMs = -1 }},
		{name: "missing summary", readErr: fmt.Errorf("get summary: %w", basestorage.ErrNotFound)},
		{name: "summary from another queue", changeSummary: func(s *entity.RequestSummary) { s.Queue = "other" }},
		{name: "summary for another request", changeSummary: func(s *entity.RequestSummary) { s.RequestID = "2" }},
		{name: "summary has another receipt time", changeSummary: func(s *entity.RequestSummary) { s.ReceivedAtMs = 101 }},
		{name: "hidden accepting summary", changeSummary: func(s *entity.RequestSummary) { s.Status = entity.RequestStatusAccepting }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newListTestFixture(t)
			badReceipt, badSummary := receipt, summary
			if tt.changeReceipt != nil {
				tt.changeReceipt(&badReceipt)
			} else {
				if tt.changeSummary != nil {
					tt.changeSummary(&badSummary)
				}
				f.summaries.EXPECT().Get(gomock.Any(), "1").Return(badSummary, tt.readErr)
			}
			f.queueConfigs.EXPECT().Get(gomock.Any(), "q").Return(entity.QueueConfig{}, nil)
			f.receipts.EXPECT().List(gomock.Any(), gomock.Any()).Return([]entity.RequestReceipt{badReceipt}, nil)

			result, err := f.controller.List(context.Background(), entity.ListRequest{Queue: "q", ReceivedAtOrAfterMs: 1, ReceivedBeforeMs: 200})

			require.Error(t, err)
			assert.True(t, IsInternalConsistency(err))
			assert.False(t, errs.IsUserError(err))
			assert.Equal(t, entity.ListResult{}, result)
		})
	}
}

func TestList_DoesNotReturnPartialPage(t *testing.T) {
	f := newListTestFixture(t)
	f.queueConfigs.EXPECT().Get(gomock.Any(), "q").Return(entity.QueueConfig{}, nil)
	f.receipts.EXPECT().List(gomock.Any(), gomock.Any()).Return([]entity.RequestReceipt{
		{Queue: "q", RequestID: "2", ReceivedAtMs: 100},
		{Queue: "q", RequestID: "1", ReceivedAtMs: 90},
	}, nil)
	f.summaries.EXPECT().Get(gomock.Any(), "2").Return(entity.RequestSummary{
		Queue: "q", RequestID: "2", ReceivedAtMs: 100, Status: entity.RequestStatusAccepted,
	}, nil)
	f.summaries.EXPECT().Get(gomock.Any(), "1").Return(entity.RequestSummary{}, basestorage.ErrNotFound)

	result, err := f.controller.List(context.Background(), entity.ListRequest{Queue: "q", ReceivedAtOrAfterMs: 1, ReceivedBeforeMs: 200})

	assert.True(t, IsInternalConsistency(err))
	assert.Equal(t, entity.ListResult{}, result)
}
