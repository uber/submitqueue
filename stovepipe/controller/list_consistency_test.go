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

	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/mock/gomock"
)

func TestListRejectsInconsistentRecordsWithoutPartialResults(t *testing.T) {
	first := listTestSummary(900, "9")
	second := listTestSummary(800, "1")
	mapping := entity.RequestAcceptance{Queue: second.Queue, AcceptedAtMs: second.AcceptedAtMs, RequestID: second.RequestID}
	for _, tt := range []struct {
		name       string
		change     func(*entity.RequestSummary)
		readErr    error
		wantReason string
	}{
		{
			name: "wrong queue", change: func(summary *entity.RequestSummary) { summary.Queue = "other/main" },
			wantReason: "acceptance mapping disagrees with summary",
		},
		{
			name: "wrong request", change: func(summary *entity.RequestSummary) { summary.RequestID = first.RequestID },
			wantReason: "acceptance mapping disagrees with summary",
		},
		{
			name: "unknown acceptance", change: func(summary *entity.RequestSummary) { summary.AcceptedAtMs = 0 },
			wantReason: "acceptance mapping disagrees with summary",
		},
		{
			name: "different acceptance", change: func(summary *entity.RequestSummary) { summary.AcceptedAtMs++ },
			wantReason: "acceptance mapping disagrees with summary",
		},
		{
			name: "missing summary", readErr: fmt.Errorf("summary read failed: %w", storage.ErrNotFound),
			wantReason: "acceptance mapping has no summary",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newListTestFixture(t)
			corrupt := second
			if tt.change != nil {
				tt.change(&corrupt)
			}
			f.factory.EXPECT().For(gomock.Any()).Return(f.stores, nil)
			f.acceptances.EXPECT().List(gomock.Any(), gomock.Any()).Return([]entity.RequestAcceptance{
				{Queue: first.Queue, AcceptedAtMs: first.AcceptedAtMs, RequestID: first.RequestID}, mapping,
			}, nil)
			gomock.InOrder(
				f.summaries.EXPECT().Get(gomock.Any(), first.RequestID).Return(first, nil),
				f.summaries.EXPECT().Get(gomock.Any(), second.RequestID).Return(corrupt, tt.readErr),
			)
			result, err := f.controller.List(context.Background(), entity.ListRequest{Queue: first.Queue})
			require.True(t, IsListConsistency(err))
			require.True(t, IsListConsistency(fmt.Errorf("List failed: %w", err)))
			var consistency *ListConsistencyError
			require.ErrorAs(t, err, &consistency)
			require.Equal(t, first.Queue, consistency.Queue)
			require.Equal(t, second.RequestID, consistency.RequestID)
			require.Equal(t, tt.wantReason, consistency.Reason)
			require.Equal(t, tt.readErr, consistency.Err)
			if tt.readErr != nil {
				require.ErrorIs(t, err, tt.readErr)
				require.ErrorIs(t, err, storage.ErrNotFound)
			}
			require.False(t, errs.IsRetryable(err))
			require.False(t, errs.IsUserError(err))
			require.Equal(t, entity.ListResult{}, result)
		})
	}
}

func TestListRejectsInvalidMappingBeforeSummaryLookup(t *testing.T) {
	for _, mapping := range []entity.RequestAcceptance{
		{Queue: "other/main", AcceptedAtMs: 900, RequestID: "request/other/main/1"},
		{Queue: "monorepo/main", AcceptedAtMs: 0, RequestID: "request/monorepo/main/1"},
		{Queue: "monorepo/main", AcceptedAtMs: 900},
	} {
		t.Run(mapping.RequestID, func(t *testing.T) {
			f := newListTestFixture(t)
			f.factory.EXPECT().For(gomock.Any()).Return(f.stores, nil)
			f.acceptances.EXPECT().List(gomock.Any(), gomock.Any()).Return([]entity.RequestAcceptance{mapping}, nil)
			result, err := f.controller.List(context.Background(), entity.ListRequest{Queue: "monorepo/main"})
			require.True(t, IsListConsistency(err))
			var consistency *ListConsistencyError
			require.ErrorAs(t, err, &consistency)
			require.Equal(t, "monorepo/main", consistency.Queue)
			require.Equal(t, mapping.RequestID, consistency.RequestID)
			require.Equal(t, "invalid acceptance mapping", consistency.Reason)
			require.Nil(t, consistency.Err)
			require.False(t, errs.IsRetryable(err))
			require.False(t, errs.IsUserError(err))
			require.Equal(t, entity.ListResult{}, result)
		})
	}
}
