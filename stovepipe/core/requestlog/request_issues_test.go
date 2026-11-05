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

package requestlog

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

func TestRepairEventReplayRepairsProjectionAfterPartialFailure(t *testing.T) {
	m, stores, logs, _, _ := newTestMaterializer(t)
	summaries := storagemock.NewMockRequestIssueSummaryStore(gomock.NewController(t))
	stores.EXPECT().GetRequestIssueSummaryStore().Return(summaries).AnyTimes()
	log := NewRequestEventLog(entity.Request{ID: testRequestID, Queue: testQueue}, entity.RequestEventFixSubmitted, "fix/1",
		map[string]string{MetadataKeyBuildID: "build/1", MetadataKeyTaskID: "test", MetadataKeyUnitID: "unit", MetadataKeyFixID: "fix/1"})
	log.TimestampMs = testNowMs
	expected := entity.RequestIssueSummary{RequestID: testRequestID, BuildID: "build/1", TaskID: "test", UnitID: "unit", FixID: "fix/1", Version: 1}
	gomock.InOrder(
		logs.EXPECT().Create(gomock.Any(), log).Return(nil),
		summaries.EXPECT().Get(gomock.Any(), testRequestID, "build/1", "test", "unit").Return(entity.RequestIssueSummary{}, storage.ErrNotFound),
		summaries.EXPECT().Create(gomock.Any(), expected).Return(assert.AnError),
		logs.EXPECT().Create(gomock.Any(), log).Return(storage.ErrAlreadyExists),
		logs.EXPECT().Get(gomock.Any(), testRequestID, log.ID).Return(log, nil),
		summaries.EXPECT().Get(gomock.Any(), testRequestID, "build/1", "test", "unit").Return(entity.RequestIssueSummary{}, storage.ErrNotFound),
		summaries.EXPECT().Create(gomock.Any(), expected).Return(nil),
	)
	require.ErrorIs(t, m.PersistLog(context.Background(), stores, log), assert.AnError)
	require.NoError(t, m.PersistLog(context.Background(), stores, log))
}
