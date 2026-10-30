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

package buildanalysis

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/mock/gomock"
)

func TestCompletedAnalysisHistoryPrecedesHandoffAndRepairsOnRetry(t *testing.T) {
	c, m := newTestController(t)
	issue := entity.BuildIssue{TaskID: "task", UnitID: "unit"}
	*m.historyFailure = assert.AnError
	detected := m.expectPass(entity.BuildStatusFailed, []entity.BuildIssue{issue}, nil)
	m.issues.EXPECT().Create(gomock.Any(), testBuildID, issue).Return(nil).After(detected)
	require.ErrorIs(t, c.Process(context.Background(), m.delivery(t)), assert.AnError)
	require.Empty(t, m.history)

	*m.historyFailure = nil
	for i := 0; i < 2; i++ {
		detected = m.expectPass(entity.BuildStatusFailed, []entity.BuildIssue{issue}, nil)
		saved := m.issues.EXPECT().Create(gomock.Any(), testBuildID, issue).Return(storage.ErrAlreadyExists).After(detected)
		m.expectNotification(t, true, saved).Do(func(context.Context, string, entityqueue.Message) {
			require.Len(t, m.history, 1)
		})
		require.NoError(t, c.Process(context.Background(), m.delivery(t)))
	}
	log := m.history["event/build_analysis_completed/"+testBuildID]
	require.Equal(t, entity.RequestEventBuildAnalysisCompleted, log.Event)
	require.Equal(t, map[string]string{requestlog.MetadataKeyBuildID: testBuildID, requestlog.MetadataKeyIssueCount: "1"}, log.Metadata)
	require.Equal(t, m.request.ID, log.RequestID)
	require.Empty(t, log.State)
}
