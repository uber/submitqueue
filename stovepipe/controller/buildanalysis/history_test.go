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
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/mock/gomock"
)

func TestCompletedAnalysisHistoryRepairsOnRetry(t *testing.T) {
	for _, tt := range []struct {
		name          string
		firstCount    int
		retryCount    int
		wantCount     int
		waitForWinner bool
		failHistory   bool
	}{
		{name: "empty analysis", wantCount: 0},
		{name: "idempotent replay", firstCount: 1, retryCount: 1, wantCount: 1},
		{name: "winner recorded after held pass", firstCount: 1, retryCount: 2, wantCount: 2, waitForWinner: true},
		{name: "history failure", firstCount: 1, retryCount: 2, wantCount: 2, failHistory: true},
		{name: "replay with fewer issues", firstCount: 2, retryCount: 1, wantCount: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, m := newTestController(t)
			persistedIssues := map[string]bool{}
			var retainedTimestamp int64
			for pass := 0; pass < 3; pass++ {
				count := tt.retryCount
				if pass == 0 {
					count = tt.firstCount
				}
				issues := make([]entity.BuildIssue, count)
				for i := range issues {
					issues[i] = entity.BuildIssue{TaskID: "task", UnitID: fmt.Sprintf("unit-%d", i)}
				}
				detected := m.expectPass(entity.BuildStatusFailed, issues, nil)
				last := detected
				for _, issue := range issues {
					last = m.issues.EXPECT().Create(gomock.Any(), testBuildID, issue).
						DoAndReturn(func(_ context.Context, _ string, observed entity.BuildIssue) error {
							if persistedIssues[observed.UnitID] {
								return storage.ErrAlreadyExists
							}
							persistedIssues[observed.UnitID] = true
							return nil
						}).After(last)
				}
				waiting := pass == 0 && tt.waitForWinner
				historyFailure := pass == 0 && tt.failHistory
				if waiting {
					m.request.TerminalBuildID = ""
					m.request.State = entity.RequestStateProcessing
				}
				if historyFailure {
					*m.historyFailure = assert.AnError
				} else {
					*m.historyFailure = nil
				}
				d := m.delivery(t)
				if waiting {
					d.EXPECT().Hold(PollDelayMs).After(last)
				}
				err := c.Process(context.Background(), d)
				if historyFailure {
					require.ErrorIs(t, err, assert.AnError)
				} else {
					require.NoError(t, err)
				}
				if waiting || historyFailure {
					require.Empty(t, m.history)
					continue
				}
				require.Len(t, m.history, 1)
				log := m.history["event/build_analysis_completed/"+testBuildID]
				require.Equal(t, entity.RequestEventBuildAnalysisCompleted, log.Event)
				require.Equal(t, map[string]string{
					requestlog.MetadataKeyBuildID:    testBuildID,
					requestlog.MetadataKeyIssueCount: strconv.Itoa(tt.wantCount),
				}, log.Metadata)
				require.Equal(t, m.request.ID, log.RequestID)
				if retainedTimestamp == 0 {
					retainedTimestamp = log.TimestampMs
				}
				require.Positive(t, retainedTimestamp)
				require.Equal(t, retainedTimestamp, log.TimestampMs)
			}
			require.Len(t, persistedIssues, max(tt.firstCount, tt.retryCount))
		})
	}
}

func TestCompletedAnalysisHistoryRetainsConcurrentSnapshot(t *testing.T) {
	for _, tt := range []struct {
		name      string
		createErr error
		staleRead bool
	}{
		{name: "concurrent delivery", createErr: storage.ErrAlreadyExists},
		{name: "uncertain write", createErr: assert.AnError},
		{name: "stale initial read", createErr: storage.ErrAlreadyExists, staleRead: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, m := newTestController(t)
			retained := requestlog.NewRequestEventLog(*m.request, entity.RequestEventBuildAnalysisCompleted, testBuildID,
				map[string]string{requestlog.MetadataKeyBuildID: testBuildID, requestlog.MetadataKeyIssueCount: "1"})
			retained.TimestampMs = 123
			if tt.staleRead {
				m.history[retained.ID] = retained
				stale := m.historyStore.EXPECT().Get(gomock.Any(), m.request.ID, retained.ID).
					Return(entity.RequestLog{}, storage.ErrNotFound)
				m.historyRead.After(stale)
			}
			created := m.historyStore.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, entity.RequestLog) error {
				m.history[retained.ID] = retained
				return tt.createErr
			})
			m.historyCreate.After(created)
			m.expectPass(entity.BuildStatusFailed, nil, nil)
			require.NoError(t, c.Process(context.Background(), m.delivery(t)))
			require.Len(t, m.history, 1)
			require.Equal(t, retained, m.history[retained.ID])
		})
	}
}

func TestCompletedAnalysisHistoryRejectsStorageAndIdentityFailures(t *testing.T) {
	for _, tt := range []struct {
		name    string
		readErr error
		buildID string
	}{
		{name: "history read failure", readErr: assert.AnError},
		{name: "conflicting build identity", buildID: "another-build"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, m := newTestController(t)
			log := requestlog.NewRequestEventLog(*m.request, entity.RequestEventBuildAnalysisCompleted, testBuildID,
				map[string]string{requestlog.MetadataKeyBuildID: tt.buildID, requestlog.MetadataKeyIssueCount: "3"})
			log.TimestampMs = 123
			if tt.readErr != nil {
				read := m.historyStore.EXPECT().Get(gomock.Any(), m.request.ID, log.ID).Return(entity.RequestLog{}, tt.readErr)
				m.historyRead.After(read)
			} else {
				m.history[log.ID] = log
			}
			m.expectPass(entity.BuildStatusFailed, nil, nil)
			err := c.Process(context.Background(), m.delivery(t))
			require.Error(t, err)
			if tt.readErr != nil {
				require.ErrorIs(t, err, tt.readErr)
			} else {
				require.Equal(t, log, m.history[log.ID])
			}
		})
	}
}
