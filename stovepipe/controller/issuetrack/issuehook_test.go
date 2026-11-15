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

package issuetrack

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	basehook "github.com/uber/submitqueue/api/base/hook"
	"github.com/uber/submitqueue/platform/base/failure"
	"github.com/uber/submitqueue/stovepipe/entity"
)

func TestIssueHookLifecycle(t *testing.T) {
	f := newFixture(t)
	key := issueKey{"task", "unit"}
	for i, reported := range []bool{true, true, false, false, true} {
		build := []string{"build/100", "build/101", "build/102", "build/103", "build/104"}[i]
		request := []string{"100", "101", "102", "103", "104"}[i]
		f.source(build, request, entity.BuildStatusSucceeded)
		progress := currentCursor()
		progress.BuildID, progress.RequestID = build, request
		progress.Phase, progress.Finalize = entity.IssueTrackingPhaseFinalizing, true
		progress.CursorTaskID, progress.CursorUnitID = "", ""
		if f.hasProgress {
			progress.Version = f.progress.Version
		}
		f.retain(progress)
		_, err := f.controller.processIssue(context.Background(), f.store, testQueue, progress, key, reported)
		require.NoError(t, err)
		require.Len(t, f.hookMessages, []int{1, 1, 2, 2, 3}[i])
		if i == 0 {
			issue := f.states[key]
			issue.Metadata = map[string]json.RawMessage{"notification": json.RawMessage(`{"thread":"123.456"}`)}
			issue.Version++
			f.states[key] = issue
		}
		require.JSONEq(t, `{"thread":"123.456"}`, string(f.states[key].Metadata["notification"]))
	}
	first, closed, reopened := &basehook.HookEvent{}, &basehook.HookEvent{}, &basehook.HookEvent{}
	require.NoError(t, basehook.Unmarshal(f.hookMessages[0].Payload, first))
	require.NoError(t, basehook.Unmarshal(f.hookMessages[1].Payload, closed))
	require.NoError(t, basehook.Unmarshal(f.hookMessages[2].Payload, reopened))
	require.Equal(t, "build/100", first.GetPayload().GetFields()["opening_build_id"].GetStringValue())
	require.Equal(t, "build/104", reopened.GetPayload().GetFields()["opening_build_id"].GetStringValue())
	require.NotEqual(t, first.GetId(), reopened.GetId())
	require.Equal(t, "issue.closed", closed.GetType())
	require.Equal(t, map[string]any{"queue": testQueue, "task_id": "task", "unit_id": "unit", "closing_build_id": "build/102"}, closed.GetPayload().AsMap())
	logs := f.store.GetIssueLogStore()
	closure, err := logs.Get(context.Background(), "task", "unit", "build/102", entity.IssueEventClosed)
	require.NoError(t, err)
	require.Equal(t, "build/100", closure.FirstSeenBuildID)
	opening, err := logs.Get(context.Background(), "task", "unit", closure.FirstSeenBuildID, entity.IssueEventOpened)
	require.NoError(t, err)
	require.Equal(t, "build/100", opening.BuildID)
	require.Equal(t, "build/104", f.states[key].FirstSeenBuildID)
}

func TestIssueHookRetriesAfterDurableHistory(t *testing.T) {
	for _, closed := range []bool{false, true} {
		for _, failedStep := range []string{"history/create", "hook/publish"} {
			name := "opened/" + failedStep
			if closed {
				name = "closed/" + failedStep
			}
			t.Run(name, func(t *testing.T) {
				f := newFixture(t)
				status := entity.IssueStatusOpen
				if closed {
					f.source("build/100", "100", entity.BuildStatusSucceeded)
					f.seedIssue("task", "unit", entity.IssueStatusOpen)
					status = entity.IssueStatusClosed
				} else {
					f.source("build/100", "100", entity.BuildStatusRunning)
					f.reportIssue("build/100", "task", "unit")
				}
				f.failures[failedStep] = 1
				msg := f.message("build/100", closed)
				if closed {
					progress := currentCursor()
					progress.Phase, progress.Finalize = entity.IssueTrackingPhaseFinalizing, true
					f.retain(progress)
				}
				require.ErrorIs(t, f.deliver(msg, false), assert.AnError)
				f.controller.nowMs = func() int64 { return 2000 }
				if failedStep == "history/create" {
					require.Empty(t, f.hookMessages)
				} else {
					require.Len(t, f.logs, 1)
					require.Len(t, f.hookMessages, 1)
				}
				require.NoError(t, f.deliver(msg, false))
				require.Equal(t, status, f.states[issueKey{"task", "unit"}].Status)
				require.Len(t, f.logs, 1)
				if failedStep == "hook/publish" {
					require.Len(t, f.hookMessages, 2)
					require.Equal(t, f.hookMessages[0].ID, f.hookMessages[1].ID)
					require.JSONEq(t, string(f.hookMessages[0].Payload), string(f.hookMessages[1].Payload))
				} else {
					require.Len(t, f.hookMessages, 1)
				}
			})
		}
	}
}

func TestIssueHookSkippedByDLQ(t *testing.T) {
	for _, closed := range []bool{false, true} {
		for _, failedStep := range []string{"history/create", "hook/publish"} {
			name := "opened/" + failedStep
			if closed {
				name = "closed/" + failedStep
			}
			t.Run(name, func(t *testing.T) {
				f := newFixture(t)
				status := entity.IssueStatusOpen
				if closed {
					f.source("build/100", "100", entity.BuildStatusSucceeded)
					f.seedIssue("task", "unit", entity.IssueStatusOpen)
					status = entity.IssueStatusClosed
				} else {
					f.source("build/100", "100", entity.BuildStatusRunning)
					f.reportIssue("build/100", "task", "unit")
				}
				f.failures[failedStep] = 1
				msg := f.message("build/100", closed)
				if closed {
					progress := currentCursor()
					progress.Phase, progress.Finalize = entity.IssueTrackingPhaseFinalizing, true
					f.retain(progress)
				}
				require.ErrorIs(t, f.deliver(msg, false), assert.AnError)
				attempts := len(f.hookMessages)
				f.failures["hook/publish"] = 1

				require.NoError(t, f.recoverHistory(msg, failure.Failure{}))
				require.Equal(t, entity.IssueTrackingPhaseAbandoned, f.progress.Phase)
				require.Equal(t, status, f.states[issueKey{"task", "unit"}].Status)
				require.Len(t, f.logs, 1)
				require.Len(t, f.hookMessages, attempts)
				require.Empty(t, f.fixMessages)
				require.Equal(t, 1, f.failures["hook/publish"])
				require.NoError(t, f.recoverHistory(msg, failure.Failure{}))
				require.Len(t, f.hookMessages, attempts)
			})
		}
	}
}
