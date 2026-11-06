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

func TestIssueOpeningHookLifecycle(t *testing.T) {
	f := newFixture(t)
	key := issueKey{"task", "unit"}
	for i, reported := range []bool{true, true, false, true} {
		build := []string{"build/100", "build/101", "build/102", "build/103"}[i]
		request := []string{"100", "101", "102", "103"}[i]
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
		require.Len(t, f.hookMessages, []int{1, 1, 1, 2}[i])
		if i == 0 {
			issue := f.states[key]
			issue.Metadata = map[string]json.RawMessage{"notification": json.RawMessage(`{"thread":"123.456"}`)}
			issue.Version++
			f.states[key] = issue
		}
		require.JSONEq(t, `{"thread":"123.456"}`, string(f.states[key].Metadata["notification"]))
	}
	first, reopened := &basehook.HookEvent{}, &basehook.HookEvent{}
	require.NoError(t, basehook.Unmarshal(f.hookMessages[0].Payload, first))
	require.NoError(t, basehook.Unmarshal(f.hookMessages[1].Payload, reopened))
	require.Equal(t, "build/100", first.GetPayload().GetFields()["opening_build_id"].GetStringValue())
	require.Equal(t, "build/103", reopened.GetPayload().GetFields()["opening_build_id"].GetStringValue())
	require.NotEqual(t, first.GetId(), reopened.GetId())
}

func TestIssueOpeningHookRetriesAfterDurableHistory(t *testing.T) {
	for _, failedStep := range []string{"history/create", "hook/publish"} {
		t.Run(failedStep, func(t *testing.T) {
			f := newFixture(t)
			f.source("build/100", "100", entity.BuildStatusRunning)
			f.reportIssue("build/100", "task", "unit")
			f.failures[failedStep] = 1
			msg := f.message("build/100", false)
			require.ErrorIs(t, f.deliver(msg, false), assert.AnError)
			f.controller.nowMs = func() int64 { return 2000 }
			if failedStep == "history/create" {
				require.Empty(t, f.hookMessages)
			} else {
				require.Len(t, f.logs, 1)
				require.Len(t, f.hookMessages, 1)
			}
			require.NoError(t, f.deliver(msg, false))
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

func TestIssueOpeningHookSkippedByDLQ(t *testing.T) {
	for _, failedStep := range []string{"history/create", "hook/publish"} {
		t.Run(failedStep, func(t *testing.T) {
			f := newFixture(t)
			f.source("build/100", "100", entity.BuildStatusRunning)
			f.reportIssue("build/100", "task", "unit")
			f.failures[failedStep] = 1
			msg := f.message("build/100", false)
			require.ErrorIs(t, f.deliver(msg, false), assert.AnError)
			attempts := len(f.hookMessages)
			f.failures["hook/publish"] = 1

			require.NoError(t, f.recoverHistory(msg, failure.Failure{}))
			require.Equal(t, entity.IssueTrackingPhaseAbandoned, f.progress.Phase)
			require.Equal(t, entity.IssueStatusOpen, f.states[issueKey{"task", "unit"}].Status)
			require.Len(t, f.logs, 1)
			require.Len(t, f.hookMessages, attempts)
			require.Empty(t, f.fixMessages)
			require.Equal(t, 1, f.failures["hook/publish"])
			require.NoError(t, f.recoverHistory(msg, failure.Failure{}))
			require.Len(t, f.hookMessages, attempts)
		})
	}
}
