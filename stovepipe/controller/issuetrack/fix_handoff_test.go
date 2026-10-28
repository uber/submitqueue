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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
)

func TestOpeningPassPublishesOneBuildRepairRequest(t *testing.T) {
	for _, tt := range []struct {
		name   string
		status entity.BuildStatus
		final  bool
	}{
		{name: "running refresh", status: entity.BuildStatusRunning},
		{name: "terminal opening pass", status: entity.BuildStatusFailed, final: true},
		{name: "canceled build", status: entity.BuildStatusCancelled, final: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			f.source("build", "10", tt.status)
			f.seedIssue("reopened", "unit", entity.IssueStatusClosed)
			for _, task := range []string{"new", "reopened"} {
				f.reportIssue("build", task, "unit")
			}
			f.onFixPublish = func(msg entityqueue.Message) {
				require.Equal(t, entity.IssueTrackingPhaseRefreshing, f.progress.Phase)
				for _, task := range []string{"new", "reopened"} {
					require.Equal(t, entity.IssueStatusOpen, f.states[issueKey{task, "unit"}].Status)
					require.Contains(t, f.logs, logKey{issueKey{task, "unit"}, "build", entity.IssueEventOpened})
				}
				payload := &stovepipemq.FixRequest{}
				require.NoError(t, stovepipemq.Unmarshal(msg.Payload, payload))
				require.Equal(t, "build", payload.GetId())
				require.Equal(t, testQueue, payload.GetQueueName())
				require.Equal(t, testQueue, msg.Tenant)
				require.Equal(t, "build", msg.PartitionKey)
			}
			msg := f.message("build", tt.final)
			closing := tt.final && tt.status != entity.BuildStatusCancelled
			require.NoError(t, f.deliver(msg, closing))
			require.Len(t, f.fixMessages, 1)
			if closing {
				require.NoError(t, f.deliver(msg, false))
				require.Len(t, f.fixMessages, 1)
			}
			require.NoError(t, f.deliver(msg, false))
			require.Len(t, f.fixMessages, 1)
		})
	}
}

func TestRepairHandoffSkipsEmptyPassAndPublishesLaterPasses(t *testing.T) {
	for _, tt := range []struct {
		name             string
		reportExisting   bool
		initialPublishes int
	}{
		{name: "empty pass"},
		{name: "previously open issues", reportExisting: true, initialPublishes: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			f.source("build", "10", entity.BuildStatusRunning)
			f.seedIssue("existing", "unit", entity.IssueStatusOpen)
			if tt.reportExisting {
				f.reportIssue("build", "existing", "unit")
			}
			require.NoError(t, f.deliver(f.message("build", false), false))
			require.Len(t, f.fixMessages, tt.initialPublishes)
			f.reportIssue("build", "new", "unit")
			require.NoError(t, f.deliver(f.message("build", false), false))
			require.Len(t, f.fixMessages, tt.initialPublishes+1)
			if tt.initialPublishes != 0 {
				require.NotEqual(t, f.fixMessages[0].ID, f.fixMessages[1].ID)
				require.Equal(t, f.fixMessages[0].PartitionKey, f.fixMessages[1].PartitionKey)
			}
		})
	}
}

func TestRepairHandoffWaitsForAllOpeningBatches(t *testing.T) {
	for _, issueCount := range []int{_issueBatchSize, _issueBatchSize + 1} {
		t.Run(fmt.Sprintf("%d issues", issueCount), func(t *testing.T) {
			f := newFixture(t)
			f.source("build", "10", entity.BuildStatusRunning)
			f.reportIssue("build", "a-new", "unit")
			for i := 1; i < issueCount; i++ {
				unit := fmt.Sprintf("unit/%03d", i)
				f.seedIssue("existing", unit, entity.IssueStatusOpen)
				f.reportIssue("build", "existing", unit)
			}
			msg := f.message("build", false)
			require.NoError(t, f.deliver(msg, true))
			require.Empty(t, f.fixMessages)

			f.onFixPublish = func(entityqueue.Message) {
				for _, issue := range f.states {
					require.Equal(t, "build", issue.LastProcessedBuildID)
				}
			}
			require.NoError(t, f.deliver(msg, false))
			require.Len(t, f.fixMessages, 1)
		})
	}
}

func TestRepairHandoffRetriesBeforeCompletingPass(t *testing.T) {
	for _, tt := range []struct {
		name             string
		failure          string
		failOnPhase      entity.IssueTrackingPhase
		initialPublishes int
	}{
		{name: "issue write", failure: "issue/create"},
		{name: "opening history", failure: "history/create"},
		{name: "repair publication", failure: "fix/publish", initialPublishes: 1},
		{name: "completion checkpoint", failOnPhase: entity.IssueTrackingPhaseRefreshed, initialPublishes: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			f.source("build", "10", entity.BuildStatusRunning)
			f.reportIssue("build", "new", "unit")
			f.failures[tt.failure] = 1
			f.failOnPhase = tt.failOnPhase
			msg := f.message("build", false)
			require.ErrorIs(t, f.deliver(msg, false), assert.AnError)
			require.Equal(t, entity.IssueTrackingPhaseRefreshing, f.progress.Phase)
			require.Len(t, f.fixMessages, tt.initialPublishes)

			require.NoError(t, f.deliver(msg, false))
			require.Equal(t, entity.IssueTrackingPhaseRefreshed, f.progress.Phase)
			require.Len(t, f.fixMessages, tt.initialPublishes+1)
			require.Len(t, f.logs, 1)
			if tt.initialPublishes != 0 {
				require.Equal(t, f.fixMessages[0].ID, f.fixMessages[1].ID)
				require.Equal(t, f.fixMessages[0].Payload, f.fixMessages[1].Payload)
			}
		})
	}
}

func TestRepairHandoffUsesResumedBuildAndPassIdentity(t *testing.T) {
	f := newFixture(t)
	f.source("build", "10", entity.BuildStatusRunning)
	f.source("next-build", "11", entity.BuildStatusRunning)
	f.reportIssue("build", "new", "unit")
	f.failures["fix/publish"] = 1
	msg := f.message("build", false)
	require.ErrorIs(t, f.deliver(msg, false), assert.AnError)

	nextMessage := f.message("next-build", false)
	require.NoError(t, f.deliver(nextMessage, true))
	require.Equal(t, "build", f.progress.BuildID)
	require.Len(t, f.fixMessages, 2)
	require.Equal(t, f.fixMessages[0].ID, f.fixMessages[1].ID)
	require.Equal(t, "build", f.fixMessages[1].PartitionKey)

	require.NoError(t, f.deliver(msg, false))
	require.Len(t, f.fixMessages, 2)

	f.reportIssue("next-build", "new", "unit")
	require.NoError(t, f.deliver(nextMessage, false))
	require.Len(t, f.fixMessages, 3)
	require.NotEqual(t, f.fixMessages[0].ID, f.fixMessages[2].ID)
	require.Equal(t, "next-build", f.fixMessages[2].PartitionKey)
}

func TestTaskWideFailureStillPublishesRepair(t *testing.T) {
	f := newFixture(t)
	f.source("build", "10", entity.BuildStatusFailed)
	f.seedIssue("existing", "unit", entity.IssueStatusOpen)
	f.reportIssue("build", "setup", "")
	require.NoError(t, f.deliver(f.message("build", true), false))
	require.Equal(t, entity.IssueTrackingPhaseFinalized, f.progress.Phase)
	require.Equal(t, entity.IssueStatusOpen, f.states[issueKey{"existing", "unit"}].Status)
	require.Len(t, f.fixMessages, 1)
}
